//! 删除缺席观测钩子与 WATCH 版本推进收口集成测试（对标
//! garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:InitialDeleter
//! 无条件 watchVersionMap.IncrementVersion——缺席键墓碑追加同向计入；与
//! libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnDispose 的
//! DisposeReason.Deleted 臂 → VectorManager.RequestDeletion——值域外登记态
//! 随删除清退，命中登记即视同删除成功）：
//!
//! 1. 异步 delete（collection 层）双域物理未命中且宿主登记命中 → 钩子被回调
//!    且视同删除成功，与同步内核 try_delete_sync 同判据同单点；
//! 2. 异步 delete 全缺席键 → 返回 false 且 WATCH 版本仍无条件推进（同步删
//!    与异步删零分叉）；
//! 3. 同步内核缺席删除推进版本（C# InitialDeleter 对位回归锚定）。

use std::{
  collections::HashSet,
  sync::{Arc, Mutex},
};

use aok::Void;
use wdev::SegmentedDevice;
use wkv::{DeleteMissHook, WatchHook, WedbStore};
use wtest_base::open_test_store;

/// 会话默认物理前缀（ns 0 / db 0 的 Varint 投影）
const DEFAULT_PREFIX: &[u8] = &[0, 0];

type WatchBumps = Arc<Mutex<Vec<(Vec<u8>, Vec<u8>)>>>;

/// 宿主登记态删除缺席观测 + WATCH 版本推进观测（生产同型装配：两钩子均
/// 装配期一次性注入，先后于会话派生）
fn setup(
  store: &Arc<WedbStore<SegmentedDevice>>,
  registry: Arc<Mutex<HashSet<Vec<u8>>>>,
  bumps: WatchBumps,
) {
  let reg = Arc::clone(&registry);
  assert!(
    store.set_delete_miss_hook(DeleteMissHook::new(move |_prefix, key| {
      let hit = reg.lock().unwrap().remove(key);
      Box::pin(async move { hit })
    })),
    "delete_miss_hook 装配期一次性注入"
  );
  assert!(
    store.set_watch_hook(WatchHook::new(bumps, |b, prefix, key| b
      .lock()
      .unwrap()
      .push((prefix.to_vec(), key.to_vec())))),
    "watch_hook 装配期一次性注入"
  );
}

/// 测试 1 + 2：异步 delete 缺席收口——登记键 hook 命中视同删除成功且版本
/// 推进恰一次；全缺席键返回 false 仍无条件推进版本
#[compio::test]
async fn async_delete_miss_hook_and_unconditional_watch_bump() -> Void {
  let (_dir, store) = open_test_store("delete_miss_watch_async")?;
  let registry = Arc::new(Mutex::new(HashSet::from([b"reg:key".to_vec()])));
  let bumps: WatchBumps = Arc::new(Mutex::new(Vec::new()));
  setup(&store, Arc::clone(&registry), Arc::clone(&bumps));
  let session = store.new_session()?;

  // 登记键：String/ObjectEnvelope 双域物理未命中，hook 摘除登记 → 视同删除成功
  assert!(session.delete(b"reg:key").await?, "hook 命中应视同删除成功");
  assert!(
    !registry.lock().unwrap().contains("reg:key".as_bytes()),
    "登记态应随缺席删除清退"
  );
  {
    let bumps = bumps.lock().unwrap();
    assert_eq!(bumps.len(), 1, "登记键删除版本推进恰一次");
    assert_eq!(
      bumps.last().map(|(p, k)| (p.as_slice(), k.as_slice())),
      Some((DEFAULT_PREFIX, "reg:key".as_bytes())),
      "版本推进与写入物理落点同域同键"
    );
  }

  // 全缺席键：无任何物理命中也无登记，返回 false 但版本仍无条件推进
  assert!(!session.delete(b"ghost").await?, "全缺席键维持缺席口径");
  {
    let bumps = bumps.lock().unwrap();
    assert_eq!(bumps.len(), 2, "缺席键删除同样推进版本");
    assert_eq!(
      bumps.last().map(|(_, k)| k.as_slice()),
      Some(b"ghost".as_slice())
    );
  }
  Ok(())
}

/// 测试 3：同步内核缺席删除推进版本（InitialDeleter 对位回归锚定：
/// Ok(Ok(false)) 与命中删除 Ok(Ok(true)) 同向计入，快慢路径零分叉）
#[compio::test]
async fn sync_delete_absent_and_hit_bump_watch() -> Void {
  let (_dir, store) = open_test_store("delete_miss_watch_sync")?;
  let bumps: WatchBumps = Arc::new(Mutex::new(Vec::new()));
  assert!(
    store.set_watch_hook(WatchHook::new(Arc::clone(&bumps), |b, prefix, key| b
      .lock()
      .unwrap()
      .push((prefix.to_vec(), key.to_vec()))))
  );
  let session = store.new_session()?;

  // 命中删除：相对基线恰一次推进（upsert 自身的 bump 不计入基线）
  session.upsert(b"hit", b"v").await?;
  let n0 = bumps.lock().unwrap().len();
  assert!(
    matches!(session.try_delete_sync(b"hit")?, Ok(true)),
    "命中删除应成功"
  );
  assert_eq!(
    bumps.lock().unwrap().len() - n0,
    1,
    "命中删除版本推进恰一次"
  );

  // 缺席删除：同样推进（C# 缺席键墓碑追加同向计入）
  let n1 = bumps.lock().unwrap().len();
  assert!(matches!(session.try_delete_sync(b"absent")?, Ok(false)));
  {
    let bumps = bumps.lock().unwrap();
    assert_eq!(bumps.len() - n1, 1, "同步缺席删除版本推进恰一次");
    assert_eq!(
      bumps.last().map(|(_, k)| k.as_slice()),
      Some(b"absent".as_slice())
    );
  }
  Ok(())
}
