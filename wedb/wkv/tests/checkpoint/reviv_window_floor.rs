//! 复活池取臂跨轮窗口期地板抬升回归（票 zcode-r42-whlogfix 发现一·池取臂）
//!
//! C# 对位：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/BlockAllocate.cs:71-77
//! —— `Ctx.IsInV1` 期把 minRevivAddress 抬升至本轮检查点 startLogicalAddress，自由表
//! 绝不发出地板之下槽位。
//!
//! 场景：REST 期写入 K（无前驱）→ 删除触发 Record Elision 整帧归池（槽位地址
//! addr1 恒低于后续轮次开窗地板）→ 开窗（floor = 开窗瞬间 tail > addr1）→ 窗口期
//! 内重写同形 K。抬升缺失时池取直发 addr1 并整头覆写为携带纪元位的新记录：字节进
//! 快照物理收录面（addr < index_start 不在 undoNextVersion 回滚窗），AOF 版本戳
//! > covered 必重放——非幂等效果恰双算。抬升生效时槽位仅跳过让位不清零，写落
//! > 尾部追加面（addr >= index_start），恢复侧回滚剔除、无 AOF 形态下零承接即恰一次面。

use std::{fs::create_dir_all, sync::Arc};

use aok::{OK, Void};
use tempfile::tempdir;
use wcpr::{CheckpointType, next_token_above};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

const K: &[u8] = b"reviv:window:k";
/// 删除归池的原值（与重写值同长，保证槽位尺寸精确适配）
const V_OLD: &[u8] = &[b'a'; 512];
/// 窗口期重写值
const V_NEW: &[u8] = &[b'b'; 512];

/// 指定地址当前是否作为空闲槽位存在于复活池分桶
fn slot_in_pool(store: &WedbStore<SegmentedDevice>, addr: u64) -> bool {
  store
    .reviv_pool
    .bins
    .iter()
    .flat_map(|bin| bin.slots.iter())
    .any(|slot| !slot.is_empty() && slot.address() == addr)
}

#[compio::test]
async fn window_open_pool_take_lifts_floor_above_stale_slot() -> Void {
  let dir = tempdir()?;
  let db_path = dir.path().join("reviv_window_floor.db");
  let ckpt_dir = dir.path().join("checkpoints");
  create_dir_all(&ckpt_dir)?;

  let config = StoreConfig::new(1024, 4 * 1024, 16, 0.5)?.with_revivification(true);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
  let session = store.new_session()?;

  // 垫高尾部：K 的槽位归池后仍稳居复活水位之上（用例结构前提）
  session.upsert(b"reviv:window:pad", &[b'P'; 1200]).await?;

  let addr1 = session.upsert(K, V_OLD).await?;
  assert!(session.delete(K).await?, "单记录删除触发 Record Elision");
  assert_eq!(session.read(K).await?, None, "删除后键不存活");
  assert!(slot_in_pool(&store, addr1), "elide 删除必须整帧归池");

  // 开窗：地板 = 开窗瞬间 tail，必须越过归池槽位（跨轮低地址形态）
  let floor = store.begin_version_shift(1);
  assert!(
    floor > addr1,
    "结构前提: 地板必须越过归池槽位: floor={floor:#x} addr1={addr1:#x}"
  );
  assert!(
    addr1 >= store.min_revivifiable_address(),
    "结构前提: 槽位须在复活水位之上，否则池取水位线即淘汰，用例失真"
  );

  // 窗口期重写同形 K：取槽下界必须抬升至地板——槽位仅跳过让位不清零，
  // 写落尾部追加面（抬升缺失时此处复用 addr1，即快照收录 + AOF 重放双算形态）
  let addr_new = session.upsert(K, V_NEW).await?;
  assert!(
    addr_new >= floor && addr_new != addr1,
    "窗口期取槽必须越过地板落尾部追加: addr={addr_new:#x} floor={floor:#x}"
  );
  assert!(
    slot_in_pool(&store, addr1),
    "低于地板槽位仅跳过让位，不得被窗口期取槽消耗"
  );
  assert_eq!(
    session.read(K).await?,
    Some(V_NEW.to_vec()),
    "活态一致: 窗口写本身即时生效，跨轮承接面交由恢复侧裁决"
  );

  // 宿主时序对位：窗口保持开启进快照段，index_start 精准取开窗地板，返回后收口
  let token = next_token_above(0);
  let meta = store
    .create_checkpoint_with_token(&ckpt_dir, CheckpointType::FoldOver, token, floor)
    .await?;
  store.end_version_shift();
  assert_eq!(
    meta.index_start_logical_address, floor,
    "快照模糊区地板必须与开窗地板同源"
  );

  // 模拟断电宕机后恢复：窗内追加面（带位、addr >= index_start）被 undoNextVersion
  // 回滚剔除，无 AOF 形态下窗口写零承接——K 不得经快照复活
  drop(session);
  drop(store);
  drop(device);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let restored = Arc::new(WedbStore::recover_latest(&ckpt_dir, device).await?);
  let session = restored.new_session()?;

  assert_eq!(
    session.read(K).await?,
    None,
    "K 不得复活: 窗内写由恢复侧回滚，效果应由 AOF 重放恰一次承接;\
     抬升缺失时快照已物理收录地板之下的复活字节，此处反中"
  );
  assert_eq!(
    session.read(b"reviv:window:pad").await?,
    Some(vec![b'P'; 1200]),
    "垫高键（稳定面）不受跨轮窗口面影响"
  );
  OK
}
