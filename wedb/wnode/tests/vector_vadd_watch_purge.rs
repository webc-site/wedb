//! VADD 登记创建位过期残留清退 × WATCH 假弃语义锁（案 zcode-r151c-exwatch 案二）
//!
//! 缺陷形（修前红灯，按票面 c 留档）：`SET k v PX →` 到期后 wkv 值域残留
//! TTL 旁路记录 + 判死值记录；向量守卫对「判死残留」视同缺席放行（本身正确，
//! 键死后重建向量合 Redis 语义），但登记创建不清残——其后任意会话的到期惰性
//! 清退 / GC 轮（`check_expired → purge_expired → delete` 级联）对已复活为
//! 向量键的现代存活键推进一次「过去代残留假事件」版本，WATCH 登记的基线
//! （纯版本快照零存活探针，对标面 C# AddWatch 同形）被假脏，EXEC 假回
//! `*-1` 弃。C# 无对位假事件：VADD 落主存同记录槽
//! （garnet/libs/server/Storage/Session/MainStore/VectorStoreOps.cs:190），
//! 撞过期记录经写臂 CheckExpiry → ExpireAndStop 当时消费
//! （libs/server/Storage/Functions/MainStore/RMWMethods.cs:441/:766-768），
//! TTL 随记录一体换写零残留。
//!
//! 修后终态：VADD 命令内清退（exec 同步快臂 + 慢臂 delete 级联承接，见
//! `ttl_sync::purge_expired_residue_sync`），清退 bump 与 VADD 自身 bump
//! 合并同命令、恒先于任何后续 WATCH 登记；登记面已无残留可清，命令后驱动
//! 到期清退一轮零推进，WATCH 纯读事务照常 EXEC 成功。
//!
//! 环境为真实引擎 + 共享版本表 + 生产同径装配（vector_watch_env，与
//! tiered_watch_fence.rs 同款），无任何 mock；§75 组合锁（向量键 EXISTS :1 /
//! TTL :-2 / EXPIRE :0 不落旁路）在本夹具同步复核不破形。

use std::{sync::Arc, thread::sleep, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_store_callbacks::{
        ActiveDedicatedVectorSession, OwnedActiveVectorSession, WedbVectorStoreCallbacks,
      },
    },
  },
  service::StoreSwapSlot,
  storage::session::storage_session::{vector_version_watch_hook, version_map_watch_hook},
};
use wnode_test::drain_output;
use wresp::command::RespCommand;
use wtxn::{TxnKeyEntryComparison, TxnLockTable, WatchVersionMap};
use wval::SessionPrefixBuf;
use wvector::Callbacks;

type TestStore = WedbStore<SegmentedDevice>;

/// 向量域 WATCH 测试环境（生产同径装配：登记表管理器 + 引擎级写面钩子 +
/// vector_version_watch_hook 换算入版本轨，与 tiered_watch_fence.rs 同款）
struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  map: Arc<WatchVersionMap>,
  lock_table: TxnLockTable,
  api: GarnetApi,
  _dir: tempfile::TempDir,
}

fn vector_watch_env(tag: &str) -> (Env, Arc<VectorManager>) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&map))),
    "引擎级写面钩子应首次挂载"
  );
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new())),
  ));
  let fs = Arc::clone(&store);
  vm.attach_dedicated_session_factory(Arc::new(move || {
    fs.new_session()
      .ok()
      .map(OwnedActiveVectorSession::new)
      .map(ActiveDedicatedVectorSession::from_bound)
  }));
  vm.set_watch_bump(vector_version_watch_hook(
    Arc::clone(&store),
    StoreSwapSlot::new(),
    Arc::clone(&map),
  ));
  let api: GarnetApi = Arc::new(
    StoreGarnetApi::new(store.new_session().unwrap()).with_vector_manager(Arc::clone(&vm)),
  );
  let env = Env {
    rt: Runtime::new().unwrap(),
    store,
    map,
    lock_table: TxnLockTable::new(),
    api,
    _dir: dir,
  };
  (env, vm)
}

fn session_with(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

/// 命令面同步求值并回帧字节（慢路径经本线程 compio runtime 收割）
fn auto_exec(env: &Env, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

fn frame(cmds: &[&[&[u8]]]) -> Vec<u8> {
  let mut out = Vec::new();
  for c in cmds {
    out.extend_from_slice(format!("*{}\r\n", c.len()).as_bytes());
    for a in c.iter() {
      out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
      out.extend_from_slice(a);
      out.extend_from_slice(b"\r\n");
    }
  }
  out
}

/// 直填会话输入泵并取输出（WATCH/MULTI/EXEC 纯命令面驱动）
fn pump(s: &mut RespServerSession, bytes: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(bytes);
  assert!(s.try_consume_messages().is_some(), "帧应完整消费");
  drain_output(s)
}

/// 版本表读点（与 wtxn 校验同一哈希面：根域 scoped）
fn ver(env: &Env, key: &[u8]) -> u64 {
  env
    .map
    .read_version(
      TxnKeyEntryComparison::scoped_key_hash(SessionPrefixBuf::ROOT.as_slice(), key) as u64,
    )
}

/// 手动驱动 wkv 到期清退一轮（check_expired 公开臂，票面 b 点形制）
async fn drive_ttl_purge(env: &Env, key: &[u8]) -> bool {
  let sess = env.store.new_session().unwrap();
  let purged = sess.check_expired(key).await.unwrap();
  drop(sess);
  purged
}

fn fp32_vec(seed: f32) -> Vec<u8> {
  [seed; 4].iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// 核心锁（票面 a+b）：SET k v PX → 到期（残留在场）→ VADD 成功且命令内清退
/// → EXISTS :1 / TTL :-2（§75 组合不破）→ WATCH k → MULTI/EXEC 纯读事务成功
/// 非弃；再手动驱动到期清退一轮 → 零推进、照常 EXEC 成功，钉「登记面已无
/// 残留可清」终态。
///
/// 修前红灯留档（票面 c）：同序列下 VADD 后残留存续，WATCH 登记基线后
/// `check_expired(k)` 回 true 且版本 +1（purge_expired → delete 级联 bump），
/// EXEC 假回 `*-1` 弃；本夹具断言形即修后绿态（false / 零推进 / `*1 +PONG`）。
#[test]
fn vadd_purges_expired_residue_so_watch_txn_not_spuriously_aborted() {
  Runtime::new().unwrap().block_on(async {
    let (env, _vm) = vector_watch_env("vadd-residue-watch.db");
    let mut s = session_with(&env);
    s.attach_transaction_components(Arc::clone(&env.map), env.lock_table.clone());

    // 1. 现代缺席前代：SET 带短 TTL，自然到期，死残留物理在场
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Set, &[b"k", b"v", b"PX", b"30"]),
      b"+OK\r\n"
    );
    sleep(Duration::from_millis(60));

    // 2. VADD 判死残留放行建档，成功应答且命令内清退残留
    let vec4 = fp32_vec(0.5);
    assert_eq!(
      auto_exec(
        &env,
        &mut s,
        RespCommand::Vadd,
        &[b"k", b"FP32", &vec4, b"elem1"]
      ),
      b":1\r\n",
      "VADD 于过期残留键建档应放行成功"
    );

    // 3. §75 组合锁不破：登记表第四态 EXISTS :1、TTL :-2
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Exists, &[b"k"]),
      b":1\r\n"
    );
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Ttl, &[b"k"]),
      b":-2\r\n"
    );

    // 4. 纯读事务基线：WATCH → MULTI → EXEC 成功非弃
    assert_eq!(
      pump(
        &mut s,
        &frame(&[&[b"WATCH", b"k"], &[b"MULTI"], &[b"PING"]])
      ),
      b"+OK\r\n+OK\r\n+QUEUED\r\n"
    );
    assert_eq!(
      pump(&mut s, &frame(&[&[b"EXEC"]])),
      b"*1\r\n+PONG\r\n",
      "VADD 清退后 WATCH 纯读事务不得被残留假事件弃（EXEC 假回 *-1 即案二缺陷形）"
    );

    // 5. 登记面已无残留可清：手动驱动到期清退一轮 → 无 TTL 旁路记录可判
    //    到期（false），版本零推进
    let v_after = ver(&env, b"k");
    assert!(
      !drive_ttl_purge(&env, b"k").await,
      "VADD 登记创建位已清残留，命令后不得再有可清退之过去代记录"
    );
    assert_eq!(ver(&env, b"k"), v_after, "清退驱动后版本必须零推进");

    // 6. 清退驱动之后照常 EXEC 成功
    assert_eq!(
      pump(
        &mut s,
        &frame(&[&[b"WATCH", b"k"], &[b"MULTI"], &[b"PING"]])
      ),
      b"+OK\r\n+OK\r\n+QUEUED\r\n"
    );
    assert_eq!(
      pump(&mut s, &frame(&[&[b"EXEC"]])),
      b"*1\r\n+PONG\r\n",
      "到期清退驱动后 WATCH 事务照常成功（向量键零变更）"
    );
  });
}

/// 交叠形锁（票面 d）：VADD 与同键 EXPIRE / 重复 VADD / 新元素 VADD 交叠——
/// 清退位前移命令内后重复添加零推进、EXPIRE 族对向量键恒 :0 不落旁路、
/// 登记幂等零双写、后续合法变更推进正常弃形不误伤
#[test]
fn vadd_overlap_forms_purge_idempotent_and_bump_clean() {
  Runtime::new().unwrap().block_on(async {
    let (env, vm) = vector_watch_env("vadd-residue-overlap.db");
    let mut s = session_with(&env);

    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Set, &[b"o", b"v", b"PX", b"30"]),
      b"+OK\r\n"
    );
    sleep(Duration::from_millis(60));

    let vec4 = fp32_vec(1.0);
    assert_eq!(
      auto_exec(
        &env,
        &mut s,
        RespCommand::Vadd,
        &[b"o", b"FP32", &vec4, b"e1"]
      ),
      b":1\r\n"
    );
    let v0 = ver(&env, b"o");

    // EXPIRE 族对向量键判死早退 :0（§75：不落 TTL 旁路记录，无可残形）
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Expire, &[b"o", b"100"]),
      b":0\r\n"
    );
    assert!(!drive_ttl_purge(&env, b"o").await);
    assert_eq!(ver(&env, b"o"), v0, "EXPIRE :0 与清退驱动均须零推进");

    // 重复添加 Duplicate：零变更零推进（登记幂等，bump 只随真实变更）
    assert_eq!(
      auto_exec(
        &env,
        &mut s,
        RespCommand::Vadd,
        &[b"o", b"FP32", &vec4, b"e1"]
      ),
      b":0\r\n"
    );
    assert_eq!(ver(&env, b"o"), v0, "Duplicate VADD 零推进");

    // 新元素：恰一次推进（合法变更，WATCH 该弃形不误伤——EXEC 假弃仅指残留假事件）
    let vec4b = fp32_vec(2.0);
    assert_eq!(
      auto_exec(
        &env,
        &mut s,
        RespCommand::Vadd,
        &[b"o", b"FP32", &vec4b, b"e2"]
      ),
      b":1\r\n"
    );
    assert_eq!(ver(&env, b"o"), v0 + 1, "新元素 VADD 恰推进一次");

    // 登记表零双写：同键条目唯一（read_stored_index 单点在案）
    let prefix = SessionPrefixBuf::ROOT;
    assert!(vm.read_stored_index(prefix.as_slice(), b"o").is_some());
  });
}
