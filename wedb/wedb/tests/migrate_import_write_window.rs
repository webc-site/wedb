#![recursion_limit = "256"]
//! 迁移帧导入写回 RMW 窗回归（票
//! wedb-migrate-import-frame-bare-write-outside-rmw-window）
//!
//! 缺陷形态：`import_migration_frames` 记录写回三步（信封臂旧 TTL 清退、
//! 值写、TTL 回填 `put_ttl_sync`）在 RMW 窗外裸写，绕开同址本键桶闩——与
//! 目的端 IMPORTING 态 ASKING 放行后的用户写（`slot_verify.rs` 与 C#
//! ClusterSlotVerify.cs 同形放行本端执行）构成非可串行化交错：用户窗内读
//! 旧值后、写回前导入裸写落域，用户写回覆盖迁移值，源端已 ACK 删键即两侧
//! 永久发散；`put_ttl_sync` 裸回填落他者持闩 EXPIRE NX 的读—写间隙，条件臂
//! 按旧 TTL 裁决覆盖迁移 TTL。C# 锁形下不可产生（InternalUpsert.cs:67 与
//! InternalRMW.cs:70 同取 FindOrCreateTagAndTryEphemeralXLock，导入
//! `basicGarnetApi.SET` 与用户 RMW 同锁表串行）。
//!
//! 修复态：写回臂逐键先取本键窗（同步快窗单次尝试，失窗对齐用户 SET 族
//! 慢臂走 `rmw_window` 异步域窗），清退 / 值写 / TTL 回填三段并入同一窗
//! 临界区；窗内只配无闩变体（`clear_ttl`/`put_ttl_sync`），`persist_key`/
//! `expire_at_ticks` 系 `batch.persist`/`batch.expire_at` 自取同址本键桶闩，
//! 窗内直调即双取自锁——TTL 回填未闭环先出窗再降级带闩全量口。
//!
//! 断言口径（结构臂焊死闩在路径上 + 真并发臂锁串行化终态，先例
//! expire_persist_latch_concurrency 三测形制；IMPORTING 门系控制面放行
//! 条件非串行化机制本体，用户写以真 RESP 会话驱动放行后的同一写链）：
//! - 他会话持本键窗期间导入写回绝不落域（值与 TTL 双判），放闩重放即
//!   完整闭环；
//! - APPEND 与导入帧真并发：终值恒以迁移值开头且余段为整后缀（丢写
//!   「迁移值被顶」非可串行化终态按构造消失）；
//! - EXPIRE NX 与带 TTL 导入帧真并发：终态 TTL 恒为迁移刻度（条件臂按
//!   旧 TTL 误裁覆盖迁移 TTL 的终态消失）。

use std::{
  sync::{Arc, Barrier},
  thread,
};

use compio::runtime::Runtime;
use parking_lot::Mutex;
use wbase::time::now_ticks;
use wconn::record::{MigrateVal, MigrationFrame, MigrationRecord};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  migration::{
    chunk_reassembler::ChunkReassembler,
    frame_import::{FrameImport, import_migration_frames},
    migrate_driver::{LiveValue, read_live_value},
  },
};
use wedb_test::store_node::{StoreNode, open_store};
use wkv::WedbStore;
use wnode::StorageSession;
use wnode_test::{consumer_on, roundtrip};
use wresp::cmd_strings::RESP_ERR_SLOW_PATH_STORAGE;

/// 对拍键（用户写与导入帧同键争用）
const K: &[u8] = b"mig:win:k";
/// 导入帧迁移值（终值前缀判据锚点）
const MIG_VAL: &[u8] = b"migrated-value";
/// 用户 APPEND 后缀（终值余段须为整后缀重复）
const USER_SUFFIX: &[u8] = b"-u";
/// 用户 EXPIRE NX 时长秒（与迁移刻度异值，条件臂误裁终态可分辨）
const USER_TTL_SECONDS: &[u8] = b"120000";

/// 一次迁移帧导入（MIGRATE 导槽链形态），错误透传供持闩判败臂断言
async fn run_import(
  store: &Arc<WedbStore<SegmentedDevice>>,
  provider: &Arc<ClusterProvider>,
  frames: Vec<MigrationFrame<'static>>,
  replace: bool,
) -> Result<(), String> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  let chunks = Mutex::new(ChunkReassembler::new());
  let ri = None;
  let import = FrameImport {
    provider,
    session: &session,
    storage: &storage,
    chunks: &chunks,
    ri: &ri,
    replace,
    vector_slot: 0,
    accept_domain_frames: false,
  };
  import_migration_frames(frames, &import).await
}

/// 迁移帧集：单 Str 记录携迁移值与绝对过期 ticks
fn mig_frames(expire_ticks: i64) -> Vec<MigrationFrame<'static>> {
  vec![MigrationFrame::Record(MigrationRecord::Str {
    key: K,
    val: MIG_VAL,
    expire_ticks,
  })]
}

/// 读回落盘值字节与绝对过期 ticks（不可迁移即判负）
async fn read_back(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> (Vec<u8>, i64) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  match read_live_value(&storage, None, key).await.unwrap() {
    LiveValue::Migratable(MigrateVal::Str(v), expire)
    | LiveValue::Migratable(MigrateVal::Env(v), expire) => (v, expire),
    _ => panic!("键 {} 应可迁移读回", String::from_utf8_lossy(key)),
  }
}

/// 他会话持本键 RMW 窗期间，导入写回臂绝不落域（丢写臂结构判据：旧码
/// 窗外裸写在持闩期即落域，随后用户写回覆盖迁移值——源端 ACK 删键后两侧
/// 永久发散）；放闩重放（客户端重试语义）即完整闭环
#[compio::test]
async fn import_frame_write_yields_to_foreign_rmw_window() {
  let StoreNode { _dir, store } = open_store("mig-win-yield.db");
  let provider = Arc::new(ClusterProvider::new());
  let expire_ticks = now_ticks() + 60_000 * 10_000;

  // 用户会话持本键窗（对位 ASKING 放行用户 RMW 命令的窗内读—算—写全程）
  let sess = store.new_session().unwrap();
  let batch = sess.enter_batch();
  let window = batch.try_rmw_window(K).expect("空 store 上单次取窗必成");

  // 持闩期间导入判错（既有存储失败判败口径），且值与 TTL 双零落域
  let err = run_import(&store, &provider, mig_frames(expire_ticks), true)
    .await
    .expect_err("他会话持窗期间导入写回不得落域");
  assert_eq!(
    err, RESP_ERR_SLOW_PATH_STORAGE,
    "判败口径须为存储慢路径错误"
  );
  {
    let rt = Runtime::new().unwrap();
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GET", K]),
      b"$-1\r\n",
      "持窗期间导入值写不得落域"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"PTTL", K]),
      b":-2\r\n",
      "持窗期间导入 TTL 回填不得落域"
    );
  }

  // 放闩重放即闭环：迁移值与迁移刻度完整落域
  drop(window);
  run_import(&store, &provider, mig_frames(expire_ticks), true)
    .await
    .expect("放闩后导入重放应成功");
  let (val, expire) = read_back(&store, K).await;
  assert_eq!(val, MIG_VAL, "放闩重放后迁移值应完整落域");
  assert_eq!(expire, expire_ticks, "放闩重放后迁移刻度应完整落域");
}

/// 他会话持本键窗期间，带 TTL 导入帧的 TTL 回填臂（`put_ttl_sync`）绝不
/// 落域（TTL 误裁臂结构判据：旧码裸回填落他者持闩 EXPIRE NX 读—写间隙，
/// 条件臂按旧 TTL 裁决覆盖迁移刻度——目的端键提前过期或永生）；预置存活
/// 旧值同判不被覆写，放闩重放后值与刻度一体落域
#[compio::test]
async fn import_frame_ttl_backfill_yields_to_foreign_rmw_window() {
  let StoreNode { _dir, store } = open_store("mig-win-ttl.db");
  let provider = Arc::new(ClusterProvider::new());
  let expire_ticks = now_ticks() + 60_000 * 10_000;

  // 预置存活旧值（SET 无 TTL）
  {
    let rt = Runtime::new().unwrap();
    let mut c = consumer_on(&store);
    assert_eq!(roundtrip(&rt, &mut c, &[b"SET", K, b"old"]), b"+OK\r\n");
  }

  let sess = store.new_session().unwrap();
  let batch = sess.enter_batch();
  let window = batch.try_rmw_window(K).expect("存活键上单次取窗必成");

  let err = run_import(&store, &provider, mig_frames(expire_ticks), true)
    .await
    .expect_err("他会话持窗期间带 TTL 导入帧不得落域");
  assert_eq!(
    err, RESP_ERR_SLOW_PATH_STORAGE,
    "判败口径须为存储慢路径错误"
  );
  {
    let rt = Runtime::new().unwrap();
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GET", K]),
      b"$3\r\nold\r\n",
      "持窗期间导入值写不得覆写预置旧值"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"PTTL", K]),
      b":-1\r\n",
      "持窗期间导入 TTL 回填不得落域"
    );
  }

  drop(window);
  run_import(&store, &provider, mig_frames(expire_ticks), true)
    .await
    .expect("放闩后导入重放应成功");
  let (val, expire) = read_back(&store, K).await;
  assert_eq!(val, MIG_VAL, "放闩重放后迁移值应完整落域");
  assert_eq!(expire, expire_ticks, "放闩重放后迁移刻度应完整落域");
}

/// ASKING 用户写与落帧真并发混战：每轮串行重置预置存活键后，用户臂
/// （EXPIRE NX + APPEND 连发）与导入臂（带 TTL 迁移帧，判败按客户端重试
/// 语义重放）栅栏放行真并发交叉，逐轮断言可串行化终态：
/// - 终值恒以迁移值开头且余段为整 APPEND 后缀（旧码「用户窗内读旧值 →
///   导入裸写落域 → 用户写回顶替迁移值」的丢写终态按构造消失）；
/// - 终态 TTL 恒等于迁移刻度（EXPIRE NX 至多 :1 一次且必先于导入闭环，
///   导入刻度随后覆盖；旧码 `put_ttl_sync` 裸回填与条件臂写回交叠出的
///   「用户刻度顶替迁移刻度」终态消失）。
///
/// 末尾覆盖度断言：NX :1 判胜轮非零——「用户写先闭环、导入随后覆盖」的
/// 判别序真实发生过
#[test]
fn concurrent_asking_user_write_vs_import_frame_serializable() {
  let StoreNode { _dir, store } = open_store("mig-win-race.db");
  let provider = Arc::new(ClusterProvider::new());
  let rt = Runtime::new().unwrap();
  const ROUNDS: usize = 40;
  const APPENDS: usize = 4;
  let mut nx1_total = 0usize;

  for _ in 0..ROUNDS {
    // 串行重置：预置存活键（SET 无 TTL），每轮从可判定起点出发
    let mig_expire_ticks = now_ticks() + 60_000 * 10_000;
    {
      let mut c = consumer_on(&store);
      assert_eq!(
        roundtrip(&rt, &mut c, &[b"SET", K, b"seed"]),
        b"+OK\r\n",
        "轮前预置须成功"
      );
    }

    // 用户臂与导入臂各自独立连接 + 独立 Runtime，栅栏放行真并发交叉
    let gate = Arc::new(Barrier::new(2));
    let nx1 = {
      let store = Arc::clone(&store);
      let gate = Arc::clone(&gate);
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut c = consumer_on(&store);
        gate.wait();
        let out = roundtrip(&rt, &mut c, &[b"EXPIRE", K, USER_TTL_SECONDS, b"NX"]);
        let nx1 = usize::from(out == b":1\r\n");
        for _ in 0..APPENDS {
          let out = roundtrip(&rt, &mut c, &[b"APPEND", K, USER_SUFFIX]);
          assert!(
            !out.starts_with(b"-"),
            "APPEND 不应报错：{:?}",
            String::from_utf8_lossy(&out)
          );
        }
        nx1
      })
    };
    {
      let store = Arc::clone(&store);
      let provider = Arc::clone(&provider);
      let gate = Arc::clone(&gate);
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        gate.wait();
        // 判败沿既有可重试存储错通道按客户端重试语义重放（预算耗尽
        // LockTimeout 属瞬态，重放必闭环）
        for attempt in 0..200 {
          match rt.block_on(run_import(
            &store,
            &provider,
            mig_frames(mig_expire_ticks),
            true,
          )) {
            Ok(()) => return,
            Err(err) => {
              assert_eq!(
                err, RESP_ERR_SLOW_PATH_STORAGE,
                "导入失败只允许可重试存储错（第 {attempt} 次重放）"
              );
            }
          }
        }
        panic!("导入重放 200 次未闭环");
      })
      .join()
      .unwrap();
    }
    nx1_total += nx1.join().unwrap();

    // 可串行化终态判定一：终值恒以迁移值开头且余段为整 APPEND 后缀
    let (val, expire) = rt.block_on(read_back(&store, K));
    assert!(
      val.starts_with(MIG_VAL),
      "迁移值被并发用户写顶替（丢写，源端已 ACK 删键即永久发散）: {:?}",
      String::from_utf8_lossy(&val)
    );
    let rest = &val[MIG_VAL.len()..];
    assert_eq!(
      rest.len() % USER_SUFFIX.len(),
      0,
      "终值余段出现撕裂碎段: {:?}",
      String::from_utf8_lossy(&val)
    );
    assert!(
      rest.chunks(USER_SUFFIX.len()).all(|c| c == USER_SUFFIX),
      "终值余段非整 APPEND 后缀重复: {:?}",
      String::from_utf8_lossy(&val)
    );
    // 可串行化终态判定二：终态 TTL 恒为迁移刻度（EXPIRE NX 判胜必先于
    // 导入闭环，迁移刻度随后覆盖；导入先闭环则 NX 判 :0 不落笔）
    assert_eq!(
      expire, mig_expire_ticks,
      "终态 TTL 被并发 EXPIRE NX 误裁覆盖（条件臂按旧 TTL 裁决）"
    );
  }
  assert!(
    nx1_total > 0,
    "覆盖度不足：EXPIRE NX :1 判胜轮为零，判别串行序未发生过"
  );
}
