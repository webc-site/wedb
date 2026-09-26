//! 对象族四处 RMW 的 AOF 入队失败吞错假成功矩阵收口回归（票
//! wnode-objrmw-aof-enqueue-swallow-matrix）
//!
//! 缺陷形：对象族四处入账点（run_sync_rmw 增量条目 notify_object_rmw、
//! obj_save_custom_notified 信封整值、rename_sync 对象域直发
//! notify_envelope_upsert、emit_tiered_mirror 分层稳态镜像）对
//! `wkv::error::AofEnqueue` 契约（「主存写入已生效，AOF 缺条目，调用方须以
//! 错误拒绝该命令防主从发散」）集体吞错冒答成功：客户端得 `:1`/`+OK`/计数
//! 帧而副本/恢复端永缺条目，主从静默发散。
//! 对标 C# 锚：Garnet 日志先行单点 TsavoriteLog.cs:1311 `void Enqueue` 无
//! 吞错路径；Resp 层持久化失败一律 `SetErr(AofTaskStore)` 拒命令
//! （libs/server/Resp/ObjectStore/*Commands.cs）。
//! 修法（单点收敛于 AofEnqueue 契约，无第五机制）：四处沿既有 Result 链上
//! 抛——同步骨架终态 [`SyncRmwOutcome::AofFail`]（严禁借 Degrade：慢臂整体
//! 重放对 HINCRBY/ZINCRBY/LPUSH 等非幂等算子二次施加）、同步信封漏斗
//! `Result` 上抛转既有 RESP_ERR_GENERIC 臂、rename 走 `bail_err_frame!`、
//! 分层臂尾撤帧转 `Err(())` 落 RESP_ERR_SLOW_PATH_STORAGE；内存写不回滚
//! （发散可见可感知，客户端得错误而非假成功，口径同 r167c-aoffail）。
//!
//! 判据（全走生产命令面 + 真故障 sink 注错，禁假 mock，注入形制同
//! wkv/tests/range_index_aof_enqueue_fail.rs）：
//! 1. 同步快臂（ObjectStoreRMW 增量条目注错）：HSET/HINCRBY/LPUSH 一律回
//!    `-ERR generic error` 错误帧，禁 `:1`/`:N` 假成功；HINCRBY 终值恰一次
//!    施加（借 Degrade 重放即二次施加转红），解除注错后重试照常入账；
//! 2. 同步信封漏斗（obj_save_custom_notified 入账注错，LPUSHX 臂）：错误帧
//!    拒绝、成功入账计数零推进；
//! 3. RENAME 对象键（EnvelopeUpsert 入账注错）：禁 `+OK`；旧键未删、新键已
//!    写（不回滚，双键并存对客户端以错误显式可见）；解除后重试 `+OK` 且镜
//!    像恰一次入账；
//! 4. 异步 obj_save 臂对照（冷键慢路径，同 EnvelopeUpsert 故障同机制对偶）：
//!    回 `ERR slow path storage error` 帧禁 `:15`；变更恰一次生效不回滚；
//! 5. 分层稳态臂（手工升阶 zset，TieredCollectionWrite 入账注错）：ZADD 回
//!    存储错误帧禁计数帧、撤帧零成功负载残留、镜像零入账；解除注错后新成
//!    员 ZADD 闭环 `:1` 且镜像恰一次入账（对照臂先行证注入面真实生效）。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  thread::sleep,
  time::Duration,
};

use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wcol::types::member_ttl::encode_member;
use wdev::SegmentedDevice;
use wkv::{Error as WkvError, StoreConfig, StoreEvent, StoreEventSink, WedbStore};
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::{err_frame, roundtrip};
use wresp::cmd_strings::{RESP_ERR_GENERIC, RESP_ERR_SLOW_PATH_STORAGE};
use wtest_base::test_store_config;
use wval::GarnetObjectType;

type TestStore = WedbStore<SegmentedDevice>;

/// 同步臂通用错误帧与慢路径存储错误帧期望字节（单点常量派生，同
/// setrange/load_type 族先例）
fn generic_frame() -> Vec<u8> {
  err_frame(&format!("ERR {RESP_ERR_GENERIC}"))
}

fn slow_storage_frame() -> Vec<u8> {
  err_frame(RESP_ERR_SLOW_PATH_STORAGE)
}

/// 按事件类型注错的 AOF 入账故障上下文（未注错事件直通并计数成功入账）
struct FaultCtx {
  fail_objrmw: AtomicBool,
  fail_envelope: AtomicBool,
  fail_tiered: AtomicBool,
  objrmw_ok: AtomicU64,
  envelope_ok: AtomicU64,
  tiered_ok: AtomicU64,
}

impl FaultCtx {
  fn new() -> Self {
    Self {
      fail_objrmw: AtomicBool::new(false),
      fail_envelope: AtomicBool::new(false),
      fail_tiered: AtomicBool::new(false),
      objrmw_ok: AtomicU64::new(0),
      envelope_ok: AtomicU64::new(0),
      tiered_ok: AtomicU64::new(0),
    }
  }
}

fn inject() -> WkvError {
  WkvError::AofEnqueue("注入：AOF 入账失败（测试）".into())
}

fn fault_handler(ctx: &FaultCtx, _ver: i64, _sid: i32, event: StoreEvent<'_>) -> wkv::Result<()> {
  match event {
    StoreEvent::ObjectRmw(_) => {
      if ctx.fail_objrmw.load(Ordering::Acquire) {
        return Err(inject());
      }
      ctx.objrmw_ok.fetch_add(1, Ordering::Relaxed);
    }
    StoreEvent::EnvelopeUpsert { .. } => {
      if ctx.fail_envelope.load(Ordering::Acquire) {
        return Err(inject());
      }
      ctx.envelope_ok.fetch_add(1, Ordering::Relaxed);
    }
    StoreEvent::TieredCollectionWrite(_) => {
      if ctx.fail_tiered.load(Ordering::Acquire) {
        return Err(inject());
      }
      ctx.tiered_ok.fetch_add(1, Ordering::Relaxed);
    }
    _ => {}
  }
  Ok(())
}

/// 存储装配：sink 须在创建任何会话前注入（OnceLock 独占）；
/// `tiered` = 分层引擎配置（与 tiered_promote_contract_gate 同款建树受理面）
fn open_store(tag: &str, tiered: bool) -> (Arc<TestStore>, TempDir, Arc<FaultCtx>) {
  let dir = tempdir().unwrap();
  let config = if tiered {
    StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap()
  } else {
    test_store_config()
  };
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let ctx = Arc::new(FaultCtx::new());
  assert!(
    store.set_event_sink(StoreEventSink::new(Arc::clone(&ctx), fault_handler)),
    "事件分发器注入失败（重复注入或时机过晚）"
  );
  (store, dir, ctx)
}

fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 后台事件分发/驻留落稳基线
fn quiesce() {
  sleep(Duration::from_millis(20));
}

/// 案一 同步快臂（ObjectStoreRMW 增量条目）注错：HSET/HINCRBY/LPUSH 一律
/// 错误帧拒绝，禁 `:1`/`:N` 假成功；HINCRBY 终值恰一次施加（非幂等算子防
/// 借 Degrade 重放二次施加的刚性判点）；解除注错后照常入账
#[test]
fn warm_sync_arm_objrmw_enqueue_failure_rejects_without_double_apply() {
  let (store, _dir, ctx) = open_store("objaof-sync-arm.db", false);
  let rt = Runtime::new().unwrap();

  // 种子（注错关）：三键暖态驻留，同步臂受理面锚定
  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"HSET", b"h:w1", b"f", b"v"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"HSET", b"h:w2", b"n", b"10"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"LPUSH", b"l:w3", b"a"]),
    b":1\r\n"
  );
  drop(seed);
  quiesce();

  let base_objrmw = ctx.objrmw_ok.load(Ordering::Relaxed);
  ctx.fail_objrmw.store(true, Ordering::Release);

  let mut c = consumer_on(&store);
  // HSET：入账失败必须拒绝命令，禁 `:1`/`:0` 假成功帧
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"h:w1", b"f2", b"v2"]),
    generic_frame(),
    "HSET 同步臂 AOF 入队失败不得冒答计数成功帧"
  );
  // HINCRBY：非幂等算子，严禁借 Degrade 转异步重放（重放即二次施加）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HINCRBY", b"h:w2", b"n", b"5"]),
    generic_frame(),
    "HINCRBY 同步臂入账失败不得冒答 :15 值成功帧"
  );
  // LPUSH：同上禁 `:2` 长度成功帧
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LPUSH", b"l:w3", b"b"]),
    generic_frame(),
    "LPUSH 同步臂入账失败不得冒答长度成功帧"
  );
  assert_eq!(
    ctx.objrmw_ok.load(Ordering::Relaxed),
    base_objrmw,
    "入账失败轮严禁计入成功条目"
  );
  drop(c);

  // 内存写不回滚（发散显式可见）且恰一次施加：重放双施即此处转红
  let mut v = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut v, &[b"HGET", b"h:w2", b"n"]),
    b"$2\r\n15\r\n",
    "HINCRBY 入账失败臂变更恰一次生效（借 Degrade 重放即二次施加为 20）"
  );
  assert_eq!(roundtrip(&rt, &mut v, &[b"LLEN", b"l:w3"]), b":2\r\n");
  assert_eq!(
    roundtrip(&rt, &mut v, &[b"HEXISTS", b"h:w1", b"f2"]),
    b":1\r\n",
    "HSET 入账失败臂字段已生效可见"
  );
  drop(v);

  // 解除注错：同键续写照常受理并入账（反空跑：修法非「一律拒写」）
  ctx.fail_objrmw.store(false, Ordering::Release);
  let base_objrmw = ctx.objrmw_ok.load(Ordering::Relaxed);
  let mut w = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut w, &[b"HSET", b"h:w1", b"f2", b"v2"]),
    b":0\r\n",
    "解除注错后重复 HSET 回 0 新增（字段已存在），命令照常受理"
  );
  assert_eq!(
    roundtrip(&rt, &mut w, &[b"HINCRBY", b"h:w2", b"n", b"5"]),
    b":20\r\n"
  );
  assert_eq!(
    ctx.objrmw_ok.load(Ordering::Relaxed),
    base_objrmw + 2,
    "解除注错后两笔变更镜像各恰一次入账"
  );
}

/// 案二 同步信封整值漏斗（obj_save_custom_notified 经 obj_save_or_gc）注错：
/// LPUSHX 臂沿既有 Result 链上抛转 RESP_ERR_GENERIC 错误帧，禁长度假成功；
/// 解除后重试闭环且镜像恰一次入账
#[test]
fn warm_sync_envelope_funnel_enqueue_failure_rejects() {
  let (store, _dir, ctx) = open_store("objaof-sync-env.db", false);
  let rt = Runtime::new().unwrap();

  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"LPUSH", b"l:e1", b"a"]),
    b":1\r\n"
  );
  drop(seed);
  quiesce();

  let base_env = ctx.envelope_ok.load(Ordering::Relaxed);
  ctx.fail_envelope.store(true, Ordering::Release);
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LPUSHX", b"l:e1", b"x"]),
    generic_frame(),
    "同步信封漏斗入账失败不得冒答长度成功帧"
  );
  assert_eq!(
    ctx.envelope_ok.load(Ordering::Relaxed),
    base_env,
    "入账失败轮严禁计入成功条目"
  );
  drop(c);

  // 主存先行不回滚：弹出前值已生效可见（恰一次施加）
  let mut v = consumer_on(&store);
  assert_eq!(roundtrip(&rt, &mut v, &[b"LLEN", b"l:e1"]), b":2\r\n");
  drop(v);

  ctx.fail_envelope.store(false, Ordering::Release);
  let base_env = ctx.envelope_ok.load(Ordering::Relaxed);
  let mut w = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut w, &[b"LPUSHX", b"l:e1", b"y"]),
    b":3\r\n"
  );
  assert_eq!(
    ctx.envelope_ok.load(Ordering::Relaxed),
    base_env + 1,
    "解除注错后信封整值镜像恰一次入账"
  );
}

/// 案三 RENAME 对象键入账注错：禁 `+OK` 假成功（漏记即副本只删旧键、新键
/// 无从建立）；失败臂旧键未删、新键已写（不回滚，双键并存对客户端以错误
/// 显式可见）；解除注错后重试 `+OK` 且镜像恰一次入账闭环
#[test]
fn rename_object_key_enqueue_failure_rejects_then_retry_ok() {
  let (store, _dir, ctx) = open_store("objaof-rename.db", false);
  let rt = Runtime::new().unwrap();

  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"HSET", b"rn:src", b"f", b"v"]),
    b":1\r\n"
  );
  drop(seed);
  quiesce();

  let base_env = ctx.envelope_ok.load(Ordering::Relaxed);
  ctx.fail_envelope.store(true, Ordering::Release);
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RENAME", b"rn:src", b"rn:dst"]),
    generic_frame(),
    "RENAME 对象域入账失败不得冒答 +OK"
  );
  assert_eq!(
    ctx.envelope_ok.load(Ordering::Relaxed),
    base_env,
    "入账失败轮严禁计入成功条目"
  );
  // 新键写已生效、旧键删除尚未执行（笔序即回滚禁入的形态面）：双键并存可见
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HEXISTS", b"rn:dst", b"f"]),
    b":1\r\n",
    "RENAME 失败臂新键写入不回滚（发散可见）"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HEXISTS", b"rn:src", b"f"]),
    b":1\r\n",
    "RENAME 失败臂旧键未删（错误帧外零假象）"
  );
  drop(c);

  ctx.fail_envelope.store(false, Ordering::Release);
  let base_env = ctx.envelope_ok.load(Ordering::Relaxed);
  let mut w = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut w, &[b"RENAME", b"rn:src", b"rn:dst"]),
    b"+OK\r\n",
    "解除注错后重试 RENAME 闭环 +OK"
  );
  assert_eq!(
    ctx.envelope_ok.load(Ordering::Relaxed),
    base_env + 1,
    "重试轮 RENAME 镜像恰一次入账"
  );
  assert_eq!(roundtrip(&rt, &mut w, &[b"EXISTS", b"rn:src"]), b":0\r\n");
  assert_eq!(
    roundtrip(&rt, &mut w, &[b"HEXISTS", b"rn:dst", b"f"]),
    b":1\r\n",
    "重试闭环后新键数据入账可查"
  );
}

/// 案四 异步 obj_save 臂对照（同步骨架修复的既有正确对偶）：冷键慢路径
/// 重放 HINCRBY，同 EnvelopeUpsert 故障同机制 `?` 上抛——回
/// RESP_ERR_SLOW_PATH_STORAGE 帧禁 `:15` 值成功帧；变更恰一次生效不回滚，
/// 解除注错后续施加终值自洽（无双施）
#[test]
fn cold_async_objsave_arm_same_failure_same_rejection() {
  let (store, _dir, ctx) = open_store("objaof-async-arm.db", false);
  let rt = Runtime::new().unwrap();

  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"HSET", b"h:c1", b"n", b"10"]),
    b":1\r\n"
  );
  drop(seed);
  rt.block_on(store.flush_and_evict_all())
    .expect("冷化刷盘不得报错");
  quiesce();

  let base_env = ctx.envelope_ok.load(Ordering::Relaxed);
  ctx.fail_envelope.store(true, Ordering::Release);
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HINCRBY", b"h:c1", b"n", b"5"]),
    slow_storage_frame(),
    "异步 obj_save 臂入账失败必须落慢路径存储错误帧，禁 :15 假成功"
  );
  assert_eq!(
    ctx.envelope_ok.load(Ordering::Relaxed),
    base_env,
    "入账失败轮严禁计入成功条目"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HGET", b"h:c1", b"n"]),
    b"$2\r\n15\r\n",
    "异步臂失败同样不回滚主存写、恰一次施加"
  );
  drop(c);

  ctx.fail_envelope.store(false, Ordering::Release);
  // 失败轮 obj_save 已把信封写回主存（记录驻留），重试回落同步快臂走增量
  // 条目通道：入账计数须 +1（变更镜像不丢），信封整值通道零新增
  let base_objrmw = ctx.objrmw_ok.load(Ordering::Relaxed);
  let base_env = ctx.envelope_ok.load(Ordering::Relaxed);
  let mut w = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut w, &[b"HINCRBY", b"h:c1", b"n", b"5"]),
    b":20\r\n",
    "解除注错后终值 20 证前臂未借重放双施"
  );
  assert_eq!(
    ctx.objrmw_ok.load(Ordering::Relaxed),
    base_objrmw + 1,
    "解除注错后重试轮增量条目镜像恰一次入账"
  );
  assert_eq!(
    ctx.envelope_ok.load(Ordering::Relaxed),
    base_env,
    "暖态重试回落同步快臂，信封整值通道零新增"
  );
}

/// 案五 分层稳态臂（emit_tiered_mirror）注错：手工升阶 zset 后 ZADD 回
/// 存储错误帧禁计数帧、撤帧零成功负载残留、镜像零入账；树写已生效不回滚
///（恰一次施加）。解除注错后新成员 ZADD 闭环 `:1` 且镜像恰一次入账
#[test]
fn tiered_zadd_mirror_enqueue_failure_rejects_and_retry_emits_once() {
  let (store, _dir, ctx) = open_store("objaof-tiered.db", true);
  let rt = Runtime::new().unwrap();

  // 手工升阶（tiered_promote_contract_gate 同款 entries 形制）
  rt.block_on(store.new_session().unwrap().promote_collection_to_bftree(
    b"t:z1",
    GarnetObjectType::SortedSet,
    vec![(b"m1".to_vec(), encode_member(&1f64.to_be_bytes(), None))],
    i64::MAX,
    false,
  ))
  .expect("手工升阶不得报错");
  quiesce();

  let mut c = consumer_on(&store);
  // 对照臂先行：注错关时 ZADD 走分层稳态臂成功且镜像入账（证注入面真实
  // 命中分层通道，反「一律拒写」虚设）
  let base_tiered = ctx.tiered_ok.load(Ordering::Relaxed);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"t:z1", b"2", b"mx"]),
    b":1\r\n"
  );
  assert_eq!(
    ctx.tiered_ok.load(Ordering::Relaxed),
    base_tiered + 1,
    "对照轮分层镜像恰一次入账"
  );

  ctx.fail_tiered.store(true, Ordering::Release);
  let base_tiered = ctx.tiered_ok.load(Ordering::Relaxed);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"t:z1", b"3", b"my"]),
    slow_storage_frame(),
    "分层臂镜像入账失败必须撤帧落存储错误帧，禁计数帧/半成品负载残留"
  );
  assert_eq!(
    ctx.tiered_ok.load(Ordering::Relaxed),
    base_tiered,
    "入账失败轮严禁计入成功条目（副本据此永缺该镜像即假成功发散面）"
  );
  // 树写已生效不回滚：错误帧外成员分值恰一次可见
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZSCORE", b"t:z1", b"my"]),
    b"$1\r\n3\r\n",
    "分层臂失败臂树写恰一次生效（发散可见，禁回滚伪装零副作用）"
  );
  drop(c);

  ctx.fail_tiered.store(false, Ordering::Release);
  let base_tiered = ctx.tiered_ok.load(Ordering::Relaxed);
  let mut w = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut w, &[b"ZADD", b"t:z1", b"4", b"mz"]),
    b":1\r\n",
    "解除注错后新成员 ZADD 照常闭环"
  );
  assert_eq!(
    ctx.tiered_ok.load(Ordering::Relaxed),
    base_tiered + 1,
    "解除注错后镜像恰一次入账（失败轮零入账 + 重试轮恰一次，无双记）"
  );
  assert_eq!(roundtrip(&rt, &mut w, &[b"ZCARD", b"t:z1"]), b":4\r\n");
}
