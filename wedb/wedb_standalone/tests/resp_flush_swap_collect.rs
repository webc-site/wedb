//! 端到端集成测试：FLUSHDB / FLUSHALL / SWAPDB / HCOLLECT `*` / ZCOLLECT `*`
//! 慢路径真执行
//!
//! 对标 garnet/test/standalone/Garnet.test.scripting/MultiDatabaseTests.cs 的
//! MultiDatabaseSwapDatabasesTestLC（跨库交换后字符串与集合对象互换可读）
//! 与 MultiDatabaseMultiSessionSwapDatabasesErrorTestLC（活跃会话 > 1 时
//! SWAPDB 回错拒绝）；FLUSHDB 族对标 FlushTests。命令同步段仅校验参数，
//! 清库 / 跨库交换 / 全库对象收集经 [`wnode::resp::slow_path::SlowWait`]
//! 挂起后异步闭环
use std::{
  sync::{Arc, Mutex, OnceLock, atomic::AtomicI64},
  time::Duration,
};

use compio::{runtime::Runtime, time::sleep as async_sleep};
use wbase::time::now_ticks;
use wdev::SegmentedDevice;
use wedb_test::test_store_config;
use wkv::{CheckpointManager, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{CheckpointCtx, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
  },
  servers::consumer_registry::ConsumerRegistry,
};

/// SWAPDB 族测试串行锁：会话门控测试须在进程级注册表上登记 2 个活跃条目，
/// 与其他 SWAPDB 测试共享此锁避免并行误触发门控
static SWAP_TESTS_LOCK: Mutex<()> = Mutex::new(());

/// 取（或首次安装）进程级活跃会话注册表
fn global_registry() -> Arc<ConsumerRegistry> {
  static ENSURE: OnceLock<()> = OnceLock::new();
  let _ = ENSURE.get_or_init(|| {
    Arc::new(ConsumerRegistry::new()).install_global();
  });
  ConsumerRegistry::global().expect("注册表必须已安装")
}

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭）
fn consumer() -> RespSessionConsumer {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("admin.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions {
      allow_multi_db: true,
      ..RespServerSessionOptions::default()
    },
    Arc::new(StoreGarnetApi::new(session)),
  )
}

/// 单命令往返（同步快路径）
fn roundtrip(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = c.try_consume_messages(frame);
  assert_eq!(consumed, frame.len(), "帧应被完整消费: {frame:?}");
  out
}

/// 慢命令往返：同步段消费（挂起不产输出）→ 网络泵 await 慢路径 →
/// 应答按流水线顺序写回（此处 block_on 承担网络泵角色）
fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = c.try_consume_messages(frame);
  assert_eq!(consumed, frame.len(), "帧应被完整消费: {frame:?}");
  let Some(slow) = c.take_slow_wait() else {
    return out;
  };
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  out
}

/// SET 若干键
fn set_keys(c: &mut RespSessionConsumer, keys: &[&str]) {
  for k in keys {
    assert_eq!(
      roundtrip(
        c,
        format!("*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n", k.len()).as_bytes()
      ),
      b"+OK\r\n"
    );
  }
}

/// FLUSHDB / FLUSHALL：清库闭环 + 语法校验
#[test]
fn flushdb_and_flushall_clear_database() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  set_keys(&mut c, &["a", "b"]);
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nHSET\r\n$2\r\nh1\r\n$1\r\nf\r\n$1\r\nv\r\n"
    ),
    b":1\r\n"
  );

  // 语法错误同步段快答（ASYNC 与 SYNC 互斥 / 未知 token）
  assert_eq!(
    roundtrip(
      &mut c,
      b"*3\r\n$7\r\nFLUSHDB\r\n$5\r\nASYNC\r\n$4\r\nSYNC\r\n"
    ),
    b"-ERR syntax error\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$7\r\nFLUSHDB\r\n$3\r\nfoo\r\n"),
    b"-ERR syntax error\r\n"
  );

  // FLUSHDB SYNC → +OK，字符串与集合键全清（Meta 键走完整删除）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$7\r\nFLUSHDB\r\n$4\r\nSYNC\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":0\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n"),
    b"$-1\r\n"
  );

  // 重建数据后 FLUSHALL（单库模型同径）→ +OK 清库
  set_keys(&mut c, &["x"]);
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$8\r\nFLUSHALL\r\n$5\r\nASYNC\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":0\r\n"
  );

  // UNSAFETRUNCATELOG 破坏性截断路径走通（清库 + 物理段截断）
  set_keys(&mut c, &["t"]);
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      b"*2\r\n$7\r\nFLUSHDB\r\n$17\r\nUNSAFETRUNCATELOG\r\n"
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":0\r\n"
  );
}

/// SWAPDB：跨库键值交换（对标 MultiDatabaseSwapDatabasesTestLC）
///
/// 字符串与 Hash/Set/ZSet/List 对象键互换可读；TTL 严格跟随交换后 DB；
/// 对象键不再被删除（回归旧实现对象信封不搬移 + reset_database 误删缺陷）
#[test]
fn swapdb_exchanges_databases() {
  let _guard = SWAP_TESTS_LOCK.lock().unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  // db0：字符串 + TTL、Hash / ZSet / List 对象键
  set_keys(&mut c, &["k1"]);
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$6\r\nPSETEX\r\n$2\r\nk1\r\n$6\r\n100000\r\n$2\r\nv1\r\n"
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      b"*4\r\n$4\r\nHSET\r\n$2\r\nh1\r\n$1\r\nf\r\n$1\r\nv\r\n"
    ),
    b":1\r\n"
  );
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      b"*4\r\n$4\r\nZADD\r\n$2\r\nz1\r\n$1\r\n1\r\n$1\r\nm\r\n"
    ),
    b":1\r\n"
  );
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      b"*3\r\n$5\r\nLPUSH\r\n$2\r\nl1\r\n$2\r\ne1\r\n"
    ),
    b":1\r\n"
  );

  // db1：字符串 + Set 对象键
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  set_keys(&mut c, &["k2"]);
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      b"*3\r\n$4\r\nSADD\r\n$2\r\ns2\r\n$4\r\nval2\r\n"
    ),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );

  // 同库交换短路 +OK（无搬移语义）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );

  // SWAPDB 0 1（慢路径真交换）→ +OK
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );

  // db1 现持有原 db0 数据（值、TTL 与全部对象键均随迁）
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n"),
    b"$2\r\nv1\r\n"
  );
  let ttl = slow_roundtrip(&rt, &mut c, b"*2\r\n$3\r\nTTL\r\n$2\r\nk1\r\n");
  let ttl_str = String::from_utf8_lossy(&ttl);
  assert!(
    ttl_str.starts_with(':') && !ttl_str.starts_with(":-"),
    "TTL 应为正剩余毫秒: {ttl_str}"
  );
  // 对象键互换可读（回归旧 bug：交换中对象信封被物理删除）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*3\r\n$4\r\nHGET\r\n$2\r\nh1\r\n$1\r\nf\r\n"),
    b"$1\r\nv\r\n",
    "Hash 对象须随库交换且内容完整"
  );
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      b"*3\r\n$6\r\nZSCORE\r\n$2\r\nz1\r\n$1\r\nm\r\n"
    ),
    b"$1\r\n1\r\n",
    "ZSet 对象须随库交换且分值完整"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$4\r\nLPOP\r\n$2\r\nl1\r\n"),
    b"$2\r\ne1\r\n",
    "List 对象须随库交换且元素完整"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\nk2\r\n"),
    b"$-1\r\n",
    "原 db1 键应已搬离"
  );

  // db0 现持有原 db1 数据（Set 对象 + 字符串）
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\nk2\r\n"),
    b"$1\r\nv\r\n"
  );
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      b"*3\r\n$9\r\nSISMEMBER\r\n$2\r\ns2\r\n$4\r\nval2\r\n"
    ),
    b":1\r\n",
    "Set 对象须随库交换且成员完整"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n"),
    b"$-1\r\n",
    "原 db0 键应已搬离"
  );
}

/// SWAPDB：带 TTL 键在交换后过期，双方正确消失
#[test]
fn swapdb_ttl_expires_after_swap() {
  let _guard = SWAP_TESTS_LOCK.lock().unwrap();
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("swap_ttl.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持 TTL 惰性过期语义
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let mut c = RespSessionConsumer::new(
    1,
    RespServerSessionOptions {
      allow_multi_db: true,
      ..RespServerSessionOptions::default()
    },
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  // db0 键带 150ms 短 TTL
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$6\r\nPSETEX\r\n$4\r\ndie0\r\n$3\r\n150\r\n$1\r\nv\r\n"
    ),
    b"+OK\r\n"
  );
  // db1 键无 TTL，交换后落在 db0 且不过期
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  set_keys(&mut c, &["live1"]);
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );

  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );

  // 交换后 TTL 跟随（db1 的 die0 有未到期 TTL 记录，绝对 ticks 原样迁移）
  rt.block_on(async {
    let probe = store.new_session().unwrap();
    probe.set_context(0, 1);
    let ttl = probe.ttl_of(b"die0").await.unwrap();
    assert!(
      ttl.is_some_and(|t| t > now_ticks()),
      "交换后 TTL 须未到期跟随: {ttl:?}"
    );
  });

  // 等待短 TTL 到期（TTL 语义跨库不变；compio 异步 sleep 与运行时共同时钟）
  rt.block_on(async {
    async_sleep(Duration::from_millis(250)).await;
  });
  // db1（现持有 die0）：过期后正确消失
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$3\r\nGET\r\n$4\r\ndie0\r\n"),
    b"$-1\r\n",
    "带 TTL 键交换后过期须消失"
  );
  // db0（现持有 live1）：无 TTL 键不受影响
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$5\r\nlive1\r\n"),
    b"$1\r\nv\r\n"
  );
}

/// SWAPDB 活跃会话门控：注册表活跃会话数 > 1 时回错拒绝，
/// 回落到单会话后恢复可用（对标 MultiDatabaseMultiSessionSwapDatabasesErrorTestLC）
#[test]
fn swapdb_rejected_with_multiple_active_sessions() {
  let _guard = SWAP_TESTS_LOCK.lock().unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  set_keys(&mut c, &["gk"]);

  let registry = global_registry();
  let e1 = registry.register(90001, "t1".into(), "t1".into());
  let e2 = registry.register(90002, "t2".into(), "t2".into());

  // 两活跃会话：SWAPDB 回 C# 同文案错误，数据不动
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n1\r\n"),
    b"-ERR SWAPDB is currently unsupported when multiple clients are connected.\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\ngk\r\n"),
    b"$1\r\nv\r\n",
    "被拒绝的交换不得搬移数据"
  );

  // 注销至单会话：SWAPDB 恢复可用
  registry.unregister(90002);
  drop(e2);
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\ngk\r\n"),
    b"$-1\r\n",
    "交换后 db0 应搬离 gk"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\ngk\r\n"),
    b"$1\r\nv\r\n",
    "交换后 db1 应持有 gk"
  );

  registry.unregister(90001);
  drop(e1);
}

/// SWAPDB + SAVE → 重启 recover：交换结果持久（T6 场景）
///
/// 交换产生的全部物理写经写端口进日志，SAVE 快照固化交换后视图；
/// 全新引擎从快照恢复后 db0/db1 数据仍为交换后布局
#[test]
fn swapdb_survives_save_and_recover() {
  let _guard = SWAP_TESTS_LOCK.lock().unwrap();
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap().keep();
  let cp_dir = dir.join("Store").join("checkpoints");
  let db_file = dir.join("swap_recover.db");
  let device = Arc::new(SegmentedDevice::single_file(&db_file).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let api = StoreGarnetApi::new(store.new_session().unwrap()).with_checkpoint_ctx(CheckpointCtx {
    dir: cp_dir.clone(),
    last_save_ms: Arc::new(AtomicI64::new(0)),
    aof: None,
  });
  let mut c = RespSessionConsumer::new(
    1,
    RespServerSessionOptions {
      allow_multi_db: true,
      ..RespServerSessionOptions::default()
    },
    Arc::new(api),
  );

  // db0：字符串 + Hash 对象；db1：字符串
  set_keys(&mut c, &["k0"]);
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      b"*4\r\n$4\r\nHSET\r\n$2\r\nh0\r\n$1\r\nf\r\n$1\r\nv\r\n"
    ),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  set_keys(&mut c, &["k1"]);
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );

  // 交换 → 落盘
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$4\r\nSAVE\r\n"),
    b"+OK\r\n"
  );
  drop(c);
  drop(store);

  // 重启恢复：快照中交换后视图 1:1 重现
  rt.block_on(async {
    let token = CheckpointManager::<SegmentedDevice>::find_latest_checkpoint(&cp_dir)
      .unwrap()
      .expect("SAVE 后必须存在快照");
    let device = Arc::new(SegmentedDevice::single_file(&db_file).unwrap());
    let recovered = Arc::new(
      CheckpointManager::recover(&cp_dir, token, device)
        .await
        .unwrap(),
    );
    let session = recovered.new_session().unwrap();

    session.set_context(0, 0);
    assert_eq!(session.read(b"k0").await.unwrap(), None, "db0 应已搬离 k0");
    assert_eq!(session.read(b"k1").await.unwrap(), Some(b"v".to_vec()));
    session.set_context(0, 1);
    assert_eq!(session.read(b"k1").await.unwrap(), None, "db1 应已搬离 k1");
    assert_eq!(session.read(b"k0").await.unwrap(), Some(b"v".to_vec()));
  });
}

/// HCOLLECT `*` / ZCOLLECT `*`：全库对象收集闭环
#[test]
fn hcollect_and_zcollect_star_full_db() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  // 两个 hash 键 + 一个 zset 键 + 干扰字符串键
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nHSET\r\n$2\r\nha\r\n$1\r\nf\r\n$1\r\nv\r\n"
    ),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nHSET\r\n$2\r\nhb\r\n$1\r\ng\r\n$1\r\nw\r\n"
    ),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nZADD\r\n$2\r\nza\r\n$1\r\n1\r\n$1\r\nm\r\n"
    ),
    b":1\r\n"
  );
  set_keys(&mut c, &["plain"]);

  // HCOLLECT * → +OK（收集后对象仍完整可读）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$8\r\nHCOLLECT\r\n$1\r\n*\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$4\r\nHGET\r\n$2\r\nha\r\n$1\r\nf\r\n"),
    b"$1\r\nv\r\n"
  );

  // ZCOLLECT * → +OK
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$8\r\nZCOLLECT\r\n$1\r\n*\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$6\r\nZSCORE\r\n$2\r\nza\r\n$1\r\nm\r\n"),
    b"$1\r\n1\r\n"
  );

  // 收集后清库态可再收集（幂等）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$8\r\nHCOLLECT\r\n$1\r\n*\r\n"),
    b"+OK\r\n"
  );
}
