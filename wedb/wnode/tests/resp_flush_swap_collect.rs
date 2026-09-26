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
  sync::{Arc, Mutex, OnceLock, atomic::Ordering},
  thread::sleep,
  time::Duration,
};

use compio::{runtime::Runtime, time::sleep as async_sleep};
use wbase::time::now_ticks;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
  },
  servers::consumer_registry::ConsumerRegistry,
};
use wnode_test::{err_frame, pump};
use wresp::cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR;
use wtest_base::{resp_frame_str, test_store_config};

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

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭；
/// 挂常驻 SingleDatabaseManager——FLUSH 族经 manager 一处漏斗产生
/// AOF 广播条目，对标 C# storeWrapper.databaseManager 常驻单例）
fn consumer() -> RespSessionConsumer {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("admin.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  let session = store.new_session().unwrap();
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    device,
    dir.join("checkpoints"),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(dir.join("checkpoints"), db));
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(session).with_database_manager(mgr)),
  )
}

/// 未注入管理面的裸执行域会话（清库唯一漏斗缺位形态，用于断言显式回错）
fn consumer_without_manager() -> RespSessionConsumer {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("admin.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 单命令往返（同步快路径）
fn roundtrip(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// 慢命令往返：同步段消费（挂起不产输出）→ 网络泵 await 慢路径 →
/// 应答经唯一并入单点 [`RespSessionConsumer::resolve_slow_wait_into`]
/// 按流水线顺序写回（此处 block_on 承担网络泵角色）
///
/// HLEN/ZCARD 信封水位越线落物化矫正臂（collection.md §6.3）时同步段
/// `Ok(false)` 挂 SlowWait：帧整段消费而应答延迟产出，无人驱动即空应答
/// （同 23cc330d 口径 slow-wait 唯一通道，禁第二条回写路径）
fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  let Some(slow) = c.take_slow_wait() else {
    return out;
  };
  rt.block_on(async {
    let reply = slow.resolve().await;
    c.resolve_slow_wait_into(&reply, &mut out);
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
    err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR)
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$7\r\nFLUSHDB\r\n$3\r\nfoo\r\n"),
    err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR)
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

/// ns 0 下 FLUSHALL 后的会话状态一致性：
/// 换号/全域清库成功后，会话本地缓存的代数与 active_vdb 同步刷新，
/// 防止缓存陈旧代数与旧虚库号，后续命令立即在新鲜虚库上正常读写
#[test]
fn flushall_ns0_session_active_db_consistency() {
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("flushall_sync.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  let session = store.new_session().unwrap();
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    device,
    dir.join("checkpoints"),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(dir.join("checkpoints"), db));
  let api = Arc::new(StoreGarnetApi::new(session).with_database_manager(mgr));
  let mut c = RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::clone(&api) as GarnetApi,
  );

  // 切至 db 2 并写入数据
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n"),
    b"+OK\r\n"
  );
  set_keys(&mut c, &["k_db2"]);
  assert_eq!(api.session.active_db(), 2);
  let old_gen = api.session.last_generation.load(Ordering::Relaxed);

  // ns 0 执行 FLUSHALL（超管全域清库）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$8\r\nFLUSHALL\r\n"),
    b"+OK\r\n"
  );

  // 验证会话状态同步刷新：
  // 1. 代数已推进到存储全局最新代数，不再是陈旧的 old_gen
  let current_gen = store.vdb.generation.load(Ordering::Relaxed);
  assert!(current_gen > old_gen, "全域清库后全局代数应已递增");
  assert_eq!(
    api.session.last_generation.load(Ordering::Relaxed),
    current_gen,
    "ns 0 FLUSHALL 后会话代数必须同步更新至全局最新代数"
  );

  // 2. active_db 仍保持 2，且 active_vdb 必须已重新映射为清库后新分配的虚库号
  assert_eq!(api.session.active_db(), 2);
  let new_vdb = api.session.active_vdb.load(Ordering::Relaxed);
  let (expected_vns, expected_vdb) = store.vdb.get_virtual_ids(0, 2);
  assert_eq!(api.session.active_vns.load(Ordering::Relaxed), expected_vns);
  assert_eq!(
    new_vdb, expected_vdb,
    "ns 0 FLUSHALL 后会话 active_vdb 必须同步为新鲜虚拟库号"
  );

  // 3. 原键已清空，新写入立即在新鲜库上生效且计数正确
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$5\r\nk_db2\r\n"),
    b"$-1\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":0\r\n"
  );
  set_keys(&mut c, &["k_new"]);
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$5\r\nk_new\r\n"),
    b"$1\r\nv\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":1\r\n"
  );

  // 4. 切回 db 0 后再次 FLUSHALL 的代数与状态同步
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );
  set_keys(&mut c, &["k_db0"]);
  let gen_before = store.vdb.generation.load(Ordering::Relaxed);
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$8\r\nFLUSHALL\r\n"),
    b"+OK\r\n"
  );
  let gen_after = store.vdb.generation.load(Ordering::Relaxed);
  assert!(gen_after > gen_before);
  assert_eq!(
    api.session.last_generation.load(Ordering::Relaxed),
    gen_after,
    "db 0 下 FLUSHALL 后会话代数同样同步至全局最新代数"
  );
  let (vns_0, vdb_0) = store.vdb.get_virtual_ids(0, 0);
  assert_eq!(api.session.active_vns.load(Ordering::Relaxed), vns_0);
  assert_eq!(api.session.active_vdb.load(Ordering::Relaxed), vdb_0);
}

/// 清库唯一漏斗缺位（管理面未注入）：FLUSHDB / FLUSHALL 显式回错，
/// 绝不静默退回 store 直调
///
/// store 直调换号只写 KeyTag::DbMeta 映射记录——wnode AOF 写端口按标签过滤
/// （service.rs:on_aof_store_event 仅镜像 String / Acl 域），该类记录永不进
/// 日志，副本侧因此没有任何换号条目可回放，主从读写域自此次清库起分叉；
/// 故漏斗缺位时宁可拒绝命令，不得执行半截清库
#[test]
fn flush_without_manager_is_rejected_not_silently_flushed() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer_without_manager();
  set_keys(&mut c, &["k"]);

  for (frame, name) in [
    (b"*1\r\n$7\r\nFLUSHDB\r\n".as_slice(), "FLUSHDB"),
    (b"*1\r\n$8\r\nFLUSHALL\r\n".as_slice(), "FLUSHALL"),
  ] {
    assert_eq!(
      slow_roundtrip(&rt, &mut c, frame),
      b"-ERR checkpoint channel not configured\r\n",
      "{name} 缺漏斗必须显式回错"
    );
    // 键仍在：未发生任何旁路清库
    assert_eq!(
      roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
      b"$1\r\nv\r\n",
      "{name} 拒绝后数据域不得被旁路清空"
    );
  }
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
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let mut c = RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
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
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    device,
    cp_dir.clone(),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(cp_dir.clone(), db));
  let api = StoreGarnetApi::new(store.new_session().unwrap()).with_database_manager(mgr);
  let mut c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api));

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
    let token = wcpr::find_latest_checkpoint(&cp_dir)
      .unwrap()
      .expect("SAVE 后必须存在快照");
    let device = Arc::new(SegmentedDevice::single_file(&db_file).unwrap());
    let recovered = Arc::new(WedbStore::recover(&cp_dir, token, device).await.unwrap());
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

/// HCOLLECT `*` 游标分批流式收全库（对标 C# ObjectCollect 的
/// `do { DbScan(..., 100, typeObject); foreach RMW } while (storeCursor != 0)`
/// ——libs/server/Storage/Session/ObjectStore/Common.cs:820-827）：
/// 键数远超单批上限（garnet_api/objects.rs `COLLECT_BATCH_KEYS`）时须逐批
/// 续扫至游标归零，末批键同样被物理出账。旧实现单趟扫描先物化全库键清单
/// 再逐键收集（内存 O(总键数)、扫描与收集串行分相），本测试跨批面即其盲区
#[test]
fn hcollect_star_streams_across_batch_boundary() {
  // 250 > 2 × 批上限 100：一轮收集须跨三批
  const KEYS: usize = 250;
  let rt = Runtime::new().unwrap();
  let mut c = consumer();

  for i in 0..KEYS {
    let key = format!("h{i}");
    assert_eq!(
      roundtrip(
        &mut c,
        &resp_frame_str(&["HSET", &key, "keep", "v", "gone", "v"])
      ),
      b":2\r\n"
    );
    assert_eq!(
      roundtrip(
        &mut c,
        &resp_frame_str(&["HPEXPIRE", &key, "100", "FIELDS", "1", "gone"])
      ),
      b"*1\r\n:1\r\n"
    );
  }
  sleep(Duration::from_millis(200));

  // 前置：HLEN 计数前先走堆序惰性剔除（collection.md §6.3），信封水位越线
  // 即落物化矫正臂（挂 SlowWait，经 slow_roundtrip 泵驱动闭环），到期字段不计入应答计数
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &resp_frame_str(&["HLEN", "h0"])),
    b":1\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &resp_frame_str(&["HLEN", "h249"])),
    b":1\r\n",
    "末批键须真实存在"
  );

  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$8\r\nHCOLLECT\r\n$1\r\n*\r\n"),
    b"+OK\r\n"
  );

  // 每一键（含第 2、3 批）的到期字段均已物理出账并回写固化
  for i in 0..KEYS {
    let key = format!("h{i}");
    assert_eq!(
      slow_roundtrip(&rt, &mut c, &resp_frame_str(&["HLEN", &key])),
      b":1\r\n",
      "{key}（第 {} 批）未被收集轮覆盖",
      i / 100 + 1
    );
  }
}

/// 信封态字段级 TTL 计数精度（collection.md §6.3「计数前先走堆序惰性剔除」）：
/// 信封头 `[4B 计数][8B 到期水位]` 水位内 HLEN/ZCARD 走 O(1) 快道；越线即
/// 落物化矫正臂（purge + mutated_by_ttl 升格写回一次矫正），部分成员过期后
/// 计数回正确存活数、与 HGET/HGETALL 同口径；全成员到期计数归零并触发删空
/// 自愈（键回收，杜绝幽灵空键）
#[test]
fn envelope_field_ttl_length_purges_and_self_heals() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();

  // hash：3 字段挂 1 枚 100ms 到期；zset：2 成员挂 1 枚 100ms 到期
  assert_eq!(
    roundtrip(
      &mut c,
      &resp_frame_str(&["HSET", "fh", "a", "1", "b", "2", "gone", "3"])
    ),
    b":3\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut c,
      &resp_frame_str(&["HPEXPIRE", "fh", "100", "FIELDS", "1", "gone"])
    ),
    b"*1\r\n:1\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut c,
      &resp_frame_str(&["ZADD", "fz", "1", "m1", "2", "gone"])
    ),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut c,
      &resp_frame_str(&["ZPEXPIRE", "fz", "100", "MEMBERS", "1", "gone"])
    ),
    b"*1\r\n:1\r\n"
  );

  // 水位内：计数命令直读头部 O(1)，存活计数含未到期挂期成员
  assert_eq!(
    roundtrip(&mut c, &resp_frame_str(&["HLEN", "fh"])),
    b":3\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, &resp_frame_str(&["ZCARD", "fz"])),
    b":2\r\n"
  );

  sleep(Duration::from_millis(200));

  // 水位越线（部分成员过期）：物化矫正臂（挂 SlowWait，slow_roundtrip 泵
  // 驱动经 resolve_slow_wait_into 唯一通道闭环）purge 后回正确存活计数
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &resp_frame_str(&["HLEN", "fh"])),
    b":2\r\n",
    "HLEN 必须剔除到期字段计数"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &resp_frame_str(&["ZCARD", "fz"])),
    b":1\r\n",
    "ZCARD 必须剔除到期成员计数"
  );
  // 与点查同口径：到期字段视同不存在
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &resp_frame_str(&["HGET", "fh", "gone"])),
    b"$-1\r\n"
  );

  // 余下存活成员全部挂期到期：计数归零并删空自愈
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      &resp_frame_str(&["HPEXPIRE", "fh", "100", "FIELDS", "2", "a", "b"])
    ),
    b"*2\r\n:1\r\n:1\r\n"
  );
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      &resp_frame_str(&["ZPEXPIRE", "fz", "100", "MEMBERS", "1", "m1"])
    ),
    b"*1\r\n:1\r\n"
  );
  sleep(Duration::from_millis(200));

  assert_eq!(
    slow_roundtrip(&rt, &mut c, &resp_frame_str(&["HLEN", "fh"])),
    b":0\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &resp_frame_str(&["ZCARD", "fz"])),
    b":0\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &resp_frame_str(&["EXISTS", "fh"])),
    b":0\r\n",
    "hash 全字段到期 HLEN 触发删空自愈"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &resp_frame_str(&["EXISTS", "fz"])),
    b":0\r\n",
    "zset 全成员到期 ZCARD 触发删空自愈"
  );
}
