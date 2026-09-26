//! STORE 覆写 String 域目标键双域清退回归（票 zcode-r32-retirematrix）
//!
//! 缺陷形态：
//! STORE 覆写族（S*STORE / Z*STORE / GEOSEARCHSTORE）原先对 String 域目标键
//! 只清 TTL 不清 String 物理记录，信封写后导致 String + Envelope 双域并存：
//! 1. 读面分叉：GET/STRLEN String 域先行命中回旧字符串，TYPE 回 string（Redis/C# 应 WRONGTYPE/zset/set）；
//! 2. 写面永久毒化：probe_alive_domain 三域序 String 先行回 Some(String)，
//!    后续一切对象 RMW 写臂（SADD/ZADD 等）复验（期望 ObjectEnvelope）恒失败回存储忙，键永久写死。
//!
//! 修复落点：
//! 1. 快路径拒写门：`SyncStoreWindow::begin` 对异构域（!matches!(loaded, None | Some(ObjectEnvelope))）
//!    一律返回 None 转既有 Ok(false) 慢路径；
//! 2. 慢路径收尾清退：`store_dest_cold_common` 在持窗写临界区内，非空 obj_save 前对
//!    开窗存活域为 String 的目标键先执行 `delete_string`，彻底杜绝双域并存。
//!
//! AOF 重放臂护栏（票 wnode-storeoverwrite-aof-replay-regression，retirematrix
//! 测试点三迟到收口）：主臂命令条目镜像链——SET 落 StoreUpsert、SADD 落
//! ObjectStoreRMW、覆写清退 `delete_string` 落 StoreDelete 墓碑、信封接管落
//! ObjectStoreUpsert（notify_envelope_upsert 单点）；重放端（AofProcessor →
//! store_upsert/object_store_rmw/store_delete/object_store_upsert）按同一序
//! 重演即同构清退。若后续重构在重放通道绕过命令层清退与拒写门，双域形态
//! 将在主从一致地复发且不可经重放自愈——重放臂三连问（TYPE/GET/SCARD）
//! 断主从应答字节全等即拦截。

use std::sync::Arc;

use compio::runtime::Runtime;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  AofProcessor, GarnetAppendOnlyFile, GarnetLog, RespSessionConsumer,
  aof::{aof_processor::ReplayTarget, recover::aof_recover::AofRecover},
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  service::NodeService,
  storage::session::storage_session::StorageSession,
};
use wnode_test::roundtrip;
use wtest_base::open_test_store;

type TestStore = WedbStore<SegmentedDevice>;

fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 核心验证：SET dst "old" 后执行 SINTERSTORE dst k1（k1 非空）：
/// - GET dst 回 WRONGTYPE
/// - TYPE dst 回 +set
/// - 后续集合写入（SADD）能成功落库（不出现双域毒化导致的存储忙/永久拒写）
#[test]
fn test_sinterstore_overwrite_string_no_dual_domain() {
  let (_dir, store) = open_test_store("sinterstore-overwrite-str.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  // 1. 初始化目标键为 String，源集合为非空 Set
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"dst", b"old"]),
    b"+OK\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"SADD", b"k1", b"v1"]), b":1\r\n");

  // 2. 执行 SINTERSTORE 覆写目标键
  let store_reply = roundtrip(&rt, &mut c, &[b"SINTERSTORE", b"dst", b"k1"]);
  assert_eq!(store_reply, b":1\r\n", "SINTERSTORE 应答须为基数 :1");

  // 3. 断言读面类型：GET 须报 WRONGTYPE，TYPE 须为 set
  let get_reply = roundtrip(&rt, &mut c, &[b"GET", b"dst"]);
  assert!(
    get_reply.starts_with(b"-WRONGTYPE"),
    "覆写后 GET dst 须为 WRONGTYPE，实测: {:?}",
    String::from_utf8_lossy(&get_reply)
  );

  let type_reply = roundtrip(&rt, &mut c, &[b"TYPE", b"dst"]);
  assert_eq!(type_reply, b"+set\r\n", "覆写后 TYPE dst 须为 set");

  // 4. 断言写面无毒化：后续 SADD 正常落库，不得出现存储忙/永久拒写
  let sadd_reply = roundtrip(&rt, &mut c, &[b"SADD", b"dst", b"v2"]);
  assert_eq!(
    sadd_reply,
    b":1\r\n",
    "覆写后 SADD 须能成功落库，不得因双域并存报存储忙: {:?}",
    String::from_utf8_lossy(&sadd_reply)
  );

  assert_eq!(roundtrip(&rt, &mut c, &[b"SCARD", b"dst"]), b":2\r\n");
}

/// ZSET 覆写验证：SET dst "old" 后执行 ZINTERSTORE dst 1 zk1（zk1 非空）：
/// - GET dst 回 WRONGTYPE
/// - TYPE dst 回 +zset
/// - 后续集合写入（ZADD）能成功落库
#[test]
fn test_zinterstore_overwrite_string_no_dual_domain() {
  let (_dir, store) = open_test_store("zinterstore-overwrite-str.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"zdst", b"old"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"zk1", b"1.0", b"m1"]),
    b":1\r\n"
  );

  let store_reply = roundtrip(&rt, &mut c, &[b"ZINTERSTORE", b"zdst", b"1", b"zk1"]);
  assert_eq!(store_reply, b":1\r\n");

  let get_reply = roundtrip(&rt, &mut c, &[b"GET", b"zdst"]);
  assert!(
    get_reply.starts_with(b"-WRONGTYPE"),
    "覆写后 GET zdst 须为 WRONGTYPE，实测: {:?}",
    String::from_utf8_lossy(&get_reply)
  );

  assert_eq!(roundtrip(&rt, &mut c, &[b"TYPE", b"zdst"]), b"+zset\r\n");

  let zadd_reply = roundtrip(&rt, &mut c, &[b"ZADD", b"zdst", b"2.0", b"m2"]);
  assert_eq!(
    zadd_reply,
    b":1\r\n",
    "覆写后 ZADD 须能成功落库: {:?}",
    String::from_utf8_lossy(&zadd_reply)
  );

  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b"zdst"]), b":2\r\n");
}

/// 空结果回收验证：SET dst "old" 后执行空结果 SINTERSTORE / ZINTERSTORE：
/// - 整键彻底清退（EXISTS 回 0，GET 回 nil，TYPE 回 none）
#[test]
fn test_store_overwrite_string_empty_result_reclaims() {
  let (_dir, store) = open_test_store("store-overwrite-empty.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  // 1. Set 空交集回收
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"dst_s", b"old_set"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SINTERSTORE", b"dst_s", b"nonexistent"]),
    b":0\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"dst_s"]), b":0\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"dst_s"]), b"$-1\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"TYPE", b"dst_s"]), b"+none\r\n");

  // 2. ZSet 空结果回收
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"dst_z", b"old_zset"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"ZINTERSTORE", b"dst_z", b"1", b"nonexistent_z"]
    ),
    b":0\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"dst_z"]), b":0\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"dst_z"]), b"$-1\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"TYPE", b"dst_z"]), b"+none\r\n");
}

/// dst == src 同键场景验证：
/// 1. dst 为已有 Set 且与 src 相同：正常覆写并支持后续写入；
/// 2. dst 为已有 String 且与 src 相同：源装载报 WRONGTYPE，旧 String 保持完好；
/// 3. dst 为已有 Set 且空交集回收：整键删除。
#[test]
fn test_store_dest_equals_src_scenarios() {
  let (_dir, store) = open_test_store("store-dest-equals-src.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  // 1. dst == src 且为 Set（自交集）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"s_same", b"a", b"b"]),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SINTERSTORE", b"s_same", b"s_same"]),
    b":2\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"TYPE", b"s_same"]), b"+set\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"s_same", b"c"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"SCARD", b"s_same"]), b":3\r\n");

  // 2. dst == src 且为 String：源装载报 WRONGTYPE，键保持 String 原样
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"str_same", b"hello"]),
    b"+OK\r\n"
  );
  let wrong_reply = roundtrip(&rt, &mut c, &[b"SINTERSTORE", b"str_same", b"str_same"]);
  assert!(
    wrong_reply.starts_with(b"-WRONGTYPE"),
    "源键为 String 须报 WRONGTYPE: {:?}",
    String::from_utf8_lossy(&wrong_reply)
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GET", b"str_same"]),
    b"$5\r\nhello\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"TYPE", b"str_same"]),
    b"+string\r\n"
  );

  // 3. dst == src 与空结果结合：整键删除
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"s_empty", b"x"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"SINTERSTORE", b"s_empty", b"s_empty", b"nonexistent"]
    ),
    b":0\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"s_empty"]), b":0\r\n");
}

/// 冷数据覆写验证：SET dst "old" 落盘后执行 SINTERSTORE dst k1
#[test]
fn test_sinterstore_overwrite_cold_string_no_dual_domain() {
  let (_dir, store) = open_test_store("sinterstore-overwrite-cold-str.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"cold_dst", b"old_val"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"cold_k1", b"mem1"]),
    b":1\r\n"
  );

  // 刷盘冷化
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let store_reply = roundtrip(&rt, &mut c, &[b"SINTERSTORE", b"cold_dst", b"cold_k1"]);
  assert_eq!(store_reply, b":1\r\n");

  let get_reply = roundtrip(&rt, &mut c, &[b"GET", b"cold_dst"]);
  assert!(
    get_reply.starts_with(b"-WRONGTYPE"),
    "冷化覆写后 GET 须为 WRONGTYPE: {:?}",
    String::from_utf8_lossy(&get_reply)
  );

  assert_eq!(roundtrip(&rt, &mut c, &[b"TYPE", b"cold_dst"]), b"+set\r\n");

  let sadd_reply = roundtrip(&rt, &mut c, &[b"SADD", b"cold_dst", b"mem2"]);
  assert_eq!(
    sadd_reply,
    b":1\r\n",
    "冷化覆写后 SADD 须能成功落库: {:?}",
    String::from_utf8_lossy(&sadd_reply)
  );

  assert_eq!(roundtrip(&rt, &mut c, &[b"SCARD", b"cold_dst"]), b":2\r\n");
}

/// 内存 AOF 装配（aof_store_rmw_replay.rs 同款驱动形态：无盘拓扑，
/// 重放面与磁盘拓扑同路径）
fn memory_aof(tag: &str) -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = wnode_test::test_sublogs(tag, 1);
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ))
}

/// 全量重放 AOF 已提交流进全新 store 会话（aof_store_rmw_replay.rs 同款
/// 重放驱动：AofProcessor + ReplayTarget + AofRecover::single_log_recover），
/// 返回重放条目计数
async fn replay_all_into(store2: &Arc<TestStore>, aof: &Arc<GarnetAppendOnlyFile>) -> u64 {
  let replay_session = store2.new_session().unwrap();
  let batch = replay_session.enter_batch();
  let storage = StorageSession::new(batch);
  let processor = AofProcessor::new(Arc::clone(aof));
  let target = ReplayTarget::new(&storage, store2);
  AofRecover::single_log_recover(&processor, aof, 0, 0, -1, &target)
    .await
    .expect("AOF 全量重放")
}

/// AOF 重放臂闭环（热路径覆写）：主臂 SET dst old 后 SINTERSTORE dst k1
/// （k1 非空），命令条目镜像 AOF；重放进全新 store 会话后对同一键
/// TYPE/GET/SCARD 三连问，主从应答字节须全等——重放通道若绕过命令层
/// 清退（StoreDelete 墓碑缺席）或信封接管错域，String 域旧记录与信封
/// 双域并存将在主从一致地复发（TYPE 回 string / GET 回旧值），本护栏即红
#[test]
fn test_sinterstore_overwrite_string_replay_no_dual_domain() {
  let (_dir, store) = open_test_store("sinterstore-str-replay.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  // 主臂挂 AOF 写监听（NodeService 装配事件镜像端口）
  let aof = memory_aof("sinterstore_str_replay");
  let _service = NodeService::new(Arc::clone(&store), Arc::clone(&aof)).unwrap();

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"dst", b"old"]),
    b"+OK\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"SADD", b"k1", b"v1"]), b":1\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SINTERSTORE", b"dst", b"k1"]),
    b":1\r\n"
  );

  // 主臂三连问应答字节存档（TYPE 回 set / GET 回 WRONGTYPE / SCARD 回基数）
  let p_type = roundtrip(&rt, &mut c, &[b"TYPE", b"dst"]);
  let p_get = roundtrip(&rt, &mut c, &[b"GET", b"dst"]);
  let p_scard = roundtrip(&rt, &mut c, &[b"SCARD", b"dst"]);
  assert_eq!(p_type, b"+set\r\n", "主臂覆写后 TYPE dst 须为 set");
  assert!(
    p_get.starts_with(b"-WRONGTYPE"),
    "主臂覆写后 GET dst 须为 WRONGTYPE: {:?}",
    String::from_utf8_lossy(&p_get)
  );
  assert_eq!(p_scard, b":1\r\n", "主臂覆写后 SCARD dst 须为基数 1");

  // AOF 提交后重放进全新 store 会话
  aof.log().commit();
  let (_dir2, store2) = open_test_store("sinterstore-str-replay-replica.db").unwrap();
  let replayed = rt.block_on(replay_all_into(&store2, &aof));
  assert!(replayed > 0, "重放臂须消费到镜像条目");

  let mut r = consumer_on(&store2);
  assert_eq!(
    roundtrip(&rt, &mut r, &[b"TYPE", b"dst"]),
    p_type,
    "重放后 TYPE dst 主从应答字节须全等"
  );
  assert_eq!(
    roundtrip(&rt, &mut r, &[b"GET", b"dst"]),
    p_get,
    "重放后 GET dst 主从应答字节须全等（双域复发即回旧字符串）"
  );
  assert_eq!(
    roundtrip(&rt, &mut r, &[b"SCARD", b"dst"]),
    p_scard,
    "重放后 SCARD dst 主从应答字节须全等"
  );
}

/// AOF 重放臂闭环（冷路径覆写变体，对齐
/// test_sinterstore_overwrite_cold_string_no_dual_domain 同型）：SET dst 落盘
/// 冷化后 SINTERSTORE 覆写，重放臂三连问主从应答字节须全等——冷化使目的键
/// String 记录走盘态装载，重放通道若绕过清退，盘态旧记录复活即双域复发
#[test]
fn test_sinterstore_overwrite_cold_string_replay_no_dual_domain() {
  let (_dir, store) = open_test_store("sinterstore-cold-str-replay.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  let aof = memory_aof("sinterstore_cold_str_replay");
  let _service = NodeService::new(Arc::clone(&store), Arc::clone(&aof)).unwrap();

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"cold_dst", b"old_val"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"cold_k1", b"mem1"]),
    b":1\r\n"
  );

  // 刷盘冷化后覆写（目的键 String 记录转盘态候选）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SINTERSTORE", b"cold_dst", b"cold_k1"]),
    b":1\r\n"
  );

  let p_type = roundtrip(&rt, &mut c, &[b"TYPE", b"cold_dst"]);
  let p_get = roundtrip(&rt, &mut c, &[b"GET", b"cold_dst"]);
  let p_scard = roundtrip(&rt, &mut c, &[b"SCARD", b"cold_dst"]);
  assert_eq!(p_type, b"+set\r\n", "冷化覆写后 TYPE 须为 set");
  assert!(
    p_get.starts_with(b"-WRONGTYPE"),
    "冷化覆写后 GET 须为 WRONGTYPE: {:?}",
    String::from_utf8_lossy(&p_get)
  );
  assert_eq!(p_scard, b":1\r\n");

  aof.log().commit();
  let (_dir2, store2) = open_test_store("sinterstore-cold-str-replay-replica.db").unwrap();
  let replayed = rt.block_on(replay_all_into(&store2, &aof));
  assert!(replayed > 0, "重放臂须消费到镜像条目");

  let mut r = consumer_on(&store2);
  assert_eq!(
    roundtrip(&rt, &mut r, &[b"TYPE", b"cold_dst"]),
    p_type,
    "冷化重放后 TYPE 主从应答字节须全等"
  );
  assert_eq!(
    roundtrip(&rt, &mut r, &[b"GET", b"cold_dst"]),
    p_get,
    "冷化重放后 GET 主从应答字节须全等（双域复发即回旧字符串）"
  );
  assert_eq!(
    roundtrip(&rt, &mut r, &[b"SCARD", b"cold_dst"]),
    p_scard,
    "冷化重放后 SCARD 主从应答字节须全等"
  );
}
