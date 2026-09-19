//! 对象命令族冷数据降级语义集成测试（exec_slow 冷键分派验收）
//!
//! 快路径遇磁盘冷键（flush_and_evict 后仅驻磁盘）返回 `Ok(false)`：会话
//! 统一挂起 [`SlowWait`]，本测试直接驱动挂起体闭环（网络泵同款驱动方式），
//! 校验慢路径异步装载后从存储态整体重放 RMW（对标 C# Tsavorite pending 读
//! CompletePending 后重放：HashCommands.cs / SetCommands.cs / ListCommands.cs /
//! SortedSetCommands.cs 与 ArrayCommands.cs:NetworkMGET/NetworkDEL）。
//! 红线断言：写侧降级绝不把降级信号当结果应答，慢路径必须保留历史数据
//! 再叠加本次变更（HINCRBY / SADD / RPUSH / ZADD 增量口径）。

use std::{mem::take, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::command::RespCommand;

/// 小容量单文件存储执行域（hll_cold_degrade.rs 同款配置，附 store 句柄供驱逐）
fn open_env(
  tag: &str,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

/// 挂接分派器的会话（降级链路须经 session.garnet_api 挂起 SlowWait）
fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 同步闭环执行：热键写与全缺失键读（无磁盘候选可装载）均同步应答
fn sync_exec(
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  let out = take(&mut s.output);
  assert!(!out.is_empty(), "无降级形态的 {cmd} 必须同步闭环而非挂起");
  out
}

/// 冷键降级注入执行：快路径必须空应答挂起 SlowWait（绝不同步误答），
/// 驱动挂起体闭环（网络泵同款）后返回慢路径应答字节
fn cold_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  assert!(
    s.output.is_empty(),
    "冷键 {cmd} 必须降级挂起而非同步应答：{:?}",
    String::from_utf8_lossy(&s.output)
  );
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("冷键 {cmd} 降级未挂起 SlowWait"));
  let out = rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 冷区哈希：HSET/HGET/HMGET 异步装载保留历史字段；HINCRBY 从存储态
/// 整体重放 RMW（修复前口径：降级信号当缺失，自 0 起算丢失历史）
#[test]
fn hash_cold_degrade_replays_rmw() {
  let (rt, api, store, _dir) = open_env("obj-cold-hash.db");
  let mut s = session_with(&api);

  // 建键：两个字段 + 一个待增计数基值
  assert_eq!(
    sync_exec(
      &api,
      &mut s,
      RespCommand::Hset,
      &[b"h", b"f1", b"v1", b"f2", b"v2"]
    ),
    b":2\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Hset, &[b"h", b"n", b"10"]),
    b":1\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 冷键 HSET：快路径降级挂起，慢路径装载后仅计新字段（写回后键回热，
  // 后续冷口径断言前须重新驱逐）
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Hset, &[b"h", b"f3", b"v3"]),
    b":1\r\n"
  );
  // 历史字段保留（修复前盲插覆盖即丢 f1/f2）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Hget, &[b"h", b"f1"]),
    b"$2\r\nv1\r\n"
  );
  // HMGET 应答序随查询序（含缺失位）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hmget,
      &[b"h", b"f3", b"f1", b"nope"]
    ),
    b"*3\r\n$2\r\nv3\r\n$2\r\nv1\r\n$-1\r\n"
  );
  // HINCRBY：历史基值 10 之上重放增量（降级误答口径会回 ：5）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Hincrby, &[b"h", b"n", b"5"]),
    b":15\r\n"
  );
}

/// 冷区集合：SADD 装载后仅计新成员，SMISMEMBER / SCARD 反映存储态全集
#[test]
fn set_cold_degrade_preserves_members() {
  let (rt, api, store, _dir) = open_env("obj-cold-set.db");
  let mut s = session_with(&api);

  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Sadd, &[b"s", b"a", b"b", b"c"]),
    b":3\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 冷键 SADD：c 已存在仅 d 新增（降级误答口径会盲建集回 ：2）
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Sadd, &[b"s", b"c", b"d"]),
    b":1\r\n"
  );
  // 成员查询序随查询序，全集保留
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Smismember,
      &[b"s", b"a", b"b", b"c", b"d", b"e"]
    ),
    b"*5\r\n:1\r\n:1\r\n:1\r\n:1\r\n:0\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Scard, &[b"s"]),
    b":4\r\n"
  );
}

/// 冷区列表：RPUSH 装载后尾部追加，LRANGE 保序，LPOP 头出队后剩余保序
#[test]
fn list_cold_degrade_preserves_order() {
  let (rt, api, store, _dir) = open_env("obj-cold-list.db");
  let mut s = session_with(&api);

  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Rpush, &[b"l", b"a", b"b"]),
    b":2\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 冷键 RPUSH：装载 [a,b] 后尾插（降级误答口径会新建 [c] 回 ：1）
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Rpush, &[b"l", b"c"]),
    b":3\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Lrange, &[b"l", b"0", b"-1"]),
    b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
  );
  // 头出队整体重放：弹出 a 后剩余 [b, c]
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l"]),
    b"$1\r\na\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Llen, &[b"l"]),
    b":2\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Lrange, &[b"l", b"0", b"-1"]),
    b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
  );
}

/// 冷区有序集合：ZADD 仅计新增，ZSCORE / ZINCRBY / ZRANGE 反映存储态
#[test]
fn zset_cold_degrade_preserves_scores() {
  let (rt, api, store, _dir) = open_env("obj-cold-zset.db");
  let mut s = session_with(&api);

  assert_eq!(
    sync_exec(
      &api,
      &mut s,
      RespCommand::Zadd,
      &[b"z", b"1", b"one", b"2", b"two"]
    ),
    b":2\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 冷键 ZADD：仅 three 新增（降级误答口径会新建集回 ：3）
  assert_eq!(
    cold_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"z", b"3", b"three"]
    ),
    b":1\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"z", b"one"]),
    b"$1\r\n1\r\n"
  );
  // ZINCRBY：历史分值 1 之上重放增量（降级误答口径自 0 起算回 5）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zincrby,
      &[b"z", b"5", b"one"]
    ),
    b"$1\r\n6\r\n"
  );
  // ZRANGE 按分值升序（one 已被上步增到 6）：two(2) three(3) one(6)
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Zrange, &[b"z", b"0", b"-1"]),
    b"*3\r\n$3\r\ntwo\r\n$5\r\nthree\r\n$3\r\none\r\n"
  );
}

/// 冷区批量：MGET 字符串命中写值、信封与缺失写 nil（写 nil 不降级误答）；
/// DEL 混合键计数，删除后同会话冷读确认回收
#[test]
fn mget_del_cold_degrade_batch() {
  let (rt, api, store, _dir) = open_env("obj-cold-mget-del.db");
  let mut s = session_with(&api);

  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Set, &[b"k1", b"v1"]),
    b"+OK\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Set, &[b"k2", b"v2"]),
    b"+OK\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Hset, &[b"h", b"f", b"v"]),
    b":1\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 冷键 MGET：字符串键装载写值，信封键与缺失键写 nil（对标 C#
  // NetworkMGET 的 pending 读重放，objectEnvelope 不作字符串解引用）
  assert_eq!(
    cold_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Mget,
      &[b"k1", b"k2", b"nope", b"h"]
    ),
    b"*4\r\n$2\r\nv1\r\n$2\r\nv2\r\n$-1\r\n$-1\r\n"
  );
  // 冷键 DEL：混合字符串与信封键逐一异步删除计数
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Del, &[b"k1", b"h", b"nope"]),
    b":2\r\n"
  );
  // 删除后冷读：缺失同步可答（无可装载候选，不再降级）
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Mget, &[b"k1"]),
    b"*1\r\n$-1\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Hget, &[b"h", b"f"]),
    b"$-1\r\n"
  );
}
