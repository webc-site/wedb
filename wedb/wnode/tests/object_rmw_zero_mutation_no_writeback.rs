//! 集合对象 RMW 零变更禁写回归（票 wnode-object-rmw-zero-mutation-spurious-writeback）
//!
//! 缺陷形：ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZPOPMIN count=0 / SADD 全重复 /
//! HSETNX 字段已存在等零变更操作，因对象层未回填 result1（或回填了但
//! should_write_back 通配臂漏门控），被误判为已变更 → 全量 to_blob 重序列化 +
//! HLog 追加新信封记录（写放大），且 notify_object_rmw 向 AOF 追加
//! ObjectStoreRMW 增量条目（副本重放与恢复流程全量反序列化-再序列化连锁放大）。
//! C# 无此形：对象常驻 Tsavorite 内存池，InPlaceUpdater 就地执行零变异操作后
//! 状态未变，不触发任何持久化写入
//!（libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:InPlaceUpdater）。
//! 修法：对象层回填移除/新增计数（wcol zset），三族 should_write_back 把
//! ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZPOPMIN / ZPOPMAX / SADD / HSETNX 纳入
//! result1 计数门控（TTL 惰性剔除经 mutated_by_ttl 升格不变）。
//!
//! 判据（全走生产 run_sync_rmw / run_async_rmw，无 mock）：
//! - HLog tail_address 零推进 = 底层存储零写回（伪写回必追加信封新记录）；
//! - StoreEventSink 计 ObjectRmw 事件 = AOF ObjectStoreRMW 队列追加数；
//! - 反空跑对照：真变更（ZREM/SADD 新成员/HSETNX 新字段）必须 tail 前进且
//!   AOF +1，杜绝把修法写成「一律禁写」的虚设实现。

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  thread::sleep,
  time::Duration,
};

use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use wkv::{StoreEvent, StoreEventSink, WedbStore};
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::roundtrip;
use wtest_base::test_store_config;

type TestStore = WedbStore<SegmentedDevice>;

/// AOF ObjectStoreRMW 追加计数器（StoreEvent::ObjectRmw 单点拦查）
fn on_store_event(
  counter: &AtomicU64,
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  if matches!(event, StoreEvent::ObjectRmw(_)) {
    counter.fetch_add(1, Ordering::Relaxed);
  }
  Ok(())
}

/// 存储装配：sink 须在创建任何会话前注入（OnceLock 独占）
fn open_store(tag: &str) -> (Arc<TestStore>, TempDir, Arc<AtomicU64>) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let aof_rmw_count = Arc::new(AtomicU64::new(0));
  assert!(
    store.set_event_sink(StoreEventSink::new(
      Arc::clone(&aof_rmw_count),
      on_store_event
    )),
    "事件分发器注入失败（重复注入或时机过晚）"
  );
  (store, dir, aof_rmw_count)
}

fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// HLog 尾地址（写回判据：伪写回必追加信封新记录使 tail 前进）
fn hlog_tail(store: &Arc<TestStore>) -> u64 {
  store.new_session().unwrap().store.tail_address()
}

/// 落稳 HLog 布局与后台事件分发后取判据基线
fn quiesce() {
  sleep(Duration::from_millis(20));
}

/// 零变更场景集（票面五形）逐一执行并断言：应答精确、HLog tail 零推进、
/// AOF ObjectStoreRMW 零追加
fn assert_zero_mutation(
  rt: &Runtime,
  store: &Arc<TestStore>,
  aof_rmw_count: &Arc<AtomicU64>,
  cases: &[(&[&[u8]], &[u8])],
) {
  let base_tail = hlog_tail(store);
  let base_aof = aof_rmw_count.load(Ordering::Relaxed);
  let mut c = consumer_on(store);
  for (args, want) in cases {
    assert_eq!(
      &roundtrip(rt, &mut c, args),
      want,
      "零变更命令应答走样: {:?}",
      args
    );
  }
  quiesce();
  assert_eq!(
    hlog_tail(store),
    base_tail,
    "零变更操作发生伪写回：HLog 追加了新信封记录（全量重序列化写放大）"
  );
  assert_eq!(
    aof_rmw_count.load(Ordering::Relaxed),
    base_aof,
    "零变更操作向 AOF 追加了 ObjectStoreRMW 增量条目（副本重放/恢复面连锁放大）"
  );
}

/// 暖态（同步臂）五形零变更：zset 范围删/弹 + SADD 重复 + HSETNX 已存在
#[test]
fn zero_mutation_warm_path_never_writes_back() {
  let (store, _dir, aof_rmw_count) = open_store("zero-mut-warm.db");
  let rt = Runtime::new().unwrap();

  // 种子：三族各一暖键（种子写回自身产生基线 AOF 计数，落位后取判据基线）
  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(
      &rt,
      &mut seed,
      &[b"ZADD", b"zm:key", b"1", b"m1", b"2", b"m2", b"3", b"m3"]
    ),
    b":3\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"SADD", b"zm:set", b"a"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"HSET", b"zm:hash", b"f", b"v"]),
    b":1\r\n"
  );
  drop(seed);
  quiesce();

  assert_zero_mutation(
    &rt,
    &store,
    &aof_rmw_count,
    &[
      // ZREMRANGEBYRANK 区间越界零命中
      (&[b"ZREMRANGEBYRANK", b"zm:key", b"100", b"200"], b":0\r\n"),
      // ZREMRANGEBYSCORE 分值区间零命中
      (&[b"ZREMRANGEBYSCORE", b"zm:key", b"100", b"200"], b":0\r\n"),
      // ZPOPMIN count=0 空弹出
      (&[b"ZPOPMIN", b"zm:key", b"0"], b"*0\r\n"),
      // SADD 全重复成员
      (&[b"SADD", b"zm:set", b"a"], b":0\r\n"),
      // HSETNX 字段已存在
      (&[b"HSETNX", b"zm:hash", b"f", b"other"], b":0\r\n"),
    ],
  );

  // 对象内容零损伤：种子态原样可读
  let mut c = consumer_on(&store);
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b"zm:key"]), b":3\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"SCARD", b"zm:set"]), b":1\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", b"zm:hash"]), b":1\r\n");
}

/// 冷态（异步臂）：flush 冷化后零变更命令经 run_async_rmw 同门控禁写
#[test]
fn zero_mutation_cold_path_never_writes_back() {
  let (store, _dir, aof_rmw_count) = open_store("zero-mut-cold.db");
  let rt = Runtime::new().unwrap();

  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(
      &rt,
      &mut seed,
      &[b"ZADD", b"zc:key", b"1", b"m1", b"2", b"m2", b"3", b"m3"]
    ),
    b":3\r\n"
  );
  drop(seed);
  rt.block_on(store.flush_and_evict_all())
    .expect("冷化刷盘不得报错");

  assert_zero_mutation(
    &rt,
    &store,
    &aof_rmw_count,
    &[
      (&[b"ZREMRANGEBYRANK", b"zc:key", b"100", b"200"], b":0\r\n"),
      (&[b"ZREMRANGEBYSCORE", b"zc:key", b"100", b"200"], b":0\r\n"),
      (&[b"ZPOPMIN", b"zc:key", b"0"], b"*0\r\n"),
    ],
  );
  assert_eq!(
    {
      let mut c = consumer_on(&store);
      roundtrip(&rt, &mut c, &[b"ZCARD", b"zc:key"])
    },
    b":3\r\n",
    "冷态零变更操作损伤对象内容"
  );
}

/// 反空跑对照：真变更必须照常写回并广播 AOF（杜绝「一律禁写」虚设实现）
#[test]
fn real_mutation_still_writes_back_and_notifies() {
  let (store, _dir, aof_rmw_count) = open_store("zero-mut-ctl.db");
  let rt = Runtime::new().unwrap();

  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(
      &rt,
      &mut seed,
      &[b"ZADD", b"rc:key", b"1", b"m1", b"2", b"m2", b"3", b"m3"]
    ),
    b":3\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"SADD", b"rc:set", b"a"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"HSET", b"rc:hash", b"f", b"v"]),
    b":1\r\n"
  );
  drop(seed);
  quiesce();

  let base_tail = hlog_tail(&store);
  let base_aof = aof_rmw_count.load(Ordering::Relaxed);
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZREM", b"rc:key", b"m1"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"rc:set", b"b"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSETNX", b"rc:hash", b"f2", b"v2"]),
    b":1\r\n"
  );
  quiesce();

  assert!(
    hlog_tail(&store) > base_tail,
    "真变更被误禁写：HLog tail 未推进"
  );
  assert_eq!(
    aof_rmw_count.load(Ordering::Relaxed),
    base_aof + 3,
    "真变更的 AOF ObjectStoreRMW 广播丢失"
  );

  // 终态语义复核：三族各恰一变更生效
  let mut c = consumer_on(&store);
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b"rc:key"]), b":2\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"SCARD", b"rc:set"]), b":2\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", b"rc:hash"]), b":2\r\n");
}
