//! 分层 ZCARD / HLEN 计数读锁化回归（M14）
//!
//! 缺陷形：计数臂按命令码静态归写面（zset_needs_write 含 Zcard、
//! hash_needs_write 含 Hlen），稳态水位内（`now <= next_expiry`，树内零到期
//! 成员、零树访问）的只读计数也取独占写锁——高并发读计数与写臂互斥串行。
//!
//! 修复语义（两段式，见 tiered_collection_ops::common::tiered_count）：计数臂
//! 先取共享读锁，锁内重读元记录（弃锁外装载快照——快照水位可能被 HEXPIRE
//! 重灌收紧、size 可能被并发写推高，直读即偏）；水位内 O(1) 直读 size 应答；
//! 水位越过才放读锁升级独占写锁走 expire_sweep_or_rebuild 物理出账——首个
//! 计数命令出账一次、每到期纪元至多一扫、树内零墓碑的裁决语义原样保留
//!（doc/zh/collection.md 大键 O(1) 计数规约第 3 条分层态补则）。
//!
//! 断言口径：
//! - 稳态：并发写者 ACK 后任意 HLEN/ZCARD 应答不得读到旧 size（线性化下界，
//!   锁内重读是正确性前提）；并发计数期间 WATCH 版本零推进；
//! - 水位跨越：并发计数全部应答精确存活数（快路径不满足直读条件即必出账，
//!   无「含到期未出账成员」的偏大应答面）；出账物理发生（HTTL/ZTTL -2）；
//!   出账后 WATCH 恰推进一次，后续计数回归稳态零推进。

use std::{
  mem::take,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use aok::{OK, Void};
use compio::runtime::spawn;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcol::types::member_ttl::encode_member;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::storage_session::version_map_watch_hook,
};
use wresp::command::RespCommand;
use wtxn::{TxnKeyEntryComparison, WatchVersionMap};
use wval::{GarnetObjectType, SessionPrefixBuf};

type TestStore = WedbStore<SegmentedDevice>;

/// 测试环境：真实引擎 + 共享版本表 + 引擎级写面钩子（与生产装配同径）
struct Env {
  store: Arc<TestStore>,
  map: Arc<WatchVersionMap>,
  api: GarnetApi,
  _dir: tempfile::TempDir,
}

fn env(tag: &str) -> Env {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&map))),
    "引擎级写面钩子应首次挂载"
  );
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  Env {
    store,
    map,
    api,
    _dir: dir,
  }
}

fn api_of(store: &Arc<TestStore>) -> aok::Result<GarnetApi> {
  Ok(Arc::new(StoreGarnetApi::new(store.new_session()?)))
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 异步域命令泵（慢路径 resolve 直接 await，不嵌套 block_on——并发任务依赖
/// 同一 worker 轮转调度）
async fn exec_cmd(
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = slow.resolve().await;
  s.output.clear();
  out
}

/// 手工升阶（entries 为 member_ttl 编码形态；next_expiry 为灌入批水位）
async fn promote(
  store: &Arc<TestStore>,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
  next_expiry: i64,
) -> Void {
  let sess = store.new_session()?;
  sess
    .promote_collection_to_bftree(key, obj_type, entries, next_expiry, false)
    .await?;
  assert!(
    sess.load_collection_stub(key).await?.is_some(),
    "键应处于 wbftree 分层态"
  );
  OK
}

/// `:N\r\n` 整数帧解析
fn parse_int(frame: &[u8]) -> i64 {
  assert!(frame.first() == Some(&b':'), "应答应为整数帧: {frame:?}");
  from_utf8(&frame[1..frame.len() - 2])
    .unwrap()
    .parse()
    .unwrap_or_else(|e| panic!("整数帧解析失败 {e}: {frame:?}"))
}

/// 版本表读点（与 wtxn 校验同一哈希面：根域 scoped，与默认写会话前缀同源）
fn ver(map: &WatchVersionMap, key: &[u8]) -> u64 {
  map.read_version(
    TxnKeyEntryComparison::scoped_key_hash(SessionPrefixBuf::ROOT.as_slice(), key) as u64,
  )
}

/// 并发写者：逐条 HSET 新字段，整数应答（ACK）后 `acked` 递增——ACK 即
/// `save_tiered_meta` 已落盘，后续任意计数应答必须包含它
async fn hset_writer(
  store: Arc<TestStore>,
  key: &'static [u8],
  rounds: u32,
  acked: Arc<AtomicU64>,
) -> Void {
  let api = api_of(&store)?;
  let mut s = session_with(&api);
  for i in 0..rounds {
    let field = format!("w{i:05}");
    let owned = [
      key.to_vec(),
      field.into_bytes(),
      format!("v{i}").into_bytes(),
    ];
    let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
    let reply = exec_cmd(&api, &mut s, RespCommand::Hset, &refs).await;
    assert_eq!(reply.first(), Some(&b':'), "HSET 应整数应答: {reply:?}");
    acked.fetch_add(1, Ordering::Release);
  }
  OK
}

/// 并发计数读者：每轮先取 ACK 下界再计数，应答不得低于下界（与并发写的
/// 线性化契约——ACK 落盘后计数单调不回退）；同时计数无写键
async fn count_reader(
  store: Arc<TestStore>,
  hash_key: &'static [u8],
  zset_key: &'static [u8],
  base_hash: u64,
  rounds: u32,
  acked: Arc<AtomicU64>,
) -> Void {
  let api = api_of(&store)?;
  let mut s = session_with(&api);
  for _ in 0..rounds {
    let lb = acked.load(Ordering::Acquire);
    let frame = exec_cmd(&api, &mut s, RespCommand::Hlen, &[hash_key]).await;
    let n = parse_int(&frame);
    assert!(
      n >= (base_hash + lb) as i64,
      "ACK 写入后 HLEN 不得读旧 size：{n} >= {base_hash} + {lb}"
    );
    let frame = exec_cmd(&api, &mut s, RespCommand::Zcard, &[zset_key]).await;
    let z = parse_int(&frame);
    assert_eq!(z, 2, "zset 无并发写，ZCARD 恒精确");
  }
  OK
}

/// 稳态水位内并发计数：共享读锁直读锁内重读的新鲜 size（ACK 后计数单调
/// 不回退），并发计数期间 WATCH 版本零推进（读臂不得触栅栏）
#[compio::test]
async fn steady_watermark_concurrent_counts_read_fresh_and_keep_watch() -> Void {
  let env = env("tiered-count-steady.db");
  promote(
    &env.store,
    b"h",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
    ],
    i64::MAX,
  )
  .await?;
  promote(
    &env.store,
    b"z",
    GarnetObjectType::SortedSet,
    vec![
      (b"m1".to_vec(), encode_member(&1.0f64.to_be_bytes(), None)),
      (b"m2".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
    ],
    i64::MAX,
  )
  .await?;

  const WRITER_ROUNDS: u32 = 400;
  const READER_ROUNDS: u32 = 300;
  let acked = Arc::new(AtomicU64::new(0));
  let before_h = ver(&env.map, b"h");
  let before_z = ver(&env.map, b"z");

  let writer = spawn(hset_writer(
    Arc::clone(&env.store),
    b"h",
    WRITER_ROUNDS,
    Arc::clone(&acked),
  ));
  let readers = [
    spawn(count_reader(
      Arc::clone(&env.store),
      b"h",
      b"z",
      3,
      READER_ROUNDS,
      Arc::clone(&acked),
    )),
    spawn(count_reader(
      Arc::clone(&env.store),
      b"h",
      b"z",
      3,
      READER_ROUNDS,
      Arc::clone(&acked),
    )),
  ];
  writer.await.expect("并发写任务异常退出")?;
  for r in readers {
    r.await.expect("并发读任务异常退出")?;
  }

  // 并发计数全程零推进：读臂（含稳态计数快路径）不触 WATCH 栅栏；版本增量
  // 全部来自写者 HSET（每条写恰推进一次）
  assert_eq!(ver(&env.map, b"z"), before_z, "并发 ZCARD 不得推进版本");
  assert_eq!(
    ver(&env.map, b"h"),
    before_h + WRITER_ROUNDS as u64,
    "版本增量应恰等于 HSET 条数（计数零贡献）"
  );

  // 终态精确：3 存活种子 + 全部 ACK 写入
  let mut s = session_with(&env.api);
  let frame = exec_cmd(&env.api, &mut s, RespCommand::Hlen, &[b"h"]).await;
  assert_eq!(parse_int(&frame), (3 + WRITER_ROUNDS as u64) as i64);
  let frame = exec_cmd(&env.api, &mut s, RespCommand::Zcard, &[b"z"]).await;
  assert_eq!(parse_int(&frame), 2);
  OK
}

/// 水位越过并发计数：快路径直读条件不满足即必升级出账，全部应答精确存活数
///（无「含到期未出账成员」的偏大面）；出账物理发生（HTTL/ZTTL -2）、
/// WATCH 恰推进一次、出账后回归稳态零推进（每到期纪元至多一扫）
#[compio::test]
async fn crossed_watermark_concurrent_counts_sweep_once_and_reply_exact() -> Void {
  let env = env("tiered-count-sweep.db");
  let past = now_ticks() - TICKS_PER_SECOND;
  // hash：3 存活 + 2 到期种子；zset：2 存活 + 1 到期种子；水位随灌入批落
  // 过去值——首个计数命令必触发出账
  promote(
    &env.store,
    b"h",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
      (b"e1".to_vec(), encode_member(b"x1", Some(past))),
      (b"e2".to_vec(), encode_member(b"x2", Some(past))),
    ],
    past,
  )
  .await?;
  promote(
    &env.store,
    b"z",
    GarnetObjectType::SortedSet,
    vec![
      (b"m1".to_vec(), encode_member(&1.0f64.to_be_bytes(), None)),
      (b"m2".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
      (
        b"me".to_vec(),
        encode_member(&9.0f64.to_be_bytes(), Some(past)),
      ),
    ],
    past,
  )
  .await?;

  let before_h = ver(&env.map, b"h");
  let before_z = ver(&env.map, b"z");

  // 并发计数读者：升级出账（首者）与 Below 双检直读（余者）交错，应答
  // 恒精确存活数——偏大值（含未出账到期成员）只能出自误判水位的直读
  let readers = [0, 1, 2].map(|i| {
    spawn(sweep_count_reader(
      Arc::clone(&env.store),
      b"h",
      b"z",
      30 + i as u32,
    ))
  });
  for r in readers {
    r.await.expect("并发计数任务异常退出")?;
  }

  // 出账物理发生：到期成员 HTTL/ZTTL -2（物理消失，非惰性过滤态 -2）
  let mut s = session_with(&env.api);
  let frame = exec_cmd(
    &env.api,
    &mut s,
    RespCommand::Httl,
    &[b"h", b"FIELDS", b"1", b"e1"],
  )
  .await;
  assert!(
    frame.ends_with(b":-2\r\n"),
    "出账后到期字段应物理消失（HTTL -2）: {frame:?}"
  );
  let frame = exec_cmd(
    &env.api,
    &mut s,
    RespCommand::Zttl,
    &[b"z", b"MEMBERS", b"1", b"me"],
  )
  .await;
  assert!(
    frame.ends_with(b":-2\r\n"),
    "出账后到期成员应物理消失（ZTTL -2）: {frame:?}"
  );

  // WATCH 恰一次推进（唯一真实出账者置脏；快路径与 Below 双检零推进）
  assert_eq!(ver(&env.map, b"h"), before_h + 1, "hash 出账恰推进一次");
  assert_eq!(ver(&env.map, b"z"), before_z + 1, "zset 出账恰推进一次");

  // 出账后回归稳态：大量重复计数零推进（每到期纪元至多一扫）
  for _ in 0..100 {
    let frame = exec_cmd(&env.api, &mut s, RespCommand::Hlen, &[b"h"]).await;
    assert_eq!(parse_int(&frame), 3, "出账后 HLEN 恒精确");
    let frame = exec_cmd(&env.api, &mut s, RespCommand::Zcard, &[b"z"]).await;
    assert_eq!(parse_int(&frame), 2, "出账后 ZCARD 恒精确");
  }
  assert_eq!(
    ver(&env.map, b"h"),
    before_h + 1,
    "稳态计数回归零推进（每纪元一扫）"
  );
  assert_eq!(ver(&env.map, b"z"), before_z + 1);
  OK
}

/// 水位越过场景的并发计数读者：交替 HLEN/ZCARD，成功应答恒为存活数；命中
/// 出账封窗（首者 sweep → promote 重灌）的计数臂按存储忙拒绝（错误帧、零
/// 副作用，与改前写锁臂同形，重试即收敛），跳过后续轮照常精确
async fn sweep_count_reader(
  store: Arc<TestStore>,
  hash_key: &'static [u8],
  zset_key: &'static [u8],
  rounds: u32,
) -> Void {
  let api = api_of(&store)?;
  let mut s = session_with(&api);
  for i in 0..rounds {
    let frame = exec_cmd(&api, &mut s, RespCommand::Hlen, &[hash_key]).await;
    if frame.first() == Some(&b':') {
      assert_eq!(parse_int(&frame), 3, "水位越过计数应答精确存活数");
    }
    let frame = exec_cmd(&api, &mut s, RespCommand::Zcard, &[zset_key]).await;
    if frame.first() == Some(&b':') {
      assert_eq!(parse_int(&frame), 2, "水位越过计数应答精确存活数");
    }
    // 与输出臂交错（读锁共享面）：HGET 流式点读成功时值精确；命中出账封窗
    //（路由探测 load_collection_stub 的 MigrationBusy 拒绝）同写者口径跳过
    if i % 7 == 0 {
      let frame = exec_cmd(&api, &mut s, RespCommand::Hget, &[hash_key, b"f1"]).await;
      if frame.first() == Some(&b'$') {
        assert_eq!(frame, b"$2\r\nv1\r\n", "并发读不受计数臂干扰");
      }
    }
  }
  OK
}
