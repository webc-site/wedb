//! zset r15 审查票五条发现的修复锚回归（票 zcode-r15-zset）
//!
//! 1. STORE 族同步臂对分层态目标键拒写降级（SyncStoreWindow::begin 单点）：
//!    内存态双源 ZINTERSTORE/ZUNIONSTORE 到分层 dst 回执后立即可见新内容，
//!    Meta 存根 + 旧树由慢路径 store_dest_cold → retire_tiered_dest 单点清退，
//!    无双域残留（修复前同步盲写信封被旧树遮蔽，已 ACK 写入不可见）；
//! 2. ZADD 中段选项词形（ZADD k 1 m NX）双态错误帧逐字节一致
//!    （分层臂惰性判定复用 wcol 单源 sorted_set_add_get_options，与内存态、
//!    C# SortedSetAdd 三方同帧 NOT_VALID_FLOAT，修复前分层回 syntax error）；
//! 3. 分层态 ZINCRBY 不唤醒阻塞观察者（slow.rs notify 收敛仅 Zadd，与内存态
//!    快路径、慢路径 rmw_spec 臂、C# SortedSetIncrement 无经纪通知四方同口径）；
//! 4. ZDIFFSTORE 单键形态成员级 TTL 不随行（diff_sets 单键 entries 重建，
//!    对标 C# CopyDiff 纯字典产物，修复前 clone 连带 ExpiryLedger 使 dst 成员
//!    随源过期消失）；
//! 5. ZINTER 聚合累加序与 C# 同序（keys[0] 种子按索引序累计，三键
//!    {1e308, 1, -1e308} SUM 得 0；修复前最小集种子在最小集非 keys[0] 时
//!    浮点非结合性分叉得 1）。

use std::{sync::Arc, thread, time::Duration};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use tempfile::tempdir;
use wbase::time::now_ticks;
use wcol::{
  SortedSetObject,
  itembroker::{collection_item_broker::CollectionItemBroker, item_broker_face::SharedItemBroker},
  object_payload::{GarnetObjectPayload, obj_decode},
  types::member_ttl::encode_member,
};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    objects::collection_item_source::CollectionItemSource,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;
use wtest_base::{resp_frame_str, test_store_config};
use wval::{GarnetObjectType, KeyTag};

type TestStore = WedbStore<SegmentedDevice>;

fn open_env(tag: &str) -> (Runtime, GarnetApi, Arc<TestStore>, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 手工升阶（与生产 export_entries 同形 entries：成员 + encode_member 记录）
fn promote_zset(rt: &Runtime, store: &Arc<TestStore>, key: &[u8], members: &[(&[u8], f64)]) {
  let ents: Vec<(Vec<u8>, Vec<u8>)> = members
    .iter()
    .map(|(m, s)| (m.to_vec(), encode_member(&s.to_be_bytes(), None)))
    .collect();
  let sess = store.new_session().unwrap();
  rt.block_on(sess.promote_collection_to_bftree(
    key,
    GarnetObjectType::SortedSet,
    ents,
    i64::MAX,
    // 一次性首升阶（键尚无旧树）：replace=false 保留 IndexExists 去重门
    false,
  ))
  .unwrap();
}

/// 物理域取证：指定 tag 的物理记录是否在场（双域残留 / 清退闭环判据）
fn record_present(rt: &Runtime, store: &Arc<TestStore>, tag: KeyTag, key: &[u8]) -> bool {
  let sess = store.new_session().unwrap();
  let rec_k = sess.session_tag_key(tag, key);
  rt.block_on(sess.read_raw(&rec_k)).unwrap().is_some()
}

/// ---- 发现一：内存态双源 STORE 到分层 dst，回执即可见 + 单域闭环 ----

#[test]
fn tiered_dst_combine_store_visible_and_single_domain() {
  let (rt, api, store, _dir) = open_env("r15-store-tiered-dst.db");
  let mut s = session_with(&api);

  // 内存态双源
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"a", b"1", b"x", b"2", b"y"]
    ),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"b", b"2", b"y", b"3", b"z"]
    ),
    b":2\r\n"
  );
  // dst 预置旧成员后升阶（修复前旧树遮蔽新信封，ZRANGE 永回 [old]）
  promote_zset(&rt, &store, b"dst", &[(b"old" as &[u8], 9.0)]);
  promote_zset(&rt, &store, b"dst2", &[(b"old" as &[u8], 9.0)]);

  // ZINTERSTORE：交集 {y: 2+2=4}，同步臂拒写 → 慢路径闭环回 :1
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zinterstore,
      &[b"dst", b"2", b"a", b"b"]
    ),
    b":1\r\n"
  );
  // 回执后立即可见新内容（不被旧树遮蔽）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrange,
      &[b"dst", b"0", b"-1", b"WITHSCORES"]
    ),
    b"*2\r\n$1\r\ny\r\n$1\r\n4\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[b"dst"]),
    b":1\r\n"
  );
  // 单域闭环：Meta 存根 + 旧树清退，信封接管
  assert!(
    !record_present(&rt, &store, KeyTag::Meta, b"dst"),
    "ZINTERSTORE 后分层存根残留：retire_tiered_dest 未闭环"
  );
  assert!(
    record_present(&rt, &store, KeyTag::ObjectEnvelope, b"dst"),
    "ZINTERSTORE 后信封记录缺席：慢路径写回未落库"
  );

  // ZUNIONSTORE 同形（并集 {x:1, y:4, z:3}）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zunionstore,
      &[b"dst2", b"2", b"a", b"b"]
    ),
    b":3\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[b"dst2"]),
    b":3\r\n"
  );
  assert!(
    !record_present(&rt, &store, KeyTag::Meta, b"dst2"),
    "ZUNIONSTORE 后分层存根残留"
  );
}

/// ---- 发现二：ZADD 中段选项词形双态错误帧逐字节一致 ----

#[test]
fn zadd_mid_option_token_parity() {
  let (rt, api, store, _dir) = open_env("r15-zadd-mid-opt.db");
  let mut s = session_with(&api);

  // 双态基线：内存态 km 与分层 zk 各持成员 b1
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[b"km", b"1", b"b1"]),
    b":1\r\n"
  );
  promote_zset(&rt, &store, b"zk", &[(b"b1" as &[u8], 1.0)]);

  // 中段选项词形：首 token 即合法分值，选项段已解析后再遇非分值 token
  // → 三方（内存态 / 分层态 / C#）同帧 NOT_VALID_FLOAT
  for key in [b"km" as &[u8], b"zk"] {
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Zadd,
        &[key, b"1", b"m", b"NX"]
      ),
      b"-ERR value is not a valid float\r\n",
      "{} 中段 NX 帧分叉",
      String::from_utf8_lossy(key)
    );
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Zadd,
        &[key, b"1", b"m", b"GT", b"2", b"n"]
      ),
      b"-ERR value is not a valid float\r\n",
      "{} 中段 GT 帧分叉",
      String::from_utf8_lossy(key)
    );
    // 案一数据侧残余锁（deviations §142）：中段错臂出帧前序合法对 (1, m)
    // 已就地改本地 obj（C# 形即部分提交留痕），rust 由回写门 `-` 首字节
    // 整体丢弃——新成员 m 必不在场、既有成员 b1 contents 零变化（双态同锚）
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[key, b"m"]),
      b"$-1\r\n",
      "{} 中段错出帧后前序对 (1, m) 残留落库",
      String::from_utf8_lossy(key)
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[key]),
      b":1\r\n",
      "{} 中段错出帧后既有键 contents 变化",
      String::from_utf8_lossy(key)
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[key, b"b1"]),
      b"$1\r\n1\r\n",
      "{} 中段错出帧后既有成员 b1 分值变化",
      String::from_utf8_lossy(key)
    );
  }

  // 前缀选项正路径与互斥校验不回退（选项段判定单源双态同帧；「选项后空段」
  // 被 RESP 层 ZADD arity 门拦下，两实现同表）
  for key in [b"km" as &[u8], b"zk"] {
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Zadd,
        &[key, b"NX", b"99", b"b1"]
      ),
      b":0\r\n"
    );
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Zadd,
        &[key, b"NX", b"XX", b"1", b"x"]
      ),
      b"-ERR XX and NX options at the same time are not compatible\r\n"
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[key, b"XX"]),
      b"-ERR wrong number of arguments for 'ZADD' command\r\n"
    );
  }

  // ---- 案一第 3 点数据侧锁（deviations §142）：中段非浮点错三臂全退 ----

  // 缺键形：前序对 (1, m) 已落进本地新建 obj，`-` 门整臂拒写＋缺键防幻键
  // 第二门（承 §19 追加澄记／§47a 反幻键裁决）→ 恒零建键
  for key in [b"kx" as &[u8], b"kxt"] {
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Zadd,
        &[key, b"1", b"m", b"GT", b"2", b"n"]
      ),
      b"-ERR value is not a valid float\r\n",
      "缺键中段 GT 形帧分叉"
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[key]),
      b":0\r\n",
      "{} 缺键错误臂残键（幻键）",
      String::from_utf8_lossy(key)
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[key]),
      b":0\r\n",
      "{} 缺键错误臂后 ZCARD 非恒零",
      String::from_utf8_lossy(key)
    );
    assert!(
      !record_present(&rt, &store, KeyTag::ObjectEnvelope, key),
      "{} 错误臂信封落盘（幻键）",
      String::from_utf8_lossy(key)
    );
    assert!(
      !record_present(&rt, &store, KeyTag::Meta, key),
      "{} 错误臂分层存根落盘（幻存根）",
      String::from_utf8_lossy(key)
    );
  }

  // 既有键形：先 (base, 5) 建基线，再中段错（前序对 (1, a) 已就地改 obj）→
  // 出帧后 ZCARD 回 :1、ZSCORE base 仍 5、a 不在场（内存键 / 分层键同锚）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"kb", b"5", b"base"]
    ),
    b":1\r\n"
  );
  promote_zset(&rt, &store, b"kbt", &[(b"base" as &[u8], 5.0)]);
  for key in [b"kb" as &[u8], b"kbt"] {
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Zadd,
        &[key, b"1", b"a", b"GT", b"2", b"b"]
      ),
      b"-ERR value is not a valid float\r\n",
      "{} 既有键中段 GT 形帧分叉",
      String::from_utf8_lossy(key)
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[key]),
      b":1\r\n",
      "{} 错误臂前序对残留扩量",
      String::from_utf8_lossy(key)
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[key, b"a"]),
      b"$-1\r\n",
      "{} 错误臂前序对 (1, a) 残留",
      String::from_utf8_lossy(key)
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[key, b"base"]),
      b"$1\r\n5\r\n",
      "{} 错误臂反噬既有分值",
      String::from_utf8_lossy(key)
    );
  }

  // 重装载复验（错误帧不落盘持久化锁）：刷盘冷化后重载，终态与出帧前一致、
  // 幻键不在盘上——错误臂零信封覆写零增量条目，磁盘侧无新可装载物
  rt.block_on(store.flush_and_evict_all())
    .expect("刷盘冷化不得报存储错误");
  let mut s2 = session_with(&api);
  assert_eq!(
    auto_exec(&api, &rt, &mut s2, RespCommand::Exists, &[b"kx"]),
    b":0\r\n",
    "重装载后错误臂幻键出现在盘上"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s2, RespCommand::Zcard, &[b"kb"]),
    b":1\r\n",
    "重装载后内存键基线走样"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s2, RespCommand::Zscore, &[b"kb", b"base"]),
    b"$1\r\n5\r\n",
    "重装载后内存键基线分值走样"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s2, RespCommand::Zscore, &[b"kb", b"a"]),
    b"$-1\r\n",
    "重装载后错误臂前序对复活"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s2, RespCommand::Zcard, &[b"kbt"]),
    b":1\r\n",
    "重装载后分层键基线走样"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s2, RespCommand::Zscore, &[b"kbt", b"base"]),
    b"$1\r\n5\r\n",
    "重装载后分层键基线分值走样"
  );
}

/// ---- 案二 b 支锁（deviations §142）：ZADD 错臂上成员级 TTL 剔除不固化 ----
///
/// zset 回写门 `-` 支短路使 mutated_by_ttl 升格支对错误臂不可达（与 hash 面
/// 已修豁免支的不对称系主理席刻意裁决：zset 装载即裁、无 hash 面 HEXPIRE
/// 统计旁路差，裁无需固化）。锁现态：错误帧逐字节不变；错臂前后信封物理载荷
/// 逐字节等形（到期成员 a 仍携于盘上、剔除不落盘；解码面装载即裁不入对照，
/// §142 裁决理据即此）、访问即裁且可见结果不变。分层态零影响（树内到期
/// 视同缺席，不受本门辖）。禁按 hash 面豁免形回改 zset 门（§142）。
#[test]
fn zadd_ttl_purge_not_solidified_memory_arm() {
  let (rt, api, store, _dir) = open_env("r141c-ttl-mem.db");
  let mut s = session_with(&api);
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"k", b"1", b"a", b"2", b"b"]
    ),
    b":2\r\n"
  );
  // 成员级 TTL：a 挂 1s 即刻到期，b 存活
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zexpire,
      &[b"k", b"1", b"MEMBERS", b"1", b"a"]
    ),
    b"*1\r\n:1\r\n"
  );

  // 基线快照——到期前刷盘冷化直读信封物理记录：成员级 TTL 随 ZEXPIRE 回写
  // 落盘，此刻 a 未到期，载荷必携成员 a 与 b（后续等形对照的 carriers 基线）
  rt.block_on(store.flush_and_evict_all())
    .expect("基线刷盘冷化不得报存储错误");
  let sess = store.new_session().unwrap();
  let rec_k = sess.session_tag_key(KeyTag::ObjectEnvelope, b"k");
  let raw_base = rt
    .block_on(sess.read_raw(&rec_k))
    .expect("基线信封读取不得报存储错误")
    .expect("基线信封不得消亡");
  let payload_base =
    obj_decode(&raw_base, GarnetObjectType::SortedSet).expect("基线信封标签须为 SortedSet");
  let obj_base = SortedSetObject::from_blob(payload_base).expect("基线信封载荷解码不得失败");
  assert!(obj_base.sorted_set_dict.contains_key(b"a".as_slice()));
  assert!(obj_base.sorted_set_dict.contains_key(b"b".as_slice()));

  thread::sleep(Duration::from_millis(1200));

  // sorted_set_add 入口 delete_expired_items 先行物理剔除，随后选项互斥错臂
  // 出帧——应答逐字节不变（锁应答面）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"k", b"XX", b"NX", b"1", b"m"]
    ),
    b"-ERR XX and NX options at the same time are not compatible\r\n",
    "案二：错臂携剔除时错误帧不得走样"
  );

  // 锁现态——刻意不固化（见 deviations §142）：错臂出帧后再次刷盘冷化直读
  // 信封物理记录，载荷逐字节仍等于基线快照（`-` 门整臂拒写，剔除不落盘，
  // 到期成员 a 物理仍携于盘上）。注：信封解码 deserialize_from_slice 本身
  // 即「装载即裁」（§142 裁决理据），解码后 dict 必剔到期成员，故固化面只
  // 能以物理字节等形锁，不得按解码后成员在册断言
  rt.block_on(store.flush_and_evict_all())
    .expect("刷盘冷化不得报存储错误");
  let sess = store.new_session().unwrap();
  let rec_k = sess.session_tag_key(KeyTag::ObjectEnvelope, b"k");
  let raw = rt
    .block_on(sess.read_raw(&rec_k))
    .expect("信封读取不得报存储错误")
    .expect("错误臂信封不得消亡");
  let payload = obj_decode(&raw, GarnetObjectType::SortedSet).expect("信封标签须为 SortedSet");
  assert_eq!(
    payload, payload_base,
    "锁现态：zset 面刻意不固化，错臂后信封物理载荷应逐字节仍携基线成员 a（勿按 hash 面豁免形回改，§142）"
  );

  // 重装载后访问即裁：可见结果与出帧前一致（ZCARD 恒存活数、a 视同缺席）
  let mut s2 = session_with(&api);
  assert_eq!(
    auto_exec(&api, &rt, &mut s2, RespCommand::Zcard, &[b"k"]),
    b":1\r\n",
    "重装载后访问即裁，可见计数应为存活数"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s2, RespCommand::Zscore, &[b"k", b"a"]),
    b"$-1\r\n",
    "重装载后到期成员须不可见"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s2, RespCommand::Zscore, &[b"k", b"b"]),
    b"$1\r\n2\r\n"
  );
}

/// 案二分层态零影响锁（deviations §142）：分层臂不受信封回写门辖，错误帧
/// 逐字节不变、树不损、错误臂不产信封
#[test]
fn zadd_ttl_purge_tiered_arm_zero_impact() {
  let (rt, api, store, _dir) = open_env("r141c-ttl-tiered.db");
  // 手工灌「已到期未出账」树态（成员级 TTL 面已穿透物化，客户端命令不再
  // 产生此态，形制照 tiered_field_ttl.rs）：a 挂过去刻度、b 存活
  let stale = now_ticks() - 1;
  let ents: Vec<(Vec<u8>, Vec<u8>)> = vec![
    (
      b"a".to_vec(),
      encode_member(&1.0f64.to_be_bytes(), Some(stale)),
    ),
    (b"b".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
  ];
  let sess = store.new_session().unwrap();
  rt.block_on(sess.promote_collection_to_bftree(
    b"tz",
    GarnetObjectType::SortedSet,
    ents,
    stale,
    false,
  ))
  .expect("手工升阶不得失败");

  let mut s = session_with(&api);
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"tz", b"XX", b"NX", b"1", b"m"]
    ),
    b"-ERR XX and NX options at the same time are not compatible\r\n",
    "案二：分层态错误帧须逐字节不变"
  );
  // 零影响：错误臂不产信封、树内到期成员视同缺席、存活成员照常可读
  assert!(
    !record_present(&rt, &store, KeyTag::ObjectEnvelope, b"tz"),
    "分层态错误臂不得生成信封"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"tz", b"a"]),
    b"$-1\r\n",
    "分层到期成员应视同缺席"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"tz", b"b"]),
    b"$1\r\n2\r\n",
    "分层存活成员照常可读"
  );
}

/// ---- 发现三：ZINCRBY 不唤醒阻塞观察者（双态同口径）----
/// 经纪装配（共享存储 + 共享经纪，对位 resp_blocking_commands 单机形态）
fn broker_env(
  tag: &str,
) -> (
  Runtime,
  Arc<TestStore>,
  Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(store.new_session().unwrap()),
  ))));
  (Runtime::new().unwrap(), store, broker, dir)
}

fn broker_client(
  store: &Arc<TestStore>,
  broker: &Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  id: u64,
) -> RespSessionConsumer {
  let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut consumer = RespSessionConsumer::new(id, RespServerSessionOptions::default(), api);
  consumer.set_item_broker(broker.clone());
  consumer
}

/// 单客户端驱动面：同步消费 + 阻塞挂起续驱
struct BlockClient {
  consumer: RespSessionConsumer,
}

impl BlockClient {
  fn new(
    store: &Arc<TestStore>,
    broker: &Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
    id: u64,
  ) -> Self {
    Self {
      consumer: broker_client(store, broker, id),
    }
  }

  fn feed(&mut self, frame: &[u8]) -> Vec<u8> {
    let mut scratch = self.consumer.take_recv_scratch();
    scratch.extend_from_slice(frame);
    self.consumer.return_recv_scratch(scratch);
    let mut resp = Vec::new();
    let remaining = self.consumer.try_consume_messages_into(&mut resp);
    assert!(remaining.is_some(), "命令帧应被完整消费");
    resp
  }

  /// 已 feed 挂起的阻塞命令驱动到完成
  async fn resolve_blocked(&mut self) -> Vec<u8> {
    let mut blocked = self
      .consumer
      .take_blocked_wait()
      .expect("应存在挂起的阻塞等待");
    let (cmd, result) = blocked.resolve().await;
    let mut reply = Vec::new();
    self
      .consumer
      .resolve_blocked_wait_into(cmd, result, &mut reply);
    reply
  }

  /// 单命令往返（含慢路径续驱）
  async fn roundtrip(&mut self, frame: &[u8]) -> Vec<u8> {
    let mut resp = self.feed(frame);
    if let Some(slow) = self.consumer.take_slow_wait() {
      resp.extend_from_slice(&slow.resolve().await);
    }
    resp
  }
}

#[test]
fn zincrby_never_wakes_blocked_observer_in_both_domains() {
  let (rt, store, broker, _dir) = broker_env("r15-zincrby-wake.db");
  rt.block_on(async {
    let mut a = BlockClient::new(&store, &broker, 1);
    let mut b = BlockClient::new(&store, &broker, 2);

    // 内存态：BZPOPMIN park 在空键上，ZINCRBY 新建成员不得唤醒（C#
    // SortedSetIncrement 无经纪通知，内存态快路径同口径），超时空回
    a.feed(&resp_frame_str(&["BZPOPMIN", "kz", "1"]));
    spawn(async move {
      sleep(Duration::from_millis(100)).await;
      b.feed(&resp_frame_str(&["ZINCRBY", "kz", "5", "n"]));
    })
    .detach();
    let reply = a.resolve_blocked().await;
    assert_eq!(reply, b"$-1\r\n", "内存态 ZINCRBY 唤醒了 BZPOPMIN 观察者");

    // 分层态：空分层键 BZPOPMIN 同样挂起（双态阻塞形态一致），分层 ZINCRBY
    // 新建成员不得唤醒（slow.rs notify 收敛仅 Zadd）——超时空回即未被唤醒
    let mut c = BlockClient::new(&store, &broker, 3);
    let mut g = BlockClient::new(&store, &broker, 4);
    let mut f = BlockClient::new(&store, &broker, 5);
    let d = store.new_session().unwrap();
    rt.block_on(d.promote_collection_to_bftree(
      b"zk2",
      GarnetObjectType::SortedSet,
      vec![],
      i64::MAX,
      false,
    ))
    .unwrap();
    c.feed(&resp_frame_str(&["BZPOPMIN", "zk2", "1"]));
    spawn(async move {
      sleep(Duration::from_millis(100)).await;
      g.feed(&resp_frame_str(&["ZINCRBY", "zk2", "5", "n"]));
    })
    .detach();
    let reply = c.resolve_blocked().await;
    assert_eq!(reply, b"$-1\r\n", "分层 ZINCRBY 唤醒了 BZPOPMIN 观察者");

    // 分层 ZINCRBY 落树生效：ZSCORE 直读树内新分值（分层阻塞观察者由
    // notify/超时驱动出件，属经纪既有驱动面，非本票范围，不在此锚）
    let reply = f.roundtrip(&resp_frame_str(&["ZSCORE", "zk2", "n"])).await;
    assert_eq!(reply, b"$1\r\n5\r\n", "分层 ZINCRBY 落树未生效");
  });
}

/// ---- 发现四：ZDIFFSTORE 单键形态成员级 TTL 不随行 ----

#[test]
fn zdiffstore_single_key_member_ttl_not_carried() {
  let (rt, api, _store, _dir) = open_env("r15-diffstore-ttl.db");
  let mut s = session_with(&api);

  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[b"k", b"1", b"m"]),
    b":1\r\n"
  );
  // 挂成员级 TTL（150ms，越期判定走存活视图 purge）；Z EXPIRE 族带 MEMBERS
  // 回每键状态数组帧（Garnet SortedSetExpire 口径）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zpexpire,
      &[b"k", b"150", b"MEMBERS", b"1", b"m"]
    ),
    b"*1\r\n:1\r\n"
  );
  // 单键 ZDIFFSTORE（C# CopyDiff(first, null) 纯字典产物，TTL 不随行）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zdiffstore,
      &[b"dst", b"1", b"k"]
    ),
    b":1\r\n"
  );

  thread::sleep(Duration::from_millis(400));

  // 源成员已越期；dst 成员不受源 TTL 传染仍回分值（修复前随 ledger 消失回 nil）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"k", b"m"]),
    b"$-1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"dst", b"m"]),
    b"$1\r\n1\r\n",
    "ZDIFFSTORE 单键落键成员被源成员级 TTL 传染过期"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[b"dst"]),
    b":1\r\n"
  );
}

/// ---- 发现五：ZINTER 聚合累加序与 C# 同序（keys[0] 种子）----

#[test]
fn zinter_aggregation_order_matches_csharp() {
  let (rt, api, store, _dir) = open_env("r15-zinter-order.db");
  let mut s = session_with(&api);

  // 三键单成员 {1e308, 1, -1e308}：基数全等时最小集取最后一个（k2 种子），
  // 修复前 SUM 累加序重排致浮点非结合分叉得 1；C# keys[0] 种子同序得 0
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"k0", b"1e308", b"v"]
    ),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[b"k1", b"1", b"v"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"k2", b"-1e308", b"v"]
    ),
    b":1\r\n"
  );

  // 三键 SUM：1e308 + 1 → 1e308（吸收），再 + (-1e308) → 0
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zinter,
      &[b"3", b"k0", b"k1", b"k2", b"WITHSCORES"]
    ),
    b"*2\r\n$1\r\nv\r\n$1\r\n0\r\n"
  );

  // 2 键与 1 键回归：SUM 位模式与源键分值一致（1e308 吸收 1），分值文本与
  // ZSCORE 同一格式化单点，逐字节对照免硬编码大数词形
  let k0_text = auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"k0", b"v"]);
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zinter,
      &[b"2", b"k0", b"k1", b"WITHSCORES"]
    ),
    [b"*2\r\n$1\r\nv\r\n".as_slice(), k0_text.as_slice()].concat()
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zinter,
      &[b"1", b"k0", b"WITHSCORES"]
    ),
    [b"*2\r\n$1\r\nv\r\n".as_slice(), k0_text.as_slice()].concat()
  );

  // 分层面：升阶 dst 承接 ZINTERSTORE（STORE 覆写族，发现一单点同享），
  // 落库分值与内存态应答同文本
  promote_zset(&rt, &store, b"dst5", &[(b"old" as &[u8], 9.0)]);
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zinterstore,
      &[b"dst5", b"3", b"k0", b"k1", b"k2"]
    ),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"dst5", b"v"]),
    b"$1\r\n0\r\n",
    "分层落库聚合分值与 C# 同序应得 0"
  );
}
