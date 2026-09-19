//! 分层态字段级 TTL 端到端回归（review-0918 分层缺口 2）
//!
//! 缺陷：分层树记录原本不带字段过期位——升阶导出只给 (field, value) 裸值，
//! HEXPIRE 族对分层键穿透物化后经 apply_rmw_post_operate 重灌把 expiration
//! 整片丢弃（客户端见 HEXPIRE 成功而 TTL 消失）；两态 HLEN/ZCARD 计数也不
//! 等价（信封态按 C# Count 扣已过期成员，分层态直读裸 size）。
//!
//! 机制：树记录经 `wcol::types::member_ttl` 编码可选 8B 过期刻度（升阶导出
//! `export_entries` 与物化还原 `tiered_materialize_blob` 同步承载）；HEXPIRE/
//! HTTL/HPERSIST 与 ZEXPIRE/ZTTL/ZPERSIST 族树内原地写（不再穿透物化）；
//! `MetaValue.next_expiry` 最早到期水位 + `collect_expired_members` 唯一收集
//! 内核（计数臂校正 / 显式 HCOLLECT·ZCOLLECT / `*` 全库周期收集共用）物理
//! 出账到期成员并回写 meta.size，两态计数 O(1) 等价。
//!
//! 对标 C#：libs/server/Objects/Hash/HashObject.cs:Count/IsExpired/
//! DeleteExpiredItems、SortedSetObject.cs 序列化 ExpirationBitMask、
//! libs/server/StoreWrapper.cs:ObjectCollectTaskAsync 周期收集。
//!
//! 灌树契约：promote helper 的 entries 与 `IGarnetObject::export_entries`
//! 同构（member_ttl 编码形态）。

use std::{mem::take, sync::Arc, thread::sleep, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcol::{
  types::{garnet_object::IGarnetObject, member_ttl::encode_member},
  zset::sorted_set_object::SortedSetObject,
};
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
use wtxn::WatchVersionMap;
use wval::GarnetObjectType;

type TestStore = WedbStore<SegmentedDevice>;

struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  api: GarnetApi,
  _dir: tempfile::TempDir,
}

fn env(tag: &str) -> Env {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::new(WatchVersionMap::new(
      1 << 10
    )))),
    "引擎级写面钩子应首次挂载"
  );
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  Env {
    rt: Runtime::new().unwrap(),
    store,
    api,
    _dir: dir,
  }
}

fn session_with(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

/// 慢路径命令同步求值并回帧字节（与 tiered_watch_fence 同款泵）
fn auto_exec(env: &Env, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  s.output.clear();
  env.api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = env.rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 手工升阶（entries 与 `IGarnetObject::export_entries` 同构：编码形态）
fn promote(env: &Env, key: &[u8], obj_type: GarnetObjectType, entries: Vec<(Vec<u8>, Vec<u8>)>) {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(key, obj_type, entries))
    .unwrap();
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(key))
      .unwrap()
      .is_some(),
    "键应处于 wbftree 分层态"
  );
}

fn is_tiered(env: &Env, key: &[u8]) -> bool {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
}

fn int(v: i64) -> Vec<u8> {
  format!(":{v}\r\n").into_bytes()
}

fn bulk(v: &[u8]) -> Vec<u8> {
  let mut out = format!("${}\r\n", v.len()).into_bytes();
  out.extend_from_slice(v);
  out.extend_from_slice(b"\r\n");
  out
}

fn arr(items: &[&[u8]]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", items.len()).into_bytes();
  for it in items {
    out.extend_from_slice(it);
  }
  out
}

/// 取应答帧最后一个 RESP 整数（HTTL/ZTTL 数组尾项）
fn last_int(frame: &[u8]) -> Option<i64> {
  let text = String::from_utf8_lossy(frame);
  text
    .rsplit(':')
    .next()?
    .trim_end_matches("\r\n")
    .parse()
    .ok()
}

/// 分层 hash 三字段（f1/f2/f3，member_ttl 编码形态）
fn promote_hash3(env: &Env, key: &[u8]) {
  promote(
    env,
    key,
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
    ],
  );
}

/// 分层 zset 双成员（m1=1.0 / m2=2.0）
fn promote_zset2(env: &Env, key: &[u8]) {
  promote(
    env,
    key,
    GarnetObjectType::SortedSet,
    vec![
      (b"m1".to_vec(), encode_member(&1.0f64.to_be_bytes(), None)),
      (b"m2".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
    ],
  );
}

/// 分层 HEXPIRE 原地写：字段挂 TTL → HTTL 读回、HGETALL/HLEN 存活全集口径、
/// HPERSIST 清 TTL；树内写不得降阶键（原实现穿透物化会把键打回信封态并丢 TTL）
#[test]
fn tiered_hash_expire_sets_and_reads_back() {
  let env = env("tiered-ttl-hash-expire.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);

  // HEXPIRE h 600 FIELDS 1 f1 → *1\r\n:1（ExpireUpdated）
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hexpire,
      &[b"h", b"600", b"FIELDS", b"1", b"f1"]
    ),
    arr(&[&int(1)]),
    "分层 HEXPIRE 应树内原地写并回 ExpireUpdated"
  );
  assert!(is_tiered(&env, b"h"), "HEXPIRE 树内原地写不得降阶键");

  // HTTL h FIELDS 1 f1 → 正值（≤600）
  let ttl_frame = auto_exec(
    &env,
    &mut s,
    RespCommand::Httl,
    &[b"h", b"FIELDS", b"1", b"f1"],
  );
  let ttl = last_int(&ttl_frame).expect("HTTL 应答整数项");
  assert!(
    (1..=600).contains(&ttl),
    "HTTL 应读回分层挂载的 TTL：{ttl_frame:?}"
  );

  // HGETALL 存活全集 + HGET 原值 + HLEN 直读
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hgetall, &[b"h"]),
    arr(&[
      &bulk(b"f1"),
      &bulk(b"v1"),
      &bulk(b"f2"),
      &bulk(b"v2"),
      &bulk(b"f3"),
      &bulk(b"v3")
    ]),
    "挂 TTL 的存活成员必须在 HGETALL 全集"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"f1"]),
    bulk(b"v1")
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    int(3),
    "未到期 HLEN 直读 size 恒精确"
  );

  // HPERSIST → 清 TTL 回 :1；再 HTTL → -1
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hpersist,
      &[b"h", b"FIELDS", b"1", b"f1"]
    ),
    arr(&[&int(1)]),
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Httl,
      &[b"h", b"FIELDS", b"1", b"f1"]
    ),
    arr(&[&int(-1)]),
    "HPERSIST 后 HTTL 应为 -1"
  );
}

/// 分层计数抵扣：HEXPIRE 过去时刻物理出账（KeyAlreadyExpired），HLEN/HGET
/// 即刻反映；到期窗口（未来 TTL 越过）经收集内核校正，与信封对照组计数一致
#[test]
fn tiered_expire_count_accounting_matches_envelope() {
  let env = env("tiered-ttl-count.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);

  // 过去时刻：HEXPIRE h 0 → :2（KeyAlreadyExpired）且物理出账
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hexpire,
      &[b"h", b"0", b"FIELDS", b"1", b"f1"]
    ),
    arr(&[&int(2)]),
    "过去时刻应回 KeyAlreadyExpired"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    int(2),
    "物理出账后 HLEN 应扣减"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"f1"]),
    b"$-1\r\n",
    "已出账字段 HGET 应为 null"
  );

  // ---- 信封对照组：同数据同序列两态计数一致 ----
  let mut es = session_with(&env);
  assert_eq!(
    auto_exec(&env, &mut es, RespCommand::Hset, &[b"e", b"f1", b"v1"]),
    int(1)
  );
  assert_eq!(
    auto_exec(&env, &mut es, RespCommand::Hset, &[b"e", b"f2", b"v2"]),
    int(1)
  );
  assert_eq!(
    auto_exec(&env, &mut es, RespCommand::Hset, &[b"e", b"f3", b"v3"]),
    int(1)
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut es,
      RespCommand::Hexpire,
      &[b"e", b"0", b"FIELDS", b"1", b"f1"]
    ),
    arr(&[&int(2)]),
  );
  assert_eq!(
    auto_exec(&env, &mut es, RespCommand::Hlen, &[b"e"]),
    int(2),
    "信封对照组 HLEN 应与分层态同口径"
  );
}

/// 分层到期后 HGET 惰性过滤、HCOLLECT 显式单键收集物理出账（树内执行体）
#[test]
fn tiered_expired_member_hidden_and_collected() {
  let env = env("tiered-ttl-collect.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);

  // f1 挂 1 秒 TTL，越过到期窗口
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hexpire,
      &[b"h", b"1", b"FIELDS", b"1", b"f1"]
    ),
    arr(&[&int(1)])
  );
  sleep(Duration::from_millis(1200));

  // 到期窗口内 HGET 惰性过滤（视同不存在）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"f1"]),
    b"$-1\r\n",
    "到期字段 HGET 应视同不存在"
  );

  // HCOLLECT 显式单键 → 树内收集执行体物理出账（+OK）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hcollect, &[b"h"]),
    b"+OK\r\n",
    "显式 HCOLLECT 应走分层收集执行体闭环"
  );

  // 收集后 HTTL → -2（物理消失，非惰性过滤态）；HLEN 校正为 2
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Httl,
      &[b"h", b"FIELDS", b"1", b"f1"]
    ),
    arr(&[&int(-2)]),
    "收集后到期字段应物理消失（HTTL -2）"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    int(2),
    "收集执行体应回写 meta.size 抵扣"
  );

  // HGETALL 存活全集只剩 f2/f3
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hgetall, &[b"h"]),
    arr(&[&bulk(b"f2"), &bulk(b"v2"), &bulk(b"f3"), &bulk(b"v3")]),
  );
}

/// `HCOLLECT *` 全库收集纳入分层键（周期对象收集任务同执行体）：
/// 分层树内到期成员物理消失
#[test]
fn collect_all_covers_tiered_keys() {
  let env = env("tiered-ttl-collect-all.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);

  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hexpire,
      &[b"h", b"1", b"FIELDS", b"2", b"f1", b"f2"]
    ),
    arr(&[&int(1), &int(1)])
  );
  sleep(Duration::from_millis(1200));

  // `*` 全库收集（hlog 扫 Meta 域分层键清单 → 树内执行体）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hcollect, &[b"*"]),
    b"+OK\r\n",
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    int(1),
    "全库收集后分层键到期成员应物理出账"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hgetall, &[b"h"]),
    arr(&[&bulk(b"f3"), &bulk(b"v3")]),
  );
}

/// 分层 zset：ZEXPIRE 原地挂 TTL / ZTTL 读回 / ZADD 覆盖清 TTL（C#
/// TryRemoveExpiration 口径）/ 过去时刻物理出账 / ZCARD 抵扣
#[test]
fn tiered_zset_expire_family_and_add_clears_ttl() {
  let env = env("tiered-ttl-zset.db");
  promote_zset2(&env, b"z");
  let mut s = session_with(&env);

  // ZEXPIRE z 600 MEMBERS 1 m1 → :1
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zexpire,
      &[b"z", b"600", b"MEMBERS", b"1", b"m1"]
    ),
    arr(&[&int(1)]),
  );
  // ZTTL 正值
  let ttl_frame = auto_exec(
    &env,
    &mut s,
    RespCommand::Zttl,
    &[b"z", b"MEMBERS", b"1", b"m1"],
  );
  let ttl = last_int(&ttl_frame).expect("ZTTL 应答整数项");
  assert!(
    (1..=600).contains(&ttl),
    "ZTTL 应读回分层挂载的 TTL：{ttl_frame:?}"
  );

  // ZADD 覆盖存活挂 TTL 成员 → 清 TTL（ZTTL → -1），分值更新
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zadd, &[b"z", b"9", b"m1"]),
    int(0),
    "覆盖已存在成员 ZADD 回 0"
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zttl,
      &[b"z", b"MEMBERS", b"1", b"m1"]
    ),
    arr(&[&int(-1)]),
    "ZADD 覆盖后应清字段 TTL（C# TryRemoveExpiration）"
  );

  // ZEXPIRE 过去时刻 → :2 物理出账；ZCARD 抵扣
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zexpire,
      &[b"z", b"0", b"MEMBERS", b"1", b"m2"]
    ),
    arr(&[&int(2)]),
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zcard, &[b"z"]),
    int(1),
    "ZCARD 应反映收集内核出账"
  );
}

/// 升阶 / 物化往返 TTL 保真（验收核心）：分层态设 TTL → ZPOPMIN 穿透物化
/// （树记录 TTL 还原进对象过期堆）→ 迟滞死区懒降阶信封写回 → 信封 ZTTL 读回
/// → 再升阶（export_entries 编码信封 TTL 灌树）→ 分层 ZTTL 读回一致
#[test]
fn promote_demote_roundtrip_preserves_field_ttl() {
  let env = env("tiered-ttl-roundtrip.db");
  promote_zset2(&env, b"z");
  let mut s = session_with(&env);

  // 分层态挂 TTL 到大分值成员 m2
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zexpire,
      &[b"z", b"600", b"MEMBERS", b"1", b"m2"]
    ),
    arr(&[&int(1)])
  );

  // ZPOPMIN z 1 → 删最小分值 m1（穿透物化：m2 的 TTL 随树记录还原进对象），
  // 剩 1 成员跌回迟滞死区 → 懒降阶信封写回（应答为成员+分值对，C# 口径）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zpopmin, &[b"z", b"1"]),
    arr(&[&bulk(b"m1"), &bulk(b"1")]),
    "穿透 ZPOPMIN 应走对象层单源删最小分值成员"
  );
  assert!(!is_tiered(&env, b"z"), "迟滞死区内应懒降阶回信封态");

  // 信封态 ZTTL 读回 m2 的 TTL（物化还原未丢）
  let ttl_frame = auto_exec(
    &env,
    &mut s,
    RespCommand::Zttl,
    &[b"z", b"MEMBERS", b"1", b"m2"],
  );
  let ttl = last_int(&ttl_frame).expect("信封态 ZTTL 应答整数项");
  assert!(
    (1..=600).contains(&ttl),
    "降阶信封后 TTL 必须保真：{ttl_frame:?}"
  );

  // 再升阶：信封对象载 TTL 经 IGarnetObject::export_entries 导出（信封过期
  // 结构 → 树记录刻度，验收「导出承载过期位」本体）
  let mut obj = SortedSetObject::new();
  obj.sorted_set_dict.insert(b"m2".to_vec(), 9.0);
  obj.insert_expiration(b"m2".to_vec(), now_ticks() + 600 * TICKS_PER_SECOND);
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(
      b"z",
      GarnetObjectType::SortedSet,
      obj.export_entries(),
    ))
    .unwrap();
  let ttl_frame = auto_exec(
    &env,
    &mut s,
    RespCommand::Zttl,
    &[b"z", b"MEMBERS", b"1", b"m2"],
  );
  assert!(
    (1..=600).contains(&last_int(&ttl_frame).unwrap_or(-999)),
    "再升阶后分层 ZTTL 必须读回保真 TTL：{ttl_frame:?}"
  );
}

/// 信封对照组：同数据 HSET + HEXPIRE（内存对象过期堆）后 HCOLLECT 收集，
/// 收集与删空语义与分层态同口径
#[test]
fn envelope_counterpart_ttl_semantics() {
  let env = env("tiered-ttl-envelope.db");
  let mut s = session_with(&env);

  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &[b"h", b"f1", b"v1"]),
    int(1)
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hexpire,
      &[b"h", b"1", b"FIELDS", b"1", b"f1"]
    ),
    arr(&[&int(1)])
  );
  sleep(Duration::from_millis(1200));

  // 信封态 HCOLLECT 显式键：对象层收集物理剔除 → +OK；唯一成员被剔删空，HLEN 0
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hcollect, &[b"h"]),
    b"+OK\r\n",
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    int(0),
    "信封态收集后唯一成员被剔，键应删空（HLEN 0）"
  );
}
