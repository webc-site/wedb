//! 分层态字段级 TTL 端到端回归（review-0918 分层缺口 2；本票收口残余墓碑源）
//!
//! 机制：树记录经 `wcol::types::member_ttl` 编码可选 8B 过期刻度（升阶导出
//! `export_entries` 与物化还原 `tiered_materialize_blob` 同步承载）；HEXPIRE/
//! HTTL/HPERSIST 与 ZEXPIRE/ZTTL/ZPERSIST 族**穿透物化降级**（对象层单源求值
//! → `apply_rmw_post_operate` 整值写回，迟滞死区内就地懒降阶、TTL 随信封
//! expiration 结构保真），树内不再有逐成员删除记录；`MetaValue.next_expiry`
//! 最早到期水位 + 到期重灌内核 `expire_sweep_or_rebuild`（计数臂校正 / 显式
//! HCOLLECT·ZCOLLECT / `*` 全库周期收集共用）单趟扫描 + 整值重灌出账到期
//! 成员并回写 meta.size——树内零墓碑，扫描栈深度自变量彻底消失。
//!
//! 对标 C#：libs/server/Objects/Hash/HashObject.cs:Count/IsExpired/
//! DeleteExpiredItems、SortedSetObject.cs 序列化 ExpirationBitMask、
//! libs/server/StoreWrapper.cs:ObjectCollectTaskAsync 周期收集。
//!
//! 灌树契约：promote helper 的 entries 与 `IGarnetObject::export_entries`
//! 同构（member_ttl 编码形态），next_expiry 为灌入批最早到期水位。

use std::{mem::take, str::from_utf8, sync::Arc, thread::sleep, time::Duration};

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
use wresp::{cmd_strings as cs, command::RespCommand};
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

/// 手工升阶（entries 与 `IGarnetObject::export_entries` 同构：编码形态；
/// next_expiry 为灌入批最早到期水位，`i64::MAX` = 无成员挂 TTL）
fn promote(
  env: &Env,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
  next_expiry: i64,
) {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(key, obj_type, entries, next_expiry, false))
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

/// RESP 错误帧（write_error_raw 同形：`-msg\r\n`）
fn err(msg: &str) -> Vec<u8> {
  format!("-{msg}\r\n").into_bytes()
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
    i64::MAX,
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
    i64::MAX,
  );
}

/// 分层键 HEXPIRE 穿透物化降级：字段挂 TTL 后迟滞死区内就地懒降阶信封态，
/// TTL 随信封 expiration 结构保真读回（HTTL/HGETALL/HGET/HLEN 全走两态同口径）；
/// HPERSIST 清 TTL。旧「树内原地写」臂已随残余墓碑源收口删除
#[test]
fn tiered_hash_expire_sets_and_reads_back() {
  let env = env("tiered-ttl-hash-expire.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);

  // HEXPIRE h 600 FIELDS 1 f1 → *1\r\n:1（ExpireUpdated，对象层单源求值）
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hexpire,
      &[b"h", b"600", b"FIELDS", b"1", b"f1"]
    ),
    arr(&[&int(1)]),
    "分层 HEXPIRE 穿透物化应回 ExpireUpdated"
  );
  // 三字段迟滞死区内：穿透物化的写回就地懒降阶（物化求值不丢 TTL 是前置缺陷
  // 已修：tiered_materialize_blob 还原 expiration → 信封写回保真）
  assert!(!is_tiered(&env, b"h"), "迟滞死区内穿透写回应就地懒降阶");

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
    "HTTL 应读回穿透挂载的 TTL（物化降级保真）：{ttl_frame:?}"
  );

  // HGETALL 存活全集（哈希表迭代无序，校验 3 对 6 元素全集） + HGET 原值 + HLEN 直读
  let hgetall = auto_exec(&env, &mut s, RespCommand::Hgetall, &[b"h"]);
  assert!(
    hgetall.starts_with(b"*6\r\n"),
    "HGETALL 应返回 6 元素（3 对），实际 {hgetall:?}"
  );
  for (f, v) in [(b"f1", b"v1"), (b"f2", b"v2"), (b"f3", b"v3")] {
    let f_bulk = bulk(f);
    let v_bulk = bulk(v);
    assert!(
      hgetall
        .windows(f_bulk.len())
        .any(|w| w == f_bulk.as_slice()),
      "HGETALL 应包含字段 {:?}",
      from_utf8(f).unwrap()
    );
    assert!(
      hgetall
        .windows(v_bulk.len())
        .any(|w| w == v_bulk.as_slice()),
      "HGETALL 应包含值 {:?}",
      from_utf8(v).unwrap()
    );
  }
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

/// 分层态到期成员：HGET 惰性过滤（纯读不出账）→ HLEN 计数臂触发到期重灌内核
/// 单趟扫描 + 整值重灌出账（树内零墓碑）；键级 TTL 在重灌迁移中逐 tick 保全
#[test]
fn tiered_expired_member_hidden_and_collected() {
  let env = env("tiered-ttl-collect.db");
  // 手工灌「已到期未出账」态：f1 挂过去刻度、水位随灌入批落盘（复刻自然到期
  // 后、收集前的树态——成员级 TTL 面已穿透物化，客户端命令不再产生此态）
  let stale = now_ticks() - 1;
  promote(
    &env,
    b"h",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", Some(stale))),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
    ],
    stale,
  );
  let mut s = session_with(&env);
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Expire, &[b"h", b"3600"]),
    b":1\r\n"
  );
  let sess = env.store.new_session().unwrap();
  let key_ttl = env.rt.block_on(sess.ttl_of(b"h")).unwrap();

  // 到期成员 HGET 惰性过滤（视同不存在，纯读臂不出账）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"f1"]),
    b"$-1\r\n",
    "到期字段 HGET 应视同不存在"
  );
  assert!(
    is_tiered(&env, b"h"),
    "纯读臂不得触发出账（读路径零写放大）"
  );

  // HLEN 计数臂水位命中 → 到期重灌内核：单趟扫描 + drain + bulk_load 重建
  // （树内零墓碑），size 校正为 2，键保持分层态（重灌不评估降阶）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    int(2),
    "计数臂应触发到期重灌出账并回写 size"
  );
  assert!(is_tiered(&env, b"h"), "整值重灌后键应保持分层态");

  // 出账后 HTTL → -2（物理消失，非惰性过滤态）；HGETALL 存活全集只剩 f2/f3
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Httl,
      &[b"h", b"FIELDS", b"1", b"f1"]
    ),
    arr(&[&int(-2)]),
    "出账后到期字段应物理消失（HTTL -2）"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hgetall, &[b"h"]),
    arr(&[&bulk(b"f2"), &bulk(b"v2"), &bulk(b"f3"), &bulk(b"v3")]),
  );
  // 重灌是键存活的迁移臂（keep_ttl=true）：键级 TTL 逐 tick 原样保留
  let sess = env.store.new_session().unwrap();
  assert_eq!(
    env.rt.block_on(sess.ttl_of(b"h")).unwrap(),
    key_ttl,
    "到期重灌不得触碰键级 TTL 旁路"
  );
}

/// `HCOLLECT *` 全库收集纳入分层键（周期对象收集任务同执行体）：分层树内
/// 到期成员经到期重灌物理出账
#[test]
fn collect_all_covers_tiered_keys() {
  let env = env("tiered-ttl-collect-all.db");
  let stale = now_ticks() - 1;
  promote(
    &env,
    b"h",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", Some(stale))),
      (b"f2".to_vec(), encode_member(b"v2", Some(stale))),
      (b"f3".to_vec(), encode_member(b"v3", None)),
    ],
    stale,
  );
  let mut s = session_with(&env);

  // `*` 全库收集（hlog 扫 Meta 域分层键清单 → 到期重灌执行体）
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
  // 结构 → 树记录刻度，验收「导出承载过期位」本体）；水位随灌入批同帧落盘
  let mut obj = SortedSetObject::new();
  obj.sorted_set_dict.insert(b"m2".to_vec(), 9.0);
  let member_expiry = now_ticks() + 600 * TICKS_PER_SECOND;
  obj.insert_expiration(b"m2".to_vec(), member_expiry);
  let entries = obj.export_entries();
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(
      b"z",
      GarnetObjectType::SortedSet,
      entries,
      member_expiry,
      false,
    ))
    .unwrap();
  // 水位必须随重灌前移（假水位 MAX 会骗过计数校正与周期收集，已到期成员
  // 永不出账）：升阶元记录的 next_expiry 与灌入批最早刻度逐 tick 相等
  let (meta, _) = env
    .rt
    .block_on(sess.load_collection_stub(b"z"))
    .unwrap()
    .expect("再升阶后键应处分层态");
  assert_eq!(
    meta.next_expiry, member_expiry,
    "重灌/升阶水位必须随灌入批落盘（member_expiry 刻度）"
  );
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

/// 折叠臂到期覆写回复计数（agy 轮5 条1）：HSET 覆写「到期在树」字段应回 1
///（C# HashSet 入口先 DeleteExpiredItems 再按缺席计 1，
/// libs/server/Objects/Hash/HashObjectImpl.cs:187/:206，与内存态一致）；
/// 水位越过先经出账内核物理剔除再落折叠，批量 upsert 的树内 Found 前查才与
/// 真缺席同计。旧实现以 tree_put_batch 返回值直作应答 → 覆写到期字段回 0，
/// 分层态与内存态应答分叉
#[test]
fn tiered_hset_overwrite_expired_field_replies_one() {
  let env = env("tiered-ttl-hset-fold.db");
  // 手工灌「已到期未出账」态（复刻自然到期后、收集前的树态，同
  // tiered_expired_member_hidden_and_collected 的灌树口径）
  let stale = now_ticks() - 1;
  promote(
    &env,
    b"h",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", Some(stale))),
      (b"f2".to_vec(), encode_member(b"v2", None)),
    ],
    stale,
  );
  let mut s = session_with(&env);

  // 覆写到期字段：出账前置后树内已无 f1 → 新增计 1
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &[b"h", b"f1", b"v1x"]),
    int(1),
    "覆写到期字段应与内存态同回 1（C# DeleteExpiredItems 后缺席计 1）"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    int(2),
    "出账 f2 存活 + 覆写 f1，size 应为 2"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"f1"]),
    bulk(b"v1x"),
    "覆写值应真实落树"
  );

  // 混合批：到期覆写（再造一个到期在树字段 a）+ 真新增 + 存活覆写，批量口径
  // = 到期覆写计 1 + 真新增计 1，存活覆写计 0 → 回 2
  let stale2 = now_ticks() - 1;
  promote(
    &env,
    b"m",
    GarnetObjectType::Hash,
    vec![
      (b"a".to_vec(), encode_member(b"1", Some(stale2))),
      (b"b".to_vec(), encode_member(b"2", None)),
    ],
    stale2,
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hset,
      &[b"m", b"a", b"9", b"c", b"3", b"b", b"2x"]
    ),
    int(2),
    "混合批应计到期覆写 + 真新增各 1，存活覆写计 0"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    int(2),
    "h 键不因 m 键折叠受扰"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"m"]),
    int(3),
    "m 键 size = b + a(复活) + c"
  );
}

/// 全到期批折叠：唯一字段已到期，出账前置触发删空自愈（键消亡）→ Ok(false)
/// 穿透物化降级段按键不存在新建承接（C# DeleteExpiredItems 清空后 Add），应答
/// 仍回 1 且键存活。旧实现该场景同样回 0；穿透误降存储错误则命令失败
#[test]
fn tiered_hset_all_expired_fold_rebuilds_from_missing() {
  let env = env("tiered-ttl-hset-drain.db");
  let stale = now_ticks() - 1;
  promote(
    &env,
    b"g",
    GarnetObjectType::Hash,
    vec![(b"e1".to_vec(), encode_member(b"v", Some(stale)))],
    stale,
  );
  let mut s = session_with(&env);

  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &[b"g", b"e1", b"vx"]),
    int(1),
    "全到期批覆写应按新建语义回 1"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"g", b"e1"]),
    bulk(b"vx"),
    "新建承接后覆写值应可读"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"g"]),
    int(1),
    "新建承接后计数自洽"
  );
}

/// 增量族三态判据（agy r6-data 条 2）：HINCRBY / HINCRBYFLOAT / ZINCRBY 命中
/// 「到期在树」成员走物理覆盖零计数——成员已在 size 中（记账契约：树内已到期
/// 未删除成员由 size 承载），覆盖后条目数不变，再 +1 即永久虚增且无收敛点
///（覆盖为存活后 sweep 不再将其数入出账，HLEN/ZCARD 恒多 1）。C# 净零由入口
/// DeleteExpiredItems 先摘（size 减）再加（size 加）承担
///（libs/server/Objects/Hash/HashObjectImpl.cs HashIncrement:303 /
/// HashIncrementFloat:378、SortedSetObjectImpl.SortedSetIncrement 同型）。
/// 值语义不变：到期视同不存在、按增量起算；真缺席才 +1（与 HSETNX / ZADD
/// None 分支三态判据同面）
#[test]
fn tiered_incr_expired_in_tree_overwrites_without_size_drift() {
  let env = env("tiered-ttl-incr-expired.db");
  let stale = now_ticks() - 1;
  // hash：f1/f2 到期在树（数值形态存量）、f3 存活（size = 3）
  promote(
    &env,
    b"h",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"9", Some(stale))),
      (b"f2".to_vec(), encode_member(b"1.5", Some(stale))),
      (b"f3".to_vec(), encode_member(b"v", None)),
    ],
    stale,
  );
  let mut s = session_with(&env);

  // HINCRBY 命中到期在树字段：按增量起算回 :5，物理覆盖零计数
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hincrby, &[b"h", b"f1", b"5"]),
    int(5),
    "到期在树字段应视同不存在、按增量起算"
  );
  // HINCRBYFLOAT 同判据：回增量原文 bulk
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrbyfloat,
      &[b"h", b"f2", b"0.5"]
    ),
    bulk(b"0.5"),
    "HINCRBYFLOAT 到期在树应按增量起算"
  );
  // 计数恒精确：三字段全存活，HLEN = 3（旧实现每次虚增 +1 → 5）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    int(3),
    "到期命中物理覆盖零计数，HLEN 不得虚增"
  );
  // 覆盖值真实落树且不带旧 TTL（再读为存活值）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"f1"]),
    bulk(b"5")
  );

  // zset：m1 到期在树（size = 2）→ ZINCRBY 回 5，ZCARD 恒 2
  promote(
    &env,
    b"z",
    GarnetObjectType::SortedSet,
    vec![
      (
        b"m1".to_vec(),
        encode_member(&1.0f64.to_be_bytes(), Some(stale)),
      ),
      (b"m2".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
    ],
    stale,
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zincrby, &[b"z", b"5", b"m1"]),
    bulk(b"5"),
    "ZINCRBY 到期在树应按增量起算"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zcard, &[b"z"]),
    int(2),
    "ZINCRBY 到期命中零计数，ZCARD 不得虚增"
  );

  // 真缺席对照：新字段 / 新成员照常 +1（三态判据两态皆保真）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hincrby, &[b"h", b"new", b"1"]),
    int(1),
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    int(4),
    "真缺席字段应计 +1"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zincrby, &[b"z", b"1", b"m3"]),
    bulk(b"1"),
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zcard, &[b"z"]),
    int(3),
    "真缺席成员应计 +1"
  );
}

/// 增量族入参解析失败错误面（r7-data 条 1）：分层 HINCRBY / HINCRBYFLOAT /
/// ZINCRBY 输入增量非数（含 HINCRBYFLOAT 的 ±inf 词形）必须进树前前置校验回
/// C# 同款值域错误（信封臂同面），不得经慢路径漏斗折叠成
/// "ERR slow path storage error" 与信封对象层分叉。C# 锚点：
/// HashObjectImpl.cs HashIncrement（NumUtils.TryParse 失败 →
/// RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER）、HashIncrementFloat（TryGetDouble
/// 失败 → RESP_ERR_NOT_VALID_FLOAT、IsInfinity → RESP_ERR_GENERIC_NAN_INFINITY）、
/// SortedSetObjectImpl.cs SortedSetIncrement（TryGetDouble canBeInfinite=true
/// 失败 → RESP_ERR_NOT_VALID_FLOAT，±inf 词形合法放行）
#[test]
fn tiered_incr_parse_failures_reply_value_domain_errors() {
  let env = env("tiered-ttl-incr-parse-err.db");
  promote(
    &env,
    b"h",
    GarnetObjectType::Hash,
    vec![(b"f1".to_vec(), encode_member(b"1", None))],
    i64::MAX,
  );
  promote(
    &env,
    b"z",
    GarnetObjectType::SortedSet,
    vec![(b"m1".to_vec(), encode_member(&1.0f64.to_be_bytes(), None))],
    i64::MAX,
  );
  let mut s = session_with(&env);

  // HINCRBY 非整数增量 → 通用整数值域错误（旧实现折成存储错误）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hincrby, &[b"h", b"f1", b"abc"]),
    err(cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER),
    "HINCRBY 非整数增量应回 C# 同款值域错误"
  );
  // HINCRBYFLOAT 两态分明：非浮点 / ±inf 词形
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrbyfloat,
      &[b"h", b"f1", b"abc"]
    ),
    err(cs::RESP_ERR_NOT_VALID_FLOAT),
    "HINCRBYFLOAT 非浮点增量应回 NOT_VALID_FLOAT"
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrbyfloat,
      &[b"h", &b"f1"[..], &b"+inf"[..]]
    ),
    err(cs::RESP_ERR_GENERIC_NAN_INFINITY),
    "HINCRBYFLOAT ±inf 词形应回 NAN_INFINITY（C# IsInfinity 同门）"
  );
  // ZINCRBY 非浮点回 NOT_VALID_FLOAT；±inf 词形合法放行（信封层
  // strict_f64(.., true) 同判据），回 inf 分值且不折成存储错误
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zincrby, &[b"z", b"abc", b"m1"]),
    err(cs::RESP_ERR_NOT_VALID_FLOAT),
    "ZINCRBY 非浮点增量应回 NOT_VALID_FLOAT"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zincrby, &[b"z", b"+inf", b"m1"]),
    bulk(b"inf"),
    "ZINCRBY ±inf 词形应放行回 inf 分值（C# canBeInfinite 同口径）"
  );

  // 错误臂零树内副作用：值与计数恒不变（未走写漏斗，dirty 恒假）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"f1"]),
    bulk(b"1"),
    "解析失败臂不得改写存量值"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    int(1),
    "解析失败臂不得动计数"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zcard, &[b"z"]),
    int(1),
    "ZINCRBY 放行与失败臂均不得虚增计数"
  );
}

/// 分层态 HINCRBY 旧值解析对齐 strict_i64：非整数及前导零等非规范整数回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER
#[test]
fn test_tiered_hash_hincrby_old_value_strict_i64() {
  let env = env("tiered-hincrby-strict-i64.db");
  promote(
    &env,
    b"h_strict",
    GarnetObjectType::Hash,
    vec![
      (b"f_valid".to_vec(), encode_member(b"42", None)),
      (b"f_str".to_vec(), encode_member(b"abc", None)),
      (b"f_lead0".to_vec(), encode_member(b"042", None)),
      (b"f_space".to_vec(), encode_member(b" 42", None)),
      (b"f_float".to_vec(), encode_member(b"42.0", None)),
    ],
    i64::MAX,
  );
  let mut s = session_with(&env);

  // 合法旧值增量成功
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrby,
      &[b"h_strict", b"f_valid", b"8"]
    ),
    int(50),
    "合法旧值累加应成功"
  );

  // 现存旧值为字符串 → HASH_VALUE_IS_NOT_INTEGER
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrby,
      &[b"h_strict", b"f_str", b"1"]
    ),
    err(cs::RESP_ERR_HASH_VALUE_IS_NOT_INTEGER),
    "字符串旧值应回 HASH_VALUE_IS_NOT_INTEGER"
  );

  // 现存旧值带前导零 → strict_i64 拒绝并回 HASH_VALUE_IS_NOT_INTEGER
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrby,
      &[b"h_strict", b"f_lead0", b"1"]
    ),
    err(cs::RESP_ERR_HASH_VALUE_IS_NOT_INTEGER),
    "前导零旧值应回 HASH_VALUE_IS_NOT_INTEGER"
  );

  // 现存旧值带空格 → strict_i64 拒绝并回 HASH_VALUE_IS_NOT_INTEGER
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrby,
      &[b"h_strict", b"f_space", b"1"]
    ),
    err(cs::RESP_ERR_HASH_VALUE_IS_NOT_INTEGER),
    "带空格旧值应回 HASH_VALUE_IS_NOT_INTEGER"
  );

  // 现存旧值为浮点字面量 → strict_i64 拒绝并回 HASH_VALUE_IS_NOT_INTEGER
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrby,
      &[b"h_strict", b"f_float", b"1"]
    ),
    err(cs::RESP_ERR_HASH_VALUE_IS_NOT_INTEGER),
    "浮点数字面量旧值应回 HASH_VALUE_IS_NOT_INTEGER"
  );

  // 解析失败臂不修改原值，不改变 HLEN
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h_strict", b"f_lead0"]),
    bulk(b"042"),
    "失败臂不得修改现存值"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h_strict"]),
    int(5),
    "失败臂不得影响计数"
  );
}

/// 解析 SCAN 族 RESP 二元数组应答 `[next_cursor, [item...]]`
fn parse_scan_reply(frame: &[u8]) -> (String, Vec<Vec<u8>>) {
  let text = from_utf8(frame).expect("SCAN 应答应为合法 UTF-8");
  let mut lines = text.split("\r\n");
  assert_eq!(lines.next(), Some("*2"), "SCAN 应答外层必为 2 元数组");
  let _cur_len = lines.next().expect("游标长度头缺失");
  let next_cur = lines.next().expect("游标文本缺失").to_string();
  let arr_line = lines.next().expect("条目数组头缺失");
  let count: usize = arr_line
    .trim_start_matches('*')
    .parse()
    .expect("条目数组长度解析失败");
  let mut items = Vec::with_capacity(count);
  for _ in 0..count {
    let _item_len = lines.next().expect("条目长度头缺失");
    let item_val = lines.next().expect("条目文本缺失");
    items.push(item_val.as_bytes().to_vec());
  }
  (next_cur, items)
}

/// 分层哈希带字段级 TTL 分页扫描无重复条目回归
#[test]
fn test_tiered_hash_scan_with_ttl_pagination_no_duplicates() {
  let env = env("tiered-hash-scan-ttl-page.db");
  // 构造 6 个字段，其中 f2 和 f5 挂过期 TTL（ticks = 1）
  let expired_ticks = 1_i64;
  promote(
    &env,
    b"h",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", Some(expired_ticks))),
      (b"f3".to_vec(), encode_member(b"v3", None)),
      (b"f4".to_vec(), encode_member(b"v4", None)),
      (b"f5".to_vec(), encode_member(b"v5", Some(expired_ticks))),
      (b"f6".to_vec(), encode_member(b"v6", None)),
    ],
    expired_ticks,
  );
  assert!(is_tiered(&env, b"h"), "键应处于 wbftree 分层态");

  let mut s = session_with(&env);
  let mut cursor = "0".to_string();
  let mut seen_fields = Vec::new();
  let mut page_count = 0;

  loop {
    let out = auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"h", cursor.as_bytes(), b"COUNT", b"2"],
    );
    let (next_cursor, items) = parse_scan_reply(&out);
    page_count += 1;
    // HSCAN 每一项为 field, value 成对产出
    assert_eq!(items.len() % 2, 0, "HSCAN 结果应为 field-value 成对");
    for chunk in items.as_chunks::<2>().0 {
      seen_fields.push(chunk[0].clone());
    }
    cursor = next_cursor;
    if cursor == "0" {
      break;
    }
    assert!(page_count <= 10, "分页游标未能正常收敛归零，陷入死循环");
  }

  assert_eq!(page_count, 2, "两页恰好扫完存活字段");
  // 验证返回的存活字段严格不重复且顺序正确
  assert_eq!(
    seen_fields,
    vec![
      b"f1".to_vec(),
      b"f3".to_vec(),
      b"f4".to_vec(),
      b"f6".to_vec()
    ],
    "分页扫描结果应严格包含全部有效存活字段，无重复且无已过期字段"
  );
}
