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

use std::{str::from_utf8, sync::Arc, thread::sleep, time::Duration};

use itoa::Buffer as ItoaBuffer;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcol::{
  types::{garnet_object::IGarnetObject, member_ttl::encode_member},
  zset::sorted_set_object::SortedSetObject,
};
use wnode::resp::resp_server_session::RespServerSession;
use wnode_test::{TestEnv, session_with, tiered_env};
use wresp::{cmd_strings as cs, command::RespCommand};
use wval::GarnetObjectType;

/// 慢路径命令同步求值并回帧字节（与 tiered_watch_fence 同款泵）
fn auto_exec(
  env: &TestEnv,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

/// 手工升阶（entries 与 `IGarnetObject::export_entries` 同构：编码形态；
/// next_expiry 为灌入批最早到期水位，`i64::MAX` = 无成员挂 TTL）
fn promote(
  env: &TestEnv,
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

fn is_tiered(env: &TestEnv, key: &[u8]) -> bool {
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
fn promote_hash3(env: &TestEnv, key: &[u8]) {
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
fn promote_zset2(env: &TestEnv, key: &[u8]) {
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
  let env = tiered_env("tiered-ttl-hash-expire.db");
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
  let env = tiered_env("tiered-ttl-count.db");
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
  let env = tiered_env("tiered-ttl-collect.db");
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
  let env = tiered_env("tiered-ttl-collect-all.db");
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
  let env = tiered_env("tiered-ttl-zset.db");
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
  let env = tiered_env("tiered-ttl-roundtrip.db");
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
  obj.sorted_set_dict.insert(Arc::from(&b"m2"[..]), 9.0);
  let member_expiry = now_ticks() + 600 * TICKS_PER_SECOND;
  obj.insert_expiration(Arc::from(&b"m2"[..]), member_expiry);
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
  let env = tiered_env("tiered-ttl-envelope.db");
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
  let env = tiered_env("tiered-ttl-hset-fold.db");
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
  let env = tiered_env("tiered-ttl-hset-drain.db");
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
  let env = tiered_env("tiered-ttl-incr-expired.db");
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
  let env = tiered_env("tiered-ttl-incr-parse-err.db");
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

/// 分层态 HINCRBY 旧值解析基座对齐：非整数（空串/空白/垃圾/浮点字面量）回
/// RESP_ERR_HASH_VALUE_IS_NOT_INTEGER；前导零/`+` 号等 NumUtils.TryParse 合法文法放行
#[test]
fn test_tiered_hash_hincrby_old_value_tryparse_base() {
  let env = tiered_env("tiered-hincrby-strict-i64.db");
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

  // 现存旧值带前导零 → 对标 C# NumUtils.TryParse 接受前导零，042 + 1 = 43
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrby,
      &[b"h_strict", b"f_lead0", b"1"]
    ),
    int(43),
    "前导零旧值对标 C# NumUtils.TryParse 累加应成功"
  );

  // 现存旧值带空格 → FromStr 整体消费拒绝并回 HASH_VALUE_IS_NOT_INTEGER
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

  // 现存旧值为浮点字面量 → 拒绝并回 HASH_VALUE_IS_NOT_INTEGER
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
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h_strict", b"f_str"]),
    bulk(b"abc"),
    "失败臂不得修改现存值"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h_strict"]),
    int(5),
    "失败臂不得影响计数"
  );
}

/// 案一（zcode-r151c-hincrby）分层态 HINCRBYFLOAT 求和溢出特值词形锁：与信封态
/// resp_hash.rs::hincrbyfloat_sum_overflow_inf_wordform_envelope 同判据点、双臂
/// 逐字节全等（§80 同族第二消费位，deviations §1 尾回指注在册）。树内浮点臂
/// 和逾 DBL_MAX 后经 format_double 单源回树+应答同源锁 3 字节 "inf"，禁按 C#
/// "Infinity" 回改、禁补求和结果门；存量 "inf" 次轮落树内存量无穷门回错不写值、
/// 不动 size、不推进 dirty
#[test]
fn tiered_hincrbyfloat_sum_overflow_inf_wordform() {
  let env = tiered_env("tiered-hincrbyfloat-ovf.db");
  promote(
    &env,
    b"hov",
    GarnetObjectType::Hash,
    vec![(b"f".to_vec(), encode_member(b"1e308", None))],
    i64::MAX,
  );
  assert!(is_tiered(&env, b"hov"), "手工灌树后应处分层态");
  let mut s = session_with(&env);

  // 首轮求和溢出：回帧 $3 "inf"（与信封臂逐字节全等）
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrbyfloat,
      &[b"hov", b"f", b"1e308"]
    ),
    bulk(b"inf"),
    "分层求和溢出锁现树 inf 词形，禁回改 Infinity"
  );
  // 树内读回：落树文本不漂第三形（回树臂不改写词形）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"hov", b"f"]),
    bulk(b"inf")
  );
  // HSTRLEN 锁现树 3 字节形（C# 8 字节仅在册对照，注见信封同名锁测）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hstrlen, &[b"hov", b"f"]),
    int(3)
  );

  // 次轮：存量 "inf" 经 §2 词形放行落树内存量无穷门，错帧不改值
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrbyfloat,
      &[b"hov", b"f", b"1"]
    ),
    err(cs::RESP_ERR_GENERIC_NAN_INFINITY_INCR)
  );
  // 整数族存量门对特值文本同拒
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hincrby, &[b"hov", b"f", b"1"]),
    err(cs::RESP_ERR_HASH_VALUE_IS_NOT_INTEGER)
  );
  // 错臂零副作用：值存续、计数不漂
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"hov", b"f"]),
    bulk(b"inf"),
    "存量门错帧不得覆写 inf 文本"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"hov"]),
    int(1),
    "错帧臂不得动计数"
  );
}

/// 案二（zcode-r151c-hincrby）分层态增量族存活字段 TTL 存续锁：与信封臂
/// hash_ttl.rs::incr_family_preserves_live_field_ttl_envelope 同判据点（deviations
/// §66a 回指注在册家族不对称）。手工灌带活刻度存活字段，树内 HINCRBY/
/// HINCRBYFLOAT 累加以 old_expiry 原值回写（Hincrby 臂「存活成员增量保留既有
/// TTL」文注位），HTTL 穿透读回（形制对齐 promote_demote 件物化降级保真）仍回
/// 正剩余；未挂期字段对照恒 -1（增量不误挂期）；家族不对称对照——HSET 覆写
/// 折叠臂清期回 -1，勿顺手双向统一。只走裸 HEXPIRE 判定面，不触 §66b/hexmatrix
/// 选项矩阵在途面
#[test]
fn tiered_hash_incr_preserves_live_field_ttl() {
  let env = tiered_env("tiered-hincrby-live-ttl.db");
  let future = now_ticks() + 600 * TICKS_PER_SECOND;
  promote(
    &env,
    b"hlt",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"1", Some(future))),
      (b"f2".to_vec(), encode_member(b"1.5", Some(future))),
      (b"f3".to_vec(), encode_member(b"7", None)),
    ],
    future,
  );
  assert!(is_tiered(&env, b"hlt"), "手工灌树后应处分层态");
  let mut s = session_with(&env);

  // 树内累加：整数臂回 :3、浮点臂回 "2.5"（携刻度回写不改词形）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hincrby, &[b"hlt", b"f1", b"2"]),
    int(3)
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hincrbyfloat,
      &[b"hlt", b"f2", b"1"]
    ),
    bulk(b"2.5")
  );

  // 核心锁：HTTL 读回存活字段 TTL 存续（增量臂若被回改成清期即 -1，静默可观测）
  let f1_frame = auto_exec(
    &env,
    &mut s,
    RespCommand::Httl,
    &[b"hlt", b"FIELDS", b"1", b"f1"],
  );
  let t1 = last_int(&f1_frame).expect("HTTL f1 整数项");
  assert!(
    (1..=600).contains(&t1),
    "HINCRBY 存活字段累加不得清期：{f1_frame:?}"
  );
  let f2_frame = auto_exec(
    &env,
    &mut s,
    RespCommand::Httl,
    &[b"hlt", b"FIELDS", b"1", b"f2"],
  );
  let t2 = last_int(&f2_frame).expect("HTTL f2 整数项");
  assert!(
    (1..=600).contains(&t2),
    "HINCRBYFLOAT 存活字段累加不得清期：{f2_frame:?}"
  );
  // 对照：未挂期字段恒 -1（增量族亦不得误挂期）
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Httl,
      &[b"hlt", b"FIELDS", b"1", b"f3"]
    ),
    arr(&[&int(-1)]),
    "未挂期字段累加后仍应 -1"
  );

  // 家族不对称对照：HSET 覆写存活挂期字段 → 清期 -1（折叠臂「覆盖写清字段
  // TTL」在册纪律，与增量存期反向，防按覆写直觉双向回改）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &[b"hlt", b"f1", b"9"]),
    int(0),
    "存活字段 HSET 覆写计 0"
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Httl,
      &[b"hlt", b"FIELDS", b"1", b"f1"]
    ),
    arr(&[&int(-1)]),
    "HSET 覆写清期与增量存期同框对照（§66a 回指注）"
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
  let env = tiered_env("tiered-hash-scan-ttl-page.db");
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

/// 分层 HSCAN 全量分页（NOVALUES / WITHVALUES 双模式共用泵）：逐页记录
/// 应答游标序列与字段投影序列（WITHVALUES 取 field-value 对的 field 臂）
fn scan_all_pages(
  env: &TestEnv,
  s: &mut RespServerSession,
  key: &[u8],
  no_values: bool,
) -> (Vec<String>, Vec<Vec<u8>>) {
  let mut cursor = "0".to_string();
  let mut cursors = Vec::new();
  let mut fields = Vec::new();
  let mut pages = 0;
  loop {
    let mut args: Vec<&[u8]> = vec![key, cursor.as_bytes(), b"COUNT", b"2048"];
    if no_values {
      args.push(b"NOVALUES");
    }
    let out = auto_exec(env, s, RespCommand::Hscan, &args);
    let (next, items) = parse_scan_reply(&out);
    if no_values {
      fields.extend(items);
    } else {
      for pair in items.as_chunks::<2>().0 {
        fields.push(pair[0].clone());
      }
    }
    pages += 1;
    assert!(pages <= 2000, "分页游标未收敛归零，陷入死循环");
    if next == "0" {
      break;
    }
    cursors.push(next.clone());
    cursor = next;
  }
  (cursors, fields)
}

/// 本测试真驱动全链路：HSET 批量越过升阶门限自然升阶 → HPEXPIRE 部分字段
/// 挂 100ms 短 TTL（穿透物化回写保持分层态）→ 到期后断言两模式全量分页的
/// 游标序列逐页全等、字段投影全等，且到期字段在两模式应答中均不可见
#[test]
fn tiered_hscan_novalues_expired_hidden_and_cursor_matches_withvalues() {
  let env = tiered_env("tiered-hscan-novalues-expire.db");
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut s = session_with(&env);

  // 批量 HSET 跨越升阶门限自然升阶（与 collection_adaptive_tiering 同灌法）
  let mut buf = ItoaBuffer::new();
  for chunk_start in (1..=total).step_by(16384) {
    let chunk_end = (chunk_start + 16383).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(b"hbig".to_vec());
    for i in chunk_start..=chunk_end {
      let s = buf.format(i).as_bytes();
      let mut field = Vec::with_capacity(s.len() + 1);
      field.push(b'f');
      field.extend_from_slice(s);
      let mut val = Vec::with_capacity(s.len() + 1);
      val.push(b'v');
      val.extend_from_slice(s);
      args.push(field);
      args.push(val);
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&env, &mut s, RespCommand::Hset, &arg_slices);
  }
  assert!(
    is_tiered(&env, b"hbig"),
    "越升阶门限后键应处于 wbftree 分层态"
  );

  // HEXPIRE 两字段挂 3s 短 TTL：穿透物化求值后字段数仍在升阶门限之上，
  // 写回保持分层态（TTL 随树记录 member_ttl 编码承载；安全余量覆盖物化反序列化耗时）
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hexpire,
      &[b"hbig", b"3", b"FIELDS", b"2", b"f2", b"f7000"]
    ),
    arr(&[&int(1), &int(1)]),
    "分层态 HEXPIRE 应回 ExpireUpdated"
  );
  assert!(is_tiered(&env, b"hbig"), "迟滞死区上方穿透写回应保持分层态");

  // 等待 TTL 到期（纯读 HSCAN 不触发收集出账，到期字段仍在树内——
  // 正是旧实现泄露路径的复现前置）
  sleep(Duration::from_millis(3200));

  let (cursors_nov, fields_nov) = scan_all_pages(&env, &mut s, b"hbig", true);
  let (cursors_wv, fields_wv) = scan_all_pages(&env, &mut s, b"hbig", false);

  // 游标推进逐页全等（到期剔除基数同口径后分页边界不得分叉）
  assert_eq!(
    cursors_nov, cursors_wv,
    "NOVALUES 与 WITHVALUES 分页游标序列应逐页全等"
  );
  // 字段投影全等且顺序一致（同一树迭代序）
  assert_eq!(
    fields_nov, fields_wv,
    "NOVALUES 应答字段序列应与 WITHVALUES 字段投影全等"
  );
  // 到期字段严格不可见（HGET 视同不存在的读径同口径）
  assert!(
    !fields_nov.iter().any(|f| f == b"f2" || f == b"f7000"),
    "已到期字段 f2/f7000 不得出现在 NOVALUES 应答"
  );
  assert_eq!(
    fields_nov.len(),
    total - 2,
    "两模式应答应恰为全部存活字段（到期字段剔除）"
  );
}

/// 残低水位零到期窗全量读面回归（r15-hash 发现二）：Hset 批量臂 / Hsetnx 到期
/// 覆盖写清字段 TTL 但不重算 `next_expiry`（new_fields=0 不落 meta），被覆盖
/// 字段恰为水位承载者时元记录水位残低；越过该刻度后的**首条**
/// HGETALL/HKEYS/HVALS 命中「水位越过 + 零到期」形态——出账内核
/// `expire_sweep_or_rebuild` 零到期臂必须把存活全集交输出面（修复前 Swept
/// 不调 drain_live、三臂仅 Below 分支扫树 → 非空哈希输出空集，数据面假空）。
/// HGET 点查同刻正常作对照（缺陷面仅全量读）。对标 C#
/// HashObjectImpl.HashGetAll/HashGetKeysOrValues 恒输出 Count() 全集
#[test]
fn tiered_stale_watermark_zero_expiry_full_reads_nonempty() {
  let env = tiered_env("tiered-ttl-stale-watermark.db");
  // 双字段挂 TTL（t1 < t2），f1 为水位承载者
  let exp1 = now_ticks() + TICKS_PER_SECOND;
  let exp2 = now_ticks() + 60 * TICKS_PER_SECOND;
  for key in [b"w1".as_slice(), b"w2".as_slice(), b"w3".as_slice()] {
    promote(
      &env,
      key,
      GarnetObjectType::Hash,
      vec![
        (b"f1".to_vec(), encode_member(b"v1", Some(exp1))),
        (b"f2".to_vec(), encode_member(b"v2", Some(exp2))),
      ],
      exp1,
    );
  }
  let mut s = session_with(&env);

  // 水位内覆盖 f1：出账前置 Below 零出账，tree_put_batch 清 TTL，
  // new_fields=0 不落 meta → next_expiry 残 exp1（树内已无该刻度成员）
  for key in [b"w1".as_slice(), b"w2".as_slice(), b"w3".as_slice()] {
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Hset, &[key, b"f1", b"v1x"]),
      int(0),
      "存活覆写计 0"
    );
  }

  // 越过残低水位（f1 原到期刻度已过，树内实际零到期）
  sleep(Duration::from_millis(1100));

  // 三命令首条：零到期臂交出存活全集 → f1:v1x + f2:v2 恒非空
  let hgetall = auto_exec(&env, &mut s, RespCommand::Hgetall, &[b"w1"]);
  assert!(
    hgetall.starts_with(b"*4\r\n"),
    "残低水位零到期窗 HGETALL 必须输出 2 对全集，实际 {hgetall:?}"
  );
  for (f, v) in [
    (b"f1".as_slice(), b"v1x".as_slice()),
    (b"f2".as_slice(), b"v2".as_slice()),
  ] {
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

  let hkeys = auto_exec(&env, &mut s, RespCommand::Hkeys, &[b"w2"]);
  assert!(
    hkeys.starts_with(b"*2\r\n"),
    "残低水位零到期窗 HKEYS 必须输出 2 字段，实际 {hkeys:?}"
  );
  for f in [b"f1".as_slice(), b"f2".as_slice()] {
    let f_bulk = bulk(f);
    assert!(
      hkeys.windows(f_bulk.len()).any(|w| w == f_bulk.as_slice()),
      "HKEYS 应包含字段 {:?}",
      from_utf8(f).unwrap()
    );
  }

  let hvals = auto_exec(&env, &mut s, RespCommand::Hvals, &[b"w3"]);
  assert!(
    hvals.starts_with(b"*2\r\n"),
    "残低水位零到期窗 HVALS 必须输出 2 值，实际 {hvals:?}"
  );
  for v in [b"v1x".as_slice(), b"v2".as_slice()] {
    let v_bulk = bulk(v);
    assert!(
      hvals.windows(v_bulk.len()).any(|w| w == v_bulk.as_slice()),
      "HVALS 应包含值 {:?}",
      from_utf8(v).unwrap()
    );
  }

  // 对照：同刻点查恒正常（修复前 HGETALL 空集而 HGET 有值，同一键自相矛盾）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"w1", b"f1"]),
    bulk(b"v1x")
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"w1", b"f2"]),
    bulk(b"v2")
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"w1"]),
    int(2),
    "计数面恒精确（出账后直读 size）"
  );
}

/// 分层 HSETNX 到期字段视同缺席（r15-hash 发现一「与分层态同口径」对照锚）：
/// f1 挂短 TTL 到期后 HSETNX 走插入臂回 1、新值可读、计数不漂移；信封态
/// hash_set 的到期守卫与该三态判定同口径
///（libs/server/Objects/Hash/HashObjectImpl.cs:HashSet 第一臂
/// `!exists || IsExpired`）
#[test]
fn tiered_hsetnx_expired_field_replies_one_and_readable() {
  let env = tiered_env("tiered-ttl-hsetnx-expired.db");
  let exp1 = now_ticks() + TICKS_PER_SECOND;
  promote(
    &env,
    b"n",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", Some(exp1))),
      (b"f2".to_vec(), encode_member(b"v2", None)),
    ],
    exp1,
  );
  let mut s = session_with(&env);

  sleep(Duration::from_millis(1100));

  // 到期在树：视同缺席插入，回 1（修复前信封态同场景拒插回 0 的分层对照面）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hsetnx, &[b"n", b"f1", b"v1n"]),
    int(1),
    "到期字段 HSETNX 应视同缺席回 1"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"n", b"f1"]),
    bulk(b"v1n"),
    "插入的新值应可读"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"n"]),
    int(2),
    "到期→重插净计数不漂移"
  );
}

/// 分层 ZADD NX 到期成员视同缺席（zcode-r34-memberttl2 双态收敛回归）：
/// 成员挂短 TTL 到期后，分层 ZADD NX 视同缺席走新增臂回 1、新值可读、清 TTL；
/// 与信封态 sorted_set_add 的到期守卫同口径
#[test]
fn tiered_zset_expired_member_zadd_nx_treated_as_missing() {
  let env = tiered_env("tiered-ttl-zaddnx-expired.db");
  let exp1 = now_ticks() + TICKS_PER_SECOND;
  promote(
    &env,
    b"zn",
    GarnetObjectType::SortedSet,
    vec![
      (
        b"m1".to_vec(),
        encode_member(&10.0f64.to_be_bytes(), Some(exp1)),
      ),
      (b"m2".to_vec(), encode_member(&20.0f64.to_be_bytes(), None)),
    ],
    exp1,
  );
  let mut s = session_with(&env);

  sleep(Duration::from_millis(1100));

  // 到期在树：视同缺席新增，NX 不拦截，回 1
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zadd,
      &[b"zn", b"NX", b"30", b"m1"]
    ),
    int(1),
    "到期成员 ZADD NX 应视同缺席回 1"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscore, &[b"zn", b"m1"]),
    bulk(b"30"),
    "插入的新分值应可读"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zcard, &[b"zn"]),
    int(2),
    "到期覆盖新增净计数不漂移"
  );
}
