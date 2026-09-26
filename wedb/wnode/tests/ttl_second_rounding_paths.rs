//! TTL 慢路径折秒四舍五入回归测试（票 zcode-r15-expire 发现二）
//!
//! 缺陷背景：TTL 命令慢路径（key_admin_slow 的 C::Ttl 臂）把 wkv `pttl_ms`
//! 出参毫秒 `value.div_euclid(1000)` 折秒——毫秒截断后再 floor 的双重向下取
//! 整；快路径 `network_ttl` 经 `seconds_from_diff_ticks`（(diff + TPS/2)/TPS）
//! 与 C# `ConvertUtils.SecondsFromDiffUtcNowTicks` 同式四舍五入。同一键剩余
//! 1.5 秒余量时快路径回 2、慢路径回 1、C# 回 2：TTL 记录降温（磁盘候选）即
//! 观测到 TTL 跳变，违反多路径行为同构。
//!
//! 修复：慢路径改毫秒域四舍五入 `(value + 500) / 1000`——ticks → 毫秒截断
//! 损失 r < 10_000 ticks（<1ms）且半秒余量 5_000_000 ticks 与 500ms 整对
//! 齐，r 恒不跨进位边界，与快路径 `seconds_from_diff_ticks` 逐值等价。
//!
//! C# 对位：
//! - libs/common/ConvertUtils.cs:SecondsFromDiffUtcNowTicks（24-32 四舍五入）
//! - libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleTtl
//!
//! 验证点（票面发现二第 3 条）：剩余 1.5 秒/1.499 秒边界 TTL 键，分别以
//! 内存态（快路径）与磁盘候选态（`flush_and_evict_all` 后慢路径）执行 TTL，
//! 断言双侧同值且等于 C# 口径（1.5 秒余量回 2）。PTTL 无舍入面（恒截断），
//! 快慢路径原样一致。

use std::{str::from_utf8, sync::Arc, thread::sleep, time::Duration};

use compio::runtime::Runtime;
use tempfile::TempDir;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::roundtrip;
use wtest_base::test_store_config;

fn open_store(tag: &str) -> (Runtime, Arc<WedbStore<SegmentedDevice>>, TempDir) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  (Runtime::new().unwrap(), store, dir)
}

fn consumer_on(store: &Arc<WedbStore<SegmentedDevice>>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// `:N\r\n` 整数回执解析
fn reply_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?.strip_suffix(b"\r\n")?;
  from_utf8(body).ok()?.parse().ok()
}

/// 快路径（内存态）TTL 应答
fn ttl_fast(rt: &Runtime, store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<i64> {
  let mut c = consumer_on(store);
  reply_int(&roundtrip(rt, &mut c, &[b"TTL", key]))
}

/// 快路径（内存态）PTTL 应答
fn pttl_fast(rt: &Runtime, store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<i64> {
  let mut c = consumer_on(store);
  reply_int(&roundtrip(rt, &mut c, &[b"PTTL", key]))
}

/// 快路径（内存态）EXPIRETIME 应答
fn expiretime_fast(
  rt: &Runtime,
  store: &Arc<WedbStore<SegmentedDevice>>,
  key: &[u8],
) -> Option<i64> {
  let mut c = consumer_on(store);
  reply_int(&roundtrip(rt, &mut c, &[b"EXPIRETIME", key]))
}

/// 快路径（内存态）PEXPIRETIME 应答
fn pexpiretime_fast(
  rt: &Runtime,
  store: &Arc<WedbStore<SegmentedDevice>>,
  key: &[u8],
) -> Option<i64> {
  let mut c = consumer_on(store);
  reply_int(&roundtrip(rt, &mut c, &[b"PEXPIRETIME", key]))
}

/// 磁盘候选态（慢路径）单命令应答
fn slow_reply(
  rt: &Runtime,
  store: &Arc<WedbStore<SegmentedDevice>>,
  args: &[&[u8]],
) -> Option<i64> {
  let mut c = consumer_on(store);
  reply_int(&roundtrip(rt, &mut c, args))
}

/// 核心边界：剩余 1.5 秒以上余量（PX 1600 → 剩余 ≈1.6s）快路径回 2，冷化
/// 后慢路径（毫秒 1500+ 折秒）也必须回 2——收口前 div_euclid 双重向下取整
/// 回 1，同键随冷热降级跳变
#[test]
fn ttl_half_second_boundary_agrees_across_paths() {
  let (rt, store, _dir) = open_store("ttl-round-1500.db");
  let mut c = consumer_on(&store);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"rr:half", b"v", b"PX", b"1600"]),
    b"+OK\r\n"
  );
  // 快路径：seconds_from_diff_ticks 四舍五入 → 1.6s ⇒ 2（C# 口径）
  assert_eq!(
    ttl_fast(&rt, &store, b"rr:half"),
    Some(2),
    "快路径 1.5 秒余量应回 2（C# SecondsFromDiffUtcNowTicks 口径）"
  );

  // 冷化：数据与 TTL 记录均落盘，TTL 命令降级慢路径
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    slow_reply(&rt, &store, &[b"TTL", b"rr:half"]),
    Some(2),
    "慢路径 1.5 秒余量须与快路径同值回 2（收口前 div_euclid 回 1）"
  );
}

/// 次边界：剩余 1.499 秒（PX 1499 → 剩余 ≈1.4989s）快慢路径同回 1
///（四舍五入与截断在该侧同值，守回归面）
#[test]
fn ttl_sub_half_second_boundary_agrees_across_paths() {
  let (rt, store, _dir) = open_store("ttl-round-1499.db");
  let mut c = consumer_on(&store);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"rr:sub", b"v", b"PX", b"1499"]),
    b"+OK\r\n"
  );
  assert_eq!(
    ttl_fast(&rt, &store, b"rr:sub"),
    Some(1),
    "快路径 1.499 秒余量应回 1"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    slow_reply(&rt, &store, &[b"TTL", b"rr:sub"]),
    Some(1),
    "慢路径 1.499 秒余量须与快路径同值回 1"
  );
}

/// PTTL 无舍入面（恒毫秒截断）：快慢路径同口径同值域；TTL/PTTL 的 -1
///（无 TTL）与 -2（缺失）哨兵值在两路径下逐字节一致
#[test]
fn pttl_and_sentinels_agree_across_paths() {
  let (rt, store, _dir) = open_store("ttl-round-pttl.db");
  let mut c = consumer_on(&store);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"rr:pttl", b"v", b"PX", b"1501"]),
    b"+OK\r\n"
  );
  roundtrip(&rt, &mut c, &[b"SET", b"rr:plain", b"v"]);
  let pttl_hot = pttl_fast(&rt, &store, b"rr:pttl").expect("PTTL 快路径应整数回执");
  assert!(
    (1000..=1600).contains(&pttl_hot),
    "PTTL 应为毫秒截断值域: {pttl_hot}"
  );

  // 哨兵值（快路径）
  assert_eq!(ttl_fast(&rt, &store, b"rr:plain"), Some(-1), "无 TTL 键 -1");
  assert_eq!(ttl_fast(&rt, &store, b"rr:gone"), Some(-2), "缺失键 -2");

  // 冷化后慢路径：PTTL 同口径、哨兵值逐字节一致
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let pttl_cold = slow_reply(&rt, &store, &[b"PTTL", b"rr:pttl"]).expect("PTTL 慢路径应整数回执");
  assert!(
    (500..=pttl_hot).contains(&pttl_cold),
    "PTTL 慢路径须同口径（流逝后 ≤ 快路径读值 {pttl_hot}）：{pttl_cold}"
  );
  assert_eq!(
    slow_reply(&rt, &store, &[b"TTL", b"rr:plain"]),
    Some(-1),
    "无 TTL 键慢路径 -1"
  );
  assert_eq!(
    slow_reply(&rt, &store, &[b"TTL", b"rr:gone"]),
    Some(-2),
    "缺失键慢路径 -2"
  );
}

/// EXPIRETIME / PEXPIRETIME 慢路径与快路径对齐（票 zcode-r35-ttlround）
///
/// 缺陷背景：EXPIRETIME 慢路径无条件 `value.div_euclid(1000)`，将哨兵 -2（键缺失
/// 或已过期）向下取整折成 -1（无 TTL），导致冷化键在快慢路径间产生 -2 → -1 跳变。
///
/// 修复：仅在 value > 0 时换算秒数，-2 与 -1 原样透传。
///
/// 验证点：
/// 1. 缺失键 (-2) 在快慢路径下恒为 -2（修复前慢路径 EXPIRETIME 误回 -1）；
/// 2. 盘上已过期键 (-2) 在冷化后慢路径下经惰性清退恒为 -2（修复前慢路径误回 -1）；
/// 3. 无 TTL 键 (-1) 在快慢路径下恒为 -1；
/// 4. 有 TTL 键绝对 Unix 秒 / 毫秒在快慢路径下逐字节一致，且毫秒换算秒恒等。
#[test]
fn expiretime_and_pexpiretime_agree_across_paths() {
  let (rt, store, _dir) = open_store("ttl-round-expiretime.db");
  let mut c = consumer_on(&store);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"rr:exp", b"v", b"PX", b"100000"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"rr:expired", b"v", b"PX", b"50"]),
    b"+OK\r\n"
  );
  roundtrip(&rt, &mut c, &[b"SET", b"rr:plain", b"v"]);

  // 快路径（内存态）
  let exp_fast = expiretime_fast(&rt, &store, b"rr:exp").expect("EXPIRETIME 快路径应返回整数");
  let pexp_fast = pexpiretime_fast(&rt, &store, b"rr:exp").expect("PEXPIRETIME 快路径应返回整数");
  assert!(
    exp_fast > 0,
    "在场键 EXPIRETIME 应为正 Unix 秒戳: {exp_fast}"
  );
  assert!(
    pexp_fast > 0,
    "在场键 PEXPIRETIME 应为正 Unix 毫秒戳: {pexp_fast}"
  );
  assert_eq!(
    pexp_fast.div_euclid(1000),
    exp_fast,
    "毫秒戳换算秒应与 EXPIRETIME 一致"
  );

  assert_eq!(
    expiretime_fast(&rt, &store, b"rr:plain"),
    Some(-1),
    "无 TTL 键快路径 EXPIRETIME 恒 -1"
  );
  assert_eq!(
    pexpiretime_fast(&rt, &store, b"rr:plain"),
    Some(-1),
    "无 TTL 键快路径 PEXPIRETIME 恒 -1"
  );
  assert_eq!(
    expiretime_fast(&rt, &store, b"rr:gone"),
    Some(-2),
    "缺失键快路径 EXPIRETIME 恒 -2"
  );
  assert_eq!(
    pexpiretime_fast(&rt, &store, b"rr:gone"),
    Some(-2),
    "缺失键快路径 PEXPIRETIME 恒 -2"
  );

  // 冷化：在 rr:expired 未过期前将其刷盘降温
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 等待 rr:expired 达到过期态（盘上已过期记录）
  sleep(Duration::from_millis(60));

  // 冷化后慢路径验证
  let exp_slow =
    slow_reply(&rt, &store, &[b"EXPIRETIME", b"rr:exp"]).expect("EXPIRETIME 慢路径应返回整数");
  let pexp_slow =
    slow_reply(&rt, &store, &[b"PEXPIRETIME", b"rr:exp"]).expect("PEXPIRETIME 慢路径应返回整数");
  assert_eq!(
    exp_slow, exp_fast,
    "在场键慢路径 EXPIRETIME 应与快路径逐字节一致"
  );
  assert_eq!(
    pexp_slow, pexp_fast,
    "在场键慢路径 PEXPIRETIME 应与快路径逐字节一致"
  );

  // 哨兵：无 TTL 键
  assert_eq!(
    slow_reply(&rt, &store, &[b"EXPIRETIME", b"rr:plain"]),
    Some(-1),
    "无 TTL 键慢路径 EXPIRETIME 恒 -1"
  );
  assert_eq!(
    slow_reply(&rt, &store, &[b"PEXPIRETIME", b"rr:plain"]),
    Some(-1),
    "无 TTL 键慢路径 PEXPIRETIME 恒 -1"
  );

  // 哨兵：缺失键（收口前 div_euclid 误将 -2 折为 -1）
  assert_eq!(
    slow_reply(&rt, &store, &[b"EXPIRETIME", b"rr:gone"]),
    Some(-2),
    "缺失键慢路径 EXPIRETIME 须为 -2（收口前误回 -1）"
  );
  assert_eq!(
    slow_reply(&rt, &store, &[b"PEXPIRETIME", b"rr:gone"]),
    Some(-2),
    "缺失键慢路径 PEXPIRETIME 须为 -2"
  );

  // 哨兵：盘上已过期键（惰性清退后判缺失，收口前 div_euclid 误将 -2 折为 -1）
  assert_eq!(
    slow_reply(&rt, &store, &[b"EXPIRETIME", b"rr:expired"]),
    Some(-2),
    "已过期键慢路径 EXPIRETIME 须为 -2（收口前误回 -1）"
  );
  assert_eq!(
    slow_reply(&rt, &store, &[b"PEXPIRETIME", b"rr:expired"]),
    Some(-2),
    "已过期键慢路径 PEXPIRETIME 须为 -2"
  );
}
