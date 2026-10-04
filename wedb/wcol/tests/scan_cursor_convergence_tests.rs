//! HSCAN/ZSCAN 游标收敛判别回归测试（纯集合层，内存态对象直测，不牵引擎）
//!
//! 复现票 wcol-object-scan-stale-cursor-empty-page-spin：内存态只读扫描在
//! 「存活条目数 L < 起始游标 start <= 含到期总数 N」的死锁死角，尾段相等判定
//! `cursor + expired_keys_count == len` 恒不成立，向客户端返回原游标 `([], start)`，
//! 客户端持非零游标死循环挂死；分层态（wnode `exec_tiered_scan`）在同等交叉态
//! 返回 `([], 0)` 正常收敛，构成双态应答分叉。
//!
//! 到期成员经装载单点 [`wcol::HashObject::insert_expiration`] 直挂过去刻度
//! （与 object_serialize_expiration_tests.rs 同法，避开 set_expiration 对过去
//! 刻度的删除分支），字段仍滞留字典内垫高 `len`；存活成员不挂任何过期登记
//! （is_expired 恒 false）。死锁判定与字典遍历次序无关：所有存活条目下标恒
//! < start 被跳过，cursor 停在 start 未被累加，只读路径又绝不清到期。
//!
//! scan 内核为 emit 回调直写形态（发出条目计数由回调返回，替代旧 owned Vec
//! 收集）；本文件以计数 emit 适配旧 `items.len()` 观察口径。
//!
//! 自研依据: 集合对象层扫描游标收敛（C# 服务端面对标 test/standalone/Garnet.test/RespScanCommandsTests.cs）

use std::sync::Arc;

use wbase::time::now_ticks;
use wcol::{HashObject, SortedSetObject};

/// 过去刻度跨度（is_expired_at: expiration < now 即判到期，与真实 now 保持
/// 足够间隔以免慢机竞态）。存活成员不挂任何过期登记（is_expired 恒 false），
/// 无需未来刻度常量。
const EXPIRED_SPAN: i64 = 1_000_000;

/// 构造 `live` 个存活字段 + `expired` 个到期字段的哈希（总数 N = live + expired）
fn hash_with(live: usize, expired: usize) -> HashObject {
  let now = now_ticks();
  let mut hash = HashObject::new();
  for i in 0..live {
    hash
      .hash
      .insert(Arc::from(format!("live_{i}").into_bytes()), b"v".to_vec());
  }
  for i in 0..expired {
    let field: Arc<[u8]> = Arc::from(format!("exp_{i}").into_bytes());
    hash.hash.insert(Arc::clone(&field), b"v".to_vec());
    // 装载单点直挂过去刻度：字段留在字典内垫高 len，只读扫描不清到期
    hash.insert_expiration(field, now - EXPIRED_SPAN);
  }
  hash
}

/// 构造 `live` 个存活成员 + `expired` 个到期成员的有序集合（总数 N = live + expired）
fn zset_with(live: usize, expired: usize) -> SortedSetObject {
  let now = now_ticks();
  let mut zs = SortedSetObject::new();
  for i in 0..live {
    zs.add(format!("live_{i}").as_bytes(), 1.0);
  }
  for i in 0..expired {
    let member = format!("exp_{i}");
    zs.add(member.as_bytes(), 1.0);
    zs.insert_expiration(Arc::from(member.into_bytes()), now - EXPIRED_SPAN);
  }
  zs
}

/// 计数 emit（对位旧 `items.len()` 观察口径）：hash 成对形态每命中 +2
fn pair_counter(n: &mut usize) -> impl FnMut(&[u8], &[u8]) -> usize + '_ {
  move |_, _| {
    *n += 2;
    2
  }
}

/// 计数 emit：zset 每命中 +2（成员 + 分值）
fn zset_counter(n: &mut usize) -> impl FnMut(&[u8], f64) -> usize + '_ {
  move |_, _| {
    *n += 2;
    2
  }
}

/// 判别用例（内存态 HSCAN 死锁死角）：L=2 存活、E=2 到期、N=4，取 start=3 ∈ (L, N]。
/// 修复前尾判定 `3 + 2 == 4` 恒不成立 → 返回原游标 3，客户端死循环；修复后放宽为
/// `>=` 收敛 → 游标归零，与分层态 `([], 0)` 同口径。
#[test]
fn hscan_stale_cursor_in_expired_padding_window_converges_to_zero() {
  let hash = hash_with(2, 2);
  let mut n = 0;
  let cursor = hash.scan(3, 100, b"", false, pair_counter(&mut n));
  assert_eq!(n, 0, "存活数低于起始游标时不得产出条目");
  assert_eq!(
    cursor, 0,
    "内存态 HSCAN 死锁死角（L<start<=N）游标必须归零收敛，对齐分层态 ([], 0)"
  );
}

/// 判别用例（内存态 ZSCAN 死锁死角）：同上边界，修复前返回 ([], 3) 死锁。
#[test]
fn zscan_stale_cursor_in_expired_padding_window_converges_to_zero() {
  let zs = zset_with(2, 2);
  let mut n = 0;
  let cursor = zs.scan(3, 100, b"", false, zset_counter(&mut n));
  assert_eq!(n, 0, "存活数低于起始游标时不得产出成员");
  assert_eq!(
    cursor, 0,
    "内存态 ZSCAN 死锁死角（L<start<=N）游标必须归零收敛，对齐分层态 ([], 0)"
  );
}

/// 反向护栏（HSCAN）：start 恰为 N（仍属死锁域 start>L）亦须归零。
#[test]
fn hscan_stale_cursor_at_total_count_converges_to_zero() {
  let hash = hash_with(2, 2);
  let mut n = 0;
  let cursor = hash.scan(4, 100, b"", false, pair_counter(&mut n));
  assert_eq!(n, 0);
  assert_eq!(cursor, 0, "start == N 且含到期垫数时仍须归零（4+2>=4）");
}

/// 过度放宽护栏（HSCAN）：COUNT 截断的中间页游标不得被误归零。
/// 4 存活、0 到期、N=4，is_no_value 令每项 1 条；count=2 截断后 cursor=2 < N，
/// `>=` 判定不命中，游标须保留为 2 供续页。此用例修复前后皆绿，专防把截断比较
/// 一并放宽或把尾判定写成无条件归零的错误修复。
#[test]
fn hscan_truncated_page_preserves_next_cursor() {
  let hash = hash_with(4, 0);
  // NOVALUES 单条目形态：emit 每命中只计 1
  let mut n = 0;
  let cursor = hash.scan(0, 2, b"", true, |_, _| {
    n += 1;
    1
  });
  assert_eq!(n, 2, "COUNT 截断仍应回 2 字段");
  assert_eq!(cursor, 2, "截断未及末尾不得归零，须保留续页游标");
}

/// 正常收敛护栏（HSCAN）：无截断全量遍历至末尾必须归零。
#[test]
fn hscan_full_traversal_converges_to_zero() {
  let hash = hash_with(3, 0);
  let mut n = 0;
  let cursor = hash.scan(0, 100, b"", true, |_, _| {
    n += 1;
    1
  });
  assert_eq!(n, 3);
  assert_eq!(cursor, 0, "全量遍历至末尾必须归零");
}

/// 过度放宽护栏（ZSCAN）：成员每项占 2 条（成员 + 分值），4 存活、N=4，count=2
/// 使发出计数==count*2==4 时截断，cursor=2 < N 须保留，修复前后皆绿。
#[test]
fn zscan_truncated_page_preserves_next_cursor() {
  let zs = zset_with(4, 0);
  let mut n = 0;
  let cursor = zs.scan(0, 2, b"", false, zset_counter(&mut n));
  assert_eq!(n, 4, "两个成员各占成员 + 分值 2 条");
  assert_eq!(cursor, 2, "截断未及末尾不得归零，须保留续页游标");
}

/// 正常收敛护栏（ZSCAN）：无截断全量遍历至末尾必须归零。
#[test]
fn zscan_full_traversal_converges_to_zero() {
  let zs = zset_with(3, 0);
  let mut n = 0;
  let cursor = zs.scan(0, 100, b"", false, zset_counter(&mut n));
  assert_eq!(n, 6, "三个成员各占 2 条");
  assert_eq!(cursor, 0, "全量遍历至末尾必须归零");
}
