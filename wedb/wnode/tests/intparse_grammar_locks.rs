//! 严格整数文法锁（工单 zcode-r31-intparse / doc/zh/deviations.md §32b）
//!
//! C# int32 档 RespReadUtils.TryReadInt32Safe 存在 allowLeadingZeros 死参
//! （参数声明但未消费），导致 SessionParseState.TryGetInt 全消费点实际放行
//! "007"/"+007" 形态。Rust 全域维持严格十进制整数文法（`wbase/src/num.rs:scan_digits`
//! 单点收口，与 C# 自家 i64 档同口径），全消费点恒定拒绝前导零。
//!
//! 本用例覆盖票面要求的 11 处核心参数面消费点文法锁（第 12 处 CLUSTER COUNTKEYSINSLOT
//! 见 `wedb/wedb/tests/cluster_resp_session.rs:cluster_countkeysinslot_grammar_lock`）：
//! 1. SELECT 007
//! 2. SWAPDB 007 0
//! 3. SETRANGE k 007 X
//! 4. GETRANGE k 0 009
//! 5. SET k v EX 007
//! 6. SETEX k 007 v
//! 7. HELLO 007
//! 8. LTRIM k 007 -1
//! 9. LINDEX k 007
//! 10. SRANDMEMBER k 007
//! 11. ZINTERCARD（LIMIT 007 与 numkeys 007）
//!
//! 全部断言前导零形态恒回 not-integer / invalid index 错误帧，且对照组 "7" 形态成功。

use wnode::resp::acl_store::AclStore;
use wnode_test::{test_env, with_batch};

const NOT_INTEGER: &[u8] = b"-ERR value is not an integer or out of range.\r\n";

/// 1. SELECT 007 前导零文法锁（对标 ArrayCommands.cs:125 TryGetInt 走 parse_db_index）
#[test]
fn select_leading_zero_grammar_lock() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_select(&[b"007"], batch, &mut out).unwrap();
    assert_eq!(out, NOT_INTEGER, "SELECT 007 必须恒回报 not-integer 错误帧");

    out.clear();
    s.network_select(&[b"7"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n", "对照组 SELECT 7 必须成功回 +OK");
  });
}

/// 2. SWAPDB 007 0 前导零文法锁（对标 ArrayCommands.cs:175/180 TryGetInt）
#[test]
fn swapdb_leading_zero_grammar_lock() {
  with_batch(|s, _batch| {
    let mut out = Vec::new();
    s.network_swapdb(&[b"007", b"0"], &mut out).unwrap();
    assert_eq!(
      out, b"-ERR invalid first DB index.\r\n",
      "SWAPDB 007 0 必须恒回 invalid first DB index 错误帧"
    );

    out.clear();
    s.network_swapdb(&[b"0", b"0"], &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n", "对照组 SWAPDB 0 0 必须成功回 +OK");
  });
}

/// 3. SETRANGE k 007 X 前导零文法锁（对标 BasicCommands.cs:450 TryGetInt）
#[test]
fn setrange_leading_zero_grammar_lock() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set_range(&[b"k", b"007", b"X"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out, NOT_INTEGER,
      "SETRANGE k 007 X 必须恒回报 not-integer 错误帧"
    );

    out.clear();
    s.network_set_range(&[b"k", b"7", b"X"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out, b":8\r\n",
      "对照组 SETRANGE k 7 X 必须成功写入并返回长度"
    );
  });
}

/// 4. GETRANGE k 0 009 前导零文法锁（对标 BasicCommands.cs:499 TryGetInt）
#[test]
fn getrange_leading_zero_grammar_lock() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"k", b"0123456789"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    s.network_get_range(&[b"k", b"0", b"009"], batch, &mut out, "GETRANGE")
      .unwrap();
    assert_eq!(
      out, NOT_INTEGER,
      "GETRANGE k 0 009 必须恒回报 not-integer 错误帧"
    );

    out.clear();
    s.network_get_range(&[b"k", b"0", b"9"], batch, &mut out, "GETRANGE")
      .unwrap();
    assert_eq!(
      out, b"$10\r\n0123456789\r\n",
      "对照组 GETRANGE k 0 9 必须成功返回切片"
    );
  });
}

/// 5. SET k v EX 007 前导零文法锁（对标 BasicCommands.cs:653 TryGetInt）
#[test]
fn set_ex_leading_zero_grammar_lock() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"k", b"v", b"EX", b"007"], batch, None, &mut out)
      .unwrap();
    assert_eq!(
      out, NOT_INTEGER,
      "SET k v EX 007 必须恒回报 not-integer 错误帧"
    );

    out.clear();
    s.network_set(&[b"k", b"v", b"EX", b"7"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n", "对照组 SET k v EX 7 必须成功回 +OK");
  });
}

/// 6. SETEX k 007 v 前导零文法锁（对标 BasicCommands.cs:542 TryGetInt）
#[test]
fn setex_leading_zero_grammar_lock() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_setex(&[b"k", b"007", b"v"], batch, None, &mut out)
      .unwrap();
    assert_eq!(
      out, NOT_INTEGER,
      "SETEX k 007 v 必须恒回报 not-integer 错误帧"
    );

    out.clear();
    s.network_setex(&[b"k", b"7", b"v"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n", "对照组 SETEX k 7 v 必须成功回 +OK");
  });
}

/// 7. HELLO 007 前导零文法锁（对标 BasicCommands.cs:1455 TryGetInt）
///
/// C# 放行 007 解析为 7 后落协议不支持门回 unsupported protocol version，
/// Rust strict_i32 严格文法收口直接报 Protocol value is not an integer or out of range
#[compio::test]
async fn hello_leading_zero_grammar_lock() {
  let (_dir, session, mut resp) = test_env(false);
  let acl_store = AclStore::new(&session);

  let mut out = Vec::new();
  resp
    .network_hello(&[b"007"], &acl_store, &mut out)
    .await
    .unwrap();
  assert_eq!(
    out, b"-ERR Protocol version is not an integer or out of range.\r\n",
    "HELLO 007 必须恒回 protocol version is not an integer 错误帧"
  );

  out.clear();
  resp
    .network_hello(&[b"2"], &acl_store, &mut out)
    .await
    .unwrap();
  assert!(
    out.starts_with(b"*"),
    "对照组 HELLO 2 必须成功返回协议元数据数组"
  );
}

/// 8. LTRIM k 007 -1 前导零文法锁（对标 ListCommands.cs:464/512 TryGetInt）
#[test]
fn ltrim_leading_zero_grammar_lock() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.list_trim(&[b"k", b"007", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out, NOT_INTEGER,
      "LTRIM k 007 -1 必须恒回报 not-integer 错误帧"
    );

    out.clear();
    s.list_trim(&[b"k", b"7", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n", "对照组 LTRIM k 7 -1 必须成功回 +OK");
  });
}

/// 9. LINDEX k 007 前导零文法锁（对标 ListCommands.cs:564 TryGetInt）
#[test]
fn lindex_leading_zero_grammar_lock() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.list_index(&[b"k", b"007"], batch, &mut out).unwrap();
    assert_eq!(
      out, NOT_INTEGER,
      "LINDEX k 007 必须恒回报 not-integer 错误帧"
    );

    out.clear();
    s.list_index(&[b"k", b"7"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n", "对照组 LINDEX k 7 必须成功回 nil");
  });
}

/// 10. SRANDMEMBER k 007 前导零文法锁（对标 SetCommands.cs:531 TryGetInt）
#[test]
fn srandmember_leading_zero_grammar_lock() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_random_member(&[b"k", b"007"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out, NOT_INTEGER,
      "SRANDMEMBER k 007 必须恒回报 not-integer 错误帧"
    );

    out.clear();
    s.set_random_member(&[b"k", b"7"], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n", "对照组 SRANDMEMBER k 7 必须成功返回空列表");
  });
}

/// 11. ZINTERCARD 前导零文法锁（对标 SortedSetCommands.cs:423/1211 TryGetInt）
#[test]
fn zintercard_leading_zero_grammar_lock() {
  with_batch(|s, batch| {
    // 11.1 LIMIT 伴参前导零拒绝
    let mut out = Vec::new();
    s.sorted_set_intersect_length(&[b"2", b"k1", b"k2", b"LIMIT", b"007"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out, NOT_INTEGER,
      "ZINTERCARD 2 k1 k2 LIMIT 007 必须恒回报 not-integer 错误帧"
    );

    out.clear();
    s.sorted_set_intersect_length(&[b"2", b"k1", b"k2", b"LIMIT", b"7"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out, b":0\r\n",
      "对照组 ZINTERCARD 2 k1 k2 LIMIT 7 必须成功返回计数"
    );

    // 11.2 numkeys 参数前导零拒绝
    out.clear();
    s.sorted_set_intersect_length(&[b"007", b"k1"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out, NOT_INTEGER,
      "ZINTERCARD 007 k1 必须恒回报 not-integer 错误帧"
    );

    out.clear();
    s.sorted_set_intersect_length(&[b"1", b"k1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n", "对照组 ZINTERCARD 1 k1 必须成功返回计数");

    // 11.3 票面格式字面量直接覆盖
    out.clear();
    s.sorted_set_intersect_length(&[b"k1", b"k2", b"LIMIT", b"007"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out, NOT_INTEGER,
      "ZINTERCARD k1 k2 LIMIT 007 必须恒回报 not-integer 错误帧"
    );
  });
}
