//! HEXPIRE/HPEXPIRE 字段族相对大值「C# 掐连对 rust 饱和钳正常应答」分叉语义锁
//! （工单 zcode-r143c-hexmatrix 案一，P4 登记级零行为改动，钉 rust 现状）。
//!
//! 对标 C#：`garnet/libs/server/Resp/Objects/HashCommands.cs:627-628` 字段族
//! 相对秒/毫秒双域走 `DateTimeOffset.UtcNow.AddSeconds/AddMilliseconds`，超阈
//! （秒域约 2.5e11、毫秒域约 2.5e14）抛 `ArgumentOutOfRangeException` 掐连；
//! rust 换算单点 `wbase/src/convert.rs` 饱和钳（`expire_after_to_ticks`/
//! `expire_after_ms_to_ticks`）后逐字段正常求值应答 `*N :1` 且连接存活。
//! 有意偏差登记见 doc/zh/deviations.md §4a，严禁回改饱和算式复刻掐连。
//!
//! 本档独立于 tiered_field_ttl.rs（并行席同面补锁避撞），既有锁零漂移。

use std::str;

use wnode_test::with_batch;

/// 解析 `*N` + `:int` 应答为整数数组
fn parse_int_array(frame: &[u8]) -> Vec<i64> {
  let mut items = Vec::new();
  let mut pos = match frame.iter().position(|&b| b == b'\n') {
    Some(p) => p + 1,
    None => return items,
  };
  while pos < frame.len() && frame[pos] == b':' {
    let end = frame[pos..].iter().position(|&b| b == b'\r').unwrap() + pos;
    items.push(
      str::from_utf8(&frame[pos + 1..end])
        .unwrap()
        .parse()
        .unwrap(),
    );
    pos = end + 2;
  }
  items
}

/// HEXPIRE 相对秒域大值 3e11（超 C# AddSeconds 阈约 2.5e11，rust 掐连对位值）：
/// 断言 `*2 :1 :1`、HTTL 读回大正数不回绕不负、同会话后续命令可用（连接存活）。
#[test]
fn hexpire_large_relative_seconds_saturates_and_keeps_session() {
  const BIG_SEC: i64 = 300_000_000_000;
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"h", b"f1", b"v1", b"f2", b"v2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n", "HSET 建字段应回 :2");
    out.clear();

    // C# 于此掐连；rust 饱和换算后逐字段落账 *2 :1 :1
    s.hash_expire(
      "HEXPIRE",
      &[b"h", b"300000000000", b"FIELDS", b"2", b"f1", b"f2"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(
      parse_int_array(&out),
      vec![1, 1],
      "HEXPIRE 相对秒域大值应饱和落账回 *2 :1 :1（§4a），不得掐连: {out:?}"
    );
    out.clear();

    // HTTL 读回：大正数、不回绕不负（窗口锁死在请求值与 C# 掐连阈之间口径）
    s.hash_time_to_live(
      "HTTL",
      &[b"h", b"FIELDS", b"2", b"f1", b"f2"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    let ttl = parse_int_array(&out);
    assert_eq!(ttl.len(), 2, "HTTL 帧形数组长恒 numFields: {out:?}");
    for v in &ttl {
      assert!(
        *v > 250_000_000_000 && *v <= BIG_SEC,
        "HTTL 应读回大正数不回绕不负（饱和远未来刻余量）: {v}"
      );
    }
    out.clear();

    // 同会话后续命令可用 = 连接存活
    s.hash_get_all(&[b"h"], batch, &mut out).unwrap();
    assert!(
      out.starts_with(b"*4\r\n"),
      "大值 HEXPIRE 后同连接后续命令须存活应答: {out:?}"
    );
  });
}

/// HPEXPIRE 相对毫秒域大值 3e14（超 C# AddMilliseconds 阈约 2.5e14，rust 掐连
/// 对位值）：同款断言 `*2 :1 :1`、HPTTL 读回大正数、同会话后续命令可用。
#[test]
fn hexpire_large_relative_milliseconds_saturates_and_keeps_session() {
  const BIG_MS: i64 = 300_000_000_000_000;
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"h", b"f1", b"v1", b"f2", b"v2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n", "HSET 建字段应回 :2");
    out.clear();

    s.hash_expire(
      "HPEXPIRE",
      &[b"h", b"300000000000000", b"FIELDS", b"2", b"f1", b"f2"],
      batch,
      &mut out,
      true,
      false,
    )
    .unwrap();
    assert_eq!(
      parse_int_array(&out),
      vec![1, 1],
      "HPEXPIRE 相对毫秒域大值应饱和落账回 *2 :1 :1（§4a），不得掐连: {out:?}"
    );
    out.clear();

    s.hash_time_to_live(
      "HPTTL",
      &[b"h", b"FIELDS", b"2", b"f1", b"f2"],
      batch,
      &mut out,
      true,
      false,
    )
    .unwrap();
    let ttl = parse_int_array(&out);
    assert_eq!(ttl.len(), 2, "HPTTL 帧形数组长恒 numFields: {out:?}");
    for v in &ttl {
      assert!(
        *v > 250_000_000_000_000 && *v <= BIG_MS,
        "HPTTL 应读回大正数不回绕不负（饱和远未来刻余量）: {v}"
      );
    }
    out.clear();

    s.hash_get_all(&[b"h"], batch, &mut out).unwrap();
    assert!(
      out.starts_with(b"*4\r\n"),
      "大值 HPEXPIRE 后同连接后续命令须存活应答: {out:?}"
    );
  });
}
