//! ZPOPMIN count 形会话级「声明数恒等实写 + PING 帧边界干净」回归（zcode-r159c-zpopcnt）
//!
//! ZADD 多员 + ZEXPIRE 尾段挂短 TTL（100ms），立即 ZPOPMIN key <基数>：对象臂
//! 环前一次剔除、环内免重采样——尾段成员环前采样时仍存活计入钳制
//! 声明、弹出时按存活原样写出，外层头声明数恒等实写条目数，应答帧恰在
//! 帧尾收束；随后 PING 应答独立完整不挂读（缺陷形下环内每轮重采样水位、
//! 中途剔员致头多宣，客户端按声明续读即吞掉 PING 应答帧体，RESP 流永久
//! 错位）。裁决与收口注记：doc/zh/deviations.md §53。
//! 注：环内跨到期刻的压力形由 wcol 结构化锁 member_ttl_pop_reply_header
//!（3 万员弹出环必跨到期刻）承压；本会话锁取 100ms 抗全量门禁负载抖动。

use wnode::resp::RespServerSession;
use wnode_test::with_batch;
use wtest_base::a;

/// 批量 ZADD 便捷封装（entries 为 (score, member) 序）
fn zadd<'a, D: wdev::Device>(
  s: &mut RespServerSession,
  batch: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  entries: &[(&[u8], &[u8])],
) {
  let mut args: Vec<&[u8]> = vec![key];
  for &(score, member) in entries {
    args.push(score);
    args.push(member);
  }
  let mut out = Vec::new();
  s.sorted_set_add(&args, batch, &mut out).unwrap();
}

#[test]
fn zpopmin_count_declared_equals_written_and_ping_frame_boundary_clean() {
  with_batch(|s, batch| {
    let key = b"zpop_ttl9";
    let entries: &[(&[u8], &[u8])] = &[
      (b"1", b"m01"),
      (b"2", b"m02"),
      (b"3", b"m03"),
      (b"4", b"m04"),
      (b"5", b"m05"),
      (b"6", b"m06"),
      (b"7", b"m07"),
      (b"8", b"m08"),
      (b"9", b"m09"),
      (b"10", b"m10"),
      (b"11", b"m11"),
      (b"12", b"m12"),
    ];
    zadd(s, batch, key, entries);

    // 尾段 4 员挂 100ms 短 TTL：环前剔除采样（微秒级同步路径）时仍存活，
    // 计入钳制声明（1ms 形在全量门禁负载下会被环前剔除，抖动形归 wcol 锁）
    let mut out = Vec::new();
    s.sorted_set_expire(
      "ZPEXPIRE",
      &[
        key, b"100", b"MEMBERS", b"4", b"m09", b"m10", b"m11", b"m12",
      ],
      batch,
      &mut out,
      true,
      false,
    )
    .unwrap();
    assert_eq!(out, b"*4\r\n:1\r\n:1\r\n:1\r\n:1\r\n");

    // ZPOPMIN key 12：环内免重采样，12 员（含尾段到期窗成员）按存活原样
    // 弹出，RESP2 加倍头声明 24 项恒等实写
    out.clear();
    s.sorted_set_pop(a![key, b"12"], batch, &mut out, true)
      .unwrap();

    let mut expected = Vec::from(&b"*24\r\n"[..]);
    for &(score, member) in entries {
      expected.push(b'$');
      expected.extend_from_slice(member.len().to_string().as_bytes());
      expected.extend_from_slice(b"\r\n");
      expected.extend_from_slice(member);
      expected.extend_from_slice(b"\r\n$");
      expected.push(b'0' + score.len() as u8);
      expected.extend_from_slice(b"\r\n");
      expected.extend_from_slice(score);
      expected.extend_from_slice(b"\r\n");
    }
    assert_eq!(
      out, expected,
      "声明 24 项恒等实写 24 项（尾段到期窗成员按存活弹出）"
    );

    // PING 帧边界干净：前帧恰在帧尾收束，PING 应答独立完整不被误吞
    let mut ping = Vec::new();
    s.network_ping(&[], &mut ping).unwrap();
    assert_eq!(ping, b"+PONG\r\n");
  });
}
