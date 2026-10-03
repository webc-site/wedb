//! RESP 解析模糊回归锁（对标 garnet/test/standalone/Garnet.test/Resp/
//! RespParseFuzzRegressionTests.cs：模糊测试历史缺陷不回退）
//!
//! C# 侧 FuzzParseCommandBuffer 异常上抛（逐样本断言 RespParsingException）；
//! rust 对位契约是优雅拒绝：对抗样本不 panic、返回 (false, RespCommand::Invalid)。
//! 样本族与 C# 逐一对应：MakeUpperCase 越界访问三样本、长命令名断言移除面、
//! 短命令名断言移除面、数组计数断言移除面（合法数字位豁免与 C# 同口径）。
//! 消费端即 wnode/src/resp/parser/resp_command.rs 的 fuzz_parse_command_buffer
//!（C# Garnet.fuzz 的 rust 面唯一测试消费点，语义级死代码检测的在册锚）。
//! 消费端仅 debug_assertions 构建导出（「仅测试可见」门），本文件随门裁剪。

#![cfg(debug_assertions)]

use wnode::resp::resp_server_session::RespServerSession;
use wresp::command::RespCommand;

/// C# MakeUpperCaseAccessViolation 三样本：大写化路径越界访问史
const UPPER_CASE_SAMPLES: [&[u8]; 3] = [
  &[
    0x2A, 0x33, 0x0D, 0x0A, 0x24, 0x36, 0x0D, 0x0A, 0x6C, 0x78, 0x4A, 0x43, 0x34, 0x0D, 0xFF, 0x00,
    0xFF, 0x87, 0x0D, 0x00, 0x87,
  ],
  &[
    0x2A, 0x31, 0x0D, 0x0A, 0x24, 0x37, 0x0D, 0x0A, 0x0D, 0x32, 0x0D, 0x6B, 0x6B, 0x6B, 0x6B, 0x6B,
    0x6B, 0x6B, 0x6B,
  ],
  &[
    0x2A, 0x32, 0x0D, 0x0A, 0x24, 0x35, 0x0D, 0x0A, 0x42, 0x38, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0xFF,
    0xFF, 0x00, 0x87,
  ],
];

/// 优雅拒绝契约断言（对抗样本不 panic、恒 (false, Invalid)）
fn assert_graceful_reject(session: &mut RespServerSession, sample: &[u8]) {
  let (parsed, cmd) = session.fuzz_parse_command_buffer(sample);
  assert!(!parsed, "对抗样本须被拒: {sample:?}");
  assert_eq!(
    cmd,
    RespCommand::Invalid,
    "对抗样本命令须为 Invalid: {sample:?}"
  );
}

/// C# MakeUpperCaseAccessViolation：三样本压力循环（原 1000 次迭代量级）
#[test]
fn make_upper_case_adversarial_samples_rejected() {
  let mut session = RespServerSession::default();
  for sample in UPPER_CASE_SAMPLES {
    for _ in 0..200 {
      assert_graceful_reject(&mut session, sample);
    }
  }
}

/// C# FastParseArrayCommandLongCommandAssertFailure：长命令名位全值扫描
/// （数字位合法豁免，其余 251 值一律拒绝）
#[test]
fn fast_parse_long_command_name_rejects_non_digit_length() {
  let mut example = [
    0x2Au8, 0x32, 0x0D, 0x0A, 0x24, 0x36, 0x25, 0x0D, 0x0A, 0x43, 0x34, 0xFF, 0xFF, 0xFF, 0xFF,
    0x00, 0x87,
  ];
  let mut session = RespServerSession::default();
  for val in 0..=u8::MAX {
    if val.is_ascii_digit() {
      continue;
    }
    example[6] = val;
    assert_graceful_reject(&mut session, &example);
  }
}

/// C# FastParseArrayCommandShortCommandAssertFailure：短命令名位全值扫描
/// （'1'-'9' 合法豁免，前导 0 与其余值拒绝）
#[test]
fn fast_parse_short_command_name_rejects_non_digit_length() {
  let mut example = [
    0x2Au8, 0x32, 0x30, 0x0D, 0x0A, 0x24, 0x3C, 0x0D, 0x0A, 0x24, 0x32, 0x0D, 0x0A,
  ];
  let mut session = RespServerSession::default();
  for val in 0..=u8::MAX {
    if (b'1'..=b'9').contains(&val) {
      continue;
    }
    example[6] = val;
    assert_graceful_reject(&mut session, &example);
  }
}

/// C# FastParseArrayCommandArrayCountAssertFailure：数组计数位全值扫描
#[test]
fn fast_parse_array_count_rejects_non_digit_count() {
  let mut example = [
    0x2Au8, 0x41, 0x0D, 0x0A, 0x24, 0x35, 0x0D, 0x0A, 0x78, 0x78, 0x78, 0x78, 0x78, 0x7C, 0x78,
  ];
  let mut session = RespServerSession::default();
  for val in 0..=u8::MAX {
    if (b'1'..=b'9').contains(&val) {
      continue;
    }
    example[1] = val;
    assert_graceful_reject(&mut session, &example);
  }
}
