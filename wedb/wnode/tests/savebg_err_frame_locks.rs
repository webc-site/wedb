//! SAVE/BGSAVE/FLUSH 后台拒绝臂错误帧逐字节锁（zcode-r133c）
//!
//! 三臂文案入 `wresp/cmd_strings.rs` 无符号裸文案单源（对位 C#
//! CmdStrings.cs:318 裸文案 + TryWriteError 单点补负号形态），经产线唯一
//! 成帧通道 `write_error_raw` → `write_error_frame_to` 落帧；本测试锁线上
//! 帧形为单负号 `-ERR …\r\n`，杜绝 `--ERR` 双负号回归。

use wresp::cmd_strings::{
  RESP_ERR_DEVICE_CONTAMINATED_REFUSING_CHECKPOINT, RESP_ERR_DEVICE_CONTAMINATED_REFUSING_FLUSH,
  RESP_ERR_FLUSH_TRUNCATE_LOG_NS0, write_error_raw,
};

/// 单臂逐字节锁：帧首单 `-`、负载以 `ERR ` 起、单 `\r\n` 收尾且帧内无裸 CR/LF
fn lock_err_frame(msg: &str, expected: &[u8]) {
  // 文案单源契约：裸文案不带前导负号与 CRLF（带即经成帧器复现双负号错帧）
  assert!(!msg.starts_with('-'), "文案不得自带前导负号: {msg:?}");
  assert!(
    !msg.contains('\r') && !msg.contains('\n'),
    "文案不得自带 CRLF: {msg:?}"
  );
  let mut out = Vec::new();
  write_error_raw(&mut out, msg);
  assert_eq!(out, expected, "错误帧逐字节不符");
  assert_eq!(out[0], b'-', "帧首负号");
  assert_ne!(out[1], b'-', "帧首仅一枚负号（锁单 `-`，杜绝 `--ERR`）");
  assert!(out[1..].starts_with(b"ERR "), "负载以 ERR 起");
  assert!(
    out.ends_with(b"\r\n") && out[out.len() - 3] != b'\r',
    "单 \\r\\n 收尾"
  );
  // 简单错误帧必须单行闭合：负号外不再含裸 CR/LF
  assert_eq!(
    out.iter().filter(|b| **b == b'\r').count(),
    1,
    "帧内仅一处 CR"
  );
}

/// FLUSHALL UNSAFETRUNCATELOG 非 0 租户门禁拒绝臂（slow.rs:flush_command_slow）
#[test]
fn flush_truncate_log_ns_gate_arm_frames_single_minus_err() {
  lock_err_frame(
    RESP_ERR_FLUSH_TRUNCATE_LOG_NS0,
    b"-ERR permission denied: only namespace 0 can truncate log\r\n",
  );
}

/// FLUSH 族污染拒绝臂（slow.rs:flush_command_slow）
#[test]
fn flush_contaminated_arm_frames_single_minus_err() {
  lock_err_frame(
    RESP_ERR_DEVICE_CONTAMINATED_REFUSING_FLUSH,
    b"-ERR device is contaminated by a failed checkpoint receive, refusing flush\r\n",
  );
}

/// SAVE/BGSAVE 污染拒绝臂（slow.rs:checkpoint_command_slow）
#[test]
fn checkpoint_contaminated_arm_frames_single_minus_err() {
  lock_err_frame(
    RESP_ERR_DEVICE_CONTAMINATED_REFUSING_CHECKPOINT,
    b"-ERR device is contaminated by a failed checkpoint receive, refusing checkpoint\r\n",
  );
}
