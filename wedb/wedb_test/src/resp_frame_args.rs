//! RESP2 数组帧解析单源（借用切片形态：帧总字节数 + 参数切片视图）
//!
//! 收口 cluster_migration / cluster_migration_domain / diskless_replica_sync_session /
//! migrate_deleting_unheld_claim / migrate_fail_inject / migrate_epoch_drain_failclose /
//! reviv_pause_migration_interleave 七册逐字同形的 `try_parse_frame_args`。
//! 消费面经 `wedb_test::resp_frame_args` 引用（原 common/ 直挂面已收口进本
//! crate），调用点零改动。

use std::str::from_utf8;

/// 解析缓冲中首个完整 RESP2 数组帧：返回 (帧总字节数, 全部参数切片)，
/// 不完整返回 None
pub fn try_parse_frame_args(buf: &[u8]) -> Option<(usize, Vec<&[u8]>)> {
  if buf.first() != Some(&b'*') {
    return None;
  }
  let header_end = buf.iter().position(|b| *b == b'\n')? + 1;
  let argc: usize = from_utf8(&buf[1..header_end - 2]).ok()?.parse().ok()?;
  let mut pos = header_end;
  let mut args = Vec::with_capacity(argc);
  for _ in 0..argc {
    if buf.get(pos) != Some(&b'$') {
      return None;
    }
    let len_line_end = buf[pos + 1..].iter().position(|b| *b == b'\n')? + pos + 2;
    let len: usize = from_utf8(&buf[pos + 1..len_line_end - 2])
      .ok()?
      .parse()
      .ok()?;
    let end = len_line_end + len;
    if end + 2 > buf.len() {
      return None;
    }
    args.push(&buf[len_line_end..end]);
    pos = end + 2;
  }
  Some((pos, args))
}
