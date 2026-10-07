//! 假端点逐帧泵单源（读缓冲累积 → RESP2 帧切分 → 逐帧回调）
//!
//! 收口 appendlog_reject_disconnect / cluster_migration /
//! diskless_replica_sync_session / failover_primary_probe /
//! replica_partial_resync_truncation_pin / replication_snapshot_reader_pin /
//! reviv_pause_migration_interleave / scripted_migrate_target 各册逐字同形
//! 的 TCP 假端点读循环骨架：读块累积进缓冲、按完整 RESP2 数组帧切分、逐帧
//! 交回调裁决应答。差异面（读块尺寸、应答编排）以参数与回调暴露；外层
//! accept / spawn 外壳与每连接状态由各册自持。消费面经
//! `wedb_test::fake_frame_pump` 引用；帧解析单源见
//! `wtest_base::parse_frame_slices`。
//!

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
};
use wtest_base::parse_frame_slices;

/// 逐帧泵：读尽即返回（连接收口）；回调写败同样收口（拒收断连语义）
///
/// - `buf_size`：读块尺寸（各册原值保留：4096 / 65536 / 8192）
/// - `on_frame`：帧裁决回调，入参（帧原文, 参数切片）；返回 `Some(reply)`
///   即回写续泵，`None` 静默不答（目标挂起语义）；drain 在回调之后，帧原文
///   与参数在回调期内均有效
pub async fn pump_frames(
  stream: &mut TcpStream,
  buf_size: usize,
  mut on_frame: impl AsyncFnMut(&[u8], &[&[u8]]) -> Option<Vec<u8>>,
) {
  let mut acc: Vec<u8> = Vec::new();
  let mut buf = vec![0u8; buf_size];
  loop {
    let BufResult(res, next) = stream.read(buf).await;
    buf = next;
    let n = match res {
      Ok(n) if n > 0 => n,
      _ => break,
    };
    acc.extend_from_slice(&buf[..n]);
    while let Some((frame_len, args)) = parse_frame_slices(&acc) {
      let frame = &acc[..frame_len];
      if let Some(reply) = on_frame(frame, &args).await {
        acc.drain(..frame_len);
        if stream.write_all(reply).await.is_err() {
          return;
        }
      } else {
        acc.drain(..frame_len);
      }
    }
  }
}
