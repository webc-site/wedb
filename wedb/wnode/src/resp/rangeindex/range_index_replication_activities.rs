//! 范围索引 AOF 复制活动追踪（对标 libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs）
//!
//! C# 以 `Stopwatch.GetTimestamp` + `ILogger` 记录一次流式发送 / 一次流重组的
//! 全程度量；Rust 侧以 coarsetime 粗粒度时钟 + `log` 门面承接，字段为普通
//! 值语义（活动实例不跨线程共享，由持有方单线程驱动）。

use coarsetime::Clock;

use super::range_index_manager_migration::PublishMigratedIndexResult;

/// 纳秒时间戳（coarsetime 粗粒度时钟，活动计时足够）
#[inline]
fn now_ns() -> u64 {
  Clock::now_since_epoch().as_u64()
}

/// 流式发送活动（对标 libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs:StreamActivity）
///
/// 追踪把一份迁移索引快照文件分块灌入 AOF 的全过程：文件长度、块计数、
/// 累计入队字节、首个错误与总耗时。
#[derive(Debug, Clone)]
pub struct StreamActivity {
  /// 起始纳秒时间戳
  started_ns: u64,
  /// 目标分块大小（字节）
  chunk_size: usize,
  /// 快照文件长度（字节）
  file_size_bytes: i64,
  /// 已入队块数
  chunk_count: usize,
  /// 已入队总字节
  total_bytes_enqueued: i64,
  /// 首个错误（C# `??=` 语义：首错固化，后续覆盖无效）
  error: Option<String>,
}

impl StreamActivity {
  /// libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs:StartActivity
  pub fn start_activity(chunk_size: usize) -> Self {
    Self {
      started_ns: now_ns(),
      chunk_size,
      file_size_bytes: 0,
      chunk_count: 0,
      total_bytes_enqueued: 0,
      error: None,
    }
  }

  /// libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs:OnFileLength
  pub fn on_file_length(&mut self, file_bytes: i64) {
    self.file_size_bytes = file_bytes;
  }

  /// libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs:OnChunkEnqueued
  pub fn on_chunk_enqueued(&mut self, bytes: usize) {
    self.chunk_count += 1;
    self.total_bytes_enqueued += bytes as i64;
  }

  /// libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs:OnError
  ///
  /// 仅首个错误生效（C# `this.error ??= error`）
  pub fn on_error(&mut self, error: &str) {
    self.error.get_or_insert_with(|| error.to_string());
  }

  /// libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs:EndAndLog
  ///
  /// 成功与异常路径均须调用一次；以 info 级输出全程度量
  pub fn end_and_log(&self, key: &[u8]) {
    let total_ticks = now_ns().saturating_sub(self.started_ns);
    log::info!(
      "RangeIndexReplicationStreamActivity: key={key} isError={is_error} errorStr={error} chunkSize={chunk_size} fileSizeBytes={file_size_bytes} chunkCount={chunk_count} totalBytesEnqueued={total_bytes_enqueued} totalTicks={total_ticks}",
      key = String::from_utf8_lossy(key),
      is_error = self.error.is_some(),
      error = self.error.as_deref().unwrap_or(""),
      chunk_size = self.chunk_size,
      file_size_bytes = self.file_size_bytes,
      chunk_count = self.chunk_count,
      total_bytes_enqueued = self.total_bytes_enqueued,
      total_ticks = total_ticks,
    );
  }
}

/// 流重组活动（对标 libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs:ReassemblyActivity）
///
/// 追踪副本 / 恢复侧重按 AOF 流块重组一份迁移索引的全过程：块计数、累计
/// 接收字节、发布结果与总耗时。
#[derive(Debug, Clone)]
pub struct ReassemblyActivity {
  /// 起始纳秒时间戳
  started_ns: u64,
  /// 已接收块数
  chunk_count: usize,
  /// 已接收总字节
  total_bytes_received: i64,
  /// 发布结果（流完成时记录）
  publish_result: Option<PublishMigratedIndexResult>,
}

impl ReassemblyActivity {
  /// ReassemblyActivity 的 StartActivity 入口
  pub fn start_activity() -> Self {
    Self {
      started_ns: now_ns(),
      chunk_count: 0,
      total_bytes_received: 0,
      publish_result: None,
    }
  }

  /// libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs:OnChunkReceived
  pub fn on_chunk_received(&mut self, chunk_length: usize) {
    self.chunk_count += 1;
    self.total_bytes_received += chunk_length as i64;
  }

  /// libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs:OnPublishResult
  pub fn on_publish_result(&mut self, result: PublishMigratedIndexResult) {
    self.publish_result = Some(result);
  }

  /// ReassemblyActivity 的 EndAndLog 入口
  ///
  /// `reason` 标注结束原因（Complete / PublishFailed / ChunkProcessingError 等）
  pub fn end_and_log(&self, key: &[u8], reason: &str) {
    let total_ticks = now_ns().saturating_sub(self.started_ns);
    let publish_result_text = self
      .publish_result
      .as_ref()
      .map_or("n/a".to_string(), |r| r.to_string());
    log::info!(
      "RangeIndexReplicationReassemblyActivity: key={key} reason={reason} publishResult={publish_result} chunkCount={chunk_count} totalBytesReceived={total_bytes_received} totalTicks={total_ticks}",
      key = String::from_utf8_lossy(key),
      reason = reason,
      publish_result = publish_result_text,
      chunk_count = self.chunk_count,
      total_bytes_received = self.total_bytes_received,
      total_ticks = total_ticks,
    );
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn stream_activity_counters_and_first_error_wins() {
    let mut a = StreamActivity::start_activity(4096);
    a.on_file_length(100_000);
    a.on_chunk_enqueued(4096);
    a.on_chunk_enqueued(2048);
    assert_eq!(a.chunk_count, 2);
    assert_eq!(a.total_bytes_enqueued, 6144);

    a.on_error("ZeroLengthChunkFromReader");
    a.on_error("SecondErrorIgnored");
    // C# ??= 语义：首错固化
    assert_eq!(a.chunk_size, 4096);
    assert_eq!(a.error.as_deref(), Some("ZeroLengthChunkFromReader"));

    // 结束日志不得 panic
    a.end_and_log(b"idx-key");
  }

  #[test]
  fn stream_activity_success_path_logs() {
    let mut a = StreamActivity::start_activity(1024);
    a.on_file_length(2048);
    a.on_chunk_enqueued(1024);
    a.on_chunk_enqueued(1024);
    assert!(a.error.is_none());
    a.end_and_log(b"single-chunk");
  }

  #[test]
  fn reassembly_activity_counters_and_publish_result() {
    let mut a = ReassemblyActivity::start_activity();
    a.on_chunk_received(512);
    a.on_chunk_received(512);
    a.on_chunk_received(47);
    assert_eq!(a.chunk_count, 3);
    assert_eq!(a.total_bytes_received, 1071);

    // 完成前结束日志：发布结果显示 n/a
    a.end_and_log(b"k", "ChunkProcessingError");

    a.on_publish_result(PublishMigratedIndexResult::Success);
    a.end_and_log(b"k", "Complete");
  }
}
