//! 范围索引 AOF 复制活动追踪（对标 libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs）
//!
//! C# 以 `Stopwatch.GetTimestamp` + `ILogger` 记录一次流式发送 / 一次流重组的
//! 全程度量；Rust 侧以 `wbase::time::now_stopwatch_ticks`（100ns 单调刻度域，
//! 对标 C# Stopwatch 同域同单位）+ `log` 门面承接，字段为普通值语义
//! （活动实例不跨线程共享，由持有方单线程驱动）。

use wbase::time::now_stopwatch_ticks;

use super::range_index_manager_migration::PublishMigratedIndexResult;

/// 流式发送活动（对标 libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs:StreamActivity）
///
/// 追踪把一份迁移索引快照文件分块灌入 AOF 的全过程：文件长度、块计数、
/// 累计入队字节、首个错误与总耗时。
#[derive(Debug)]
pub struct StreamActivity {
  /// 起始单调刻度（100ns 域，Stopwatch 对标）
  pub started_ticks: u64,
  /// 目标分块大小（字节）
  pub chunk_size: usize,
  /// 快照文件长度（字节）
  pub file_size_bytes: i64,
  /// 已入队块数
  pub chunk_count: usize,
  /// 已入队总字节
  pub total_bytes_enqueued: i64,
  /// 首个错误（C# `??=` 语义：首错固化，后续覆盖无效）
  pub error: Option<String>,
}

impl StreamActivity {
  /// libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs:StartActivity
  pub fn start_activity(chunk_size: usize) -> Self {
    Self {
      started_ticks: now_stopwatch_ticks(),
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
    let total_ticks = now_stopwatch_ticks().saturating_sub(self.started_ticks);
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
#[derive(Debug)]
pub struct ReassemblyActivity {
  /// 起始单调刻度（100ns 域，Stopwatch 对标）
  pub started_ticks: u64,
  /// 已接收块数
  pub chunk_count: usize,
  /// 已接收总字节
  pub total_bytes_received: i64,
  /// 发布结果（流完成时记录）
  pub publish_result: Option<PublishMigratedIndexResult>,
}

impl ReassemblyActivity {
  /// ReassemblyActivity 的 StartActivity 入口
  pub fn start_activity() -> Self {
    Self {
      started_ticks: now_stopwatch_ticks(),
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
    let total_ticks = now_stopwatch_ticks().saturating_sub(self.started_ticks);
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
