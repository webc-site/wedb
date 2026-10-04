//! 范围索引集群迁移活动追踪（对标 libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs）
//!
//! 1:1 对标 C# 三类迁移活动：
//! - `MigrateActivity`：追踪 `MigrateRangeIndexKeysAsync` 批量迁移
//! - `TransmitActivity`：追踪 `TransmitRangeIndexAsync` 单键分块发送
//! - `ReceiveActivity`：追踪 `RangeIndexMigrationReceiveState` 单键分块流式重组与发布
//!
//! 区间计时一律取 [`wbase::time::now_stopwatch_ticks`]（100ns 单调刻度域，
//! 对标 C# `Stopwatch.GetTimestamp`），与 C# 日志 totalTicks 同域同量纲，
//! 实时钟回拨免疫；字段 0 值即未初始化哨兵（单调域读数恒 > 0）。

use std::path::{Path, PathBuf};

use wbase::time::now_stopwatch_ticks;

use super::range_index_manager_migration::PublishMigratedIndexResult;

/// 批量范围索引键迁移活动追踪
/// （对标 libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:MigrateActivity）
#[derive(Debug)]
pub struct MigrateActivity {
  pub started_ticks: u64,
  pub transmitting_ticks: u64,
  pub deleting_ticks: u64,
  pub ended_ticks: u64,
  pub key_count: usize,
  pub error: Option<String>,
}

impl MigrateActivity {
  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:StartActivity
  pub fn start_activity(key_count: usize) -> Self {
    Self {
      started_ticks: now_stopwatch_ticks(),
      transmitting_ticks: 0,
      deleting_ticks: 0,
      ended_ticks: 0,
      key_count,
      error: None,
    }
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:OnTransmitting
  pub fn on_transmitting(&mut self) {
    self.transmitting_ticks = now_stopwatch_ticks();
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:OnDeleting
  pub fn on_deleting(&mut self) {
    self.deleting_ticks = now_stopwatch_ticks();
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:OnError
  pub fn on_error(&mut self, error: &str) {
    self.error.get_or_insert_with(|| error.to_string());
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:End
  pub fn end(&mut self) {
    self.ended_ticks = now_stopwatch_ticks();
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:LogActivity
  pub fn log_activity(&self) {
    // saturating_sub 系单调域下的纵深防御（同域作差恒非负）
    let total_ticks = self.ended_ticks.saturating_sub(self.started_ticks);
    let wait_transmitting_ticks = if self.transmitting_ticks > 0 {
      self.transmitting_ticks.saturating_sub(self.started_ticks) as i64
    } else {
      -1
    };
    let transmitting_ticks = if self.transmitting_ticks > 0 && self.deleting_ticks > 0 {
      self.deleting_ticks.saturating_sub(self.transmitting_ticks) as i64
    } else {
      -1
    };
    let deleting_ticks = if self.deleting_ticks > 0 && self.ended_ticks > 0 {
      self.ended_ticks.saturating_sub(self.deleting_ticks) as i64
    } else {
      -1
    };
    log::info!(
      "MigrateRangeIndexKeysAsync: keyCount={key_count} isError={is_error} errorStr={error} totalTicks={total_ticks} waitTransmittingTicks={wait_transmitting_ticks} transmittingTicks={transmitting_ticks} deletingTicks={deleting_ticks}",
      key_count = self.key_count,
      is_error = self.error.is_some(),
      error = self.error.as_deref().unwrap_or(""),
    );
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:EndAndLogActivity
  pub fn end_and_log_activity(&mut self) {
    self.end();
    self.log_activity();
  }
}

/// 单键范围索引快照与传输活动追踪
/// （对标 libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:TransmitActivity）
#[derive(Debug)]
pub struct TransmitActivity {
  pub started_ticks: u64,
  pub ended_ticks: u64,
  pub total_bytes_sent: usize,
  pub file_size_bytes: i64,
  pub snapshot_ticks: u64,
  pub error: Option<String>,
}

impl TransmitActivity {
  pub fn start_activity() -> Self {
    Self {
      started_ticks: now_stopwatch_ticks(),
      ended_ticks: 0,
      total_bytes_sent: 0,
      file_size_bytes: 0,
      snapshot_ticks: 0,
      error: None,
    }
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:OnSnapshotCompleted
  pub fn on_snapshot_completed(&mut self, file_size_bytes: i64) {
    self.snapshot_ticks = now_stopwatch_ticks().saturating_sub(self.started_ticks);
    self.file_size_bytes = file_size_bytes;
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:OnChunkSent
  pub fn on_chunk_sent(&mut self, bytes_sent: usize) {
    self.total_bytes_sent += bytes_sent;
  }

  pub fn on_error(&mut self, error: &str) {
    self.error.get_or_insert_with(|| error.to_string());
  }

  pub fn end(&mut self) {
    self.ended_ticks = now_stopwatch_ticks();
  }

  pub fn log_activity(&self, key: &[u8]) {
    let total_ticks = self.ended_ticks.saturating_sub(self.started_ticks);
    let transmit_ticks = total_ticks.saturating_sub(self.snapshot_ticks);
    log::info!(
      "TransmitRangeIndexAsync: key={key} isError={is_error} errorStr={error} fileSizeBytes={file_size_bytes} totalBytesSent={total_bytes_sent} snapshotTicks={snapshot_ticks} transmitTicks={transmit_ticks} totalTicks={total_ticks}",
      key = String::from_utf8_lossy(key),
      is_error = self.error.is_some(),
      error = self.error.as_deref().unwrap_or(""),
      file_size_bytes = self.file_size_bytes,
      total_bytes_sent = self.total_bytes_sent,
      snapshot_ticks = self.snapshot_ticks,
    );
  }

  pub fn end_and_log_activity(&mut self, key: &[u8]) {
    self.end();
    self.log_activity(key);
  }
}

/// 范围索引接收与重组活动追踪
/// （对标 libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:ReceiveActivity）
#[derive(Debug)]
pub struct ReceiveActivity {
  pub started_ticks: u64,
  pub file_path: PathBuf,
  pub publish_ticks: u64,
  pub ended_ticks: u64,
  pub chunk_count: usize,
  pub total_bytes_received: usize,
  pub error: Option<String>,
  pub session_disposed: bool,
  pub publish_result: Option<PublishMigratedIndexResult>,
}

impl ReceiveActivity {
  pub fn start_activity(migrated_file_path: &Path) -> Self {
    Self {
      started_ticks: now_stopwatch_ticks(),
      file_path: migrated_file_path.to_path_buf(),
      publish_ticks: 0,
      ended_ticks: 0,
      chunk_count: 0,
      total_bytes_received: 0,
      error: None,
      session_disposed: false,
      publish_result: None,
    }
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:OnChunkReceived
  pub fn on_chunk_received(&mut self, chunk_length: usize) {
    self.chunk_count += 1;
    self.total_bytes_received += chunk_length;
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:OnPublishing
  pub fn on_publishing(&mut self) {
    self.publish_ticks = now_stopwatch_ticks();
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:OnPublishResult
  pub fn on_publish_result(&mut self, result: PublishMigratedIndexResult) {
    self.publish_result = Some(result);
  }

  pub fn on_error(&mut self, error: &str) {
    self.error.get_or_insert_with(|| error.to_string());
  }

  /// libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs:OnSessionDisposed
  pub fn on_session_disposed(&mut self) {
    self.session_disposed = true;
  }

  pub fn end(&mut self) {
    self.ended_ticks = now_stopwatch_ticks();
  }

  pub fn log_activity(&self, key: &[u8]) {
    let total_ticks = self.ended_ticks.saturating_sub(self.started_ticks);
    let publish_ticks = if self.publish_ticks > 0 {
      self.ended_ticks.saturating_sub(self.publish_ticks) as i64
    } else {
      -1
    };
    let key_str = if key.is_empty() {
      "null".into()
    } else {
      String::from_utf8_lossy(key)
    };
    log::info!(
      "RangeIndexMigrationReceive: key={key_str} filePath={file_path} isError={is_error} errorStr={error} sessionDisposed={session_disposed} publishResult={publish_result} chunkCount={chunk_count} totalBytesReceived={total_bytes_received} publishTicks={publish_ticks} totalTicks={total_ticks}",
      key_str = key_str,
      file_path = self.file_path.display(),
      is_error = self.error.is_some(),
      error = self.error.as_deref().unwrap_or(""),
      session_disposed = self.session_disposed,
      publish_result = self
        .publish_result
        .map(|r| r.to_string())
        .unwrap_or_else(|| "n/a".to_string()),
      chunk_count = self.chunk_count,
      total_bytes_received = self.total_bytes_received,
    );
  }

  pub fn end_and_log_activity(&mut self, key: &[u8]) {
    self.end();
    self.log_activity(key);
  }
}
