//! 单物理日志封装（对标 libs/server/AOF/SingleLog.cs:SingleLog）

use std::sync::Arc;

use waof::AofAddress;

use super::sublog::Sublog;

/// 提供单个底层日志实例的包装，暴露核心地址与管理操作（对标 libs/server/AOF/SingleLog.cs:SingleLog）。
pub struct SingleLog {
  /// 底层日志后端（对标 C# TsavoriteLog log）
  pub log: Arc<Sublog>,
}

impl SingleLog {
  /// libs/server/AOF/SingleLog.cs:SingleLog
  pub fn new(log: Arc<Sublog>) -> Self {
    Self { log }
  }

  /// libs/server/AOF/SingleLog.cs:HeaderSize
  #[inline]
  pub fn header_size(&self) -> i64 {
    24
  }

  /// libs/server/AOF/SingleLog.cs:BeginAddress
  #[inline]
  pub fn begin_address(&self) -> AofAddress {
    AofAddress::create(1, self.log.begin_address())
  }

  /// libs/server/AOF/SingleLog.cs:TailAddress
  #[inline]
  pub fn tail_address(&self) -> AofAddress {
    AofAddress::create(1, self.log.tail_address())
  }

  /// libs/server/AOF/SingleLog.cs:CommittedUntilAddress
  #[inline]
  pub fn committed_until_address(&self) -> AofAddress {
    AofAddress::create(1, self.log.committed_until_address())
  }

  /// libs/server/AOF/SingleLog.cs:CommittedBeginAddress
  #[inline]
  pub fn committed_begin_address(&self) -> AofAddress {
    AofAddress::create(1, self.log.begin_address())
  }

  /// libs/server/AOF/SingleLog.cs:FlushedUntilAddress
  #[inline]
  pub fn flushed_until_address(&self) -> AofAddress {
    AofAddress::create(1, self.log.flushed_until_address())
  }

  /// libs/server/AOF/SingleLog.cs:MaxMemorySizeBytes
  #[inline]
  pub fn max_memory_size_bytes(&self) -> AofAddress {
    AofAddress::create(1, self.log.memory_size_bytes())
  }

  /// libs/server/AOF/SingleLog.cs:MemorySizeBytes
  #[inline]
  pub fn memory_size_bytes(&self) -> AofAddress {
    AofAddress::create(1, self.log.memory_size_bytes())
  }

  /// libs/server/AOF/SingleLog.cs:RecoverAsync
  #[inline]
  pub async fn recover_async(&self) {
    self.log.recover_async().await;
  }

  /// libs/server/AOF/SingleLog.cs:Reset
  #[inline]
  pub fn reset(&self) {
    self.log.reset();
  }

  /// libs/server/AOF/SingleLog.cs:Initialize
  #[inline]
  pub fn initialize(&self, begin_address: i64, committed_until_address: i64, last_commit_num: i64) {
    self
      .log
      .safe_initialize(begin_address, committed_until_address, last_commit_num);
  }

  /// libs/server/AOF/SingleLog.cs:SafeInitialize
  #[inline]
  pub fn safe_initialize(
    &self,
    begin_address: i64,
    committed_until_address: i64,
    last_commit_num: i64,
  ) {
    self
      .log
      .safe_initialize(begin_address, committed_until_address, last_commit_num);
  }

  #[inline]
  pub fn log(&self) -> &Arc<Sublog> {
    &self.log
  }
}
