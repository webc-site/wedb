//! 单物理日志封装（对标 libs/server/AOF/SingleLog.cs:SingleLog）

use std::sync::Arc;

use waof::AofAddress;

use super::waof_sublog::AofSublog;

/// libs/server/AOF/SingleLog.cs:SingleLog
///
/// 单个底层日志实例的包装，成员集与 C# 一致：7 个地址属性 + RecoverAsync/Reset。
/// 其余操作（Enqueue / Commit / SafeInitialize / Initialize / Scan / Truncate）由
/// GarnetLog 经 `log` 字段直达后端，对标 C# 的 `singleLog.log.X`
/// （GarnetLog.cs:427 `singleLog.log.SafeInitialize`、:462 `singleLog.log.Initialize`、
/// :337 `singleLog.log.Scan`）。
pub struct SingleLog {
  /// 底层日志后端（对标 C# TsavoriteLog log）
  pub log: Arc<AofSublog>,
}

impl SingleLog {
  /// libs/server/AOF/SingleLog.cs:SingleLog
  pub fn new(log: Arc<AofSublog>) -> Self {
    Self { log }
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
  ///
  /// 已提交 begin 快照（底层 TsavoriteLog.CommittedBeginAddress，
  /// TsavoriteLog.cs:120；非实时 begin）。
  #[inline]
  pub fn committed_begin_address(&self) -> AofAddress {
    AofAddress::create(1, self.log.committed_begin_address())
  }

  /// libs/server/AOF/SingleLog.cs:FlushedUntilAddress
  #[inline]
  pub fn flushed_until_address(&self) -> AofAddress {
    AofAddress::create(1, self.log.flushed_until_address())
  }

  /// libs/server/AOF/SingleLog.cs:MaxMemorySizeBytes
  #[inline]
  pub fn max_memory_size_bytes(&self) -> AofAddress {
    AofAddress::create(1, self.log.max_memory_size_bytes())
  }

  /// libs/server/AOF/SingleLog.cs:MemorySizeBytes
  #[inline]
  pub fn memory_size_bytes(&self) -> AofAddress {
    AofAddress::create(1, self.log.memory_size_bytes())
  }

  /// libs/server/AOF/SingleLog.cs:RecoverAsync
  ///
  /// 设备面异常沿 ValueTask 透明上抛（C# TsavoriteLog.cs:623 RecoverAsync），
  /// 本口不吞错。C# 侧续行门控 FailOnRecoveryError 生效默认关（catch 吞错续行）；
  /// rust 侧该旗标零代码消费、回放驱动无此门，错误沿链 `?` 直上恒拒启
  ///（刻意收紧，见 deviations.md §122）。
  #[inline]
  pub async fn recover_async(&self) -> waof::Result<()> {
    self.log.recover_async().await
  }

  /// libs/server/AOF/SingleLog.cs:Reset
  #[inline]
  pub async fn reset_async(&self) {
    self.log.reset_async().await;
  }
}
