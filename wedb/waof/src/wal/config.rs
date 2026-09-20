/// 默认并发写入在途追踪槽位容量
const DEFAULT_INFLIGHT_SLOTS: usize = 256;

/// 默认内存环形写缓冲区容量（16MB）
const DEFAULT_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// 提交同步策略位（对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/
/// TsavoriteLogSettings.cs:AutoCommit 的提交持久性显式配置面：C# 以
/// `AutoCommit` 布尔决定记录插入是否自动提交落盘，本实现提交频率由组提交
/// 流水线承担（Leader/Follower 合批同步），收敛为按提交批次选择是否等待
/// 设备 `sync_data` 落盘）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
  /// 每次提交批次写设备后无条件 `sync_data`（默认）：commit 返回即持久落盘，
  /// 对标 C# CommitAsync 完成语义
  Always,
  /// 提交仅写设备（页缓存），跳过 `sync_data`：commit 返回 = 已提交至设备，
  /// 掉电窗口由后续 `Always` 提交或宿主显式同步兜底（`sync_data` 为设备级
  /// 全量同步，任意一次 Always 批次即补齐此前 Deferred 批次的落盘）
  Deferred,
}

/// WAL 引擎配置参数
///
/// 扇区物理对齐大小不在此配置：对齐口径以底层设备 `Device::sector_size()`
/// 为单一真源（对标 C# TsavoriteLog 由设备决定扇区大小），避免配置与设备
/// 不一致导致 `write_aligned` 对齐校验失败
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalConfig {
  /// 内存环形写缓冲区容量（字节，须为设备扇区大小的整数倍，默认 16MB；对标
  /// C# TsavoriteLogSettings 的 MemorySizeBits —— 日志常驻内存窗口总量）
  pub buffer_size: usize,
  /// 日志页容量（字节；对标 C# TsavoriteLogSettings 的 PageSizeBits —— 单条非
  /// 分块记录须容于一页，分块记录的片上界即页容量减头部，AOF 域经
  /// `WaofSublog::log_page_size_bits` 取此值定块。缺省与 [`Self::buffer_size`]
  /// 同值：窗口即单页，与引入本页维度之前的口径逐字节一致）
  pub page_size: usize,
  /// 并发写入在途追踪槽位容量（默认 256）
  pub inflight_slots: usize,
  /// 提交同步策略位（默认 [`FsyncPolicy::Always`]）
  pub fsync: FsyncPolicy,
}

impl Default for WalConfig {
  fn default() -> Self {
    Self {
      buffer_size: DEFAULT_BUFFER_SIZE,
      page_size: DEFAULT_BUFFER_SIZE,
      inflight_slots: DEFAULT_INFLIGHT_SLOTS,
      fsync: FsyncPolicy::Always,
    }
  }
}

impl WalConfig {
  /// 创建指定容量的配置（页容量随窗口同值，同步策略取 [`FsyncPolicy::Always`]
  /// 默认档）
  pub const fn new(buffer_size: usize) -> Self {
    Self {
      buffer_size,
      page_size: buffer_size,
      inflight_slots: DEFAULT_INFLIGHT_SLOTS,
      fsync: FsyncPolicy::Always,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 默认与构造入口均取 Always 持久档（对标 C# AutoCommit=false 的默认保守语义）
  #[test]
  fn default_fsync_is_always() {
    assert_eq!(WalConfig::default().fsync, FsyncPolicy::Always);
    assert_eq!(WalConfig::new(1024).fsync, FsyncPolicy::Always);
  }
}
