/// 默认并发写入在途追踪槽位容量
const DEFAULT_INFLIGHT_SLOTS: usize = 256;

/// 默认内存环形写缓冲区容量（16MB）
const DEFAULT_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// WAL 引擎配置参数
///
/// 扇区物理对齐大小不在此配置：对齐口径以底层设备 `Device::sector_size()`
/// 为单一真源（对标 C# TsavoriteLog 由设备决定扇区大小），避免配置与设备
/// 不一致导致 `write_aligned` 对齐校验失败
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalConfig {
  /// 内存环形写缓冲区容量（字节，须为设备扇区大小的整数倍，默认 16MB）
  pub buffer_size: usize,
  /// 并发写入在途追踪槽位容量（默认 256）
  pub inflight_slots: usize,
}

impl Default for WalConfig {
  fn default() -> Self {
    Self {
      buffer_size: DEFAULT_BUFFER_SIZE,
      inflight_slots: DEFAULT_INFLIGHT_SLOTS,
    }
  }
}

impl WalConfig {
  /// 创建指定容量的配置
  pub const fn new(buffer_size: usize) -> Self {
    Self {
      buffer_size,
      inflight_slots: DEFAULT_INFLIGHT_SLOTS,
    }
  }
}
