//! 存储设备配置与选项（对标 Tsavorite.core DeviceOptions.cs）

use serde::{Deserialize, Serialize};

/// 存储设备类型（对标 Tsavorite.core.DeviceType）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum DeviceType {
  /// 本地文件存储设备
  #[default]
  LocalStorage = 0,
  /// 原生异步 IO 设备（libaio / io_uring）
  Native = 1,
  /// 纯内存模拟设备
  LocalMemory = 2,
  /// 分片存储设备
  Sharded = 3,
  /// 空设备（NullDevice）
  Null = 4,
}

/// Linux 原生异步 IO 后端模式（对标 NativeStorageDevice.IoBackend）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum IoBackend {
  /// 系统默认探测
  #[default]
  Default = 0,
  /// Linux libaio 后端
  LibAio = 1,
  /// Linux io_uring 高性能后端
  IoUring = 2,
}

/// Linux 原生设备后端调优选项（对标 Tsavorite.core.NativeDeviceOptions）
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NativeDeviceOptions {
  /// IO 后端模式
  pub io_backend: IoBackend,
  /// 独立环数 / 上下文数（0 为自适应）
  pub num_io_contexts: i32,
  /// 每环内核提交队列深度（0 为默认）
  pub queue_depth: i32,
  /// io_uring SQPOLL 内核轮询模式
  pub uring_sq_poll: bool,
  /// io_uring SQPOLL 轮询线程空闲毫秒
  pub uring_sq_poll_idle_ms: i32,
}

/// 纯内存设备调优选项（对标 Tsavorite.core.LocalMemoryDeviceOptions）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalMemoryDeviceOptions {
  /// 段大小（字节，默认 1GB）
  pub segment_size: i64,
  /// 环容量（0 为默认）
  pub ring_capacity: i32,
}

impl Default for LocalMemoryDeviceOptions {
  fn default() -> Self {
    Self {
      segment_size: 1i64 << 30,
      ring_capacity: 0,
    }
  }
}

/// 设备配置错误
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeviceConfigError {
  #[error("Segment size must be a positive power of 2, got {0}")]
  InvalidSegmentSize(i64),
  #[error("Capacity must be positive, got {0}")]
  InvalidCapacity(i64),
}

/// 统一存储设备选项
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceOptions {
  /// 设备类型
  pub device_type: DeviceType,
  /// 设备预设容量（字节，-1 表示未限制）
  pub capacity: i64,
  /// 关闭时是否删除底层文件
  pub delete_on_close: bool,
  /// 是否预分配文件空间
  pub preallocate_file: bool,
  /// 是否启用故障恢复
  pub recover_device: bool,
  /// 原生设备专用配置
  pub native_options: Option<NativeDeviceOptions>,
  /// 内存设备专用配置
  pub local_memory_options: Option<LocalMemoryDeviceOptions>,
}

impl Default for DeviceOptions {
  fn default() -> Self {
    Self {
      device_type: DeviceType::LocalStorage,
      capacity: -1,
      delete_on_close: false,
      preallocate_file: false,
      recover_device: true,
      native_options: None,
      local_memory_options: None,
    }
  }
}

impl DeviceOptions {
  /// 校验设备配置合法性
  pub fn validate(&self) -> Result<(), DeviceConfigError> {
    if self.capacity != -1 && self.capacity <= 0 {
      return Err(DeviceConfigError::InvalidCapacity(self.capacity));
    }
    if let Some(ref mem) = self.local_memory_options
      && (mem.segment_size <= 0 || !(mem.segment_size as u64).is_power_of_two())
    {
      return Err(DeviceConfigError::InvalidSegmentSize(mem.segment_size));
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_device_options_default() {
    let opts = DeviceOptions::default();
    assert_eq!(opts.device_type, DeviceType::LocalStorage);
    assert_eq!(opts.capacity, -1);
    assert!(opts.recover_device);
    assert!(opts.validate().is_ok());
  }

  #[test]
  fn test_local_memory_options_validation() {
    let mut opts = DeviceOptions {
      local_memory_options: Some(LocalMemoryDeviceOptions {
        segment_size: 1024,
        ring_capacity: 0,
      }),
      ..Default::default()
    };
    assert!(opts.validate().is_ok());

    // 非 2 的幂
    opts.local_memory_options = Some(LocalMemoryDeviceOptions {
      segment_size: 1000,
      ring_capacity: 0,
    });
    assert!(matches!(
      opts.validate().unwrap_err(),
      DeviceConfigError::InvalidSegmentSize(1000)
    ));
  }
}
