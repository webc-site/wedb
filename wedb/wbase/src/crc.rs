//! 高吞吐 CRC32 校验和工具（硬件指令加速）
//!
//! 统一收敛存储引擎的 WAL 预写日志校验、Checkpoint 元数据封签与段完整性验证，
//! 杜绝上层模块散落手写整数标量字节切片更新逻辑。

/// 计算给定字节切片的 CRC32 校验和
#[inline(always)]
pub fn crc32(data: &[u8]) -> u32 {
  crc32fast::hash(data)
}

/// 流式 CRC32 摘要累加器
#[derive(Debug, Default, Clone)]
pub struct Crc32Hasher {
  hasher: crc32fast::Hasher,
}

impl Crc32Hasher {
  #[inline(always)]
  pub fn new() -> Self {
    Self {
      hasher: crc32fast::Hasher::new(),
    }
  }

  /// 喂入原始字节切片
  #[inline(always)]
  pub fn update(&mut self, bytes: &[u8]) {
    self.hasher.update(bytes);
  }

  /// 以小端字节序喂入 64 位整型（无堆分配）
  #[inline(always)]
  pub fn update_u64(&mut self, val: u64) {
    self.hasher.update(&val.to_le_bytes());
  }

  /// 以小端字节序喂入 32 位整型（无堆分配）
  #[inline(always)]
  pub fn update_u32(&mut self, val: u32) {
    self.hasher.update(&val.to_le_bytes());
  }

  /// 完成计算并返回 32 位校验码
  #[inline(always)]
  pub fn finalize(self) -> u32 {
    self.hasher.finalize()
  }
}
