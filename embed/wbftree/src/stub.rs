//! RangeIndex 存根结构 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:Index.cs 中 RangeIndexStub)
//!
//! 定长 35 字节二进制结构，存储于底层 Tsavorite / HybridLog 主存储日志中，记录 BfTree 配置与在线实例指针。

use crate::{
  error::{Error, Result},
  types::StorageBackendType,
};

/// 存根字节总长度 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:Size = 35)
pub const RANGE_INDEX_STUB_SIZE: usize = 35;

/// 字段偏移量 (小端定长布局)
const TREE_HANDLE_OFFSET: usize = 0;
const CACHE_SIZE_OFFSET: usize = 8;
const STORAGE_BACKEND_OFFSET: usize = 32;
const FLAGS_OFFSET: usize = 33;
const SERIALIZATION_PHASE_OFFSET: usize = 34;

/// 标志位掩码定义
const FLUSHED_BIT_MASK: u8 = 1 << 0;
const RECOVERED_BIT_MASK: u8 = 1 << 1;
const TRANSFERRED_BIT_MASK: u8 = 1 << 2;

/// 存储在底层存储引擎日志中的定长元数据存根 (35 字节二进制定长结构)
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub struct RangeIndexStub {
  /// 在线 BfTreeService 句柄裸指针 (8B)
  ///
  /// **仅作句柄标识，永不解引用，跨重启无效。**
  /// 本 crate 内部绝不通过该字段访问树实例——在线树一律经
  /// [`RangeIndexManager`](crate::RangeIndexManager) 的 `Arc<BfTreeService>`
  /// 注册表路由；该值由 [`BfTreeService::native_ptr`](crate::BfTreeService::native_ptr)
  /// 写入，仅用于同进程内判断「存根是否已绑定在线树」(0 = 未绑定)。与 C#
  /// RangeIndexStub.TreeHandle 的差异：C# 经该 nint 直接调用原生层，重启后由
  /// OnDiskRead 清零 (InvalidateStub)；本实现持久化后必然失效 (指针不复位)，
  /// 读侧见到非零值也只当「需查注册表」处理，绝不解引用。
  pub tree_handle: u64,
  /// 环形缓冲区大小 (8B)
  pub cache_size: u64,
  /// 最小记录大小 (4B)
  pub min_record_size: u32,
  /// 最大记录大小 (4B)
  pub max_record_size: u32,
  /// 最大键长度 (4B)
  pub max_key_len: u32,
  /// 叶子页面大小 (4B)
  pub leaf_page_size: u32,
  /// 存储后端 (1B: 0=Disk/Std, 1=Memory)
  pub storage_backend: u8,
  /// 标志位 (1B: Flushed, Recovered, Transferred)
  pub flags: u8,
  /// 检查点快照协调阶段号 (1B)
  pub serialization_phase: u8,
}

impl RangeIndexStub {
  /// 创建全新未刷盘的存根
  pub fn new(
    tree_handle: u64,
    cache_size: u64,
    min_record_size: u32,
    max_record_size: u32,
    max_key_len: u32,
    leaf_page_size: u32,
    storage_backend: impl Into<StorageBackendType>,
  ) -> Self {
    Self {
      tree_handle,
      cache_size,
      min_record_size,
      max_record_size,
      max_key_len,
      leaf_page_size,
      storage_backend: storage_backend.into().to_u8(),
      flags: 0,
      serialization_phase: 0,
    }
  }

  /// 检查是否已被刷入冷存储区
  #[inline]
  pub const fn is_flushed(&self) -> bool {
    (self.flags & FLUSHED_BIT_MASK) != 0
  }

  /// 设置刷盘标志位
  #[inline]
  pub fn set_flushed(&mut self, flushed: bool) {
    if flushed {
      self.flags |= FLUSHED_BIT_MASK;
    } else {
      self.flags &= !FLUSHED_BIT_MASK;
    }
  }

  /// 检查是否从快照文件恢复
  #[inline]
  pub const fn is_recovered(&self) -> bool {
    (self.flags & RECOVERED_BIT_MASK) != 0
  }

  /// 设置从快照恢复标志位
  #[inline]
  pub fn set_recovered(&mut self, recovered: bool) {
    if recovered {
      self.flags |= RECOVERED_BIT_MASK;
    } else {
      self.flags &= !RECOVERED_BIT_MASK;
    }
  }

  /// 检查所有权是否已转移到新记录
  #[inline]
  pub const fn is_transferred(&self) -> bool {
    (self.flags & TRANSFERRED_BIT_MASK) != 0
  }

  /// 设置所有权转移标志位
  #[inline]
  pub fn set_transferred(&mut self, transferred: bool) {
    if transferred {
      self.flags |= TRANSFERRED_BIT_MASK;
    } else {
      self.flags &= !TRANSFERRED_BIT_MASK;
    }
  }

  /// 重置所有标志位为 0 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ResetFlags)
  #[inline]
  pub fn reset_flags(&mut self) {
    self.flags = 0;
  }

  /// 清零树句柄裸指针 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ClearTreeHandle)
  #[inline]
  pub fn clear_tree_handle(&mut self) {
    self.tree_handle = 0;
  }

  /// 标记已从检查点恢复并清零句柄 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:MarkRecoveredFromCheckpoint)
  #[inline]
  pub fn mark_recovered_from_checkpoint(&mut self) {
    self.tree_handle = 0;
    self.set_recovered(true);
  }

  /// 重新激活恢复的句柄并清除恢复标记 (1:1 对标 Garnet RecreateIndex)
  ///
  /// 仅用于**同进程内**激活占位存根：把存根重新绑定到当前在线树的
  /// [`native_ptr`](crate::BfTreeService::native_ptr) 值并清除 Recovered 标志
  /// (对标 C# RecreateIndex 语义——清除 IsRecovered 使后续淘汰走刷盘快照而非
  /// 过期检查点快照)。跨重启的存根句柄已失效，不得传入旧持久化值；
  /// 新句柄同样仅作标识，永不解引用 (见 [`tree_handle`](Self::tree_handle) 文档)。
  #[inline]
  pub fn recreate_index(&mut self, new_tree_handle: u64) {
    self.tree_handle = new_tree_handle;
    self.set_recovered(false);
  }

  /// 编码为定长 35 字节切片 (零堆分配，4×64位字级展开与二进制打包，1:1 对标 Garnet 与存储引擎规范)
  #[inline(always)]
  pub const fn encode(&self) -> [u8; RANGE_INDEX_STUB_SIZE] {
    let [t0, t1, t2, t3, t4, t5, t6, t7] = self.tree_handle.to_le_bytes();
    let [c0, c1, c2, c3, c4, c5, c6, c7] = self.cache_size.to_le_bytes();
    let [r0, r1, r2, r3] = self.min_record_size.to_le_bytes();
    let [m0, m1, m2, m3] = self.max_record_size.to_le_bytes();
    let [k0, k1, k2, k3] = self.max_key_len.to_le_bytes();
    let [l0, l1, l2, l3] = self.leaf_page_size.to_le_bytes();

    [
      t0,
      t1,
      t2,
      t3,
      t4,
      t5,
      t6,
      t7,
      c0,
      c1,
      c2,
      c3,
      c4,
      c5,
      c6,
      c7,
      r0,
      r1,
      r2,
      r3,
      m0,
      m1,
      m2,
      m3,
      k0,
      k1,
      k2,
      k3,
      l0,
      l1,
      l2,
      l3,
      self.storage_backend,
      self.flags,
      self.serialization_phase,
    ]
  }

  /// 写入输出切片中 (零堆分配，就地编码)
  #[inline(always)]
  pub fn encode_into(&self, out: &mut [u8]) -> Result<()> {
    if out.len() < RANGE_INDEX_STUB_SIZE {
      return Err(Error::InvalidArgument(format!(
        "输出切片长度不足 {RANGE_INDEX_STUB_SIZE} 字节: {}",
        out.len()
      )));
    }
    out[..RANGE_INDEX_STUB_SIZE].copy_from_slice(&self.encode());
    Ok(())
  }

  /// 从定长切片零拷贝解码为 RangeIndexStub (小端对齐，单次模式匹配零越界检查，零堆分配，const fn)
  #[inline]
  pub const fn decode_opt(bytes: &[u8]) -> Option<Self> {
    match bytes {
      [
        t0,
        t1,
        t2,
        t3,
        t4,
        t5,
        t6,
        t7,
        c0,
        c1,
        c2,
        c3,
        c4,
        c5,
        c6,
        c7,
        r0,
        r1,
        r2,
        r3,
        m0,
        m1,
        m2,
        m3,
        k0,
        k1,
        k2,
        k3,
        l0,
        l1,
        l2,
        l3,
        storage_backend,
        flags,
        serialization_phase,
        ..,
      ] => Some(Self {
        tree_handle: u64::from_le_bytes([*t0, *t1, *t2, *t3, *t4, *t5, *t6, *t7]),
        cache_size: u64::from_le_bytes([*c0, *c1, *c2, *c3, *c4, *c5, *c6, *c7]),
        min_record_size: u32::from_le_bytes([*r0, *r1, *r2, *r3]),
        max_record_size: u32::from_le_bytes([*m0, *m1, *m2, *m3]),
        max_key_len: u32::from_le_bytes([*k0, *k1, *k2, *k3]),
        leaf_page_size: u32::from_le_bytes([*l0, *l1, *l2, *l3]),
        storage_backend: *storage_backend,
        flags: *flags,
        serialization_phase: *serialization_phase,
      }),
      _ => None,
    }
  }

  /// 从定长切片解码为 RangeIndexStub (小端对齐，单次模式匹配零越界检查，零堆分配)
  #[inline]
  pub fn decode(bytes: &[u8]) -> Result<Self> {
    Self::decode_opt(bytes).ok_or_else(|| {
      Error::InvalidArgument(format!(
        "RangeIndexStub 切片长度不足 {RANGE_INDEX_STUB_SIZE} 字节: {}",
        bytes.len()
      ))
    })
  }

  /// 快速探针：从切片零拷贝提取在线树句柄（const fn，单次模式匹配）
  #[inline]
  pub const fn read_tree_handle(bytes: &[u8]) -> Option<u64> {
    match bytes {
      [t0, t1, t2, t3, t4, t5, t6, t7, ..] => {
        Some(u64::from_le_bytes([*t0, *t1, *t2, *t3, *t4, *t5, *t6, *t7]))
      }
      _ => None,
    }
  }

  /// 快速探针：从切片零拷贝提取存储后端类型（const fn）
  #[inline]
  pub const fn read_storage_backend(bytes: &[u8]) -> Option<u8> {
    if bytes.len() > STORAGE_BACKEND_OFFSET {
      Some(bytes[STORAGE_BACKEND_OFFSET])
    } else {
      None
    }
  }

  /// 快速探针：从切片零拷贝提取序列化阶段号（const fn）
  #[inline]
  pub const fn read_serialization_phase(bytes: &[u8]) -> Option<u8> {
    if bytes.len() > SERIALIZATION_PHASE_OFFSET {
      Some(bytes[SERIALIZATION_PHASE_OFFSET])
    } else {
      None
    }
  }

  /// 快速探针：从切片零拷贝提取标志位（const fn，单次模式匹配）
  #[inline]
  pub const fn read_flags(bytes: &[u8]) -> Option<u8> {
    if bytes.len() > FLAGS_OFFSET {
      Some(bytes[FLAGS_OFFSET])
    } else {
      None
    }
  }

  /// 快速探针：从切片零拷贝检查是否已被刷入冷存储区（const fn）
  #[inline]
  pub const fn read_is_flushed(bytes: &[u8]) -> Option<bool> {
    match Self::read_flags(bytes) {
      Some(f) => Some((f & FLUSHED_BIT_MASK) != 0),
      None => None,
    }
  }

  /// 快速探针：从切片零拷贝检查是否从快照文件恢复（const fn）
  #[inline]
  pub const fn read_is_recovered(bytes: &[u8]) -> Option<bool> {
    match Self::read_flags(bytes) {
      Some(f) => Some((f & RECOVERED_BIT_MASK) != 0),
      None => None,
    }
  }

  /// 快速探针：从切片零拷贝检查所有权是否已转移到新记录（const fn）
  #[inline]
  pub const fn read_is_transferred(bytes: &[u8]) -> Option<bool> {
    match Self::read_flags(bytes) {
      Some(f) => Some((f & TRANSFERRED_BIT_MASK) != 0),
      None => None,
    }
  }

  /// 直接在二进制切片上就地清零树指针 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ClearTreeHandle / InvalidateStub)
  #[inline]
  pub fn slice_clear_tree_handle(slice: &mut [u8]) -> Result<()> {
    if slice.len() < CACHE_SIZE_OFFSET {
      return Err(Error::InvalidArgument("切片长度不足 8 字节".into()));
    }
    slice[TREE_HANDLE_OFFSET..CACHE_SIZE_OFFSET].fill(0);
    Ok(())
  }

  /// 直接在二进制切片上就地置位刷盘标志 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:SetFlushedFlag)
  #[inline]
  pub fn slice_set_flushed(slice: &mut [u8], flushed: bool) -> Result<()> {
    Self::validate_slice(slice)?;
    if flushed {
      slice[FLAGS_OFFSET] |= FLUSHED_BIT_MASK;
    } else {
      slice[FLAGS_OFFSET] &= !FLUSHED_BIT_MASK;
    }
    Ok(())
  }

  /// 直接在二进制切片上就地置位转移标志 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:SetTransferredFlag)
  #[inline]
  pub fn slice_set_transferred(slice: &mut [u8], transferred: bool) -> Result<()> {
    Self::validate_slice(slice)?;
    if transferred {
      slice[FLAGS_OFFSET] |= TRANSFERRED_BIT_MASK;
    } else {
      slice[FLAGS_OFFSET] &= !TRANSFERRED_BIT_MASK;
    }
    Ok(())
  }

  /// 直接在二进制切片上标记从检查点恢复 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:MarkRecoveredFromCheckpoint)
  #[inline]
  pub fn slice_mark_recovered_from_checkpoint(slice: &mut [u8]) -> Result<()> {
    Self::validate_slice(slice)?;
    slice[TREE_HANDLE_OFFSET..CACHE_SIZE_OFFSET].fill(0);
    slice[FLAGS_OFFSET] |= RECOVERED_BIT_MASK;
    Ok(())
  }

  /// 直接在二进制切片上更新重新激活句柄 (1:1 对标 Garnet RecreateIndex)
  ///
  /// 同 [`recreate_index`](Self::recreate_index)：仅同进程内激活占位存根，
  /// 句柄仅作标识永不解引用 (见 [`tree_handle`](Self::tree_handle) 文档)。
  #[inline]
  pub fn slice_recreate_index(slice: &mut [u8], new_tree_handle: u64) -> Result<()> {
    Self::validate_slice(slice)?;
    slice[TREE_HANDLE_OFFSET..CACHE_SIZE_OFFSET].copy_from_slice(&new_tree_handle.to_le_bytes());
    slice[FLAGS_OFFSET] &= !RECOVERED_BIT_MASK;
    Ok(())
  }

  /// 校验切片长度至少容纳完整存根
  #[inline]
  fn validate_slice(slice: &[u8]) -> Result<()> {
    if slice.len() < RANGE_INDEX_STUB_SIZE {
      return Err(Error::InvalidArgument(format!(
        "切片长度不足 {RANGE_INDEX_STUB_SIZE} 字节: {}",
        slice.len()
      )));
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::types::StorageBackendType;

  #[test]
  fn test_range_index_stub_roundtrip_and_probes() {
    let mut stub = RangeIndexStub::new(
      0x1234_5678_90ab_cdef,
      1024 * 1024 * 64,
      128,
      1024,
      256,
      4096,
      StorageBackendType::Memory,
    );
    stub.set_flushed(true);
    stub.set_recovered(true);

    let bytes = stub.encode();
    assert_eq!(bytes.len(), RANGE_INDEX_STUB_SIZE);

    let decoded = RangeIndexStub::decode(&bytes).expect("decode failed");
    assert_eq!(decoded, stub);

    // 快速探针断言
    assert_eq!(
      RangeIndexStub::read_tree_handle(&bytes),
      Some(0x1234_5678_90ab_cdef)
    );
    assert_eq!(RangeIndexStub::read_is_flushed(&bytes), Some(true));
    assert_eq!(RangeIndexStub::read_is_recovered(&bytes), Some(true));
    assert_eq!(RangeIndexStub::read_is_transferred(&bytes), Some(false));
    assert_eq!(
      RangeIndexStub::read_storage_backend(&bytes),
      Some(StorageBackendType::Memory.to_u8())
    );
    assert_eq!(RangeIndexStub::read_serialization_phase(&bytes), Some(0));

    // 编译期常量构造与探针验证
    const C_STUB: RangeIndexStub = RangeIndexStub {
      tree_handle: 88,
      cache_size: 4096,
      min_record_size: 64,
      max_record_size: 512,
      max_key_len: 128,
      leaf_page_size: 4096,
      storage_backend: 0,
      flags: 1, // FLUSHED_BIT_MASK
      serialization_phase: 2,
    };
    const C_BYTES: [u8; RANGE_INDEX_STUB_SIZE] = C_STUB.encode();
    const C_HANDLE: Option<u64> = RangeIndexStub::read_tree_handle(&C_BYTES);
    assert!(matches!(C_HANDLE, Some(88)));
    const C_FLUSHED: Option<bool> = RangeIndexStub::read_is_flushed(&C_BYTES);
    assert!(matches!(C_FLUSHED, Some(true)));
    const C_BACKEND: Option<u8> = RangeIndexStub::read_storage_backend(&C_BYTES);
    assert!(matches!(C_BACKEND, Some(0)));
    const C_PHASE: Option<u8> = RangeIndexStub::read_serialization_phase(&C_BYTES);
    assert!(matches!(C_PHASE, Some(2)));
    const C_DECODED: Option<RangeIndexStub> = RangeIndexStub::decode_opt(&C_BYTES);
    assert_eq!(C_DECODED, Some(C_STUB));
  }
}
