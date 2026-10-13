//! RangeIndex 存根结构 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:RangeIndexStub)
//!
//! 定长 35 字节二进制结构，存储于底层 Tsavorite / HybridLog 主存储日志中，记录 BfTree 配置与在线实例指针。
//!
//! 自研依据: doc/zh/collection.md 存根句柄（StorageEncoding::FlattenedTree）

use crate::{
  error::{Error, Result},
  types::{StorageBackendType, TreeTuning},
};

/// 存根字节总长度 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:Size = 35)
pub const RANGE_INDEX_STUB_SIZE: usize = 35;

/// 标志位掩码定义
const FLUSHED_BIT_MASK: u8 = 1 << 0;
const RECOVERED_BIT_MASK: u8 = 1 << 1;
const TRANSFERRED_BIT_MASK: u8 = 1 << 2;

/// 存储在底层存储引擎日志中的定长元数据存根 (35 字节二进制定长结构)
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
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
  /// 存储后端 (1B: 0=Disk, 1=Memory)
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
    storage_backend: StorageBackendType,
  ) -> Self {
    Self {
      tree_handle,
      cache_size,
      min_record_size,
      max_record_size,
      max_key_len,
      leaf_page_size,
      storage_backend: storage_backend.to_u8(),
      flags: 0,
      serialization_phase: 0,
    }
  }

  /// 基于强类型调优参数结构体创建全新未刷盘的存根（类型安全，杜绝位置传参错误）
  #[inline]
  pub fn from_tuning(
    tree_handle: u64,
    tuning: &TreeTuning,
    storage_backend: StorageBackendType,
  ) -> Self {
    Self::new(
      tree_handle,
      tuning.cache_size as u64,
      tuning.min_record_size as u32,
      tuning.max_record_size as u32,
      tuning.max_key_len as u32,
      tuning.leaf_page_size as u32,
      storage_backend,
    )
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

  /// 标记已从检查点恢复并清零句柄 (存根字段位变更面；C# 侧该语义是
  /// RangeIndexManager.Index.cs 内的 in-span 静态变更器 MarkRecoveredFromCheckpoint，
  /// 其符号锚点 1:1 挂在持有值域的位变更器 `wkv::range_index::heal::mark_recovered_patch`，
  /// 本方法只负责两个字段位，不复挂)
  #[inline]
  pub fn mark_recovered_from_checkpoint(&mut self) {
    self.tree_handle = 0;
    self.set_recovered(true);
  }

  /// 重新激活恢复的句柄并清除恢复标记 (存根字段位变更面；C# 侧该语义是
  /// RangeIndexManager.Index.cs 内的 in-span 静态变更器 RecreateIndex，其符号锚点
  /// 1:1 挂在持有值域的位变更器 `wkv::range_index::heal::recreate_patch`，
  /// 本方法只负责两个字段位，不复挂)
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
}
