use crate::{
  error::{Error, Result},
  header::{HEADER_SIZE, RecordHeader},
  simd::fast_key_eq,
};

/// 记录的只读零拷贝视图
///
/// 紧凑包装底层切片借用，无任何堆内存分配与数据拷贝。
///
/// 生命周期安全（引用跨 epoch 保护边界）：当借用来自日志页缓冲时，Rust 生命周期只
/// 约束页缓冲存活，不约束页字节稳定——页槽位经内部可变性回收复用（head 滑动 + 清零
/// 覆写），脱离 epoch 保护（或页读锁）窗口后，key/value 切片内容可能被并发覆写（页
/// 内存随实例存活，绝无悬垂 UB，但读取语义失效）。C# 同风险显式规避：拉取式扫描器
/// 将记录整体拷贝至临时缓冲（SpanByteScanIterator「so we don't have a ref to log
/// data outside epoch protection」）。本类型在 whlog 的等价契约：零拷贝视图仅在
/// `HybridLog::probe_resident` / `ScanIterator` 的 epoch 守卫或页读锁窗口内消费，
/// 跨窗口持有须先经 `RecordOutput` 拷贝出页。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordRef<'a> {
  /// 记录头元数据
  pub header: RecordHeader,
  /// 键切片引用（零拷贝借用）
  pub key: &'a [u8],
  /// 值切片引用（零拷贝借用）
  pub value: &'a [u8],
}

impl<'a> RecordRef<'a> {
  /// 从字节切片直接构造只读零拷贝视图
  ///
  /// 若切片长度不足以容纳完整记录（对齐逻辑尺寸，含隐式对齐填充），返回
  /// `Error::BufferTooShort`。
  #[inline]
  pub fn from_slice(slice: &'a [u8]) -> Result<Self> {
    let header = RecordHeader::from_slice(slice)?;
    let key_len = header.key_len() as usize;
    let val_len = header.val_len() as usize;
    let total_size = header.record_size();

    if slice.len() < total_size {
      return Err(Error::BufferTooShort {
        expected: total_size,
        actual: slice.len(),
      });
    }

    let key_end = HEADER_SIZE + key_len;
    // SAFETY: 前面已校验 slice.len() >= total_size >= kv_size = HEADER_SIZE + key_len + val_len，
    // 键/值切片精确止于 KV 区段（隐式对齐填充不外露）
    let key = unsafe { slice.get_unchecked(HEADER_SIZE..key_end) };
    let value = unsafe { slice.get_unchecked(key_end..key_end + val_len) };

    Ok(Self { header, key, value })
  }

  /// 从只读切片中切分出首条记录视图与剩余切片
  #[inline]
  pub fn split_from_slice(slice: &'a [u8]) -> Result<(Self, &'a [u8])> {
    let rec = Self::from_slice(slice)?;
    let phys_size = rec
      .header
      .checked_physical_size()
      .ok_or(Error::RecordSizeOverflow)?;

    if slice.len() < phys_size {
      return Err(Error::BufferTooShort {
        expected: phys_size,
        actual: slice.len(),
      });
    }

    // SAFETY: 前面已校验 slice.len() >= phys_size
    let remaining = unsafe { slice.get_unchecked(phys_size..) };
    Ok((rec, remaining))
  }

  /// 提取 8 位松弛填充词数量（每词代表 8 字节填充，代理自 header.filler_words）
  #[inline(always)]
  pub const fn filler_words(&self) -> u8 {
    self.header.filler_words()
  }

  /// 获取松弛填充字节总数（FillerWords * 8，词粒度）
  #[inline(always)]
  pub const fn filler_bytes(&self) -> usize {
    self.header.filler_bytes()
  }

  /// 获取当前记录槽位物理容纳值的最大字节容量（val_len + filler_bytes）
  #[inline(always)]
  pub const fn val_capacity(&self) -> usize {
    self.header.val_capacity()
  }

  /// 获取整条记录在物理上占据的总字节大小（头 + 键 + 值 + 松弛填充）
  #[inline(always)]
  pub const fn physical_size(&self) -> usize {
    self.header.physical_size()
  }

  /// 获取记录头引用
  #[inline]
  pub const fn header(&self) -> &RecordHeader {
    &self.header
  }

  /// 获取键切片借用
  #[inline]
  pub const fn key(&self) -> &'a [u8] {
    self.key
  }

  /// 基于 SIMD 高效比对当前记录键是否与指定目标键相同（对标 Tsavorite KeysEqual，键长不等由 fast_key_eq 极速短路）
  #[inline(always)]
  pub fn matches_key(&self, target_key: &[u8]) -> bool {
    fast_key_eq(self.key, target_key)
  }

  /// 获取值切片借用
  #[inline]
  pub const fn value(&self) -> &'a [u8] {
    self.value
  }

  /// 获取 48 位前驱版本逻辑地址
  #[inline]
  pub const fn prev_address(&self) -> u64 {
    self.header.address()
  }

  /// 获取 48 位前驱版本逻辑地址
  #[inline(always)]
  pub const fn previous_address(&self) -> u64 {
    self.prev_address()
  }

  /// 是否有效（非密封且非失效状态）
  #[inline(always)]
  pub const fn is_valid(&self) -> bool {
    self.header.is_valid()
  }

  /// 是否处于失效状态
  #[inline(always)]
  pub const fn is_invalid(&self) -> bool {
    self.header.is_invalid()
  }

  /// 扫描跳过判定
  #[inline(always)]
  pub const fn skip_on_scan(&self) -> bool {
    self.header.skip_on_scan()
  }

  /// 是否为墓碑删除记录
  #[inline(always)]
  pub const fn is_tombstone(&self) -> bool {
    self.header.is_tombstone()
  }

  /// 是否带有修改标记（代理自 header.is_modified）
  #[inline(always)]
  pub const fn is_modified(&self) -> bool {
    self.header.is_modified()
  }

  /// 是否带有原位更新标记（代理自 header.is_in_place_updated）
  #[inline(always)]
  pub const fn is_in_place_updated(&self) -> bool {
    self.header.is_in_place_updated()
  }

  /// 是否处于关闭/密封状态（代理自 header.is_closed）
  #[inline(always)]
  pub const fn is_closed(&self) -> bool {
    self.header.is_closed()
  }

  /// 是否关闭或带有墓碑（代理自 header.is_closed_or_tombstoned）
  #[inline(always)]
  pub const fn is_closed_or_tombstoned(&self) -> bool {
    self.header.is_closed_or_tombstoned()
  }

  /// 是否带有密封标记（代理自 header.is_sealed）
  #[inline(always)]
  pub const fn is_sealed(&self) -> bool {
    self.header.is_sealed()
  }

  /// 是否属于 Checkpoint 新版本纪元（代理自 header.is_in_new_version）
  #[inline(always)]
  pub const fn is_in_new_version(&self) -> bool {
    self.header.is_in_new_version()
  }

  /// 是否标记为读缓存记录（代理自 header.is_read_cache）
  #[inline(always)]
  pub const fn is_read_cache(&self) -> bool {
    self.header.is_read_cache()
  }

  /// 判断当前记录是否支持原位更新指定长度的新值（const fn）
  #[inline(always)]
  pub const fn can_update_in_place(&self, new_val_len: usize) -> bool {
    self.header.can_update_in_place(new_val_len)
  }

  /// 获取键字节长度
  #[inline]
  pub const fn key_len(&self) -> u32 {
    self.header.key_len()
  }

  /// 获取值字节长度
  #[inline]
  pub const fn val_len(&self) -> u32 {
    self.header.val_len()
  }

  /// 获取整条记录的对齐逻辑字节长度（头 + 键 + 值 + 隐式对齐填充）
  #[inline]
  pub const fn total_size(&self) -> usize {
    self.header.record_size()
  }
}
