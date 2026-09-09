use crate::{
  error::{Error, Result},
  header::{HEADER_SIZE, RecordHeader},
  simd::fast_key_eq,
};

/// 记录的只读零拷贝视图
///
/// 紧凑包装底层切片借用，无任何堆内存分配与数据拷贝。
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
  /// 从字节切片直接构造只读零拷贝视图（const fn）
  ///
  /// 若切片长度不足以容纳记录头或完整键值数据，返回 `Error::BufferTooShort`。
  #[inline]
  pub const fn from_slice(slice: &'a [u8]) -> Result<Self> {
    let header = match RecordHeader::from_slice(slice) {
      Ok(h) => h,
      Err(e) => return Err(e),
    };
    let key_len = header.key_len as usize;
    let total_size = match header.checked_record_size() {
      Some(s) => s,
      None => return Err(Error::RecordSizeOverflow),
    };

    if slice.len() < total_size {
      return Err(Error::BufferTooShort {
        expected: total_size,
        actual: slice.len(),
      });
    }

    let (_, after_hdr) = slice.split_at(HEADER_SIZE);
    let (key, after_key) = after_hdr.split_at(key_len);
    let (value, _) = after_key.split_at(header.val_len as usize);

    Ok(Self { header, key, value })
  }

  /// 从只读切片中切分出首条记录视图与剩余切片（const fn）
  #[inline]
  pub const fn split_from_slice(slice: &'a [u8]) -> Result<(Self, &'a [u8])> {
    let rec = match Self::from_slice(slice) {
      Ok(r) => r,
      Err(e) => return Err(e),
    };
    let phys_size = match rec.header.checked_physical_size() {
      Some(s) => s,
      None => return Err(Error::RecordSizeOverflow),
    };

    if slice.len() < phys_size {
      return Err(Error::BufferTooShort {
        expected: phys_size,
        actual: slice.len(),
      });
    }

    let (_, remaining) = slice.split_at(phys_size);
    Ok((rec, remaining))
  }

  /// 提取 8 位松弛填充词数量（每词代表 8 字节填充，对标 Garnet RecordDataHeader.FillerWords）
  #[inline(always)]
  pub const fn filler_words(&self) -> u8 {
    self.header.filler_words()
  }

  /// 提取 3 位单字节松弛填充余数（0..7 字节）
  #[inline(always)]
  pub const fn filler_rem(&self) -> u8 {
    self.header.filler_rem()
  }

  /// 获取松弛填充字节总数（FillerWords * 8 + FillerRem，单字节级高精度）
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

  /// 基于 SIMD 高效比对当前记录键是否与指定目标键相同
  #[inline]
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

  /// 是否为墓碑删除记录
  #[inline(always)]
  pub const fn is_tombstone(&self) -> bool {
    self.header.is_tombstone()
  }

  /// 是否带有修改标记（对标 C# RecordInfo.Modified）
  #[inline(always)]
  pub const fn is_modified(&self) -> bool {
    self.header.is_modified()
  }

  /// 是否带有密封标记（对标 C# RecordInfo.IsSealed / TrySeal）
  #[inline(always)]
  pub const fn is_sealed(&self) -> bool {
    self.header.is_sealed()
  }

  /// 是否属于 Checkpoint 新版本纪元（对标 C# RecordInfo.IsInNewVersion）
  #[inline(always)]
  pub const fn is_in_new_version(&self) -> bool {
    self.header.is_in_new_version()
  }

  /// 是否标记为读缓存记录（对标 C# RecordInfo.IsReadCache）
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
    self.header.key_len
  }

  /// 获取值字节长度
  #[inline]
  pub const fn val_len(&self) -> u32 {
    self.header.val_len
  }

  /// 获取整条记录的字节长度（头 + 键 + 值）
  #[inline]
  pub const fn total_size(&self) -> usize {
    self.header.record_size()
  }
}
