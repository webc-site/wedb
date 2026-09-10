use log::trace;

use crate::{
  error::{Error, Result},
  header::{HEADER_SIZE, MAX_FILLER_BYTES, RecordHeader},
  record_ref::RecordRef,
  simd::fast_key_eq,
};

/// 记录的可变原位视图
///
/// 专为 HybridLog 可变区（Mutable Region）设计，支持定长键值对原位更新（In-place update）
/// 以及前驱指针与墓碑标记的原位修改，避免写放大与追加分配。
#[derive(Debug)]
pub struct RecordMut<'a> {
  header: RecordHeader,
  slice: &'a mut [u8],
}

/// 解析记录头并校验物理槽位完整性（返回头与物理槽位大小）
#[inline]
fn parse_header_phys(slice: &[u8]) -> Result<(RecordHeader, usize)> {
  let header = RecordHeader::from_slice(slice)?;
  let phys_size = header
    .checked_physical_size()
    .ok_or(Error::RecordSizeOverflow)?;

  if slice.len() < phys_size {
    return Err(Error::BufferTooShort {
      expected: phys_size,
      actual: slice.len(),
    });
  }
  Ok((header, phys_size))
}

impl<'a> RecordMut<'a> {
  /// 从可变字节切片构造原位视图
  ///
  /// 若切片长度不足以容纳物理槽位数据，返回 `Error::BufferTooShort`。
  #[inline]
  pub fn from_slice_mut(slice: &'a mut [u8]) -> Result<Self> {
    let (header, phys_size) = parse_header_phys(slice)?;
    let slice = &mut slice[..phys_size];
    Ok(Self { header, slice })
  }

  /// 获取记录头引用
  #[inline]
  pub const fn header(&self) -> &RecordHeader {
    &self.header
  }

  /// 键数据结束偏移（即值数据起始偏移，构造时已校验不越界）
  #[inline]
  const fn key_end(&self) -> usize {
    HEADER_SIZE + self.header.key_len as usize
  }

  /// 将头部前 8 字节复合字（地址 + 标志位）同步写回底层切片
  #[inline]
  fn sync_word(&mut self) {
    self.slice[0..8].copy_from_slice(&self.header.prev_address.to_le_bytes());
  }

  /// 获取键切片只读引用
  #[inline]
  pub fn key(&self) -> &[u8] {
    let key_end = self.key_end();
    // 安全性保证：构造时已严格校验 slice.len() >= key_end（物理槽位含松弛填充恒覆盖键区间）
    unsafe { self.slice.get_unchecked(HEADER_SIZE..key_end) }
  }

  /// 基于 SIMD 高效比对当前记录键是否与指定目标键相同
  #[inline]
  pub fn matches_key(&self, target_key: &[u8]) -> bool {
    fast_key_eq(self.key(), target_key)
  }

  /// 获取值切片只读引用
  #[inline]
  pub fn value(&self) -> &[u8] {
    let key_end = self.key_end();
    let total_size = key_end + self.header.val_len as usize;
    // 安全性保证：构造时已严格校验 slice.len() >= total_size（物理槽位 = 头 + 键 + 值 + 松弛填充）
    unsafe { self.slice.get_unchecked(key_end..total_size) }
  }

  /// 获取值切片可变引用
  #[inline]
  pub fn value_mut(&mut self) -> &mut [u8] {
    let key_end = self.key_end();
    let total_size = key_end + self.header.val_len as usize;
    // 安全性保证：构造时已严格校验 slice.len() >= total_size（物理槽位 = 头 + 键 + 值 + 松弛填充）
    unsafe { self.slice.get_unchecked_mut(key_end..total_size) }
  }

  /// 原位更新值内容（要求非墓碑且新值长度与当前记录中定义的值长度完全一致）
  ///
  /// 墓碑拦截与 [Self::can_update_in_place] 查询语义严格一致；复活须走 [Self::revivify_with_slack]。
  #[inline]
  pub fn update_value_in_place(&mut self, new_val: &[u8]) -> Result<()> {
    if self.is_tombstone() {
      return Err(Error::TombstoneUpdate);
    }
    let val_len = self.header.val_len as usize;
    if new_val.len() != val_len {
      return Err(Error::ValueLengthMismatch {
        expected: val_len,
        actual: new_val.len(),
      });
    }

    let key_len = self.header.key_len;
    trace!("原位更新记录值: key_len={key_len}, val_len={val_len}");

    self.value_mut().copy_from_slice(new_val);
    Ok(())
  }

  /// 基于 FillerWords 与动态松弛写入新值的公共实现（严格对标 libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs:TrySetPinnedValueSpan & InternalRMW.cs）
  ///
  /// - 非复活路径（`clear_tombstone == false`）拦截墓碑记录，与 [Self::can_update_with_slack] 查询语义一致；
  /// - 校验新值长度不超过槽位物理容量（val_capacity）且富余松弛可被头部完整表达；
  /// - 原位覆写值内容，富余空间折算为单字节精度的松弛填充；
  /// - `clear_tombstone` 为 true 时同步清除墓碑位（链内原地复活），单次刷回 16 字节头部。
  #[inline]
  fn write_val_with_slack(&mut self, new_val: &[u8], clear_tombstone: bool) -> Result<()> {
    let is_tombstone = self.header.is_tombstone();
    if is_tombstone && !clear_tombstone {
      return Err(Error::TombstoneUpdate);
    }

    let total_capacity = self.header.val_capacity();
    if new_val.len() > total_capacity || (total_capacity - new_val.len()) > MAX_FILLER_BYTES {
      return Err(Error::ValueLengthMismatch {
        expected: total_capacity,
        actual: new_val.len(),
      });
    }

    let key_end = self.key_end();
    let new_val_end = key_end + new_val.len();

    // 1. 写入新值内容
    self.slice[key_end..new_val_end].copy_from_slice(new_val);

    // 2. 更新 Header（复活路径同步清除墓碑位，富余松弛精确折算为 filler bytes）
    let remaining_slack = total_capacity - new_val.len();
    self.header.val_len = new_val.len() as u32;
    self.header.set_filler_bytes(remaining_slack);
    if clear_tombstone {
      self.header.set_tombstone(false);
    }

    // 3. 单次刷回完整 16 字节头部，避免总线重复写
    self.slice[0..HEADER_SIZE].copy_from_slice(&self.header.to_bytes());

    let key_len = self.header.key_len;
    let val_len = self.header.val_len;
    trace!(
      "原位更新记录值(动态松弛): key_len={key_len}, val_len={val_len}, filler_bytes={remaining_slack}, clear_tombstone={clear_tombstone}"
    );

    Ok(())
  }

  /// 基于 FillerWords 与动态松弛的原位值更新（严格对标 libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs:TrySetPinnedValueSpan & InternalRMW.cs）
  ///
  /// - 若新值长度不超过槽位当前物理容纳容量（val_capacity），直接原位更新，零追加、零换页；
  /// - 腾出的富余空间自动折算为单字节高精度的松弛填充并写回记录头，绝对保证物理槽位大小恒定；
  /// - 若超出当前物理容量，返回 `Error::ValueLengthMismatch`。
  #[inline]
  pub fn update_value_with_slack(&mut self, new_val: &[u8]) -> Result<()> {
    self.write_val_with_slack(new_val, false)
  }

  /// 链内原地复活专用方法：单次覆写原子完成新值填入、松弛吸纳与清除墓碑标记
  #[inline]
  pub fn revivify_with_slack(&mut self, new_val: &[u8]) -> Result<()> {
    self.write_val_with_slack(new_val, true)
  }

  /// 判断当前记录是否可以通过动态松弛原位容纳指定长度的新值（const fn）
  #[inline(always)]
  pub const fn can_update_with_slack(&self, new_val_len: usize) -> bool {
    self.header.can_update_with_slack(new_val_len)
  }

  /// 原位设置或清除墓碑标记（同步写回底层切片，保留原有前驱地址）
  #[inline]
  pub fn set_tombstone(&mut self, is_tombstone: bool) {
    self.header.set_tombstone(is_tombstone);
    self.sync_word();
    let prev_addr = self.header.address();
    trace!("原位修改记录墓碑标记: is_tombstone={is_tombstone}, prev_addr={prev_addr:#x}");
  }

  /// 原位翻转墓碑标记（同步写回底层切片，保留原有前驱地址），返回翻转后的新状态
  #[inline]
  pub fn flip_tombstone(&mut self) -> bool {
    let is_tombstone = self.header.flip_tombstone();
    self.sync_word();
    let prev_addr = self.header.address();
    trace!("原位翻转记录墓碑标记: is_tombstone={is_tombstone}, prev_addr={prev_addr:#x}");
    is_tombstone
  }

  /// 判断当前记录是否支持原位更新指定长度的新值（const fn）
  #[inline(always)]
  pub const fn can_update_in_place(&self, new_val_len: usize) -> bool {
    self.header.can_update_in_place(new_val_len)
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

  /// 是否带有修改标记（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:Modified）
  #[inline(always)]
  pub const fn is_modified(&self) -> bool {
    self.header.is_modified()
  }

  /// 原位设置或清除修改标记（同步写回底层切片，保留原有前驱地址与其他位）
  #[inline]
  pub fn set_modified(&mut self, modified: bool) {
    self.header.set_modified(modified);
    self.sync_word();
  }

  /// 是否带有密封标记（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsSealed / TrySeal）
  #[inline(always)]
  pub const fn is_sealed(&self) -> bool {
    self.header.is_sealed()
  }

  /// 原位设置或清除密封标记（同步写回底层切片，保留原有前驱地址与其他位）
  #[inline]
  pub fn set_sealed(&mut self, sealed: bool) {
    self.header.set_sealed(sealed);
    self.sync_word();
  }

  /// 是否属于 Checkpoint 新版本纪元（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsInNewVersion）
  #[inline(always)]
  pub const fn is_in_new_version(&self) -> bool {
    self.header.is_in_new_version()
  }

  /// 原位设置或清除 Checkpoint 新版本纪元标记（同步写回底层切片，保留原有前驱地址与其他位）
  #[inline]
  pub fn set_in_new_version(&mut self, in_new_version: bool) {
    self.header.set_in_new_version(in_new_version);
    self.sync_word();
  }

  /// 是否标记为读缓存记录（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsReadCache）
  #[inline(always)]
  pub const fn is_read_cache(&self) -> bool {
    self.header.is_read_cache()
  }

  /// 原位设置或清除读缓存标记（同步写回底层切片，保留原有前驱地址与其他位）
  #[inline]
  pub fn set_read_cache(&mut self, is_read_cache: bool) {
    self.header.set_read_cache(is_read_cache);
    self.sync_word();
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

  /// 获取底层完整记录字节切片只读引用（包含头、键、值）
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    self.slice
  }

  /// 提取 8 位松弛填充词数量（每词代表 8 字节填充，对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:FillerWords）
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

  /// 获取整条记录的理论逻辑字节长度（头 + 键 + 值）
  #[inline]
  pub const fn total_size(&self) -> usize {
    self.header.record_size()
  }

  /// 转换为只读零拷贝视图
  #[inline]
  pub fn as_ref(&self) -> RecordRef<'_> {
    RecordRef {
      header: self.header,
      key: self.key(),
      value: self.value(),
    }
  }

  /// 消耗视图并转换为对应完整生命周期的只读零拷贝视图
  #[inline]
  pub fn into_ref(self) -> RecordRef<'a> {
    let key_len = self.header.key_len as usize;
    let val_len = self.header.val_len as usize;
    let key_end = HEADER_SIZE + key_len;
    let total_size = key_end + val_len;
    RecordRef {
      header: self.header,
      key: unsafe { self.slice.get_unchecked(HEADER_SIZE..key_end) },
      value: unsafe { self.slice.get_unchecked(key_end..total_size) },
    }
  }

  /// 释放视图，归还底层可变字节切片
  #[inline]
  pub fn into_slice(self) -> &'a mut [u8] {
    self.slice
  }
}

impl<'a> PartialEq for RecordMut<'a> {
  fn eq(&self, other: &Self) -> bool {
    self.header == other.header && self.slice as &[u8] == other.slice as &[u8]
  }
}

impl<'a> Eq for RecordMut<'a> {}
