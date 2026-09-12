use core::sync::atomic::{AtomicU64, Ordering};

use log::trace;

use crate::{
  error::{Error, Result},
  header::{
    HEADER_READ_CACHE_BIT, HEADER_SIZE, IN_NEW_VERSION_BIT, MAX_FILLER_BYTES, MODIFIED_BIT,
    RecordHeader, SEALED_BIT, TOMBSTONE_BIT, align_record_size,
  },
  record_ref::RecordRef,
  simd::fast_key_eq,
};

/// 记录的可变原位视图
///
/// 专为 HybridLog 可变区（Mutable Region）设计，支持定长键值对原位更新（In-place update）
/// 以及前驱指针与墓碑标记的原位修改，避免写放大与追加分配。
///
/// 并发与发布协议（1:1 对标 C# RecordDataHeader 单 8 字节原子字发布）：
/// - 16 字节头由两个 8 字节原子字构成：RecordInfo 字（地址 + 标志位，offset 0）与
///   RDH 原子字（filler + key_len + val_len 位段，offset 8，记录 8 字节对齐不变式
///   [crate::RECORD_ALIGNMENT] 保证其对齐）；
/// - 值内容先落笔，随后 [Self::write_val_with_slack] 以**单次原子 store** 发布 RDH 字——
///   filler 与 val_len 同字更新，并发无锁读者（[RecordHeader::from_ptr_atomic] Acquire 载入）
///   只会观察到前态或后态，绝无「新 val_len + 旧 filler」混合态推出的错误物理尺寸；
/// - RecordInfo 字的标志位（墓碑/修改/密封）以原子 RMW（fetch_or/fetch_and）维护，
///   与无锁读者的 Acquire 载入配对；
/// - 本视图对底层切片的普通读（键值字节、头缓存解析）依赖页写锁互斥其余写者；
///   无锁读者仅并发只读，普通读与之共存安全；
/// - C# 的 Valid/Sealed 写侧状态机（TrySeal / TryUpdateAddress / SetInvalidAtomic /
///   TryResetModifiedAtomic）由 wreviv/whlog 在格式层之外承担：wreviv 复活池对自身
///   FreeRecord 槽位字做单字 CAS。本层的 [Self::try_seal] 等 CAS 委托原语额外要求头部
///   首字 8 字节对齐（见 [Self::atomic_word]）——记录 8 字节对齐不变式下 whlog 页内
///   记录头恒对齐，原语可安全用于真实系统并发协议；
///   [Self::set_sealed] / [Self::set_modified] 等为原子 RMW 位操作原语。
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
    // SAFETY: parse_header_phys 已校验 slice.len() >= phys_size
    let slice = unsafe { slice.get_unchecked_mut(..phys_size) };
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
    HEADER_SIZE + self.header.key_len() as usize
  }

  /// 获取 RecordInfo 字（offset 0，8 字节对齐）的原子引用
  ///
  /// # Safety 契约
  /// 头部首字必须 8 字节对齐——记录对齐不变式 [crate::RECORD_ALIGNMENT] 保证 whlog 页内
  /// 记录头恒对齐；独立分配的槽位/测试向量亦须遵守。
  #[inline]
  fn atomic_info_word(&self) -> &AtomicU64 {
    debug_assert_eq!(
      self.slice.as_ptr() as usize % 8,
      0,
      "RecordHeader 必须 8 字节对齐"
    );
    // SAFETY: 调用方契约保证指针 8 字节对齐且指向已初始化的 8 字节头部首字；
    // AtomicU64 与 u64 布局一致，返回引用生命周期受 &self 约束
    unsafe { &*(self.slice.as_ptr() as *const AtomicU64) }
  }

  /// 获取 RDH 原子字（offset 8，8 字节对齐）的原子引用
  ///
  /// 对标 C# RecordDataHeader.word 的单字原子发布槽位。
  #[inline]
  fn atomic_rdh_word(&self) -> &AtomicU64 {
    debug_assert_eq!(
      (self.slice.as_ptr() as usize + 8) % 8,
      0,
      "RDH 原子字必须 8 字节对齐"
    );
    // SAFETY: 头部第二字 8 字节对齐（记录对齐不变式）且指向已初始化内存
    unsafe { &*(self.slice.as_ptr().wrapping_add(8) as *const AtomicU64) }
  }

  /// RecordInfo 字原子按位或置位标志（同步缓存，供各标志位 setter 共享）
  #[inline]
  fn info_fetch_or(&mut self, mask: u64) {
    self.atomic_info_word().fetch_or(mask, Ordering::AcqRel);
    self.header.prev_address |= mask;
  }

  /// RecordInfo 字原子按位与清零标志（同步缓存，供各标志位 setter 共享）
  #[inline]
  fn info_fetch_and(&mut self, mask: u64) {
    self.atomic_info_word().fetch_and(mask, Ordering::AcqRel);
    self.header.prev_address &= mask;
  }

  /// RecordInfo 字原子按开关置位/清零标志位
  #[inline]
  fn info_set_bit(&mut self, mask: u64, on: bool) {
    if on {
      self.info_fetch_or(mask);
    } else {
      self.info_fetch_and(!mask);
    }
  }

  /// RDH 原子字单次发布：将缓存的 filler + key_len + val_len 位段以单次 Release store 写回
  ///
  /// 对标 C# RecordDataHeader.Initialize 的单 8 字节原子写协议——单次对齐原子写发布
  /// 完整一致的新记录布局，recordLength 由该字单独推导，并发无锁读者只会观察到
  /// 前态或后态（Release 配对读者的 Acquire，保证值字节先于布局可见）。
  #[inline]
  fn publish_rdh(&mut self) {
    self
      .atomic_rdh_word()
      .store(self.header.rdh_word, Ordering::Release);
  }

  /// 获取键切片只读引用
  #[inline]
  pub fn key(&self) -> &[u8] {
    let key_end = self.key_end();
    // 安全性保证：构造时已严格校验 slice.len() >= key_end（物理槽位含松弛填充恒覆盖键区间）
    unsafe { self.slice.get_unchecked(HEADER_SIZE..key_end) }
  }

  /// 基于 SIMD 高效比对当前记录键是否与指定目标键相同（对标 Tsavorite KeysEqual，键长不等由 fast_key_eq 极速短路）
  #[inline(always)]
  pub fn matches_key(&self, target_key: &[u8]) -> bool {
    fast_key_eq(self.key(), target_key)
  }

  /// 获取值切片只读引用
  #[inline]
  pub fn value(&self) -> &[u8] {
    let key_end = self.key_end();
    let total_size = key_end + self.header.val_len() as usize;
    // 安全性保证：构造时已严格校验 slice.len() >= total_size（物理槽位 = 头 + 键 + 值 + 松弛填充）
    unsafe { self.slice.get_unchecked(key_end..total_size) }
  }

  /// 获取值切片可变引用
  #[inline]
  pub fn value_mut(&mut self) -> &mut [u8] {
    let key_end = self.key_end();
    let total_size = key_end + self.header.val_len() as usize;
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
    let val_len = self.header.val_len() as usize;
    if new_val.len() != val_len {
      return Err(Error::ValueLengthMismatch {
        expected: val_len,
        actual: new_val.len(),
      });
    }

    let key_len = self.header.key_len();
    trace!("原位更新记录值: key_len={key_len}, val_len={val_len}");

    self.value_mut().copy_from_slice(new_val);
    self.set_modified(true);
    Ok(())
  }

  /// 基于 FillerWords 与动态松弛写入新值的公共实现（严格对标 Tsavorite TrySetPinnedValueSpan & InternalRMW 原位写入语义）
  ///
  /// - 非复活路径（`clear_tombstone == false`）拦截墓碑记录，与 [Self::can_update_with_slack] 查询语义一致；
  /// - 校验新对齐逻辑尺寸不超过槽位物理占用，且腾出的显式松弛可被 8 位 FillerWords 完整表达；
  /// - 原位覆写值内容，富余空间折算为 8 字节词粒度的松弛填充；
  /// - `clear_tombstone` 为 true 时同步原子清除墓碑位（链内原地复活）；
  /// - 发布协议：值内容先落笔 → RDH 原子字单次 store 发布新布局（filler + val_len 同字）→
  ///   RecordInfo 字原子置位 MODIFIED（复活路径原子清墓碑），严格对标 C#
  ///   RecordDataHeader.Initialize 单 8 字节原子写发布协议。
  #[inline]
  fn write_val_with_slack(&mut self, new_val: &[u8], clear_tombstone: bool) -> Result<()> {
    let is_tombstone = self.header.is_tombstone();
    if is_tombstone && !clear_tombstone {
      return Err(Error::TombstoneUpdate);
    }

    let physical_size = self.header.physical_size();
    let key_len = self.header.key_len() as usize;
    let new_aligned = align_record_size(HEADER_SIZE + key_len + new_val.len());
    if new_aligned > physical_size || (physical_size - new_aligned) > MAX_FILLER_BYTES {
      return Err(Error::ValueLengthMismatch {
        expected: physical_size,
        actual: new_val.len(),
      });
    }

    let key_end = self.key_end();
    let new_val_end = key_end + new_val.len();

    // 1. 写入新值内容（先于布局发布，Release store 保证读者观察到新布局时值字节就绪）
    // SAFETY: 前置已校验 new_aligned <= physical_size == self.slice.len()，且 new_val_end <= new_aligned
    unsafe { self.slice.get_unchecked_mut(key_end..new_val_end) }.copy_from_slice(new_val);

    // 2. 富余空间精确折算为 8 字节词粒度松弛填充（记录对齐不变式下差值恒为词整数倍），
    //    更新缓存的 RDH 位段
    let remaining_slack = physical_size - new_aligned;
    self.header.set_val_len(new_val.len() as u32);
    self.header.set_filler_words((remaining_slack >> 3) as u8);

    // 3. RDH 原子字单次发布完整新布局（filler + key_len + val_len 同字原子可见）
    self.publish_rdh();

    // 4. RecordInfo 字原子置位 MODIFIED 标记对齐 Tsavorite InPlaceWriter；
    //    复活路径原子清除墓碑位（单次 CAS 同步完成置位与清零，消除双重总线同步与瞬态窗口，
    //    保证读者观察到非墓碑时新布局与新值必已发布）
    if clear_tombstone {
      let mut word = self.atomic_info_word().load(Ordering::Relaxed);
      loop {
        let new_word = (word | MODIFIED_BIT) & !TOMBSTONE_BIT;
        match self.atomic_info_word().compare_exchange_weak(
          word,
          new_word,
          Ordering::AcqRel,
          Ordering::Acquire,
        ) {
          Ok(_) => break,
          Err(actual) => word = actual,
        }
      }
      self.header.prev_address = (self.header.prev_address | MODIFIED_BIT) & !TOMBSTONE_BIT;
    } else {
      self.info_fetch_or(MODIFIED_BIT);
    }

    let val_len = self.header.val_len();
    trace!(
      "原位更新记录值(动态松弛): key_len={key_len}, val_len={val_len}, filler_bytes={remaining_slack}, clear_tombstone={clear_tombstone}"
    );

    Ok(())
  }

  /// 基于 FillerWords 与动态松弛的原位值更新（严格对标 Tsavorite TrySetPinnedValueSpan & InternalRMW 原位更新语义）
  ///
  /// - 若新值经对齐折算后可被槽位当前物理容量容纳，直接原位更新，零追加、零换页；
  /// - 腾出的富余空间自动折算为词粒度松弛填充并随 RDH 原子字单次发布，绝对保证物理槽位大小恒定；
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

  /// 原位设置或清除墓碑标记（RecordInfo 字原子 RMW，保留原有前驱地址）
  #[inline]
  pub fn set_tombstone(&mut self, is_tombstone: bool) {
    self.info_set_bit(TOMBSTONE_BIT, is_tombstone);
    let prev_addr = self.header.address();
    trace!("原位修改记录墓碑标记: is_tombstone={is_tombstone}, prev_addr={prev_addr:#x}");
  }

  /// 原位清除墓碑标记（委托 [Self::set_tombstone]）
  #[inline]
  pub fn clear_tombstone(&mut self) {
    self.set_tombstone(false);
  }

  /// 原位翻转墓碑标记（RecordInfo 字原子异或，保留原有前驱地址），返回翻转后的新状态
  #[inline]
  pub fn flip_tombstone(&mut self) -> bool {
    self
      .atomic_info_word()
      .fetch_xor(TOMBSTONE_BIT, Ordering::AcqRel);
    self.header.flip_tombstone();
    let prev_addr = self.header.address();
    trace!(
      "原位翻转记录墓碑标记: is_tombstone={}, prev_addr={prev_addr:#x}",
      self.header.is_tombstone()
    );
    self.header.is_tombstone()
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

  /// 原位设置或清除修改标记（RecordInfo 字原子 RMW，保留原有前驱地址与其他位）
  #[inline]
  pub fn set_modified(&mut self, modified: bool) {
    self.info_set_bit(MODIFIED_BIT, modified);
  }

  /// 是否带有原位更新标记（代理自 header.is_in_place_updated）
  #[inline(always)]
  pub const fn is_in_place_updated(&self) -> bool {
    self.header.is_in_place_updated()
  }

  /// 原位设置或清除原位更新标记
  #[inline]
  pub fn set_in_place_updated(&mut self, in_place_updated: bool) {
    self.set_modified(in_place_updated);
  }

  /// 获取 RecordInfo 字的原子引用（用于并发 CAS 操作，如 TrySeal / TryResetModifiedAtomic）
  ///
  /// # Safety 契约
  /// 头部首字必须 8 字节对齐——记录 8 字节对齐不变式保证 whlog 页内记录头恒对齐。
  #[inline]
  pub fn atomic_word(&self) -> &AtomicU64 {
    self.atomic_info_word()
  }

  /// 尝试原子密封记录（委托底层 RecordHeader::try_seal）
  #[inline]
  pub fn try_seal(&mut self, invalidate: bool) -> bool {
    let ok = RecordHeader::try_seal(self.atomic_word(), invalidate);
    if ok {
      self.header.set_sealed(true);
    }
    ok
  }

  /// 尝试原子清除 MODIFIED 标记（委托底层 RecordHeader::try_reset_modified_atomic）
  #[inline]
  pub fn try_reset_modified_atomic(&mut self) -> bool {
    let ok = RecordHeader::try_reset_modified_atomic(self.atomic_word());
    if ok {
      self.header.set_modified(false);
    }
    ok
  }

  /// 尝试原子更新前驱地址（委托底层 RecordHeader::try_update_address）
  #[inline]
  pub fn try_update_address(&mut self, expected_prev_addr: u64, new_prev_addr: u64) -> bool {
    let ok =
      RecordHeader::try_update_address(self.atomic_word(), expected_prev_addr, new_prev_addr);
    if ok {
      _ = self.header.set_address(new_prev_addr);
    }
    ok
  }

  /// 原子设置失效状态（委托底层 RecordHeader::set_invalid_atomic）
  #[inline]
  pub fn set_invalid_atomic(&mut self) {
    RecordHeader::set_invalid_atomic(self.atomic_word());
    self.header.set_sealed(true);
  }

  /// 解除密封状态（RecordInfo 字原子 RMW，对标 RecordInfo.cs:UnsealAndValidate）
  #[inline]
  pub fn unseal_and_validate(&mut self) {
    self.info_set_bit(SEALED_BIT, false);
  }

  /// 密封并废弃槽位（RecordInfo 字原子 RMW，对标 RecordInfo.cs:SealAndInvalidate）
  #[inline]
  pub fn seal_and_invalidate(&mut self) {
    self.info_set_bit(SEALED_BIT, true);
  }

  /// 置为失效状态（RecordInfo 字原子 RMW，对标 RecordInfo.cs:SetInvalid）
  #[inline]
  pub fn set_invalid(&mut self) {
    self.info_set_bit(SEALED_BIT, true);
  }

  /// 是否处于关闭/密封状态（委托底层 RecordHeader::is_closed）
  #[inline(always)]
  pub const fn is_closed(&self) -> bool {
    self.header.is_closed()
  }

  /// 是否关闭或带有墓碑（委托底层 RecordHeader::is_closed_or_tombstoned）
  #[inline(always)]
  pub const fn is_closed_or_tombstoned(&self) -> bool {
    self.header.is_closed_or_tombstoned()
  }

  /// 是否带有密封标记（代理自 header.is_sealed）
  #[inline(always)]
  pub const fn is_sealed(&self) -> bool {
    self.header.is_sealed()
  }

  /// 原位设置或清除密封标记（RecordInfo 字原子 RMW，保留原有前驱地址与其他位）
  #[inline]
  pub fn set_sealed(&mut self, sealed: bool) {
    self.info_set_bit(SEALED_BIT, sealed);
  }

  /// 原位密封记录（委托 [Self::set_sealed]）
  #[inline]
  pub fn seal(&mut self) {
    self.set_sealed(true);
  }

  /// 是否属于 Checkpoint 新版本纪元（代理自 header.is_in_new_version）
  #[inline(always)]
  pub const fn is_in_new_version(&self) -> bool {
    self.header.is_in_new_version()
  }

  /// 原位设置或清除 Checkpoint 新版本纪元标记（RecordInfo 字原子 RMW）
  #[inline]
  pub fn set_in_new_version(&mut self, in_new_version: bool) {
    self.info_set_bit(IN_NEW_VERSION_BIT, in_new_version);
  }

  /// 是否标记为读缓存记录（代理自 header.is_read_cache）
  #[inline(always)]
  pub const fn is_read_cache(&self) -> bool {
    self.header.is_read_cache()
  }

  /// 原位设置或清除读缓存标记（RecordInfo 字原子 RMW）
  #[inline]
  pub fn set_read_cache(&mut self, is_read_cache: bool) {
    self.info_set_bit(HEADER_READ_CACHE_BIT, is_read_cache);
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

  /// 获取底层完整记录字节切片只读引用（包含头、键、值）
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    self.slice
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

  /// 获取整条记录的对齐逻辑字节长度（头 + 键 + 值 + 隐式对齐填充）
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
    let key_len = self.header.key_len() as usize;
    let val_len = self.header.val_len() as usize;
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
