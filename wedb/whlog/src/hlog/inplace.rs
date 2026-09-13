use std::sync::atomic::{AtomicU64, Ordering};

use wdev::Device;
use wrecord::{HEADER_SIZE, MAX_FILLER_BYTES, RecordHeader, RecordMut};

use super::HybridLog;
use crate::error::{Error, Result};

impl<D: Device> HybridLog<D> {
  /// 尝试在可变区原位更新记录的值（基于 FillerWords 与动态松弛，严格对标 Tsavorite TrySetPinnedValueSpan & InternalRMW 原位更新语义）
  ///
  /// 若记录处于内存可变区且新值长度在物理容量容纳范围内（val_len + filler_bytes），
  /// 校验 expected_key 匹配后直接原位覆写并调整松弛填充，零追加、零换页、零 I/O。
  pub fn try_update_in_place(
    &self,
    addr: u64,
    expected_key: &[u8],
    new_val: &[u8],
  ) -> Result<bool> {
    self.with_mutable_record(addr, expected_key, false, |rec_mut| {
      if rec_mut.is_tombstone() || !rec_mut.can_update_with_slack(new_val.len()) {
        return Ok(false);
      }
      rec_mut.update_value_with_slack(new_val)?;
      Ok(true)
    })
  }

  /// 尝试在可变区原位设置墓碑标记（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalDelete.cs:InPlaceDeleter）
  ///
  /// 若记录处于内存可变区且非墓碑记录，校验 expected_key 匹配后直接原位标记墓碑并置位 MODIFIED，
  /// 零追加、零换页、零哈希表 CAS。
  pub fn try_set_tombstone_in_place(&self, addr: u64, expected_key: &[u8]) -> Result<bool> {
    self.with_mutable_record(addr, expected_key, false, |rec_mut| {
      if rec_mut.is_tombstone() {
        return Ok(false);
      }
      rec_mut.set_tombstone(true);
      rec_mut.set_modified(true);
      Ok(true)
    })
  }

  /// 尝试在可变区原位读-改-写记录的值（严格对标 libs/server/Storage/Functions/MainStore/RMWMethods.cs:InPlaceUpdaterWorker & InternalRMW.cs）
  ///
  /// - 若记录处于内存可变区且非墓碑记录，并校验键与 `expected_key` 匹配（使用 SIMD 高效比对）；
  /// - 在持有页面写锁期间，向闭包 `f` 暴露底层物理内存可变切片 `&mut [u8]` 执行就地读-改-写；
  /// - 若闭包返回 `Some(r)` 说明原位修改成功，返回 `Ok(Some(r))`；
  /// - 若记录为墓碑、键不匹配、处于只读区或闭包返回 `None`（如因长度变更），安全返回
  ///   `Ok(None)` 供调用方降级走 RCU 追加写。
  pub fn try_modify_record_in_place<R>(
    &self,
    addr: u64,
    expected_key: &[u8],
    f: impl FnOnce(&mut [u8]) -> Option<R>,
  ) -> Result<Option<R>> {
    self.with_mutable_record(addr, expected_key, None, |rec_mut| {
      if rec_mut.is_tombstone() {
        return Ok(None);
      }
      let r = f(rec_mut.value_mut());
      if r.is_some() {
        rec_mut.set_modified(true);
      }
      Ok(r)
    })
  }

  /// 尝试在可变区链内原地复活墓碑记录（严格对照 C# Garnet InternalUpsert.cs:127 TryRevivifyInChain & RecordDataHeader.cs:FillerWords）
  ///
  /// 若记录处于内存可变区且为墓碑记录，校验 expected_key 匹配且物理容量（val_capacity）足以容纳新值的前提下，
  /// 原子覆写值并清除墓碑标记，富余空间自动吸纳转换为高精度松弛填充，无需分配新 Tail 槽位、无需换页、无需修改哈希索引指针。
  pub fn try_revivify_in_chain(
    &self,
    addr: u64,
    expected_key: &[u8],
    new_val: &[u8],
  ) -> Result<bool> {
    self.with_mutable_record(addr, expected_key, false, |rec_mut| {
      // 必须为墓碑记录，且富余空间可以松弛填充形式吸纳（val_capacity - new_val <= MAX_FILLER_BYTES）
      if !rec_mut.is_tombstone()
        || new_val.len() > rec_mut.val_capacity()
        || (rec_mut.val_capacity() - new_val.len()) > MAX_FILLER_BYTES
      {
        return Ok(false);
      }
      rec_mut.revivify_with_slack(new_val)?;
      Ok(true)
    })
  }

  /// 在复活池回收的物理槽位上就地覆写记录（严格对照 C# Garnet BlockAllocate.cs:77-80 TryTakeFreeRecord）
  ///
  /// 若槽位大小超出记录对齐逻辑尺寸，且富余空间足以容纳新的记录头，自动填充 PadRecord；
  /// 富余不足一个头时吸纳为当前记录的松弛填充（filler_bytes，词粒度），使整条记录物理尺寸
  /// 精确覆盖整个 `slot_size`，消除中间无法解码的残片。返回 `Ok(())` 表示复活写入成功。
  ///
  /// # 原子发布协议（对标 C# RecordDataHeader.Initialize 单 8 字节原子字发布）
  /// 槽位旧记录可能仍被无锁读者解析（prev 链在途引用），发布顺序严格保证任意中间态
  /// 布局一致且链条一致：
  /// 1. RecordInfo 字原子 store（新前驱地址 + 墓碑标记）——旧布局读者经旧 RDH 解析旧键值
  ///    （物理尺寸恒为槽位大小，越界不可能），未匹配则沿新前驱继续回溯，链条一致；
  /// 2. 键/值/填充字节落笔；
  /// 3. RDH 原子字单次 store（filler + key_len + val_len 同字发布完整新布局）——
  ///    无锁读者经 `RecordHeader::from_ptr_atomic` Acquire 载入只会观察到前态或后态。
  ///
  /// 新头为整体覆写，天然不携带 SEALED 等易失标记（解除 wedb_reviv 复活池的槽位密封约定）。
  pub fn revivify_record_at(
    &self,
    addr: u64,
    slot_size: usize,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
  ) -> Result<()> {
    if !self.addresses.is_mutable(addr) {
      return Err(Error::AddressOutOfRange {
        addr,
        begin: self.addresses.begin(),
        tail: self.addresses.tail(),
      });
    }

    let p = super::RecParams {
      prev_addr,
      key,
      val,
      is_tombstone,
    };
    let rec_size = self.validate_append_args(&p)?;
    if slot_size < rec_size {
      return Err(Error::RecordTooLarge {
        size: rec_size,
        page_size: slot_size,
      });
    }

    let page_id = self.config.page_id(addr);
    let offset = self.config.page_offset(addr);
    if !self.buffer.is_page_loaded(page_id) {
      return Err(Error::PageNotReady(page_id));
    }

    let mut guard = self.buffer.write_page(page_id);

    // Double-check 防止加锁期间地址状态发生滑动或页面槽位被重用
    if !self.addresses.is_mutable(addr) || !self.buffer.is_page_loaded(page_id) {
      return Err(Error::AddressOutOfRange {
        addr,
        begin: self.addresses.begin(),
        tail: self.addresses.tail(),
      });
    }

    let Some(slot_buf) = guard.get_mut(offset..offset + slot_size) else {
      return Err(Error::RecordTooLarge {
        size: slot_size,
        page_size: self.config.page_size,
      });
    };

    let mut header =
      RecordHeader::new(prev_addr, key.len() as u32, val.len() as u32, is_tombstone)?;
    let kv_end = HEADER_SIZE + key.len() + val.len();
    let remaining = slot_size - rec_size;

    // 1. RecordInfo 字原子发布（新前驱地址 + 墓碑标记，链条一致性先行）
    // SAFETY: 记录 8 字节对齐不变式保证槽位头两字对齐
    unsafe { &*(slot_buf.as_ptr() as *const AtomicU64) }
      .store(header.prev_address, Ordering::Release);

    // 2. 键值字节落笔 + 隐式对齐填充清零
    slot_buf[HEADER_SIZE..HEADER_SIZE + key.len()].copy_from_slice(key);
    slot_buf[HEADER_SIZE + key.len()..kv_end].copy_from_slice(val);
    slot_buf[kv_end..rec_size].fill(0);

    if remaining >= HEADER_SIZE {
      // 富余容纳一个完整头：写 Pad 填充头，逻辑尺寸精确覆盖至槽位末尾
      let pad_header = RecordHeader::pad(remaining);
      slot_buf[rec_size..rec_size + HEADER_SIZE].copy_from_slice(&pad_header.to_bytes());
    } else if remaining > 0 {
      // 富余不足一个 Pad 头（对齐不变式下恒为 8 字节整词）：吸纳为记录松弛填充，
      // 物理尺寸精确覆盖整个槽位（填充区清零，绝不解释）
      header.set_filler_bytes(remaining);
      slot_buf[rec_size..].fill(0);
    }

    // 3. RDH 原子字单次发布完整新布局（filler + key_len + val_len 同字）
    // SAFETY: 记录 8 字节对齐不变式保证槽位头两字对齐
    unsafe { &*(slot_buf.as_ptr().wrapping_add(8) as *const AtomicU64) }
      .store(header.rdh_word, Ordering::Release);

    Ok(())
  }

  /// 可变区原位操作的统一内核：加页写锁 → Double-check（可变区 + 页就绪）→ 解析 → 键匹配后执行闭包
  ///
  /// 任一前置条件不满足即返回 `Ok(degraded)`，供调用方降级为 RCU 追加写（对标 C#
  /// InPlaceUpdater 失败路径）；`degraded` 由调用方给定（bool 路径为 false，Option 路径为 None）。
  fn with_mutable_record<T>(
    &self,
    addr: u64,
    expected_key: &[u8],
    degraded: T,
    f: impl FnOnce(&mut RecordMut<'_>) -> Result<T>,
  ) -> Result<T> {
    let ro = self.addresses.read_only();
    if addr < ro || addr >= self.addresses.tail() {
      return Ok(degraded);
    }

    let page_id = self.config.page_id(addr);
    let offset = self.config.page_offset(addr);
    if offset + HEADER_SIZE > self.config.page_size || !self.buffer.is_page_loaded(page_id) {
      return Ok(degraded);
    }

    let mut guard = self.buffer.write_page(page_id);

    // Double-check 防止加锁期间地址状态发生滑动或页面槽位被重用（严格终止于 read_only_address）
    if addr < self.addresses.read_only() || !self.buffer.is_page_loaded(page_id) {
      return Ok(degraded);
    }

    let mut rec_mut = match RecordMut::from_slice_mut(&mut guard[offset..]) {
      Ok(r) => r,
      Err(_) => return Ok(degraded),
    };
    if !rec_mut.matches_key(expected_key) {
      return Ok(degraded);
    }

    f(&mut rec_mut)
  }
}
