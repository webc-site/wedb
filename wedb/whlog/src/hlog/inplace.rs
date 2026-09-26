use std::sync::atomic::{AtomicU64, Ordering};

use wdev::Device;
use wrecord::{
  HEADER_SIZE, MAX_FILLER_BYTES, RDH_WORD_OFFSET, RECORD_ALIGNMENT, RecordHeader, RecordMut, ValSrc,
};

use super::HybridLog;
use crate::error::{Error, Result};

/// 槽位复活切分时的记录内保留松弛填充字节数
/// （对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:RecordSplitRetainFillerWords
/// = 64 词 = 512 字节：`ComputeFillerWordsOrSplit` 的 SplitOverflowingFiller 分支在切出冗余空间后
/// 为记录保留 64 词填充，供后续原位增长吸纳）
const REVIV_SPLIT_RETAIN_FILLER_BYTES: usize = 64 * RECORD_ALIGNMENT;

impl<D: Device> HybridLog<D> {
  /// 尝试在可变区原位更新记录的值（基于 FillerWords 与动态松弛，严格对标 Tsavorite TrySetPinnedValueSpan & InternalRMW 原位更新语义）
  ///
  /// 若记录处于内存可变区且新值长度在物理容量容纳范围内（val_len + filler_bytes），
  /// 校验 expected_key 匹配后直接原位覆写并调整松弛填充，零追加、零换页、零 I/O。
  ///
  /// 值源泛型 [`ValSrc`]：连续切片（既有调用零改动）或分段直写源（对象信封
  /// 单次成形，消除中间整值缓冲）
  pub fn try_update_in_place<V: ValSrc + ?Sized>(
    &self,
    addr: u64,
    expected_key: &[u8],
    new_val: &V,
  ) -> Result<bool> {
    self.with_mutable_record(addr, expected_key, false, |rec_mut| {
      if rec_mut.is_tombstone() || !rec_mut.can_update_with_slack(new_val.val_len()) {
        return Ok(false);
      }
      rec_mut.update_value_with_slack(new_val)?;
      Ok(true)
    })
  }

  /// 尝试在可变区原位增长并读-改-写记录的值（原位增长内核）
  ///
  /// 此函数为可变区记录的原位增长操作提供底层支持。
  /// （由上层逻辑单点持有并在外层进行控制，本物理层函数负责最基础的原位字节操作）
  ///
  /// 与等长原位改写 [`Self::try_modify_record_in_place`] 的唯一差别是**长度可改**：
  /// 闭包拿到「逻辑旧值 + 槽位松弛富余」的完整可变容量切片（长度
  /// [`RecordHeader::val_capacity`]）与旧值长，只把新字节就地落在旧值尾部或间隙
  /// 之上，返回新逻辑值长即原位发布——旧数据零复制、零尾部追加、零中间缓冲
  /// （对位 C# `appendValue.CopyTo(logRecord.ValueSpan.Slice(originalLength))`）。
  ///
  /// 长度发布绝不再另写一处：新长度须被 [`RecordHeader::can_update_with_slack`]
  /// 吸纳，改长经 [`RecordMut::resize_val_with_slack`] 复用写侧唯一的 RDH 单字
  /// 原子发布内核。墓碑、键不匹配、只读区、闭包返回 `None`（容量不足等）或长度
  /// 超容量一律回 `Ok(None)` 供调用方降级尾部追加——闭包已落笔的富余字节在发布
  /// 前对读者不可见，故降级零副作用。
  pub fn try_grow_record_in_place(
    &self,
    addr: u64,
    expected_key: &[u8],
    f: impl FnOnce(&mut [u8], usize) -> Option<usize>,
  ) -> Result<Option<usize>> {
    self.with_mutable_record(addr, expected_key, None, |rec_mut| {
      if rec_mut.is_tombstone() {
        return Ok(None);
      }
      let old_len = rec_mut.val_len() as usize;
      let Some(new_len) = f(rec_mut.value_capacity_mut(), old_len) else {
        return Ok(None);
      };
      if !rec_mut.can_update_with_slack(new_len) {
        return Ok(None);
      }
      rec_mut.resize_val_with_slack(new_len)?;
      Ok(Some(new_len))
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

  /// 尝试在可变区原位取值并置墓碑（GETDEL 读删一体内核）
  ///
  /// 页写锁临界区内先捕获当前值再翻转墓碑标记——对标 C# GETDEL IPU 回调的
  /// 锁内一体（RMWMethods.cs:764-768 CopyRespTo 出值 + ExpireAndStop 摘除，
  /// 锁界由 InternalRMW.cs:70 FindOrCreateTagAndTryEphemeralXLock 划定）：
  /// 并发原位更新走同一页写锁，捕获与摘除对其原子，应答值即实际摘除记录的值。
  /// 已是墓碑、非可变区、页未就绪、版本推进冻结或键不匹配一律回 `Ok(None)`
  /// 供调用方按既有降级臂处理
  pub fn try_take_tombstone_in_place_with<R>(
    &self,
    addr: u64,
    expected_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    self.with_mutable_record(addr, expected_key, None, |rec_mut| {
      if rec_mut.is_tombstone() {
        return Ok(None);
      }
      let taken = f(rec_mut.value());
      rec_mut.set_tombstone(true);
      rec_mut.set_modified(true);
      Ok(Some(taken))
    })
  }

  #[inline]
  pub fn try_take_tombstone_in_place(
    &self,
    addr: u64,
    expected_key: &[u8],
  ) -> Result<Option<Vec<u8>>> {
    self.try_take_tombstone_in_place_with(addr, expected_key, |v| v.to_vec())
  }

  /// 尝试在可变区原位读-改-写记录的值（可变区就地读-改-写内核）
  ///
  /// - 若记录处于内存可变区且非墓碑记录，并校验键与 `expected_key` 匹配（使用 SIMD 高效比对）；
  /// - 在持有页面写锁期间，向闭包 `f` 暴露底层物理内存可变切片 `&mut [u8]` 执行就地读-改-写；
  /// - 若闭包返回 `Some(r)` 说明原位修改成功，返回 `Ok(Some(r))`；
  /// - 若记录为墓碑、键不匹配、处于只读区或闭包返回 `None`（如因长度变更），返回
  ///   `Ok(None)` 供调用方降级走 RCU 追加写。
  ///
  /// # 零副作用边界（调用方硬契约，票 zcode-r34-writekernel 条目二）
  ///
  /// 「安全返回 `Ok(None)`」仅指本内核不置 MODIFIED 位、不发布任何新状态；
  /// **等长臂闭包内新值字节已物理落笔**（`val_len` 未变、字节已新，对无锁读者
  /// 立即可见），`Ok(None)` 并不逆写已落笔字节。调用方必须区分两种失败面：
  /// 闭包自身以长度门在落笔前拒绝（未触碰字节，真零副作用，可降级 RCU 重写）；
  /// 闭包已落笔后的通知失败（已生效态，须以「已生效+镜像缺失」Err 上抛收口，
  /// 绝不可当 `Ok(None)` 降级 RCU——那会产生双份效果与无镜像半变更）。
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

  /// 尝试在内存驻留区原位读-改-写记录的值（不受只读区限制，专供恢复期存根自愈等独占维护路径）
  ///
  /// 只要记录所在页面当前驻留在内存环形页缓冲中且校验键匹配，即可在页写锁保护下
  /// 就地改写值切片，零追加、零换页、零 I/O（严格对标 C# Garnet OnRecoverySnapshotRead
  /// 对快照恢复记录直接原地 MarkRecoveredFromCheckpoint 的语义）。
  pub fn try_modify_resident_record_in_place<R>(
    &self,
    addr: u64,
    expected_key: &[u8],
    f: impl FnOnce(&mut [u8]) -> Option<R>,
  ) -> Result<Option<R>> {
    let page_id = self.config.page_id(addr);
    let offset = self.config.page_offset(addr);
    if offset + HEADER_SIZE > self.config.page_size || !self.buffer.is_page_loaded(page_id) {
      return Ok(None);
    }
    let mut guard = self.buffer.write_page(page_id);
    if !self.buffer.is_page_loaded(page_id) {
      return Ok(None);
    }
    let mut rec_mut = match RecordMut::from_slice_mut(&mut guard[offset..]) {
      Ok(r) => r,
      Err(_) => return Ok(None),
    };
    if !rec_mut.matches_key(expected_key) || rec_mut.is_tombstone() {
      return Ok(None);
    }
    let r = f(rec_mut.value_mut());
    if r.is_some() {
      rec_mut.set_modified(true);
    }
    Ok(r)
  }

  /// 尝试在可变区原位密封记录（委托 [`RecordMut::try_seal`]，SEAL 位单点落笔）
  ///
  /// 页面未就绪、偏移越界或头部解码失败一律回 `false`，由调用方降级走尾部追加。
  ///
  /// Pad 切分块（复活池合法驻留形态）特化臂：`key_len` 哨兵令物理尺寸恒越出页界，
  /// `RecordMut` 尺寸校验必拒；而密封只触 RecordInfo 单原子字、与键值布局无关，
  /// 故按头判 Pad 后直落密封——保证复活池「归池槽位恒处 Closed 态」不变量
  /// （对标 C# Helpers.cs:128 TryTransferToFreeList 的 IsClosed 前置断言）同样
  /// 覆盖分裂切出块。已密封槽位幂等回 `false`，不破坏旧标记。
  pub fn try_seal_record(&self, addr: u64, invalidate: bool) -> bool {
    let page_id = self.config.page_id(addr);
    let offset = self.config.page_offset(addr);
    if offset + HEADER_SIZE > self.config.page_size || !self.buffer.is_page_loaded(page_id) {
      return false;
    }
    let mut guard = self.buffer.write_page(page_id);
    if !self.buffer.is_page_loaded(page_id) {
      return false;
    }
    let slot = &mut guard[offset..];
    if RecordHeader::decode_opt(slot).is_some_and(|header| header.is_pad()) {
      // SAFETY: 记录 8 字节对齐不变式（RECORD_ALIGNMENT）保证槽位头两字对齐，
      // 与 revivify_record_at 双原子字发布同一口径
      let word = unsafe { &*(slot.as_ptr() as *const AtomicU64) };
      return RecordHeader::try_seal(word, invalidate);
    }
    if let Ok(mut rec_mut) = RecordMut::from_slice_mut(slot) {
      rec_mut.try_seal(invalidate)
    } else {
      false
    }
  }

  /// 尝试在可变区链内原地复活墓碑记录（严格对照 C# Garnet InternalUpsert.cs:127 TryRevivifyInChain & RecordDataHeader.cs:FillerWords）
  ///
  /// Upsert 与 RMW 两臂的链内复活内核为同一 rust 单点（C# 两侧各自实现，
  /// rust 折叠共用）：
  /// - libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs:TryRevivifyInChain
  /// - libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:TryRevivifyInChain
  ///
  /// 若记录处于内存可变区且为墓碑记录，校验 expected_key 匹配且新值可被
  /// [RecordHeader::slack_for_val_len] 单点容量门吸纳的前提下，原子覆写值并清除墓碑
  /// 标记，富余空间自动吸纳转换为高精度松弛填充，无需分配新 Tail 槽位、无需换页、无需修改哈希索引指针。
  ///
  /// 值源泛型 [`ValSrc`]（语义同 [`Self::try_update_in_place`]）
  pub fn try_revivify_in_chain<V: ValSrc + ?Sized>(
    &self,
    addr: u64,
    expected_key: &[u8],
    new_val: &V,
  ) -> Result<bool> {
    // 版本推进窗口期禁用原位复活（对标 C# CanRevivify 的 IsInNewVersion 抑制臂）：
    // 复活改写的是快照可能已收录的墓碑槽位，恢复内核按记录位+地址下界回滚认不到
    // 这种「老记录新内容」形态，双重生效面与 undoNextVersion 回滚锚点同被侵蚀
    if self.is_version_shift_open() {
      return Ok(false);
    }
    self.with_mutable_record(addr, expected_key, false, |rec_mut| {
      // 必须为墓碑记录，且新值可被当前槽位以松弛填充形式吸纳（与原位更新同一容量门）
      let new_len = new_val.val_len();
      if !rec_mut.is_tombstone() || rec_mut.slack_for_val_len(new_len).is_none() {
        return Ok(false);
      }
      rec_mut.revivify_with_slack(new_val)?;
      Ok(true)
    })
  }

  /// 在复活池回收的物理槽位上就地覆写记录（严格对照 C# Garnet BlockAllocate.cs:77-80 TryTakeFreeRecord）
  ///
  /// 若槽位大小超出记录对齐逻辑尺寸，富余空间优先吸纳为记录松弛填充（FillerWords，
  /// 词粒度），使整条记录物理尺寸精确覆盖整个 `slot_size`——对标 C#
  /// `RecordSizeInfo.ActualInlineRecordSize` 恒等于槽位整段 + `InitializeForRevivification`
  /// （RecordDataHeader.cs:629 `AllocatedInlineRecordSize = recordLength`）的松弛留存语义：
  /// 复活槽位整段经填充保留给本记录，供后续原位增长吸纳，零残片。
  ///
  /// 富余超过填充表示上限 [MAX_FILLER_BYTES]（255 词 = 2040 字节）时，按 C#
  /// `ComputeFillerWordsOrSplit` → `SplitOverflowingFiller`（RecordDataHeader.cs:462/:509）
  /// 分裂：记录内保留 [REVIV_SPLIT_RETAIN_FILLER_BYTES]（64 词，对位 RecordSplitRetainFillerWords），
  /// 切出剩余块并以 `Ok(Some((pad_addr, pad_size)))` 回传（对标 C# 明确要求调用层经
  /// `TryTransferToFreeList` 将切出块归还空闲列表，RecordDataHeader.cs:425 TODO / Helpers.cs:124）；
  /// 调用方必须把该块放回 reviv 池，否则切出块成为已脱钩槽位内的孤儿死内存。
  /// `Ok(None)` 表示无切出块（整槽吃满或富余全数吸纳为松弛填充）。
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
  pub fn revivify_record_at<V: ValSrc + ?Sized>(
    &self,
    args: &super::RevivifyArgs<'_, V>,
  ) -> Result<Option<(u64, u32)>> {
    let super::RevivifyArgs {
      addr,
      slot_size,
      key,
      val,
      prev_addr,
      is_tombstone,
      in_new_version,
    } = *args;
    if !self.addresses.is_mutable(addr) {
      return Err(Error::AddressOutOfRange {
        addr,
        begin: self.addresses.begin(),
        tail: self.addresses.tail(),
      });
    }

    let val_len = val.val_len();
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

    let mut header = RecordHeader::new(prev_addr, key.len() as u32, val_len as u32, is_tombstone)?;
    // 复活槽位纪元位与尾部追加同一单点裁决（对标 C# RecordInfo.cs
    // :InitializeForRevivification 的 inNewVersion 形参——复活出的新记录同样
    // 属版本推进窗口的新版记录，须携带位供恢复内核回滚）；位值由分配口在
    // 池取/暂存复用成功点紧贴单读后自 [`HybridLog::version_shift_word`] 同源
    // 下传（与 AOF 版本戳同一次读取值，读点下移反映落笔时刻窗口态），本方法
    // 不做第二点窗口读
    header.set_in_new_version(in_new_version);
    let kv_end = HEADER_SIZE + key.len() + val_len;
    let remaining = slot_size - rec_size;

    // 1. RecordInfo 字原子发布（新前驱地址 + 墓碑标记，链条一致性先行）
    // SAFETY: 记录 8 字节对齐不变式保证槽位头两字对齐
    unsafe { &*(slot_buf.as_ptr() as *const AtomicU64) }
      .store(header.prev_address, Ordering::Release);

    // 2. 键值字节落笔 + 隐式对齐填充清零（值源经 [ValSrc] 单点直写，支持分段源）
    slot_buf[HEADER_SIZE..HEADER_SIZE + key.len()].copy_from_slice(key);
    val.write_val(&mut slot_buf[HEADER_SIZE + key.len()..kv_end]);
    slot_buf[kv_end..rec_size].fill(0);

    // 切出的剩余块（对标 C# SplitOverflowingFiller 的切出记录），交由调用层归池
    debug_assert!(remaining.is_multiple_of(RECORD_ALIGNMENT));
    let pad = if remaining > MAX_FILLER_BYTES {
      // 富余超填充表示上限：按 C# 分裂语义保留 64 词松弛填充，切出其余整块回传。
      // remaining ≥ 2048 时切出块恒 ≥ 1536 字节，足容纳 Pad 头且为整词
      let pad_size = remaining - REVIV_SPLIT_RETAIN_FILLER_BYTES;
      let pad_off = rec_size + REVIV_SPLIT_RETAIN_FILLER_BYTES;
      header.set_filler_bytes(REVIV_SPLIT_RETAIN_FILLER_BYTES);
      slot_buf[rec_size..pad_off].fill(0);
      let pad_header = RecordHeader::pad(pad_size);
      slot_buf[pad_off..pad_off + HEADER_SIZE].copy_from_slice(&pad_header.to_bytes());
      Some((addr + pad_off as u64, pad_size as u32))
    } else if remaining > 0 {
      // 富余在填充表示上限内（对齐不变式下恒为 8 字节整词）：全数吸纳为记录松弛填充，
      // 物理尺寸精确覆盖整个槽位（填充区清零，绝不解释），无切出块
      header.set_filler_bytes(remaining);
      slot_buf[rec_size..].fill(0);
      None
    } else {
      None
    };

    // 3. RDH 原子字单次发布完整新布局（filler + key_len + val_len 同字）
    // SAFETY: 记录 8 字节对齐不变式保证槽位头两字对齐
    unsafe { &*(slot_buf.as_ptr().add(RDH_WORD_OFFSET) as *const AtomicU64) }
      .store(header.rdh_word, Ordering::Release);

    Ok(pad)
  }

  /// 可变区原位操作的统一内核：加页写锁 → Double-check（可变区 + 页就绪）→ 解析 → 键匹配后执行闭包
  ///
  /// 任一前置条件不满足即返回 `Ok(degraded)`，供调用方降级为 RCU 追加写；
  /// `degraded` 由调用方给定（bool 路径为 false，Option 路径为 None）。
  ///
  /// 降级作为原位操作退化的物理准入，统一承接上层存储引擎的各种原位修改降级需求。
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
    // 版本推进窗口内原位修改冻结（1:1 对标 C# Helpers.cs:IsFrozen：
    // `Ctx.IsInV1 && (logicalAddress <= startLogicalAddress ||
    // !srcRecordInfo.IsInNewVersion)`）——携带旧纪元位的记录（快照收录侧）即便
    // 等长也一律降级为 RCU 尾部追加，由携带本轮纪元位的新记录承接效果、待恢复
    // 内核按位回滚；否则「快照捕获字节 + AOF 重放」将对同一记录双重生效
    // （本仓票面的等长原位 INCR 即此形态）。位项与窗口态读的先后序保证无撕裂：
    // 本判定读恒后于宿主写入口的合字读（见 HybridLog::version_shift 一致性契约）
    if self.is_frozen_by_version_shift(addr, rec_mut.is_in_new_version()) {
      return Ok(degraded);
    }

    f(&mut rec_mut)
  }
}
