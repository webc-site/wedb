use core::{
  ptr::copy_nonoverlapping,
  sync::atomic::{Ordering, fence},
};

use log::trace;

use crate::{
  error::{Error, Result},
  header::{
    HEADER_SIZE, RDH_WORD_OFFSET, RDH_WORD_SIZE, RECORD_ALIGNMENT, RecordHeader,
    bits::{KEY_LEN_BITS, align_record_size},
  },
};

/// 计算指定键长和值长下记录的对齐逻辑字节数（头 + 键 + 值，向上对齐到
/// [crate::RECORD_ALIGNMENT] 边界，差值为隐式对齐填充）
///
/// 对标 C# RecordDataHeader.GetAlignedComponentSum：记录分配尺寸由对齐后的组件和构成，
/// 保证页内每条记录头 8 字节对齐（RDH 原子字单次原子发布的前提）。
#[inline]
pub const fn record_size(key_len: usize, val_len: usize) -> usize {
  align_record_size(HEADER_SIZE.saturating_add(key_len).saturating_add(val_len))
}

/// 安全计算指定键长和值长下头 + 键 + 值的未对齐区段字节数，若溢出 usize 则返回 None
///
/// 值数据的精确结束边界（不含隐式对齐填充），对标 C# GetUnalignedComponentSum 口径。
#[inline]
pub const fn checked_record_size(key_len: usize, val_len: usize) -> Option<usize> {
  let Some(s) = HEADER_SIZE.checked_add(key_len) else {
    return None;
  };
  s.checked_add(val_len)
}

/// 最大可表示的键长度（24 位键长位段上限：16MB - 1）
pub const MAX_KEY_LEN: usize = (1usize << KEY_LEN_BITS) - 1;

/// 校验键值长度并构造记录头（返回头与记录对齐逻辑总大小）
///
/// 地址 48 位有效性由 [RecordHeader::new] 内部统一校验，此处不再重复检查。
#[inline]
fn build_header(
  prev_addr: u64,
  key: &[u8],
  val: &[u8],
  is_tombstone: bool,
) -> Result<(RecordHeader, usize)> {
  if key.len() > MAX_KEY_LEN {
    return Err(Error::KeyLengthOverflow(key.len()));
  }
  if val.len() > u32::MAX as usize {
    return Err(Error::ValueLengthOverflow(val.len()));
  }

  let header = RecordHeader::new(prev_addr, key.len() as u32, val.len() as u32, is_tombstone)?;
  let total_size = record_size(key.len(), val.len());
  Ok((header, total_size))
}

/// 将 16 字节头 + 键 + 值连续写入目标裸指针（供切片编码与向量编码共享的底层写入实现）
///
/// 头两字分次有序发布（键值 → RecordInfo 字 → Release 屏障 → RDH 字），与
/// [RecordHeader::from_ptr_atomic] 的反向读序配对，论证见方法内注释。
///
/// # Safety
/// 调用方必须保证 `ptr` 起始的 `HEADER_SIZE + key.len() + val.len()` 字节可写，
/// 且写入区间与 key/val 指向的内存互不重叠。
#[inline]
unsafe fn write_record_unchecked(ptr: *mut u8, header: &RecordHeader, key: &[u8], val: &[u8]) {
  let hdr_bytes = header.to_bytes();
  unsafe {
    copy_nonoverlapping(key.as_ptr(), ptr.add(HEADER_SIZE), key.len());
    copy_nonoverlapping(val.as_ptr(), ptr.add(HEADER_SIZE + key.len()), val.len());
    // 16 字节头一次性 memcpy 的两半字可见次序平台不可控（弱序架构与编译器均可换序），
    // 读者可能先见新 RDH 后见零 RecordInfo 字，把在途槽位误读为「prev=0、无墓碑、布局
    // 真实」的假记录。故 RecordInfo 字先落、RDH 收尾字最后落，中间一条 Release 屏障
    // 同时定序「键值字节 → 头两字」——对标 C# RecordInfo.WriteInfo 先行 +
    // RecordDataHeader.Initialize 单字收尾的双阶段发布，与本仓 whlog 原位复活
    // `revivify_record_at` 的三步协议同形。屏障计数与旧实现一致（每条记录一次）。
    copy_nonoverlapping(hdr_bytes.as_ptr(), ptr, RDH_WORD_OFFSET);
    fence(Ordering::Release);
    copy_nonoverlapping(
      hdr_bytes.as_ptr().add(RDH_WORD_OFFSET),
      ptr.add(RDH_WORD_OFFSET),
      RDH_WORD_SIZE,
    );
  }
}

/// 预占槽位的最小 extent 头发布（在途两阶段协议第一拍，对标 C# 新记录先写头协议）
///
/// C# 新记录落笔前先把头写成「扫描器可见的关闭形态」再填键值：RecordInfo.WriteInfo
/// 先调 InitializeForNewRecord 置 `word = kSealedBitMask`（Sealed + Invalid，注释
/// 「Otherwise, Scan could return partial records」），RDH 此间为零，扫描器据
/// `SkipOnScan` 跳该记录、按 `allocatedSize` 推进游标（SpanByteScanIterator.GetNext
/// 「跳记录不跳页」），零 RDH 另有 GetRecordLength 的 FixedHeaderSize 最小长度守卫
/// （RecordDataHeader.InitializeForNewRecord 注释），两 gate 合并的效果即
/// 「游标决不整页跳过在途槽位」。
///
/// 本口为该协议第一拍在本仓的单点实现，供分配器落盘面与编解码面共用：被
/// [`crate::RecordHeader`] 的 Pad 形态承载（C# 二符号经 ignore 登记为单写发布
/// 整体取代，无逐一对应的 rust 体，叙述不持符号锚）。
///
/// 本实现以既有 Pad 填充头形态一条 store 承载同一协议：单条 8 字节对齐写发布
/// [RecordHeader::pad] 的 RDH 字（`key_len = PAD_KEY_LEN`、`val_len = rec_size -
/// HEADER_SIZE`），RecordInfo 字保持槽位置零后的零值。选择 Pad 形态而非另立密封位的
/// 理由：SEALED_BIT 在本仓是复活池槽位锁的纯易失标记（持久化路径绝不得置位），而 Pad
/// 已是扫描器按 `HEADER_SIZE + val_len` 精确推进的唯一跳步机制，在途槽位由此自动获得
/// 「按物理尺寸前进」，既不引入第二套跳过规则、也不新增头位段语义；崩溃残留该形态时
/// 恢复扫描同样精确越过空洞，页内其余已落盘记录不再永久丢失。比 C# 的 16 字节最小长度
/// 守卫更强：游标一步越过的正是整个在途槽位，绝不把槽内字节当头部误读。
/// 读者可观察的一致形态只有 `(0, Pad)` / `(real, Pad)`（均判为 Pad，尺寸恒为本槽）与
/// 终态 `(real, real)`。
///
/// # Safety
/// `ptr` 必须 8 字节对齐并指向调用方已独占预留（tail CAS 胜出或换页锁持有）的
/// `[ptr, ptr + rec_size)` 槽位首字节，`rec_size >= HEADER_SIZE` 且为
/// [crate::RECORD_ALIGNMENT] 整数倍。
#[inline]
pub unsafe fn publish_extent_header(ptr: *mut u8, rec_size: usize) {
  debug_assert!(rec_size >= HEADER_SIZE);
  debug_assert_eq!(rec_size % RECORD_ALIGNMENT, 0);
  debug_assert_eq!(ptr as usize % RECORD_ALIGNMENT, 0);
  let extent = RecordHeader::pad(rec_size).to_bytes();
  // SAFETY: 调用方契约保证 ptr 8 字节对齐且 [ptr, ptr + rec_size) 独占可写；
  // 单条对齐字写入与 C# RecordDataHeader.Initialize 的单字写同构
  unsafe {
    copy_nonoverlapping(
      extent.as_ptr().add(RDH_WORD_OFFSET),
      ptr.add(RDH_WORD_OFFSET),
      RDH_WORD_SIZE,
    );
  }
}

/// 将键值对及元数据编码写入目标字节切片
///
/// 返回写入的字节总数（即记录对齐逻辑大小）。
/// 若目标切片容量不足，返回 `Error::BufferTooShort`。
/// 若键长超出 24 位上限、值长超出 `u32` 上限，或地址超出 48 位，返回相应错误。
///
/// C# RDH 发布完整记录布局（长度/内联位/填充）的编码落点：
/// libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:Initialize
pub fn encode_to_slice(
  dst: &mut [u8],
  prev_addr: u64,
  key: &[u8],
  val: &[u8],
  is_tombstone: bool,
) -> Result<usize> {
  let (header, total_size) = build_header(prev_addr, key, val, is_tombstone)?;
  let Some(buf) = dst.get_mut(..total_size) else {
    return Err(Error::BufferTooShort {
      expected: total_size,
      actual: dst.len(),
    });
  };

  let key_len = header.key_len();
  let val_len = header.val_len();
  trace!(
    "编码记录: prev_addr={prev_addr:#x}, key_len={key_len}, val_len={val_len}, is_tombstone={is_tombstone}, total_size={total_size}"
  );

  // SAFETY: buf 已由 get_mut 预先校验长度为 total_size = 对齐(HEADER_SIZE + key.len() + val.len())，
  // dst 独占借用保证与 key/val 互不重叠。
  unsafe { write_record_unchecked(buf.as_mut_ptr(), &header, key, val) };

  Ok(total_size)
}

/// 尝试将键值对及元数据编码为全新分配的 `Vec<u8>`（单次精准容量堆分配）
///
/// 若键长超出 24 位上限、值长超出 `u32` 上限，或地址超出 48 位，返回相应错误。
/// （`try_` 前缀表明可能失败，与 wval 编解码器的 `try_` 系方法命名约定一致）
pub fn try_encode_to_vec(
  prev_addr: u64,
  key: &[u8],
  val: &[u8],
  is_tombstone: bool,
) -> Result<Vec<u8>> {
  let (header, total_size) = build_header(prev_addr, key, val, is_tombstone)?;

  let mut buf = Vec::with_capacity(total_size);
  // SAFETY: buf 已预留 total_size = 对齐(HEADER_SIZE + key.len() + val.len()) 字节空间，
  // 各写入区间互不重叠；逻辑区段之后至 total_size 的隐式对齐填充先清零，
  // 保证 set_len 暴露的全部字节均已初始化。
  unsafe {
    write_record_unchecked(buf.as_mut_ptr(), &header, key, val);
    let kv_end = HEADER_SIZE + key.len() + val.len();
    buf
      .as_mut_ptr()
      .add(kv_end)
      .write_bytes(0, total_size - kv_end);
    buf.set_len(total_size);
  }
  Ok(buf)
}
