use core::{
  ptr::copy_nonoverlapping,
  slice::from_raw_parts_mut,
  sync::atomic::{AtomicU64, Ordering},
};

use log::trace;

use crate::{
  error::{Error, Result},
  header::{
    HEADER_SIZE, RDH_WORD_OFFSET, RECORD_ALIGNMENT, RecordHeader,
    bits::{PAD_KEY_LEN, align_record_size},
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

/// 最大可表示的合法键长度：24 位键长位段顶值 [PAD_KEY_LEN] 保留为换页填充 Pad 哨兵，
/// 合法键长值域与之严格互斥（对标 C#：KeyLength 10 位位段全值域合法、页尾填充由
/// RecordInfo 状态字承载的解耦形态——Rust 全内联无 overflow key，哨兵征用位段顶值，
/// 故合法上限须让出顶值，杜绝真实键长恰为 0xFFFFFF 的记录被读侧误判为 Pad 跳过）
pub const MAX_KEY_LEN: usize = PAD_KEY_LEN as usize - 1;

/// 记录值写入源：连续切片或分段直写源
///
/// 同步快路径单次成形内核的值供给抽象（对标 C#
/// GarnetObjectSerializer.Serialize 经 BinaryObjectSerializer 直写记录
/// value span、无中间整值堆缓冲再转拷的形态，garnet/libs/server/Objects/
/// Types/GarnetObjectSerializer.cs:104）：分段源（如对象信封
/// `[1B 对象标签][payload]`）在记录槽位分配点一次成形，消除中间 `Vec`
/// 整值暂存拷贝；`[u8]` 连续切片实现保持既有路径零额外开销。
pub trait ValSrc {
  /// 值字节长度（容量判定与记录头编码单源）
  fn val_len(&self) -> usize;
  /// 值字节直写目标切片（调用方保证 `dst.len() == val_len()`，实现不得越界）
  fn write_val(&self, dst: &mut [u8]);
  /// 连续切片视图（写监听镜像零拷贝消费）；分段源无连续存储返回 `None`，
  /// 镜像由调用方专用通知口承接，杜绝为镜像再物化整值缓冲
  fn as_val_slice(&self) -> Option<&[u8]> {
    None
  }
}

impl ValSrc for [u8] {
  #[inline(always)]
  fn val_len(&self) -> usize {
    self.len()
  }

  #[inline(always)]
  fn write_val(&self, dst: &mut [u8]) {
    dst.copy_from_slice(self);
  }

  #[inline(always)]
  fn as_val_slice(&self) -> Option<&[u8]> {
    Some(self)
  }
}

impl<const N: usize> ValSrc for [u8; N] {
  #[inline(always)]
  fn val_len(&self) -> usize {
    N
  }

  #[inline(always)]
  fn write_val(&self, dst: &mut [u8]) {
    dst.copy_from_slice(self);
  }

  #[inline(always)]
  fn as_val_slice(&self) -> Option<&[u8]> {
    Some(self)
  }
}

impl ValSrc for Vec<u8> {
  #[inline(always)]
  fn val_len(&self) -> usize {
    self.len()
  }

  #[inline(always)]
  fn write_val(&self, dst: &mut [u8]) {
    dst.copy_from_slice(self);
  }

  #[inline(always)]
  fn as_val_slice(&self) -> Option<&[u8]> {
    Some(self)
  }
}

/// 共享引用转发（`&Vec<u8>` / `&&[u8]` 等双层引用调用形态零成本适配）
impl<T: ValSrc + ?Sized> ValSrc for &T {
  #[inline(always)]
  fn val_len(&self) -> usize {
    (**self).val_len()
  }

  #[inline(always)]
  fn write_val(&self, dst: &mut [u8]) {
    (**self).write_val(dst)
  }

  #[inline(always)]
  fn as_val_slice(&self) -> Option<&[u8]> {
    (**self).as_val_slice()
  }
}

/// 校验键值长度并构造记录头（返回头与记录对齐逻辑总大小）
///
/// 地址 48 位有效性由 [RecordHeader::new] 内部统一校验，此处不再重复检查。
/// `in_new_version` 随头一并与键值同字发布（对标 C# RecordInfo.cs:WriteInfo 的
/// inNewVersion 形参：检查点版本推进窗口内落笔的记录必须在发布前就携带纪元位，
/// 绝无「先发布后补位」的二段窗口——恢复内核的 undoNextVersion 回滚以此为唯一判据）。
#[inline]
fn build_header(
  prev_addr: u64,
  key: &[u8],
  val_len: usize,
  is_tombstone: bool,
  in_new_version: bool,
) -> Result<(RecordHeader, usize)> {
  if key.len() > MAX_KEY_LEN {
    return Err(Error::KeyLengthOverflow(key.len()));
  }
  if val_len > u32::MAX as usize {
    return Err(Error::ValueLengthOverflow(val_len));
  }

  let mut header = RecordHeader::new(prev_addr, key.len() as u32, val_len as u32, is_tombstone)?;
  header.set_in_new_version(in_new_version);
  let total_size = record_size(key.len(), val_len);
  Ok((header, total_size))
}

/// 将 16 字节头 + 键 + 值连续写入目标裸指针（供切片编码与向量编码共享的底层写入实现）
///
/// 头两字按目标对齐分形态发布，与读侧 [`RecordHeader::from_ptr_atomic`]／`is_zero_header`
/// 同口径：
/// - 8 字节对齐（whlog 页内记录槽位，[crate::RECORD_ALIGNMENT] 不变式保证）：经对齐
///   [`AtomicU64`] 单点发布（键值 → RecordInfo 字原子 store → RDH 字原子 Release store），
///   与无锁读者的 Acquire 载入构成 synchronizes-with，消除普通写×原子读的跨线程数据竞争；
/// - 非对齐（独立序列化缓冲／临时 Vec，绝无并发原子读者）：普通 16 字节拷贝落笔，维持
///   纯格式层编解码对 1..=7 字节偏移安全无崩溃的既有契约。
///
/// # Safety
/// 调用方必须保证 `ptr` 起始的 `HEADER_SIZE + key.len() + val.val_len()` 字节可写，
/// 且写入区间与 key / 值源（[`ValSrc`] 的 `write_val` 读侧）指向的内存互不重叠。
#[inline]
unsafe fn write_record_unchecked<V: ValSrc + ?Sized>(
  ptr: *mut u8,
  header: &RecordHeader,
  key: &[u8],
  val: &V,
) {
  let val_len = val.val_len();
  unsafe {
    copy_nonoverlapping(key.as_ptr(), ptr.add(HEADER_SIZE), key.len());
    // SAFETY: ptr 起始 HEADER_SIZE + key.len() + val_len 字节可写（调用方契约）；
    // from_raw_parts_mut 构造值区切片后经 ValSrc 单点落笔
    let dst = from_raw_parts_mut(ptr.add(HEADER_SIZE + key.len()), val_len);
    val.write_val(dst);
    if (ptr as usize).is_multiple_of(RECORD_ALIGNMENT) {
      // 对齐槽位（并发日志页）头两字必须经对齐原子字发布：普通 memcpy 与无锁读者
      // from_ptr_atomic 的 Acquire 载入并发命中同位置即数据竞争 UB（Release fence 只对
      // 原子操作定序、绝不定序普通 store，与之建立不起 synchronizes-with）。故键值先落，
      // RecordInfo 字以对齐 AtomicU64 store 落笔，RDH 收尾字最后以 Release store 发布
      // ——RDH 的 Release 语义保证其 sequenced-before 的全部写（键值 + RecordInfo 字）先于
      // RDH 可见，读侧先 Acquire 载 RDH 即与之构成 synchronizes-with，RecordInfo 字与键值
      // 随之可见。对标 C# RecordInfo.WriteInfo 先行 + RecordDataHeader.Initialize 单字收尾
      // 的双阶段发布，与本仓 whlog 原位复活 `revivify_record_at`、可变区 `publish_rdh`
      // 的原子发布内核完全同形（并发侧唯一一套协议，绝不留普通写的虚构 synchronizes-with）。
      // SAFETY: 已判 ptr 为 RECORD_ALIGNMENT 对齐，ptr 与 ptr + RDH_WORD_OFFSET 均 8 字节对齐
      (&*(ptr as *const AtomicU64)).store(header.prev_address, Ordering::Release);
      (&*(ptr.add(RDH_WORD_OFFSET) as *const AtomicU64)).store(header.rdh_word, Ordering::Release);
    } else {
      // 非对齐目标仅出现于独立序列化缓冲／临时 Vec（无并发原子读者），普通整头拷贝落笔，
      // 与读侧 is_zero_header 非对齐臂同款降级，保住纯格式层编解码的非对齐安全契约
      let hdr_bytes = header.to_bytes();
      copy_nonoverlapping(hdr_bytes.as_ptr(), ptr, HEADER_SIZE);
    }
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
/// 本实现以既有 Pad 填充头形态一条 store 承载同一协议：单条对齐 [`AtomicU64`] Release
/// store 发布 [RecordHeader::pad] 的 RDH 字（`key_len = PAD_KEY_LEN`、`val_len = rec_size -
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
  // SAFETY: 调用方契约保证 ptr 8 字节对齐且 [ptr, ptr + rec_size) 独占可写；
  // Pad 形态 RDH 字经对齐 AtomicU64 单条 Release store 发布，与 revivify_record_at、
  // RecordMut::publish_rdh、write_record_unchecked 同一原子发布内核（对标 C#
  // RecordDataHeader.Initialize 单字写），杜绝普通拷贝与原子读者的数据竞争
  unsafe {
    (&*(ptr.add(RDH_WORD_OFFSET) as *const AtomicU64))
      .store(RecordHeader::pad(rec_size).rdh_word, Ordering::Release);
  }
}

/// 将键值对及元数据编码写入目标字节切片
///
/// 返回写入的字节总数（即记录对齐逻辑大小）。
/// 若目标切片容量不足，返回 `Error::BufferTooShort`。
/// 若键长超出合法上限 [MAX_KEY_LEN]（位段全 1 顶值保留为 Pad 哨兵）、值长超出 `u32` 上限，或地址超出 48 位，返回相应错误。
/// `in_new_version`：版本推进窗口内落笔置位（对标 C# RecordInfo.cs 的 WriteInfo 形参，
/// 由 whlog 可变区追加面依窗口状态单点裁决传入）。
///
/// C# RDH 发布完整记录布局（长度/内联位/填充）的编码落点：
/// libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:Initialize
pub fn encode_to_slice<V: ValSrc + ?Sized>(
  dst: &mut [u8],
  prev_addr: u64,
  key: &[u8],
  val: &V,
  is_tombstone: bool,
  in_new_version: bool,
) -> Result<usize> {
  let (header, total_size) =
    build_header(prev_addr, key, val.val_len(), is_tombstone, in_new_version)?;
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

  // SAFETY: buf 已由 get_mut 预先校验长度为 total_size = 对齐(HEADER_SIZE + key.len() + val_len)，
  // dst 独占借用保证与 key / 值源互不重叠。
  unsafe { write_record_unchecked(buf.as_mut_ptr(), &header, key, val) };

  Ok(total_size)
}

/// 尝试将键值对及元数据编码为全新分配的 `Vec<u8>`（单次精准容量堆分配）
///
/// 若键长超出合法上限 [MAX_KEY_LEN]（位段全 1 顶值保留为 Pad 哨兵）、值长超出 `u32` 上限，或地址超出 48 位，返回相应错误。
/// （`try_` 前缀表明可能失败，与 wval 编解码器的 `try_` 系方法命名约定一致）
///
/// 本面向序列化缓冲的编码口恒不携带版本纪元位：可变日志原位落笔面唯一经
/// [`encode_to_slice`]（whlog 依窗口状态传入 `in_new_version`），本口产物不是
/// 日志页内记录，对标 C# 序列化记录（RecordSerializationInfo）不参与版本判定。
pub fn try_encode_to_vec(
  prev_addr: u64,
  key: &[u8],
  val: &[u8],
  is_tombstone: bool,
) -> Result<Vec<u8>> {
  let (header, total_size) = build_header(prev_addr, key, val.len(), is_tombstone, false)?;

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
