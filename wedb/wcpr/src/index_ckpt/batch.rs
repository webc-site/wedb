//! 对齐批缓冲域：64 字节缓存行对齐的 32KB 批缓冲区及 DirectIO 写出/读入状态机。
//!
//! [`BatchWriter`] 逐桶序列化、满批整块移交内核定位写入并流式累积 CRC32，[`BatchReader`]
//! 按批定位读入、逐桶还原；尺寸常量与槽位净化属格式域 [`super::codec`]。

use std::{
  alloc::{Layout, alloc_zeroed, handle_alloc_error},
  mem::MaybeUninit,
  ops::{Deref, DerefMut},
  slice::from_raw_parts_mut,
  sync::atomic::{AtomicU64, Ordering},
};

use compio::{
  buf::{BufResult, IntoInner, IoBuf, IoBufMut, SetLen},
  fs::File,
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use wbase::crc::Crc32Hasher;
use windex::{ENTRIES_PER_BUCKET, HashBucket, HashBucketEntry};

use crate::{
  error::{Error, Result},
  index_ckpt::codec::{
    BATCH_BYTES, BUCKET_BYTES, resolve_read_cache, sanitize_data_slot, sanitize_overflow_slot,
  },
};

/// 64 字节缓存行对齐的 32KB 批处理缓冲区结构体
#[repr(C, align(64))]
struct AlignedBatch([u8; BATCH_BYTES]);

impl AlignedBatch {
  /// 堆上直接按 64 字节对齐分配置零内存，消除栈上临时大数组拷贝与栈溢出风险
  #[inline]
  fn new_boxed() -> Box<Self> {
    let layout = Layout::new::<Self>();
    // SAFETY: Layout 大小为 BATCH_BYTES (32KB)，对齐为 64 字节，满足内存分配约束。
    // 分配后经由 Box::from_raw 托管，析构时自动使用相同的 Layout 释放。
    unsafe {
      let ptr = alloc_zeroed(layout) as *mut Self;
      if ptr.is_null() {
        handle_alloc_error(layout);
      }
      Box::from_raw(ptr)
    }
  }
}

impl Deref for AlignedBatch {
  type Target = [u8; BATCH_BYTES];
  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl DerefMut for AlignedBatch {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl IoBuf for AlignedBatch {
  #[inline]
  fn as_init(&self) -> &[u8] {
    &self.0
  }
}

impl SetLen for AlignedBatch {
  unsafe fn set_len(&mut self, len: usize) {
    // 固定容量缓冲：初始化长度恒为 BATCH_BYTES，有效字节数由各使用方的 cursor 跟踪
    debug_assert!(len <= BATCH_BYTES);
  }
}

impl IoBufMut for AlignedBatch {
  fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
    let ptr = self.0.as_mut_ptr();
    // SAFETY: 数组已完全初始化，[u8; N] 与 [MaybeUninit<u8>; N] 布局一致，原地重解释安全
    unsafe { from_raw_parts_mut(ptr.cast(), BATCH_BYTES) }
  }
}

/// 批量哈希桶写入器（利用 64 字节对齐的 32KB 批处理缓冲区大幅减少 I/O 调用并充分发挥硬件 CRC32 吞吐）
pub(crate) struct BatchWriter<'a, F> {
  file: &'a mut File,
  hasher: &'a mut Crc32Hasher,
  /// 32KB 批处理缓冲；compio 完成式 I/O 要求缓冲所有权，flush 期间瞬时移交内核后原样取回复用
  buf: Option<Box<AlignedBatch>>,
  cursor: usize,
  /// 下一笔写入的文件偏移（compio File 为定位 I/O，无内部游标）
  pos: u64,
  /// ReadCache 易失指针解析闭包（见 [resolve_read_cache]）
  rc_skip: &'a F,
}

impl<'a, F: Fn(u64) -> u64> BatchWriter<'a, F> {
  pub(crate) fn new(
    file: &'a mut File,
    hasher: &'a mut Crc32Hasher,
    start_pos: u64,
    rc_skip: &'a F,
  ) -> Self {
    Self {
      file,
      hasher,
      buf: Some(AlignedBatch::new_boxed()),
      cursor: 0,
      pos: start_pos,
      rc_skip,
    }
  }

  #[inline]
  pub(crate) async fn write_bucket(&mut self, bucket: &HashBucket) -> Result<()> {
    // 先解析全部槽位值（数据槽位可能触发 rc 断链复核重读），再取 batch 可变借用
    // 写字节，规避「缓冲可变借用期间回调 &self」的借用冲突
    let mut vals = [0u64; ENTRIES_PER_BUCKET];
    for (i, slot) in bucket.entries.iter().enumerate() {
      vals[i] = if i == HashBucket::OVERFLOW_INDEX {
        // 溢出槽位：剥离高 16 位并发自旋锁瞬态标记，仅保留低 48 位溢出桶索引
        sanitize_overflow_slot(slot.load(Ordering::Acquire))
      } else {
        sanitize_data_slot(self.resolve_slot(slot), None)
      };
    }

    let offset = self.cursor;
    // SAFETY: 缓冲仅在 flush_batch 的 await 期间被移出，该函数恢复缓冲后才返回，
    // 此处控制流上缓冲必然在位
    let batch = unsafe { self.buf.as_deref_mut().unwrap_unchecked() };
    let target = &mut batch[offset..offset + BUCKET_BYTES];
    let (chunks, _) = target.as_chunks_mut::<8>();
    for (chunk, val) in chunks.iter_mut().zip(vals.iter()) {
      *chunk = val.to_le_bytes();
    }

    self.cursor += BUCKET_BYTES;
    if self.cursor == BATCH_BYTES {
      self.flush_batch().await?;
    }
    Ok(())
  }

  /// 解析单个数据槽位为可落盘值（ReadCache 易失指针经 rc_skip 顺链回写，见
  /// [resolve_read_cache]）
  ///
  /// 断链复核（对标 C# ReadCache 快照路径依赖纪元固定的兜底差异）：槽位原子加载与
  /// rc 链解析非原子——若 CleanseHashChain（环形换装前的链恢复）恰在该间隙把槽位
  /// CAS 回主日志地址，首次解析会因读缓存记录已被清空而误报断链，直接落 0 将把
  /// 存活键从检查点静默丢弃。故对带 ReadCache 位的断链结果重读槽位一次：值已变
  /// （并发清链已生效）则以新值重新解析；值未变则链真断（记录已被覆盖失效），
  /// 交由下游净化归零。
  #[inline]
  fn resolve_slot(&self, slot: &AtomicU64) -> u64 {
    let raw = slot.load(Ordering::Acquire);
    let resolved = resolve_read_cache(self.rc_skip, raw);
    if raw != 0 && raw & HashBucketEntry::READ_CACHE_BIT != 0 && resolved == 0 {
      let fresh = slot.load(Ordering::Acquire);
      if fresh != raw {
        return resolve_read_cache(self.rc_skip, fresh);
      }
    }
    resolved
  }

  #[inline]
  pub(crate) async fn write_zero_bucket(&mut self) -> Result<()> {
    let offset = self.cursor;
    // SAFETY: 同 write_bucket，缓冲必然在位
    let batch = unsafe { self.buf.as_deref_mut().unwrap_unchecked() };
    batch[offset..offset + BUCKET_BYTES].fill(0);
    self.cursor += BUCKET_BYTES;
    if self.cursor == BATCH_BYTES {
      self.flush_batch().await?;
    }
    Ok(())
  }

  /// 将已积累的有效字节整块移交内核定位写入，完成后缓冲原样取回复用（零二次搬运）
  #[inline]
  async fn flush_batch(&mut self) -> Result<()> {
    if self.cursor > 0 {
      // SAFETY: 缓冲必然在位（下方 take 与恢复之间无对 self 的其他访问）
      self
        .hasher
        .update(&unsafe { self.buf.as_deref().unwrap_unchecked() }[..self.cursor]);
      let BufResult(res, buf) = self
        .file
        .write_all_at(
          unsafe { self.buf.take().unwrap_unchecked() }.slice(..self.cursor),
          self.pos,
        )
        .await;
      self.buf = Some(buf.into_inner());
      res?;
      self.pos += self.cursor as u64;
      self.cursor = 0;
    }
    Ok(())
  }

  pub(crate) async fn finish(mut self) -> Result<()> {
    self.flush_batch().await
  }
}

/// 批量哈希桶读取器（32KB 块级读取与流式 CRC 校验，零临时分配与直接还原）
pub(crate) struct BatchReader<'a> {
  file: &'a mut File,
  hasher: &'a mut Crc32Hasher,
  /// 32KB 批处理缓冲；compio 完成式 I/O 要求缓冲所有权，refill 期间瞬时移交内核后原样取回
  buf: Option<Box<AlignedBatch>>,
  valid_bytes: usize,
  cursor: usize,
  remaining_bytes: u64,
  /// 下一笔读取的文件偏移（compio File 为定位 I/O，无内部游标）
  pos: u64,
}

impl<'a> BatchReader<'a> {
  pub(crate) fn new(
    file: &'a mut File,
    hasher: &'a mut Crc32Hasher,
    start_pos: u64,
    total_data_bytes: u64,
  ) -> Self {
    Self {
      file,
      hasher,
      buf: Some(AlignedBatch::new_boxed()),
      valid_bytes: 0,
      cursor: 0,
      remaining_bytes: total_data_bytes,
      pos: start_pos,
    }
  }

  /// 定位读取下一批数据填充缓冲（缓冲所有权移交内核，完成后原样取回并累积 CRC）
  async fn refill(&mut self) -> Result<()> {
    let to_read = (self.remaining_bytes as usize).min(BATCH_BYTES);
    if to_read == 0 {
      return Err(Error::InvalidIndexCkpt("意外到达文件尾部".into()));
    }
    // SAFETY: 缓冲仅在 refill 的 await 期间被移出，恢复后此函数才可能被再次调用
    let BufResult(res, buf) = self
      .file
      .read_exact_at(
        unsafe { self.buf.take().unwrap_unchecked() }.slice(..to_read),
        self.pos,
      )
      .await;
    self.buf = Some(buf.into_inner());
    res?;
    self.pos += to_read as u64;
    self.remaining_bytes -= to_read as u64;
    // SAFETY: 缓冲已归还在位
    self
      .hasher
      .update(&unsafe { self.buf.as_deref().unwrap_unchecked() }[..to_read]);
    self.valid_bytes = to_read;
    self.cursor = 0;
    Ok(())
  }

  #[inline]
  pub(crate) async fn read_bucket_into(
    &mut self,
    bucket: &HashBucket,
    tail: Option<u64>,
  ) -> Result<()> {
    if self.cursor == self.valid_bytes {
      self.refill().await?;
    }

    let offset = self.cursor;
    // SAFETY: 同 refill，缓冲必然在位
    let src = &unsafe { self.buf.as_deref().unwrap_unchecked() }[offset..offset + BUCKET_BYTES];
    let (chunks, _) = src.as_chunks::<8>();
    // 读取前 7 个数据槽位
    for (slot, slot_bytes) in bucket.entries[..HashBucket::DATA_ENTRIES]
      .iter()
      .zip(chunks.iter())
    {
      let raw = u64::from_le_bytes(*slot_bytes);
      slot.store(sanitize_data_slot(raw, tail), Ordering::Relaxed);
    }
    // 读取第 7 个槽位（溢出指针）
    let overflow_raw = u64::from_le_bytes(chunks[HashBucket::OVERFLOW_INDEX]);
    bucket.entries[HashBucket::OVERFLOW_INDEX]
      .store(sanitize_overflow_slot(overflow_raw), Ordering::Relaxed);

    self.cursor += BUCKET_BYTES;
    Ok(())
  }
}
