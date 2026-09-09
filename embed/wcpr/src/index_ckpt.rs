use std::{
  alloc::{Layout, alloc_zeroed, handle_alloc_error},
  fs::remove_file,
  mem::MaybeUninit,
  ops::{Deref, DerefMut},
  path::Path,
  slice::from_raw_parts_mut,
  sync::atomic::{AtomicU64, Ordering},
};

use compio::{
  buf::{BufResult, IntoInner, IoBuf, IoBufMut, SetLen},
  fs::{File, metadata, rename},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use log::debug;
use wbase::crc::Crc32Hasher;
use windex::{ENTRIES_PER_BUCKET, HashBucket, HashBucketEntry, HashIndex};

use super::{
  error::{Error, Result},
  meta::{IndexMeta, index_filename, index_tmp_filename},
};

/// 索引快照二进制文件魔数
const INDEX_MAGIC: &[u8; 8] = b"WEDB_IDX";
/// 索引快照魔数 64 位整型（用于单周期快速比对）
const INDEX_MAGIC_U64: u64 = u64::from_le_bytes(*INDEX_MAGIC);
/// 索引快照二进制文件版本号
const INDEX_VERSION: u32 = 1;
/// 头部总大小（64 字节对齐）
const HEADER_SIZE: usize = 64;
/// 头部中 CRC32 校验和字段的字节偏移
const HEADER_CRC_OFFSET: u64 = 12;
/// 单个哈希桶序列化字节数（64 字节对齐）
const BUCKET_BYTES: usize = 64;
/// 批处理包含的哈希桶数量（512 个桶，共 32KB，匹配现代 CPU L1 缓存并最大化 SIMD 硬件 CRC32 吞吐）
const BATCH_BUCKETS: usize = 512;
/// 批处理缓冲区字节数（32KB）
const BATCH_BYTES: usize = BATCH_BUCKETS * BUCKET_BYTES;

/// 64 字节定长索引快照二进制文件头
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexCkptHeader {
  pub version: u32,
  pub crc: u32,
  pub token: u128,
  pub num_buckets: u64,
  pub overflow_count: u64,
  pub entry_count: u64,
}

impl IndexCkptHeader {
  pub const SIZE: usize = HEADER_SIZE;

  /// 编码为 64 字节定长数组（小端编码，const fn，零堆分配）
  #[inline(always)]
  pub const fn encode(&self) -> [u8; HEADER_SIZE] {
    let v = self.version.to_le_bytes();
    let c = self.crc.to_le_bytes();
    let t = self.token.to_le_bytes();
    let nb = self.num_buckets.to_le_bytes();
    let oc = self.overflow_count.to_le_bytes();
    let ec = self.entry_count.to_le_bytes();

    [
      INDEX_MAGIC[0],
      INDEX_MAGIC[1],
      INDEX_MAGIC[2],
      INDEX_MAGIC[3],
      INDEX_MAGIC[4],
      INDEX_MAGIC[5],
      INDEX_MAGIC[6],
      INDEX_MAGIC[7],
      v[0],
      v[1],
      v[2],
      v[3],
      c[0],
      c[1],
      c[2],
      c[3],
      t[0],
      t[1],
      t[2],
      t[3],
      t[4],
      t[5],
      t[6],
      t[7],
      t[8],
      t[9],
      t[10],
      t[11],
      t[12],
      t[13],
      t[14],
      t[15],
      nb[0],
      nb[1],
      nb[2],
      nb[3],
      nb[4],
      nb[5],
      nb[6],
      nb[7],
      oc[0],
      oc[1],
      oc[2],
      oc[3],
      oc[4],
      oc[5],
      oc[6],
      oc[7],
      ec[0],
      ec[1],
      ec[2],
      ec[3],
      ec[4],
      ec[5],
      ec[6],
      ec[7],
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
    ]
  }

  /// 从切片解码索引快照头（const fn，校验魔数；不足 64 字节或魔数不匹配返回 None）
  #[inline(always)]
  pub const fn decode_opt(src: &[u8]) -> Option<Self> {
    if let Some(chunk) = src.first_chunk::<HEADER_SIZE>() {
      let magic = u64::from_le_bytes([
        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
      ]);
      if magic != INDEX_MAGIC_U64 {
        return None;
      }
      let version = u32::from_le_bytes([chunk[8], chunk[9], chunk[10], chunk[11]]);
      let crc = u32::from_le_bytes([chunk[12], chunk[13], chunk[14], chunk[15]]);
      let token = u128::from_le_bytes([
        chunk[16], chunk[17], chunk[18], chunk[19], chunk[20], chunk[21], chunk[22], chunk[23],
        chunk[24], chunk[25], chunk[26], chunk[27], chunk[28], chunk[29], chunk[30], chunk[31],
      ]);
      let num_buckets = u64::from_le_bytes([
        chunk[32], chunk[33], chunk[34], chunk[35], chunk[36], chunk[37], chunk[38], chunk[39],
      ]);
      let overflow_count = u64::from_le_bytes([
        chunk[40], chunk[41], chunk[42], chunk[43], chunk[44], chunk[45], chunk[46], chunk[47],
      ]);
      let entry_count = u64::from_le_bytes([
        chunk[48], chunk[49], chunk[50], chunk[51], chunk[52], chunk[53], chunk[54], chunk[55],
      ]);
      Some(Self {
        version,
        crc,
        token,
        num_buckets,
        overflow_count,
        entry_count,
      })
    } else {
      None
    }
  }
}

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

/// 数据槽位序列化与反序列化净化：
/// 清除未提交的试探性标记（bit 63）、易失 ReadCache 虚拟指针（bit 47）
/// 以及超出一致性截断点 tail 的历史条目（截断归零）
#[inline(always)]
fn sanitize_data_slot(raw: u64, tail: Option<u64>) -> u64 {
  const INVALID_FLAGS_MASK: u64 = HashBucketEntry::TENTATIVE_MASK | HashBucketEntry::READ_CACHE_BIT;
  if raw == 0 || (raw & INVALID_FLAGS_MASK) != 0 {
    return 0;
  }
  if tail.is_some_and(|t| (raw & HashBucketEntry::ADDRESS_MASK) >= t) {
    return 0;
  }
  raw
}

/// ReadCache 易失指针写入端解析（对标 C# Tsavorite ReadCache.cs:159 SkipReadCacheBucket）
///
/// 指向读缓存记录的条目不得直接落盘：读缓存在恢复后为空，直接落盘等于丢失该键。
/// C# 在快照时将此类条目顺链回写为首个主日志记录地址后再写入快照副本，且仅换写
/// Address 字段、高 16 位指纹原样保留（HashBucketEntry.cs:49 Address setter 的
/// 掩码换写语义）；Rust 侧由调用方传入解析闭包（`ReadCache::skip_read_cache`），
/// 仅对置有 ReadCache 位的槽位调用，其余槽位零开销透传。闭包契约与 wkv 一致：
/// 接收剥离指纹后的低 48 位地址（保留 ReadCache 位供判链），返回裸主日志地址；
/// 此处仅将解析结果的地址字段拼回原槽位，指纹位不受闭包返回值污染——否则恢复后
/// `find_tag` 按高 16 位比对永失配，该键不可见。解析断链（记录已滑出读缓存窗口）
/// 返回 0，交由下游净化归零（best-effort，与 C# 依赖纪元保护的页固定同级）。
#[inline(always)]
fn resolve_read_cache(rc_skip: &dyn Fn(u64) -> u64, raw: u64) -> u64 {
  if raw != 0 && raw & HashBucketEntry::READ_CACHE_BIT != 0 {
    // 闭包输入与 wkv 调用方 `entry.address()` 同契约：低 48 位地址、保留 RC 位供判链
    match (rc_skip)(raw & HashBucketEntry::ADDRESS_MASK) {
      // 断链：整体归零交下游净化
      0 => 0,
      // 仅换写地址字段，原槽位高 16 位（指纹 + 试探态）原样保留
      real => (raw & !HashBucketEntry::ADDRESS_MASK) | (real & HashBucketEntry::ADDRESS_MASK),
    }
  } else {
    raw
  }
}

/// 溢出槽位净化：
/// 剥离高 16 位并发自旋锁（Latch）瞬态标记，仅保留低 48 位纯净溢出桶索引
#[inline(always)]
fn sanitize_overflow_slot(raw: u64) -> u64 {
  raw & HashBucketEntry::ADDRESS_MASK
}

/// 批量哈希桶写入器（利用 64 字节对齐的 32KB 批处理缓冲区大幅减少 I/O 调用并充分发挥硬件 CRC32 吞吐）
struct BatchWriter<'a> {
  file: &'a mut File,
  hasher: &'a mut Crc32Hasher,
  /// 32KB 批处理缓冲；compio 完成式 I/O 要求缓冲所有权，flush 期间瞬时移交内核后原样取回复用
  buf: Option<Box<AlignedBatch>>,
  cursor: usize,
  /// 下一笔写入的文件偏移（compio File 为定位 I/O，无内部游标）
  pos: u64,
  /// ReadCache 易失指针解析闭包（见 [resolve_read_cache]）
  rc_skip: &'a dyn Fn(u64) -> u64,
}

impl<'a> BatchWriter<'a> {
  fn new(
    file: &'a mut File,
    hasher: &'a mut Crc32Hasher,
    start_pos: u64,
    rc_skip: &'a dyn Fn(u64) -> u64,
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
  async fn write_bucket(&mut self, bucket: &HashBucket) -> Result<()> {
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
  async fn write_zero_bucket(&mut self) -> Result<()> {
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

  async fn finish(mut self) -> Result<()> {
    self.flush_batch().await
  }
}

/// 批量哈希桶读取器（32KB 块级读取与流式 CRC 校验，零临时分配与直接还原）
struct BatchReader<'a> {
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
  fn new(
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
  async fn read_bucket_into(&mut self, bucket: &HashBucket, tail: Option<u64>) -> Result<()> {
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

/// 异步将 HashIndex 的全部 Buckets 与 OverflowPool 状态原子写入持久化快照文件
///
/// 纯 compio 异步定位 I/O（`write_all_at` + `sync_all`），无线程创建：io_uring 下真正的
/// 磁盘 I/O 由内核完成，reactor 线程仅提交与收割完成事件，序列化与 CRC32 累积这类
/// 微秒级纯计算留在 reactor 线程正是 thread-per-core 模型的预期行为。
///
/// 写入流程：
/// 1. 写入带有 Magic、Version、Token 及元数据的 64 字节文件头。
/// 2. 逐一写入主哈希表中的所有 64 字节桶数据（清除槽位 7 中的并发自旋锁，剥离未提交
///    试探性标记；指向易失 ReadCache 的条目先经 `rc_skip` 顺链解析为主日志真实地址）。
/// 3. 逐一写入溢出内存池中已分配的所有 64 字节桶数据（空洞以全零桶补齐）。
/// 4. 计算全量桶数据 CRC32 校验和并回写至头部，保证数据完整性校验。
/// 5. 刷盘并执行文件重命名，保证快照写入的完全原子性（避免中途断电留下半截文件）。
///
/// `rc_skip` 对标 C# Tsavorite SkipReadCacheBucket 委托：无 ReadCache 时传恒等闭包
/// `&|addr| addr` 即可（对无 ReadCache 位槽位不产生任何调用开销）。
///
/// 失败清场：任一 I/O 阶段失败（如磁盘满 ENOSPC）时就地回收半截 `.tmp` 残留，
/// 不遗留垃圾文件（正常路径的原子性由「tmp 写 + fsync + rename」保证）。
pub async fn write_index_checkpoint(
  index: &HashIndex,
  entry_count: usize,
  checkpoint_dir: impl AsRef<Path>,
  token: u128,
  rc_skip: &dyn Fn(u64) -> u64,
) -> Result<IndexMeta> {
  let dir = checkpoint_dir.as_ref();
  let res = write_index_checkpoint_inner(index, entry_count, dir, token, rc_skip).await;
  if res.is_err() {
    let _ = remove_file(dir.join(index_tmp_filename(token)));
  }
  res
}

/// 快照写入主流程（见 [`write_index_checkpoint`]）
async fn write_index_checkpoint_inner(
  index: &HashIndex,
  entry_count: usize,
  dir: &Path,
  token: u128,
  rc_skip: &dyn Fn(u64) -> u64,
) -> Result<IndexMeta> {
  let final_path = dir.join(index_filename(token));
  let tmp_path = dir.join(index_tmp_filename(token));

  let num_buckets = index.size as u64;
  let overflow_count = index.overflow_pool.allocated_count();

  let mut file = File::create(&tmp_path).await?;

  // 1. 构建并写入 64 字节头部（CRC 字段先占位，写入数据后回写）
  let ckpt_hdr = IndexCkptHeader {
    version: INDEX_VERSION,
    crc: 0,
    token,
    num_buckets,
    overflow_count,
    entry_count: entry_count as u64,
  };
  file.write_all_at(ckpt_hdr.encode(), 0).await.0?;

  let mut hasher = Crc32Hasher::new();
  {
    let mut batch_writer = BatchWriter::new(&mut file, &mut hasher, HEADER_SIZE as u64, rc_skip);

    // 2. 写入主哈希表桶数据
    for bucket in index.buckets.iter() {
      batch_writer.write_bucket(bucket).await?;
    }

    // 3. 写入溢出内存池桶数据（缺失的分配空洞以全零桶补齐，保证与头部声明的数量严格对齐）
    for id in 1..=overflow_count {
      if let Some(bucket) = index.overflow_pool.get(id) {
        batch_writer.write_bucket(bucket).await?;
      } else {
        batch_writer.write_zero_bucket().await?;
      }
    }

    batch_writer.finish().await?;
  }

  // 4. 计算 CRC32 校验和并回写至头部第 12..16 字节
  let crc = hasher.finalize();
  file
    .write_all_at(crc.to_le_bytes(), HEADER_CRC_OFFSET)
    .await
    .0?;
  file.sync_all().await?;

  rename(&tmp_path, &final_path).await?;
  // fsync 父目录：保证 rename 目录项掉电持久，避免恢复时出现 meta 在而 index 丢失的残缺视图
  super::manager::sync_checkpoint_dir(dir).await?;

  debug!(
    "成功刷写 Index Checkpoint: token={token:#x}, num_buckets={num_buckets}, overflow_count={overflow_count}, entry_count={entry_count}, crc={crc:#x}"
  );

  Ok(IndexMeta {
    size: index.size,
    overflow_count,
    entry_count,
  })
}

/// 异步从持久化快照文件中反序列化并重建 HashIndex（支持一步到位熔合 tail 截断与条目净化）
///
/// 单次流式还原同时完成三件净化（对标 C# Tsavorite FinalizeMainIndexRecovery 后置的
/// DeleteTentativeEntries 与溢出槽位锁复位 `bucket_entries[7] &= kAddressBitMask`，
/// 但熔合为读取路径上的零二次遍历）：试探态清零、ReadCache 易失指针清零、Latch 剥离。
/// 纯 compio 异步定位读取（`read_exact_at`），反序列化与 CRC 校验为纯计算，留在
/// reactor 线程执行；大索引恢复期间的 I/O 等待由内核异步完成，不阻塞事件循环。
/// C# ClearBitsForDiskImages 还会在恢复时清除记录 SEALED 位，wedb 中 SEALED 仅存活于
/// 内存复活池语义、从不写入日志/索引的持久化镜像，故无需对应清除步骤。
pub async fn read_index_checkpoint_truncated(
  index_path: impl AsRef<Path>,
  expected_token: u128,
  tail: Option<u64>,
) -> Result<(HashIndex, IndexMeta)> {
  let path = index_path.as_ref();
  // 与 std Path::exists 等价的异步存在性探测（stat 元数据 syscall）
  if metadata(path).await.is_err() {
    return Err(Error::IndexCkptNotFound(path.to_path_buf()));
  }

  let mut file = File::open(path).await?;
  let file_len = file.metadata().await?.len();
  if file_len < HEADER_SIZE as u64 {
    return Err(Error::InvalidIndexCkpt("文件长度不足头部大小".into()));
  }

  // 1. 读取并校验头部（完成式 I/O 按值移交缓冲，完成后取回）
  let BufResult(res, header) = file.read_exact_at([0u8; HEADER_SIZE], 0).await;
  res?;

  let Some(ckpt_hdr) = IndexCkptHeader::decode_opt(&header) else {
    return Err(Error::InvalidIndexCkpt(
      "文件魔数不匹配或头部长度不足".into(),
    ));
  };

  if ckpt_hdr.version != INDEX_VERSION {
    let mut s = String::from("不支持的版本号: ");
    let mut buf = itoa::Buffer::new();
    s.push_str(buf.format(ckpt_hdr.version));
    return Err(Error::InvalidIndexCkpt(s));
  }

  let expected_crc = ckpt_hdr.crc;
  let token = ckpt_hdr.token;

  if token != expected_token {
    return Err(Error::TokenMismatch {
      expected: expected_token,
      actual: token,
    });
  }

  let num_buckets = ckpt_hdr.num_buckets;
  let overflow_count = ckpt_hdr.overflow_count;
  let entry_count = ckpt_hdr.entry_count as usize;

  let total_buckets = num_buckets
    .checked_add(overflow_count)
    .ok_or_else(|| Error::InvalidIndexCkpt("桶总数算术溢出".into()))?;
  let data_len = total_buckets
    .checked_mul(BUCKET_BYTES as u64)
    .ok_or_else(|| Error::InvalidIndexCkpt("数据长度算术溢出".into()))?;
  let expected_len = (HEADER_SIZE as u64)
    .checked_add(data_len)
    .ok_or_else(|| Error::InvalidIndexCkpt("文件总长度算术溢出".into()))?;
  if file_len != expected_len {
    let mut s = String::from("文件长度异常: 期望 ");
    let mut buf = itoa::Buffer::new();
    s.push_str(buf.format(expected_len));
    s.push_str(" 字节，实际 ");
    s.push_str(buf.format(file_len));
    s.push_str(" 字节");
    return Err(Error::InvalidIndexCkpt(s));
  }

  // 2. 初始化 HashIndex 实例与批量读取器
  let new_index = HashIndex::new(num_buckets as usize)?;
  let mut hasher = Crc32Hasher::new();
  {
    let mut batch_reader = BatchReader::new(&mut file, &mut hasher, HEADER_SIZE as u64, data_len);

    // 3. 读取并恢复主哈希表的所有桶（单次流式完成反序列化、试探态净化与截断）
    for bucket in new_index.buckets.iter() {
      batch_reader.read_bucket_into(bucket, tail).await?;
    }

    // 4. 读取并恢复溢出池中的所有桶
    for j in 1..=overflow_count {
      let id = new_index.overflow_pool.allocate()?;
      if id != j {
        let mut s = String::from("溢出桶分配序号错乱: 期望 ");
        let mut buf = itoa::Buffer::new();
        s.push_str(buf.format(j));
        s.push_str("，实际 ");
        s.push_str(buf.format(id));
        return Err(Error::InvalidIndexCkpt(s));
      }
      let overflow_bucket = new_index.overflow_pool.get(id).ok_or_else(|| {
        let mut s = String::from("无法获取分配的溢出桶 ");
        let mut buf = itoa::Buffer::new();
        s.push_str(buf.format(id));
        Error::InvalidIndexCkpt(s)
      })?;

      batch_reader.read_bucket_into(overflow_bucket, tail).await?;
    }
  }

  // 5. 校验 CRC32 校验和
  let actual_crc = hasher.finalize();
  if actual_crc != expected_crc {
    return Err(Error::ChecksumMismatch {
      expected: expected_crc,
      actual: actual_crc,
    });
  }

  debug!(
    "成功恢复 Index Checkpoint: token={token:#x}, num_buckets={num_buckets}, overflow_count={overflow_count}, entry_count={entry_count}, crc={actual_crc:#x}"
  );

  Ok((
    new_index,
    IndexMeta {
      size: num_buckets as usize,
      overflow_count,
      entry_count,
    },
  ))
}
