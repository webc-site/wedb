//! 索引快照格式域：64 字节定长文件头的编解码与落盘/读回两侧的槽位净化。
//!
//! 头部与桶/批尺寸常量、[`IndexCkptHeader`] 的 `encode`/`decode_opt`，以及数据槽位
//! （未提交试探态、易失 ReadCache 指针、超出一致性截断点的历史条目）与溢出槽位
//! （高 16 位并发自旋锁 Latch）净化；批缓冲见 [`super::batch`]，读写流程见
//! [`super`] 与 [`super::read`]。
//!
//! 自研依据: 索引检查点编解码（C# 对应 test.recovery/CheckpointManagerTests.cs）

use std::sync::atomic::{AtomicU64, Ordering};

use windex::HashBucketEntry;

/// 索引快照二进制文件魔数
const INDEX_MAGIC: &[u8; 8] = b"WEDB_IDX";
/// 索引快照魔数 64 位整型（用于单周期快速比对）
const INDEX_MAGIC_U64: u64 = u64::from_le_bytes(*INDEX_MAGIC);
/// 索引快照二进制文件版本号
pub(crate) const INDEX_VERSION: u32 = 1;
/// 头部总大小（64 字节对齐）
pub(crate) const HEADER_SIZE: usize = 64;
/// 头部中 CRC32 校验和字段的字节偏移
pub(crate) const HEADER_CRC_OFFSET: u64 = 12;
/// 单个哈希桶序列化字节数（64 字节对齐）
pub(crate) const BUCKET_BYTES: usize = 64;
/// 批处理包含的哈希桶数量（512 个桶，共 32KB，匹配现代 CPU L1 缓存并最大化 SIMD 硬件 CRC32 吞吐）
const BATCH_BUCKETS: usize = 512;
/// 批处理缓冲区字节数（32KB）
pub(crate) const BATCH_BYTES: usize = BATCH_BUCKETS * BUCKET_BYTES;

/// 64 字节定长索引快照二进制文件头
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexCkptHeader {
  pub(crate) version: u32,
  pub(crate) crc: u32,
  pub(crate) token: u128,
  pub(crate) num_buckets: u64,
  pub(crate) overflow_count: u64,
  pub(crate) entry_count: u64,
}

impl IndexCkptHeader {
  /// 编码为 64 字节定长数组（小端编码，const fn，零堆分配）
  #[inline(always)]
  pub(crate) const fn encode(&self) -> [u8; HEADER_SIZE] {
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
  pub(crate) const fn decode_opt(src: &[u8]) -> Option<Self> {
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

/// 数据槽位序列化与反序列化净化：
/// 清除未提交的试探性标记（bit 63）、易失 ReadCache 虚拟指针（bit 47）
/// 以及超出一致性截断点 tail 的历史条目（截断归零）
#[inline(always)]
pub(crate) fn sanitize_data_slot(raw: u64, tail: Option<u64>) -> u64 {
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
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:SkipReadCacheBucket
///
/// 指向读缓存记录的条目不得直接落盘：读缓存在恢复后为空，直接落盘等于丢失该键。
/// C# 在快照时将此类条目顺链回写为首个主日志记录地址后再写入快照副本，且仅换写
/// Address 字段、高 16 位指纹原样保留（HashBucketEntry.cs:49 Address setter 的
/// 掩码换写语义）；Rust 侧由调用方传入解析端口（[`crate::CprStore::skip_read_cache_with_wait`]），
/// 仅对置有 ReadCache 位的非零槽位调用，其余槽位零开销透传。端口入参为**槽位本体**：
/// 解析期间的链头以重读槽位取得（对标 C# SkipReadCache 的
/// `UpdateRecordSourceToCurrentHashEntry` 重读哈希项），驱逐清洗方 CAS 换指主日志地址
/// 后重探即得新值，故端口恒返回确定的地址字段；指纹位不受端口返回值污染——否则
/// 恢复后 `find_tag` 按高 16 位比对永失配，该键不可见。
///
/// C# 快照面靠 `epoch.Resume()` 冻结驱逐进度、在 bucket 拷贝上直走链而无需等待
/// （IndexCheckpoint.cs:146-157）；本 port 不冻结驱逐，改由端口内「逐位置锚定等待 +
/// 回链头重探」收口，与紧缩面同一内核口径。
#[inline(always)]
pub(crate) fn resolve_read_cache(rc_resolve: &impl Fn(&AtomicU64) -> u64, slot: &AtomicU64) -> u64 {
  let raw = slot.load(Ordering::Acquire);
  if raw == 0 || raw & HashBucketEntry::READ_CACHE_BIT == 0 {
    return raw;
  }
  match (rc_resolve)(slot) {
    // 链尽：RC 专属记录无主日志对应，整体归零交下游净化（指纹位不得残留致恢复期悬空槽位）
    0 => 0,
    // 仅换写地址字段，原槽位高 16 位（指纹 + 试探态）原样保留
    real => (raw & !HashBucketEntry::ADDRESS_MASK) | (real & HashBucketEntry::ADDRESS_MASK),
  }
}

/// 溢出槽位净化：
/// 剥离高 16 位并发自旋锁（Latch）瞬态标记，仅保留低 48 位纯净溢出桶索引；
/// 超出快照上限 `max_overflow`（检查点采样/头部声明的溢出桶数）的 ID 属并发新分配的
/// 模糊区增量，其物理桶未随本快照落盘，一律截断归零，交由恢复期日志重放重新链入。
///
/// 对标 C# 快照/恢复以 `overflowBucketsAllocator.GetMaxValidAddress()` 为唯一受检边界：
/// libs/storage/Tsavorite/cs/src/core/Index/Recovery/IndexCheckpoint.cs:TakeIndexFuzzyCheckpoint
/// libs/storage/Tsavorite/cs/src/core/Allocator/MallocFixedPageSize.cs:BeginRecovery
/// wedb 单段固定格式快照在写入之初即固化 overflow_count，悬空/超界溢出指针落盘会在
/// 恢复后令 `OverflowPool::get_unchecked` 解引用空 chunk，故写读两侧必须同点收口。
#[inline(always)]
pub(crate) fn sanitize_overflow_slot(raw: u64, max_overflow: u64) -> u64 {
  let id = raw & HashBucketEntry::ADDRESS_MASK;
  if id > max_overflow { 0 } else { id }
}

#[cfg(test)]
mod tests {
  use super::IndexCkptHeader;

  /// IndexCkptHeader 定长二进制编解码：往返一致、魔数错误拒绝
  #[test]
  fn index_ckpt_header_codec_roundtrip() {
    let hdr = IndexCkptHeader {
      version: 1,
      crc: 0x1234_5678,
      token: 0xfeed_cafe_dead_beef_0123_4567_89ab_cdef,
      num_buckets: 1024,
      overflow_count: 16,
      entry_count: 5000,
    };
    let bytes = hdr.encode();
    assert_eq!(bytes.len(), super::HEADER_SIZE);
    let decoded = IndexCkptHeader::decode_opt(&bytes).expect("IndexCkptHeader 解码失败");
    assert_eq!(decoded, hdr);

    // 校验魔数错误分支
    let mut bad_magic = bytes;
    bad_magic[0] ^= 0xff;
    assert!(IndexCkptHeader::decode_opt(&bad_magic).is_none());
  }
}
