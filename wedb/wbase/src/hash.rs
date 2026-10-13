//! 逐位兼容哈希算法库（精确移植自 Garnet `HashUtils.cs`）
//!
//! # 与 `whasher` 的职能分工
//! - `wbase::hash` 提供与 Redis / Garnet 严格逐位兼容的 MurmurHash2 (`murmur_hash2_x64_a`)
//!   实现，专门用于 Redis HyperLogLog 等需要协议比特级二进制兼容的场景。
//! - 通用高性能哈希（条带锁分段、内存索引 tag、校验和等）一律使用 `whasher`（基于硬件 AES
//!   向量加速的 gxhash 后端，哈希值刻意不与 Murmur 逐位兼容）。

/// garnet/libs/common/HashUtils.cs:MurmurHash2x64A
#[inline]
pub fn murmur_hash2_x64_a(b_string: &[u8], seed: u32) -> u64 {
  let m: u64 = 0xc6a4a7935bd1e995;
  let r = 47;
  let len = b_string.len();
  let mut h = (seed as u64) ^ ((len as u64).wrapping_mul(m));

  // 8 字节整块：as_chunks 消除索引边界检查，剩余尾部一次性给出
  let (blocks, suffix) = b_string.as_chunks::<8>();
  for block in blocks {
    let mut k = u64::from_le_bytes(*block);

    k = k.wrapping_mul(m);
    k ^= k >> r;
    k = k.wrapping_mul(m);
    h ^= k;
    h = h.wrapping_mul(m);
  }

  let cs = suffix.len();

  if cs >= 7 {
    h ^= (suffix[6] as u64) << 48;
  }
  if cs >= 6 {
    h ^= (suffix[5] as u64) << 40;
  }
  if cs >= 5 {
    h ^= (suffix[4] as u64) << 32;
  }
  if cs >= 4 {
    h ^= (suffix[3] as u64) << 24;
  }
  if cs >= 3 {
    h ^= (suffix[2] as u64) << 16;
  }
  if cs >= 2 {
    h ^= (suffix[1] as u64) << 8;
  }
  if cs >= 1 {
    h ^= suffix[0] as u64;
    h = h.wrapping_mul(m);
  }

  h ^= h >> r;
  h = h.wrapping_mul(m);
  h ^= h >> r;

  h
}
