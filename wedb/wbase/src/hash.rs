//! 逐位兼容哈希算法库（精确移植自 Garnet `HashUtils.cs`）
//!
//! # 与 `whasher` 的职能分工
//! - `wbase::hash` 提供与 Redis / Garnet 严格逐位兼容的 MurmurHash2 (`murmur_hash2_x64_a`)
//!   与 MurmurHash3 实现，专门用于 Redis HyperLogLog 等需要协议比特级二进制兼容的场景。
//! - 通用高性能哈希（条带锁分段、内存索引 tag、校验和等）一律使用 `whasher`（基于硬件 AES
//!   向量加速的 gxhash 后端，哈希值刻意不与 Murmur 逐位兼容）。

/// garnet/libs/common/HashUtils.cs:fmix64
#[inline]
fn fmix64(mut k: u64) -> u64 {
  k ^= k >> 33;
  k = k.wrapping_mul(0xff51afd7ed558ccd);
  k ^= k >> 33;
  k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
  k ^= k >> 33;
  k
}

/// garnet/libs/common/HashUtils.cs:MurmurHash3x64A
#[inline]
pub fn murmur_hash3_x64_a(b_string: &[u8], seed: u32) -> u64 {
  let mut h1 = seed as u64;
  let c1: u64 = 0x87c37b91114253d5;
  let c2: u64 = 0x4cf5ad432745937f;
  let mut k1: u64;

  let len = b_string.len();

  // 8 字节整块：as_chunks 消除索引边界检查，剩余尾部一次性给出
  let (blocks, suffix) = b_string.as_chunks::<8>();
  for block in blocks {
    k1 = u64::from_le_bytes(*block);

    k1 = k1.wrapping_mul(c1);
    k1 = k1.rotate_left(31);
    k1 = k1.wrapping_mul(c2);
    h1 ^= k1;
    h1 = h1.rotate_left(27);
    h1 = h1.wrapping_add(k1);
    h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);
  }

  let suffix_len = suffix.len();
  k1 = 0;

  if suffix_len >= 7 {
    k1 ^= (suffix[6] as u64) << 48;
  }
  if suffix_len >= 6 {
    k1 ^= (suffix[5] as u64) << 40;
  }
  if suffix_len >= 5 {
    k1 ^= (suffix[4] as u64) << 32;
  }
  if suffix_len >= 4 {
    k1 ^= (suffix[3] as u64) << 24;
  }
  if suffix_len >= 3 {
    k1 ^= (suffix[2] as u64) << 16;
  }
  if suffix_len >= 2 {
    k1 ^= (suffix[1] as u64) << 8;
  }
  if suffix_len >= 1 {
    k1 ^= suffix[0] as u64;
    k1 = k1.wrapping_mul(c1);
    k1 = k1.rotate_left(31);
    k1 = k1.wrapping_mul(c2);
    h1 ^= k1;
  }

  h1 ^= len as u64;
  fmix64(h1)
}

/// garnet/libs/common/HashUtils.cs:MurmurHash3x128
#[inline]
pub fn murmur_hash3_x128(b_string: &[u8], seed: u32) -> (u64, u64) {
  let mut h1 = seed as u64;
  let mut h2 = seed as u64;

  let c1: u64 = 0x87c37b91114253d5;
  let c2: u64 = 0x4cf5ad432745937f;

  let mut k1: u64;
  let mut k2: u64;

  let len = b_string.len();

  // 16 字节整块：as_chunks 消除索引边界检查，剩余尾部一次性给出
  let (blocks, suffix) = b_string.as_chunks::<16>();
  for block in blocks {
    // SAFETY: 块长恒为 16 字节，前后两个 8 字节半块读取恒在界内，零边界检查
    k1 = unsafe { (block.as_ptr() as *const u64).read_unaligned() };
    k2 = unsafe { (block.as_ptr().add(8) as *const u64).read_unaligned() };

    k1 = k1.wrapping_mul(c1);
    k1 = k1.rotate_left(31);
    k1 = k1.wrapping_mul(c2);
    h1 ^= k1;
    h1 = h1.rotate_left(27);
    h1 = h1.wrapping_add(h2);
    h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);

    k2 = k2.wrapping_mul(c2);
    k2 = k2.rotate_left(33);
    k2 = k2.wrapping_mul(c1);
    h2 ^= k2;
    h2 = h2.rotate_left(31);
    h2 = h2.wrapping_add(h1);
    h2 = h2.wrapping_mul(5).wrapping_add(0x38495ab5);
  }

  let suffix_len = suffix.len();
  k1 = 0;
  k2 = 0;

  if suffix_len >= 15 {
    k2 ^= (suffix[14] as u64) << 48;
  }
  if suffix_len >= 14 {
    k2 ^= (suffix[13] as u64) << 40;
  }
  if suffix_len >= 13 {
    k2 ^= (suffix[12] as u64) << 32;
  }
  if suffix_len >= 12 {
    k2 ^= (suffix[11] as u64) << 24;
  }
  if suffix_len >= 11 {
    k2 ^= (suffix[10] as u64) << 16;
  }
  if suffix_len >= 10 {
    k2 ^= (suffix[9] as u64) << 8;
  }
  if suffix_len >= 9 {
    k2 ^= suffix[8] as u64;
    k2 = k2.wrapping_mul(c2);
    k2 = k2.rotate_left(33);
    k2 = k2.wrapping_mul(c1);
    h2 ^= k2;
  }

  if suffix_len >= 8 {
    k1 ^= (suffix[7] as u64) << 56;
  }
  if suffix_len >= 7 {
    k1 ^= (suffix[6] as u64) << 48;
  }
  if suffix_len >= 6 {
    k1 ^= (suffix[5] as u64) << 40;
  }
  if suffix_len >= 5 {
    k1 ^= (suffix[4] as u64) << 32;
  }
  if suffix_len >= 4 {
    k1 ^= (suffix[3] as u64) << 24;
  }
  if suffix_len >= 3 {
    k1 ^= (suffix[2] as u64) << 16;
  }
  if suffix_len >= 2 {
    k1 ^= (suffix[1] as u64) << 8;
  }
  if suffix_len >= 1 {
    k1 ^= suffix[0] as u64;
    k1 = k1.wrapping_mul(c1);
    k1 = k1.rotate_left(31);
    k1 = k1.wrapping_mul(c2);
    h1 ^= k1;
  }

  h1 ^= len as u64;
  h2 ^= len as u64;
  h1 = h1.wrapping_add(h2);
  h2 = h2.wrapping_add(h1);
  h1 = fmix64(h1);
  h2 = fmix64(h2);
  h1 = h1.wrapping_add(h2);
  h2 = h2.wrapping_add(h1);

  (h1, h2)
}

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
