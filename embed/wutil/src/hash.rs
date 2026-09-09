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
  let num_blocks = len >> 3;

  for i in 0..num_blocks {
    let block = &b_string[i * 8..(i + 1) * 8];
    k1 = u64::from_le_bytes(block.try_into().unwrap());

    k1 = k1.wrapping_mul(c1);
    k1 = k1.rotate_left(31);
    k1 = k1.wrapping_mul(c2);
    h1 ^= k1;
    h1 = h1.rotate_left(27);
    h1 = h1.wrapping_add(k1);
    h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);
  }

  let suffix_len = len & 7;
  let suffix = &b_string[num_blocks * 8..];
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
  h1 = fmix64(h1);

  h1
}

/// garnet/libs/common/HashUtils.cs:MurmurHash3x64
#[inline]
pub fn murmur_hash3_x64(b_string: &[u8], seed: u32) -> u64 {
  let (h1, _) = murmur_hash3_x128(b_string, seed);
  h1
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
  let num_blocks = len >> 4;

  for i in 0..num_blocks {
    let block = &b_string[i * 16..(i + 1) * 16];
    k1 = u64::from_le_bytes(block[0..8].try_into().unwrap());
    k2 = u64::from_le_bytes(block[8..16].try_into().unwrap());

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

  let suffix_len = len & 15;
  let suffix = &b_string[num_blocks * 16..];
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
  let num_blocks = len / 8;

  for i in 0..num_blocks {
    let block = &b_string[i * 8..(i + 1) * 8];
    let mut k = u64::from_le_bytes(block.try_into().unwrap());

    k = k.wrapping_mul(m);
    k ^= k >> r;
    k = k.wrapping_mul(m);
    h ^= k;
    h = h.wrapping_mul(m);
  }

  let cs = len & 7;
  let suffix = &b_string[num_blocks * 8..];

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
