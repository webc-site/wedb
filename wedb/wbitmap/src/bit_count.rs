//! BITCOUNT 位计数驱动（对标 libs/server/Resp/Bitmap/BitmapManagerBitCount.cs，
//! C# 为 BitmapManager partial）
//!
//! C# 标量路径按 u64 四路展开 popcount，SIMD 路径经 SSSE3/AVX2 查表 + 累加；
//! Rust 侧两实现统一以 `u64::count_ones`（硬件 POPCNT/CNT 指令）承接，批处理宽度取
//! C# 的 8/32 两档，结果逐位一致。
//!
//! 分派收口为二路（scalar / 宽档单路）：C# `Ssse3.IsSupported → __simd_popcX128` 的
//! 分派轴是 ISA 指令集档位（AVX2 缺失时按向量宽度降级），本仓 SIMD 单点 `wbase::simd`
//! 走 fearless_simd `dispatch!(Level::new(), ..)`，`Level::new()` 只返回本机最强档、
//! 无按宽度降级轴，故 X128 臂不转写（js/check/ignore 登记 __simd_popcX128）。
//!
//! 自研依据: BITCOUNT 分桶查表（C# 对应 GarnetBitmapTests.cs BITCOUNT 面）

use crate::manager::{BIT_RANGE_MASKS, OFFSET_UNIT_BIT, normalize_scan_range};

/// 单字节内计位：仅统计 `[startBitOffset, endBitOffset)` 位区间（MSB 序，利用编译期掩码表单周期计算）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:BitIndexCount(byte,int,int)
#[inline(always)]
pub const fn bit_index_count(payload: u8, start_bit_offset: u32, end_bit_offset: u32) -> i64 {
  if start_bit_offset > 8 || end_bit_offset > 8 {
    return 0;
  }
  let mask = BIT_RANGE_MASKS[start_bit_offset as usize][end_bit_offset as usize];
  (mask & payload).count_ones() as i64
}

/// 位区间计数：首末字节做部分位计数，中间字节不计（由驱动按整段处理）
///
/// C# 同文件 BitIndexCount 的 `(byte*, long, long)` 重载
fn bit_index_count_range(value: &[u8], start_offset: i64, end_offset: i64) -> i64 {
  let start_byte = start_offset / 8;
  let end_byte = end_offset / 8;

  let left_bit_index = (start_offset & 7) as u32;
  let right_bit_index = (end_offset & 7) as u32 + 1;

  if start_byte == end_byte {
    bit_index_count(value[start_byte as usize], left_bit_index, right_bit_index)
  } else {
    // 首字节自 leftBitIndex 起数到字节尾；末字节自字节头数到 rightBitIndex
    bit_index_count(value[start_byte as usize], left_bit_index, 8)
      + bit_index_count(value[end_byte as usize], 0, right_bit_index)
  }
}

/// BITCOUNT 主驱动：分字节/位两口径计数
///
/// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:BitCountDriver
pub fn bit_count_driver(
  start_offset: i64,
  end_offset: i64,
  offset_type: u8,
  value: &[u8],
  val_len: i64,
) -> i64 {
  // C# 同形前置判（严禁并入归一化）：双侧负偏移且字面倒序时，归一化会把
  // 两者分别钳 0 而丢失倒序信息（小负载下本应恒空区间反被计为 [0,0]），
  // 须在归一化前按原值返回空计数
  if start_offset < 0 && end_offset < 0 && start_offset > end_offset {
    return 0;
  }

  // 区间归一化单点在 [`normalize_scan_range`]（负偏移、end 钳制、空区间早退），
  // BIT 口径按位长折算后与原字节/位双写前奏逐例等价
  let Some((mut start_offset, mut end_offset)) =
    normalize_scan_range(offset_type, start_offset, end_offset, val_len)
  else {
    // 空区间哨兵（BITCOUNT 空区间恒 0）
    return 0;
  };

  let mut count = 0;
  if offset_type == OFFSET_UNIT_BIT {
    // BIT 口径：首末字节做部分位计数
    count += bit_index_count_range(value, start_offset, end_offset);

    // 跳过首末字节后按整段字节计数
    start_offset = (start_offset / 8) + 1;
    end_offset = (end_offset / 8) - 1;

    // 刻意差异（修 C# 漏计中间整字节缺陷）：
    // C# 此处为 `if (startOffset >= endOffset) return count;`。
    // 当区间恰好跨 3 字节（如 bit 7..16）时，首末字节剔除后 start_offset 与 end_offset
    // 均等于中间字节下标 1。C# 以 `>=` 早退导致中间字节漏计（对 [0x01,0xFF,0x80] 算 7..16 返回 2，Redis 正确值为 10）。
    // Rust 改为 `>` 使两者相等时仍落入下方标量 popc 计入中间字节，与 Redis 口径对齐。
    // 裁决见 doc/zh/deviations.md 第 92 条，严禁按 C# `>=` 形态回改。
    if start_offset > end_offset {
      return count;
    }
  }

  let (start, end) = (start_offset as usize, end_offset as usize);
  // C# 四路 `scalar / Avx2→X256 / Ssse3→X128 / 兜底 scalar` 在此收口为二路：
  // 无按宽度降级轴（模块头声明），宽区间单路承接
  if end - start < 128 {
    count += __scalar_popc(value, start, end);
  } else {
    count += __simd_popc_x256(value, start, end);
  }
  count
}

/// u64 档共享收尾核：4x8 批量 → 1x8 批量 → 尾部 <8 字节逐位计数
///
/// `libs/server/Resp/Bitmap/BitmapManagerBitCount.cs` 中 `__scalar_popc` 全段与
/// `__simd_popcX256` 降级回标量的尾段同构，此处单点收口：统计 `bitmap[curr..curr+len]`
#[inline]
fn popc_u64_span(bitmap: &[u8], mut curr: usize, mut len: usize) -> u64 {
  let mut count: u64 = 0;

  // popc_4x8：每次 4 个 u64
  const BATCH_4X8: usize = 8 * 4;
  let mut tail = len % BATCH_4X8;
  let mut vend = curr + (len - tail);
  while curr < vend {
    let v00 = u64_read(bitmap, curr).count_ones() as u64;
    let v01 = u64_read(bitmap, curr + 8).count_ones() as u64;
    let v02 = u64_read(bitmap, curr + 16).count_ones() as u64;
    let v03 = u64_read(bitmap, curr + 24).count_ones() as u64;
    count += (v00 + v01) + (v02 + v03);
    curr += BATCH_4X8;
  }

  // popc_1x8：每次 1 个 u64
  len = tail;
  tail = len % 8;
  vend = curr + (len - tail);
  while curr < vend {
    count += u64_read(bitmap, curr).count_ones() as u64;
    curr += 8;
  }

  // 尾部 <8 字节逐位计数
  count + popc_tail(bitmap, curr, tail) as u64
}

/// 标量人口计数（u64 档共享核的全段入口）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:__scalar_popc
pub fn __scalar_popc(bitmap: &[u8], start: usize, end: usize) -> i64 {
  popc_u64_span(bitmap, start, (end - start) + 1) as i64
}

/// 32 字节宽度批处理人口计数（C# AVX2 / SSSE3 两档 SIMD 的单路承接）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:__simd_popcX256
pub fn __simd_popc_x256(bitmap: &[u8], start: usize, end: usize) -> i64 {
  let mut count: u64 = 0;
  let mut batch_size: usize = 8 * 32;
  let mut len = (end - start) + 1;
  let mut tail = len & (batch_size - 1);
  let mut curr = start;
  let mut vend = curr + (len - tail);

  // popc_8x32：每轮 8×32 字节
  while curr < vend {
    count += popc_bytes(bitmap, curr, 8 * 32);
    curr += batch_size;
  }
  if tail == 0 {
    return count as i64;
  }

  // popc_1x32：每次 32 字节
  len = tail;
  batch_size = 32;
  tail = len & (batch_size - 1);
  vend = curr + (len - tail);
  while curr < vend {
    count += popc_bytes(bitmap, curr, 32);
    curr += batch_size;
  }
  if tail == 0 {
    return count as i64;
  }

  // 降级回 u64 标量路径收尾（4x8→1x8→逐位三段在 popc_u64_span 单点收口）
  count += popc_u64_span(bitmap, curr, tail);
  count as i64
}

/// 非对齐读取 `bitmap[idx..idx+8]` 的 u64（C# `*(ulong*)` 语义；
/// 调用方保证区间完整；count_ones 独立于字节序，无需 to_le）
#[inline(always)]
fn u64_read(bitmap: &[u8], idx: usize) -> u64 {
  debug_assert!(idx + 8 <= bitmap.len());
  // SAFETY: 调用方循环保证 idx + 8 <= vend <= bitmap.len()
  unsafe { bitmap.as_ptr().add(idx).cast::<u64>().read_unaligned() }
}

/// 计数 `bitmap[idx..idx+width]` 的置位数（width = 32 的倍数，SIMD 宽度等效）
#[inline]
fn popc_bytes(bitmap: &[u8], idx: usize, width: usize) -> u64 {
  bitmap[idx..idx + width]
    .as_chunks::<8>()
    .0
    .iter()
    .map(|c| u64::from_ne_bytes(*c).count_ones() as u64)
    .sum()
}

/// 尾部 1..=7 字节直接 popcount 计数（消除 7 个 if 分支及位移拼装）
#[inline(always)]
fn popc_tail(bitmap: &[u8], idx: usize, tail: usize) -> u32 {
  if tail == 0 {
    return 0;
  }
  debug_assert!(idx + tail <= bitmap.len());
  bitmap[idx..idx + tail]
    .iter()
    .map(|&b| b.count_ones())
    .sum()
}
