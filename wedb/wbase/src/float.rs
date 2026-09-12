//! IEEE 754 浮点数保序二进制编解码原语 (Order-Preserving Floating Point Encoding)
//!
//! 将 IEEE 754 `f64` / `f32` 浮点数无损映射为保序大端字节数组：
//! - 映射后的字节切片可以直接使用原生 `memcmp` / `slice::cmp` 进行字典序比较；
//! - 比较结果与原生浮点数的数学大小关系一致；唯一例外：`-0.0` 与 `+0.0` 数学相等，
//!   但编码为不同字节序列且 `-0.0 < +0.0`（对存储全序是优点——两值可区分存储与遍历）；
//! - 全程采用算术右移产生动态掩码，零 CPU 分支预测失败，单指令位运算；
//! - 100% 支持 `const fn`。

/// 编码 `f64` 为 8 字节保序大端字节数组 (const fn, 零分支)
///
/// 算法原理：
/// - 非负数：仅翻转最高符号位（`1 << 63`），使得非负数整体映射到 `[0x8000_..., 0xFFFF_...]`；
/// - 负数：将所有 64 位全部翻转（`0xFFFF_...`），使得负数整体映射到 `[0x0000_..., 0x7FFF_...]`；
/// - 利用 `(bits as i64) >> 63` 算术右移在 1 条指令内生成条件掩码，免除 `if` 分支。
#[inline(always)]
pub const fn encode_f64(val: f64) -> [u8; 8] {
  let bits = val.to_bits();
  let mask = (((bits as i64) >> 63) as u64) | (1u64 << 63);
  (bits ^ mask).to_be_bytes()
}

/// 从 8 字节保序大端数组无损还原原始 `f64` (const fn, 零分支)
#[inline(always)]
pub const fn decode_f64(bytes: [u8; 8]) -> f64 {
  let sortable = u64::from_be_bytes(bytes);
  let mask = ((((!sortable) as i64) >> 63) as u64) | (1u64 << 63);
  f64::from_bits(sortable ^ mask)
}

/// 编码 `f32` 为 4 字节保序大端字节数组 (const fn, 零分支)
#[inline(always)]
pub const fn encode_f32(val: f32) -> [u8; 4] {
  let bits = val.to_bits();
  let mask = (((bits as i32) >> 31) as u32) | (1u32 << 31);
  (bits ^ mask).to_be_bytes()
}

/// 从 4 字节保序大端数组无损还原原始 `f32` (const fn, 零分支)
#[inline(always)]
pub const fn decode_f32(bytes: [u8; 4]) -> f32 {
  let sortable = u32::from_be_bytes(bytes);
  let mask = ((((!sortable) as i32) >> 31) as u32) | (1u32 << 31);
  f32::from_bits(sortable ^ mask)
}

/// 从只读切片（至少 8 字节）解析保序 `f64` (const fn, 零堆分配零越界检查)
#[inline(always)]
pub const fn decode_f64_from_slice(src: &[u8]) -> Option<f64> {
  match src {
    [b0, b1, b2, b3, b4, b5, b6, b7, ..] => {
      Some(decode_f64([*b0, *b1, *b2, *b3, *b4, *b5, *b6, *b7]))
    }
    _ => None,
  }
}

/// 从只读切片（至少 4 字节）解析保序 `f32` (const fn, 零堆分配零越界检查)
#[inline(always)]
pub const fn decode_f32_from_slice(src: &[u8]) -> Option<f32> {
  match src {
    [b0, b1, b2, b3, ..] => Some(decode_f32([*b0, *b1, *b2, *b3])),
    _ => None,
  }
}
