//! 十六进制微工具（跨模块公共件，一处定义；对标 C# 各处散落的 hex 折值内联）
//!
//! 自研依据: 十六进制编解码单点化（C# 分散于 Convert.ToHexString/FromHexString 调用点）

/// 小写十六进制字符映射表
pub const HEX_CHARS_LOWER: &[u8; 16] = b"0123456789abcdef";

/// 大写十六进制字符映射表
pub const HEX_CHARS_UPPER: &[u8; 16] = b"0123456789ABCDEF";
/// 单个十六进制字符折值（大小写均可；crate 内 hex_decode/hex_u128 共享内核，
/// crate 外零生产消费，导出面已收窄）
#[inline]
pub(crate) const fn hex_val(c: u8) -> Option<u8> {
  match c {
    b'0'..=b'9' => Some(c - b'0'),
    b'a'..=b'f' => Some(c - b'a' + 10),
    b'A'..=b'F' => Some(c - b'A' + 10),
    _ => None,
  }
}

/// 定长小写十六进制编码核心（单一真源：20/32 字节定长出口与 u128 大端视图共享）
///
/// 输出恰为 `OUT` 字节小写 hex（零堆分配，const 泛型单份循环，各出口单态化后
/// 与手写展开逐位等价）；`src.len()` 恰为 `OUT / 2` 由三个定长出口的数组类型
/// 静态钉死，核心内 debug 断言复核
const fn hex_encode_fixed<const OUT: usize>(src: &[u8]) -> [u8; OUT] {
  // assert! 而非 debug_assert_eq!：const fn 内后者不可用；本断言在编译期
  // const 求值（下方编译期断言块）即被行使，运行期成本一次比较
  assert!(src.len() * 2 == OUT, "hex 编码核心入出长度不匹配");
  let mut out = [0u8; OUT];
  let mut i = 0;
  while i < OUT / 2 {
    let b = src[i];
    out[i * 2] = HEX_CHARS_LOWER[(b >> 4) as usize];
    out[i * 2 + 1] = HEX_CHARS_LOWER[(b & 0x0f) as usize];
    i += 1;
  }
  out
}

/// 定长 20 字节（如 SHA1）小写十六进制编码：输出 40 字节定长数组（零堆分配）
#[inline]
pub const fn hex_encode_20(src: &[u8; 20]) -> [u8; 40] {
  hex_encode_fixed::<40>(src)
}

/// 定长 32 字节（如 SHA256）小写十六进制编码：输出 64 字节定长数组（零堆分配）
#[inline]
pub const fn hex_encode_32(src: &[u8; 32]) -> [u8; 64] {
  hex_encode_fixed::<64>(src)
}

/// u128 小写十六进制编码：输出 32 字节定长数组（大端，零堆分配）
#[inline]
pub(crate) const fn hex_encode_u128(src: u128) -> [u8; 32] {
  // 大端字节视图：第 i 字节即 (120 - 8i) 位起 8 位，与逐位右移取字节逐位等价
  hex_encode_fixed::<32>(&src.to_be_bytes())
}

/// u128 小写十六进制编码为 String（32 字符）
#[inline]
pub fn hex_str_u128(src: u128) -> String {
  let mut out = vec![0u8; 32];
  out.copy_from_slice(&hex_encode_u128(src));
  // SAFETY: HEX_CHARS_LOWER 仅包含 ASCII 十六进制字符，为合法 UTF-8
  unsafe { String::from_utf8_unchecked(out) }
}

/// 恰好 32 字符的十六进制串折为 u128（大小写均可，零堆分配）；
/// 长度不符或含非法字符返回 None
pub const fn hex_u128(src: &[u8]) -> Option<u128> {
  if src.len() != 32 {
    return None;
  }
  let mut bytes = [0u8; 16];
  let mut i = 0;
  while i < 32 {
    let v = match hex_val(src[i]) {
      Some(v) => v,
      None => return None,
    };
    bytes[i / 2] = if i & 1 == 0 { v << 4 } else { bytes[i / 2] | v };
    i += 1;
  }
  Some(u128::from_be_bytes(bytes))
}

/// 定长解码：恰好 2N 长度的 hex 串折为 N 字节（零堆分配）；
/// 长度不符或含非法字符返回 None
pub const fn hex_decode<const N: usize>(src: &[u8]) -> Option<[u8; N]> {
  if src.len() != N * 2 {
    return None;
  }
  let mut out = [0u8; N];
  let mut i = 0;
  while i < src.len() {
    let hi = match hex_val(src[i]) {
      Some(v) => v,
      None => return None,
    };
    let lo = match hex_val(src[i + 1]) {
      Some(v) => v,
      None => return None,
    };
    out[i / 2] = (hi << 4) | lo;
    i += 2;
  }
  Some(out)
}

/// 生成 40 字符十六进制随机身份串（对标 C# Generator.CreateHexId(40)；
/// 无状态独立随机填充，每次独立生成，不共享全局计数器状态）
pub fn generate_hex_id() -> String {
  let mut buf = [0u8; 20];
  fastrand::fill(&mut buf);
  let enc = hex_encode_20(&buf);
  // SAFETY: hex_encode_20 仅产出 ASCII '0'-'9','a'-'f'，为合法 UTF-8
  unsafe { String::from_utf8_unchecked(enc.to_vec()) }
}

/// 编译期断言：定长编码核心三出口尺寸精确、零输入产出全 '0'、u128 极值首尾字符正确
const _: () = {
  let e20 = hex_encode_20(&[0u8; 20]);
  assert!(e20.len() == 40 && e20[0] == b'0' && e20[39] == b'0');
  assert!(hex_encode_32(&[0u8; 32]).len() == 64);

  let zero = hex_encode_u128(0);
  assert!(zero.len() == 32 && zero[0] == b'0' && zero[31] == b'0');
  let max = hex_encode_u128(u128::MAX);
  assert!(max[0] == b'f' && max[31] == b'f');
};
// 行为测试见 tests/suite/hex.rs（自本文件内联 mod tests 迁出）
