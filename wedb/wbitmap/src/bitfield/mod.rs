//! BITFIELD 位域操作（对标 libs/server/Resp/Bitmap/BitmapManagerBitfield.cs，
//! C# 为 BitmapManager partial）
//!
//! 算法前提与 C# 一致：位域按 MSB 在前的字节序列存放，`offset` 为域首位的
//! 位偏移，`encoding` 为位宽。C# 以 `stackalloc byte[8]` 草稿 + 裸指针
//! （curr/cend/vend）读写；Rust 侧以游标下标承接，`vend`（位图末端）截断
//! 语义保持不变。

mod execute;
mod parse;

pub use execute::*;
pub use parse::*;

#[cfg(test)]
mod tests {
  use super::{
    BitFieldCmdArgs, BitFieldOverflow, BitFieldSecondaryCommand, bit_field_execute,
    bit_field_execute_ro, check_bitfield_overflow, check_signed_bitfield_overflow,
    check_unsigned_bitfield_overflow, get_bitfield, parse_bitfield_encoding, parse_bitfield_offset,
    parse_bitfield_overflow_slice,
  };

  /// u/i 位宽矩阵的读写回环
  #[test]
  fn get_set_roundtrip_matrix() {
    for (signed, bits) in [
      (false, 1u8),
      (true, 1),
      (false, 7),
      (true, 8),
      (false, 8),
      (true, 16),
      (false, 63),
      (true, 64),
    ] {
      let type_info = if signed { 0x80 | bits } else { bits };
      let args = BitFieldCmdArgs::new(BitFieldSecondaryCommand::Set, type_info, 3, 0, 0);
      let mut buf = vec![0u8; 16];
      // 写最大值
      let max: i64 = if signed {
        if bits == 64 {
          i64::MAX
        } else {
          (1i64 << (bits - 1)) - 1
        }
      } else {
        ((1u64 << bits) - 1) as i64
      };
      let (old, ovf) = bit_field_execute(&args, &mut buf).unwrap();
      assert_eq!((old, ovf), (0, false));
      let _ = bit_field_execute(
        &BitFieldCmdArgs::new(BitFieldSecondaryCommand::Set, type_info, 3, max, 0),
        &mut buf,
      )
      .unwrap();
      // 回读
      let v = bit_field_execute_ro(
        &BitFieldCmdArgs::new(BitFieldSecondaryCommand::Get, type_info, 3, 0, 0),
        &buf,
      )
      .unwrap();
      assert_eq!(v, max, "signed={signed} bits={bits}");
    }
  }

  /// 无符号溢出矩阵：WRAP / SAT / FAIL × 正负增量
  #[test]
  fn unsigned_overflow_matrix() {
    let fail = BitFieldOverflow::Fail as u8;
    let wrap = BitFieldOverflow::Wrap as u8;
    let sat = BitFieldOverflow::Sat as u8;

    // u8 域：value=250，incr=10
    assert_eq!(
      check_unsigned_bitfield_overflow(250, 10, 8, wrap),
      (4, true)
    );
    assert_eq!(
      check_unsigned_bitfield_overflow(250, 10, 8, sat),
      (255, true)
    );
    assert_eq!(
      check_unsigned_bitfield_overflow(250, 10, 8, fail),
      (0, true)
    );
    // 下溢：value=5，incr=-10
    assert_eq!(
      check_unsigned_bitfield_overflow(5, -10, 8, wrap),
      (251, true)
    );
    assert_eq!(check_unsigned_bitfield_overflow(5, -10, 8, sat), (0, true));
    assert_eq!(check_unsigned_bitfield_overflow(5, -10, 8, fail), (0, true));
    // 无溢出
    assert_eq!(
      check_unsigned_bitfield_overflow(5, 10, 8, fail),
      (15, false)
    );
    assert_eq!(
      check_unsigned_bitfield_overflow(0, 255, 8, sat),
      (255, false)
    );

    // u64 域边界
    assert_eq!(
      check_unsigned_bitfield_overflow(u64::MAX - 1, 10, 64, sat),
      (u64::MAX, true)
    );
    assert_eq!(check_unsigned_bitfield_overflow(0, -1, 64, sat), (0, true));
    // 通过 CheckBitfieldOverflow 的 FAIL 透传
    assert_eq!(check_bitfield_overflow(250, 10, 8, fail, false), (0, true));
    // WRAP/SAT 不透出溢出标记
    assert_eq!(check_bitfield_overflow(250, 10, 8, wrap, false), (4, false));
    assert_eq!(
      check_bitfield_overflow(250, 10, 8, sat, false),
      (255, false)
    );
  }

  /// 有符号溢出矩阵：WRAP / SAT / FAIL × 正负增量 × i8/i64
  #[test]
  fn signed_overflow_matrix() {
    let fail = BitFieldOverflow::Fail as u8;
    let wrap = BitFieldOverflow::Wrap as u8;
    let sat = BitFieldOverflow::Sat as u8;

    // i8 域：value=120，incr=10 → 上溢
    assert_eq!(
      check_signed_bitfield_overflow(120, 10, 8, wrap),
      (-126, true)
    );
    assert_eq!(check_signed_bitfield_overflow(120, 10, 8, sat), (127, true));
    assert_eq!(check_signed_bitfield_overflow(120, 10, 8, fail), (0, true));
    // 下溢：value=-120，incr=-10
    assert_eq!(
      check_signed_bitfield_overflow(-120, -10, 8, wrap),
      (126, true)
    );
    assert_eq!(
      check_signed_bitfield_overflow(-120, -10, 8, sat),
      (-128, true)
    );
    assert_eq!(
      check_signed_bitfield_overflow(-120, -10, 8, fail),
      (0, true)
    );
    // 域内
    assert_eq!(
      check_signed_bitfield_overflow(-120, 10, 8, fail),
      (-110, false)
    );
    // i64 域边界
    assert_eq!(
      check_signed_bitfield_overflow(i64::MAX - 5, 10, 64, sat),
      (i64::MAX, true)
    );
    assert_eq!(
      check_signed_bitfield_overflow(i64::MIN + 5, -10, 64, sat),
      (i64::MIN, true)
    );
    assert_eq!(
      check_signed_bitfield_overflow(i64::MAX, 1, 64, wrap),
      (i64::MIN, true)
    );
    // FAIL 透传（check_bitfield_overflow 入口）
    assert_eq!(check_bitfield_overflow(120, 10, 8, fail, true), (0, true));
    assert_eq!(
      check_bitfield_overflow(-120, -10, 8, sat, true),
      (-128, false)
    );
  }

  /// GET/SET/INCRBY 组合（含跨字节与 vend 截断）
  #[test]
  fn subcommand_combinations() {
    // SET u16 @4 = 0xABCD 后 GET 回读
    let mut buf = vec![0u8; 4];
    let (old, ovf) = bit_field_execute(
      &BitFieldCmdArgs::new(BitFieldSecondaryCommand::Set, 16, 4, 0xABCD, 0),
      &mut buf,
    )
    .unwrap();
    assert_eq!((old, ovf), (0, false));
    assert_eq!(
      bit_field_execute_ro(
        &BitFieldCmdArgs::new(BitFieldSecondaryCommand::Get, 16, 4, 0, 0),
        &buf
      ),
      Some(0xABCD)
    );

    // INCRBY u8 @0：0+100 → 100；100+100 → 200
    let mut buf = vec![0u8; 2];
    let (v, ovf) = bit_field_execute(
      &BitFieldCmdArgs::new(BitFieldSecondaryCommand::IncrBy, 8, 0, 100, 0),
      &mut buf,
    )
    .unwrap();
    assert_eq!((v, ovf), (100, false));
    let (v, ovf) = bit_field_execute(
      &BitFieldCmdArgs::new(BitFieldSecondaryCommand::IncrBy, 8, 0, 100, 0),
      &mut buf,
    )
    .unwrap();
    assert_eq!((v, ovf), (200, false));

    // INCRBY u8 FAIL 溢出：C# IncrementBitfield 溢出时仍落盘 newValue=0，
    // 仅应答层以 nil 替代（与 Redis 不落盘相异，保持 C# 行为）
    let mut buf = vec![250u8, 0];
    let (v, ovf) = bit_field_execute(
      &BitFieldCmdArgs::new(
        BitFieldSecondaryCommand::IncrBy,
        8,
        0,
        10,
        BitFieldOverflow::Fail as u8,
      ),
      &mut buf,
    )
    .unwrap();
    assert!(ovf);
    assert_eq!(v, 0);
    assert_eq!(buf[0], 0, "C# FAIL 溢出仍写入 newValue");

    // SET 对缓冲不足（C# 抛 GarnetException 场景）→ None
    let mut short = vec![0u8; 1];
    assert_eq!(
      bit_field_execute(
        &BitFieldCmdArgs::new(BitFieldSecondaryCommand::Set, 16, 8, 1, 0),
        &mut short,
      ),
      None
    );

    // GET 起点越值界恒 0
    assert_eq!(get_bitfield(&[0u8; 2], 2, 100, 8, false), Some(0));
    assert_eq!(
      get_bitfield(&[0b1000_0000], 1, 4294967295, 1, false),
      Some(0)
    );
    assert_eq!(get_bitfield(&[0b1000_0000], 1, 0, 1, false), Some(1));

    // 邻位不受扰动：u8 @4 写 0xAB
    let mut buf = vec![0x00u8, 0xFF];
    let _ = bit_field_execute(
      &BitFieldCmdArgs::new(BitFieldSecondaryCommand::Set, 8, 4, 0xAB, 0),
      &mut buf,
    )
    .unwrap();
    // 低半字节保留 0x0，位 4..12 = 0xAB，位 12..16 保留 0xF
    assert_eq!(buf, vec![0x0A, 0xBF]);
  }

  /// ENCODING / OFFSET / OVERFLOW 词元解析（C# SessionParseStateExtensions 三口口径）
  #[test]
  fn bitfield_argument_parsing() {
    // 有符号 ≤ 64，无符号 < 64
    assert_eq!(parse_bitfield_encoding(b"i32"), Some((32, true)));
    assert_eq!(parse_bitfield_encoding(b"u63"), Some((63, false)));
    assert_eq!(parse_bitfield_encoding(b"u64"), None);
    assert_eq!(parse_bitfield_encoding(b"i65"), None);
    assert_eq!(parse_bitfield_encoding(b"x32"), None);
    assert_eq!(parse_bitfield_encoding(b"i"), None);
    // `#<n>` 倍乘形式与裸位偏移
    assert_eq!(parse_bitfield_offset(b"#5"), Some((5, true)));
    assert_eq!(parse_bitfield_offset(b"7"), Some((7, false)));
    assert_eq!(parse_bitfield_offset(b"#"), None);
    assert_eq!(parse_bitfield_offset(b"nope"), None);
    // 溢出策略大小写不敏感
    assert_eq!(
      parse_bitfield_overflow_slice(b"SAT"),
      Some(BitFieldOverflow::Sat)
    );
    assert_eq!(
      parse_bitfield_overflow_slice(b"wrap"),
      Some(BitFieldOverflow::Wrap)
    );
    assert_eq!(
      parse_bitfield_overflow_slice(b"fail"),
      Some(BitFieldOverflow::Fail)
    );
    assert_eq!(parse_bitfield_overflow_slice(b"nope"), None);
  }

  /// 严格整数口径（C# TryReadInt64Safe，allowLeadingZeros: false）
  #[test]
  fn bitfield_strict_int_semantics() {
    // 前导零拒绝
    assert_eq!(parse_bitfield_encoding(b"i032"), None);
    assert_eq!(parse_bitfield_offset(b"#05"), None);
    // 可选正号接受（C# TryReadSign 允许 +）
    assert_eq!(parse_bitfield_offset(b"+5"), Some((5, false)));
    assert_eq!(parse_bitfield_offset(b"#+7"), Some((7, true)));
  }
}
