//! 有序集合 GEO 命令浮点格式化与定点展开集成测试
//! （对应 libs/server/Resp/Objects/SortedSetGeoCommands.cs）

use wnode::resp::objects::sorted_set_geo_commands::format_f6;

/// C# double.ToString("F6") 逐字节对齐（Infinity 词形与半点远离零舍入）
#[test]
fn format_f6_matches_csharp_fixed6() {
  // 无穷词形（.NET 为 Infinity，Rust {:.6} 为 inf）
  assert_eq!(format_f6(f64::INFINITY), "Infinity");
  assert_eq!(format_f6(f64::NEG_INFINITY), "-Infinity");
  // 常规定点
  assert_eq!(format_f6(181.0), "181.000000");
  assert_eq!(format_f6(12.0), "12.000000");
  assert_eq!(format_f6(13.361389), "13.361389");
  // 十进制第 7 位精确半点远离零（{:.6} 半到偶会得 .007812）
  assert_eq!(format_f6(0.0078125), "0.007813");
  assert_eq!(format_f6(180.0078125), "180.007813");
  assert_eq!(format_f6(-0.0078125), "-0.007813");
  // 负号保留（含舍入为零与 -0.0）
  assert_eq!(format_f6(-1e-7), "-0.000000");
  assert_eq!(format_f6(-0.0), "-0.000000");
  assert_eq!(format_f6(0.0), "0.000000");
  // 整数值大数定点展开
  assert_eq!(format_f6(1e20), "100000000000000000000.000000");
}
