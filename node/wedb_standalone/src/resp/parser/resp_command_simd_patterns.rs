//! 命令 SIMD 匹配模式（对标 libs/server/Resp/Parser/RespCommandSimdPatterns.cs）
//!
//! C# 经 System.Runtime.Intrinsics.Vector256 生成命令前缀匹配模式；
//! SIMD 通道属 .NET 硬件加速面，rust 解析器以自有字节比较承接。

/// 命令 SIMD 匹配模式
pub struct RespCommandSimdPatterns;

impl RespCommandSimdPatterns {
  /// libs/server/Resp/Parser/RespCommandSimdPatterns.cs:RespPattern
  ///
  /// 缺口说明：Vector256 匹配模式构造；rust 解析分派不经 SIMD 通道
  /// （豁免登记见 js/check/ignore/libs/server/Resp/Parser/
  /// RespCommandSimdPatterns.yml）
  pub fn resp_pattern() {}
}
