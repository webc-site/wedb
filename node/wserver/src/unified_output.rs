//! 统一输出（对标 libs/server/UnifiedOutput.cs 的指针构造面）
//!
//! C# UnifiedOutput 从钉住指针构造统一存输出游标；rust 经 `Vec<u8>`
//! 统一写出（结论同 string_output 域说明）。

/// 统一输出
pub struct UnifiedOutput;

impl UnifiedOutput {
  /// libs/server/UnifiedOutput.cs:FromPinnedPointer
  ///
  /// 缺口说明：C# 钉住指针输出游标构造；rust 输出统一经 `Vec<u8>` 缓冲
  /// 承接（豁免登记见 js/check/ignore/libs/server/UnifiedOutput.yml）
  pub fn from_pinned_pointer() {}
}
