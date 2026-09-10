//! 字符串输出（对标 libs/server/StringOutput.cs 的指针构造面）
//!
//! C# StringOutput 经钉住指针 / 钉住span构造输出游标（dcurr/dend 指针
//! 管道）；rust 命令层经 `output: &mut Vec<u8>` 统一写出（见 RespServerSession
//! 忽略项同款结论），本结构保留既有承载说明。

/// 字符串输出
pub struct StringOutput;

impl StringOutput {
  /// libs/server/StringOutput.cs:FromPinnedPointer
  ///
  /// 缺口说明：C# 从钉住指针构造输出游标，属 unsafe 指针管道；rust 输出
  /// 统一经 `Vec<u8>` 缓冲承接，无指针游标形态（豁免登记见 js/check/ignore/
  /// libs/server/StringOutput.yml）
  pub fn from_pinned_pointer() {}

  /// libs/server/StringOutput.cs:FromPinnedSpan
  ///
  /// 缺口说明：同 [`Self::from_pinned_pointer`]，钉住span形态由切片直写承接
  pub fn from_pinned_span() {}
}
