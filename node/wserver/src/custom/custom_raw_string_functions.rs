//! 自定义原始字符串函数基类（对标 libs/server/Custom/CustomRawStringFunctions.cs）
//!
//! C# 抽象基类为自定义字符串函数提供 StringInput 的参数读取助手
//! （GetFirstArg / GetNextArg：钉住指针槽位推进管道，委托
//! CustomCommandUtils）；rust 参数面为切片形态，指针槽位读取无对应语义。

/// 自定义原始字符串函数
pub struct CustomRawStringFunctions;

impl CustomRawStringFunctions {
  /// libs/server/Custom/CustomRawStringFunctions.cs:GetFirstArg
  ///
  /// 缺口说明：C# 钉住指针槽位参数读取助手；rust 参数面为切片形态
  /// （豁免登记见 js/check/ignore/libs/server/Custom/
  /// CustomRawStringFunctions.yml）
  pub fn get_first_arg() {}

  /// libs/server/Custom/CustomRawStringFunctions.cs:GetNextArg
  ///
  /// 缺口说明：同 [`Self::get_first_arg`]
  pub fn get_next_arg() {}
}
