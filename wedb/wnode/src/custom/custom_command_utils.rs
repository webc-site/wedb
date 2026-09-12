//! 自定义命令参数工具（对标 libs/server/Custom/CustomCommandUtils.cs）
//!
//! C# 以 Unsafe.Read 钉住指针按槽位读取自定义命令参数（GetFirstArg /
//! GetNextArg 指针管道）；rust 参数面为切片形态，指针槽位读取无对应语义。

/// 自定义命令参数工具
pub struct CustomCommandUtils;

impl CustomCommandUtils {
  /// libs/server/Custom/CustomCommandUtils.cs:GetFirstArg
  ///
  /// 缺口说明：C# 钉住指针槽位读取；rust 参数面为切片形态（豁免登记见
  /// js/check/ignore/libs/server/Custom/CustomCommandUtils.yml）
  pub fn get_first_arg() {}

  /// libs/server/Custom/CustomCommandUtils.cs:GetNextArg
  ///
  /// 缺口说明：同 [`Self::get_first_arg`]
  pub fn get_next_arg() {}
}
