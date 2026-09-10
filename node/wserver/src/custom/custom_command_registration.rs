//! 自定义命令注册（对标 libs/server/Custom/CustomCommandRegistration.cs）
//!
//! C# 承载自定义命令提供者的注册面（RegisterCommandProvider → 命令表
//! 接线）；custom 域核心（命令管理器 / 会话分派）为并行推进面。

/// 自定义命令注册
pub struct CustomCommandRegistration;

impl CustomCommandRegistration {
  /// libs/server/Custom/CustomCommandRegistration.cs:GetRegisterCustomCommandProvider
  ///
  /// 缺口说明：注册提供者获取入口；rust custom 域并行推进面（豁免登记见
  /// js/check/ignore/libs/server/Custom/CustomCommandRegistration.yml）
  pub fn get_register_custom_command_provider() {}
}

impl CustomCommandRegistration {
  /// libs/server/Custom/CustomCommandRegistration.cs:Register
  ///
  /// 缺口说明：命令表接线入口；同上承接说明
  pub fn register() {}
}
