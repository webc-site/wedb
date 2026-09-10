//! 会话 provider 抽象（对标 libs/server/Sessions/ISessionProvider.cs）
//!
//! C# 接口按网络会话上下文装配业务会话——.NET 会话工厂管道；rust 会话
//! 生命周期由宿主域（wnode service / servers 域）承接。

/// 会话 provider 抽象
pub struct ISessionProvider;

impl ISessionProvider {
  /// libs/server/Sessions/ISessionProvider.cs:GetSession
  ///
  /// 缺口说明：C# 会话工厂接口；rust 会话装配由宿主域承接，本域无工厂
  /// 入口（豁免登记见 js/check/ignore/libs/server/Sessions/
  /// ISessionProvider.yml）
  pub fn get_session() {}
}
