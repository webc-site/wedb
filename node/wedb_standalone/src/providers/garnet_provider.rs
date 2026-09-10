//! Garnet provider（对标 libs/server/Providers/GarnetProvider.cs 的会话装配面）
//!
//! C# GetSession 以存储上下文 / 会话指标装配 StorageSession——纯 .NET
//! DI 装配管道；rust 存储会话由 wkv 批处理会话生命周期绑定
//! （databases 域 SingleDatabaseManager 装配），本域保留承接说明。

/// Garnet provider
pub struct GarnetProvider;

impl GarnetProvider {
  /// libs/server/Providers/GarnetProvider.cs:GetSession
  ///
  /// 缺口说明：C# 按存储上下文装配会话；rust 会话生命周期与 wkv 批处理
  /// 会话绑定（enter_batch → StorageSession::new，见 databases 域），
  /// 无独立装配入口（豁免登记见 js/check/ignore/libs/server/Providers/
  /// GarnetProvider.yml）
  pub fn get_session() {}
}
