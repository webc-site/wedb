//! Tsavorite KV provider 基类（对标 libs/server/Providers/TsavoriteKVProviderBase.cs）
//!
//! C# 为 Tsavorite KV 存储会话装配基类（GetSession 工厂）——.NET 泛型
//! 存储装配管道；rust 存储面统一经 wkv 会话承接。

/// Tsavorite KV provider 基类
pub struct TsavoriteKVProviderBase;

impl TsavoriteKVProviderBase {
  /// libs/server/Providers/TsavoriteKVProviderBase.cs:GetSession
  ///
  /// 缺口说明：C# 存储会话工厂基类；rust 统一经 wkv 会话装配（同
  /// garnet_provider 域说明），无独立工厂（豁免登记见 js/check/ignore/
  /// libs/server/Providers/TsavoriteKVProviderBase.yml）
  pub fn get_session() {}
}
