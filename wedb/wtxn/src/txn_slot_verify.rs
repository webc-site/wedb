//! 迭代式槽位校验切面（C# TxnKeyManager 经 respSession.clusterSession 消费的
//! 函数族投影；宿主集群切面在场时注入 [`TransactionManager`]，单机形态保持
//! None 剥离——同 C# clusterEnabled 判定语义）

/// 迭代式槽位校验切面
///
/// libs/server/Transaction/TxnKeyManager.cs:ResetCacheSlotVerificationResult
/// libs/server/Transaction/TxnKeyManager.cs:VerifyKeyOwnership
/// libs/server/Transaction/TxnKeyManager.cs:WriteCachedSlotVerificationMessage
///
/// 三函数经本切面回调集群会话（C# 直连 respSession.clusterSession 的形态由
/// 宿主适配承接）；`network_iterative_slot_verify` 返回 false 表示校验失败
/// （事务置 Aborted，失败原因已缓存待 [`Self::write_cached_slot_verification_message`]
/// 落线）
pub trait TxnSlotVerifyFace: Send + Sync {
  /// 重置迭代校验缓存（新事务批次起点）
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:ResetCacheSlotVerificationResult
  fn reset_cached_slot_verification_result(&self);

  /// 逐键迭代校验；false = 校验失败
  ///
  /// libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify
  fn network_iterative_slot_verify(&self, key: &[u8], read_only: bool) -> bool;

  /// 缓存裁决非 OK 时写出槽位验证错误（MOVED/ASK/CROSSSLOT/TRYAGAIN 等）
  ///
  /// libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:WriteCachedSlotVerificationMessage
  fn write_cached_slot_verification_message(&self, output: &mut Vec<u8>);
}
