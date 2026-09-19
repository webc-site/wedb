//! 迭代式槽位校验直穿句柄（对标 libs/server/Transaction/TxnKeyManager.cs
//! 与 libs/server/Custom/CustomTransactionProcedure.cs:AddKey）
//!
//! 宿主在 RUNTXP 执行前把集群迭代校验切面以 [`SlotVerifyHandle`]（即
//! `&dyn TxnSlotVerifyFace` 借用）直穿 `run_transaction_proc` 与
//! `TxnProcedure::prepare`，生命周期仅覆盖单次 run 调用窗口，单机 / 回放
//! 路径传 None。对标 C# TxnKeyManager 直持 respSession.clusterSession
//! 对象引用的虚分派，rust 等价物即 trait 对象借用，零堆分配、免手写擦除。

/// 迭代式槽位校验切面（宿主适配器 impl 载体）
///
/// libs/server/Transaction/TxnKeyManager.cs:ResetCacheSlotVerificationResult
/// libs/server/Transaction/TxnKeyManager.cs:VerifyKeyOwnership
/// libs/server/Transaction/TxnKeyManager.cs:WriteCachedSlotVerificationMessage
///
/// 三函数经本切面回调集群会话；`network_iterative_slot_verify` 返回 false
/// 表示校验失败（事务置 Aborted，失败原因已缓存待
/// [`Self::write_cached_slot_verification_message`] 落线）
pub trait TxnSlotVerifyFace {
  /// 重置迭代校验缓存（新事务批次起点）
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:ResetCacheSlotVerificationResult
  fn reset_cached_slot_verification_result(&self);

  /// 逐键迭代校验；false = 校验失败
  ///
  /// 宿主切面委托层：真实迭代校验在集群会话固有方法（权威映射注释在该处）
  fn network_iterative_slot_verify(&self, key: &[u8], read_only: bool) -> bool;

  /// 缓存裁决非 OK 时写出槽位验证错误（MOVED/ASK/TRYAGAIN）
  ///
  /// 宿主切面委托层：真实写出在集群会话固有方法（权威映射注释在该处）
  fn write_cached_slot_verification_message(&self, output: &mut Vec<u8>);
}

/// 栈借型槽位校验句柄（trait 对象借用，对标 C# 直持会话引用；
/// 生命周期仅覆盖单次 run 调用窗口）
pub type SlotVerifyHandle<'a> = &'a dyn TxnSlotVerifyFace;
