//! 按键的未完成清理工作集合（对标 libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkSet.cs）
//!
//! 统一沉降复用 [`wbase::pool::EventWorkSet`]，彻底消除重复基建与锁自旋实现。

use wbase::pool::EventWorkSet;

/// 按键的未完成清理工作集合（按键字节的字典序等价比较）。
pub type VectorSetCleanupWorkSet<TValue> = EventWorkSet<Vec<u8>, TValue>;
