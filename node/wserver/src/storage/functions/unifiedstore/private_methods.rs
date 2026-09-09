//! 统一存函数私有辅助（对标 libs/server/Storage/Functions/UnifiedStore/PrivateMethods.cs）

use crate::storage::functions::session_functions_utils::{ExpireEval, SessionFunctionsUtils};

/// 就地过期裁决（统一视图 = 键级 TTL 记录）
///
/// libs/server/Storage/Functions/UnifiedStore/PrivateMethods.cs:EvaluateExpireInPlace
pub fn evaluate_expire_in_place(expiry_ms: Option<u64>, now_ms: u64) -> ExpireEval {
  SessionFunctionsUtils::evaluate_expire(expiry_ms, now_ms)
}

/// 拷贝更新过期裁决（到期须以墓碑拷贝更新落盘）
///
/// libs/server/Storage/Functions/UnifiedStore/PrivateMethods.cs:EvaluateExpireCopyUpdate
pub fn evaluate_expire_copy_update(expiry_ms: Option<u64>, now_ms: u64) -> bool {
  evaluate_expire_in_place(expiry_ms, now_ms) == ExpireEval::Expired
}
