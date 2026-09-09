//! 统一存完成回调（对标 libs/server/Storage/Functions/UnifiedStore/CallbackMethods.cs）

/// 读完成回调（wkv 同步闭环，输出已被消费）
///
/// libs/server/Storage/Functions/UnifiedStore/CallbackMethods.cs:ReadCompletionCallback
pub fn read_completion_callback() -> bool {
  true
}

/// RMW 完成回调
///
/// libs/server/Storage/Functions/UnifiedStore/CallbackMethods.cs:RMWCompletionCallback
pub fn rmw_completion_callback() -> bool {
  true
}
