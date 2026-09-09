//! 主存完成回调（对标 libs/server/Storage/Functions/MainStore/CallbackMethods.cs）
//!
//! 缺口说明：C# 侧回调把 Tsavorite 完成事件推入会话输出缓冲；wkv 读写
//! 同步闭环、输出经返回值直交调用方，回调退化为"已消费"确认。

/// 读完成回调（恒返回 true：输出已被同步路径消费）
///
/// libs/server/Storage/Functions/MainStore/CallbackMethods.cs:ReadCompletionCallback
pub fn read_completion_callback() -> bool {
  true
}

/// RMW 完成回调（恒返回 true）
///
/// libs/server/Storage/Functions/MainStore/CallbackMethods.cs:RMWCompletionCallback
pub fn rmw_completion_callback() -> bool {
  true
}
