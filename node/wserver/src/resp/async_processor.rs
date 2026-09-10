//! 异步处理器（对标 libs/server/Resp/AsyncProcessor.cs）
//!
//! C# 以专用异步线程排空 pending GET（AsyncGetProcessorAsync 循环 +
//! NetworkGETPending 完成回写）；rust 侧异步 GET 经 `try_read_sync` 的
//! `Ok(None) → Ok(false)` 降级信号承接（见 basic_commands
//! network_get_async 注释），独立异步线程面已被替代。

/// 异步处理器
pub struct AsyncProcessor;

impl AsyncProcessor {
  /// libs/server/Resp/AsyncProcessor.cs:NetworkGETPending
  ///
  /// 缺口说明：pending GET 完成回写；rust 统一由同步降级信号承接
  /// （豁免登记见 js/check/ignore/libs/server/Resp/AsyncProcessor.yml）
  pub fn network_get_pending() {}

  /// libs/server/Resp/AsyncProcessor.cs:AsyncGetProcessorAsync
  ///
  /// 缺口说明：异步 GET 排空线程循环；rust 无独立异步完成线程，降级
  /// 信号由命令层闭环（同上承接说明）
  pub fn async_get_processor_async() {}
}
