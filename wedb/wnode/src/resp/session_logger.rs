//! 会话日志器（对标 libs/server/Resp/SessionLogger.cs）
//!
//! C# 包装 Microsoft.Extensions.Logging.ILogger：BeginScope 产出日志
//! 作用域（IDisposable）、IsEnabled 查询级别开关；rust 经 `log` 门面
//! 承接级别查询，作用域概念无对应语义（恒无作用域）。

/// 会话日志器
pub struct SessionLogger;

impl SessionLogger {
  /// libs/server/Resp/SessionLogger.cs:IsEnabled
  ///
  /// 级别开关查询（C# ILogger.IsEnabled；经 log 门面静态开关承接）
  pub fn is_enabled(level: log::Level) -> bool {
    log::log_enabled!(level)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn is_enabled_routes_through_log_facade() {
    // 与 log 门面静态开关逐级一致（测试期未初始化 logger 时恒 false 亦一致）
    for level in [
      log::Level::Error,
      log::Level::Warn,
      log::Level::Info,
      log::Level::Debug,
      log::Level::Trace,
    ] {
      assert_eq!(SessionLogger::is_enabled(level), log::log_enabled!(level));
    }
  }
}
