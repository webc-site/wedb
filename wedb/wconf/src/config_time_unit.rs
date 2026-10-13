/// 时长类配置槽位的存储与 CONFIG 线上表达单位
///（对标 libs/server/Config/ConfigTimeUnit.cs:ConfigTimeUnit）。
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ConfigTimeUnit {
  /// 无单位：非时长类选项。
  None,
  /// 微秒。
  Microseconds,
  /// 毫秒。
  Milliseconds,
  /// 秒。
  Seconds,
}
