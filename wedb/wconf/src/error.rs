/// CONFIG SET 各环节的拒绝原因（对标 RuntimeServerConfig.TrySet /
/// ApplyCommitFrequencyUpdate 中的 `ERR ...` 错误串，文案与 C# 逐字对齐）。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
  /// 只读参数不可运行时修改。
  #[error("ERR Option '{name}' is read-only and cannot be set at runtime.")]
  ReadOnly { name: String },

  /// 整数解析失败。
  #[error("ERR Invalid value for '{name}': expected an integer.")]
  InvalidInteger { name: String },

  /// 数值超出声明区间。
  #[error("ERR Value for '{name}' is out of range ({min}..{max}).")]
  OutOfRange { name: String, min: i64, max: i64 },

  /// 布尔值仅接受 yes/true/1 或 no/false/0。
  #[error("ERR Invalid value for '{name}': expected 'yes' or 'no'.")]
  InvalidBool { name: String },

  /// 枚举成员解析失败。
  #[error("ERR Invalid value for '{name}': '{value}'.")]
  InvalidEnum { name: String, value: String },

  /// 非 runtime 可调选项。
  #[error("ERR Option '{name}' is not runtime-adjustable.")]
  NotRuntimeAdjustable { name: String },

  /// aof-commit-freq 不能在运行期改为 0（逐操作自动提交在启动时固化）。
  #[error(
    "ERR 'aof-commit-freq' cannot be set to 0 at runtime; per-operation auto-commit is fixed at startup."
  )]
  CommitFreqZero,

  /// 启动即为逐操作自动提交（0）时，aof-commit-freq 不可再改。
  #[error(
    "ERR 'aof-commit-freq' cannot be changed at runtime because the server started with per-operation auto-commit (0)."
  )]
  CommitFreqAutoCommitStart,

  /// 槽位原始值越界或非已声明枚举成员（对齐 C# GetEnum 的 InvalidOperationException）。
  #[error(
    "The raw configuration value {raw} is out of bounds or not a defined member of the declared enum."
  )]
  EnumOutOfRange { raw: i64 },
}
