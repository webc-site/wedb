/// 日志压缩类型（对标 libs/server/LogCompactionType.cs:LogCompactionType）。
///
/// 归入 config 域：当前仅被 CONFIG 表（compaction-type）引用；
/// 若存储域后续收敛独立定义，此处应改为复用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum LogCompactionType {
  /// 不压缩（默认）。
  #[default]
  None = 0,
  /// 平移 begin address 且不压缩活跃记录（有数据丢失）；
  /// 需再执行 checkpoint 才会真正删除磁盘文件。
  Shift = 1,
  /// 对压缩区间逐记录做活跃性检查（hash chain），无数据丢失
  /// （压缩后执行 checkpoint 以删除磁盘数据文件）。生产推荐。
  Lookup = 2,
  /// 自 untilAddress 向只读地址扫描做记录活跃性检查，无数据丢失
  /// （压缩后执行 checkpoint 以删除磁盘数据文件）。
  /// 不推荐：需构建与键空间成正比的临时并行 KV 索引，瞬时内存开销大。优先 Lookup。
  Scan = 3,
}

impl LogCompactionType {
  /// 全部已声明成员（判别值升序），供枚举解析与名字反查复用。
  pub const MEMBERS: [LogCompactionType; 4] = [Self::None, Self::Shift, Self::Lookup, Self::Scan];

  /// 判别值反查已声明成员（对标 Enum.IsDefined + 强转语义）。
  pub fn from_raw(raw: i64) -> Option<Self> {
    Self::MEMBERS.iter().copied().find(|m| *m as i64 == raw)
  }

  /// 成员名的规范大写形式（C# 枚举成员名），如 "None"、"Shift"。
  pub fn as_name(self) -> &'static str {
    match self {
      Self::None => "None",
      Self::Shift => "Shift",
      Self::Lookup => "Lookup",
      Self::Scan => "Scan",
    }
  }

  /// 按成员名或十进制数值解析（忽略大小写，仅接受已声明成员；
  /// 对标 EnumExtensions.TryParseEnumToLong 对 LogCompactionType 的语义）。
  pub fn try_parse(value: &str) -> Option<Self> {
    if let Ok(raw) = value.parse::<i64>() {
      return Self::from_raw(raw);
    }
    Self::MEMBERS
      .iter()
      .copied()
      .find(|m| m.as_name().eq_ignore_ascii_case(value))
  }
}
