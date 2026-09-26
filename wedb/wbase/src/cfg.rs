//! 跨 crate 基座配置定义（`cfg` 域）
//!
//! 收纳「多个互不应依赖的 crate 必须共用同一判据」的枚举与常量（规则出处
//! task/review.md:31）：底层存储引擎与上层配置库不得互相依赖，故其共享口径
//! 一律下沉到本基座模块，两侧各自直接取用，不设第二套定义、也不做二次导出。
//!
//! 当前成员：
//! - [`LogCompactionType`]：日志紧缩四档旋钮，被 wconf 的 CONFIG 表名值解析与
//!   wkv 内置 GC 的档位分派共同消费；
//! - [`MAX_DATABASES_MIN`] / [`MAX_DATABASES_MAX`]：协议逻辑库地址空间界限，被
//!   wconf 的启动期定界与 wkv 的库级路由回建枚举共同消费；
//! - [`DEFAULT_HLOG_PAGE_SIZE`]：生产主存日志页容量基线，被底层存储日志 crate
//!   （whlog 服务器页默认、waof WAL 内存写缓冲默认）与上层 wconf 的节点配置
//!   默认共同消费。

/// 逻辑数据库数量上限下界（对标 C# Options.cs:687 MaxDatabases 的
/// IntRangeValidation(1, 256, isRequired: true) 启动期定界）
pub const MAX_DATABASES_MIN: i32 = 1;

/// 逻辑数据库数量上限上界（对标 C# Options.cs:687 MaxDatabases 的
/// IntRangeValidation(1, 256, isRequired: true) 启动期定界）
///
/// 本值同时是协议层逻辑库地址空间的绝对上界：配置层按它定界 `max_databases`，
/// 存储层按它枚举回建档位，两处同一判据，杜绝上下界分叉导致的漏装载
pub const MAX_DATABASES_MAX: i32 = 256;

/// 生产默认主存日志页容量字节（16MB，对标 C# ServerOptions.cs:46
/// PageSize = "16m"）。
///
/// 归入基座 config 域：本判据同时被底层存储日志 crate（whlog 的服务器页默认、
/// waof 的 WAL 内存写缓冲默认）与上层配置库 wconf 的节点配置默认消费，而存储
/// 层依拓扑不得反向依赖配置层。C# 侧同样是服务器基线值只在顶层装配阶段向下
/// 注入存储引擎（Tsavorite core 对页容量自持基线，零引用 libs/server 配置）。
pub const DEFAULT_HLOG_PAGE_SIZE: usize = 16 * 1024 * 1024;

/// 日志压缩类型（对标 libs/server/LogCompactionType.cs:LogCompactionType）。
///
/// 归入基座 config 域而非上层 wconf：本枚四档旋钮同时被配置面（CONFIG 表
/// compaction-type 的名值解析与运行时槽位）与存储引擎内置 GC（wkv `gc` 模块按
/// 档分派 None/Shift/Lookup/Scan）消费，而存储层依拓扑不得反向依赖配置层。
/// C# 的对应关系：libs/server 的 `LogCompactionType` 是面向用户的档位，
/// `DatabaseManagerBase.cs:DoCompactionAsync` 按它分派——None 短路、Shift 走
/// `ShiftBeginAddress`、Scan/Lookup 走 Tsavorite core 的 `CompactionType`
/// （对位本仓 `wcompact::CompactionType`，仅 Scan/Lookup 两档）。两枚枚举族别
/// 不同、档位不同，各自单点定义，分派处即 C# 原作的映射点。
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
  #[inline]
  pub const fn from_raw(raw: i64) -> Option<Self> {
    match raw {
      0 => Some(Self::None),
      1 => Some(Self::Shift),
      2 => Some(Self::Lookup),
      3 => Some(Self::Scan),
      _ => None,
    }
  }

  /// 成员名的规范大写形式（C# 枚举成员名），如 "None"、"Shift"。
  #[inline]
  pub const fn as_name(self) -> &'static str {
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
    if value.eq_ignore_ascii_case("None") {
      Some(Self::None)
    } else if value.eq_ignore_ascii_case("Shift") {
      Some(Self::Shift)
    } else if value.eq_ignore_ascii_case("Lookup") {
      Some(Self::Lookup)
    } else if value.eq_ignore_ascii_case("Scan") {
      Some(Self::Scan)
    } else {
      None
    }
  }
}
