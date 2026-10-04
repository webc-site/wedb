//! RESP 协议命令目录单点：内嵌 JSON 的一次反序列化 + 全部静态索引
//!
//! 对标 libs/server/Resp/RespCommandsInfo.cs（静态构造单次反序列化，全进程
//! 一份）：数据单一真值为 wresp 内嵌 RespCommandsInfo.json，由本模块
//! [`try_initialize`] 经 [`data_provider`] 校验导入并一次性反序列化，产物
//! 同时喂两张消费面——[`commands_info`] 的完整元数据索引（按名 / 按枚举 /
//! KeySpec）与本模块的 ACL 目录（356 条 [`CmdEntry`] 扁平表，
//! 对标 libs/server/Resp/RespCommandInfoFlags.cs:RespAclCategories 的消费
//! 面）。域类型字符串字段为 `&'static str`（解析期一次定型），初始化失败
//! （JSON 损坏 / 枚举名不可解析）即空表 + 查询 None，对标 C# TryInitialize
//! 的 false 语义。
//!
//! 查询面三张索引（cmd 判别值直下标 / 父命令→子命令映射 / 单 ACL 分类成员列）
//! 在目录构建期一次填齐（对标 C# RespCommandsInfo / GarnetCommandInfo 的字典
//! O(1) 查表与 IndividualAcls 拆位预展开），[`try_get_resp_command_info`] /
//! [`children_of`] / [`commands_for_category`] 不再对扁平表线性扫。
//!
//! 在 garnet 中的相对路径:libs/server/Resp/RespCommandsInfo.cs

use std::{array, sync::OnceLock};

use bitflags::bitflags;
use wbase::map::HashMap;

use crate::command::{LAST_VALID_COMMAND, RespCommand};

/// 命令元数据（C# Garnet.resources:RespCommandsInfo.json）
pub const RESP_COMMANDS_INFO_JSON: &str = include_str!("../../RespCommandsInfo.json");

/// 命令文档（C# Garnet.resources:RespCommandsDocs.json）
pub const RESP_COMMANDS_DOCS_JSON: &str = include_str!("../../RespCommandsDocs.json");

pub mod commands_info;
pub mod data_provider;
pub mod simplified;

pub use commands_info::{
  RespCommandFlags, RespCommandsInfo, RespCommandsTables, acl_category_descriptions,
  get_resp_command_name, try_get_resp_command_info_by_cmd, try_get_resp_command_info_by_name,
  try_get_resp_command_names, try_get_resp_commands_info, try_get_resp_commands_info_count,
  try_get_resp_commands_info_ordered, try_get_simple_resp_command_info,
};
use commands_info::{RespCommandsInfoImport, build_tables};
pub use data_provider::{IRespCommandData, try_import_resp_commands_data};
pub use simplified::{
  SimpleRespCommandInfo, SimpleRespKeySpec, SimpleRespKeySpecBeginSearch,
  SimpleRespKeySpecFindKeys, extract_keys_and_flags_from_slice, extract_keys_from_slice,
  populate_simple_command_info, try_get_simple_key_spec,
};

/// 成员名串 → 位集归并公共骨架（"A, B" 逗号分隔；未知名即整串失败）
///
/// 四处成员名串解析（RespCommandFlags / RespAclCategories /
/// RespCommandArgumentFlags / KeySpecificationFlags）的单一收口；
/// `single` 承接单名 → 位（u32 口径）的解析，trim 与否由各调用方
/// 语义决定。导入期路径，非热。
pub(crate) fn parse_member_names(
  names: &str,
  mut single: impl FnMut(&str) -> Option<u32>,
) -> Option<u32> {
  let mut bits = 0_u32;
  for part in names.split(',') {
    bits |= single(part)?;
  }
  Some(bits)
}

/// 单条命令目录（RespCommandsInfo 中 ACL 消费的最小面）
pub struct CmdEntry {
  /// C# 枚举成员名（Enum.TryParse 对照，含下划线）
  pub cs: &'static str,
  /// 展示名（info.Name 小写；子命令为 parent|sub 形式）
  pub name: &'static str,
  /// 对应 RespCommand
  pub cmd: RespCommand,
  /// ACL 分类位集
  pub cats: RespAclCategories,
  /// 父命令（仅子命令有）
  pub parent: Option<RespCommand>,
}

bitflags! {
  /// RESP ACL 分类位集
  ///
  /// libs/server/Resp/RespCommandInfoFlags.cs:RespAclCategories
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
  pub struct RespAclCategories: u32 {
    /// 管理
    const ADMIN = 1;
    /// 位图
    const BITMAP = 1 << 1;
    /// 阻塞
    const BLOCKING = 1 << 2;
    /// 连接
    const CONNECTION = 1 << 3;
    /// 危险
    const DANGEROUS = 1 << 4;
    /// 地理
    const GEO = 1 << 5;
    /// 哈希
    const HASH = 1 << 6;
    /// HyperLogLog
    const HYPERLOGLOG = 1 << 7;
    /// 快速
    const FAST = 1 << 8;
    /// 键空间
    const KEYSPACE = 1 << 9;
    /// 列表
    const LIST = 1 << 10;
    /// 发布订阅
    const PUBSUB = 1 << 11;
    /// 读
    const READ = 1 << 12;
    /// 脚本
    const SCRIPTING = 1 << 13;
    /// 集合
    const SET = 1 << 14;
    /// 有序集合
    const SORTEDSET = 1 << 15;
    /// 慢速
    const SLOW = 1 << 16;
    /// 流
    const STREAM = 1 << 17;
    /// 字符串
    const STRING = 1 << 18;
    /// 事务
    const TRANSACTION = 1 << 19;
    /// 写
    const WRITE = 1 << 20;
    /// Garnet 扩展
    const GARNET = 1 << 21;
    /// 自定义命令
    const CUSTOM = 1 << 22;
    /// 向量集
    const VECTOR = 1 << 23;
    /// 全部（C# All = (Vector << 1) - 1，覆盖全部单类别位）
    const ALL = (1 << 24) - 1;
  }
}

impl RespAclCategories {
  /// ACL 分类计数（popcnt）
  #[inline]
  pub const fn count(&self) -> usize {
    self.bits().count_ones() as usize
  }

  /// 迭代 ACL 分类的 wire 描述（零堆分配）
  pub fn iter_descriptions(&self) -> impl Iterator<Item = &'static str> + '_ {
    Self::CATEGORIES
      .iter()
      .filter(|(_, cat)| self.contains(*cat))
      .map(|(desc, _)| *desc)
  }

  /// C# 枚举成员名 → 类别位（声明序，单一数据源；crate 内经
  /// [`Self::from_member_names`] 与 `acl_category_descriptions` 消费）
  pub(crate) const CATEGORIES: [(&'static str, Self); 24] = [
    ("admin", Self::ADMIN),
    ("bitmap", Self::BITMAP),
    ("blocking", Self::BLOCKING),
    ("connection", Self::CONNECTION),
    ("dangerous", Self::DANGEROUS),
    ("geo", Self::GEO),
    ("hash", Self::HASH),
    ("hyperloglog", Self::HYPERLOGLOG),
    ("fast", Self::FAST),
    ("keyspace", Self::KEYSPACE),
    ("list", Self::LIST),
    ("pubsub", Self::PUBSUB),
    ("read", Self::READ),
    ("scripting", Self::SCRIPTING),
    ("set", Self::SET),
    ("sortedset", Self::SORTEDSET),
    ("slow", Self::SLOW),
    ("stream", Self::STREAM),
    ("string", Self::STRING),
    ("transaction", Self::TRANSACTION),
    ("write", Self::WRITE),
    ("garnet", Self::GARNET),
    ("custom", Self::CUSTOM),
    ("vector", Self::VECTOR),
  ];

  /// 成员名串解析（"Fast, String, Write"；大小写不敏感，未知名即失败）
  ///
  /// libs/server/Resp/RespCommandsInfo.cs:AclCategories 反序列化
  /// （C# JsonStringEnumConverter 语义）
  pub fn from_member_names(names: &str) -> Option<Self> {
    parse_member_names(names, |part| {
      let trimmed = part.trim();
      Self::CATEGORIES
        .iter()
        .find(|(member, _)| member.eq_ignore_ascii_case(trimmed))
        .map(|(_, cat)| cat.bits())
    })
    .map(Self::from_bits_retain)
  }
}

// —— 单点编排（一次导入 → 完整表 + ACL 目录） ——

/// 单点初始化产物（完整元数据索引 + ACL 目录投影 + 查询索引一次填齐）
struct Catalog {
  tables: RespCommandsTables,
  /// 扁平目录条目（leak 定型 'static，三张查询索引以静态引用/下标持有）
  entries: &'static [CmdEntry],
  /// cmd 判别值 → 条目直下标（表长 = [`LAST_VALID_COMMAND`] 上界 + 1；同判别值
  /// 别名条目首个声明胜出，对齐线性 find 语义，如 SECONDARYOF/SLAVEOF 双别名）
  by_cmd: Vec<Option<&'static CmdEntry>>,
  /// 父命令判别值 → 直接子命令条目（声明序）
  children: HashMap<u16, Vec<&'static CmdEntry>>,
  /// 单 ACL 分类（[`RespAclCategories::CATEGORIES`] 位序）→ 成员条目下标
  /// （声明序；下标制使多类别归并可按声明序排序去重，见 [`CategoryIter`]）
  cat_members: [Vec<u16>; RespAclCategories::CATEGORIES.len()],
}

static CATALOG: OnceLock<Option<Catalog>> = OnceLock::new();

/// 导入 + 校验 + 域转换（JSON 文本 → 先序域类型树）
///
/// libs/server/Resp/RespCommandsInfo.cs:TryInitializeRespCommandsInfo
fn try_initialize_resp_commands_info() -> Option<Vec<RespCommandsInfo>> {
  let imported = try_import_resp_commands_data::<RespCommandsInfoImport>(RESP_COMMANDS_INFO_JSON)?;
  RespCommandsInfoImport::convert_all(imported)
}

/// JSON 文本 → 完整元数据索引纯函数面（不落 CATALOG 静态；测试以合成
/// JSON 对拍 C# 数据源形态，锁 Command 缺键值缺省语义与跨父重名熔断判据）
///
/// C# TryInitializeRespCommandsInfo 的纯函数测试形态（同名真身锚留
/// [`try_initialize_resp_commands_info`] 一处）
pub fn try_build_resp_commands_info_tables(json: &str) -> Option<RespCommandsTables> {
  let imported = try_import_resp_commands_data::<RespCommandsInfoImport>(json)?;
  let roots = RespCommandsInfoImport::convert_all(imported)?;
  build_tables(&roots)
}

/// 一次导入 + 反序列化 + 双面构建（全进程仅此一处解析 RespCommandsInfo.json）
///
/// libs/server/Resp/RespCommandsInfo.cs:TryInitialize
fn init_catalog() -> Option<Catalog> {
  let roots = try_initialize_resp_commands_info()?;
  let tables = build_tables(&roots)?;
  // 条目表 leak 定型 'static：by_cmd/children/cat_members 三张查询索引以静态
  // 引用/下标持有，Catalog 整体驻留 CATALOG，无自引用悬垂
  let entries: &'static [CmdEntry] = Box::leak(project_entries(&roots).into_boxed_slice());
  Some(Catalog::build(tables, entries))
}

impl Catalog {
  /// 查询索引一次填齐（对标 C# RespCommandsInfo/GarnetCommandInfo 的字典 O(1)
  /// 查表与 IndividualAcls 拆位预展开）：cmd 直下标 + 父子映射 + 单类别成员列，
  /// 构建期一遍 O(n)，查询面不再 entries().iter() 线性扫表
  fn build(tables: RespCommandsTables, entries: &'static [CmdEntry]) -> Self {
    // 表长即枚举判别值上界（LAST_VALID_COMMAND 为除 Invalid 外最大判别值，
    // 目录条目 cmd 均为真实命令，直下标恒在界内）
    let mut by_cmd: Vec<Option<&'static CmdEntry>> = vec![None; LAST_VALID_COMMAND as usize + 1];
    let mut children: HashMap<u16, Vec<&'static CmdEntry>> = HashMap::default();
    let mut cat_members = array::from_fn(|_| Vec::new());
    for (idx, entry) in entries.iter().enumerate() {
      // 首个声明条目胜出（try_get_resp_command_info 原 find 语义：别名共存时
      // SECONDARYOF 先于 SLAVEOF）
      let slot = &mut by_cmd[entry.cmd as usize];
      if slot.is_none() {
        *slot = Some(entry);
      }
      if let Some(parent) = entry.parent {
        children.entry(parent as u16).or_default().push(entry);
      }
      for (ci, (_, cat)) in RespAclCategories::CATEGORIES.iter().enumerate() {
        if entry.cats.contains(*cat) {
          cat_members[ci].push(idx as u16);
        }
      }
    }
    Self {
      tables,
      entries,
      by_cmd,
      children,
      cat_members,
    }
  }
}

/// 目录初始化（幂等；对标 C# 静态构造的首次访问触发）
pub fn try_initialize() -> bool {
  CATALOG.get_or_init(init_catalog).is_some()
}

/// 已初始化目录（三张查询索引与 entries/tables 共用的静态取数口）
fn catalog() -> Option<&'static Catalog> {
  try_initialize();
  CATALOG.get().and_then(|c| c.as_ref())
}

/// 已初始化的完整元数据索引
pub(crate) fn tables() -> Option<&'static RespCommandsTables> {
  catalog().map(|c| &c.tables)
}

/// 全量命令目录（根 + 子命令扁平表，序即 JSON 声明序先序）
pub fn entries() -> &'static [CmdEntry] {
  catalog().map(|c| c.entries).unwrap_or(&[])
}

/// ACL 目录投影：完整树先序遍历（根 → 子命令），全量收录含 IsInternal 与
/// Name=SLAVEOF 历史别名条目，对标 C# AclCommandInfo 的全收构建（SLAVEOF
/// 去重仅存在于 INFO 表扁平枚举索引，因枚举键冲突而起，ACL 目录无此约束）
fn project_entries(roots: &[RespCommandsInfo]) -> Vec<CmdEntry> {
  fn push(out: &mut Vec<CmdEntry>, entry: &RespCommandsInfo, parent: Option<RespCommand>) {
    let cs: &'static str = entry.command.into();
    out.push(CmdEntry {
      cs,
      name: commands_info::static_str(entry.name.to_lowercase()),
      cmd: entry.command,
      cats: entry.acl_categories,
      parent,
    });
    for sub in &entry.sub_commands {
      push(out, sub, Some(entry.command));
    }
  }
  let mut out = Vec::new();
  for root in roots {
    push(&mut out, root, None);
  }
  out
}

/// SET 的 ACL 展开集（对标 C# ExpandedSET）
const EXPANDED_SET: [RespCommand; 4] = [
  RespCommand::Setexnx,
  RespCommand::Setexxx,
  RespCommand::Setkeepttl,
  RespCommand::Setkeepttlxx,
];

/// BITOP 的 ACL 展开集（对标 C# ExpandedBITOP）
const EXPANDED_BITOP: [RespCommand; 5] = [
  RespCommand::BitopAnd,
  RespCommand::BitopNot,
  RespCommand::BitopOr,
  RespCommand::BitopXor,
  RespCommand::BitopDiff,
];

/// 把"并非真实命令"的枚举值归一到 ACL 等价命令
///
/// libs/server/Resp/Parser/RespCommand.cs:NormalizeForACLs
#[inline]
pub const fn normalize_for_acls(cmd: RespCommand) -> RespCommand {
  match cmd {
    RespCommand::Setexnx
    | RespCommand::Setexxx
    | RespCommand::Setkeepttl
    | RespCommand::Setkeepttlxx => RespCommand::Set,
    RespCommand::BitopAnd
    | RespCommand::BitopNot
    | RespCommand::BitopOr
    | RespCommand::BitopXor
    | RespCommand::BitopDiff => RespCommand::Bitop,
    _ => cmd,
  }
}

/// 反向于 [`normalize_for_acls`]：给出 `cmd` 覆盖的全部等价命令
///
/// libs/server/Resp/Parser/RespCommand.cs:ExpandForACLs
#[inline]
pub fn expand_for_acls(cmd: RespCommand) -> &'static [RespCommand] {
  match cmd {
    RespCommand::Set => &EXPANDED_SET,
    RespCommand::Bitop => &EXPANDED_BITOP,
    _ => &[],
  }
}

/// 未认证也可执行的命令（AUTH..=QUIT 连续区间）
///
/// libs/server/Resp/Parser/RespCommand.cs:IsNoAuth
#[inline]
pub const fn is_no_auth(cmd: RespCommand) -> bool {
  // 无符号化做区间判断：低于 AUTH 的值下溢成大数，天然出界
  let v = (cmd as u16).wrapping_sub(RespCommand::Auth as u16);
  v <= (RespCommand::Quit as u16).wrapping_sub(RespCommand::Auth as u16)
}

/// 按 RespCommand 取 ACL 目录条目（含子命令条目；cmd 判别值直下标 O(1)，
/// 别名条目首个声明胜出）
#[inline]
pub fn try_get_resp_command_info(cmd: RespCommand) -> Option<&'static CmdEntry> {
  // get 而非直下标：Invalid = 65535 等非目录判别值越界即返 None（与线性 find
  // 语义同点）
  catalog()?.by_cmd.get(cmd as usize).copied().flatten()
}

// 注：C# Enum.TryParse(ignoreCase) 的枚举成员名对照由
// [`crate::command::RespCommand::from_cs_name`] 编译期单源承接（cs 字段即其
// 反向派生产物），目录不设按 cs 名反查臂

/// 根命令的全部子命令条目（父子映射 O(1)，条目序即声明序）
#[inline]
pub fn children_of(cmd: RespCommand) -> impl Iterator<Item = &'static CmdEntry> {
  catalog()
    .and_then(|c| c.children.get(&(cmd as u16)))
    .map(Vec::as_slice)
    .unwrap_or(&[])
    .iter()
    .copied()
}

/// 分类成员条目（对标 C# AclCommandInfo 字典查询与 IndividualAcls 拆位预展开：
/// 单类别位成员列构建期一次展开，查询按命中位取列 k 路归并，不再全表扫；
/// 复合分类产出序与逐条目一次语义同线性 filter）
///
/// 在 garnet 中的相对路径:libs/server/Resp/RespCommandsInfo.cs:TryGetCommandsforAclCategory
/// 在 garnet 中的相对路径:libs/server/Resp/RespCommandsInfo.cs:IndividualAcls
#[inline]
pub fn commands_for_category(acl: RespAclCategories) -> impl Iterator<Item = &'static CmdEntry> {
  let mut cursors = array::from_fn(|_| None);
  let mut entries: &'static [CmdEntry] = &[];
  if let Some(c) = catalog() {
    for (ci, (_, cat)) in RespAclCategories::CATEGORIES.iter().enumerate() {
      if acl.contains(*cat) {
        cursors[ci] = Some((c.cat_members[ci].as_slice(), 0));
      }
    }
    entries = c.entries;
  }
  CategoryIter {
    entries,
    cursors,
    last: None,
  }
}

/// 分类成员 k 路归并迭代器：各命中类别位的成员列皆声明序升下标，k 路
/// （k ≤ 单类别位数）升序归并去重后逐条目恰产出一次，产出序即全局声明序——
/// 与逐表 filter 的「声明序、逐条目一次」语义同点，代价从全表扫描降为候选列归并
struct CategoryIter {
  /// 条目表（成员下标 → 条目）
  entries: &'static [CmdEntry],
  /// 各命中类别位的 (成员下标列, 游标)
  cursors: [Option<(&'static [u16], usize)>; RespAclCategories::CATEGORIES.len()],
  /// 最近产出下标（跨游标去重锚）
  last: Option<u16>,
}

impl Iterator for CategoryIter {
  type Item = &'static CmdEntry;

  fn next(&mut self) -> Option<Self::Item> {
    let mut min = u16::MAX;
    for (list, pos) in self.cursors.iter_mut().flatten() {
      // 各列升序，≤ 最近产出的皆为已产出条目（跨列重复），跳过
      if let Some(last) = self.last {
        while *pos < list.len() && list[*pos] <= last {
          *pos += 1;
        }
      }
      if *pos < list.len() {
        min = min.min(list[*pos]);
      }
    }
    (min != u16::MAX).then(|| {
      self.last = Some(min);
      &self.entries[min as usize]
    })
  }
}
