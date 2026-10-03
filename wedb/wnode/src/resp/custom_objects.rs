//! 扩展对象编译期静态清单（server 层扩展对象分发/校验面的唯一入口）
//!
//! server 层扩展对象分发面的单点：TYPE 注册名解析
//!（[`custom_object_type_name`]，array_commands）、扩展命令解析落槽
//!（[`match_custom_object_command`]，parser/resp_command 快路径与
//! garnet_api/slow 慢路径重放）、ACL 按名校验
//!（[`is_custom_object_command`]，acl_commands SETUSER）共用这一张
//! 按 Cargo feature 组装的 const 清单——零运行时注册、零锁、零分配
//! （对标 C# CustomCommandManager 集中分配面的静态化承接，动态注册层
//! 按转写规范删除；C# 四个 Match 与 IsCustomCommandRegistered 亦全部
//! 单行转发同一 manager）。新增一个扩展对象类型：wval 枚举加一项、扩展
//! crate 自身 `OBJECT_ENTRY` 一条、本清单加一行，不再触碰分发代码
//! 内部的标签比对臂或第二处名单。
//!
//! 装箱能力面（堆内存估算）同入清单：C# `IHeapObject.HeapMemorySize` 由
//! 对象自身单点提供（值语义各异、入口同型——RoaringBitmapObject.cs 真实
//! 记账，GarnetJsonObject.cs:350 备注 Set 不更新、恒构造常数），各清单项
//! `heap_estimate` 承接，MEMORY USAGE 消费点（object_store_utils）自此零
//! 按 tag 手拼臂。

use wcustom::{CustomCommandDisplay, CustomCommandMeta, CustomObjectEntry};

/// 扩展对象静态描述清单（编译期按特性组装；未启用扩展特性为空表）
pub const CUSTOM_OBJECT_ENTRIES: &[CustomObjectEntry] = &[
  #[cfg(feature = "roaring")]
  wext_roaring::RoaringCommand::OBJECT_ENTRY,
  #[cfg(feature = "json")]
  wext_json::JsonCommand::OBJECT_ENTRY,
];

/// 信封内层标签 → Redis TYPE 类型串（扩展段单点；None = 未知标签）
///
/// const 可用：清单元素个数为编译期常量，线性扫描由编译器展开为
/// 定长比对链（内建段见 `GarnetObjectType`，此处只承接扩展段）。
pub const fn custom_object_type_name(tag: u8) -> Option<&'static str> {
  match custom_object_entry(tag) {
    Some(entry) => Some(entry.type_name),
    None => None,
  }
}

/// 信封内层标签 → 清单项（扩展标签域命中判定单点；None = 内置段 / 未知标签）
///
/// COSCAN 域收口消费：命中即自定义对象（C# header.type == All 仅
/// `CustomObjectBase.Operate` 接受转 Scan），未命中（内置 Hash/Set/SortedSet、
/// List 与未知标签）即 WRONGTYPE——与 [`custom_object_type_name`] 同一
/// 定长比对链形态
pub const fn custom_object_entry(tag: u8) -> Option<&'static CustomObjectEntry> {
  let mut i = 0;
  while i < CUSTOM_OBJECT_ENTRIES.len() {
    if CUSTOM_OBJECT_ENTRIES[i].tag.as_u8() == tag {
      return Some(&CUSTOM_OBJECT_ENTRIES[i]);
    }
    i += 1;
  }
  None
}

/// 按名解析扩展命令 → 清单项 + 命令元数据（None = 未命中 / 空清单）
///
/// 信封标签自命中清单项的 `tag` 单点取值，命令元数据只承载执行面
/// （名称 / 命令类型 / arity / 执行体）。
pub fn match_custom_object_command(
  command: &[u8],
) -> Option<(&'static CustomObjectEntry, CustomCommandMeta)> {
  CUSTOM_OBJECT_ENTRIES
    .iter()
    .find_map(|entry| (entry.match_command)(command).map(|meta| (entry, meta)))
}

/// 名字是否为清单内登记的扩展命令（ACL SETUSER 按名规则的未知名失败关闭门）
///
/// 与解析面同一张清单、同一次命中判定（C# 侧注册名查询与按名 Match 同读
/// CustomCommandManager 一处字典，见其 IsCustomCommandRegistered）：
/// 大小写不敏感语义由各清单项 `match_command` 单点承接。未启用任何扩展
/// 特性时清单为空表（恒 false），与 ACL 侧 `ccm == null` 跳过校验门同侧，
/// 故仅在扩展编译期在场时接线。
#[cfg(any(feature = "roaring", feature = "json"))]
pub fn is_custom_object_command(name: &str) -> bool {
  match_custom_object_command(name.as_bytes()).is_some()
}

/// 自省展示项总数（COMMAND 全表长度 / COMMAND COUNT 的扩展加项单点）
///
/// 与 [`CUSTOM_OBJECT_ENTRIES`] 同一张清单按特性组装：未启用扩展特性时
/// 清单为空表、各清单项 `command_display` 长度求和自然为 0——消费点
/// （basic_commands 的 COUNT / 全表两处）零 cfg 特判、零 feature 算术
/// （对标 C# GetCustomCommandInfoCount 即 customCommandsInfo 字典计数，
/// CustomCommandManager.cs:390；C# 未装载模块时计数 0 同源同形）。
pub const fn custom_command_display_count() -> usize {
  let mut total = 0;
  let mut i = 0;
  while i < CUSTOM_OBJECT_ENTRIES.len() {
    total += CUSTOM_OBJECT_ENTRIES[i].command_display.len();
    i += 1;
  }
  total
}

/// 自省展示项全集迭代（对标 C# BasicCommands.cs:1144 全表消费
/// GetAllCustomCommandsInfos 的枚举面；追加序 = 清单静态序 + 各清单项宏条目
/// 序，确定性可锁测；C# 侧 ConcurrentDictionary 枚举序本无定序，
/// rust 不仿制第二机制）
pub fn custom_command_displays() -> impl Iterator<Item = &'static CustomCommandDisplay> {
  CUSTOM_OBJECT_ENTRIES
    .iter()
    .flat_map(|entry| entry.command_display.iter())
}
