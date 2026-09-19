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
//! 装箱能力面（堆内存估算）不入清单：C# 侧仅部分扩展对象维护
//! `IHeapObject.HeapMemorySize`，各扩展 crate 的对象级记账入口自持，
//! MEMORY USAGE 消费点按现状直呼，不另立第二口径。

use wcustom::{CustomCommandMeta, CustomObjectEntry};

/// 扩展对象静态描述清单（编译期按特性组装；未启用扩展特性为空表）
pub(crate) const CUSTOM_OBJECT_ENTRIES: &[CustomObjectEntry] = &[
  #[cfg(feature = "roaring")]
  wext_roaring::RoaringCommand::OBJECT_ENTRY,
  #[cfg(feature = "json")]
  wext_json::JsonCommand::OBJECT_ENTRY,
];

/// 信封内层标签 → Redis TYPE 类型串（扩展段单点；None = 未知标签）
///
/// const 可用：清单元素个数为编译期常量，线性扫描由编译器展开为
/// 定长比对链（内建段见 `GarnetObjectType`，此处只承接扩展段）。
pub(crate) const fn custom_object_type_name(tag: u8) -> Option<&'static str> {
  let mut i = 0;
  while i < CUSTOM_OBJECT_ENTRIES.len() {
    if CUSTOM_OBJECT_ENTRIES[i].tag.as_u8() == tag {
      return Some(CUSTOM_OBJECT_ENTRIES[i].type_name);
    }
    i += 1;
  }
  None
}

/// 按名解析扩展命令 → 清单项 + 命令元数据（None = 未命中 / 空清单）
///
/// 信封标签自命中清单项的 `tag` 单点取值，命令元数据只承载执行面
/// （名称 / 命令类型 / arity / 执行体）。
pub(crate) fn match_custom_object_command(
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
pub(crate) fn is_custom_object_command(name: &str) -> bool {
  match_custom_object_command(name.as_bytes()).is_some()
}

#[cfg(all(test, any(feature = "roaring", feature = "json")))]
mod tests {
  use super::{
    CUSTOM_OBJECT_ENTRIES, custom_object_type_name, is_custom_object_command,
    match_custom_object_command,
  };

  /// 逐名核对解析面与 ACL 门判定一致（同一张清单、同一次命中，含大小写混写）
  fn assert_both_faces_agree(names: &[&'static str]) {
    for name in names {
      assert!(
        match_custom_object_command(name.as_bytes()).is_some(),
        "静态清单未登记扩展命令名 {name}"
      );
      assert!(
        is_custom_object_command(name),
        "ACL 门与清单判定漂移 {name}"
      );
      let lower = name.to_ascii_lowercase();
      assert!(
        match_custom_object_command(lower.as_bytes()).is_some() && is_custom_object_command(&lower),
        "大小写混写判定漂移 {lower}"
      );
    }
  }

  /// 未登记名（内建命令名 / 前缀残缺 / 空名）两面一致拒绝
  #[test]
  fn unregistered_names_rejected_on_both_faces() {
    for name in ["SET", "GET", "R.SETBI", "JSON.SETX", ""] {
      assert!(
        match_custom_object_command(name.as_bytes()).is_none(),
        "清单误命中非扩展命令 {name}"
      );
      assert!(!is_custom_object_command(name), "ACL 门误放行 {name}");
    }
  }

  /// 清单按特性组装：条目数与启用侧数一致，且每条标签 → TYPE 注册名可反查
  #[test]
  fn entries_cover_enabled_extensions() {
    let expected = usize::from(cfg!(feature = "roaring")) + usize::from(cfg!(feature = "json"));
    assert_eq!(CUSTOM_OBJECT_ENTRIES.len(), expected);
    for entry in CUSTOM_OBJECT_ENTRIES {
      assert_eq!(
        custom_object_type_name(entry.tag.as_u8()),
        Some(entry.type_name),
        "标签 → TYPE 注册名反查与清单项漂移"
      );
    }
  }

  #[cfg(feature = "roaring")]
  #[test]
  fn roaring_directory_matches_single_list() {
    let names: Vec<&'static str> = wext_roaring::COMMAND_INFOS
      .iter()
      .map(|info| info.name)
      .collect();
    assert_both_faces_agree(&names);
  }

  #[cfg(feature = "json")]
  #[test]
  fn json_directory_matches_single_list() {
    let names: Vec<&'static str> = wext_json::COMMAND_INFOS
      .iter()
      .map(|info| info.name)
      .collect();
    assert_both_faces_agree(&names);
  }
}
