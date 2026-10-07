#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 自定义扩展对象清单与展示面契约测试（custom_objects.rs 内联测试迁出）

use wcustom::{CommandType, KeyScope};
#[cfg(any(feature = "roaring", feature = "json"))]
use wnode::resp::custom_objects::is_custom_object_command;
use wnode::resp::custom_objects::{
  CUSTOM_OBJECT_ENTRIES, custom_command_display_count, custom_command_displays,
  custom_object_type_name, match_custom_object_command,
};

#[cfg(any(feature = "roaring", feature = "json"))]
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

#[cfg(any(feature = "roaring", feature = "json"))]
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

#[cfg(any(feature = "roaring", feature = "json"))]
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

#[cfg(any(feature = "roaring", feature = "json"))]
fn enabled_command_names() -> Vec<&'static str> {
  let mut names = Vec::new();
  #[cfg(feature = "roaring")]
  names.extend(wext_roaring::COMMAND_INFOS.iter().map(|info| info.name));
  #[cfg(feature = "json")]
  names.extend(wext_json::COMMAND_INFOS.iter().map(|info| info.name));
  names
}

#[cfg(any(feature = "roaring", feature = "json"))]
#[test]
fn multi_read_scope_commands_are_read_only() {
  let mut multi_read = 0;
  for name in enabled_command_names() {
    let (_, meta) = match_custom_object_command(name.as_bytes()).expect("清单未登记扩展命令名");
    if let KeyScope::MultiRead { .. } = meta.key_scope {
      assert_eq!(
        meta.command_type,
        CommandType::Read,
        "多键读命令 {name} 未登记只读"
      );
      multi_read += 1;
    }
  }
  assert_eq!(multi_read, 1, "多键读扩展命令面漂移");
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

#[test]
fn every_display_resolves_on_parse_face() {
  let mut iterated = 0;
  for display in custom_command_displays() {
    iterated += 1;
    let (_, meta) = match_custom_object_command(display.name.as_bytes())
      .unwrap_or_else(|| panic!("展示项 {} 未被解析面登记", display.name));
    assert_eq!(meta.name, display.name, "展示项名与解析面规范名漂移");
    assert_eq!(
      meta.arity, display.arity,
      "展示项 {} arity 与解析面漂移",
      display.name
    );
    let lower = display.name.to_ascii_lowercase();
    let (_, meta_lower) = match_custom_object_command(lower.as_bytes())
      .unwrap_or_else(|| panic!("展示项 {} 小写形未被解析面登记", display.name));
    assert_eq!(meta_lower.name, display.name, "小写命中规范名漂移");
  }
  assert_eq!(
    iterated,
    custom_command_display_count(),
    "展示迭代数与计数口漂移"
  );
}

#[test]
fn display_total_equals_crate_infos() {
  let mut expected = 0usize;
  #[cfg(feature = "roaring")]
  {
    expected += wext_roaring::COMMAND_INFOS.len();
  }
  #[cfg(feature = "json")]
  {
    expected += wext_json::COMMAND_INFOS.len();
  }
  assert_eq!(
    custom_command_display_count(),
    expected,
    "展示项总数与扩展 crate 命令清单长度和漂移"
  );
}

#[test]
fn each_entry_carries_its_own_display_slice() {
  for entry in CUSTOM_OBJECT_ENTRIES {
    assert!(
      !entry.command_display.is_empty(),
      "清单项 {} 展示子表为空",
      entry.type_name
    );
    for display in entry.command_display {
      assert!(
        entry
          .command_display
          .iter()
          .filter(|d| d.name == display.name)
          .count()
          == 1,
        "清单项 {} 展示名 {} 重复",
        entry.type_name,
        display.name
      );
    }
  }
}

#[cfg(not(any(feature = "roaring", feature = "json")))]
#[test]
fn zero_without_extension_features() {
  assert_eq!(custom_command_display_count(), 0);
  assert_eq!(custom_command_displays().count(), 0);
}
