#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wacl::CommandPermissionSet;
use wresp::command::{LAST_VALID_COMMAND, RespCommand};

#[test]
fn sentinels_and_copy() {
  let none = CommandPermissionSet::none();
  assert!(none.is_none());
  assert!(!none.can_run_command(RespCommand::Get));
  assert_eq!(none.description, "");

  let all = CommandPermissionSet::all();
  assert!(all.is_all());
  assert!(all.can_run_command(RespCommand::Get));

  // All 物化拷贝：全一，不再是哨兵，但与 All 等价性按身份判定
  let materialized = all.copy();
  assert!(!materialized.is_all());
  assert!(materialized.can_run_command(RespCommand::Get));
  assert!(!materialized.is_equivalent_to(&all));
  assert!(all.is_equivalent_to(&CommandPermissionSet::all()));

  // None 拷贝后与空集等价（对标 C# 位图比较路径）
  let empty = none.copy();
  assert!(empty.is_equivalent_to(&CommandPermissionSet::none()));
  // C# 怪癖：All 实例底座位图全零，空位图与之比较经位图路径返回 true
  assert!(empty.is_equivalent_to(&all));
}

#[test]
fn add_remove_command_with_expansion() {
  let mut set = CommandPermissionSet::none().copy();
  set.add_command(RespCommand::Set);
  // SET 置位同时展开 SETEXNX / SETEXXX / SETKEEPTTL / SETKEEPTTLXX
  for cmd in [
    RespCommand::Set,
    RespCommand::Setexnx,
    RespCommand::Setexxx,
    RespCommand::Setkeepttl,
    RespCommand::Setkeepttlxx,
  ] {
    assert!(set.can_run_command(cmd));
  }
  set.remove_command(RespCommand::Set);
  assert!(!set.can_run_command(RespCommand::Set));
  assert!(!set.can_run_command(RespCommand::Setkeepttl));
}

#[test]
fn no_auth_commands_cannot_be_removed() {
  let mut set = CommandPermissionSet::all().copy();
  set.remove_command(RespCommand::Auth);
  set.remove_command(RespCommand::Hello);
  set.remove_command(RespCommand::Quit);
  for cmd in [RespCommand::Auth, RespCommand::Hello, RespCommand::Quit] {
    assert!(set.can_run_command(cmd));
  }
}

#[test]
fn custom_command_deny_precedence() {
  let mut set = CommandPermissionSet::none().copy();
  assert!(!set.can_run_custom_command(RespCommand::Customobjcmd, "json.set"));

  set.add_custom_command("JSON.SET");
  assert!(set.can_run_custom_command(RespCommand::Customobjcmd, "json.set"));
  assert!(set.can_run_custom_command(RespCommand::Customobjcmd, "JSON.SET"));

  // 后写胜出：再拒绝后，拒绝优先
  set.remove_custom_command("json.set");
  assert!(!set.can_run_custom_command(RespCommand::Customobjcmd, "json.set"));

  // 泛型位兜底（+CustomObjCmd 置位后按名未列也放行）
  set.add_command(RespCommand::Customobjcmd);
  assert!(set.can_run_custom_command(RespCommand::Customobjcmd, "other.cmd"));
}

#[test]
fn command_list_length_covers_all_commands() {
  // QUIT = 367 为最大有效命令，SUNSUBSCRIBE = 370 为扩展最大值 → 需 371 位 → 6 个 u64
  let len = CommandPermissionSet::get_command_list_length();
  assert_eq!(len, 6);
  assert!(len * 64 > LAST_VALID_COMMAND as u16 as usize);
}
