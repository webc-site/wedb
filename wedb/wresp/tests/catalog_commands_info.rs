#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wbase::store_type::StoreType;
use wresp::{
  catalog::{
    RespAclCategories, RespCommandFlags, commands_for_category,
    commands_info::acl_category_descriptions, get_resp_command_name,
    try_get_resp_command_info_by_cmd, try_get_resp_command_info_by_name,
    try_get_resp_commands_info, try_get_resp_commands_info_count, try_get_simple_resp_command_info,
    try_initialize,
  },
  command::{LAST_VALID_COMMAND, RespCommand},
  resp_memory_writer::{Resp2, Resp3, RespWriter},
};

/// 表初始化 + 基本检索（COMMAND 表快照的入口断言）
#[test]
fn tables_initialize_and_lookup() {
  assert!(try_initialize());

  let all = try_get_resp_commands_info(false).unwrap();
  let external = try_get_resp_commands_info(true).unwrap();
  assert_eq!(all.len(), 259, "根命令数快照");
  assert_eq!(external.len(), 258, "外部根命令数快照");
  assert_eq!(try_get_resp_commands_info_count(false), Some(all.len()));
  assert_eq!(try_get_resp_commands_info_count(true), Some(external.len()));

  let sunsub = try_get_resp_command_info_by_name("SUNSUBSCRIBE", false, false).unwrap();
  assert_eq!(sunsub.command, RespCommand::Sunsubscribe);
  assert_eq!(sunsub.arity, -1);
  assert!(sunsub.acl_categories.contains(RespAclCategories::PUBSUB));

  let get = try_get_resp_command_info_by_name("GET", false, false).unwrap();
  assert_eq!(get.arity, 2);
  assert_eq!(get.first_key, 1);
  assert_eq!(get.last_key, 1);
  assert_eq!(get.step, 1);
  assert_eq!(get.store_type, StoreType::Main);
  assert_eq!(
    get.flags,
    RespCommandFlags::from_member_names("Fast, ReadOnly").unwrap()
  );

  let acl_cat = try_get_resp_command_info_by_name("ACL|CAT", false, true).unwrap();
  assert_eq!(acl_cat.command, RespCommand::AclCat);
  assert!(
    try_get_resp_command_info_by_name("ACL|CAT", false, false).is_none(),
    "不带子命令检索时不可达"
  );

  let set = try_get_resp_command_info_by_cmd(RespCommand::Set, false).unwrap();
  assert_eq!(set.name, "SET");
  assert!(try_get_resp_command_info_by_cmd(RespCommand::Async, true).is_none());
  assert!(try_get_resp_command_info_by_cmd(RespCommand::Async, false).is_some());
  assert!(try_get_resp_command_info_by_cmd(RespCommand::Secondaryof, false).is_some());
  assert!(try_get_resp_command_info_by_cmd(RespCommand::Replicaof, false).is_some());

  assert_eq!(get_resp_command_name(RespCommand::Bitcount), "BITCOUNT");
  assert_eq!(get_resp_command_name(RespCommand::Invalid), "UNKNOWN");

  let simple = try_get_simple_resp_command_info(RespCommand::Get).unwrap();
  assert!(simple.allowed_in_txn);
  assert_eq!(simple.arity, 2);
}

#[test]
fn acl_category_index() {
  let bitmap: Vec<&str> = commands_for_category(RespAclCategories::BITMAP)
    .map(|c| c.name)
    .collect();
  assert!(bitmap.contains(&"setbit"));
  assert!(bitmap.contains(&"getbit"));
  assert!(bitmap.contains(&"bitcount"));
  assert!(bitmap.contains(&"bitpos"));
  assert!(bitmap.contains(&"bitfield"));
  assert!(bitmap.contains(&"bitfield_ro"));
  assert!(bitmap.contains(&"bitop"));
}

/// COMMAND INFO 快照：GET（RESP3）逐字节对标 C# ToRespFormat 输出
#[test]
fn command_info_resp3_snapshot_get() {
  let get = try_get_resp_command_info_by_cmd(RespCommand::Get, false).unwrap();
  let mut w = RespWriter::<Vec<u8>, Resp3>::new();
  get.to_resp_format(&mut w);
  let expected = concat!(
    "*10\r\n$3\r\nGET\r\n:2\r\n~2\r\n+fast\r\n+readonly\r\n:1\r\n:1\r\n:1\r\n",
    "~3\r\n+@fast\r\n+@read\r\n+@string\r\n~0\r\n~1\r\n%3\r\n$5\r\nflags\r\n~2\r\n+RO\r\n+access\r\n",
    "$12\r\nbegin_search\r\n%2\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n%1\r\n$5\r\nindex\r\n:1\r\n",
    "$9\r\nfind_keys\r\n%2\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n%3\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n",
  );
  assert_eq!(String::from_utf8(w.into_inner()).unwrap(), expected);
}

/// COMMAND INFO 快照：BITFIELD（RESP3，含 notes 与四重键标记）
#[test]
fn command_info_resp3_snapshot_bitfield() {
  let bf = try_get_resp_command_info_by_cmd(RespCommand::Bitfield, false).unwrap();
  let mut w = RespWriter::<Vec<u8>, Resp3>::new();
  bf.to_resp_format(&mut w);
  let expected = concat!(
    "*10\r\n$8\r\nBITFIELD\r\n:-2\r\n~2\r\n+denyoom\r\n+write\r\n:1\r\n:1\r\n:1\r\n",
    "~3\r\n+@bitmap\r\n+@slow\r\n+@write\r\n~0\r\n~1\r\n%4\r\n$5\r\nnotes\r\n$59\r\nThis command allows both access and modification of the key\r\n",
    "$5\r\nflags\r\n~4\r\n+RW\r\n+access\r\n+update\r\n+variable_flags\r\n",
    "$12\r\nbegin_search\r\n%2\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n%1\r\n$5\r\nindex\r\n:1\r\n",
    "$9\r\nfind_keys\r\n%2\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n%3\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n",
  );
  assert_eq!(String::from_utf8(w.into_inner()).unwrap(), expected);
}

/// COMMAND INFO 快照：SETBIT（RESP2 降级面）
#[test]
fn command_info_resp2_snapshot_setbit() {
  let setbit = try_get_resp_command_info_by_cmd(RespCommand::Setbit, false).unwrap();
  let mut w = RespWriter::<Vec<u8>, Resp2>::new();
  setbit.to_resp_format(&mut w);
  let expected = concat!(
    "*10\r\n$6\r\nSETBIT\r\n:4\r\n*2\r\n+denyoom\r\n+write\r\n:1\r\n:1\r\n:1\r\n",
    "*3\r\n+@bitmap\r\n+@slow\r\n+@write\r\n*0\r\n*1\r\n*6\r\n$5\r\nflags\r\n*3\r\n+RW\r\n+access\r\n+update\r\n",
    "$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n*2\r\n$5\r\nindex\r\n:1\r\n",
    "$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n*6\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n",
  );
  assert_eq!(String::from_utf8(w.into_inner()).unwrap(), expected);
}

/// 常量与辅助面锚定
#[test]
fn constants_and_helpers() {
  let cats = RespAclCategories::from_member_names("Fast, Read").unwrap();
  assert_eq!(acl_category_descriptions(cats), vec!["fast", "read"]);
  let tables_ok = try_get_simple_resp_command_info(LAST_VALID_COMMAND).is_some();
  assert!(tables_ok);
}
