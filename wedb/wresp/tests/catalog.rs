#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wresp::{
  catalog::{
    CmdEntry, RespAclCategories, children_of, commands_for_category, entries,
    try_get_resp_command_info, try_initialize,
  },
  command::RespCommand,
};

/// 目录全量锚定：259 根 + 96 子 = 355 条（含 1 条内部根命令与两条
/// SECONDARYOF 历史别名条目，逐条对照内嵌 JSON；相对 C# 356 条：删
/// module/module|loadcs/registercs 三条模块加载命令、删 customtxn/
/// customrawstringcmd/customprocedure 三条占位内部命令、增
/// cluster|flushall_ns 与 ri.count 与 sunsubscribe 与 pubsub|shardchannels
/// 与 pubsub|shardnumsub 五条 rust 扩展）
#[test]
fn catalog_size_and_aliases() {
  assert!(try_initialize());
  assert_eq!(entries().len(), 355);
  assert_eq!(entries().iter().filter(|e| e.parent.is_none()).count(), 259);
  assert_eq!(entries().iter().filter(|e| e.parent.is_some()).count(), 96);

  // 历史别名：SECONDARYOF 枚举承载 SECONDARYOF 与 SLAVEOF 两个展示名
  let aliases: Vec<&CmdEntry> = entries()
    .iter()
    .filter(|e| e.cmd == RespCommand::Secondaryof)
    .collect();
  assert_eq!(aliases.len(), 2);
  assert!(aliases.iter().any(|e| e.name == "secondaryof"));
  assert!(aliases.iter().any(|e| e.name == "slaveof"));

  // 扁平序即 JSON 声明序（先序：根紧随其子命令）
  let head: Vec<&str> = entries().iter().take(3).map(|e| e.name).collect();
  assert_eq!(head, ["acl", "acl|cat", "acl|deluser"]);
}

/// ACL 消费面抽查（分类 / 父子关系 / 大小写不敏感枚举名对照）
#[test]
fn acl_lookup() {
  let acl = try_get_resp_command_info(RespCommand::Acl).unwrap();
  assert_eq!(acl.cs, "ACL");
  assert_eq!(acl.name, "acl");
  assert_eq!(acl.cats, RespAclCategories::SLOW);
  assert_eq!(acl.parent, None);

  // 枚举名对照单源锁：cs 投影与 RespCommand::from_cs_name 互逆
  // （Enum.TryParse(ignoreCase) 的 rust 对位在此，目录不设反查臂）
  let cat = try_get_resp_command_info(RespCommand::AclCat).unwrap();
  assert_eq!(cat.cs, "ACL_CAT");
  assert_eq!(RespCommand::from_cs_name("acl_cat"), Some(cat.cmd));
  assert_eq!(cat.parent, Some(RespCommand::Acl));
  assert_eq!(cat.cats, RespAclCategories::SLOW);

  // DELUSER = Admin + Dangerous + Slow
  let deluser = try_get_resp_command_info(RespCommand::AclDeluser).unwrap();
  assert_eq!(deluser.cs, "ACL_DELUSER");
  assert_eq!(
    deluser.cats,
    RespAclCategories::ADMIN | RespAclCategories::DANGEROUS | RespAclCategories::SLOW
  );

  // 根命令的全部子命令（10 个）
  assert_eq!(children_of(RespCommand::Acl).count(), 10);

  // rust 自增命令 SUNSUBSCRIBE（位 370，C# 目录无对位）：入册后 ACL 侧
  // 才有按名与按 +@pubsub 类别的置位入口
  let sunsub = try_get_resp_command_info(RespCommand::Sunsubscribe).unwrap();
  assert_eq!(sunsub.cs, "SUNSUBSCRIBE");
  assert_eq!(sunsub.name, "sunsubscribe");
  assert_eq!(sunsub.cmd, RespCommand::Sunsubscribe);
  assert_eq!(sunsub.parent, None);
  assert!(sunsub.cats.contains(RespAclCategories::PUBSUB));

  // rust 自增分片域查询族（位 371/372，C# 目录无对位）：同口径入册锚定
  for (cmd, cs, name) in [
    (
      RespCommand::PubsubShardchannels,
      "PUBSUB_SHARDCHANNELS",
      "pubsub|shardchannels",
    ),
    (
      RespCommand::PubsubShardnumsub,
      "PUBSUB_SHARDNUMSUB",
      "pubsub|shardnumsub",
    ),
  ] {
    let e = try_get_resp_command_info(cmd).unwrap();
    assert_eq!(e.cs, cs);
    assert_eq!(e.name, name);
    assert_eq!(e.cmd, cmd);
    assert_eq!(e.parent, Some(RespCommand::Pubsub));
    assert!(e.cats.contains(RespAclCategories::PUBSUB));
  }

  // 未知名（非枚举成员名，strum 单源判定即拒）
  assert_eq!(RespCommand::from_cs_name("NO_SUCH_COMMAND"), None);
}

/// 分类 → 命令（C# TryGetCommandsforAclCategory 单类别位语义）
#[test]
fn category_members() {
  let bitmap: Vec<&CmdEntry> = commands_for_category(RespAclCategories::BITMAP).collect();
  assert!(bitmap.iter().any(|e| e.name == "setbit"));
  // 全类别覆盖全部条目
  assert_eq!(
    commands_for_category(RespAclCategories::ALL).count(),
    entries().len()
  );
}

/// 成员名串解析（C# JsonStringEnumConverter 语义）
#[test]
fn member_names_parse() {
  let cats = RespAclCategories::from_member_names("Fast, String, Write").unwrap();
  assert_eq!(
    cats,
    RespAclCategories::FAST | RespAclCategories::STRING | RespAclCategories::WRITE
  );
  assert!(RespAclCategories::from_member_names("Nope").is_none());
}
