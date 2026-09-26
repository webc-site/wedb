//! ACL 规则解析器纯规则面锁测（自 src/acl_parser.rs 内联 tests 迁出）
//!
//! 对标 garnet AclParserTests：规则 → 描述折叠、错误形态、口令操作、
//! 命令名解析特例与分类互查。

use wacl::{AclError, AclParser, AclPassword};
use wresp::{catalog::RespAclCategories, command::RespCommand};

/// 对标 garnet AclParserTests.ParseACLRuleDescriptionTest（规则 → 期望描述）
#[test]
fn parse_acl_rule_description() {
  // (规则, 期望描述)
  const CASES: &[(&str, &str)] = &[
    ("user 1-command on +set", "+set"),
    ("user 2-command on +set +get", "+set +get"),
    ("user 3-command-duplicates-reduce on +set +set", "+set"),
    (
      "user 4-command-duplicates-complicated on +set +set -set +set",
      "+set",
    ),
    (
      "user 5-command-duplicates-complicated on +get -set +set",
      "+get +set",
    ),
    ("user 6-category on +@keyspace", "+@keyspace"),
    ("user 7-category-reduces on +@all", "+@all"),
    ("user 7-category-reduces on -@all", ""),
    ("user 8-category-reduces on -@all +@keyspace", "+@keyspace"),
    ("user 9-category-reduces on +@all +@keyspace", "+@all"),
    (
      "user 10-category-command-reduces on +@keyspace +del",
      "+@keyspace",
    ),
    (
      "user 11-category-command-reduces on +@keyspace +set",
      "+@keyspace +set",
    ),
    (
      "user 12-category-command-reduces on +@keyspace +del -del",
      "+@keyspace -del",
    ),
    ("user 13-category-command-reduces on +del -@keyspace", ""),
    (
      "user 14-category-command-reduces on -del +@keyspace",
      "+@keyspace",
    ),
    (
      "user 15-category-command-reduces on +set +@keyspace",
      "+set +@keyspace",
    ),
    ("user 16-category-command-reduces on +@all +set", "+@all"),
    (
      "user 17-category-command-reduces on +@all +set +get +incr -decr",
      "+@all -decr",
    ),
    ("user 18-category-command-reduces on -@all +set", "+set"),
    (
      "user 19-category-command-reduces on -@all +set +get",
      "+set +get",
    ),
    (
      "user 20-category-command-reduces on -@all +set +get +incr +decr +incrby +decrby",
      "+set +get +incr +decr +incrby +decrby",
    ),
    (
      "user 21-category-command-reduces on -@all +ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget",
      "+ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget",
    ),
    (
      "user 22-category-command-reduces on -@all +ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget +config|get",
      "+ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget +config|get",
    ),
    (
      "user 23-category-command-reduces on -@all +set +get +incr +decr +@keyspace +@hash +incrby +decrby",
      "+set +get +incr +decr +@keyspace +@hash +incrby +decrby",
    ),
    (
      "user 24-multi-category-reduces on -@all +@keyspace +@hash",
      "+@keyspace +@hash",
    ),
    (
      "user 25-multi-category-reduces on -@all +@keyspace +@hash -flushdb",
      "+@keyspace +@hash -flushdb",
    ),
    (
      "user 26-multi-category-reduces on -@all +@keyspace -flushdb +@hash -flushdb",
      "+@keyspace -flushdb +@hash",
    ),
    (
      "user 27-multi-category-reduces on -@all +set +get +incr +decr +@keyspace +@hash +incrby +decrby +script|exists +@pubsub +expire +ttl",
      "+set +get +incr +decr +@keyspace +@hash +incrby +decrby +script|exists +@pubsub",
    ),
    ("user 28-command-reversed-duplicates on -set +set", "+set"),
  ];
  for &(rule, expected) in CASES {
    let user = AclParser::parse_acl_rule(rule).unwrap_or_else(|e| panic!("{rule}: {e}"));
    assert_eq!(
      user.get_enabled_commands_description(),
      expected,
      "rule: {rule}"
    );
  }
}

/// 对标 garnet AclParserTests.ParseACLRuleDescriptionTimeoutsTest
#[test]
fn parse_acl_rule_description_timeouts() {
  const CASES: &[(&str, &str)] = &[
    (
      "user 1-command-notimeout on +auth +ping +get +set +del +exists +incr +decr +mget +mset +expire +ttl +keys +scan +hget +hset +lpush +rpush +sadd +decrby",
      "+auth +ping +get +set +del +exists +incr +decr +mget +mset +expire +ttl +keys +scan +hget +hset +lpush +rpush +sadd +decrby",
    ),
    (
      "user 2-category-command-notimeout on -@all +ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget +mget +mset +eval +evalsha +setex",
      "+ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget +mget +mset +eval +evalsha +setex",
    ),
    (
      "user 3-category-command-notimeout on -@all +client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +del +expire +flushdb +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex +unlink",
      "+client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +del +expire +flushdb +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex +unlink",
    ),
    (
      "user 4-category-command-notimeout on +@keyspace +client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +del +expire +flushdb +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex +unlink",
      "+@keyspace +client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex",
    ),
    (
      "user 5-category-command-notimeout on -@all +@keyspace +client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +del +expire +flushdb +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex +unlink",
      "+@keyspace +client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex",
    ),
  ];
  for &(rule, expected) in CASES {
    let user = AclParser::parse_acl_rule(rule).unwrap_or_else(|e| panic!("{rule}: {e}"));
    assert_eq!(
      user.get_enabled_commands_description(),
      expected,
      "rule: {rule}"
    );
  }
}

/// 规则头与参数校验错误
#[test]
fn parse_acl_rule_malformed() {
  // 少于 3 个 token
  assert!(matches!(
    AclParser::parse_acl_rule("user x"),
    Err(AclError::Parsing { .. })
  ));
  // 不以 USER 开头
  assert!(matches!(
    AclParser::parse_acl_rule("usr x on"),
    Err(AclError::Parsing { .. })
  ));
  // 未知操作
  assert!(matches!(
    AclParser::parse_acl_rule("user x on whatsthis"),
    Err(AclError::UnknownOperation(op)) if op == "whatsthis"
  ));
  // 未知分类
  assert!(matches!(
    AclParser::parse_acl_rule("user x on +@nosuch"),
    Err(AclError::CategoryDoesNotExist(c)) if c == "nosuch"
  ));
  // 未知命令且非法自定义名（'!' 不在自定义名字符集内）
  assert!(matches!(
    AclParser::parse_acl_rule("user x on +bad!name"),
    Err(AclError::CommandDoesNotExist(c)) if c == "bad!name"
  ));
}

/// 口令操作语义
#[test]
fn parse_acl_rule_password_ops() {
  const HASH: &str = "8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9";
  let user = AclParser::parse_acl_rule(&format!("user x on >passw0rd #{HASH}")).unwrap();
  // 两类口令（同哈希去重成一个）
  assert!(user.validate_password(&AclPassword::from_string("passw0rd")));

  // < 明文删除
  let user = AclParser::parse_acl_rule("user x on >passw0rd <passw0rd").unwrap();
  assert!(!user.validate_password(&AclPassword::from_string("passw0rd")));

  // ! 哈希删除
  let user = AclParser::parse_acl_rule(&format!("user x on #{HASH} !{HASH}")).unwrap();
  assert!(!user.validate_password(&AclPassword::from_string("passw0rd")));

  // 非法哈希长度 → 解析错误
  assert!(matches!(
    AclParser::parse_acl_rule("user x on #deadbeef"),
    Err(AclError::Parsing { .. })
  ));

  // nopass：清口令且任意口令可过
  let user = AclParser::parse_acl_rule("user x on >p nopass").unwrap();
  assert!(user.validate_password(&AclPassword::from_string("anything")));

  // resetpass：清口令且关闭免密
  let user = AclParser::parse_acl_rule("user x on nopass resetpass").unwrap();
  assert!(!user.validate_password(&AclPassword::from_string("anything")));
}

/// 启停 / 重置 / 键模式无操作语义
#[test]
fn parse_acl_rule_flag_ops() {
  let user = AclParser::parse_acl_rule("user x on +set ~* resetkeys").unwrap();
  assert!(user.is_enabled());
  assert!(user.can_access_command(RespCommand::Set));

  let user = AclParser::parse_acl_rule("user x on off").unwrap();
  assert!(!user.is_enabled());

  // reset：清权限 + 禁用
  let user = AclParser::parse_acl_rule("user x on +set >p reset").unwrap();
  assert!(!user.is_enabled());
  assert!(!user.can_access_command(RespCommand::Set));
  assert!(!user.validate_password(&AclPassword::from_string("p")));
}

/// 命令名解析的特例（子命令 / 去点 / 别名 / 实现细节值）
#[test]
fn try_parse_command_for_acl_cases() {
  // 大小写不敏感
  assert_eq!(
    AclParser::try_parse_command_for_acl("GET"),
    Some(RespCommand::Get)
  );
  // 子命令折名
  assert_eq!(
    AclParser::try_parse_command_for_acl("client|getname"),
    Some(RespCommand::ClientGetname)
  );
  // 仅首个 '|' 折名：第二个 '|' 残留无法解析
  assert_eq!(AclParser::try_parse_command_for_acl("a|b|c"), None);
  // 去点重试
  assert_eq!(
    AclParser::try_parse_command_for_acl("ri.create"),
    Some(RespCommand::Ricreate)
  );
  // 别名
  assert_eq!(
    AclParser::try_parse_command_for_acl("slaveof"),
    Some(RespCommand::Secondaryof)
  );
  assert_eq!(
    AclParser::try_parse_command_for_acl("cluster|set-config-epoch"),
    Some(RespCommand::ClusterSetconfigepoch)
  );
  // rust 自增命令 SUNSUBSCRIBE：入命令目录后按名解析才有目录条目（C# 无对位）
  assert_eq!(
    AclParser::try_parse_command_for_acl("sunsubscribe"),
    Some(RespCommand::Sunsubscribe)
  );
  // 实现细节值不可 ACL（SETEXNX 归一化为 SET）
  assert_eq!(AclParser::try_parse_command_for_acl("setexnx"), None);
  // 含数字的名字被拒（Enum.TryParse 怪癖防护）
  assert_eq!(AclParser::try_parse_command_for_acl("get123"), None);
  // 未知名
  assert_eq!(AclParser::try_parse_command_for_acl("nosuchcmd"), None);
}

/// 自定义命令名合法性
#[test]
fn is_valid_custom_command_name_cases() {
  assert!(AclParser::is_valid_custom_command_name("json.set"));
  assert!(AclParser::is_valid_custom_command_name("JSON|SET"));
  assert!(AclParser::is_valid_custom_command_name("a-b_c"));
  // 数字开头合法（C# LegalFirstChars 含数字）
  assert!(AclParser::is_valid_custom_command_name("1abc"));
  // 空 / 非法字符
  assert!(!AclParser::is_valid_custom_command_name(""));
  assert!(!AclParser::is_valid_custom_command_name("bad name"));
  assert!(!AclParser::is_valid_custom_command_name("bad!name"));
}

/// 分类名 ↔ 位互查
#[test]
fn category_lookup() {
  assert_eq!(
    AclParser::get_acl_category_by_name("KEYSPACE"),
    Some(RespAclCategories::KEYSPACE)
  );
  assert_eq!(
    AclParser::get_acl_category_by_name("all"),
    Some(RespAclCategories::ALL)
  );
  assert_eq!(AclParser::get_acl_category_by_name("nosuch"), None);
  assert_eq!(
    AclParser::get_name_by_acl_category(RespAclCategories::HYPERLOGLOG),
    "hyperloglog"
  );
  assert_eq!(
    AclParser::get_name_by_acl_category(RespAclCategories::ALL),
    "all"
  );
  // 全部分类名
  let names = AclParser::list_categories();
  assert_eq!(names.len(), 25);
  assert!(names.contains(&"admin"));
  assert!(names.contains(&"all"));
}
