//! ACL 规则解析器（对标 libs/server/ACL/ACLParser.cs）
//!
//! 支持的规则语法（Redis ACL 规则子集）：
//!
//! ```text
//! ACL_RULE := user <username> (<ACL_OPERATION>)+
//! ACL_OPERATION := on | off | +@<category> | -@<category>
//! ```
//!
//! 口令操作：`><明文>` / `<<明文>` / `#<哈希>` / `!<哈希>` / `nopass` /
//! `resetpass`；键模式 `~*` / `allkeys` / `resetkeys` 仅支持全通配，均为
//! 无操作。

use std::sync::Arc;

use super::{
  AclPassword, RespAclCategories, access_control_list::AccessControlList, acl_exception::AclError,
  command_catalog as catalog, user::User, user_handle::UserHandle,
};
use wresp::RespCommand;

/// 分类名对照表（对标 C# categoryNames；序即 ListCategories 的列举序）
const CATEGORY_NAMES: [(&str, RespAclCategories); 25] = [
  ("admin", RespAclCategories::ADMIN),
  ("bitmap", RespAclCategories::BITMAP),
  ("blocking", RespAclCategories::BLOCKING),
  ("connection", RespAclCategories::CONNECTION),
  ("dangerous", RespAclCategories::DANGEROUS),
  ("geo", RespAclCategories::GEO),
  ("hash", RespAclCategories::HASH),
  ("hyperloglog", RespAclCategories::HYPERLOGLOG),
  ("fast", RespAclCategories::FAST),
  ("keyspace", RespAclCategories::KEYSPACE),
  ("list", RespAclCategories::LIST),
  ("pubsub", RespAclCategories::PUBSUB),
  ("read", RespAclCategories::READ),
  ("scripting", RespAclCategories::SCRIPTING),
  ("set", RespAclCategories::SET),
  ("sortedset", RespAclCategories::SORTEDSET),
  ("slow", RespAclCategories::SLOW),
  ("stream", RespAclCategories::STREAM),
  ("string", RespAclCategories::STRING),
  ("transaction", RespAclCategories::TRANSACTION),
  ("vector", RespAclCategories::VECTOR),
  ("write", RespAclCategories::WRITE),
  ("garnet", RespAclCategories::GARNET),
  ("custom", RespAclCategories::CUSTOM),
  ("all", RespAclCategories::ALL),
];

/// ACL 解析器（全部为无状态静态方法）
pub struct AclParser;

impl AclParser {
  /// 解析单行 ACL 规则并返回应用该规则后的用户
  ///
  /// `acl` 提供时按用户名取 / 建用户并就地修改，否则创建脱离列表的新用户
  ///
  /// libs/server/ACL/ACLParser.cs:ParseACLRule
  pub fn parse_acl_rule(
    input: &str,
    acl: Option<&AccessControlList>,
  ) -> Result<Arc<User>, AclError> {
    // 词法切分（空白分隔、丢弃空 token，对标 C# Split + RemoveEmptyEntries）
    let tokens: Vec<&str> = input.split_whitespace().collect();

    // 完整性初检
    if tokens.len() < 3 {
      return Err(Self::parsing_err("Malformed ACL rule"));
    }

    // 必须以 USER 关键字开头
    if !tokens[0].eq_ignore_ascii_case("user") {
      return Err(Self::parsing_err(
        "ACL rules need to start with the USER keyword",
      ));
    }

    // 用户名
    let username = tokens[1];

    // 从访问控制列表取 / 建用户
    let user = match acl.and_then(|acl| acl.get_user_handle(username)) {
      Some(handle) => handle.user(),
      None => {
        let user = Arc::new(User::new(username.to_string()));
        if let Some(acl) = acl {
          acl.add_user_handle(Arc::new(UserHandle::new(Arc::clone(&user))))?;
        }
        user
      }
    };

    // 其余 token 全部作为 ACL 操作应用
    for op in &tokens[2..] {
      Self::apply_acl_op_to_user(&user, op)?;
    }
    Ok(user)
  }

  /// 无文件上下文的解析错误
  #[inline]
  fn parsing_err(message: &str) -> AclError {
    AclError::Parsing {
      message: message.into(),
      filename: String::new(),
      line: -1,
    }
  }

  /// 解析单个 ACL 操作并应用到用户
  ///
  /// libs/server/ACL/ACLParser.cs:ApplyACLOpToUser
  pub fn apply_acl_op_to_user(user: &User, op: &str) -> Result<(), AclError> {
    // 空操作直接跳过
    if op.is_empty() {
      return Ok(());
    }

    let first = op.as_bytes()[0];
    match first {
      // 启用账号
      _ if op.eq_ignore_ascii_case("on") => user.set_enabled(true),
      // 禁用账号
      _ if op.eq_ignore_ascii_case("off") => user.set_enabled(false),
      // 免密并清空全部口令
      _ if op.eq_ignore_ascii_case("nopass") => {
        user.clear_passwords();
        user.set_passwordless(true);
      }
      // 清全部口令与访问权并禁用
      _ if op.eq_ignore_ascii_case("reset") => user.reset(),
      // 清全部口令并关闭免密
      _ if op.eq_ignore_ascii_case("resetpass") => {
        user.clear_passwords();
        user.set_passwordless(false);
      }
      // 明文加 / 删口令
      b'>' => user.add_password_hash(AclPassword::from_string(&op[1..])),
      b'<' => user.remove_password_hash(AclPassword::from_string(&op[1..])),
      // 哈希加 / 删口令
      b'#' | b'!' => {
        let hash = AclPassword::from_hash(&op[1..]).map_err(|e| AclError::Parsing {
          message: e.to_string(),
          filename: String::new(),
          line: -1,
        })?;
        if first == b'#' {
          user.add_password_hash(hash);
        } else {
          user.remove_password_hash(hash);
        }
      }
      // 分类加减
      b'-' | b'+' if op.len() >= 2 && op.as_bytes()[1] == b'@' => {
        let category_name = &op[2..];
        let category = Self::get_acl_category_by_name(category_name)
          .ok_or_else(|| AclError::CategoryDoesNotExist(category_name.to_string()))?;
        if first == b'-' {
          user.remove_category(category)?;
        } else {
          user.add_category(category)?;
        }
      }
      // 单命令 / 命令|子命令对；未知时回落自定义命令名
      b'-' | b'+' => {
        let command_name = &op[1..];
        match Self::try_parse_command_for_acl(command_name) {
          Some(command) => {
            if first == b'-' {
              user.remove_command(command)?;
            } else {
              user.add_command(command)?;
            }
          }
          None if Self::is_valid_custom_command_name(command_name) => {
            // 模块可能尚未加载（ACL 文件先于 LoadModules 解析），先按名记录
            if first == b'-' {
              user.remove_custom_command(command_name)?;
            } else {
              user.add_custom_command(command_name)?;
            }
          }
          None => return Err(AclError::CommandDoesNotExist(command_name.to_string())),
        }
      }
      // 无操作：当前仅支持全通配键模式（若未来支持按键模式，GET 的
      // scatter-gather 快路径须按 key 复查 ACL）
      _ if op == "~*" || op.eq_ignore_ascii_case("allkeys") => {}
      // 无操作：同上
      _ if op.eq_ignore_ascii_case("resetkeys") => {}
      _ => return Err(AclError::UnknownOperation(op.to_string())),
    }
    Ok(())
  }

  /// 命令名解析为 RespCommand（含子命令折名 / 去点 / 兼容别名处理）
  ///
  /// libs/server/ACL/ACLParser.cs:TryParseCommandForAcl
  pub fn try_parse_command_for_acl(command_name: &str) -> Option<RespCommand> {
    // 首个 '|' 折为 '_'（仅首个，对标 C# IndexOf + 拼接）
    let sep_ix = command_name.find('|');
    let (effective, is_sub_command) = match sep_ix {
      Some(ix) => (
        format!("{}_{}", &command_name[..ix], &command_name[ix + 1..]),
        true,
      ),
      None => (command_name.to_string(), false),
    };

    let command = Self::lookup_command(&effective)
      // 去点重试（RI.CREATE -> RICREATE）
      .or_else(|| {
        effective.contains('.').then(|| {
          let dotless: String = effective.chars().filter(|&c| c != '.').collect();
          Self::lookup_command(&dotless)
        })?
      })
      // 兼容别名：SLAVEOF -> SECONDARYOF
      .or_else(|| {
        command_name
          .eq_ignore_ascii_case("SLAVEOF")
          .then_some(RespCommand::Secondaryof)
      })
      // 兼容别名：CLUSTER|SET-CONFIG-EPOCH -> CLUSTER_SETCONFIGEPOCH
      .or_else(|| {
        command_name
          .eq_ignore_ascii_case("CLUSTER|SET-CONFIG-EPOCH")
          .then_some(RespCommand::ClusterSetconfigepoch)
      })?;

    // 子命令解析结果须能取到与该命令一致的目录信息（info.Command == command
    // 由目录按命令键查询天然成立）
    if is_sub_command && catalog::try_get_resp_command_info(command).is_none() {
      return None;
    }
    (!Self::is_invalid_command_to_acl(command)).then_some(command)
  }

  /// 目录名对照（Enum.TryParse(ignoreCase) + 解析有效性校验）
  #[inline]
  fn lookup_command(effective_name: &str) -> Option<RespCommand> {
    let entry = catalog::try_get_by_cs_name(effective_name)?;
    Self::is_valid_parse(entry.cmd, effective_name).then_some(entry.cmd)
  }

  /// 解析值是否可能对应该输入（处理 Enum.TryParse 的怪癖：含数字即否）
  ///
  /// libs/server/ACL/ACLParser.cs:IsValidParse
  #[inline]
  pub fn is_valid_parse(command: RespCommand, from_str: &str) -> bool {
    command != RespCommand::None
      && command != RespCommand::Invalid
      && !from_str.bytes().any(|b| b.is_ascii_digit())
  }

  /// ACL 不应接受的"并非真命令"枚举值（实现细节或哨兵）
  ///
  /// libs/server/ACL/ACLParser.cs:IsInvalidCommandToAcl
  #[inline]
  pub fn is_invalid_command_to_acl(command: RespCommand) -> bool {
    command == RespCommand::Invalid
      || command == RespCommand::None
      || catalog::normalize_for_acls(command) != command
  }

  /// 自定义命令名的语法合法性：首字符字母数字，其余允许 `. _ - |`
  ///
  /// 严格校验防止未知名回落误收 RESP 元字节或含空白的垃圾串
  ///
  /// libs/server/ACL/ACLParser.cs:IsValidCustomCommandName
  pub fn is_valid_custom_command_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    match bytes.split_first() {
      None => false,
      Some((&first, rest)) => {
        first.is_ascii_alphanumeric()
          && rest
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'|'))
      }
    }
  }

  /// 按名查分类（大小写不敏感；未知名返回 None，对标 C# 键不存在）
  ///
  /// libs/server/ACL/ACLParser.cs:GetACLCategoryByName
  pub fn get_acl_category_by_name(category_name: &str) -> Option<RespAclCategories> {
    CATEGORY_NAMES
      .iter()
      .find(|e| e.0.eq_ignore_ascii_case(category_name))
      .map(|&(_, cat)| cat)
  }

  /// 分类位的名称（未知组合位返回 "unknown"，对标 C# 反查缺项异常路径）
  ///
  /// libs/server/ACL/ACLParser.cs:GetNameByACLCategory
  pub fn get_name_by_acl_category(category: RespAclCategories) -> &'static str {
    CATEGORY_NAMES
      .iter()
      .find(|e| e.1 == category)
      .map_or("unknown", |&(name, _)| name)
  }

  /// 全部合法分类名
  ///
  /// libs/server/ACL/ACLParser.cs:ListCategories
  pub fn list_categories() -> &'static [&'static str] {
    // 与 CATEGORY_NAMES 平行的静态名表（免每次分配）
    const NAMES: [&str; CATEGORY_NAMES.len()] = [
      "admin",
      "bitmap",
      "blocking",
      "connection",
      "dangerous",
      "geo",
      "hash",
      "hyperloglog",
      "fast",
      "keyspace",
      "list",
      "pubsub",
      "read",
      "scripting",
      "set",
      "sortedset",
      "slow",
      "stream",
      "string",
      "transaction",
      "vector",
      "write",
      "garnet",
      "custom",
      "all",
    ];
    &NAMES
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::access_control_list::AccessControlList;

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
      let user = AclParser::parse_acl_rule(rule, None).unwrap_or_else(|e| panic!("{rule}: {e}"));
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
      let user = AclParser::parse_acl_rule(rule, None).unwrap_or_else(|e| panic!("{rule}: {e}"));
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
      AclParser::parse_acl_rule("user x", None),
      Err(AclError::Parsing { .. })
    ));
    // 不以 USER 开头
    assert!(matches!(
      AclParser::parse_acl_rule("usr x on", None),
      Err(AclError::Parsing { .. })
    ));
    // 未知操作
    assert!(matches!(
      AclParser::parse_acl_rule("user x on whatsthis", None),
      Err(AclError::UnknownOperation(op)) if op == "whatsthis"
    ));
    // 未知分类
    assert!(matches!(
      AclParser::parse_acl_rule("user x on +@nosuch", None),
      Err(AclError::CategoryDoesNotExist(c)) if c == "nosuch"
    ));
    // 未知命令且非法自定义名（'!' 不在自定义名字符集内）
    assert!(matches!(
      AclParser::parse_acl_rule("user x on +bad!name", None),
      Err(AclError::CommandDoesNotExist(c)) if c == "bad!name"
    ));
  }

  /// 口令操作语义
  #[test]
  fn parse_acl_rule_password_ops() {
    const HASH: &str = "8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9";
    let user = AclParser::parse_acl_rule(&format!("user x on >passw0rd #{HASH}"), None).unwrap();
    // 两类口令（同哈希去重成一个）
    assert!(user.validate_password(&AclPassword::from_string("passw0rd")));

    // < 明文删除
    let user = AclParser::parse_acl_rule("user x on >passw0rd <passw0rd", None).unwrap();
    assert!(!user.validate_password(&AclPassword::from_string("passw0rd")));

    // ! 哈希删除
    let user = AclParser::parse_acl_rule(&format!("user x on #{HASH} !{HASH}"), None).unwrap();
    assert!(!user.validate_password(&AclPassword::from_string("passw0rd")));

    // 非法哈希长度 → 解析错误
    assert!(matches!(
      AclParser::parse_acl_rule("user x on #deadbeef", None),
      Err(AclError::Parsing { .. })
    ));

    // nopass：清口令且任意口令可过
    let user = AclParser::parse_acl_rule("user x on >p nopass", None).unwrap();
    assert!(user.validate_password(&AclPassword::from_string("anything")));

    // resetpass：清口令且关闭免密
    let user = AclParser::parse_acl_rule("user x on nopass resetpass", None).unwrap();
    assert!(!user.validate_password(&AclPassword::from_string("anything")));
  }

  /// 启停 / 重置 / 键模式无操作语义
  #[test]
  fn parse_acl_rule_flag_ops() {
    let user = AclParser::parse_acl_rule("user x on +set ~* resetkeys", None).unwrap();
    assert!(user.is_enabled());
    assert!(user.can_access_command(RespCommand::Set));

    let user = AclParser::parse_acl_rule("user x on off", None).unwrap();
    assert!(!user.is_enabled());

    // reset：清权限 + 禁用
    let user = AclParser::parse_acl_rule("user x on +set >p reset", None).unwrap();
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

  /// 携带 ACL 的解析会就地修改既有用户 / 新建并入表
  #[test]
  fn parse_acl_rule_with_acl_mutates_list() {
    let acl = AccessControlList::new("", None).unwrap();
    AclParser::parse_acl_rule("user alice on +set", Some(&acl)).unwrap();
    let handle = acl.get_user_handle("alice").expect("alice added");
    assert!(handle.user().can_access_command(RespCommand::Set));

    // 再次解析同名规则：就地修改既有用户
    AclParser::parse_acl_rule("user alice on +get", Some(&acl)).unwrap();
    assert!(
      acl
        .get_user_handle("alice")
        .unwrap()
        .user()
        .can_access_command(RespCommand::Get)
    );
    // 原有权限仍在
    assert!(
      acl
        .get_user_handle("alice")
        .unwrap()
        .user()
        .can_access_command(RespCommand::Set)
    );
  }
}
