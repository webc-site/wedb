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
//!
//! 在 garnet 中的相对路径: libs/server/ACL/AclParser.cs? 对标 C# ACL 规则解析器

use std::{borrow::Cow, sync::Arc};

use wresp::{
  catalog::{self, RespAclCategories},
  command::RespCommand,
};

use super::{
  AclPassword,
  acl_exception::AclError,
  user::{User, parse_user_namespace, validate_username},
};

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

/// 与 CATEGORY_NAMES 平行的静态名表（编译期单点派生，免每次分配并杜绝名表分裂）
const NAMES: [&str; CATEGORY_NAMES.len()] = {
  let mut arr = [""; CATEGORY_NAMES.len()];
  let mut i = 0;
  while i < CATEGORY_NAMES.len() {
    arr[i] = CATEGORY_NAMES[i].0;
    i += 1;
  }
  arr
};

/// ACL 解析器（全部为无状态静态方法）
pub struct AclParser;

/// 用户加减方法对（`[减臂, 加臂]`，元素序即方向布尔值，供 [`dir`] 索引派发）
type AddRm<A, R = ()> = [fn(&mut User, A) -> R; 2];

/// 口令 / 分类 / 命令三对加减方法的派发槽（自定义名臂含借用生命周期，
/// 无法入 const，在调用处以同形局部数组选取）
const PW: AddRm<AclPassword> = [User::remove_password_hash, User::add_password_hash];
const CAT: AddRm<RespAclCategories, Result<(), AclError>> =
  [User::remove_category, User::add_category];
const CMD: AddRm<RespCommand, Result<(), AclError>> = [User::remove_command, User::add_command];

/// 方向分派单源：`add` 真取加臂、假取减臂（与 user.rs 的 apply_* 方向参数化同形态）
#[inline]
fn dir<A, R>(user: &mut User, add: bool, arg: A, ops: AddRm<A, R>) -> R {
  ops[usize::from(add)](user, arg)
}

impl AclParser {
  /// 解析单行 ACL 规则并返回脱离列表的新用户（纯解析，不落存储）
  ///
  /// rust 侧已无 acl 字典入参（对标 C# ParseACLRule 的 `acl = null` 分支）：
  /// 用户表写入面由 `KeyTag::Acl` 存储记录读改写承接
  /// （wedb/wnode/src/resp/acl_commands.rs:apply_set_user）
  ///
  /// libs/server/ACL/ACLParser.cs:ParseACLRule
  pub fn parse_acl_rule(input: &str) -> Result<Arc<User>, AclError> {
    let mut tokens = input.split_whitespace();
    let (Some(first), Some(raw_username), Some(op1)) =
      (tokens.next(), tokens.next(), tokens.next())
    else {
      return Err(Self::parsing_err("Malformed ACL rule"));
    };

    if !first.eq_ignore_ascii_case("user") {
      return Err(Self::parsing_err(
        "ACL rules need to start with the USER keyword",
      ));
    }

    // `<ns>#<name>` 形态校验命名空间段；写存储的目标命名空间由调用方路由
    let username = if raw_username.contains('#') {
      parse_user_namespace(raw_username)?.0
    } else {
      validate_username(raw_username)?;
      raw_username
    };

    // 规则在独占可变的构造体上逐条落定，全部应用完才升为 Arc 只读共享
    let mut user = User::new(username.to_string());
    Self::apply_acl_op_to_user(&mut user, op1)?;
    for op in tokens {
      Self::apply_acl_op_to_user(&mut user, op)?;
    }
    Ok(Arc::new(user))
  }

  /// 无文件上下文的解析错误
  #[inline]
  fn parsing_err(message: impl Into<String>) -> AclError {
    AclError::Parsing {
      message: message.into(),
      filename: String::new(),
      line: -1,
    }
  }

  /// 解析单个 ACL 操作并应用到用户（独占可变：一条规则的改权就地落定，
  /// 无 C# 共享 User 形态下的原子换代重试环）
  ///
  /// libs/server/ACL/ACLParser.cs:ApplyACLOpToUser
  pub fn apply_acl_op_to_user(user: &mut User, op: &str) -> Result<(), AclError> {
    // 空操作直接跳过
    if op.is_empty() {
      return Ok(());
    }

    let first = op.as_bytes()[0];
    match first {
      // 启用账号
      b'o' | b'O' if op.eq_ignore_ascii_case("on") => user.set_enabled(true),
      // 禁用账号
      b'o' | b'O' if op.eq_ignore_ascii_case("off") => user.set_enabled(false),
      // 免密并清空全部口令
      b'n' | b'N' if op.eq_ignore_ascii_case("nopass") => {
        user.clear_passwords();
        user.set_passwordless(true);
      }
      // 清全部口令与访问权并禁用
      b'r' | b'R' if op.eq_ignore_ascii_case("reset") => user.reset(),
      // 清全部口令并关闭免密
      b'r' | b'R' if op.eq_ignore_ascii_case("resetpass") => {
        user.clear_passwords();
        user.set_passwordless(false);
      }
      // 口令加 / 删（>< 明文、#! 哈希；哈希非法归入解析错误）
      b'#' | b'!' | b'>' | b'<' => {
        let pw = match first {
          b'#' | b'!' => {
            AclPassword::from_hash(&op[1..]).map_err(|e| Self::parsing_err(e.to_string()))?
          }
          _ => AclPassword::from_string(&op[1..]),
        };
        dir(user, matches!(first, b'#' | b'>'), pw, PW);
      }
      // 分类加减
      b'-' | b'+' if op.len() >= 2 && op.as_bytes()[1] == b'@' => {
        let category_name = &op[2..];
        let category = Self::get_acl_category_by_name(category_name)
          .ok_or_else(|| AclError::CategoryDoesNotExist(category_name.to_string()))?;
        dir(user, first == b'+', category, CAT)?;
      }
      // 单命令 / 命令|子命令对；未知时回落自定义命令名
      b'-' | b'+' => {
        let command_name = &op[1..];
        let add = first == b'+';
        match Self::try_parse_command_for_acl(command_name) {
          Some(command) => dir(user, add, command, CMD)?,
          None if Self::is_valid_custom_command_name(command_name) => {
            // 模块可能尚未加载（ACL 文件先于 LoadModules 解析），先按名记录
            let f = [User::remove_custom_command, User::add_custom_command];
            f[usize::from(add)](user, command_name)?;
          }
          None => return Err(AclError::CommandDoesNotExist(command_name.to_string())),
        }
      }
      // 无操作：当前仅支持全通配键模式（若未来支持按键模式，GET 的
      // scatter-gather 快路径须按 key 复查 ACL）
      b'~' if op == "~*" => {}
      b'a' | b'A' if op.eq_ignore_ascii_case("allkeys") => {}
      // 无操作：同上
      b'r' | b'R' if op.eq_ignore_ascii_case("resetkeys") => {}
      _ => return Err(AclError::UnknownOperation(op.to_string())),
    }
    Ok(())
  }

  /// 命令名解析为 RespCommand（含子命令折名 / 去点 / 兼容别名处理）
  ///
  /// 全臂精确查表不修剪首尾空白：`" get"` 类带空白词形本侧拒收，
  /// C# `Enum.TryParse` 的空白修剪怪癖不镜像（deviations §105 在册，严禁补 trim）
  ///
  /// libs/server/ACL/ACLParser.cs:TryParseCommandForAcl
  pub fn try_parse_command_for_acl(command_name: &str) -> Option<RespCommand> {
    // 首个 '|' 折为 '_'（仅首个，对标 C# IndexOf + 拼接）
    let (effective, is_sub_command) = match command_name.find('|') {
      Some(ix) => (
        Cow::Owned(format!(
          "{}_{}",
          &command_name[..ix],
          &command_name[ix + 1..]
        )),
        true,
      ),
      None => (Cow::Borrowed(command_name), false),
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
    &NAMES
  }
}
