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
//! 在 garnet 中的相对路径: libs/server/ACL/ACLParser.cs(对标 C# ACL 规则解析器)

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

/// ACL 分类单一源表：`(小写分类名, 分类位)`，声明序即 C# 列举序（含复合项 all）。
///
/// [`CATEGORY_NAMES`]（列举序名表）、[`BIT_NAMES`]（位权反查表）与按名查位
/// 均由本表单点派生，消除三处手工同步；新增分类只改此表一处
const CATEGORIES: [(&str, RespAclCategories); 25] = [
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

/// 全部合法分类静态名表（与 C# 列举序完全一致；编译期由 [`CATEGORIES`]
/// 声明序派生，禁手写第二份名单）
const CATEGORY_NAMES: [&str; CATEGORIES.len()] = {
  let mut names = [""; CATEGORIES.len()];
  let mut i = 0;
  while i < CATEGORIES.len() {
    names[i] = CATEGORIES[i].0;
    i += 1;
  }
  names
};

/// 分类位按权值映射表（0..23 位与 RespAclCategories 单类别位权值严格对齐；
/// 编译期由 [`CATEGORIES`] 派生——仅单类别位（2 的幂）入表，复合项 all 跳过）
const BIT_NAMES: [&str; 24] = {
  let mut names = [""; 24];
  let mut i = 0;
  while i < CATEGORIES.len() {
    let (name, bits) = (CATEGORIES[i].0, CATEGORIES[i].1.bits());
    if bits.is_power_of_two() {
      names[bits.trailing_zeros() as usize] = name;
    }
    i += 1;
  }
  names
};

/// ACL 解析器（全部为无状态静态方法）
pub struct AclParser;

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
        if matches!(first, b'#' | b'>') {
          user.add_password_hash(pw);
        } else {
          user.remove_password_hash(pw);
        }
      }
      // 分类加减
      b'-' | b'+' if op.len() >= 2 && op.as_bytes()[1] == b'@' => {
        let category_name = &op[2..];
        let category = Self::get_acl_category_by_name(category_name)
          .ok_or_else(|| AclError::CategoryDoesNotExist(category_name.to_string()))?;
        if first == b'+' {
          user.add_category(category)?;
        } else {
          user.remove_category(category)?;
        }
      }
      // 单命令 / 命令|子命令对；枚举亦无的未知名回落自定义命令名轨
      // （枚举在场、目录缺席名在此照常 Some，由 add/remove_command 查目录
      // 失配失败关闭回 "Unable to obtain ACL information…"，不落回落臂，
      // 对标 garnet/libs/server/ACL/User.cs:AddCommand/RemoveCommand 抛形）
      b'-' | b'+' => {
        let command_name = &op[1..];
        let add = first == b'+';
        match Self::try_parse_command_for_acl(command_name) {
          Ok(Some(command)) => {
            if add {
              user.add_command(command)?;
            } else {
              user.remove_command(command)?;
            }
          }
          // 子命令折名目录缺席：C# 同位 throw 的 fail-closed（原样上抛，
          // 文案逐字对位 ACLException），绝不落回落轨
          Err(err) => return Err(err),
          Ok(None) if Self::is_valid_custom_command_name(command_name) => {
            // 模块可能尚未加载（ACL 文件先于 LoadModules 解析），先按名记录
            if add {
              user.add_custom_command(command_name)?;
            } else {
              user.remove_custom_command(command_name)?;
            }
          }
          Ok(None) => return Err(AclError::CommandDoesNotExist(command_name.to_string())),
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
  /// 解析面与 C# 同为「全枚举成员」判定（strum 单源，见 `lookup_command`），
  /// 目录在场与否不参与顶层解析臂——顶层枚举在场、目录缺席之名返回
  /// `Ok(Some)`，失败关闭顺延至 `User::add_command`/`remove_command` 的
  /// 目录查询（对标 C# AddCommand 查目录抛 ACLException，garnet/libs/server/
  /// ACL/User.cs:187-190/:316-318），不走自定义回落。
  /// **子命令折名例外**：折叠名目录缺席（BITOP_AND 族五成员枚举在场而
  /// RespCommandsInfo.json 零条目）对位 C# ACLParser.cs `if(isSubCommand)`
  /// 块 `throw "Couldn't load information for {X}, shouldn't be possible"`
  ///（先于 IsInvalidCommandToAcl，绝不落自定义回落轨），返回 Err 失败关闭
  ///
  /// 全臂精确查表不修剪首尾空白：`" get"` 类带空白词形本侧拒收，
  /// C# `Enum.TryParse` 的空白修剪怪癖不镜像（deviations §105 在册，严禁补 trim）
  ///
  /// libs/server/ACL/ACLParser.cs:TryParseCommandForAcl
  ///
  /// 返回：`Ok(Some)` = 解析命中；`Ok(None)` = 枚举亦无名（调用方按自定义
  /// 回落轨裁决）；`Err` = 子命令折名目录缺席的 fail-closed
  pub fn try_parse_command_for_acl(command_name: &str) -> Result<Option<RespCommand>, AclError> {
    // 首个 '|' 折为 '_'（仅首个，对标 C# IndexOf + 拼接）
    let (effective, is_sub_command) = match command_name.find('|') {
      Some(ix) => {
        let mut buf = String::with_capacity(command_name.len());
        buf.push_str(&command_name[..ix]);
        buf.push('_');
        buf.push_str(&command_name[ix + 1..]);
        (Cow::Owned(buf), true)
      }
      None => (Cow::Borrowed(command_name), false),
    };

    let command = match Self::lookup_command(&effective)
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
      }) {
      Some(command) => command,
      None => return Ok(None),
    };

    // 子命令折名须能取到目录信息：目录缺席即登记数据面缺口（C# 同位
    // throw），fail-closed 上抛，绝不静默落自定义回落轨
    if is_sub_command && catalog::try_get_resp_command_info(command).is_none() {
      return Err(Self::parsing_err(format!(
        "Couldn't load information for {effective}, shouldn't be possible"
      )));
    }
    Ok((!Self::is_invalid_command_to_acl(command)).then_some(command))
  }

  /// 目录名对照（Enum.TryParse(ignoreCase) + 解析有效性校验）
  ///
  /// 判定集为 `RespCommand` 枚举成员名集的 strum 编译期单源派生
  /// （[`RespCommand::from_cs_name`]，即 C# `Enum.TryParse(effectiveName,
  /// ignoreCase: true)` 的 rust 对位，garnet/libs/server/ACL/ACLParser.cs:281/
  /// :285），不查目录、无运行时新字典新锁、禁第二份手写名单。
  /// 枚举在场而目录缺席的名（DELIFEXPIM=9 / RIPROMOTE=63 / RIRESTORE=64，
  /// garnet/libs/server/Resp/Parser/RespCommand.cs:40/:94/:95，
  /// RespCommandsInfo.json 零条目）在此照常解析命中，随后由
  /// `User::apply_command` 查目录失配失败关闭（对标 C# User.cs:189/:318
  /// 抛 "Unable to obtain ACL information, this shouldn't be possible"），
  /// 绝不落自定义命令回落臂
  #[inline]
  fn lookup_command(effective_name: &str) -> Option<RespCommand> {
    let command = RespCommand::from_cs_name(effective_name)?;
    Self::is_valid_parse(command, effective_name).then_some(command)
  }

  /// 解析值是否可能对应该输入（处理 Enum.TryParse 的怪癖：含数字即否）
  ///
  /// libs/server/ACL/ACLParser.cs:IsValidParse
  #[inline]
  fn is_valid_parse(command: RespCommand, from_str: &str) -> bool {
    command != RespCommand::None
      && command != RespCommand::Invalid
      && !from_str.bytes().any(|b| b.is_ascii_digit())
  }

  /// ACL 不应接受的"并非真命令"枚举值（实现细节或哨兵）
  ///
  /// libs/server/ACL/ACLParser.cs:IsInvalidCommandToAcl
  #[inline]
  fn is_invalid_command_to_acl(command: RespCommand) -> bool {
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

  /// 按名查分类（源表 [`CATEGORIES`] 线性扫一遍，大小写不敏感；未知名返回
  /// None，对标 C# 键不存在。25 条静态表线性比对在 ACL 规则解析面无热点）
  ///
  /// libs/server/ACL/ACLParser.cs:GetACLCategoryByName
  pub fn get_acl_category_by_name(name: &str) -> Option<RespAclCategories> {
    CATEGORIES
      .iter()
      .find(|(category_name, _)| category_name.eq_ignore_ascii_case(name))
      .map(|(_, category)| *category)
  }

  /// 分类位的名称（O(1) 尾零硬件指令寻址，未知组合位返回 "unknown"，对标 C# 反查缺项异常路径）
  ///
  /// libs/server/ACL/ACLParser.cs:GetNameByACLCategory
  pub const fn get_name_by_acl_category(category: RespAclCategories) -> &'static str {
    let bits = category.bits();
    if bits == RespAclCategories::ALL.bits() {
      return "all";
    }
    if bits.is_power_of_two() {
      let tz = bits.trailing_zeros() as usize;
      if tz < BIT_NAMES.len() {
        return BIT_NAMES[tz];
      }
    }
    "unknown"
  }

  /// 全部合法分类名
  ///
  /// libs/server/ACL/ACLParser.cs:ListCategories
  pub const fn list_categories() -> &'static [&'static str] {
    &CATEGORY_NAMES
  }
}
