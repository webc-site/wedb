//! ACL 命令目录（对标 libs/server/Resp/RespCommandsInfo.cs 中 ACL 消费的面）
//!
//! C# 侧 ACL 经静态 `RespCommandsInfo` 取命令元数据（名称 / 父子关系 /
//! 分类归属）；rust 侧以 [`CMD_ENTRIES`] 静态表承接（数据自
//! garnet/libs/resources/RespCommandsInfo.json 生成），全部查询为无锁
//! 单遍扫描——目录仅 356 条且 ACL 规则修改是管理频度操作。

use super::command_catalog_data::CMD_ENTRIES;
pub(crate) use super::command_catalog_data::CmdEntry;
use crate::types::RespCommand;

bitflags::bitflags! {
    /// RESP ACL 分类位集
    ///
    /// libs/server/Resp/RespCommandInfoFlags.cs:RespAclCategories
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct RespAclCategories: u32 {
        /// 管理
        const ADMIN = 1;
        /// 位图
        const BITMAP = 1 << 1;
        /// 阻塞
        const BLOCKING = 1 << 2;
        /// 连接
        const CONNECTION = 1 << 3;
        /// 危险
        const DANGEROUS = 1 << 4;
        /// 地理
        const GEO = 1 << 5;
        /// 哈希
        const HASH = 1 << 6;
        /// HyperLogLog
        const HYPERLOGLOG = 1 << 7;
        /// 快速
        const FAST = 1 << 8;
        /// 键空间
        const KEYSPACE = 1 << 9;
        /// 列表
        const LIST = 1 << 10;
        /// 发布订阅
        const PUBSUB = 1 << 11;
        /// 读
        const READ = 1 << 12;
        /// 脚本
        const SCRIPTING = 1 << 13;
        /// 集合
        const SET = 1 << 14;
        /// 有序集合
        const SORTEDSET = 1 << 15;
        /// 慢速
        const SLOW = 1 << 16;
        /// 流
        const STREAM = 1 << 17;
        /// 字符串
        const STRING = 1 << 18;
        /// 事务
        const TRANSACTION = 1 << 19;
        /// 写
        const WRITE = 1 << 20;
        /// Garnet 扩展
        const GARNET = 1 << 21;
        /// 自定义命令
        const CUSTOM = 1 << 22;
        /// 向量集
        const VECTOR = 1 << 23;
        /// 全部（C# All = (Vector << 1) - 1，覆盖全部单类别位）
        const ALL = (1 << 24) - 1;
    }
}

/// SET 的 ACL 展开集（对标 C# ExpandedSET）
const EXPANDED_SET: [RespCommand; 4] = [
  RespCommand::Setexnx,
  RespCommand::Setexxx,
  RespCommand::Setkeepttl,
  RespCommand::Setkeepttlxx,
];

/// BITOP 的 ACL 展开集（对标 C# ExpandedBITOP）
const EXPANDED_BITOP: [RespCommand; 5] = [
  RespCommand::BitopAnd,
  RespCommand::BitopNot,
  RespCommand::BitopOr,
  RespCommand::BitopXor,
  RespCommand::BitopDiff,
];

/// 把"并非真实命令"的枚举值归一到 ACL 等价命令
///
/// libs/server/Resp/Parser/RespCommand.cs:NormalizeForACLs
#[inline]
pub const fn normalize_for_acls(cmd: RespCommand) -> RespCommand {
  match cmd {
    RespCommand::Setexnx
    | RespCommand::Setexxx
    | RespCommand::Setkeepttl
    | RespCommand::Setkeepttlxx => RespCommand::Set,
    RespCommand::BitopAnd
    | RespCommand::BitopNot
    | RespCommand::BitopOr
    | RespCommand::BitopXor
    | RespCommand::BitopDiff => RespCommand::Bitop,
    _ => cmd,
  }
}

/// 反向于 [`normalize_for_acls`]：给出 `cmd` 覆盖的全部等价命令
///
/// libs/server/Resp/Parser/RespCommand.cs:ExpandForACLs
#[inline]
pub fn expand_for_acls(cmd: RespCommand) -> &'static [RespCommand] {
  match cmd {
    RespCommand::Set => &EXPANDED_SET,
    RespCommand::Bitop => &EXPANDED_BITOP,
    _ => &[],
  }
}

/// 未认证也可执行的命令（AUTH..=QUIT 连续区间）
///
/// libs/server/Resp/Parser/RespCommand.cs:IsNoAuth
#[inline]
pub const fn is_no_auth(cmd: RespCommand) -> bool {
  // 无符号化做区间判断：低于 AUTH 的值下溢成大数，天然出界
  let v = (cmd as u16).wrapping_sub(RespCommand::Auth as u16);
  v <= (RespCommand::Quit as u16).wrapping_sub(RespCommand::Auth as u16)
}

/// 最后一个有效命令（除 INVALID 外的最大值，对标 C# LastValidCommand）
pub const LAST_VALID_COMMAND: RespCommand = RespCommand::Quit;

/// 按 RespCommand 取目录条目（含子命令条目）
///
/// 对标 libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandInfo
#[inline]
pub(crate) fn try_get_resp_command_info(cmd: RespCommand) -> Option<&'static CmdEntry> {
  CMD_ENTRIES.iter().find(|e| e.cmd == cmd)
}

/// 按命令名（大小写不敏感）取目录条目
///
/// 覆盖 Enum.TryParse(ignoreCase) 的对照（`cs` 即 C# 枚举成员名）
#[inline]
pub(crate) fn try_get_by_cs_name(name: &str) -> Option<&'static CmdEntry> {
  CMD_ENTRIES.iter().find(|e| e.cs.eq_ignore_ascii_case(name))
}

/// 根命令的全部子命令条目
#[inline]
pub(crate) fn children_of(cmd: RespCommand) -> impl Iterator<Item = &'static CmdEntry> {
  CMD_ENTRIES.iter().filter(move |e| e.parent == Some(cmd))
}

/// 分类成员条目（对标 C# AclCommandInfo 字典查询；复合分类按单类别位并集）
///
/// 对标 libs/server/Resp/RespCommandsInfo.cs:TryGetCommandsforAclCategory
pub(crate) fn try_get_commands_for_acl_category(
  acl: RespAclCategories,
) -> Option<Vec<&'static CmdEntry>> {
  if acl.is_empty() {
    return None;
  }
  let members: Vec<_> = CMD_ENTRIES
    .iter()
    .filter(|e| e.cats.intersects(acl))
    .collect();
  Some(members)
}
