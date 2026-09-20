//! RESP 命令解析（对标 libs/server/Resp/Parser/RespCommand.cs）
//!
//! C# `RespCommand.cs` 为 `RespServerSession` 的 partial 分片（命令枚举 +
//! 快速解析 + 哈希查表 + 子命令分派），Rust 侧映射为本文件的
//! `impl RespServerSession` 扩展块；命令枚举本体在
//! [`wresp::command::RespCommand`]（types 域，判别值逐项对齐）。
//!
//! 解析分级（与 C# 一致）：
//! 1. 内联命令（`PING\r\n` / `QUIT\r\n`）；
//! 2. 16 字节模式表（固定参数个数热命令，一次 16B 载入 + 分组掩码整向量
//!    全等，与 C# SIMD Vector128 路径同型；模式字节逐项与
//!    RespCommandSimdPatterns.cs 对齐）+ 会话 MRU 双槽缓存（同一定长窗口上
//!    逐槽掩码全等）；
//! 3. 单数字帧标量快路径（`*_\r\n$_\r\n` 掩码比较；二级表含变参热命令与
//!    SET/GETEX/EXPIRE/PEXPIRE 长名命令）；
//! 4. 慢路径：大写化 → 数组头 → 命令名查表（有序名称表二分承接
//!    RespCommandHashLookup 的 Lookup / LookupSubcommand 语义）→ 子命令分派。

use std::{
  mem::{swap, take},
  str::from_utf8,
};

use memchr::memmem;
use wbase::simd::{first_masked_eq, masked_eq};
use wresp::{
  cmd_strings as cs, command::RespCommand, ext::sanitize_error_str,
  read::try_read_unsigned_array_length,
};
use wtxn::TxnState;

use super::{
  super::resp_server_session::RespServerSession,
  command_table::{lookup_primary, lookup_subcommand},
  fast_patterns::{FAST_PATTERN_GROUPS, FAST_PATTERN_TABLE, mask_for, pattern_matches},
};
use crate::resp::custom_objects::match_custom_object_command;

/// libs/server/Resp/Parser/RespCommand.cs:TryParseCustomCommand
///
/// 扩展命令编译期静态清单匹配（脱离会话借用的纯函数，零锁零分配）：
/// 内建表未命中时以命令名直查扩展对象静态清单
///（[`crate::resp::custom_objects`] 单点组装），命中产出全量命令引用
///（静态清单规范名 + 命令类型 + 键作用域 + arity + 执行体 + 信封标签，
/// 标签自命中清单项、作用域自命令元数据单点取值）；未命中 / 未配置扩展
/// 特性（空清单）维持 None（与
/// C# 注册表无该项时的行为一致）。
fn try_parse_custom_command(
  command: &[u8],
) -> Option<super::super::resp_server_session::CustomCommandRef> {
  let (entry, meta) = match_custom_object_command(command)?;
  Some(super::super::resp_server_session::CustomCommandRef {
    name: meta.name,
    command_type: meta.command_type,
    key_scope: meta.key_scope,
    arity: meta.arity,
    object_tag: entry.tag,
    fns: meta.fns,
  })
}

/// C# MaxRespArrayLength：单条 RESP 命令参数上限（防预认证内存耗尽）
pub const MAX_RESP_ARRAY_LENGTH: usize = 1 << 20;

/// C# libs/server/Resp/Parser/RespCommand.cs:IsAofIndependent：AOF 无关命令集（读写不依赖日志直写）。
/// 逐项对标 libs/server/Resp/Parser/RespCommand.cs:AofIndependentCommands
///（47 项，含 CLIENT/COMMAND/MEMORY/CONFIG/LATENCY/SLOWLOG 全族与 MULTI）。
/// `matches!` 编译为判别值跳转表——每条命令解析后调用（GET/SET 等 AOF 相关
/// 命令为最频命令，线性全扫 47 项改为 switch），全集对齐由测试逐项校验
#[inline]
pub const fn is_aof_independent(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    RespCommand::Async
      | RespCommand::Ping
      | RespCommand::Select
      | RespCommand::Swapdb
      | RespCommand::Echo
      | RespCommand::Monitor
      | RespCommand::Info
      | RespCommand::Time
      | RespCommand::Lastsave
      // ACL 族
      | RespCommand::AclCat
      | RespCommand::AclDeluser
      | RespCommand::AclGenpass
      | RespCommand::AclGetuser
      | RespCommand::AclList
      | RespCommand::AclLoad
      | RespCommand::AclSave
      | RespCommand::AclSetuser
      | RespCommand::AclUsers
      | RespCommand::AclWhoami
      // Client 族
      | RespCommand::ClientId
      | RespCommand::ClientInfo
      | RespCommand::ClientList
      | RespCommand::ClientKill
      | RespCommand::ClientGetname
      | RespCommand::ClientSetname
      | RespCommand::ClientSetinfo
      | RespCommand::ClientUnblock
      // Command 族
      | RespCommand::Command
      | RespCommand::CommandCount
      | RespCommand::CommandDocs
      | RespCommand::CommandInfo
      | RespCommand::CommandGetkeys
      | RespCommand::CommandGetkeysandflags
      // Memory / Config 族
      | RespCommand::MemoryUsage
      | RespCommand::ConfigGet
      | RespCommand::ConfigRewrite
      | RespCommand::ConfigSet
      // Latency 族
      | RespCommand::LatencyHelp
      | RespCommand::LatencyHistogram
      | RespCommand::LatencyReset
      // Slowlog 族
      | RespCommand::SlowlogHelp
      | RespCommand::SlowlogLen
      | RespCommand::SlowlogGet
      | RespCommand::SlowlogReset
      // 事务
      | RespCommand::Multi
  )
}

/// libs/server/Resp/Parser/RespCommand.cs:IsAllowedInSubscriptionMode
///
/// 返回 true 如果命令允许在 pub/sub 订阅模式下执行（RESP2）。
/// Sunsubscribe 一臂为 rust 补全项（C# :735-742 允许集无此命令，见
/// wpubsub::session_commands 的 network_sunsubscribe 声明），其余与 C# 逐臂同。
#[inline]
pub const fn is_allowed_in_subscription_mode(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    RespCommand::Subscribe
      | RespCommand::Unsubscribe
      | RespCommand::Psubscribe
      | RespCommand::Punsubscribe
      | RespCommand::Ssubscribe
      | RespCommand::Sunsubscribe
      | RespCommand::Ping
      | RespCommand::Quit
  )
}

/// 会话侧 MRU 命令缓存（C# _cachedCmd0/1 双槽；哈希表命中后填充，
/// 槽 1 命中由调用方晋升换位 —— C# SimdFastParse 的 promote 语义）
#[derive(Default)]
pub(crate) struct MruCommandCache {
  slot0: Option<MruEntry>,
  slot1: Option<MruEntry>,
}

#[derive(Clone, Copy)]
struct MruEntry {
  pattern: [u8; 16],
  /// 消费长度之后的字节清零（C# _cachedMaskN，随槽位一同换入换出）
  mask: [u8; 16],
  len: u8,
  cmd: RespCommand,
  count: u8,
}

impl MruCommandCache {
  /// C# UpdateCommandCache 尾段：新命中晋升槽 0，原槽 0 降级槽 1
  fn update(&mut self, frame: &[u8], consumed: usize, cmd: RespCommand, count: u8) {
    debug_assert!((13..=16).contains(&consumed) && frame.len() >= consumed);
    let mut entry = MruEntry {
      pattern: [0; 16],
      mask: mask_for(consumed),
      len: consumed as u8,
      cmd,
      count,
    };
    entry.pattern[..consumed].copy_from_slice(&frame[..consumed]);
    self.slot1 = self.slot0;
    self.slot0 = Some(entry);
  }

  /// 双槽掩码全等命中（C# SimdFastParse 的 MRU 两臂
  /// `EqualsAll(BitwiseAnd(input, _cachedMaskN), _cachedPatternN)`）：
  /// 每槽一次按位与 + 整向量全等，槽 0 未填充时两槽皆不进比较
  /// （C# `_cachedCmd0 != RespCommand.NONE` 门）；返回命中帧与命中槽号
  /// （槽 1 命中由调用方执行晋升换位）
  fn lookup(&self, window: &[u8; 16]) -> Option<(MruEntry, usize)> {
    let slot0 = self.slot0?;
    if masked_eq(window, &slot0.mask, &slot0.pattern) {
      return Some((slot0, 0));
    }
    let slot1 = self.slot1?;
    if masked_eq(window, &slot1.mask, &slot1.pattern) {
      return Some((slot1, 1));
    }
    None
  }

  /// 槽 1 命中 → 两槽互换（C# SimdFastParse 的 swap 晋升）
  fn promote(&mut self, slot_idx: usize) {
    if slot_idx == 1 {
      swap(&mut self.slot0, &mut self.slot1);
    }
  }
}

impl RespServerSession {
  /// libs/server/Resp/Parser/RespCommand.cs:ParseCommand
  ///
  /// 解析缓冲内下一条命令：快路径 → 慢路径 → 装载解析态 → AOF 阻塞标记。
  /// 返回 None 表示命令未完整到达或协议错误（C# success = false / 抛
  /// RespParsingException，由上层断连）；未知命令返回 RespCommand::Invalid
  /// 并按 C# writeErrorOnFailure 写错误应答。
  pub fn parse_command(&mut self) -> Option<RespCommand> {
    self.parse_command_with(true)
  }

  /// ParseCommand 主体（`write_error_on_failure` 对齐 C# 同名参数；
  /// 独立缓冲校验入口传 false，不往输出缓冲写解析错误）
  fn parse_command_with(&mut self, write_error_on_failure: bool) -> Option<RespCommand> {
    let mut count: isize = -1;
    self.end_read_head = self.read_head;

    // 快速解析
    let mut cmd = self.fast_parse_command(&mut count);

    // 慢路径
    if cmd == RespCommand::None {
      let cmd_start_offset = self.read_head;
      cmd = self.array_parse_command(&mut count, write_error_on_failure)?;
      // MRU 缓存更新（哈希表命中的命令；排除扩展静态清单命中的自定义对象命令，
      // 其命令名逐条各异，不入按名定长缓存）
      if cmd != RespCommand::Invalid && cmd != RespCommand::None && cmd != RespCommand::Customobjcmd
      {
        self.update_command_cache(cmd_start_offset, cmd, count);
      }
    }

    if count > MAX_RESP_ARRAY_LENGTH as isize {
      // C# RespParsingException.ThrowExcessiveArgumentCount → 上层 catch 写
      // ERR Protocol Error 后断连
      self.violation_excessive_arg_count(count);
      return None;
    }
    let count = count.max(0) as usize;

    // 装载解析态（C# parseState.Initialize(count) + parseState.Read(i, ...)）
    self.parse_state.initialize(count);
    let mut ptr = self.read_head;
    for i in 0..count {
      match self
        .parse_state
        .read(i, &self.recv_buffer, &mut ptr, self.bytes_read)
      {
        Ok(true) => {}
        // 参数负载未到齐（C# Read 返回 false → success = false）：回 None 由
        // process_messages 回退双游标等待下一批字节
        Ok(false) => return None,
        // 协议违例（C# Read 内 ThrowUnexpectedToken / ThrowInvalidStringLength
        // / ThrowIntegerOverflow 抛出即断连）：置同一违例载体，游标不回退，
        // 由消费入口写 ERR Protocol Error 后断连——绝不得退化为半包等待，
        // 否则畸形帧令会话永久挂死
        Err(err) => {
          self.violation_parse_error(err);
          return None;
        }
      }
    }
    self.end_read_head = ptr;

    // C# EnableAOF + WaitForCommit 时才按命令依赖性维护阻塞标记
    //（RespCommand.cs:ParseCommand 尾部门控）
    if self.aof_commit_mode_gate {
      self.handle_aof_commit_mode(cmd);
    }
    Some(cmd)
  }

  /// libs/server/Resp/Parser/RespCommand.cs:UpdateCommandCache
  ///
  /// 参数个数超 u8 / 可用字节不足 16 / 实际消费不在 13..=16 字节均不缓存
  fn update_command_cache(&mut self, cmd_start_offset: usize, cmd: RespCommand, arg_count: isize) {
    let arg_count = arg_count.max(0) as usize;
    if arg_count > usize::from(u8::MAX) {
      return;
    }
    // 16 字节窗口须完整可用（C# Vector128.LoadUnsafe 前置条件）
    if self.bytes_read - cmd_start_offset < 16 {
      return;
    }
    // 命令名（含子命令）的解析消费长度，仅单 Vector128 窗口内可缓存
    let consumed = self.read_head - cmd_start_offset;
    if !(13..=16).contains(&consumed) {
      return;
    }
    let frame = &self.recv_buffer[cmd_start_offset..cmd_start_offset + 16];
    self.mru_cache.update(frame, consumed, cmd, arg_count as u8);
  }

  /// libs/server/Resp/Parser/RespCommand.cs:FastParseCommand
  pub fn fast_parse_command(&mut self, count: &mut isize) -> RespCommand {
    let start = self.read_head;
    let remaining = self.bytes_read.saturating_sub(start);

    // 模式表 + MRU 快路径（C# SimdFastParse 对位：>= 16 字节且数组帧，
    // 一次 16B 载入 + 分组掩码整向量全等；模式长度之后的输入字节不作约束，
    // 与 C# 尾部掩码语义一致）
    if remaining >= 16
      // 定长 16B 窗口（remaining 门已保证 16 个有效字节可读，切分仅消边界 panic 面）
      && let Some((window, _rest)) = self.recv_buffer[start..].split_first_chunk::<16>()
      && window[0] == b'*'
    {
      // 模式表臂：命中下标即 FAST_PATTERN_TABLE 的全局序位（判定序与 C# 同）
      if let Some((pattern, cmd, arg_count)) =
        first_masked_eq(window, FAST_PATTERN_GROUPS).and_then(|idx| FAST_PATTERN_TABLE.get(idx))
      {
        self.read_head += pattern.len();
        *count = isize::from(*arg_count);
        return *cmd;
      }
      if let Some((entry, slot_idx)) = self.mru_cache.lookup(window) {
        self.mru_cache.promote(slot_idx);
        self.read_head += entry.len as usize;
        *count = isize::from(entry.count);
        return entry.cmd;
      }
    }

    // 标量快路径：单数字数组帧 + 单数字串长（C# 0xFFFF00FFFFFF00FF 掩码技巧）
    if remaining >= 8
      && self.recv_buffer[start] == b'*'
      && self.recv_buffer[start + 2] == b'\r'
      && self.recv_buffer[start + 3] == b'\n'
      && self.recv_buffer[start + 4] == b'$'
      && self.recv_buffer[start + 6] == b'\r'
      && self.recv_buffer[start + 7] == b'\n'
    {
      // 数组元素总数 - 1（首 token 即命令名；i64 算术杜绝 u8 下溢 panic）
      *count = i64::from(self.recv_buffer[start + 1]) as isize - isize::from(b'1');
      let length = i64::from(self.recv_buffer[start + 5]) - i64::from(b'0');

      // 命令名 1..=9 字节且完整帧在缓冲内（10 = 帧头 8 + 名尾 \r\n 2）
      if (1..=9).contains(&length) && remaining >= length as usize + 10 {
        let frame_len = length as usize + 10;
        let frame_end = start + frame_len;
        self.read_head += frame_len;

        // (1) 固定参数个数热命令（C# 第一标量表：count/lastWord 判定，
        // 与整帧字节比对等价；缓冲 < 16 字节时的主路径）
        for (pattern, cmd, _) in FAST_PATTERN_TABLE {
          if pattern.len() == frame_len && pattern_matches(&self.recv_buffer, start, pattern) {
            return *cmd;
          }
        }

        // (2) 变参热命令 + 名称超 6 字符命令（C# 第二标量表）
        // lastWord = 帧末 8 字节；prefix = 名首 2 字节
        let buf = &self.recv_buffer;
        let last_word = &buf[start + length as usize + 2..frame_end];
        let prefix = &buf[start + 8..start + 10];
        return match (*count, length) {
          (2, 7) if last_word == b"UBLISH\r\n" && buf[start + 8] == b'P' => RespCommand::Publish,
          (2, 8) if last_word == b"UBLISH\r\n" && prefix == b"SP" => RespCommand::Spublish,
          (3, 8) if last_word == b"TRANGE\r\n" && prefix == b"SE" => RespCommand::Setrange,
          (3, 8) if last_word == b"TRANGE\r\n" && prefix == b"GE" => RespCommand::Getrange,
          // (3) 长名/变参（C# 嵌套 (length << 4) | count 表）
          (3..=7, 3) if last_word == b"3\r\nSET\r\n" => RespCommand::Setexnx,
          (1..=3, 5) if last_word == b"\nGETEX\r\n" => RespCommand::Getex,
          (2..=3, 6) if last_word == b"EXPIRE\r\n" => RespCommand::Expire,
          (2..=3, 7) if last_word == b"EXPIRE\r\n" && buf[start + 8] == b'P' => {
            RespCommand::Pexpire
          }
          _ => self.matched_none(start, count),
        };
      }
      *count = -1;
      return RespCommand::None;
    }

    // 内联命令
    self.fast_parse_inline_command(count)
  }

  /// libs/server/Resp/Parser/RespCommand.cs:FastParseInlineCommand
  pub fn fast_parse_inline_command(&mut self, count: &mut isize) -> RespCommand {
    let start = self.read_head;
    *count = 0;
    // 内联命令形如 "XXXX\r\n"，首猜 PING / QUIT（精确大小写匹配，对齐 C#）
    if self.bytes_read - start >= 6 {
      let word = &self.recv_buffer[start..start + 4];
      if &self.recv_buffer[start + 4..start + 6] == b"\r\n" {
        self.read_head += 6;
        if word == b"PING" {
          return RespCommand::Ping;
        }
        if word == b"QUIT" {
          return RespCommand::Quit;
        }
        // 未命中回退游标
        self.read_head -= 6;
      }
    }
    RespCommand::None
  }

  /// libs/server/Resp/Parser/RespCommand.cs:MatchedNone（局部函数）
  fn matched_none(&mut self, old_read_head: usize, count: &mut isize) -> RespCommand {
    self.read_head = old_read_head;
    *count = -1;
    RespCommand::None
  }

  /// libs/server/Resp/Parser/RespCommand.cs:AttemptSkipLine
  ///
  /// 跳至行尾（畸形内联输入）；找到 "\r\n" 返回 true 并推进双游标
  pub fn attempt_skip_line(&mut self) -> bool {
    let start = self.read_head;
    if start < self.bytes_read
      && let Some(pos) = memmem::find(&self.recv_buffer[start..self.bytes_read], b"\r\n")
    {
      self.read_head = start + pos + 2;
      self.end_read_head = self.read_head;
      return true;
    }
    false
  }

  /// 旁路缓冲解析骨架（C# `ParseRespCommandBuffer` / `FuzzParseCommandBuffer` 共有的
  /// 「暂存主会话接收态 → 灌入旁路缓冲 → 解析 → 复位」样板，两口单点承接）
  ///
  /// 解析内核与主会话同一入口（`parse_command_with`），本壳只换入换出缓冲；
  /// 协议违规哨兵仅反映本轮旁路解析结果，不外溢主会话状态
  fn with_bypass_buffer<R>(&mut self, buffer: &[u8], parse: impl FnOnce(&mut Self) -> R) -> R {
    let saved = self.take_receive_state();
    self.recv_buffer.clear();
    self.recv_buffer.extend_from_slice(buffer);
    self.bytes_read = self.recv_buffer.len();
    self.read_head = 0;
    let parsed = parse(self);
    self.restore_receive_state(saved);
    self.parse_violation = None;
    parsed
  }

  /// libs/server/Resp/Parser/RespCommand.cs:ParseRespCommandBuffer
  ///
  /// 独立缓冲命令解析（校验用途；不写错误应答）。生产消费位：脚本域 `redis.call`
  /// 的成帧后命令名解析（`ScriptingApi::parse_resp_command_buffer`）
  pub fn parse_resp_command_buffer(&mut self, buffer: &[u8]) -> Option<RespCommand> {
    self
      .with_bypass_buffer(buffer, |s| s.parse_command_with(false))
      .filter(|cmd| *cmd != RespCommand::Invalid)
  }

  /// libs/server/Resp/Parser/RespCommand.cs:FuzzParseCommandBuffer
  ///
  /// 模糊测试入口：允许部分命令；返回 (是否完整解析, 命令)。
  /// C# 侧 `internal` 且唯一消费者是 Garnet.fuzz / Garnet.test（本仓不转写 benchmark），
  /// 故以 `debug_assertions` 门复现「仅测试可见」，发布构建不导出
  #[cfg(debug_assertions)]
  pub fn fuzz_parse_command_buffer(&mut self, buffer: &[u8]) -> (bool, RespCommand) {
    let cmd = self.with_bypass_buffer(buffer, |s| {
      if s.bytes_read >= 4 {
        s.parse_command_with(false).unwrap_or(RespCommand::Invalid)
      } else {
        RespCommand::Invalid
      }
    });
    (cmd != RespCommand::Invalid, cmd)
  }

  /// libs/server/Resp/Parser/RespCommand.cs:HandleAofCommitMode
  ///
  /// 无未发送数据时重置阻塞标记；命令 AOF 相关则保持/置位
  pub fn handle_aof_commit_mode(&mut self, cmd: RespCommand) {
    if self.pending_output_len() == 0 {
      self.wait_for_aof_blocking = false;
    }
    // 事务跳过模式中的命令不执行，不置位
    if self.txn_state == TxnState::Started {
      return;
    }
    self.wait_for_aof_blocking = self.wait_for_aof_blocking || !is_aof_independent(cmd);
  }

  /// libs/server/Resp/Parser/RespCommand.cs:ArrayParseCommand
  pub fn array_parse_command(
    &mut self,
    count: &mut isize,
    write_error_on_failure: bool,
  ) -> Option<RespCommand> {
    self.end_read_head = self.read_head;
    let start = self.read_head;

    // 全大写化后重试快路径（C# MakeUpperCase → FastParseCommand）
    if start < self.bytes_read && self.make_upper_case(start, self.bytes_read - start) {
      let cmd = self.fast_parse_command(count);
      if cmd != RespCommand::None {
        return Some(cmd);
      }
    }

    // 须为数组帧
    if start >= self.bytes_read || self.recv_buffer[start] != b'*' {
      // 内联命令包：跳行（畸形输入；行尾未完整到达返回 None）
      if !self.attempt_skip_line() {
        return None;
      }
      return Some(RespCommand::Invalid);
    }

    // 数组长度头（C# RespReadUtils.TryReadUnsignedArrayLength →
    // TryReadSignedLengthHeader）与命令名/参数侧共用 wresp::read 单点解码：
    // 非法 token（首字符非数字 / 负长度 / 终止符不符）→ 违例断连（C# throw），
    // 字节未到齐（不足 3 字节 / '-'+不足 4 字节 / 终止符两字节未达）→ 等待
    let mut head: &[u8] = &self.recv_buffer[start..self.bytes_read];
    let mut array_len = 0;
    let header = try_read_unsigned_array_length(&mut array_len, &mut head);
    let ptr = self.bytes_read - head.len();
    match header {
      Ok(true) => {}
      Ok(false) => return None,
      Err(err) => {
        self.violation_parse_error(err);
        return None;
      }
    }
    self.read_head = ptr;
    *count = array_len as isize;

    // 命令名查表（哈希查表 + 子命令分派）；None = 命令名未完整到达
    //（C# success = false，不写错误应答，由上层等待后续字节）
    let mut specific_error: Option<Vec<u8>> = None;
    let cmd = self.hash_lookup_command(count, &mut specific_error)?;

    // 未知命令：写错误应答（C# writeErrorOnFailure 门；
    // RespWriteUtils.TryWriteError 的 -msg\r\n 封套）
    if write_error_on_failure && cmd == RespCommand::Invalid {
      if let Some(error) = specific_error {
        let err_str = from_utf8(&error).unwrap_or("");
        self.abort_error_message(err_str);
      } else {
        self.abort_error_message(cs::RESP_ERR_GENERIC_UNK_CMD);
      }
    }
    Some(cmd)
  }

  /// libs/server/Resp/Parser/RespCommand.cs:HashLookupCommand
  ///
  /// 返回 None 表示命令名未完整到达（C# success = false）；未知命令返回
  /// Invalid 并填充 `specific_error`
  pub fn hash_lookup_command(
    &mut self,
    count: &mut isize,
    specific_error: &mut Option<Vec<u8>>,
  ) -> Option<RespCommand> {
    let (start, len) = self.get_command_range()?;
    // 命令名自读游标移除
    *count -= 1;

    let primary = lookup_primary(&self.recv_buffer[start..start + len]);
    match primary {
      None => {
        // 非内建命令 → 扩展命令静态清单匹配（冷路径；纯编译期函数，命令名
        // 直接借接收缓冲切片，免锁免堆分配——命中即全量填充会话侧当前
        // 命令引用，执行域零回查）
        if let Some(custom) = try_parse_custom_command(&self.recv_buffer[start..start + len]) {
          self.current_custom_command = Some((RespCommand::Customobjcmd, custom));
          return Some(RespCommand::Customobjcmd);
        }
        Some(RespCommand::Invalid)
      }
      Some((cmd, has_subcommands)) => {
        if has_subcommands {
          self.handle_subcommand_lookup(cmd, count, specific_error)
        } else {
          Some(cmd)
        }
      }
    }
  }

  /// libs/server/Resp/Parser/RespCommand.cs:HandleSubcommandLookup
  pub fn handle_subcommand_lookup(
    &mut self,
    parent_cmd: RespCommand,
    count: &mut isize,
    specific_error: &mut Option<Vec<u8>>,
  ) -> Option<RespCommand> {
    // COMMAND 无参 → COMMAND（列出全部命令）
    if parent_cmd == RespCommand::Command && *count == 0 {
      return Some(RespCommand::Command);
    }
    // 多数父命令要求至少一个子命令（BITOP 为语法错误文案；父命令名取
    // C# 枚举成员的大写形式，单源 resp_command_to_cs_name 零分配）
    if *count == 0 {
      let parent = parent_cmd.to_cs_name();
      *specific_error = Some(if parent_cmd == RespCommand::Bitop {
        cs::RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes().to_vec()
      } else {
        cs::GENERIC_ERR_WRONG_NUM_ARGS
          .replace("{0}", parent)
          .into_bytes()
      });
      return Some(RespCommand::Invalid);
    }

    let (sub_start, sub_len) = self.get_upper_case_command_range()?;
    *count -= 1;

    let sub_command = &self.recv_buffer[sub_start..sub_start + sub_len];
    if let Some(sub_cmd) = lookup_subcommand(parent_cmd, sub_command) {
      return Some(sub_cmd);
    }

    // 未知子命令错误文案（BITOP → 语法错误；CLUSTER/LATENCY 带帮助提示，
    // 其余为无提示版 —— 逐字节对齐 C# CmdStrings，净化防换行注入）
    let sub_text = String::from_utf8_lossy(sub_command);
    let clean_sub = sanitize_error_str(&sub_text, cs::MAX_PARAM_NAME_LEN);
    let parent = parent_cmd.to_cs_name();
    let clean_parent = sanitize_error_str(parent, cs::MAX_PARAM_NAME_LEN);
    *specific_error = Some(if parent_cmd == RespCommand::Bitop {
      cs::RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes().to_vec()
    } else if matches!(parent_cmd, RespCommand::Cluster | RespCommand::Latency) {
      cs::GENERIC_ERR_UNKNOWN_SUB_COMMAND
        .replace("{0}", clean_sub)
        .replace("{1}", clean_parent)
        .into_bytes()
    } else {
      cs::GENERIC_ERR_UNKNOWN_SUB_COMMAND_NO_HELP
        .replace("{0}", clean_sub)
        .into_bytes()
    });
    Some(RespCommand::Invalid)
  }

  /// 接收状态快照（ParseRespCommandBuffer / FuzzParseCommandBuffer 恢复用）
  fn take_receive_state(&mut self) -> (Vec<u8>, usize, usize, usize) {
    (
      take(&mut self.recv_buffer),
      self.bytes_read,
      self.read_head,
      self.end_read_head,
    )
  }

  fn restore_receive_state(&mut self, saved: (Vec<u8>, usize, usize, usize)) {
    let (buffer, bytes_read, read_head, end_read_head) = saved;
    self.recv_buffer = buffer;
    self.bytes_read = bytes_read;
    self.read_head = read_head;
    self.end_read_head = end_read_head;
  }
}

#[cfg(test)]
mod tests {
  use super::{super::command_table::*, *};

  /// 测试用父命令-子表三元组(父命令名, 父命令, 子命令表)
  type ParentSubtable = (
    &'static str,
    RespCommand,
    &'static [(&'static str, RespCommand)],
  );

  fn parse_one(
    session: &mut RespServerSession,
    buffer: &[u8],
  ) -> (Option<RespCommand>, Vec<Vec<u8>>) {
    session.recv_buffer.clear();
    session.recv_buffer.extend_from_slice(buffer);
    session.bytes_read = session.recv_buffer.len();
    session.read_head = 0;
    let cmd = session.parse_command();
    let args = (0..session.parse_state.count)
      .map(|i| session.parse_state.arg_in(&session.recv_buffer, i).to_vec())
      .collect();
    (cmd, args)
  }

  /// 主表 + 子命令表全量端到端 round-trip:每一条表项都构造真实 RESP 帧
  /// 解析,命中期望命令(二分可达性 + 名称/枚举映射的全量回归,不止抽样)
  #[test]
  fn every_table_entry_round_trips_via_parse() {
    let mut s = RespServerSession::default();

    // 主表:非父命令 `*2 NAME k` → 命令本身(1 个参数)
    for (name, cmd, has_subcommands) in PRIMARY_TABLE {
      if *has_subcommands {
        continue;
      }
      let frame = format!("*2\r\n${}\r\n{name}\r\n$1\r\nk\r\n", name.len());
      let (parsed, _) = parse_one(&mut s, frame.as_bytes());
      assert_eq!(parsed, Some(*cmd), "主表 {name} 解析不可达");
    }

    // 父命令 + 子命令:`*2 PARENT SUB` → 子命令
    let subtables: [ParentSubtable; 12] = [
      ("CLIENT", RespCommand::Client, CLIENT_SUBTABLE),
      ("CONFIG", RespCommand::Config, CONFIG_SUBTABLE),
      ("COMMAND", RespCommand::Command, COMMAND_SUBTABLE),
      ("ACL", RespCommand::Acl, ACL_SUBTABLE),
      ("SCRIPT", RespCommand::Script, SCRIPT_SUBTABLE),
      ("PUBSUB", RespCommand::Pubsub, PUBSUB_SUBTABLE),
      ("LATENCY", RespCommand::Latency, LATENCY_SUBTABLE),
      ("SLOWLOG", RespCommand::Slowlog, SLOWLOG_SUBTABLE),
      ("MEMORY", RespCommand::Memory, MEMORY_SUBTABLE),
      ("OBJECT", RespCommand::Object, OBJECT_SUBTABLE),
      ("CLUSTER", RespCommand::Cluster, CLUSTER_SUBTABLE),
      ("BITOP", RespCommand::Bitop, BITOP_SUBTABLE),
    ];
    for (parent_name, parent_cmd, table) in subtables {
      // 父命令项须在主表且带子命令标记
      let entry = lookup_primary(parent_name.as_bytes());
      assert_eq!(
        entry,
        Some((parent_cmd, true)),
        "主表缺父命令 {parent_name}"
      );
      for (sub_name, sub_cmd) in table {
        let frame = format!(
          "*2\r\n${}\r\n{parent_name}\r\n${}\r\n{sub_name}\r\n",
          parent_name.len(),
          sub_name.len()
        );
        let (parsed, _) = parse_one(&mut s, frame.as_bytes());
        assert_eq!(
          parsed,
          Some(*sub_cmd),
          "{parent_name} {sub_name} 解析不可达"
        );
      }
    }
  }
}
