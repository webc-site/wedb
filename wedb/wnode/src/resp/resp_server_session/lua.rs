//! Lua 脚本会话面（对标 libs/server/Lua/LuaCommands.cs 与 LuaRunner.cs 的
//! 会话侧投影：EVAL / EVALSHA / SCRIPT 接线、脚本窗口隔离、redis.call 重入
//! 适配器，及 no-script 位图源与脚本门）。

use std::{
  iter,
  mem::{self, size_of},
  sync::{Arc, OnceLock},
};

use wbase::{future::blocking_wait, map::HashMap as GxHashMap};
use wlua::{LuaCommands, LuaSessionContext, ScriptingApi};
use wresp::{
  catalog::{RespCommandFlags, try_get_resp_commands_info},
  cmd_strings::{self as cs},
  command::RespCommand,
  ext::RespVecExt,
  read::{ReplyError, parse_bulk_reply, parse_simple_reply},
};

use super::core::{REDIS_PROTOCOL_VERSION, RespServerSession};

impl RespServerSession {
  /// Lua 命令（EVAL / EVALSHA / SCRIPT 等）会话侧接线：构建 [`LuaSessionContext`] 并分派
  /// （输出缓冲为脚本期本地缓冲，结束后并入会话输出）。
  ///
  /// 脚本期 no-script 位图在本窗口挂载、结束摘除：C# 位图挂在内嵌 processor
  /// （SessionScriptCache 构造的独立 RespServerSession，仅承接脚本内
  /// redis.call）上，LuaRunner.cs:242 构造期挂载且常驻，外层连接会话位图
  /// 恒 null——主循环命令不受 no-script 门限；rust 无内嵌 processor，脚本
  /// 内 redis.call 经 [`RespScriptingApi`] 重入共享会话，以窗口式挂/摘承接
  /// 同一可观测语义。
  pub(super) fn run_lua_command(&mut self, cmd: RespCommand) -> bool {
    let Some(mut session_cache) = self.session_script_cache.take() else {
      // 单一 Lua 启用门（对标 libs/server/Lua/LuaCommands.cs:CheckLuaEnabled）：
      // session_script_cache 仅在 enable_lua 时创建，None 即未启用，回 RESP_ERR_LUA_DISABLED。
      self.abort_error_message(cs::RESP_ERR_LUA_DISABLED);
      return true;
    };
    self.attach_no_script_bitmap();
    let store_cache = Arc::clone(&self.store_script_cache);
    let args = self.collect_args();
    // 配置链取值先行克隆（api 的 &mut 借用窗口内不可再借 &self）。
    // LuaOptions 标量为主、allowed_functions 常空，克隆零/单次分配。
    let lua_options = self.lua_options.clone();
    // 脚本窗口隔离（C# 内嵌 processor 自带独立接收缓冲与应答暂存发送器的
    // 等价物）：redis.call 的合成 RESP 请求覆写会话接收窗，故外层批的接收
    // 游标与已产出应答先换出、窗口关闭原样挂回——否则同批 EVAL 之后尚未
    // 消费的命令随覆写凭空消失，前序命令的应答更会被 Lua 应答转换器误读出
    // 成本条 redis.call 的应答
    let outer_output = mem::take(&mut self.output);
    let outer_recv = mem::take(&mut self.recv_buffer);
    let outer_cursors = (self.read_head, self.end_read_head, self.bytes_read);
    // 共享会话窗口态快照：rust 无内嵌 processor，脚本内 setresp 与 SELECT
    // 直接落本会话字段（C# 落 SessionScriptCache 独立 processor 的独立会话，
    // 外层连接恒不变），入口保存、收尾在窗口关闭处原样恢复
    let outer_protocol_version = self.resp_protocol_version;
    let outer_active_db_id = self.active_db_id;
    let mut script_out = Vec::new();
    {
      let mut api = RespScriptingApi(&mut *self);
      let mut ctx = LuaSessionContext {
        args: &args,
        out: &mut script_out,
        session_cache: &mut session_cache,
        store_cache: &store_cache,
        session: &mut api,
        // 配置链装配（C# SessionScriptCache 装配面：LuaOptions 逐字段
        // 下传），不再写死 default。
        redis_version: REDIS_PROTOCOL_VERSION,
        lua_options: &lua_options,
      };
      match cmd {
        RespCommand::Eval => LuaCommands::try_eval(&mut ctx),
        RespCommand::Evalsha => LuaCommands::try_evalsha(&mut ctx),
        RespCommand::ScriptExists => LuaCommands::network_script_exists(&mut ctx),
        RespCommand::ScriptFlush => LuaCommands::network_script_flush(&mut ctx),
        RespCommand::ScriptLoad => LuaCommands::network_script_load(&mut ctx),
        _ => true,
      };
    }
    // 收尾不变式：脚本窗口退出时挂起态必为 None——每次重入的
    // [`RespScriptingApi::dispatch_resp`] 已就地承接两态并把应答并入脚本应答。
    // 残留挂起体若不摘除，网络泵会把它当成本连接的挂起命令 resolve，产出一帧
    // 不属于任何客户端命令的应答插进出网流（协议流错插的最后一道闸）；此处
    // 写明并就地取消，与 [`Self::dispose`] 同一取消口径
    if let Some(blocked) = self.take_blocked_wait() {
      log::error!("脚本窗口退出时残留阻塞挂起体，已就地取消");
      blocked.abort();
    }
    if self.take_slow_wait().is_some() {
      log::error!("脚本窗口退出时残留慢路径挂起体，已就地取消");
    }
    // 会话窗口挂回：外层游标与接收缓冲复位，水位让渡哨兵复位为派发前的
    // false（EVAL 能被派发即说明前序命令未触水位）
    self.recv_buffer = outer_recv;
    (self.read_head, self.end_read_head, self.bytes_read) = outer_cursors;
    self.output_watermark_yield = false;
    // 共享会话窗口态恢复（与入口快照同一处收尾）：协议版本弃脚本期窗口值
    // （setresp(3) 不穿透外层连接）；活动库经切库单点回写，连带复位脚本内
    // SELECT 移交的存储上下文（入口库恒已装载，set_context 必即时成功）
    self.resp_protocol_version = outer_protocol_version;
    _ = self.try_switch_active_database_session(outer_active_db_id);
    // 窗口缓冲整段弃用（C# 内嵌 processor 的 ScratchBufferNetworkSender 随窗口
    // 丢弃同款）：dispatch_resp 收尾已把窗口应答尽数冲入脚本应答，此处无可残留
    self.output = outer_output;
    // 脚本窗口关闭：外层连接命令恢复 no-script 门豁免（对齐 C# 外层会话
    // 位图恒 null）
    self.no_script_bitmap = None;
    self.session_script_cache = Some(session_cache);
    self.output.extend_from_slice(&script_out);
    true
  }

  /// 脚本期 no-script 位图静态源（C# LuaRunner.cs:148 NoScriptDetails，
  /// static readonly 单次构建；进程级缓存，挂载面零 Arc 借用）
  pub(crate) fn no_script_bitmap_source() -> (i32, &'static [u64]) {
    static SOURCE: OnceLock<(i32, Box<[u64]>)> = OnceLock::new();
    let (start, bitmap) = SOURCE.get_or_init(|| {
      let (start, bitmap) = Self::no_script_details();
      (start, bitmap.into_boxed_slice())
    });
    (*start, bitmap)
  }

  /// 挂载脚本期 no-script 位图（C# LuaRunner.cs:242：LuaRunner 构造期
  /// `(noScriptStart, noScriptBitmap) = NoScriptDetails` 的字段赋值动作；
  /// 挂/摘时机由 [`Self::run_lua_command` 的脚本窗口承接，见该处语义说明]）
  pub fn attach_no_script_bitmap(&mut self) {
    if self.no_script_bitmap.is_none() {
      let (start, bitmap) = Self::no_script_bitmap_source();
      self.no_script_start = start;
      self.no_script_bitmap = Some(bitmap);
    }
  }

  /// 子命令判别值 → 顶层命令判别值归一表（C# 门语义对齐：ProcessMessages
  /// 解析产出顶层命令（SCRIPT/ACL/CLUSTER 等的子命令在分派 handler 内二次
  /// 解析），CheckScriptPermissions 查顶层判别值——位图虽含子命令位（构建
  /// 端 InitializeNoScriptDetails 一并收集），顶层查询命中不了子命令位；
  /// rust 解析器直接产出子命令判别值，查位图前须归一，否则 SCRIPT|EXISTS
  /// 等被位图中的子命令位误拦）
  fn no_script_gate_cmd(cmd: RespCommand) -> u16 {
    static TABLE: OnceLock<GxHashMap<u16, u16>> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
      let Some(all_commands) = try_get_resp_commands_info(true) else {
        return GxHashMap::default();
      };
      all_commands
        .values()
        .flat_map(|info| {
          info
            .sub_commands
            .iter()
            .map(|sub| (sub.command as u16, info.command as u16))
        })
        .collect()
    });
    table.get(&(cmd as u16)).copied().unwrap_or(cmd as u16)
  }

  /// libs/server/Resp/RespServerSession.cs:CheckScriptPermissions（实现体
  /// AdminCommands.cs:95-115）
  ///
  /// 位图未挂载（本连接未进入过脚本期）恒放行，等价 C# noScriptBitmap ==
  /// null 路径；挂载后按 C# 字节粒度位检查（除数 8 字节而非 64 位，
  /// [`Self::no_script_details`] 构建端同款怪癖，两端一致故位序吻合）
  pub fn check_script_permissions(&self, cmd: RespCommand) -> bool {
    let Some(bitmap) = self.no_script_bitmap else {
      return true;
    };
    let ix = i32::from(Self::no_script_gate_cmd(cmd)) - self.no_script_start;
    if ix >= 0 {
      let word_ix = ix as usize / size_of::<u64>(); // C# sizeof(ulong) = 8
      if let Some(&word) = bitmap.get(word_ix)
        && word & (1_u64 << (ix as usize % size_of::<u64>())) != 0
      {
        // C# :108 OnACLOrNoScriptFailure：custom 命令环境态清理，rust
        // current_custom_command 在分派段之后才置位，门期无环境态可清
        return false;
      }
    }
    true
  }

  /// 构建 NoScript 命令集位图（对齐 LuaRunner InitializeNoScriptDetails 集合）
  ///
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:InitializeNoScriptDetails
  ///
  /// 从 [`wresp::catalog::try_get_resp_commands_info`]（externalOnly
  /// 口径）的 NoScript 标志动态构建：顶层命令与子命令的判别值升序铺开，
  /// 位图按字节粒度置位（C# `ulongIndex = stepped / sizeof(ulong)`、
  /// `bitIndex = stepped % sizeof(ulong)`，除数为 8 字节而非 64 位）。
  /// Garnet 命令数据无 FCALL/FCALL_RO/EVAL_RO/EVALSHA_RO/FUNCTION（Redis 侧
  /// 命令，Garnet RespCommandsInfo 未定义），故无对应位。
  pub fn no_script_details() -> (i32, Vec<u64>) {
    const BITS_PER_WORD: usize = size_of::<u64>(); // C# sizeof(ulong) = 8

    let Some(all_commands) = try_get_resp_commands_info(true) else {
      // 命令元数据导入失败（C# 抛 InvalidOperationException 路径）；数据为
      // 构建期内嵌资源，运行时不可达，按空位图退化
      return (0, vec![0]);
    };

    let mut no_script: Vec<u16> = all_commands
      .values()
      .flat_map(|info| iter::once(info).chain(info.sub_commands.iter()))
      .filter(|info| info.flags.intersects(RespCommandFlags::NO_SCRIPT))
      .filter(|info| info.command != RespCommand::None)
      .map(|info| info.command as u16)
      .collect();
    no_script.sort_unstable();
    no_script.dedup();

    let Some(&start) = no_script.first() else {
      return (0, vec![0]);
    };
    let end = no_script.last().copied().expect("非空");
    let size = (end - start) as usize + 1;
    let mut num_words = size / BITS_PER_WORD;
    // C# 上游怪癖：余数对 numULongs 而非字宽取模，1:1 保留
    if !size.is_multiple_of(num_words) {
      num_words += 1;
    }

    let mut bitmap = vec![0_u64; num_words];
    for discriminant in no_script {
      let stepped = (discriminant - start) as usize;
      bitmap[stepped / BITS_PER_WORD] |= 1_u64 << (stepped % BITS_PER_WORD);
    }

    (start as i32, bitmap)
  }
}

/// 会话的 [`ScriptingApi`] 适配器（redis.call 落地面）
///
/// C# ProcessCommandFromScripting 把参数格式化为 RESP 请求后重入内嵌
/// processor 的 TryConsumeMessages；rust 无内嵌 processor，经同一解析/分派
/// 路径重入共享会话，响应字节追加写入调用方传入的 `&mut Vec<u8>`。外层批的
/// 接收窗与输出缓冲由 [`RespServerSession::run_lua_command`] 在脚本窗口换出、
/// 会话脚本缓存同时摘除，故重入路径与脚本期借用互斥且外层字节不被覆写污染。
struct RespScriptingApi<'a>(&'a mut RespServerSession);

impl RespScriptingApi<'_> {
  /// 拼装 RESP 数组请求（数组头 + 命令 + 参数，单点经 RespVecExt）。
  fn resp_request(cmd: &[u8], args: &[&[u8]]) -> Vec<u8> {
    let mut request = Vec::new();
    request.write_resp_array_len(args.len() + 1);
    request.write_resp_bulk_string(cmd);
    for arg in args {
      request.write_resp_bulk_string(arg);
    }
    request
  }
}

impl ScriptingApi for RespScriptingApi<'_> {
  /// 分派 RESP 请求（对标 C# TryConsumeMessages；C# 的
  /// ScratchBufferNetworkSender 占位在 rust 无 INetworkSender 形状约束，
  /// 应答直写 Vec 缓冲）
  fn dispatch_resp(&mut self, request: &[u8], response: &mut Vec<u8>) {
    // 对标 C# LuaRunner.Functions.cs:ProcessCommandFromScripting 尾部
    // `respServerSession.TryConsumeMessages(request.ptr, request.length)`：
    // 脚本格式化缓冲切为接收内容重入消费装配（C# 的 recvBufferPtr 切到
    // reqBuffer + 入口 `if (!txnSkip) readHead = 0` 的游标归零）。C# 切的是
    // 内嵌 processor 的接收窗，外层批字节不受扰动；rust 重入共享会话，外层
    // 接收窗与已产出应答由 [`RespServerSession::run_lua_command`] 在脚本窗口
    // 换出、收尾挂回，此处只覆写窗口内的会话接收窗
    let session = &mut *self.0;
    session.recv_buffer.clear();
    session.recv_buffer.extend_from_slice(request);
    session.read_head = 0;
    session.end_read_head = 0;
    // 消费序与网络泵同构（drive.rs 泵循环的脚本重入投影）：消费 → 水位让渡
    // 冲出应答后续消费 → 挂起态就地驱动闭环。
    //
    // 挂起承接是 C# 重入语义的必需项：C# 侧脚本内命令的磁盘 pending 与阻塞
    // 等待都在 TryConsumeMessages 的调用栈上同步收割，应答齐了才返回，故 C#
    // 不存在「重入返回而命令仍挂起」的形态；rust 侧两态以会话挂起体承载，
    // 本函数返回前必须取走并驱动——留 pending 回会话即令本条 redis.call 拿
    // 空应答（resp_convert 落 UnexpectedError）且后续每条 redis.call 被消费
    // 入口门连锁挡回，残留挂起体更会被网络泵当作本连接的挂起 resolve，把一帧
    // 不属于任何客户端命令的应答插进 EVAL 之后的出网流
    loop {
      if session.try_consume_messages().is_none() {
        break;
      }
      // 批内输出水位让渡：应答先并入 response 再续消费——会话输出缓冲在下一
      // 批入口即被清空，不冲出即丢整段已产出应答（大应答脚本命令曾据此凭空
      // 截断）
      if session.take_output_watermark_yield() {
        session.take_output_into(response);
        continue;
      }
      let mut resumed = false;
      // 阻塞挂起承接：应答经泵同款并入口直写 response，不碰会话出网缓冲
      if let Some(blocked) = session.take_blocked_wait() {
        let (cmd, result) = blocking_wait(blocked.resolve());
        session.resolve_blocked_wait_into(cmd, result, response);
        resumed = true;
      }
      // 慢路径挂起承接（冷键降级 / 槽位门等待）
      if let Some(slow) = session.take_slow_wait() {
        let reply = blocking_wait(slow.resolve());
        session.resolve_slow_wait_into(&reply, response);
        resumed = true;
      }
      if !resumed {
        break;
      }
    }
    session.take_output_into(response);
  }

  /// GET 特例（C# api.GET）：RESP 请求闭环后解析批量串/null 应答
  ///（C# 为存储 API 直连，rust 重入会话解析面，须带完整数组头）
  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Vec<u8>> {
    let request = Self::resp_request(b"GET", &[key]);
    let mut response = Vec::new();
    self.dispatch_resp(&request, &mut response);
    parse_bulk_reply(&response)
      .map(|opt| opt.map(<[u8]>::to_vec))
      .map_err(reply_error_bytes)
  }

  /// SET 特例（C# api.SET）：+OK 或错误应答（同上，带完整数组头）
  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Vec<u8>> {
    let request = Self::resp_request(b"SET", &[key, value]);
    let mut response = Vec::new();
    self.dispatch_resp(&request, &mut response);
    parse_simple_reply(&response).map_err(reply_error_bytes)
  }

  fn resp_protocol_version(&self) -> u8 {
    self.0.resp_protocol_version
  }

  fn update_resp_protocol_version(&mut self, version: u8) {
    self.0.update_resp_protocol_version(version);
  }

  /// 独立缓冲解析（redis.acl_check_cmd 有效性判定的会话单点）
  fn parse_resp_command_buffer(&mut self, buffer: &[u8]) -> Option<RespCommand> {
    self.0.parse_resp_command_buffer(buffer)
  }

  /// ACL 位图门（Lua redis.call / redis.acl_check_cmd 路径，
  /// LuaRunner.Functions.cs:2993 / :2879）
  fn check_acl_permissions(&self, command: RespCommand) -> bool {
    self.0.acl_permits(command)
  }
}

/// 脚本快路径协议错误文案（应答非 `$`/`+`/`-` 帧，无真实错误文本可携）。
const ERR_SCRIPT_PROTOCOL: &[u8] = b"protocol error";

/// [`wresp::read::ReplyError`] → [`ScriptingApi`] 错误面映射：错误帧透传真实
/// 文本（'-' 头与 CRLF 已剥离，与 fallback 臂 RESP 错误帧的 Lua 错误载荷同
/// 形态，脚本错误不再降级静态串）
fn reply_error_bytes(err: ReplyError<'_>) -> Vec<u8> {
  match err {
    ReplyError::ErrorReply(text) => text.to_vec(),
    ReplyError::Malformed => ERR_SCRIPT_PROTOCOL.to_vec(),
  }
}
