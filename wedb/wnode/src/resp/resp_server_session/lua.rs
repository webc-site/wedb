//! Lua 脚本会话面（对标 libs/server/Lua/LuaCommands.cs 与 LuaRunner.cs 的
//! 会话侧投影：EVAL / EVALSHA / SCRIPT 接线、脚本窗口隔离、redis.call 重入
//! 适配器，及 no-script 位图源与脚本门）。
//!
//! 挂起承接（协程化）：脚本执行在 Luau 协程上续跑（wlua 执行器
//! lua_newthread + lua_resume），脚本内 redis.call 命中阻塞/慢路径时挂起体
//! 让渡本会话挂起槽（[`Self::park_script_suspend`]），redis.call 收尾读
//! 让渡标记挂起协程——VM 同步绑定内零内联收割（`inline_wait` 已清除）。
//! 网络泵消费返回后经 [`Self::resume_suspended_script`] await 驱动挂起体，
//! 应答转换值回填协程续跑至脚本完成（C# 回调栈同步收割的协程对位）。

use std::{
  mem::{self, size_of},
  sync::Arc,
};

use wlua::{LuaCommands, LuaSessionContext, ScriptApiError, ScriptingApi};
use wresp::{
  cmd_strings::{self as cs},
  command::RespCommand,
  ext::RespVecExt,
  read::{ReplyError, parse_bulk_reply, parse_simple_reply},
};

use super::core::{REDIS_PROTOCOL_VERSION, RespServerSession, collect_arg_views};
use crate::resp::{BlockedWait, slow_path::SlowWait};

/// 脚本挂起让渡标记（`#[repr(i32)]` 协议值：dispatch_resp 命中挂起体时经
/// [`RespServerSession::park_script_suspend`] 置入让渡槽，redis.call 收尾
/// 压协程栈作让渡值，外层续跑循环解码分派挂起体驱动臂）。
#[repr(i32)]
#[derive(Debug, Copy, PartialEq, Eq, Clone)]
pub(crate) enum ScriptYieldTag {
  /// 阻塞挂起（BLPOP/BRPOP 等经经纪等待的命令）
  Blocked = 1,
  /// 慢路径挂起（冷键降级 / 槽位门等待 / 冷上下文装载）
  Slow = 2,
}

/// 脚本内挂起态（协程挂起窗口的会话侧承载：挂起体 + 挂起期脚本窗口值；
/// 分派标记以协程栈让渡值为单一真源，此处不重复持有）。
pub(crate) struct ScriptSuspend {
  /// 阻塞挂起体（标记 = Blocked 时非空；dispose/兜底复位臂就地取消）
  pub(crate) blocked: Option<BlockedWait>,
  /// 慢路径挂起体（标记 = Slow 时非空）
  slow: Option<SlowWait>,
  /// 挂起期脚本窗口协议版本（setresp 窗口值；续跑窗口重开时回贴会话，
  /// 挂起体应答的成帧版本与 redis.call 应答转换版本同源）
  protocol_version: u8,
}

impl ScriptYieldTag {
  /// 协议值解码（协程栈让渡值 → 标记；未知值 None）。
  fn from_i32(value: i32) -> Option<Self> {
    if value == Self::Blocked as i32 {
      Some(Self::Blocked)
    } else if value == Self::Slow as i32 {
      Some(Self::Slow)
    } else {
      None
    }
  }
}

impl RespServerSession {
  /// 脚本挂起让渡登记（协程化挂起协议，dispatch_resp 消费命中后调用）：
  /// 挂起体移出泵可见槽入本槽（泵同款挂起分支不会误驱动），置让渡标记供
  /// redis.call 收尾读取
  pub(crate) fn park_script_suspend(
    &mut self,
    tag: ScriptYieldTag,
    blocked: Option<BlockedWait>,
    slow: Option<SlowWait>,
  ) {
    self.script_yield_tag = Some(tag as i32);
    self.script_suspend = Some(ScriptSuspend {
      blocked,
      slow,
      protocol_version: self.resp_protocol_version,
    });
  }

  /// 是否有脚本内挂起（网络泵消费返回后的续跑探测口）。
  pub fn has_script_suspend(&self) -> bool {
    self.script_suspend.is_some()
  }

  /// 挂起脚本续跑（协程化承接的 async 驱动口，网络泵在消费返回后调用）：
  /// 循环「await 驱动挂起体 → 应答转换值回填 → resume 协程」至脚本完成
  /// （先驱动后 resume：挂起必有未决命令，redis.call 的返回值不得空拍回填）。
  /// 挂起体应答经 `resolve_blocked_wait_into` / `resolve_slow_wait_into`
  /// 写中间缓冲后由 wlua 转 Lua 值压协程栈——`resolve_*_into` 的调用点自
  /// 内联收割位外提至此（await 之后）。
  ///
  /// 脚本余量应答直入 `resp_buf`（与消费返回时已冲出的前序应答同一流水线
  /// 通道）：挂起即让渡窗口，前序应答的出网通道由驱动方承接，脚本余量
  /// 回流会话输出会令其错位到前序应答之后（同批 EVAL 夹普通命令的对齐
  /// 回归根因）。首段挂起前的部分脚本应答仍随首段窗口关闭并入会话输出
  /// （彼时尚未冲出，顺序无扰）。
  ///
  /// 脚本窗口镜像 [`Self::run_lua_command`] 两段式换出（挂起即让渡窗口，
  /// 此处重开续段）；窗口态恢复口径与首段一致。
  pub async fn resume_suspended_script(&mut self, resp_buf: &mut Vec<u8>) {
    let Some(mut suspend) = self.script_suspend.take() else {
      return;
    };
    let Some(mut session_cache) = self.session_script_cache.take() else {
      // 缓存缺失（理论不可达：挂起态与缓存同生命周期）：就地取消挂起体防挂死
      log::error!("脚本挂起续跑时会话脚本缓存缺失，挂起体已就地取消");
      if let Some(blocked) = suspend.blocked.take() {
        blocked.abort();
      }
      return;
    };
    self.attach_no_script_bitmap();
    let store_cache = Arc::clone(&self.store_script_cache);
    // 接收缓冲与游标换出（redis.call 重入覆写窗口，同首段口径）
    let outer_recv = mem::take(&mut self.recv_buffer);
    let lua_options = self.lua_options.clone();
    let outer_output = mem::take(&mut self.output);
    let outer_cursors = (self.read_head, self.end_read_head, self.bytes_read);
    let outer_protocol_version = self.resp_protocol_version;
    let outer_active_db_id = self.active_db_id;
    // 挂起期协议窗口值回贴（setresp 的脚本期窗口值跨挂起有效；挂起体应答
    // 成帧与 redis.call 应答转换共用）
    self.resp_protocol_version = suspend.protocol_version;

    let mut script_out = Vec::new();
    // 挂起体应答中间缓冲（await 后经 resolve_*_into 写入，转换后复用清零）
    let mut reply_buf = Vec::new();
    // 循环顶统一驱动：首段挂起必有未决命令（先驱动后 resume，redis.call 的
    // 返回值不得空拍回填）；续跑再挂起由返回标记分派（标记与挂起体同源，
    // 单一真源）
    let mut pending = Some(if suspend.blocked.is_some() {
      ScriptYieldTag::Blocked
    } else {
      ScriptYieldTag::Slow
    });
    loop {
      // 挂起体驱动：await 至应答就绪，写回应答（外层承接）
      let reply: (&[u8], u8) = match pending {
        Some(ScriptYieldTag::Blocked) => {
          let Some(mut blocked) = suspend.blocked.take() else {
            log::error!("阻塞挂起标记与挂起体不配对，脚本续跑中止");
            break;
          };
          reply_buf.clear();
          let (cmd, result) = blocked.resolve().await;
          // 不入账承接（内嵌字节不入外层活跃面，C# 内嵌 processor 私有计数
          // 随 SessionScriptCache.Dispose 只入 history 的对位形）
          self.resolve_blocked_wait_into(cmd, result, &mut reply_buf, false);
          (reply_buf.as_slice(), suspend.protocol_version)
        }
        Some(ScriptYieldTag::Slow) => {
          let Some(slow) = suspend.slow.take() else {
            log::error!("慢路径挂起标记与挂起体不配对，脚本续跑中止");
            break;
          };
          reply_buf.clear();
          let reply_bytes = slow.resolve().await;
          // 不入账承接（同上：脚本内挂起体应答字节不入外层活跃面）
          self.resolve_slow_wait_into(&reply_bytes, &mut reply_buf, false);
          (reply_buf.as_slice(), suspend.protocol_version)
        }
        None => unreachable!("循环顶恒有挂起体待驱动"),
      };
      // 应答转换值回填协程并续跑（完成或再次挂起）；借用舞步：ctx（会话
      // 可变借用）仅覆盖同步 resume 步，await 段归还
      let phase = {
        let mut api = RespScriptingApi(&mut *self);
        let mut ctx = LuaSessionContext {
          args: &[],
          out: &mut script_out,
          session_cache: &mut session_cache,
          store_cache: &store_cache,
          session: &mut api,
          redis_version: REDIS_PROTOCOL_VERSION,
          lua_options: &lua_options,
        };
        LuaCommands::continue_execute_script(&mut ctx, reply)
      };
      pending = phase.and_then(ScriptYieldTag::from_i32);
      if pending.is_none() {
        break;
      }
      if let Some(next) = self.script_suspend.take() {
        suspend = next;
      }
    }

    // 窗口关闭（镜像首段收尾）：游标与接收缓冲复位、水位哨兵复位、会话
    // 窗口态恢复；脚本余量应答直入驱动方 resp_buf（窗口换出的残余输出若
    // 有也先行按序冲出，两者同通道同序）
    self.recv_buffer = outer_recv;
    (self.read_head, self.end_read_head, self.bytes_read) = outer_cursors;
    self.output_watermark_yield = false;
    self.resp_protocol_version = outer_protocol_version;
    _ = self.try_switch_active_database_session(outer_active_db_id);
    self.output = outer_output;
    self.no_script_bitmap = None;
    self.session_script_cache = Some(session_cache);
    // 脚本余量直入驱动方 resp_buf（与消费返回时已冲出的前序应答同通道同
    // 序）：回流会话输出会令其错位到前序应答之后；窗口换出的残余输出若
    // 有也先行按序冲出
    self.take_output_into(resp_buf, true);
    let residual_len = script_out.len();
    resp_buf.extend_from_slice(&script_out);
    // 余量即最终 EVAL 应答（外层连接应出向字节）：绕过冲出口直写驱动
    // 缓冲，经 account_output 单点补一次入账（C# 外层 Send 对最终应答
    // 计一次的对位；窗内内嵌应答走 dispatch_resp 不入账臂，此处不重复）
    self.account_output(residual_len as u64);
  }

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
    // 前一挂起未续跑的兜底复位（网络泵路径不可达——挂起后消费返回即续跑；
    // 非驱动型消费者路径防御）：挂起体就地取消，挂起协程随 runner 复位
    if let Some(suspend) = self.script_suspend.take() {
      log::error!("前一脚本挂起未续跑，挂起体已就地取消");
      if let Some(blocked) = suspend.blocked {
        blocked.abort();
      }
      self.script_yield_tag = None;
      LuaCommands::abort_suspended_script(&mut session_cache);
    }
    self.attach_no_script_bitmap();
    let store_cache = Arc::clone(&self.store_script_cache);
    // 接收缓冲换出提前：参数切片视图借局部 outer_recv（脚本窗口重入覆写的
    // 是会话侧已换出的空缓冲，outer_recv 不被触碰），视图借用在 &mut self
    // 会话窗口之外，物理可行且零字节拷贝
    let outer_recv = mem::take(&mut self.recv_buffer);
    // 参数视图栈上收集（SmallVec 内联，对标 C# parseState.GetArgSliceByRef
    // 零拷贝直通；借用收集单点 collect_arg_views，N+1 堆分配自此消除）
    let args = collect_arg_views(&self.parse_state, &outer_recv);
    // 配置链取值先行克隆（api 的 &mut 借用窗口内不可再借 &self）。
    // LuaOptions 标量为主、allowed_functions 常空，克隆零/单次分配。
    let lua_options = self.lua_options.clone();
    // 脚本窗口隔离（C# 内嵌 processor 自带独立接收缓冲与应答暂存发送器的
    // 等价物）：redis.call 的合成 RESP 请求覆写会话接收窗，故外层批的接收
    // 游标与已产出应答先换出、窗口关闭原样挂回——否则同批 EVAL 之后尚未
    // 消费的命令随覆写凭空消失，前序命令的应答更会被 Lua 应答转换器误读出
    // 成本条 redis.call 的应答
    let outer_output = mem::take(&mut self.output);
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
    // 参数视图表先于接收缓冲挂回显式终结（SmallVec 带 Drop，视图借用随之
    // 释放，零成本；此后接收缓冲可安全移回会话）
    drop(args);
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

  /// 脚本期 no-script 位图静态源（C# LuaRunner.cs:148 NoScriptDetails 的
  /// 编译期对位：位图与起点 const 定型，零运行时构建、零锁、零 Arc 借用）
  pub(crate) fn no_script_bitmap_source() -> (i32, &'static [u64]) {
    (NO_SCRIPT_START, &NO_SCRIPT_BITMAP)
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

  /// 位图未挂载（本连接未进入过脚本期）恒放行，等价 C# noScriptBitmap ==
  /// null 路径；挂载后按 C# 字节粒度位检查（除数 8 字节而非 64 位，
  /// NO_SCRIPT 表端同款怪癖，两端一致故位序吻合）。判别值取解析产出原值
  /// 直查（C# AdminCommands.cs:99 `(int)cmd - noScriptStart`，cmd 恒为
  /// HandleSubcommandLookup 产出的子命令判别值，无归一步）——子命令族
  /// （SCRIPT|* / ACL|* / CLIENT|* 等）判别值本身在位图置位，归一到父
  /// 命令反使父判别值（未置位）放行，子命令族拦截全失效
  pub fn check_script_permissions(&self, cmd: RespCommand) -> bool {
    let Some(bitmap) = self.no_script_bitmap else {
      return true;
    };
    let ix = i32::from(cmd as u16) - self.no_script_start;
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
}

// ---------- NoScript 编译期位图（C# LuaRunner.cs:2634 InitializeNoScriptDetails 的 const 对位） ----------

/// NoScript 命令判别值清单（升序去重；1:1 对齐
/// wresp/RespCommandsInfo.json externalOnly 口径的 NoScript 标志集——顶层
/// 命令与子命令一并收集，SLAVEOF/SECONDARYOF 同判别值单条。本清单与 JSON
/// 目录是双真源，漂移由 wnode/tests/no_script_gate_tests.rs 全量对拍测试
/// 锁死：改目录 NoScript 标志必须同步本表，反之亦然）
const NO_SCRIPT_COMMANDS: &[RespCommand] = &[
  // 根命令（判别值 177..=370 升序段一：SUBSCRIBE 族 / WATCH 族 / 脚本族）
  RespCommand::Spublish,
  RespCommand::Ssubscribe,
  RespCommand::Watch,
  RespCommand::Watchms,
  RespCommand::Watchos,
  RespCommand::Eval,
  RespCommand::Evalsha,
  RespCommand::Async,
  RespCommand::Subscribe,
  RespCommand::Psubscribe,
  RespCommand::Unsubscribe,
  RespCommand::Punsubscribe,
  // 子命令 CLIENT|ID..CLIENT|UNBLOCK（判别值 245..=252 连续段）
  RespCommand::ClientId,
  RespCommand::ClientInfo,
  RespCommand::ClientList,
  RespCommand::ClientKill,
  RespCommand::ClientGetname,
  RespCommand::ClientSetname,
  RespCommand::ClientSetinfo,
  RespCommand::ClientUnblock,
  RespCommand::Monitor,
  RespCommand::Multi,
  RespCommand::Exec,
  RespCommand::Discard,
  RespCommand::Unwatch,
  RespCommand::Runtxp,
  RespCommand::Replicaof,
  RespCommand::Secondaryof,
  RespCommand::Role,
  RespCommand::Save,
  RespCommand::Expdelscan,
  RespCommand::Bgsave,
  RespCommand::Commitaof,
  RespCommand::Failover,
  // 子命令 SCRIPT|EXISTS..SCRIPT|LOAD（判别值 280..=282 连续段）
  RespCommand::ScriptExists,
  RespCommand::ScriptFlush,
  RespCommand::ScriptLoad,
  // 子命令 ACL|CAT..ACL|WHOAMI（判别值 284..=293 连续段）
  RespCommand::AclCat,
  RespCommand::AclDeluser,
  RespCommand::AclGenpass,
  RespCommand::AclGetuser,
  RespCommand::AclList,
  RespCommand::AclLoad,
  RespCommand::AclSave,
  RespCommand::AclSetuser,
  RespCommand::AclUsers,
  RespCommand::AclWhoami,
  // 子命令 CONFIG|GET..CONFIG|SET（判别值 304..=306 连续段）
  RespCommand::ConfigGet,
  RespCommand::ConfigRewrite,
  RespCommand::ConfigSet,
  RespCommand::Debug,
  // 子命令 LATENCY|HELP..LATENCY|RESET（判别值 309..=311 连续段）
  RespCommand::LatencyHelp,
  RespCommand::LatencyHistogram,
  RespCommand::LatencyReset,
  // 子命令 CLUSTER|* 的 NoScript 集（判别值 320..=366 升序散布）
  RespCommand::ClusterAdvanceTime,
  RespCommand::ClusterAppendlog,
  RespCommand::ClusterAttachSync,
  RespCommand::ClusterBanlist,
  RespCommand::ClusterBeginReplicaRecover,
  RespCommand::ClusterDelkeysinslot,
  RespCommand::ClusterDelkeysinslotrange,
  RespCommand::ClusterEndpoint,
  RespCommand::ClusterFailreplicationoffset,
  RespCommand::ClusterFailstopwrites,
  RespCommand::ClusterFlushall,
  RespCommand::ClusterFlushallNs,
  RespCommand::ClusterGossip,
  RespCommand::ClusterHelp,
  RespCommand::ClusterInitiateReplicaSync,
  RespCommand::ClusterMigrate,
  RespCommand::ClusterMlogKeyTime,
  RespCommand::ClusterMtasks,
  RespCommand::ClusterMyparentid,
  RespCommand::ClusterPublish,
  RespCommand::ClusterSpublish,
  RespCommand::ClusterReserve,
  RespCommand::ClusterReset,
  RespCommand::ClusterSendCkptFileSegment,
  RespCommand::ClusterSendCkptMetadata,
  RespCommand::ClusterSetslotsrange,
  RespCommand::ClusterSlotstate,
  RespCommand::ClusterSnapshotData,
  RespCommand::ClusterSync,
  // 根命令尾段（判别值 367..=370：AUTH/HELLO/QUIT + rust 追加位 SUNSUBSCRIBE）
  RespCommand::Auth,
  RespCommand::Hello,
  RespCommand::Quit,
  RespCommand::Sunsubscribe,
];

/// 位图起点（C# `start = noScript[0]`，即清单首判别值）
const NO_SCRIPT_START: i32 = NO_SCRIPT_COMMANDS[0] as u16 as i32;

/// 位图终点（C# `end = noScript[^1]`，即清单尾判别值）
const NO_SCRIPT_END: u16 = NO_SCRIPT_COMMANDS[NO_SCRIPT_COMMANDS.len() - 1] as u16;

/// 判别值跨度（C# `size = end - start + 1`）
const NO_SCRIPT_SPAN: usize = (NO_SCRIPT_END as usize) - (NO_SCRIPT_START as usize) + 1;

/// C# 位粒度怪癖：置位除数为 `sizeof(ulong)` 字节数而非 64 位，构建端
/// （本表）与查询端（[`RespServerSession::check_script_permissions`]）一致
const NO_SCRIPT_BITS_PER_WORD: usize = size_of::<u64>();

/// 字数（C# 怪癖 1:1：`numULongs = size / sizeof(ulong)`，余数对 numULongs
/// 而非字宽取模后补一字；跨度小于单字时字数为 0，取模除零在 const 求值
/// 即编译红——跨度恒由清单编译期锚定，此处不设恒真断言）
const fn no_script_word_count() -> usize {
  let mut words = NO_SCRIPT_SPAN / NO_SCRIPT_BITS_PER_WORD;
  if !NO_SCRIPT_SPAN.is_multiple_of(words) {
    words += 1;
  }
  words
}

/// 位图（下标 = (判别值 - start) / 8；const 派生零运行时构建，越界置位在
/// const 求值即编译红）
const NO_SCRIPT_BITMAP: [u64; no_script_word_count()] = build_no_script_bitmap();

/// [`NO_SCRIPT_BITMAP`] 构建端（const 求值）
const fn build_no_script_bitmap() -> [u64; no_script_word_count()] {
  let mut bitmap = [0_u64; no_script_word_count()];
  let mut i = 0;
  while i < NO_SCRIPT_COMMANDS.len() {
    let stepped = (NO_SCRIPT_COMMANDS[i] as u16 as usize) - (NO_SCRIPT_START as usize);
    bitmap[stepped / NO_SCRIPT_BITS_PER_WORD] |= 1_u64 << (stepped % NO_SCRIPT_BITS_PER_WORD);
    i += 1;
  }
  bitmap
}

// 编译期不变式：清单升序无重 + 位计数对齐（丢位/越界/重号即编译红）
const _: () = {
  let mut i = 1;
  while i < NO_SCRIPT_COMMANDS.len() {
    assert!(
      (NO_SCRIPT_COMMANDS[i - 1] as u16) < (NO_SCRIPT_COMMANDS[i] as u16),
      "NoScript 清单须判别值升序去重"
    );
    i += 1;
  }
  let mut bits = 0;
  let mut w = 0;
  while w < NO_SCRIPT_BITMAP.len() {
    bits += NO_SCRIPT_BITMAP[w].count_ones() as usize;
    w += 1;
  }
  assert!(
    bits == NO_SCRIPT_COMMANDS.len(),
    "NoScript 位图置位数与清单条数不符"
  );
  assert!(
    (NO_SCRIPT_END as usize) - (NO_SCRIPT_START as usize)
      < NO_SCRIPT_BITMAP.len() * NO_SCRIPT_BITS_PER_WORD,
    "NoScript 尾判别值越出位图"
  );
};

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

  /// 窗内改权收口错误帧判别（`-<ACL_CHANGED_TEXT>\r\n` 整帧等值；该帧唯一
  /// 写点为 [`Self::dispatch_resp`] 环尾停车臂）。快路 get/set 据此报
  /// [`ScriptApiError::AclChanged`]，不经 §97 折叠臂、不落假协议违规。
  fn is_acl_park_reply(response: &[u8]) -> bool {
    let text = ScriptApiError::ACL_CHANGED_TEXT.as_bytes();
    response.len() == text.len() + 3
      && response[0] == b'-'
      && response[1..response.len() - 2] == *text
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
    // 冲出应答后续消费 → 挂起态让渡。
    //
    // 挂起承接（协程化）：C# 侧脚本内命令的磁盘 pending 与阻塞等待都在
    // TryConsumeMessages 的调用栈上同步收割（网络线程 BlockingWait 内联），
    // 应答齐了才返回；rust 侧 VM 同步绑定内不可跨 await，命中挂起体即移入
    // 脚本挂起让渡槽（[`RespServerSession::park_script_suspend`]，泵可见槽
    // 已空不会被误驱动）并终止消费——redis.call 收尾（wlua settle_script_call）
    // 读让渡标记挂起协程，网络泵在消费返回后经
    // [`RespServerSession::resume_suspended_script`] await 驱动挂起体，
    // 应答转换值回填协程续跑。留 pending 回会话即令本条 redis.call 拿空
    // 应答且后续每条 redis.call 被消费入口门连锁挡回，残留挂起体更会被
    // 网络泵当作本连接的挂起 resolve，把一帧不属于任何客户端命令的应答
    // 插进 EVAL 之后的出网流
    loop {
      if session.try_consume_messages().is_none() {
        break;
      }
      // 批内输出水位让渡：应答先并入 response 再续消费——会话输出缓冲在下一
      // 批入口即被清空，不冲出即丢整段已产出应答（大应答脚本命令曾据此凭空
      // 截断）。不入账（C# 内嵌 processor 应答只进 ScratchBufferNetworkSender
      // 与私有计数句柄，不入外层连接活跃面，SessionScriptCache.cs:24-26/:64）
      if session.take_output_watermark_yield() {
        session.take_output_into(response, false);
        continue;
      }
      // 阻塞挂起让渡：挂起命令本批无应答产出，应答缓冲清零复用，
      // redis.call 的实际应答由外层续跑回填
      if let Some(blocked) = session.take_blocked_wait() {
        session.park_script_suspend(ScriptYieldTag::Blocked, Some(blocked), None);
        response.clear();
        return;
      }
      // 慢路径挂起让渡（冷键降级 / 槽位门等待）
      if let Some(slow) = session.take_slow_wait() {
        session.park_script_suspend(ScriptYieldTag::Slow, None, Some(slow));
        response.clear();
        return;
      }
      break;
    }
    // dispatch 收尾臂冲出（同水位臂不入账：本 response 即本条 redis.call 的
    // 内嵌应答，随转换值消亡，唯最终 EVAL 应答经批尾冲出一次入账）
    session.take_output_into(response, false);
    // 窗内改权停车收口（deviations §159）：门链 Parked 臂已回退游标、本条
    // 零存储执行且无应答产出——空应答旧形在 fallback 折 nil 污染脚本、在
    // GET/SET 快路折成假协议违规，此处改写成专属错误帧，三消费面统一以
    // 确定性 Lua 错误中断脚本。判据为无副作用现读（不动 pending_acl_refresh，
    // EVAL 收尾泵仍即时刷新）；有真实应答产出时（窗口内竞态 bump）不叠加
    if response.is_empty() && session.acl_refresh_park_needed() {
      cs::write_error_raw(response, ScriptApiError::ACL_CHANGED_TEXT);
    }
  }

  /// 脚本挂起让渡标记读取（协程化挂起协议；读取即复位）
  fn take_script_yield(&mut self) -> Option<i32> {
    self.0.script_yield_tag.take()
  }

  /// GET 特例（C# api.GET）：RESP 请求闭环后解析批量串/null 应答
  ///（C# 为存储 API 直连，rust 重入会话解析面，须带完整数组头；
  /// 错误帧形态经 [`ScriptApiError`] 上交，由快路径按 C# status 语义折叠）
  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ScriptApiError> {
    let request = Self::resp_request(b"GET", &[key]);
    let mut response = Vec::new();
    self.dispatch_resp(&request, &mut response);
    if Self::is_acl_park_reply(&response) {
      return Err(ScriptApiError::AclChanged);
    }
    parse_bulk_reply(&response)
      .map(|opt| opt.map(<[u8]>::to_vec))
      .map_err(script_api_error)
  }

  /// SET 特例（C# api.SET）：+OK 或错误应答（同上，带完整数组头，
  /// 错误帧形态同 GET）
  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), ScriptApiError> {
    let request = Self::resp_request(b"SET", &[key, value]);
    let mut response = Vec::new();
    self.dispatch_resp(&request, &mut response);
    if Self::is_acl_park_reply(&response) {
      return Err(ScriptApiError::AclChanged);
    }
    parse_simple_reply(&response).map_err(script_api_error)
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

  /// 窗内改权预门（deviations §159）：直读跨连接收敛预门同一判据，
  /// 无副作用（不置 pending、不动游标）
  fn acl_mount_stale(&self) -> bool {
    self.0.acl_refresh_park_needed()
  }
}

/// [`wresp::read::ReplyError`] → [`ScriptApiError`] 形态映射：错误帧只上交
/// 拒绝形态（C# 存储 API 直连下 status 对脚本不可见，折叠归快路径单点）；
/// 不可解析应答归协议级错误（无 C# 对应态，wlua 侧保持上抛）。
fn script_api_error(err: ReplyError<'_>) -> ScriptApiError {
  match err {
    ReplyError::ErrorReply(_) => ScriptApiError::ErrorReply,
    ReplyError::Malformed => ScriptApiError::Protocol,
  }
}
