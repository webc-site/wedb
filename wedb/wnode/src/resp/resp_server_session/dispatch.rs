//! 分派器（对标 libs/server/Resp/RespServerSession.cs:ProcessBasicCommands /
//! ProcessArrayCommands / ProcessOtherCommands 三段链 + 经 [`GarnetApi`]
//! 注入面的存储分派与 AUTH/HELLO/ACL 停车预筛）。消费主循环见
//! [`super::consume`]。

use std::{fmt, mem};

use itoa::Buffer;
use wbase::time::now_nanos;
use wmetric::{GarnetInfoMetrics, GarnetServerMonitor, InfoCommand};
use wresp::{
  cmd_strings::{self as cs},
  command::{RespCommand, is_cluster_sub_command},
  ext::{RespSliceExt, RespVecExt},
};
use wtxn::TxnState;

use super::{
  auth::RESP_ERR_AUTH_IN_MULTI,
  core::{RespServerSession, collect_arg_views},
};
use crate::resp::{
  basic_commands::parse_hello_args,
  garnet_api::is_acl_command,
  info_provider::{SessionInfoSource, info_scan_section},
};

/// 存储执行域未挂载时的拒绝文案（C# 构造必带 storeWrapper 无此态；
/// rust 侧为宿主装配缺口的显式防线）
const ERR_STORE_DOMAIN_NOT_ATTACHED: &str = "ERR store execution domain not attached";

/// TIME 应答 ns→秒/微秒换算刻度（编译期单源，杜绝 1e9 双字面量）
const NANOS_PER_SEC: u64 = 1_000_000_000;

/// TIME 应答微秒段定宽位数（C# utcTime.ToString("ffffff") 补零宽度）
const MICROS_DIGITS: usize = 6;

impl RespServerSession {
  /// libs/server/Resp/RespServerSession.cs:ProcessBasicCommands
  ///
  /// fast 命令族分派（WARNING: 仅 @fast 命令，慢命令走 OtherCommands）。
  /// 命令实现位于 resp 命令文件（并行域），经 [`GarnetApi`] 注入面
  /// 接入；PING/ASKING/QUIT/事务族在会话侧闭环（分派臂只转调，单一实现
  /// 在 basic_commands）。
  pub fn process_basic_commands(&mut self, cmd: RespCommand) -> bool {
    match cmd {
      RespCommand::Ping => {
        // C# RespServerSession.cs:855：PING→NetworkPING/NetworkArrayPING
        //（帧字面量单点在 basic_commands 方法体的 cs 常量）
        let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
        let r = Self::network_ping_impl(
          self.is_subscription_session,
          self.resp_protocol_version,
          &args,
          &mut self.output,
        );
        matches!(r, Ok(true))
      }
      RespCommand::Asking => {
        // C# RespServerSession.cs:856：ASKING→NetworkASKING
        self.network_asking_to_output()
      }
      RespCommand::Quit => {
        self.to_dispose = true;
        self.output.extend_from_slice(cs::RESP_OK);
        true
      }
      // C# NetworkREADONLY
      RespCommand::Readonly => self.network_readonly(),
      // C# NetworkREADWRITE
      RespCommand::Readwrite => self.network_readwrite(),
      // C# ProcessBasicCommands switch：MULTI / EXEC / DISCARD / UNWATCH / RUNTXP
      RespCommand::Multi => self.network_multi(),
      RespCommand::Exec => self.network_exec(),
      RespCommand::Discard => self.network_discard(),
      RespCommand::Unwatch => self.network_unwatch(),
      // C# ProcessBasicCommands：RUNTXP
      //（libs/server/Resp/RespServerSession.cs:RespCommand.RUNTXP）
      RespCommand::Runtxp => self.network_runtxp(),
      // C# 链式回退：fast 表未命中的命令继续走 array → other 分派链
      _ => self.process_array_commands(cmd),
    }
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessArrayCommands
  ///
  /// @fast 数组族会话级命令（WARNING: 仅 @fast，慢命令走 OtherCommands）；
  /// 存储面数组命令经 [`GarnetApi`] 注入面承接。
  pub fn process_array_commands(&mut self, cmd: RespCommand) -> bool {
    match cmd {
      // C# ProcessArrayCommands：WATCH / WATCHMS / WATCHOS
      RespCommand::Watch => self.network_watch(),
      RespCommand::Watchms => self.network_watch_ms(),
      RespCommand::Watchos => self.network_watch_os(),
      // 发布订阅族（C# ProcessArrayCommands 的 pub/sub 段；wire 为会话自持）
      RespCommand::Ssubscribe | RespCommand::Spublish => self.process_pubsub_command(cmd, true),
      RespCommand::Subscribe
      | RespCommand::Psubscribe
      | RespCommand::Unsubscribe
      | RespCommand::Punsubscribe
      | RespCommand::Sunsubscribe
      | RespCommand::Publish
      | RespCommand::PubsubChannels
      | RespCommand::PubsubNumsub
      | RespCommand::PubsubNumpat
      | RespCommand::PubsubShardchannels
      | RespCommand::PubsubShardnumsub => self.process_pubsub_command(cmd, false),
      // C# 链式回退末端：未归类命令走 other（慢命令）分派
      _ => self.process_other_commands(cmd),
    }
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessOtherCommands
  ///
  /// 慢命令族分派（此处可安全放 @slow 命令）。C# ProcessOtherCommands :1065
  /// 入口 `containsSlowCommand = true` 的结构对位：会话侧闭环臂
  ///（[`Self::process_other_session_commands`]）命中即在本单点置位后返回，
  /// 新增会话臂落入伞内自动覆盖，杜绝逐臂补丁漏标；存储命令穿透尾部
  /// [`Self::dispatch_via_garnet_api`]（fast 段直通不置位，slow 段由
  /// `dispatch_slow` 入口既有置位承接——布尔写幂等，与 C# Other/Admin 两点
  /// 入口同义），段位判定与 C# 三段 switch 等效
  pub fn process_other_commands(&mut self, cmd: RespCommand) -> bool {
    if let Some(handled) = self.process_other_session_commands(cmd) {
      // 批延迟切 NET_RS_LAT_ADMIN 桶（latency_batch_stop 批出口消费复位）
      self.contains_slow_command = true;
      return handled;
    }
    // 自定义对象命令（Customobjcmd）经 dispatch_via_garnet_api 的存储执行域承接；
    // C# CustomTxn / CustomRawStringCmd / CustomProcedure 三族随动态注册层删除，
    // 解析器不再产出这些枚举，事务过程入口单点收敛到 RUNTXP
    let store = self.collect_args_store();
    let args = store.views();
    self.dispatch_via_garnet_api(cmd, &args);
    true
  }

  /// 会话侧闭环臂（C# ProcessOtherCommands switch 的会话 partial 段 +
  /// ProcessAdminCommands 的无存储面子集）；`None` = 存储命令（含 INFO
  /// 扫描族段降级放行），放行 [`Self::process_other_commands`] 尾部存储分派。
  /// 派发单点 `match`，臂序严格等同原顺序 if 链（判序/流水线语义不变）；
  /// 物化参写出骨架收敛 [`Self::args_via_local`]，Echo/Async 借用形态
  /// 合不进（见其文档注）
  fn process_other_session_commands(&mut self, cmd: RespCommand) -> Option<bool> {
    let handled = match cmd {
      // Lua 脚本族（C# NetworkEVAL / NetworkEVALSHA / NetworkScript*）
      RespCommand::Eval
      | RespCommand::Evalsha
      | RespCommand::ScriptExists
      | RespCommand::ScriptFlush
      | RespCommand::ScriptLoad => Some(self.run_lua_command(cmd)),
      RespCommand::ClientId => {
        if self.parse_state.count != 0 {
          // C# NetworkCLIENTID 的参数校验在会话侧（AbortWithWrongNumberOfArguments）
          self.abort_wrong_num_args("client|id");
        } else {
          // C# TryWriteInt64(Id)，走 wresp 整数帧单点
          self.output.write_resp_int(self.id);
        }
        Some(true)
      }
      RespCommand::Echo => {
        // C# RespServerSession.cs:1089：ECHO→NetworkECHO
        let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
        let r = Self::network_echo_impl(&args, &mut self.output);
        Some(matches!(r, Ok(true)))
      }
      RespCommand::Time => {
        // 在 garnet 中的相对路径:libs/server/Resp/BasicCommands.cs:NetworkTIME
        if self.parse_state.count != 0 {
          self.abort_wrong_num_args("TIME");
          return Some(true);
        }
        let now_nanos = now_nanos();
        let secs = now_nanos / NANOS_PER_SEC;
        let usecs = (now_nanos % NANOS_PER_SEC) / 1_000;
        let mut b1 = Buffer::new();
        let s_str = b1.format(secs);
        // C# utcTime.ToString("ffffff")：秒内小数截断至微秒，串恒 6 位零填充、
        // $ 头恒 6。usecs ≤ 999999 恒成立，借 itoa 单点后定长左补零，无运行期格式串
        let mut b2 = Buffer::new();
        let us_str = b2.format(usecs);
        let mut us_buf = [b'0'; MICROS_DIGITS];
        us_buf[MICROS_DIGITS - us_str.len()..].copy_from_slice(us_str.as_bytes());
        self.output.write_resp_array_len(2);
        self.output.write_resp_bulk_string(s_str.as_bytes());
        self.output.write_resp_bulk_string(&us_buf);
        Some(true)
      }
      RespCommand::Async => {
        // C# NetworkASYNC：委托 basic_commands 单一定义（参数校验与降级回包全在彼处）
        let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
        let r = Self::apply_async_param_impl(self.resp_protocol_version, &args, &mut self.output);
        Some(matches!(r, Ok(true)))
      }
      // CLIENT 族（ctx 告警文案逐字保留）
      RespCommand::ClientInfo => {
        Some(self.args_via_local("client info error", Self::network_clientinfo))
      }
      RespCommand::ClientList => {
        Some(self.write_via_local("client list error", |s, out| s.network_clientlist(out)))
      }
      RespCommand::ClientKill => {
        Some(self.write_via_local("client kill error", |s, out| s.network_clientkill(out)))
      }
      RespCommand::ClientGetname => {
        Some(self.args_via_local("client getname error", Self::network_clientgetname))
      }
      RespCommand::ClientSetname => {
        Some(self.args_via_local("client setname error", Self::network_clientsetname))
      }
      RespCommand::ClientSetinfo => {
        Some(self.args_via_local("client setinfo error", Self::network_clientsetinfo))
      }
      RespCommand::ClientUnblock => {
        Some(self.args_via_local("client unblock error", Self::network_clientunblock))
      }
      // C# 分派：FAILOVER/REPLICAOF/SECONDARYOF 显式路由 + IsClusterSubCommand
      // 区间整体经 NetworkProcessClusterCommand；切面未挂时报集群支持未启用
      _ if cmd == RespCommand::Cluster
        || is_cluster_sub_command(cmd)
        || matches!(
          cmd,
          RespCommand::Failover
            | RespCommand::Replicaof
            | RespCommand::Migrate
            | RespCommand::Secondaryof
        ) =>
      {
        Some(self.write_via_local("cluster command error", |s, out| {
          s.network_process_cluster_command(cmd, out)
        }))
      }
      RespCommand::Role => Some(self.args_via_local("role error", Self::network_role)),
      // C# NetworkCOMMAND（COMMAND 根命令）：带参报未知子命令，无参列全部命令
      RespCommand::Command => {
        Some(self.write_via_local("command error", |s, out| s.network_command_root(out)))
      }
      _ => None,
    };
    if handled.is_some() {
      return handled;
    }
    // C# NetworkAUTH（ProcessOtherCommands 段）与 ACL 族不在此分派：二者须
    // 点查底层存储（ACL 为唯一真源），由存储执行域 `StoreGarnetApi::exec`
    // 在批处理纪元保护区外统一承接（冷记录落盘回读 + 会话本地句柄回写）

    // LATENCY / SLOWLOG 族（C# Metrics/Latency、Metrics/Slowlog 的会话 partial）
    if let Some(handled) = self.process_metrics_commands(cmd) {
      return Some(handled);
    }
    // MONITOR / DEBUG / SAVE 族（C# ProcessAdminCommands）
    if let Some(handled) = self.process_admin_session_commands(cmd) {
      return Some(handled);
    }
    if cmd == RespCommand::Info {
      // libs/server/Resp/RespServerSession.cs:ProcessOtherCommands 的 INFO 段
      //（wmetric 段分发：段解析 + 各信息域填充经 SessionInfoSource 数据源
      // 承接）。扫描族段（KEYSPACE 全库扫描计数 / HLOGSCAN 混合日志分布
      // 扫描 / STOREHASHTABLE 哈希分布诊断扫描 / STOREREVIV 复活统计转储，
      // C# PopulateKeyspaceInfo → GetKeyspaceStats 专用扫描会话、
      // PopulateHlogScanInfo → HybridLogDistributionScan、
      // PopulateStoreHashDistribution → DumpDistribution、
      // PopulateStoreRevivInfo → DumpRevivificationStats 均为逐段实填，
      // DEFAULT/ALL 段集合不含这些段）——rust 存储域扫描须跨 await，与
      // DBSIZE 同构降级慢路径（garnet_api 漏斗挂 SlowWait，扫描行与非扫描
      // 面两路合成 InfoSlowSource 全段集渲染）。降级门为 any 语义：解析后
      // 段集（ALL/DEFAULT 关键字已展开、去重）含任一扫描族段即整请求放行
      // 分派漏斗，混合段请求（如 INFO server keyspace）不再以同步面
      // (0,0) 假计数渲染 KEYSPACE；RESET / HELP / 非法段在 C# 先于任何
      // 段填充短路且从不触达扫描数据，一律留在同步面呈现（段解析单点复用
      // wmetric InfoCommand::parse_sections，与渲染面零分叉）
      let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
      let parsed = InfoCommand::parse_sections(&args);
      if parsed.invalid.is_none()
        && !parsed.reset
        && !parsed.help
        && parsed.sections.iter().any(|s| info_scan_section(*s))
      {
        // 放行到存储分派（C# ProcessOtherCommands 末端 ProcessAdminCommands
        // 形态），由存储执行域承接；执行域未挂载时经 dispatch_via_garnet_api
        // 显式拒绝，绝不静默吞命令
        return None;
      }
      let mut out = mem::take(&mut self.output);
      {
        let provider = SessionInfoSource::new(self);
        let mut info = GarnetInfoMetrics::new();
        InfoCommand::network_info(
          &args,
          self.active_db_id as i32,
          &provider,
          &mut info,
          // C# InfoCommand.cs:63 直写 storeWrapper.monitor.resetEventFlags
          // [STATS]=true，经进程级监视器置位、采样轮消费复位（未安装为
          // no-op，对齐 C# monitor != null 判定）
          &mut |section| {
            if let Some(monitor) = GarnetServerMonitor::global() {
              monitor.set_info_reset_flag(section);
            }
          },
          self.resp_protocol_version,
          &mut out,
        );
      }
      self.output = out;
      return Some(true);
    }
    // 存储命令放行尾部存储分派（fast 直通，slow 由 dispatch_slow 入口置位）
    None
  }

  /// 本地缓冲写出 → 并回会话输出（CLIENT/CLUSTER/ROLE 族 take/log/restore
  /// 共同骨架：take 输出缓冲 → 本地写出 → 失败告警 → 并回）
  fn write_via_local<F, E>(&mut self, ctx: &'static str, f: F) -> bool
  where
    F: FnOnce(&mut Self, &mut Vec<u8>) -> Result<bool, E>,
    E: fmt::Display,
  {
    let mut out = mem::take(&mut self.output);
    if let Err(err) = f(self, &mut out) {
      log::warn!("{ctx}: {err}");
    }
    self.output = out;
    true
  }

  /// CLIENT/ROLE 族公共骨架：物化参单分配 → [`Self::write_via_local`] → 并回
  /// 输出，臂只余「ctx 文案 + 方法名」。Echo/Async/PING 借用形态不可并入：
  /// 参数视图持有 `recv_buffer` 借用期间，helper 无法再取 `&mut self`
  /// 实参（字段拆分借用正是 `collect_arg_views` 的显式传参契约），
  /// 故保留各处显式 take/restore
  fn args_via_local(
    &mut self,
    ctx: &'static str,
    f: impl FnOnce(&mut Self, &[&[u8]], &mut Vec<u8>) -> wresp::Result<bool>,
  ) -> bool {
    let store = self.collect_args_store();
    self.write_via_local(ctx, |s, out| f(s, &store.views(), out))
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND
  /// COMMAND 根命令入口
  fn network_command_root(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    if self.parse_state.count > 0 {
      let sub = self.parse_state.arg_in(&self.recv_buffer, 0).as_str_safe();
      cs::abort_with_unknown_subcommand(output, sub, "COMMAND");
    } else {
      self.write_command_response(output)?;
    }
    Ok(true)
  }

  /// 经注入的存储执行域分派（C# 对应 Process 链末端 ProcessAdminCommands
  /// 的兜底形态；先克隆 Arc 再调用，解 &self.garnet_api 与 &mut session
  /// 的借用相交，单次原子递增对比命令执行开销不可见）
  ///
  /// AUTH / HELLO / ACL 族预筛停车：存储点查（认证、规则读写、串行锁）
  /// 严禁同步收割，登记 (命令, 参数快照) 交网络泵经执行域 exec_auth_acl
  /// 异步臂闭环；本批消费到此为止（命令游标已推进、应答由泵按流水线顺序
  /// 冲出），后续命令待闭环后继续。事务窗内（EXEC 重放）按 §58d 一态收口：
  /// 携 AUTH 形 HELLO 与 ACL 族各回专属拒绝帧，无 AUTH 形 HELLO 走同步快臂
  /// 直出应答（详见臂内注）
  fn dispatch_via_garnet_api(&mut self, cmd: RespCommand, args: &[&[u8]]) {
    // 预筛以执行域已挂载为前提：未挂载即装配缺口，与余者同走显式拒绝。
    // 事务窗内严禁挂入 pending_auth_acl（普通命令窗口为唯一合法停泊窗）
    if (cmd == RespCommand::Auth || cmd == RespCommand::Hello || is_acl_command(cmd))
      && self.garnet_api.is_some()
    {
      // C# AUTH ∈ ProcessOtherCommands(:1065)、ACL 族 ∈ ProcessAdminCommands
      //（AdminCommands.cs:31）入口置位对位：停车闭环不经 dispatch_slow，
      // 本批延迟须切 NET_RS_LAT_ADMIN
      self.contains_slow_command = true;
      if self.txn_state != TxnState::None {
        // §58d 重放窗一态收口：文法合法且不携 AUTH 的 HELLO 形零存储点查、
        // 零停泊（协议回显/客户端名均会话元数据），经无存储同步快臂直出应答
        // map（对位 C# NetworkHELLO 无事务门正常执行、与 §58a「中止面恰等于
        // 携 AUTH 组」排队裁决面收口为一态）；携 AUTH 形与语法错形维持禁停泊
        // 围栏拒绝帧，ACL 族十子命令规则读写须点查停泊、回专属文案
        if cmd == RespCommand::Hello
          && let Ok(hello) = parse_hello_args(args)
          && hello.auth.is_none()
        {
          let mut out = mem::take(&mut self.output);
          self.commit_hello_state_and_write_reply(
            hello.protocol_version,
            hello.client_name,
            false,
            &mut out,
          );
          self.output = out;
          return;
        }
        let err = if cmd == RespCommand::Auth {
          RESP_ERR_AUTH_IN_MULTI
        } else if is_acl_command(cmd) {
          cs::RESP_ERR_ACL_IN_TXN_UNSUPPORTED
        } else {
          cs::RESP_ERR_HELLO_IN_TXN_UNSUPPORTED
        };
        cs::write_error_raw(&mut self.output, err);
        return;
      }
      self.pending_auth_acl = Some((cmd, args.iter().map(|a| a.to_vec()).collect::<Vec<_>>()));
      return;
    }
    let Some(api) = self.garnet_api.clone() else {
      // 存储执行域未挂载 = 宿主装配缺口：写明错误并告警，绝不静默吞命令
      log::error!("存储执行域未挂载，命令 {cmd:?} 被拒绝");
      self.abort_error_message(ERR_STORE_DOMAIN_NOT_ATTACHED);
      return;
    };
    api.exec(self, cmd, args);
  }
}
