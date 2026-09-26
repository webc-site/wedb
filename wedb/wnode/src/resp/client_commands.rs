//! CLIENT 族命令（对标 libs/server/Resp/ClientCommands.cs）
//!
//! C# ClientCommands.cs 为 `RespServerSession` 的 partial 分片，本文件同构
//! 映射为 `impl RespServerSession` 扩展块；CLIENT INFO/GETNAME/SETNAME/
//! SETINFO 直读会话真实状态（clientName / clientLib* / 端点 / Id）。
//!
//! CLIENT LIST/KILL 的枚举面为服务器层活跃消费者注册表
//!（[`ConsumerRegistry`]，对标 C# GarnetServerBase.ActiveConsumers；见
//! servers/consumer_registry.rs）：未装配时保持 C# 非 Garnet 服务器分支的
//! "cannot be listed" 文案。动态字段（name/db/resp/type 等）的真值单源在会话
//! 字段，注册表条目持一份跨线程可读投影：他者会话行由网络泵每批汇聚单点整体
//! 重导（见 `resp_session_consumer.rs:mirror_session_counters`，与命令计数同轨，
//! 杜绝逐字段补调的漏点），本会话行在 LIST/KILL 入口即时刷新（对标 C# 对自己
//! 活字段的即时读，兜住同一条流水线包内「先改自身字段再立刻 LIST」的边界）。
//! C# 为跨线程裸读会话活字段，rust 因所有权模型不能无锁裸读，投影即其等价承接。

use std::str::from_utf8_unchecked;

use smallvec::SmallVec;
use wbase::{ascii_sanitize, num::strict_i64, time::now_ms_i64};
use wmetric::GarnetServerMonitor;
use wresp::{
  check_args::{check_arg_count, parse_i64_arg},
  cmd_strings::{
    self as cs, abort_with_error_message, abort_with_wrong_number_of_arguments, write_raw,
  },
  ext::{RespVecExt, is_resp3},
};

use super::resp_server_session::{RespServerSession, collect_arg_views};
use crate::{
  servers::consumer_registry::{ClientView, ConsumerEntry, ConsumerRegistry},
  session_parse_state_extensions::{
    ClientType, try_get_client_name_bytes, try_get_client_type_from_token,
  },
};

impl RespServerSession {
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTLIST
  ///
  /// C# 首行即 `Server is GarnetServerBase` 判定——rust 对应注册表是否装配。
  /// 参数面：无参 = 全量；`TYPE <type>`（SLAVE 非法）；`ID <id>...`；
  /// 其余为 RESP_SYNTAX_ERROR。响应为 verbatim 字符串（RESP2 bulk），多条目
  /// 以 `\n` 分隔且尾部恒带 `\n`（Redis 语义）。
  pub fn network_clientlist(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    let Some(registry) = ConsumerRegistry::global() else {
      abort_with_error_message(output, cs::RESP_ERR_CANNOT_LIST_CLIENTS);
      return Ok(true);
    };
    self.refresh_client_view();

    let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
    let filter = match args.as_slice() {
      [] => ClientListFilter::All,
      [type_token, type_arg] if type_token.eq_ignore_ascii_case(b"TYPE") => {
        // SLAVE 于 CLIENT LIST 非法（该子命令晚于 SLAVE → REPLICA 更名）
        match try_get_client_type_from_token(type_arg) {
          Some(client_type) if client_type != ClientType::Slave => {
            ClientListFilter::Type(client_type)
          }
          _ => {
            // C# ReadString=Encoding.ASCII.GetString 逐字节折 '?'（非 lossy 保原字符）
            let unknown_type = ascii_sanitize(type_arg);
            abort_with_error_message(
              output,
              &cs::GENERIC_UNKNOWN_CLIENT_TYPE.replace("{0}", &unknown_type),
            );
            return Ok(true);
          }
        }
      }
      [id_token, id_args @ ..] if id_token.eq_ignore_ascii_case(b"ID") && !id_args.is_empty() => {
        let Some(ids) = id_args
          .iter()
          .map(|arg| strict_i64(arg))
          .collect::<Option<SmallVec<[i64; 8]>>>()
        else {
          abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_ID);
          return Ok(true);
        };
        ClientListFilter::Ids(ids)
      }
      _ => {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
    };

    let now_milliseconds = now_ms_i64();
    let mut result = String::new();
    let mut first = true;
    for entry in registry.active_consumers() {
      if !filter.matches(&entry) {
        continue;
      }
      if !first {
        // Redis 以单个 \n 分隔（非 \r\n）
        result.push('\n');
      }
      entry.write_client_info(&mut result, now_milliseconds);
      first = false;
    }
    result.push('\n');
    self.write_large_verbatim(output, result.as_bytes());
    Ok(true)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTINFO
  pub fn network_clientinfo(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, "client|info");

    // C# 拼行后 WriteLargeVerbatimString（RESP3 verbatim / RESP2 bulk）
    let mut result = String::new();
    self.write_client_info_state(&mut result);
    result.push('\n');
    self.write_large_verbatim(output, result.as_bytes());
    Ok(true)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTKILL
  ///
  /// 两种形式（C# :205-490）：
  /// - 老式 `KILL ip:port`：杀首个 addr 匹配者并回 `+OK`，无匹配回
  ///   NO_SUCH_CLIENT；
  /// - 新式过滤器 `ID/TYPE/USER/ADDR/LADDR/SKIPME/MAXAGE <value>...`：
  ///   过滤器重复报错、未知过滤器语法错、SKIPME 默认 true、SLAVE 映射
  ///   REPLICA，应答为实际杀掉数（TryKill 首杀即真）。
  pub fn network_clientkill(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    let Some(registry) = ConsumerRegistry::global() else {
      abort_with_error_message(output, cs::RESP_ERR_CANNOT_LIST_CLIENTS);
      return Ok(true);
    };
    self.refresh_client_view();

    let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
    if args.is_empty() {
      abort_with_wrong_number_of_arguments(output, "CLIENT|KILL");
      return Ok(true);
    }
    if args.len() == 1 {
      // 老式 ip:port：首个 addr 匹配即回 OK（C# 忽略 TryKill 结果）
      let target = args[0];
      for entry in registry.active_consumers() {
        if entry.remote_endpoint.as_bytes() == target {
          entry.kill_session();
          write_raw(output, cs::RESP_OK);
          return Ok(true);
        }
      }
      abort_with_error_message(output, cs::RESP_ERR_NO_SUCH_CLIENT);
      return Ok(true);
    }

    let Some(filters) = Self::parse_kill_filters(&args, output) else {
      return Ok(true);
    };
    let now_milliseconds = now_ms_i64();
    let mut killed = 0i64;
    for entry in registry.active_consumers() {
      if !filters.matches(self.id, now_milliseconds, &entry) {
        continue;
      }
      if entry.kill_session() {
        killed += 1;
      }
    }
    output.write_resp_int(killed);
    Ok(true)
  }

  /// C# NetworkCLIENTKILL 的新式过滤器解析段（偶数位过滤器名大写匹配，
  /// 奇数位取值；重复过滤器与未知过滤器按 C# 文案报错）
  fn parse_kill_filters(args: &[&[u8]], output: &mut Vec<u8>) -> Option<ClientKillFilters> {
    macro_rules! check_dup_filter {
      ($opt:expr, $name:expr) => {
        if $opt.is_some() {
          abort_with_error_message(
            output,
            &cs::GENERIC_ERR_DUPLICATE_FILTER.replace("{0}", $name),
          );
          return None;
        }
      };
    }

    let mut filters = ClientKillFilters::default();
    let mut arg_ix = 0;
    while arg_ix < args.len() {
      if arg_ix + 1 >= args.len() {
        abort_with_wrong_number_of_arguments(output, "CLIENT|KILL");
        return None;
      }
      let filter = args[arg_ix];
      let value = args[arg_ix + 1];

      if filter.eq_ignore_ascii_case(b"ID") {
        // 有意分叉（deviations §154，禁删 >0 滤片回改）：C# 仅 TryReadLong 解析失败
        // 报错，解析成功的非正值入过滤器恒不匹配回 :0；rust 对齐真 Redis 严向收口。
        // 与 §63（CLIENT UNBLOCK 负数 ID 双侧同收敛 :0）异命令异门，勿混淆
        let Some(id) = strict_i64(value).filter(|&v| v > 0) else {
          abort_with_error_message(output, cs::RESP_ERR_CLIENT_ID_GREATER_THAN_ZERO);
          return None;
        };
        check_dup_filter!(filters.id, "ID");
        filters.id = Some(id);
      } else if filter.eq_ignore_ascii_case(b"TYPE") {
        check_dup_filter!(filters.client_type, "TYPE");
        // SLAVE 映射 REPLICA（C# 注释：为后续比对铺路），余者原样
        let client_type = match try_get_client_type_from_token(value) {
          Some(ClientType::Slave) => ClientType::Replica,
          Some(parsed) => parsed,
          None => {
            // C# ReadString 逐字节折 '?'，与 ACL 域 ascii_sanitize 同一口径
            let unknown_type = ascii_sanitize(value);
            abort_with_error_message(
              output,
              &cs::GENERIC_UNKNOWN_CLIENT_TYPE.replace("{0}", &unknown_type),
            );
            return None;
          }
        };
        filters.client_type = Some(client_type);
      } else if filter.eq_ignore_ascii_case(b"USER") {
        check_dup_filter!(filters.user, "USER");
        filters.user = Some(ascii_sanitize(value).into_owned());
      } else if filter.eq_ignore_ascii_case(b"ADDR") {
        check_dup_filter!(filters.addr, "ADDR");
        filters.addr = Some(ascii_sanitize(value).into_owned());
      } else if filter.eq_ignore_ascii_case(b"LADDR") {
        check_dup_filter!(filters.laddr, "LADDR");
        filters.laddr = Some(ascii_sanitize(value).into_owned());
      } else if filter.eq_ignore_ascii_case(b"SKIPME") {
        check_dup_filter!(filters.skip_me, "SKIPME");
        filters.skip_me = if value.eq_ignore_ascii_case(b"YES") {
          Some(true)
        } else if value.eq_ignore_ascii_case(b"NO") {
          Some(false)
        } else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return None;
        };
      } else if filter.eq_ignore_ascii_case(b"MAXAGE") {
        let Some(max_age) = strict_i64(value) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return None;
        };
        check_dup_filter!(filters.max_age, "MAXAGE");
        filters.max_age = Some(max_age);
      } else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      }

      arg_ix += 2;
    }
    // SKIPME 默认 true（C# skipMe ??= true）
    filters.skip_me.get_or_insert(true);
    Some(filters)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTGETNAME
  pub fn network_clientgetname(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, "CLIENT|GETNAME");
    // C# IsNullOrEmpty → nil（空名视同未设置）
    match self.client_name.as_deref() {
      Some(name) if !name.is_empty() => output.write_resp_bulk_string(name.as_bytes()),
      _ => output.write_resp_null_ver(self.resp_protocol_version),
    }
    Ok(true)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTSETNAME
  pub fn network_clientsetname(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1, output, "CLIENT|SETNAME");

    // 对标 C# TryGetClientName：33..=126 可打印字符，空串允许（清名语义）
    match try_get_client_name_bytes(parse_state[0]) {
      Some(name) => {
        self.set_client_name(Some(name));
        write_raw(output, cs::RESP_OK);
      }
      None => abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_NAME),
    }
    Ok(true)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTSETINFO
  pub fn network_clientsetinfo(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "CLIENT|SETINFO");

    let option = parse_state[0];
    let is_lib_name = if option.eq_ignore_ascii_case(b"LIB-NAME") {
      true
    } else if option.eq_ignore_ascii_case(b"LIB-VER") {
      false
    } else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    };

    let value = parse_state[1];
    // 校验属性值：必须仅由 ASCII 33..=126 字符构成（空串亦合法）
    if !is_valid_client_attr(value) {
      // option 名已被上方 LIB-NAME/LIB-VER 严格等值门收敛为恒 ASCII，lossy 为
      // no-op（输入域恒 ASCII 收敛，非分叉面），保持不改防第三度疑报
      cs::abort_with_invalid_client_attr(output, &String::from_utf8_lossy(option));
      return Ok(true);
    }

    // 经 is_valid_client_attr 校验，字节全在 ASCII 33..=126 范围内，100% 为合法 UTF-8
    let value_str = unsafe { from_utf8_unchecked(value) }.to_string();
    if is_lib_name {
      self.client_lib_name = Some(value_str);
    } else {
      self.client_lib_version = Some(value_str);
    }
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }

  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTUNBLOCK
  ///
  /// 经纪注入时按客户端 ID 查观察者并强制解除（TIMEOUT → 空结果，
  /// ERROR → UNBLOCKED 错误应答给被解除方）；无经纪/未阻塞回 0
  pub fn network_clientunblock(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1..=2, output, "client|unblock");

    // 解析目标客户端 ID（C# TryGetLong 严格口径）
    let Some(client_id) = parse_i64_arg(parse_state[0], output) else {
      return Ok(true);
    };

    let throw_error = match parse_state.get(1) {
      None => false,
      Some(t) if t.eq_ignore_ascii_case(b"TIMEOUT") => false,
      Some(t) if t.eq_ignore_ascii_case(b"ERROR") => true,
      _ => {
        abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_UNBLOCK_REASON);
        return Ok(true);
      }
    };

    // 负数 ID 非合法会话 ID（C# ActiveConsumers 查找落空回 0）；同时防
    // 负数 as usize 投影至 [2^63, 2^64) 别名命中 BlockWaitFace 慢路径递减专用域
    // （偏差登记见 doc/zh/deviations.md §63；CLIENT KILL ID 非正值门系异命令
    // 异裁——rust 回错误帧，见 §154，勿互抄）
    if client_id < 0 {
      output.write_resp_int(0);
      return Ok(true);
    }

    // C# ActiveConsumers 找会话 → itemBroker.TryGetObserver → TryForceUnblock；
    // rust 观察者以会话 ID 直键经纪（会话 ID 即客户端 ID），未阻塞即回 0
    let unblocked = self
      .item_broker
      .as_ref()
      .and_then(|broker| broker.try_get_observer(client_id as usize))
      .is_some_and(|observer| observer.try_force_unblock(throw_error));
    output.write_resp_int(i64::from(unblocked));
    Ok(true)
  }

  /// 会话动态字段视图组源（CLIENT 族唯一字段组装点）：name/user/lib-* 直取
  /// 会话字段，flags 身份经 [`current_client_type`](Self::current_client_type)
  /// 单点判定——注册表条目投影（CLIENT LIST/KILL）与 CLIENT INFO 自身行
  /// 共用，对标 C# LIST/INFO 共函数 WriteClientInfo 的单一数据源。
  ///
  /// user 直读认证句柄（C# `targetSession._userHandle?.User.Name` 单源）：
  /// 免认证形态构造期已兜底 default 单例句柄（authenticate_user 的
  /// GetDefaultUserHandle 兜底臂），CLIENT INFO/LIST 恒带 default 与 C#
  /// 契约一致；ACL 档认证失败 / 撤销挂载为 None（C# _userHandle null 同形）
  pub(crate) fn current_client_view(&self) -> ClientView {
    ClientView {
      name: self.client_name.clone(),
      lib_name: self.client_lib_name.clone(),
      lib_ver: self.client_lib_version.clone(),
      user: self.user_name().map(str::to_string),
      db: self.active_db_id,
      resp: self.resp_protocol_version,
      // 慢订阅者丢尾观测面（rn14）：邮箱内计数随本投影同一发布轨导出,
      // 每批汇聚单点整体重导,他者会话读到最近完成批的真值
      pubsub_dropped: self.pubsub.dropped(),
      client_type: self.current_client_type(),
    }
  }

  /// flags 身份判定单点（C# WriteClientInfo 的远端节点角色臂；
  /// BasicCommands.cs:1968-1990）：集群切面与 provider 双在位且 gossip
  /// 对端已确立（remote_node_id 有值）→ 按远端节点角色出 S/M
  /// （is_replica_node，远端是否从库；非本地节点角色）；否则订阅 P /
  /// 普通 N。集群形态下普通客户端不经 gossip 建链，恒落 N/P——本地
  /// is_replica 表达「本节点是从库」，与「这条连接的远端是否从库」语义
  /// 不同源，不参与本判定
  pub(crate) fn current_client_type(&self) -> ClientType {
    // C# 双门直译：clusterProvider is not null && clusterSession?.RemoteNodeId is not null
    let replica_peer = self
      .cluster_session
      .as_deref()
      .zip(self.cluster_provider.as_deref())
      .and_then(|(session, provider)| {
        session
          .remote_node_id()
          .map(|id| provider.is_replica_node(id))
      });
    match replica_peer {
      Some(true) => ClientType::Replica,
      Some(false) => ClientType::Master,
      None => {
        if self.is_subscription_session {
          ClientType::Pubsub
        } else {
          ClientType::Normal
        }
      }
    }
  }

  /// 本会话条目视图即时刷新（仅 LIST/KILL 入口调用）：把自身动态字段投影进注册
  /// 表条目，对标 C# 枚举时对当前会话活字段的即时读——同一条流水线包内先改自身
  /// db/resp 再立刻 LIST 时，按批汇聚要到包尾才生效，此处即时刷新兜住该自行为
  /// 边界。他者会话行的新鲜度由网络泵每批汇聚发布承接（见
  /// `resp_session_consumer.rs:mirror_session_counters`），不依赖本入口
  fn refresh_client_view(&self) {
    let Some(entry) = ConsumerRegistry::global().and_then(|r| r.get(self.id)) else {
      return;
    };
    entry.update_view(self.current_client_view());
  }

  /// 大块 verbatim 写出（C# WriteLargeVerbatimString 的命令域适配点——
  /// 会话输出锚点 write_large_verbatim_string 直写 self.output，而
  /// write_via_local 闭包内须写暂存缓冲，故经协议泛型落目标缓冲）：
  /// RESP3 verbatim（`txt:` 前缀）/ RESP2 bulk string
  fn write_large_verbatim(&self, output: &mut Vec<u8>, message: &[u8]) {
    if is_resp3(self.resp_protocol_version) {
      output
        .resp_writer3()
        .write_large_verbatim_string(message, b"txt");
    } else {
      output
        .resp_writer2()
        .write_large_verbatim_string(message, b"txt");
    }
  }

  /// 会话析构尾部的指标归并（C# Dispose 尾部 `monitor.AddMetricsHistory
  /// SessionDispose(sessionMetrics, LatencyMetrics, commandStats)` 的命令域
  /// 适配点；锚点为 [`RespServerSession::dispose`]）
  ///
  /// 命令统计读共享句柄快照归并。延迟两臂不在此列：会话延迟表与 PENDING_LAT
  /// 槽均为本会话独占，残余样本由属主线程自己按引用并入全局延迟表（对位 C#
  /// 同一函数内的 Merge + Return 两臂，rust 无需把会话表借给监视器线程）。
  /// 监视器未装配时归并为无害空操作。
  pub(crate) fn merge_metrics_history_session_dispose(&mut self) {
    if let Some(latency) = &mut self.latency_metrics {
      latency.return_to_pool();
    }
    if let Some(meter) = &self.pending_latency {
      meter.flush();
    }
    let Some(monitor) = GarnetServerMonitor::global() else {
      return;
    };
    let command_stats = self
      .command_stats
      .as_deref()
      .map(|stats| stats.lock().clone());
    let session_metrics = self.session_metrics.as_ref().map(|m| m.snapshot());
    monitor.add_metrics_history_session_dispose(session_metrics.as_ref(), command_stats.as_ref());
  }
}

/// CLIENT LIST 过滤器（C# NetworkCLIENTLIST 的 toInclude 三分支）
enum ClientListFilter {
  /// 无过滤（全量）
  All,
  /// `TYPE <type>` 过滤
  Type(ClientType),
  /// `ID <id>...` 过滤（C# stackalloc long[32] 栈容量对齐；内联 8 项余者
  /// 落堆，控制枚举尺寸）
  Ids(SmallVec<[i64; 8]>),
}

impl ClientListFilter {
  fn matches(&self, entry: &ConsumerEntry) -> bool {
    match self {
      Self::All => true,
      Self::Type(client_type) => entry.client_view().client_type == *client_type,
      Self::Ids(ids) => ids.contains(&entry.id),
    }
  }
}

/// CLIENT KILL 新式过滤器集（C# NetworkCLIENTKILL 局部变量组；skip_me 解析
/// 后恒为 Some——默认 true）
#[derive(Default)]
struct ClientKillFilters {
  id: Option<i64>,
  client_type: Option<ClientType>,
  user: Option<String>,
  addr: Option<String>,
  laddr: Option<String>,
  skip_me: Option<bool>,
  max_age: Option<i64>,
}

impl ClientKillFilters {
  /// C# IsMatch（:416-489）：skipme 命中即排除自身，余下过滤器逐项 AND
  fn matches(&self, self_id: i64, now_milliseconds: i64, entry: &ConsumerEntry) -> bool {
    if self.skip_me.unwrap_or(true) && entry.id == self_id {
      return false;
    }
    if let Some(id) = self.id
      && entry.id != id
    {
      return false;
    }
    if let Some(client_type) = self.client_type
      && entry.client_view().client_type != client_type
    {
      return false;
    }
    if let Some(user) = &self.user
      && entry.client_view().user.as_deref() != Some(user.as_str())
    {
      return false;
    }
    if let Some(addr) = &self.addr
      && entry.remote_endpoint != *addr
    {
      return false;
    }
    if let Some(laddr) = &self.laddr
      && entry.local_endpoint != *laddr
    {
      return false;
    }
    if let Some(max_age) = self.max_age {
      // C#：(now - CreationTicks) / 1_000 > maxAge（严格大于）
      let age_sec = (now_milliseconds - entry.creation_ticks).max(0) / 1_000;
      if age_sec <= max_age {
        return false;
      }
    }
    true
  }
}

/// 校验 CLIENT SETINFO 属性值（仅由 ASCII 33..=126 字符构成，空串合法）
///
/// libs/server/Resp/ClientCommands.cs:IsValidClientAttr
#[inline]
fn is_valid_client_attr(value: &[u8]) -> bool {
  value.iter().all(|&b| b.wrapping_sub(33) <= (126 - 33))
}
