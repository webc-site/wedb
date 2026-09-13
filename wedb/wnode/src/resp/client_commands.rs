//! CLIENT 族命令（对标 libs/server/Resp/ClientCommands.cs）
//!
//! C# ClientCommands.cs 为 `RespServerSession` 的 partial 分片，本文件同构
//! 映射为 `impl RespServerSession` 扩展块；CLIENT INFO/GETNAME/SETNAME/
//! SETINFO 直读会话真实状态（clientName / clientLib* / 端点 / Id）。
//!
//! CLIENT LIST/KILL 的枚举面为服务器层活跃消费者注册表
//!（[`ConsumerRegistry`]，对标 C# GarnetServerBase.ActiveConsumers；见
//! servers/consumer_registry.rs）：未装配时保持 C# 非 Garnet 服务器分支的
//! "cannot be listed" 文案。动态字段（name/db/resp/type 等）以注册快照 +
//! CLIENT 族命令执行时自刷新承接（C# 为跨线程裸读会话字段）。

use std::str::from_utf8;

use smallvec::SmallVec;
use wbase::time::now_ms;
use wmetric::GarnetServerMonitor;
use wresp::{
  RespVecExt, check_arg_count, cmd_strings as cs,
  cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments, write_raw},
  strict_i64,
};

use super::resp_server_session::RespServerSession;
use crate::{
  servers::consumer_registry::{ClientView, ConsumerEntry, ConsumerRegistry},
  session_parse_state_extensions::{
    ClientType, try_get_client_name_bytes, try_get_client_type,
  },
};

/// CLIENT LIST / CLIENT KILL 的类型过滤字面量（C# CmdStrings: TYPE / ID）
const FILTER_TYPE: &[u8] = b"TYPE";
const FILTER_ID: &[u8] = b"ID";

/// SKIPME 取值字面量（C# CmdStrings: YES / NO）
const SKIPME_YES: &[u8] = b"YES";
const SKIPME_NO: &[u8] = b"NO";

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

    let args = self.get_arg_slices();
    let filter = if args.is_empty() {
      ClientListFilter::All
    } else if args.len() < 2 {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    } else if args[0].eq_ignore_ascii_case(FILTER_TYPE) {
      if args.len() != 2 {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      // SLAVE 于 CLIENT LIST 非法（该子命令晚于 SLAVE → REPLICA 更名）
      match try_get_client_type(&self.parse_state, 1) {
        Some(client_type) if client_type != ClientType::Slave => {
          ClientListFilter::Type(client_type)
        }
        _ => {
          let unknown_type = String::from_utf8_lossy(args[1]).into_owned();
          abort_with_error_message(
            output,
            &cs::GENERIC_UNKNOWN_CLIENT_TYPE.replace("{0}", &unknown_type),
          );
          return Ok(true);
        }
      }
    } else if args[0].eq_ignore_ascii_case(FILTER_ID) {
      // C# stackalloc long[32] + ArrayPool 兜底；内联容量对齐枚举尺寸约束
      let mut ids = SmallVec::<[i64; 8]>::with_capacity(args.len() - 1);
      for arg in &args[1..] {
        let Some(id) = strict_i64(arg) else {
          abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_ID);
          return Ok(true);
        };
        ids.push(id);
      }
      ClientListFilter::Ids(ids)
    } else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    };

    let now_milliseconds = now_ms().min(i64::MAX as u64) as i64;
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
    check_arg_count!(parse_state, empty, output, "client|info");

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

    let args = self.get_arg_slices();
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

    let Some(filters) = self.parse_kill_filters(&args, output) else {
      return Ok(true);
    };
    let now_milliseconds = now_ms().min(i64::MAX as u64) as i64;
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
  fn parse_kill_filters(
    &self,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Option<ClientKillFilters> {
    let mut filters = ClientKillFilters::default();
    let mut arg_ix = 0;
    while arg_ix < args.len() {
      if arg_ix + 1 >= args.len() {
        abort_with_wrong_number_of_arguments(output, "CLIENT|KILL");
        return None;
      }
      let filter = args[arg_ix];
      let value = args[arg_ix + 1];

      if filter.eq_ignore_ascii_case(FILTER_ID) {
        let Some(id) = strict_i64(value) else {
          abort_with_error_message(
            output,
            &cs::GENERIC_ERR_SHOULD_BE_GREATER_THAN_ZERO.replace("{0}", "client-id"),
          );
          return None;
        };
        if filters.id.is_some() {
          abort_with_error_message(
            output,
            &cs::GENERIC_ERR_DUPLICATE_FILTER.replace("{0}", "ID"),
          );
          return None;
        }
        filters.id = Some(id);
      } else if filter.eq_ignore_ascii_case(FILTER_TYPE) {
        if filters.client_type.is_some() {
          abort_with_error_message(
            output,
            &cs::GENERIC_ERR_DUPLICATE_FILTER.replace("{0}", "TYPE"),
          );
          return None;
        }
        // SLAVE 映射 REPLICA（C# 注释：为后续比对铺路），余者原样
        let client_type = match try_get_client_type(&self.parse_state, arg_ix + 1) {
          Some(ClientType::Slave) => ClientType::Replica,
          Some(parsed) => parsed,
          None => {
            let unknown_type = String::from_utf8_lossy(value).into_owned();
            abort_with_error_message(
              output,
              &cs::GENERIC_UNKNOWN_CLIENT_TYPE.replace("{0}", &unknown_type),
            );
            return None;
          }
        };
        filters.client_type = Some(client_type);
      } else if filter.eq_ignore_ascii_case(b"USER") {
        if filters.user.is_some() {
          abort_with_error_message(
            output,
            &cs::GENERIC_ERR_DUPLICATE_FILTER.replace("{0}", "USER"),
          );
          return None;
        }
        filters.user = Some(String::from_utf8_lossy(value).into_owned());
      } else if filter.eq_ignore_ascii_case(b"ADDR") {
        if filters.addr.is_some() {
          abort_with_error_message(
            output,
            &cs::GENERIC_ERR_DUPLICATE_FILTER.replace("{0}", "ADDR"),
          );
          return None;
        }
        filters.addr = Some(String::from_utf8_lossy(value).into_owned());
      } else if filter.eq_ignore_ascii_case(b"LADDR") {
        if filters.laddr.is_some() {
          abort_with_error_message(
            output,
            &cs::GENERIC_ERR_DUPLICATE_FILTER.replace("{0}", "LADDR"),
          );
          return None;
        }
        filters.laddr = Some(String::from_utf8_lossy(value).into_owned());
      } else if filter.eq_ignore_ascii_case(b"SKIPME") {
        if filters.skip_me.is_some() {
          abort_with_error_message(
            output,
            &cs::GENERIC_ERR_DUPLICATE_FILTER.replace("{0}", "SKIPME"),
          );
          return None;
        }
        if value.eq_ignore_ascii_case(SKIPME_YES) {
          filters.skip_me = Some(true);
        } else if value.eq_ignore_ascii_case(SKIPME_NO) {
          filters.skip_me = Some(false);
        } else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return None;
        }
      } else if filter.eq_ignore_ascii_case(b"MAXAGE") {
        let Some(max_age) = strict_i64(value) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return None;
        };
        if filters.max_age.is_some() {
          abort_with_error_message(
            output,
            &cs::GENERIC_ERR_DUPLICATE_FILTER.replace("{0}", "MAXAGE"),
          );
          return None;
        }
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
    check_arg_count!(parse_state, empty, output, "CLIENT|GETNAME");
    // C# IsNullOrEmpty → nil（空名视同未设置）
    match self.client_name.as_deref() {
      Some(name) if !name.is_empty() => output.write_resp_bulk_string(name.as_bytes()),
      _ => output.write_resp_null(),
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
        self.refresh_client_view();
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
    let value = parse_state[1];
    // C# 对 "LIB-NAME"/"lib-name" 与 "LIB-VER"/"lib-ver" 做两组精确匹配
    //（`-` 无法经 ASCII 大小写折叠，故非大小写不敏感）
    let Ok(value) = from_utf8(value) else {
      // C# GetString 为 ASCII 解码不失败；非法 UTF-8 按空值承接
      write_raw(output, cs::RESP_OK);
      return Ok(true);
    };
    if option == b"LIB-NAME" || option == b"lib-name" {
      self.client_lib_name = Some(value.to_string());
      self.refresh_client_view();
      write_raw(output, cs::RESP_OK);
    } else if option == b"LIB-VER" || option == b"lib-ver" {
      self.client_lib_version = Some(value.to_string());
      self.refresh_client_view();
      write_raw(output, cs::RESP_OK);
    } else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    }
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
    let Some(client_id) = strict_i64(parse_state[0]) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    let mut throw_error = false;
    if parse_state.len() == 2 {
      let option = parse_state[1];
      if option.eq_ignore_ascii_case(b"TIMEOUT") {
        throw_error = false;
      } else if option.eq_ignore_ascii_case(b"ERROR") {
        throw_error = true;
      } else {
        abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_UNBLOCK_REASON);
        return Ok(true);
      }
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

  /// 会话条目视图自刷新：把会话动态字段镜像进注册表条目（C# LIST/KILL 跨
  /// 线程裸读 `r.` 字段的承接——会话体单线程独占，CLIENT 族命令执行点即
  /// 本会话镜像的一致刷新点）
  ///
  /// user 仅在持认证句柄时输出：NoAuth 档 rust 无 ACL 实例可取
  /// GetDefaultUserHandle（会话构造后句柄为 None），与 C# 恒带默认用户的
  /// 输出存在域界差异（ACL 域决策，见 RespServerSession.user_handle）
  fn refresh_client_view(&self) {
    let Some(entry) = ConsumerRegistry::global().and_then(|r| r.get(self.id)) else {
      return;
    };
    let client_type = match &self.cluster_session {
      Some(cluster) => {
        if cluster.is_replica() {
          ClientType::Replica
        } else {
          ClientType::Master
        }
      }
      None => {
        if self.is_subscription_session {
          ClientType::Pubsub
        } else {
          ClientType::Normal
        }
      }
    };
    entry.update_view(ClientView {
      name: self.client_name.clone(),
      lib_name: self.client_lib_name.clone(),
      lib_ver: self.client_lib_version.clone(),
      user: self.user_handle.as_ref().map(|h| h.user().name.clone()),
      db: self.active_db_id,
      resp: self.resp_protocol_version,
      client_type,
    });
  }

  /// 大块 verbatim 写出（C# WriteLargeVerbatimString 的命令域适配点——
  /// 会话输出锚点 write_large_verbatim_string 直写 self.output，而
  /// write_via_local 闭包内须写暂存缓冲，故经协议泛型落目标缓冲）：
  /// RESP3 verbatim（`txt:` 前缀）/ RESP2 bulk string
  fn write_large_verbatim(&self, output: &mut Vec<u8>, message: &[u8]) {
    if self.resp_protocol_version >= 3 {
      output.resp_writer3().write_large_verbatim_string(message, b"txt");
    } else {
      output.resp_writer2().write_large_verbatim_string(message, b"txt");
    }
  }

  /// 会话析构尾部的指标归并（C# Dispose 尾部 `monitor.AddMetricsHistory
  /// SessionDispose(sessionMetrics, LatencyMetrics, commandStats)` 的命令域
  /// 适配点；锚点为 [`RespServerSession::dispose`]）
  ///
  /// commandStats 会话域未启用恒 None；监视器未装配时为无害空操作
  pub(crate) fn merge_metrics_history_session_dispose(&self) {
    let Some(monitor) = GarnetServerMonitor::global() else {
      return;
    };
    monitor.add_metrics_history_session_dispose(
      self.session_metrics.as_ref(),
      self.get_latency_metrics().as_deref(),
      None,
    );
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
