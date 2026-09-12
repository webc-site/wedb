//! 副本接收面会话（CLUSTER APPENDLOG 网络处理 + 主端流落盘重放）
//!
//! 对标 C# 双层结构：
//! 1. `ClusterSession.NetworkClusterAppendLog`（libs/cluster/Session/
//!    RespClusterReplicationCommands.cs）：RESP 帧解析、角色校验、初始化
//!    分支（-1/-1/-1 → InitializeReplicaReplayDriver，回 +OK）；
//! 2. `ReplicaReplaySession.ProcessPrimaryStream`（libs/cluster/Server/
//!    Replication/ReplicaOps/AOFReplay/ReplicaReplaySession.cs）：divergent
//!    衔接校验、UnsafeEnqueueRaw 保真落盘、重放驱动推进与复制位点上报。
//!
//! Rust 以 [`ClusterReplicationSession`] 承接两层（会话内直连，无中间
//! 转发）；网络链路复用 wnode 会话消费面（`MessageConsumerFace`，真
//! socket 泵），内存链路由主端 [`crate::server::replication::replica_wire::CallbackWire`]
//! 帧回调直投 [`Self::process_append_log`]。

use std::{
  io::{self, Error, ErrorKind},
  str::from_utf8,
  sync::Arc,
};

use waof::WalLog;
use wdev::Device;
use wnode::MessageConsumerFace;
use wresp::parse_resp_frame;

use crate::server::{
  cluster_provider::ClusterProvider, replication::replication_manager::ReplicationManager,
};

/// 初始化帧三方地址哨兵（对标 C# previousAddress == -1 && currentAddress ==
/// nextAddress == -1 的初始化消息判定）
const INIT_ADDRESS_SENTINEL: i64 = -1;

/// 副本重放推进端口：记录保真落盘后的重放通知
pub trait ReplicaReplayHook: Send + Sync {
  /// 完整记录帧已落盘 [current_address, next_address)，驱动重放推进
  fn on_records_persisted(
    &self,
    physical_sublog_idx: usize,
    current_address: i64,
    next_address: i64,
  );
}

/// APPENDLOG 处理结果（应答面分流）
#[derive(Debug, PartialEq, Eq)]
pub enum AppendLogOutcome {
  /// 初始化帧处理成功（回 +OK）
  Initialized,
  /// 记录帧处理成功（无应答，对标 C# 普通记录不回写）
  Record,
}

/// 副本接收面会话
pub struct ClusterReplicationSession<D: Device> {
  rm: Arc<ReplicationManager>,
  provider: Arc<ClusterProvider>,
  /// 副本本地 AOF（主端记录帧保真落盘目标）
  wal: Arc<WalLog<D>>,
  /// 重放推进端口（None = 落盘-only 形态，仅位点记账）
  replay_hook: Option<Arc<dyn ReplicaReplayHook>>,
}
impl<D: Device> ClusterReplicationSession<D> {
  /// 创建副本会话（rm 与 provider 同源装配：provider.replication_manager == rm）
  pub fn new(
    provider: Arc<ClusterProvider>,
    wal: Arc<WalLog<D>>,
    replay_hook: Option<Arc<dyn ReplicaReplayHook>>,
  ) -> Arc<Self> {
    let rm = provider
      .replication_manager()
      .expect("集群装配期 ReplicationManager 必已就绪");
    Arc::new(Self {
      rm,
      provider,
      wal,
      replay_hook,
    })
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterAppendLog
  ///
  /// CLUSTER APPENDLOG 处理入口：初始化帧分支 + 记录帧分支
  ///（角色校验与 divergent 校验内联于 [`Self::process_append_log`]）
  pub fn process_append_log(
    &self,
    node_id: &str,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<AppendLogOutcome> {
    // 角色校验（对标 C#：非 REPLICA 或 replicating 别的 primary 均抛异常断流）
    if !self.provider.is_replica() {
      return Err(Error::new(
        ErrorKind::PermissionDenied,
        "aofsync node not a replica",
      ));
    }
    self.validate_primary_id(node_id)?;

    // 初始化消息：注册重放驱动（已存在即失败，对标 C# 重复初始化抛异常）
    if previous_address == INIT_ADDRESS_SENTINEL
      && current_address == INIT_ADDRESS_SENTINEL
      && next_address == INIT_ADDRESS_SENTINEL
    {
      if !self
        .rm
        .initialize_replica_replay_driver(physical_sublog_idx)
      {
        return Err(Error::new(
          ErrorKind::AlreadyExists,
          format!(
            "Received initialization message but ReplicaReplayDriver is already initialized! [physicalSublogIdx: {physical_sublog_idx}]"
          ),
        ));
      }
      return Ok(AppendLogOutcome::Initialized);
    }

    self.process_primary_stream(
      physical_sublog_idx,
      previous_address,
      current_address,
      next_address,
      payload,
    )?;
    Ok(AppendLogOutcome::Record)
  }

  /// 校验本节点是否正在复制指定的 primary 节点 ID（热路径零分配）
  fn validate_primary_id(&self, node_id: &str) -> io::Result<()> {
    let is_match = self
      .provider
      .cluster_manager()
      .and_then(|cm| {
        cm.current_config()
          .local_node_primary_id()
          .map(|id| id == node_id)
      })
      .unwrap_or(false);
    if !is_match {
      let current = self
        .provider
        .cluster_manager()
        .and_then(|cm| {
          cm.current_config()
            .local_node_primary_id()
            .map(String::from)
        })
        .unwrap_or_default();
      return Err(Error::new(
        ErrorKind::PermissionDenied,
        format!("aofsync node replicating {current}"),
      ));
    }
    Ok(())
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplaySession.cs:ProcessPrimaryStream
  ///
  /// 主端记录流落盘与重放推进：
  /// 1. divergent 衔接校验（C# 85-90 行：本端尾位须等于主端 currentAddress；
  ///    C# 页对齐双分支在 WalLog 连续字节编址下收敛为单一尾位等值判定）；
  /// 2. enqueue_raw 保真落盘（C# UnsafeEnqueueRaw noCommit:true——落盘不
  ///    即刷，由副本 commit 循环/调用方驱动）；
  /// 3. 重放推进通知（C# 异步路径 InitializeBackgroundReplayTask）；
  /// 4. 复制位点上报推进（C# SetSublogReplicationOffset）。
  fn process_primary_stream(
    &self,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<()> {
    // 恢复中不可流式落盘（对标 C# CannotStreamAOF 校验）
    if self.rm.cannot_stream_aof() {
      return Err(Error::new(
        ErrorKind::ResourceBusy,
        "Replica is recovering cannot sync AOF",
      ));
    }

    let tail = self.wal.tail_address() as i64;
    if tail != current_address {
      return Err(Error::new(
        ErrorKind::InvalidData,
        format!(
          "Divergent AOF Stream recordLength:{}; previousAddress:{previous_address}; currentAddress:{current_address}; nextAddress:{next_address}; tailAddress:{tail}",
          payload.len()
        ),
      ));
    }

    // 保真落盘：帧字节逐字进日志（返回地址须与主端 currentAddress 严格衔接）
    let landed = self
      .wal
      .enqueue_raw(payload)
      .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
    if landed as i64 != current_address {
      return Err(Error::new(
        ErrorKind::InvalidData,
        format!("Landed address {landed} diverged from primary currentAddress {current_address}"),
      ));
    }

    // 重放推进（对标 C# 异步重放任务唤醒或同步直接消费）
    if let Some(hook) = self.replay_hook.as_ref() {
      hook.on_records_persisted(physical_sublog_idx, current_address, next_address);
    } else if let Some(driver) = self
      .rm
      .replica_replay_driver_store
      .get_replay_driver(physical_sublog_idx)
      && let Err(err) = driver.consume_direct(
        payload,
        current_address,
        next_address,
        // 直接模式仅推进位点，无需额外回调处理
        |_record, _addr| {},
      )
    {
      log::warn!("直接重放 AOF 记录失败: {err}");
    }

    // 复制位点上报推进（重放位点权威面在 replay driver，此处先记流式落盘位点）
    self
      .rm
      .set_sublog_replication_offset(physical_sublog_idx, next_address);
    Ok(())
  }

  /// 处理一条已解析的 APPENDLOG 帧参数表（7 元素初始化帧 / 8 元素记录帧，
  /// 对标 C# NetworkClusterAppendLog 的 parseState 5-6 参数两形态）
  fn dispatch_append_log_args(&self, args: &[&[u8]]) -> io::Result<AppendLogOutcome> {
    let node_id =
      from_utf8(args[2]).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
    let physical_sublog_idx = parse_i64_fast(args[3])? as usize;
    let previous_address = parse_i64_fast(args[4])?;
    let current_address = parse_i64_fast(args[5])?;
    let next_address = parse_i64_fast(args[6])?;
    // 初始化帧（7 元素）无 payload，记录帧（8 元素）末位为完整记录帧
    let payload: &[u8] = args.get(7).copied().unwrap_or(&[]);
    self.process_append_log(
      node_id,
      physical_sublog_idx,
      previous_address,
      current_address,
      next_address,
      payload,
    )
  }
}

/// 非预期命令错误行响应
const ERR_UNEXPECTED_CLUSTER_CMD: &[u8] = b"-ERR unexpected cluster command\r\n";
/// 格式畸形 APPENDLOG 帧错误行响应
const ERR_MALFORMED_APPENDLOG_FRAME: &[u8] = b"-ERR malformed APPENDLOG frame\r\n";
/// 初始化成功 +OK 响应
const RESP_OK: &[u8] = b"+OK\r\n";

/// 快速、零拷贝字节解析 i64
#[inline]
fn parse_i64_fast(bytes: &[u8]) -> io::Result<i64> {
  if bytes.is_empty() {
    return Err(Error::new(
      ErrorKind::InvalidInput,
      "value is not an integer",
    ));
  }
  let (neg, digits) = match bytes[0] {
    b'-' => (true, &bytes[1..]),
    b'+' => (false, &bytes[1..]),
    _ => (false, bytes),
  };
  if digits.is_empty() {
    return Err(Error::new(
      ErrorKind::InvalidInput,
      "value is not an integer",
    ));
  }
  let mut val: i64 = 0;
  for &b in digits {
    if !b.is_ascii_digit() {
      return Err(Error::new(
        ErrorKind::InvalidInput,
        "value is not an integer",
      ));
    }
    val = val.wrapping_mul(10).wrapping_add((b - b'0') as i64);
  }
  Ok(if neg { val.wrapping_neg() } else { val })
}

impl<D: Device> MessageConsumerFace for ClusterReplicationSession<D> {
  /// libs/server/Servers/ServerTcpNetworkHandler.cs 的 TryConsumeMessages 集群
  /// 分发路径：解析二进制安全 RESP 数组帧，CLUSTER APPENDLOG 交
  /// [`Self::process_append_log`]；初始化帧回 +OK，记录帧无应答（对标 C#
  /// NetworkClusterAppendLog 的应答面），异常回 -ERR 错误行
  fn try_consume_messages(&self, req_buffer: &[u8]) -> (usize, Vec<u8>) {
    let Some((consumed, items)) = parse_resp_frame(req_buffer) else {
      return (0, Vec::new());
    };

    // 客户端握手命令面：CLIENT SETINFO / SETNAME 回 +OK（GarnetClientSession
    // ConnectAsync 握手契约，C# 服务端全 RESP 会话承接；本会话仅承载握手放行）
    if !items.is_empty() && items[0] == b"CLIENT" {
      return (consumed, b"+OK\r\n".to_vec());
    }

    // 非 CLUSTER APPENDLOG 帧非本会话协议面（协议契约外输入）
    if items.len() < 7
      || !items[0].eq_ignore_ascii_case(b"CLUSTER")
      || !items[1].eq_ignore_ascii_case(b"APPENDLOG")
    {
      return (consumed, ERR_UNEXPECTED_CLUSTER_CMD.to_vec());
    }
    if items.len() > 8 {
      return (consumed, ERR_MALFORMED_APPENDLOG_FRAME.to_vec());
    }

    match self.dispatch_append_log_args(&items) {
      Ok(AppendLogOutcome::Initialized) => (consumed, RESP_OK.to_vec()),
      Ok(AppendLogOutcome::Record) => (consumed, Vec::new()),
      Err(e) => (consumed, format!("-ERR {e}\r\n").into_bytes()),
    }
  }

  fn dispose(&self) {
    // 断流处置：活跃复制流存在时释放重放驱动（对标 C# ClusterSession.Dispose
    // 的 `if (IsReplicating) replicaReplayDriverStore?.Dispose()`；驱动仓库
    // 重建留待 REPLICAOF/恢复路径，重连时 init 帧重注册）
    if self.rm.has_active_replication_stream() {
      self.rm.replica_replay_driver_store.dispose();
    }
  }
}
