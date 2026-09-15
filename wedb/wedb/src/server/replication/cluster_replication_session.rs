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
  mem::take,
  str::from_utf8,
  sync::Arc,
};

use waof::WalLog;
use wbase::num::strict_i64;
use wdev::Device;
use wnode::MessageConsumerFace;
use wresp::parse_resp_frame;

use crate::server::{
  cluster_provider::ClusterProvider, replication::replication_manager::ReplicationManager,
};

/// 初始化帧三方地址哨兵（对标 C# previousAddress == -1 && currentAddress ==
/// nextAddress == -1 的初始化消息判定）
const INIT_ADDRESS_SENTINEL: i64 = -1;

use std::sync::atomic::{AtomicI64, Ordering};

/// 副本重放推进具体钩子（消除动态分发）
#[derive(Clone)]
pub enum ReplicaReplayHook {
  /// 推进原子位点（测试用）
  OffsetWatermark(Arc<AtomicI64>),
  /// 函数指针
  Fn(fn(usize, i64, i64)),
}

impl ReplicaReplayHook {
  /// 完整记录帧已落盘 [current_address, next_address)，驱动重放推进
  pub fn on_records_persisted(
    &self,
    physical_sublog_idx: usize,
    current_address: i64,
    next_address: i64,
  ) {
    match self {
      Self::OffsetWatermark(watermark) => {
        watermark.fetch_max(next_address, Ordering::AcqRel);
      }
      Self::Fn(f) => f(physical_sublog_idx, current_address, next_address),
    }
  }
}

impl From<Arc<AtomicI64>> for ReplicaReplayHook {
  fn from(w: Arc<AtomicI64>) -> Self {
    Self::OffsetWatermark(w)
  }
}

impl From<fn(usize, i64, i64)> for ReplicaReplayHook {
  fn from(f: fn(usize, i64, i64)) -> Self {
    Self::Fn(f)
  }
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
  replay_hook: Option<ReplicaReplayHook>,
  /// 致命断流哨兵（APPENDLOG 拒收 / 畸形帧；泵经
  /// [`MessageConsumerFace::take_fatal_disconnect`] 取走断连）
  fatal_disconnect: bool,
}

impl<D: Device> Clone for ClusterReplicationSession<D> {
  fn clone(&self) -> Self {
    Self {
      rm: Arc::clone(&self.rm),
      provider: Arc::clone(&self.provider),
      wal: Arc::clone(&self.wal),
      replay_hook: self.replay_hook.clone(),
      fatal_disconnect: self.fatal_disconnect,
    }
  }
}
impl<D: Device> ClusterReplicationSession<D> {
  /// 创建副本会话（rm 与 provider 同源装配：provider.replication_manager == rm）
  pub fn new(
    provider: Arc<ClusterProvider>,
    wal: Arc<WalLog<D>>,
    replay_hook: Option<ReplicaReplayHook>,
  ) -> Self {
    let rm = provider
      .replication_manager()
      .expect("集群装配期 ReplicationManager 必已就绪");
    Self {
      rm,
      provider,
      wal,
      replay_hook,
      fatal_disconnect: false,
    }
  }

  /// CLUSTER APPENDLOG 处理编排（C# NetworkClusterAppendLog 函数体下半段的
  /// 下游组合：初始化帧分支对齐 InitializeReplicaReplayDriver，记录帧分支
  /// 委托 [`Self::process_primary_stream`]；角色校验内联于此）
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
  /// 4. 复制位点上报推进。位点语义登记：C# SetSublogReplicationOffset 在
  ///    重放链应用记录进存储后推进（applied）；rust 副本运行期尚无存储
  ///    应用链（C# syncReplay 同步应用与后台 ReplicaReplayTask 均未转写，
  ///    唯一应用点在进程恢复 RecoverLogDriver），位点暂记流式落盘位点
  ///    （enqueued）——掉电丢 wal 未刷帧窗口内位点超前于存储态，属已知
  ///    风险；背景重放任务落地后在应用完成点回推，位点切回 applied。
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

    // 复制位点上报推进（enqueued 语义：仅承诺「记录帧已落盘」，非 C# 的
    // applied 语义——C# 在重放链应用进存储后推进；rust 副本运行期无存储
    // 应用链，位点暂记流式落盘位点，掉电窗口超前于存储态；背景重放任务
    // 落地后改挂应用完成点回推）
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

/// 字节解析 i64（复用 wbase::num::strict_i64 保证安全与无溢出）
#[inline]
fn parse_i64_fast(bytes: &[u8]) -> io::Result<i64> {
  strict_i64(bytes).ok_or_else(|| Error::new(ErrorKind::InvalidInput, "value is not an integer"))
}

impl<D: Device> MessageConsumerFace for ClusterReplicationSession<D> {
  /// libs/server/Servers/ServerTcpNetworkHandler.cs 的 TryConsumeMessages 集群
  /// 分发路径：解析二进制安全 RESP 数组帧，CLUSTER APPENDLOG 交
  /// [`Self::process_append_log`]；初始化帧回 +OK，记录帧无应答（对标 C#
  /// NetworkClusterAppendLog 的应答面）；畸形帧与处理失败置致命断流
  ///（C# RespParsingException / GarnetException clientResponse:false 上抛
  /// → RespServerSession catch → 断连），泵发尽应答后断连。应答直写
  /// 调用方缓冲（记录帧热路径零堆分配）
  fn try_consume_messages_into(&mut self, req_buffer: &[u8], resp_buf: &mut Vec<u8>) -> usize {
    let parsed = match parse_resp_frame(req_buffer) {
      Ok(Some(parsed)) => parsed,
      // 半包未到齐：等更多网络字节
      Ok(None) => return 0,
      // 畸形帧（*-1 前导 / 坏 sigil / $-1 元素 / 溢出）：协议违规断流
      //（C# RespParsingException → catch 块写协议错误行后断连），游标不
      // 推进，泵取走致命信号后断连
      Err(e) => {
        log::error!("APPENDLOG 帧解析违规断流: {e}");
        resp_buf.extend_from_slice(b"-ERR Protocol Error: ");
        resp_buf.extend_from_slice(e.to_string().as_bytes());
        resp_buf.extend_from_slice(b"\r\n");
        self.fatal_disconnect = true;
        return 0;
      }
    };
    let (consumed, items) = parsed;

    // 客户端握手命令面：CLIENT SETINFO / SETNAME 回 +OK（GarnetClientSession
    // ConnectAsync 握手契约，C# 服务端全 RESP 会话承接；本会话仅承载握手放行）
    if !items.is_empty() && items[0] == b"CLIENT" {
      resp_buf.extend_from_slice(RESP_OK);
      return consumed;
    }

    // 非 CLUSTER APPENDLOG 帧非本会话协议面（协议契约外输入）
    if items.len() < 7
      || !items[0].eq_ignore_ascii_case(b"CLUSTER")
      || !items[1].eq_ignore_ascii_case(b"APPENDLOG")
    {
      resp_buf.extend_from_slice(ERR_UNEXPECTED_CLUSTER_CMD);
      return consumed;
    }
    if items.len() > 8 {
      resp_buf.extend_from_slice(ERR_MALFORMED_APPENDLOG_FRAME);
      return consumed;
    }

    match self.dispatch_append_log_args(&items) {
      Ok(AppendLogOutcome::Initialized) => resp_buf.extend_from_slice(RESP_OK),
      // 记录帧无应答（对标 C# 普通记录不回写）
      Ok(AppendLogOutcome::Record) => {}
      // 处理失败（角色不符 / divergent / 恢复中 / 重复初始化）：不给应答
      // 直接断流（C# GarnetException clientResponse:false 口径）——若回
      // -ERR 行保连，fire-and-forget 推流端无人消费应答，主端
      // shipped_watermark 静默推进，主从分歧扩大
      Err(e) => {
        log::error!("APPENDLOG 处理失败断流: {e}");
        self.fatal_disconnect = true;
      }
    }
    consumed
  }

  /// 致命断流信号消费：畸形帧 / APPENDLOG 拒收登记后由泵取走，发尽应答断连
  fn take_fatal_disconnect(&mut self) -> bool {
    take(&mut self.fatal_disconnect)
  }

  fn dispose(&mut self) {
    // 断流处置：活跃复制流存在时释放重放驱动（对标 C# ClusterSession.Dispose
    // 的 `if (IsReplicating) replicaReplayDriverStore?.Dispose()`；驱动仓库
    // 重建留待 REPLICAOF/恢复路径，重连时 init 帧重注册）
    if self.rm.has_active_replication_stream() {
      self.rm.replica_replay_driver_store.dispose();
    }
  }
}
