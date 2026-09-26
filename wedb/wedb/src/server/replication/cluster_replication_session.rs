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
//! socket 泵），测试链路同走真 socket（`TcpSessionWire` 建连 GarnetServer）。

use std::{
  io::{self, Error, ErrorKind},
  mem::take,
  sync::Arc,
};

use parking_lot::Mutex;
use waof::WalLog;
use wbase::{hex::hex_u128, num::strict_i64};
use wdev::Device;
use wnode::{MessageConsumerFace, resp::slow_path::SlowWait};
use wresp::{
  cmd_strings::{
    RESP_OK,
    cluster::{
      ERR_MALFORMED_APPENDLOG_FRAME, ERR_PROTOCOL_ERROR_PREFIX, ERR_UNEXPECTED_CLUSTER_CMD,
    },
    write_error_raw,
  },
  frame::parse_resp_frame,
};

use crate::server::{
  cluster_provider::ClusterProvider,
  replication::{
    replica_replay_driver_store::ReplicaReplayDriverStore, replication_manager::ReplicationManager,
  },
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
  /// 会话自有接收缓冲（泵经 take/return 直填，网络字节零拷贝直入；
  /// 缓冲驻留会话跨批持久，对齐 C# IMessageConsumer 单形态的缓冲模型）
  pub recv_buffer: Vec<u8>,
  /// 接收缓冲空闲段初始化代际（TLS 读整段清零随缓冲付一次的跨读记忆，
  /// `wbase::primed` 契约；缓冲实例永不更换，clear 不失忆）
  pub recv_prime_key: Option<(usize, usize)>,
  /// 已消费游标（整段消费完随缓冲复位）
  read_head: usize,
  /// 致命断流哨兵（APPENDLOG 拒收 / 畸形帧；消费 None 通道承运，
  /// [`MessageConsumerFace::take_fatal_disconnect`] 为泵逐批复查兜底）
  fatal_disconnect: bool,
  /// 会话自持代的重放驱动仓（对标 C# ClusterSession 会话私有字段
  /// replicaReplayDriverStore，RespClusterReplicationCommands.cs:221-224：
  /// APPENDLOG 初始化帧注册成功时捕获当时代际引用；断连 dispose 仅终结
  /// 自持代际——换代后旧连接迟到 dispose 落在已处置旧实例上幂等空转，
  /// 绝不误杀新主连接已注册的新一代驱动）
  replay_driver_store: Mutex<Option<Arc<ReplicaReplayDriverStore>>>,
  /// 主端节流挂起体（ThrottlePrimary 协程化：帧落盘后 lag 越限时由
  /// process_primary_stream 暂存，网络泵 [`Self::take_slow_wait`] 取走
  /// await——挂起协程不占线程，位点收敛 / 驱动处置即放行）
  pending_throttle: Mutex<Option<SlowWait>>,
}

impl<D: Device> Clone for ClusterReplicationSession<D> {
  fn clone(&self) -> Self {
    Self {
      rm: Arc::clone(&self.rm),
      provider: Arc::clone(&self.provider),
      wal: Arc::clone(&self.wal),
      replay_hook: self.replay_hook.clone(),
      recv_buffer: self.recv_buffer.clone(),
      recv_prime_key: None,
      read_head: self.read_head,
      fatal_disconnect: self.fatal_disconnect,
      replay_driver_store: Mutex::new(None),
      pending_throttle: Mutex::new(None),
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
      recv_buffer: Vec::new(),
      recv_prime_key: None,
      read_head: 0,
      fatal_disconnect: false,
      replay_driver_store: Mutex::new(None),
      pending_throttle: Mutex::new(None),
    }
  }

  /// CLUSTER APPENDLOG 处理编排（C# NetworkClusterAppendLog 函数体下半段的
  /// 下游组合：初始化帧分支对齐 InitializeReplicaReplayDriver，记录帧分支
  /// 委托 [`Self::process_primary_stream`]；角色校验内联于此）
  pub fn process_append_log(
    &self,
    node_id: u128,
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
      // 注册成功即捕获当时代际驱动仓为会话私有引用（对标 C#
      // RespClusterReplicationCommands.cs:221-224
      // `if (InitializeReplicaReplayDriver(...))
      //  replicaReplayDriverStore = rm.ReplicaReplayDriverStore;`）：
      // 本连接的记录帧重放与断连处置只作用于该代际，换代互不波及
      *self.replay_driver_store.lock() = Some(self.rm.current_replica_replay_driver_store());
      // 与主端同步建立，心跳时间戳刷新（对标 C# UpdateLastPrimarySyncTime 的
      // 同步建立挂点：ReplicaDiskbasedSync 恢复入口与 ReceiveCheckpointHandler
      // 三接收入口，C# EnsureReplication 轮询本体不刷新；rust 已对齐映射——
      // replica_diskbased_sync.rs 恢复点与 receive_checkpoint_handler.rs 的
      // process_snapshot_data/process_file_segment/process_metadata 三入口各自
      // 刷新，此处为 AOF 同步链初始化帧握手的同源挂点，承担 C# IsReplicating
      // 置位时刻的同步建立心跳）
      self.rm.update_last_primary_sync_time();
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
  fn validate_primary_id(&self, node_id: u128) -> io::Result<()> {
    use wbase::hex::hex_str_u128;
    let primary = self
      .provider
      .cluster_manager()
      .and_then(|cm| cm.current_config().local_node_primary_id());
    if primary != Some(node_id) {
      let current = primary.map_or_else(String::new, hex_str_u128);
      return Err(Error::new(
        ErrorKind::PermissionDenied,
        format!("aofsync node replicating {current}"),
      ));
    }
    Ok(())
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplaySession.cs:ProcessPrimaryStream
  /// libs/server/Cluster/IClusterSession.cs:ProcessPrimaryStream
  ///（接口声明折叠：rust 单实现，副本回放不经 IClusterSession 面而由复制域
  /// 直调本口）
  ///
  /// 主端记录流落盘与重放推进：
  /// 1. FastAofTruncate 跳跃重对齐（C# 54-74 行：主端截断推流产生跳跃帧时
  ///    SafeInitialize 把本地日志地址重置对齐到 currentAddress 后续流）；
  /// 2. divergent 衔接校验（C# 85-90 行：本端尾位须等于主端 currentAddress；
  ///    C# 页对齐双分支在 WalLog 连续字节编址下收敛为单一尾位等值判定）；
  /// 3. enqueue_raw 保真落盘（C# UnsafeEnqueueRaw noCommit:true——落盘不
  ///    即刷，由副本 commit 循环/调用方驱动）；
  /// 4. 背景重放启动与主端节流（C# 异步路径 InitializeBackgroundReplayTask
  ///    及 ThrottlePrimary；同步形态 maxLag==0 折叠为阻塞至追平，登记见
  ///    replica_replay_task 模块头）；
  /// 5. 复制位点推进双形态：背景重放在场 = applied 语义，位点由重放链
  ///    应用进存储后经驱动权威面回推（对标 C# SetSublogReplicationOffset
  ///    的重放链推进源，enqueued 收敛路径落地）；退化形态（重放资产缺席）
  ///    位点暂记流式落盘位点（enqueued）——掉电丢 wal 未刷帧窗口内位点
  ///    超前于存储态，属已知风险（登记见 task/done/replica-offset-
  ///    semantics.md）。
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

    // FastAofTruncate 跳跃重对齐（对标 C# 54-74 行）：主端 checkpoint 后截断
    // AOF 推流（UnsafeShiftBeginAddress snapToPageStart），活跃迭代器跨被截断
    // 区段产生跳跃帧 currentAddress > previousAddress。C# 页编址下「页边界 +
    // 短跳」可由 enqueue 自动吸收；WalLog 连续字节编址无页对齐填充，任何跳跃
    // 均无法自动衔接 → 统一显式重对齐本地地址空间（C# 显式分支的保守超集）。
    // 正常流 previousAddress == currentAddress（上一帧 next 衔接本帧 current）
    // 判定恒假，零开销跳过。
    if self.provider.fast_aof_truncate() && current_address > previous_address {
      log::warn!(
        "MainMemoryReplication: Skipping from {} to {current_address}",
        self.rm.get_sublog_replication_offset(physical_sublog_idx),
      );
      // 对标 C# GarnetLog.SafeInitialize：本地日志地址空间重置对齐跳跃点
      //（副本会话串行处理推流帧，单写者无并发 append，内存位点前跳即语义闭合）
      self
        .wal
        .safe_initialize(current_address as u64, current_address as u64);
      // C# 重对齐后 WaitForVectorOperationsToComplete 再推进位点；rust 向量域
      // 无重放期在途操作面，直接推进
      self
        .rm
        .set_sublog_replication_offset(physical_sublog_idx, current_address);
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

    // 重放推进与主端背压（对标 C# 异步路径 InitializeBackgroundReplayTask +
    // ThrottlePrimary；同步形态 maxLag==0 折叠为背景重放 + 锁步至追平——
    // 消费面为同步 trait 无法内联异步存储应用，节流挂起体交网络泵 await
    // 承接锁步：放行前不消费下一帧。折叠登记见 replica_replay_task 模块
    // 头）。重放钩子形态（测试）保持推进通知直通。
    //
    // 重放驱动是必须在册的组件（对标 C# ReplicaReplaySession.cs:106-125
    // 直调会话代际仓 replicaReplayDriverStore.GetReplayDriver(...)
    // .Consume / InitializeBackgroundReplayTask——驱动缺席即异常上抛转化为
    // clientResponse:false 致命断流，不存在落盘后跳过重放直推位点的
    // 静默旁路）：本会话自持代取不到驱动且重放资产在场，落盘不应用即
    // 数据丢失，必须致命断流交重同步收敛；仅未挂载重放资产的纯落盘
    // 记账形态（退化装配）保留直推
    let mut background_replay = false;
    if let Some(hook) = self.replay_hook.as_ref() {
      hook.on_records_persisted(physical_sublog_idx, current_address, next_address);
    } else {
      let driver = self
        .replay_driver_store
        .lock()
        .as_ref()
        .and_then(|store| store.get_replay_driver(physical_sublog_idx));
      match driver {
        Some(driver) => {
          driver.initialize_background_replay_task(previous_address);
          // 主端节流：maxLag == -1 / lag 已收敛直通（None）；越限暂存挂起体
          *self.pending_throttle.lock() =
            driver.throttle_wait(self.provider.aof_replay_max_lag_bytes());
          background_replay = driver.background_replay_started();
        }
        None => {
          if self.rm.replay_assets().is_some() {
            log::error!(
              "APPENDLOG 记录帧重放驱动缺席（会话自持代仓已处置或从未注册），致命断流 \
               physicalSublogIdx: {physical_sublog_idx}"
            );
            return Err(Error::new(
              ErrorKind::NotFound,
              format!(
                "Received record message but ReplicaReplayDriver is not registered! [physicalSublogIdx: {physical_sublog_idx}]"
              ),
            ));
          }
        }
      }
    }

    // 复制位点推进双形态：背景重放在场 = applied 语义，位点由重放链应用
    // 进存储后经驱动权威面回推（对标 C#，enqueued 收敛路径落地；本帧位点
    // 由背景任务按 ConsumeDirect 语义推进至 nextAddress）；退化形态（无
    // 重放资产）保 enqueued——落盘面直推。
    if !background_replay {
      self
        .rm
        .set_sublog_replication_offset(physical_sublog_idx, next_address);
    }
    Ok(())
  }

  /// 处理一条已解析的 APPENDLOG 帧参数表（7 元素初始化帧 / 8 元素记录帧，
  /// 对标 C# NetworkClusterAppendLog 的 parseState 5-6 参数两形态）
  fn dispatch_append_log_args(&self, args: &[&[u8]]) -> io::Result<AppendLogOutcome> {
    // 协议帧参数为 hex 节点 id，入口即解析为内部 u128 身份
    let node_id = hex_u128(args[2]).ok_or_else(|| {
      Error::new(
        ErrorKind::InvalidData,
        "node id is not a 32-char hex string",
      )
    })?;
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

/// 字节解析 i64（复用 wbase::num::strict_i64 保证安全与无溢出）
#[inline]
fn parse_i64_fast(bytes: &[u8]) -> io::Result<i64> {
  strict_i64(bytes).ok_or_else(|| Error::new(ErrorKind::InvalidInput, "value is not an integer"))
}

impl<D: Device> MessageConsumerFace for ClusterReplicationSession<D> {
  /// libs/server/Servers/ServerTcpNetworkHandler.cs 的 TryConsumeMessages 集群
  /// 分发路径：消费会话自有接收缓冲中自游标起的全部完整帧，解析二进制安全
  /// RESP 数组帧，CLUSTER APPENDLOG 交 [`Self::process_append_log`]；初始化
  /// 帧回 +OK，记录帧无应答（对标 C# NetworkClusterAppendLog 的应答面）；
  /// 畸形帧与处理失败置致命断流（C# RespParsingException /
  /// GarnetException clientResponse:false 上抛 → RespServerSession catch →
  /// 断连），泵发尽应答后断连。应答直写调用方缓冲（记录帧热路径零堆分配）
  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    loop {
      let parsed = match parse_resp_frame(&self.recv_buffer[self.read_head..]) {
        Ok(Some(parsed)) => parsed,
        // 半包未到齐：等更多网络字节
        Ok(None) => break,
        // 畸形帧（*-1 前导 / 坏 sigil / $-1 元素 / 溢出）：协议违规断流
        //（C# RespParsingException → catch 块写协议错误行后断连），None
        // 表达致命错误，泵发尽应答后断连
        Err(e) => {
          log::error!("APPENDLOG 帧解析违规断流: {e}");
          write_error_raw(resp_buf, &format!("{}{}", ERR_PROTOCOL_ERROR_PREFIX, e));
          self.fatal_disconnect = true;
          return None;
        }
      };
      let (consumed, items) = parsed;
      self.read_head += consumed;

      // 客户端握手命令面：CLIENT SETINFO / SETNAME 回 +OK（GarnetClientSession
      // ConnectAsync 握手契约，C# 服务端全 RESP 会话承接；本会话仅承载握手放行）
      if !items.is_empty() && items[0] == b"CLIENT" {
        resp_buf.extend_from_slice(RESP_OK);
        continue;
      }

      // 非 CLUSTER APPENDLOG 帧非本会话协议面（协议契约外输入）
      if items.len() < 7
        || !items[0].eq_ignore_ascii_case(b"CLUSTER")
        || !items[1].eq_ignore_ascii_case(b"APPENDLOG")
      {
        write_error_raw(resp_buf, ERR_UNEXPECTED_CLUSTER_CMD);
        continue;
      }
      if items.len() > 8 {
        write_error_raw(resp_buf, ERR_MALFORMED_APPENDLOG_FRAME);
        continue;
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
          return None;
        }
      }
    }

    // 整段消费完毕：缓冲清零复位（半包残余驻留头部不可平移）
    if self.read_head >= self.recv_buffer.len() {
      self.recv_buffer.clear();
      self.read_head = 0;
      return Some(0);
    }
    Some(self.recv_buffer.len() - self.read_head)
  }

  /// 致命断流信号消费：畸形帧 / APPENDLOG 拒收登记 / 驱动仓处置后由泵取走，发尽应答断连
  fn take_fatal_disconnect(&mut self) -> bool {
    if self.fatal_disconnect {
      self.fatal_disconnect = false;
      return true;
    }
    self
      .replay_driver_store
      .lock()
      .as_ref()
      .is_some_and(|s| s.is_disposed())
  }

  /// 取走主端节流挂起体（ThrottlePrimary 协程化承接：网络泵 await 挂起
  /// 协程——compio 挂起不占线程，位点收敛 / 驱动处置即放行后继续消费
  /// 流水线余量；C# Thread.Yield 自旋在单线程每核 runtime 的等价替换）
  fn take_slow_wait(&mut self) -> Option<SlowWait> {
    self.pending_throttle.lock().take()
  }

  fn take_recv_scratch(&mut self) -> Vec<u8> {
    take(&mut self.recv_buffer)
  }

  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.recv_buffer = buf;
  }

  fn recv_prime_key(&self) -> Option<(usize, usize)> {
    self.recv_prime_key
  }

  fn set_recv_prime_key(&mut self, key: Option<(usize, usize)>) {
    self.recv_prime_key = key;
  }

  fn dispose(&mut self) {
    // 断流处置（对标 C# ClusterSession.cs:212-219 `if (IsReplicating)
    // replicaReplayDriverStore?.Dispose()`）：仅 dispose 本连接注册成功时
    // 自持的当时代驱动仓——排空该代在册驱动并终止背景重放，容器随之关闭
    // （重注册须由 recovery 换代，杜绝孤儿驱动）；换代后旧连接迟到的
    // dispose 落在已处置的旧实例上幂等空转，绝不误杀 ReplicationManager
    // 当前代的新一代驱动。resp 会话路径的同一清理由
    // ClusterSessionFace::dispose 的会话自持代际承接，两路径消费面不同、
    // 代际隔离语义同源
    if let Some(store) = self.replay_driver_store.lock().take() {
      store.dispose();
    }
  }
}
