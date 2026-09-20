use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crossfire::{
  MAsyncTx, mpsc,
  oneshot::{RxOneshot, TxOneshot},
};
use event_listener::{Event, EventListener};

use crate::{Error, Result, network::encode_command};

/// GarnetClientSession 会话层在途命令通道缺省容量（网络泵批量编码与 FIFO
/// 派发的窗口大小；C# GarnetClientSession 以 ElasticCircularBuffer 排队、无
/// 在途上限形参，此处即会话侧容量单源。GarnetClient 的在途准入闸由构造
/// 形参 max_outstanding_tasks 承担，不读此常量抬容量）
pub(crate) const CHANNEL_CAP: usize = 1024;

/// 客户端发送侧未刷出字节硬顶（发送链字节维度的唯一上限单源，写泵拼批分片
/// 与复制溢流队列共用此常量，杜绝两处魔数）
///
/// 对标 C# 每连接 NetworkWriter 环形发送缓冲的字节顶 = 页数 × 每页尺寸：
/// `libs/client/NetworkWriter.cs:55` BufferSize = 4 页，页尺寸即构造形参
/// sendPageSize。通用客户端 `libs/client/GarnetClient.cs:149` sendPageSize =
/// 1 << 21（4 × 2MB = 8MB）；复制域按用途显式配尺寸
///（`libs/cluster/Server/Replication/ReplicationNetworkBufferSettings.cs`：
/// AOF 同步客户端 :41 `2 << AofPageSizeBits()`、副本同步会话 :26 `1 << 20`、
/// InitiateReplicaSync :33 `1 << 17`）。C# 由页满时 TryAllocate 的 RETRY_LATER
/// 把未刷出字节钉死在 4 页内，rust 以本常量承接同等字节顶。
///
/// rust 默认 wal 页 16MiB（waof `DEFAULT_BUFFER_SIZE` = 1 << 24），页尺寸取
/// C# AOF 同式 `2 << 页位` = 2 << 24 = 32MiB，页数 4，合计 128MiB。AOF 尺寸
/// 旋钮读侧接线落地后，本式可改接真实页位；未落地前按缺省页尺寸取常量。
pub const MAX_UNFLUSHED_SEND_BYTES: usize = 4 * (2 << 24);

/// 命令应答回传端口：按期望应答类型区分（标量 / 字符串数组），与网络泵的
/// 应答解析器一一对应；None 为发出即忘（协议约定无应答的命令）
pub enum ReplyTx {
  Str(TxOneshot<Result<String>>),
  Bytes(TxOneshot<Result<Vec<u8>>>),
  Array(TxOneshot<Result<Vec<String>>>),
  /// 发出即忘：不注册应答等待，写出即完成
  ///
  /// 对标 C# GarnetClientSession.ExecuteClusterAppendLog 的无 tcs 入队路径
  ///（集群 AOF 逐记录转发不注册 TaskCompletionSource，也不递增 numCommands，
  /// replica 侧对 APPENDLOG 记录帧不回写应答）
  None,
}

/// 在途命令：RESP 请求帧 + 应答回传端口
pub struct CommandItem {
  pub frame: Vec<u8>,
  pub resp_tx: ReplyTx,
}

impl CommandItem {
  #[inline]
  pub fn new<T: AsRef<[u8]>>(cmd: &[T], resp_tx: ReplyTx) -> Self {
    let mut frame = Vec::new();
    encode_command(&mut frame, cmd);
    Self { frame, resp_tx }
  }

  #[inline]
  pub fn new_str(cmd: &[&str], resp_tx: ReplyTx) -> Self {
    Self::new(cmd, resp_tx)
  }

  #[inline]
  pub fn new_bytes(cmd: &[&[u8]], resp_tx: ReplyTx) -> Self {
    Self::new(cmd, resp_tx)
  }
}

/// 泵侧进度计数：客户端在途超时的三个判定源（C# TimeoutChecker 读
/// tcsOffset / networkWriter.GetNextTaskId / networkWriter.GetTailAddress，
/// rust 侧对应量为泵的原子计数，与 in_flight 队列同源推进，不设第二套队列），
/// 由 [`GarnetClient`](crate::client::GarnetClient) 的超时检查任务周期比较
pub struct PumpProgress {
  /// 写泵已入队在途队列的带应答命令数（对标 networkWriter.GetNextTaskId）
  enqueued: AtomicU64,
  /// 读泵已回收应答数（对标 tcsOffset）
  reclaimed: AtomicU64,
  /// 写泵累计发出字节数（对标 networkWriter.GetTailAddress）
  sent_bytes: AtomicU64,
  /// 超时判成标志：检查任务判成置位，读泵见位收场退出、在途往返按超时结算
  timed_out: AtomicBool,
  /// 超时判成事件（读泵挂起唤醒单点）：判成置位即 notify，读泵无需定时
  /// 探测窗——挂起中的读请求与池化接收缓冲因此绝不因轮询而弃置
  timeout_event: Event,
  /// 退休标志：网络循环收场置位，超时检查任务见位退场（对标 C#
  /// timeoutCheckerCts 取消退出，不持通道引用以免阻断泵的自灭闭环）
  retired: AtomicBool,
}

impl PumpProgress {
  pub fn new() -> Self {
    Self {
      enqueued: AtomicU64::new(0),
      reclaimed: AtomicU64::new(0),
      sent_bytes: AtomicU64::new(0),
      timed_out: AtomicBool::new(false),
      timeout_event: Event::new(),
      retired: AtomicBool::new(false),
    }
  }

  /// 写泵：带应答命令入队在途队列后推进
  #[inline]
  pub(crate) fn record_enqueued(&self) {
    self.enqueued.fetch_add(1, Ordering::Release);
  }

  /// 读泵：应答回传完成即回收推进
  #[inline]
  pub(crate) fn record_reclaimed(&self) {
    self.reclaimed.fetch_add(1, Ordering::Release);
  }

  /// 写泵：帧批量刷出成功后累计
  #[inline]
  pub(crate) fn record_sent(&self, n: usize) {
    self.sent_bytes.fetch_add(n as u64, Ordering::Release);
  }

  /// 三进度快照（回收序号 / 发出字节 / 入队序号，即 C# 判定式的读取顺序）
  #[inline]
  pub(crate) fn snapshot(&self) -> (u64, u64, u64) {
    (
      self.reclaimed.load(Ordering::Acquire),
      self.sent_bytes.load(Ordering::Acquire),
      self.enqueued.load(Ordering::Acquire),
    )
  }

  /// 超时判成置位并唤醒挂起读泵（判成事件与粘滞标志同点推进，读泵侧以
  /// 「先注册监听、再复查标志」次序消除丢唤醒窗口）
  #[inline]
  pub(crate) fn flag_timeout(&self) {
    self.timed_out.store(true, Ordering::Release);
    self.timeout_event.notify(1);
  }

  /// 判成事件订阅（读泵挂起前注册）
  #[inline]
  pub(crate) fn listen_timeout(&self) -> EventListener {
    self.timeout_event.listen()
  }

  /// 超时判成查询
  #[inline]
  pub(crate) fn is_timed_out(&self) -> bool {
    self.timed_out.load(Ordering::Acquire)
  }

  /// 退休置位：网络循环收场，超时检查任务随之退场
  #[inline]
  pub(crate) fn retire(&self) {
    self.retired.store(true, Ordering::Release);
  }

  /// 退休查询
  #[inline]
  pub(crate) fn is_retired(&self) -> bool {
    self.retired.load(Ordering::Acquire)
  }
}

impl Default for PumpProgress {
  fn default() -> Self {
    Self::new()
  }
}

/// 命令通道发送端类型别名（客户端与会话共用）
///
/// 有界 mpsc 的容量语义对应 C# 在途命令窗口上限：GarnetClient 预分配
/// tcsArray 槽位（libs/client/GarnetClient.cs:56/:180，maxOutstandingTasks）、
/// GarnetClientSession 以 ElasticCircularBuffer<TaskCompletionSource> 排队
///（libs/client/ClientSession/GarnetClientSession.cs:27）；逐命令
/// crossfire oneshot 即 TaskCompletionSource 的零锁等价物。
pub(crate) type ChannelTx = MAsyncTx<mpsc::Array<CommandItem>>;

/// 发送命令帧并等待应答回传：请求往返的公共路径（客户端与会话共用）
///
/// `progress` 在位（客户端超时旋钮开启）且应答端口断连时，按泵侧判成标志
/// 区分断连原因：判成即在途命令超时（对标 C# TimeoutChecker 判成 Dispose
/// 后在途 TaskCompletionSource 全部置 GarnetClientTimeoutException），否则
/// 按读泵退出断连结算
#[inline]
pub(crate) async fn roundtrip<T>(
  tx: &ChannelTx,
  item: CommandItem,
  resp_rx: RxOneshot<Result<T>>,
  progress: Option<&PumpProgress>,
) -> Result<T> {
  tx.send(item).await.map_err(|_| Error::ReadPumpExited)?;
  match resp_rx.await {
    Ok(res) => res,
    Err(_) if progress.is_some_and(|p| p.is_timed_out()) => Err(Error::Timeout),
    Err(_) => Err(Error::ResponseChannelClosed),
  }
}
