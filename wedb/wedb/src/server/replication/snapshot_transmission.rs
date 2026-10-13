//! 快照发送驱动（全量同步第二段：主端检查点文件集网络下发）
//!
//! 对标 libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/：
//! - `SnapshotTransmissionDriver.cs`：按 reader → transmit source 序编排；
//! - `TsavoriteCheckpointReader.cs`：文件源/元数据源编目。C# 侧该文件名与类名
//!   不同名——类实名 TsavoriteSnapshotReader（构造期编目全部数据源，出源面
//!   GetTransmitSources，逐块读落在 FileDataSource.cs 的 ReadNextChunkAsync），
//!   按类名反查文件名会落空，锚点登记在类名侧：
//!   libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/TsavoriteCheckpointReader.cs:TsavoriteSnapshotReader
//!   （rust 统一检查点模型映射：STORE_HLOG 段流 + STORE_INDEX 段流 +
//!   STORE_SNAPSHOT 元数据单消息；wcpr 检查点产物只有 hlog/index/meta 三类，
//!   C# 的 STORE_SNAPSHOT 文件段与 STORE_INDEX 元数据在 rust 无对位段，故本
//!   路径不下发这两类——该口径在本模块头自述，不挂在途文档）；
//! - `RangeIndexSnapshotReader.cs` / `RangeIndexFileDataSource.cs` /
//!   `RangeIndexFileTransmitSource.cs`：RangeIndex 检查点快照树文件逐个
//!   三段发送（头帧 startAddress=-1 元数据 → 段帧 → 空载荷收尾；rust 元
//!   数据载荷为 key_id 16B LE，替代 C# keyHash 32B ASCII）；
//! - `FileDataSource.cs`：段切分读块（DefaultBatchSize 对标）；
//! - `TsavoriteMetadataSource.cs` / `TsavoriteMetadataTransmitSource.cs`：
//!   元数据单消息发送（rust 承载 wcpr CheckpointMeta 字节）。
//!
//! 发送顺序（元数据最后落盘 = 副本侧提交标记）：hlog 段流（空载荷收尾）→
//! index 段流（空载荷收尾）→ RangeIndex 快照树文件（逐个三段）→ meta 单
//! 消息。
//!
//! 读源口径（数据面全链路 compio 异步，零 std::fs 同步 syscall）：本路径由副
//! 本同步会话的慢路径 future 承载，跑在该连接所在的 compio 线程上（一线程一
//! CPU），同步文件读会独占当根线程、拖住心跳/gossip/其它连接。C# 侧本就全异
//! 步——设备源按异步 ReadAsync、文件源按 useAsync FileStream 且每文件一次
//! open、句柄跨块复用；rust 同等：hlog 段走 SegmentedDevice 池化异步读，index
//! 与 RangeIndex 快照树文件走 compio 异步文件段源（一次 open + 游标自持 +
//! read_exact_at 按偏移取块），元数据小文件走 compio 异步整读。仅存的同步面
//! 是设备段文件 stat 与快照树目录枚举，一次性、与 C# 构造期枚举同口径。
//!
//! 块缓冲口径（对标 C# 池化借切片直发、块尾归还）：段流读源交回的块形态为
//! [`AlignedBuf`]——设备源直接透传 [`Device::read_range`] 的池化缓冲，文件源
//! 自同一 [`BufferPool`] 借出免清零读目的地喂 read_exact_at；发送侧按切片借用
//! 喂单帧发送、块循环尾 drop 即回池，逐块零堆分配、零 memset（C# 侧同一条链
//! 若先 Get 池化缓冲再整块拷回托管堆，池化收益即归零，故 rust 不复现该拷贝）。
//!
//! 登记差异（对标 C#）：`RangeIndexSnapshotReader` 同源枚举两类文件——
//! 检查点快照（STORE_RANGEINDEX_SNAPSHOT）与逐 flush 快照
//! （STORE_RANGEINDEX_FLUSH）；rust 仅下发检查点快照类：副本 AOF 直推自
//! 检查点覆盖位点全量重放 ri_set，检查点后新建树由 ri_create 重放重建，
//! flush 快照无恢复面消费；其副本落盘需 ri_log_root 接线（接收依赖束
//! 构造点在引擎装配层），待 flush 恢复链另立任务时一并启用。

use std::{
  future::Future,
  io::{self, ErrorKind},
  path::Path,
  sync::Arc,
  time::Duration,
};

use compio::{
  buf::{BufResult, IntoInner, IoBuf},
  fs::File,
  io::AsyncReadAtExt,
};
use wbase::pool::{AlignedBuf, BufferPool};
use wbftree::RangeIndexManager;
use wdev::{Device, SegmentedDevice};

use crate::{
  client::{GarnetClient, is_ok_ack},
  server::{
    replication::{
      checkpoint_entry::{CheckpointEntry, CheckpointFileType},
      checkpoint_store::read_meta_aligned_begin,
      error::ReplicationError,
    },
    wait_async,
  },
};

/// 段切分大小（libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/
/// FileDataSource.cs:DefaultBatchSize = 1 << 17）
pub const SNAPSHOT_CHUNK_SIZE: usize = 1 << 17;

/// 快照下发依赖束（wcpr 统一检查点模型的发送源编目）
pub struct SnapshotTransmitSources {
  /// 主端引擎设备（STORE_HLOG 段流读源；快照只读封印前缀并发读安全。
  /// 其 [`Device::pool`] 同时是 index 与 RangeIndex 文件段源的块缓冲池出口）
  pub device: Arc<SegmentedDevice>,
  /// 主端检查点目录（index/meta 文件读源；RangeIndex 快照树文件枚举源，
  /// 落盘于 token 子目录 rangeindex/——纯目录枚举，无需引擎实例）
  pub checkpoint_dir: Arc<Path>,
}

/// libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/
/// SnapshotTransmissionDriver.cs:SendCheckpointAsync
///
/// 检查点文件集下发编排：逐源按段发送 + 空载荷收尾 + 元数据单消息收口。
/// 任一源失败即整体失败（副本侧半截文件集由 meta 缺席标记拒绝导入）。
pub async fn send_store_checkpoint(
  client: &GarnetClient,
  sources: &SnapshotTransmitSources,
  entry: &CheckpointEntry,
  timeout: Option<Duration>,
) -> Result<(), ReplicationError> {
  let hlog_token = entry.metadata.store_hlog_token;
  let index_token = entry.metadata.store_index_token;

  // wcpr CheckpointMeta 读取（段范围基准 + 元数据载荷一体；控制面小文件
  // 单次异步整读，与下方数据段同一 compio 异步口径；读取起址扇区下对齐
  // 单点 read_meta_aligned_begin，与主端读钉注册/滞后补删同源）
  let sector = sources.device.sector_size() as u64;
  let (meta, start) = read_meta_aligned_begin(&sources.checkpoint_dir, hlog_token, sector)
    .await
    .map_err(|e| ReplicationError::io("read local checkpoint meta", io::Error::other(e)))?;

  // 段流块缓冲池出口（对标 C# 类实名 TsavoriteSnapshotReader 的构造期建一次、
  // 注入全部数据源的 bufferPool 字段；该类文件名见模块头第一条，与类名不同名）：
  // 设备源在设备层内部即用它，文件源沿本句柄共用
  let pool = sources.device.pool();

  // 1. STORE_HLOG 段流：[扇区对齐下界(begin), 持久化幅面]——与 C#
  //    hybridLogFileStartAddress/EndAddress 同口径（C# 终点即日志文件数据
  //    末端）。终点取「设备文件幅面与持久化承诺的较大者」原样（不做扇区下
  //    圆整）：尾零头 pad 消解后文件尾精确停在逻辑写入尾（可含 flushed_until
  //    前缀的尾扇区字节），下圆整会让副本段文件缺尾字节——恢复装载按
  //    flushed_until 钳长读取即 DeviceTooShort 拒启。tail 不参与终点：tail
  //    含未刷盘内存页字节（空库 FoldOver 的初始基准地址即此形态），从未上
  //    设备的字节无从发送。设备零字节（纯空库：flushed_until 恒等于初始
  //    基准地址，段文件从未创建）恒发空流——副本侧恢复预检对
  //    flushed == initial 豁免幅面校验，空日志正常起库。恢复装载读长恒钳
  //    持久化前缀（页尾残留清零），页幅由物理文件长度承接；页内尾随零随流
  //    传输，接收侧文件幅面与主端逐字节同构
  let file_len = sources
    .device
    .get_file_size(0)
    .map_err(|e| ReplicationError::io("query hlog file size", io::Error::other(e)))?;
  let end = if start > 0 || file_len > 0 {
    // 段流起点在段 1+（begin 已随删段前移，读恒不触段 0）或段 0 已有字节：
    // 幅面 = 文件长与持久化承诺的较大者（崩溃窗 file_len < flushed 时强制
    // 读到承诺界，缺失即 SegmentNotFound 显式暴露）
    file_len.max(meta.hlog_meta.flushed_until_address)
  } else {
    // 纯空库：起点 0 且段 0 从未创建（flushed_until 恒等于初始基准地址，
    // 无任何记录落盘）→ 空流。副本侧恢复预检对 flushed == initial 豁免
    // 幅面校验，空日志正常起库
    0
  };
  let mut hlog = HlogSegmentSource::new(&sources.device, start, end);
  send_file_chunks(
    client,
    &mut hlog,
    hlog_token,
    CheckpointFileType::StoreHlog,
    timeout,
  )
  .await?;

  // 2. STORE_INDEX 段流：index_<token>.ckpt（wcpr index_filename 命名，
  //    副本侧零改名直读；文件缺席 = 无 index 检查点，跳过该源）
  let index_path = sources
    .checkpoint_dir
    .join(wcpr::index_filename(index_token));
  if let Some(mut index) = CheckpointFileSource::open(&index_path, pool).await? {
    send_file_chunks(
      client,
      &mut index,
      index_token,
      CheckpointFileType::StoreIndex,
      timeout,
    )
    .await?;
  }

  // 3. RangeIndex 检查点快照树文件（
  //    libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/RangeIndexSnapshotReader.cs:GetTransmitSources 构造期枚举
  //    libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/RangeIndexSnapshotReader.cs:Dispose
  //    libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/RangeIndexFileDataSource.cs:GetMetadata
  //    libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/RangeIndexFileDataSource.cs:SetBuffer
  //    libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/RangeIndexFileDataSource.cs:ReadNextChunkAsync
  //    libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/RangeIndexFileDataSource.cs:Dispose
  //    libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/RangeIndexFileTransmitSource.cs:TransmitAsync 逐文件三段
  //    libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/RangeIndexFileTransmitSource.cs:Dispose
  //    ）：
  //    检查点 token 目录内已快照的 .bftree 树文件逐个下发——缺失即副本
  //    检查点时刻已存在的 RI 树永不重建（检查点前 ri_create 条目在 AOF
  //    重放区间外），主从静默发散。每文件三段：头帧（startAddress=-1，
  //    载荷 = key_id 16B LE，副本据此派生落盘路径）→ 段帧（文件内偏移）
  //    → 空载荷收尾。文件为已完成检查点的不可变快照，并发读安全
  let ri_files =
    RangeIndexManager::enumerate_checkpoint_snapshots(&sources.checkpoint_dir, hlog_token)
      .map_err(|e| {
        ReplicationError::io(
          "enumerate rangeindex checkpoint snapshots",
          io::Error::other(e),
        )
      })?;
  for (key_id, path) in ri_files {
    // 头帧（C# GetMetadata + startAddress=-1 单消息控制载荷）
    let header = key_id.to_le_bytes();
    send_snapshot_data(
      client,
      hlog_token,
      CheckpointFileType::StoreRangeindexSnapshot,
      -1,
      &header,
      timeout,
    )
    .await?;

    // 段流 + 空载荷收尾（一次 open 后按游标顺序异步取块，句柄随段流生命周期）
    let mut tree = CheckpointFileSource::open_required(&path, pool).await?;
    send_file_chunks(
      client,
      &mut tree,
      hlog_token,
      CheckpointFileType::StoreRangeindexSnapshot,
      timeout,
    )
    .await?;
  }

  // 4. STORE_SNAPSHOT 元数据单消息（startAddress = -1 约定；元数据最后
  //    发送 = 副本侧提交标记，崩溃时半截文件集绝不进入恢复视图）
  //    bitcode 确定性编码，re-encode 与落盘原字节逐位恒等
  send_snapshot_data(
    client,
    hlog_token,
    CheckpointFileType::StoreSnapshot,
    -1,
    &meta.encode(),
    timeout,
  )
  .await
  .map(|_| ())
}

/// 段流发送循环（C# FileTransmitSource.TransmitAsync：逐块 SNAPSHOT_DATA +
/// 空载荷收尾帧；读源自持游标按需取块，空间 O(1)）
///
/// 块缓冲按切片借用喂单帧发送（C# `result.Buffer.GetSlice(result.BytesRead)`），
/// 发送返回后 chunk 离开作用域即 drop 回池（C# finally `result.Buffer.Return()`）
async fn send_file_chunks<S: SnapshotDataSource>(
  client: &GarnetClient,
  src: &mut S,
  token: u128,
  file_type: CheckpointFileType,
  timeout: Option<Duration>,
) -> Result<(), ReplicationError> {
  while src.has_next_chunk() {
    let start_address = src.span().cursor as i64;
    let chunk = src.read_next_chunk(SNAPSHOT_CHUNK_SIZE).await?;
    send_snapshot_data(
      client,
      token,
      file_type,
      start_address,
      chunk.as_slice(),
      timeout,
    )
    .await?;
  }
  // 空载荷收尾帧（C# FileTransmitSource 尾段 EOF 哨兵）
  let end_address = src.span().cursor as i64;
  send_snapshot_data(client, token, file_type, end_address, &[], timeout)
    .await
    .map(|_| ())
}

/// 单帧 SNAPSHOT_DATA 发送（C# GarnetClientSession.ExecuteClusterSnapshotData
/// + WaitAsync(timeout)；非 OK 应答即主端错误）
///
/// wait_async（compio timeout 无限形态）与 wconn 客户端在途超时分工不同、
/// 不收编：此处为单帧
/// 应答限时（C# tcs.WaitAsync(timeout) 形态，时限取 ReplicaSyncTimeout
/// 的单帧粒度（GarnetServerOptions.cs:420 默认 5s，经
/// DiskbasedReplication/ReplicaSyncSession.cs:140 透传），None = 0 值无限
/// 不挂计时器；超时仅本帧失败、快照下发整体
/// 报错可重试）；客户端在途超时
/// （GarnetClient timeout_millis → TimeoutChecker）为连接级无进展判定，
/// 判成即拆整条客户端，粒度与失败面均不同层，C# 侧两层并存同此分工
async fn send_snapshot_data(
  client: &GarnetClient,
  token: u128,
  file_type: CheckpointFileType,
  start_address: i64,
  data: &[u8],
  timeout: Option<Duration>,
) -> Result<String, ReplicationError> {
  match wait_async(
    timeout,
    client.snapshot_data_async(&token.to_le_bytes(), file_type as i64, start_address, data),
  )
  .await
  {
    Some(Ok(resp)) if is_ok_ack(resp.as_bytes()) => Ok(resp),
    Some(Ok(resp)) => Err(ReplicationError::PrimaryResp(resp)),
    Some(Err(e)) => Err(e.into()),
    None => Err(ReplicationError::Timeout("snapshot data send timed out")),
  }
}

/// 段流游标（C# StartOffset / CurrentOffset / EndOffset 三元组的 rust 承载）
pub struct SegmentSpan {
  /// 当前游标（C# CurrentOffset）
  pub cursor: u64,
  /// 段流终点（C# EndOffset）
  pub end: u64,
}

impl SegmentSpan {
  pub fn new(start: u64, end: u64) -> Self {
    Self { cursor: start, end }
  }

  /// 本块请求长度（C# FileDataSource.ReadNextChunkAsync 的 size 计算）
  pub fn want(&self, max_len: usize) -> usize {
    max_len.min((self.end - self.cursor) as usize)
  }

  /// 按实际读数字节推进游标（C# `CurrentOffset += bytesRead`）
  pub fn advance(&mut self, read: usize) {
    self.cursor += read as u64;
  }
}

/// 快照段流读源（C# ISnapshotDataSource 的 rust 投影：游标自持 + 异步取块；
/// 两类实现分别对应 C# 的设备源与文件源，全链路 compio 异步）
pub trait SnapshotDataSource {
  /// 段流游标（C# CurrentOffset / EndOffset）
  fn span(&self) -> &SegmentSpan;

  /// 是否还有下一块（C# HasNextChunk）
  fn has_next_chunk(&self) -> bool {
    let span = self.span();
    span.cursor < span.end
  }

  /// 取下一块，单次不超过 `max_len`（C# ReadNextChunkAsync）
  ///
  /// 块形态为池化扇区对齐缓冲（C# 交回的 `SectorAlignedMemory`）：读源自共享
  /// 缓冲池借出、发送侧按切片借用、块循环尾 drop 归还，逐块零堆分配零 memset
  fn read_next_chunk(
    &mut self,
    max_len: usize,
  ) -> impl Future<Output = Result<AlignedBuf, ReplicationError>>;
}

/// STORE_HLOG 段流源（C# FileDataSource 经 IDevice 异步读；rust 复用
/// [`SegmentedDevice`] 池化异步读，句柄由设备层自持、块缓冲自设备池借出随
/// 返回值移交发送侧，越界短读归一为设备层 UnexpectedEof 错误）
pub struct HlogSegmentSource<'a> {
  device: &'a SegmentedDevice,
  span: SegmentSpan,
}

impl<'a> HlogSegmentSource<'a> {
  pub fn new(device: &'a SegmentedDevice, start: u64, end: u64) -> Self {
    Self {
      device,
      span: SegmentSpan::new(start, end),
    }
  }
}

impl SnapshotDataSource for HlogSegmentSource<'_> {
  fn span(&self) -> &SegmentSpan {
    &self.span
  }

  async fn read_next_chunk(&mut self, max_len: usize) -> Result<AlignedBuf, ReplicationError> {
    let offset = self.span.cursor;
    let want = self.span.want(max_len);
    // 设备层 read_range 已自 device.pool() 借出池化缓冲并按 want 定长回交，
    // 直发即零拷贝（此处再拷一份 Vec 等于把刚省下的池化收益整块扔回堆）
    let buf =
      self.device.read_range(offset, want).await.map_err(|e| {
        ReplicationError::io(format!("device read at {offset}"), io::Error::other(e))
      })?;
    self.span.advance(buf.len());
    Ok(buf)
  }
}

/// 检查点文件段流源（STORE_INDEX 与 RangeIndex 快照树文件共用；C# 文件源
/// 形态——每文件一次 open、句柄随段流生命周期持有、共享缓冲顺序读，
/// 消除逐块重复 open/seek 与同步 syscall；读目的地自共享缓冲池借出，
/// 消除逐块新分配与零初始化 memset）
pub struct CheckpointFileSource<'a> {
  file: File,
  /// 共享扇区对齐缓冲池（C# 数据源构造期注入的 bufferPool 字段；rust 由装配面
  /// 沿主端引擎设备 [`Device::pool`] 下发，块缓冲 drop 即回池）
  pool: &'a Arc<BufferPool>,
  span: SegmentSpan,
}

impl<'a> CheckpointFileSource<'a> {
  /// 异步打开并按 fstat 定幅（C# 构造期定 EndOffset + 首块前惰性 open 合并为
  /// 一次 open）；文件缺席返回 None，由调用方决定跳过或报错
  pub async fn open(
    path: &Path,
    pool: &'a Arc<BufferPool>,
  ) -> Result<Option<Self>, ReplicationError> {
    let file = match File::open(path).await {
      Ok(file) => file,
      Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
      Err(e) => return Err(ReplicationError::io("open checkpoint file", e)),
    };
    let end = file
      .metadata()
      .await
      .map_err(|e| ReplicationError::io("checkpoint metadata", e))?
      .len();
    Ok(Some(Self {
      file,
      pool,
      span: SegmentSpan::new(0, end),
    }))
  }

  /// 必需文件：编目在册而磁盘缺席即主端状态与文件不一致，中止下发
  /// （副本侧半截文件集由 meta 缺席拒绝导入）
  pub async fn open_required(
    path: &Path,
    pool: &'a Arc<BufferPool>,
  ) -> Result<Self, ReplicationError> {
    Self::open(path, pool)
      .await?
      .ok_or_else(|| ReplicationError::CheckpointFileMissing {
        path: path.to_path_buf(),
      })
  }
}

impl SnapshotDataSource for CheckpointFileSource<'_> {
  fn span(&self) -> &SegmentSpan {
    &self.span
  }

  async fn read_next_chunk(&mut self, max_len: usize) -> Result<AlignedBuf, ReplicationError> {
    let offset = self.span.cursor;
    let want = self.span.want(max_len);
    // 池借出免清零读目的地（读区间整体覆写，对标 C# bufferPool.Get 的
    // clearOnReturn:false 读路径优化）；class 容量可大于本次请求，故按 want
    // 切片限定读取上界，杜绝越界多读把池尾残留送进段流
    let buf = self.pool.get_with_policy(want, false).map_err(|e| {
      ReplicationError::io(
        format!("borrow chunk buffer at {offset}"),
        io::Error::other(e),
      )
    })?;
    let BufResult(res, buf) = self.file.read_exact_at(buf.slice(..want), offset).await;
    let mut buf = buf.into_inner();
    res.map_err(|e| ReplicationError::io(format!("checkpoint read at {offset}"), e))?;
    buf
      .set_len(want)
      .map_err(|e| ReplicationError::io(format!("set chunk len at {offset}"), e))?;
    self.span.advance(want);
    Ok(buf)
  }
}
