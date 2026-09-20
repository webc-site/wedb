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

use std::{future::Future, io::ErrorKind, path::Path, sync::Arc, time::Duration};

use compio::{
  buf::{BufResult, IntoInner, IoBuf},
  fs::{File, read},
  io::AsyncReadAtExt,
};
use wbase::pool::{AlignedBuf, BufferPool};
use wbftree::RangeIndexManager;
use wcpr::CheckpointMeta;
use wdev::{Device, SegmentedDevice};

use crate::{
  client::GarnetClient,
  server::{
    replication::checkpoint_entry::{CheckpointEntry, CheckpointFileType},
    wait_async,
  },
};

/// 段切分大小（libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/
/// FileDataSource.cs:DefaultBatchSize = 1 << 17）
const SNAPSHOT_CHUNK_SIZE: usize = 1 << 17;

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
) -> Result<(), String> {
  let hlog_token = entry.metadata.store_hlog_token;
  let index_token = entry.metadata.store_index_token;

  // wcpr CheckpointMeta 读取（段范围基准 + 元数据载荷一体；控制面小文件
  // 单次异步整读，与下方数据段同一 compio 异步口径）
  let meta_path = sources.checkpoint_dir.join(wcpr::meta_filename(hlog_token));
  let meta_bytes = read(&meta_path)
    .await
    .map_err(|e| format!("IOERR read local checkpoint meta: {e}"))?;
  let meta = CheckpointMeta::decode(&meta_bytes)
    .map_err(|e| format!("local checkpoint meta decode failed: {e}"))?;

  // 段流块缓冲池出口（对标 C# 类实名 TsavoriteSnapshotReader 的构造期建一次、
  // 注入全部数据源的 bufferPool 字段；该类文件名见模块头第一条，与类名不同名）：
  // 设备源在设备层内部即用它，文件源沿本句柄共用
  let pool = sources.device.pool();

  // 1. STORE_HLOG 段流：[扇区对齐下界(begin), 设备文件幅面]——与 C#
  //    hybridLogFileStartAddress/EndAddress 同口径（C# 终点即日志文件数据
  //    末端）。终点取主端设备文件长度：恢复装载按整页读（页尾残留清零），
  //    flushed_until 水位为页内粒度，页幅必须由物理文件长度承接；页内
  //    尾随零随流传输，接收侧文件幅面与主端逐字节同构
  let sector = sources.device.sector_size() as u64;
  let start = meta.hlog_meta.begin_address / sector * sector;
  let file_len = sources
    .device
    .get_file_size(0)
    .map_err(|e| format!("IOERR query hlog file size: {e}"))?;
  let raw_end = file_len
    .max(meta.hlog_meta.flushed_until_address)
    .max(meta.hlog_meta.tail_address);
  let end = raw_end / sector * sector;
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
      .map_err(|e| format!("IOERR enumerate rangeindex checkpoint snapshots: {e}"))?;
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
  send_snapshot_data(
    client,
    hlog_token,
    CheckpointFileType::StoreSnapshot,
    -1,
    &meta_bytes,
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
) -> Result<(), String> {
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
) -> Result<String, String> {
  match wait_async(
    timeout,
    client.snapshot_data_async(&token.to_le_bytes(), file_type as i64, start_address, data),
  )
  .await
  {
    Some(Ok(resp)) if resp == "OK" => Ok(resp),
    Some(Ok(resp)) => Err(format!("Primary error at TransmitAsync {resp}")),
    Some(Err(e)) => Err(e.to_string()),
    None => Err("snapshot data send timed out".to_string()),
  }
}

/// 段流游标（C# StartOffset / CurrentOffset / EndOffset 三元组的 rust 承载）
struct SegmentSpan {
  /// 当前游标（C# CurrentOffset）
  cursor: u64,
  /// 段流终点（C# EndOffset）
  end: u64,
}

impl SegmentSpan {
  fn new(start: u64, end: u64) -> Self {
    Self { cursor: start, end }
  }

  /// 本块请求长度（C# FileDataSource.ReadNextChunkAsync 的 size 计算）
  fn want(&self, max_len: usize) -> usize {
    max_len.min((self.end - self.cursor) as usize)
  }

  /// 按实际读数字节推进游标（C# `CurrentOffset += bytesRead`）
  fn advance(&mut self, read: usize) {
    self.cursor += read as u64;
  }
}

/// 快照段流读源（C# ISnapshotDataSource 的 rust 投影：游标自持 + 异步取块；
/// 两类实现分别对应 C# 的设备源与文件源，全链路 compio 异步）
trait SnapshotDataSource {
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
  fn read_next_chunk(&mut self, max_len: usize)
  -> impl Future<Output = Result<AlignedBuf, String>>;
}

/// STORE_HLOG 段流源（C# FileDataSource 经 IDevice 异步读；rust 复用
/// [`SegmentedDevice`] 池化异步读，句柄由设备层自持、块缓冲自设备池借出随
/// 返回值移交发送侧，越界短读归一为设备层 UnexpectedEof 错误）
struct HlogSegmentSource<'a> {
  device: &'a SegmentedDevice,
  span: SegmentSpan,
}

impl<'a> HlogSegmentSource<'a> {
  fn new(device: &'a SegmentedDevice, start: u64, end: u64) -> Self {
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

  async fn read_next_chunk(&mut self, max_len: usize) -> Result<AlignedBuf, String> {
    let offset = self.span.cursor;
    let want = self.span.want(max_len);
    // 设备层 read_range 已自 device.pool() 借出池化缓冲并按 want 定长回交，
    // 直发即零拷贝（此处再拷一份 Vec 等于把刚省下的池化收益整块扔回堆）
    let buf = self
      .device
      .read_range(offset, want)
      .await
      .map_err(|e| format!("IOERR device read at {offset}: {e}"))?;
    self.span.advance(buf.len());
    Ok(buf)
  }
}

/// 检查点文件段流源（STORE_INDEX 与 RangeIndex 快照树文件共用；C# 文件源
/// 形态——每文件一次 open、句柄随段流生命周期持有、共享缓冲顺序读，
/// 消除逐块重复 open/seek 与同步 syscall；读目的地自共享缓冲池借出，
/// 消除逐块新分配与零初始化 memset）
struct CheckpointFileSource<'a> {
  file: File,
  /// 共享扇区对齐缓冲池（C# 数据源构造期注入的 bufferPool 字段；rust 由装配面
  /// 沿主端引擎设备 [`Device::pool`] 下发，块缓冲 drop 即回池）
  pool: &'a Arc<BufferPool>,
  span: SegmentSpan,
}

impl<'a> CheckpointFileSource<'a> {
  /// 异步打开并按 fstat 定幅（C# 构造期定 EndOffset + 首块前惰性 open 合并为
  /// 一次 open）；文件缺席返回 None，由调用方决定跳过或报错
  async fn open(path: &Path, pool: &'a Arc<BufferPool>) -> Result<Option<Self>, String> {
    let file = match File::open(path).await {
      Ok(file) => file,
      Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
      Err(e) => return Err(format!("IOERR open checkpoint file: {e}")),
    };
    let end = file
      .metadata()
      .await
      .map_err(|e| format!("IOERR checkpoint metadata: {e}"))?
      .len();
    Ok(Some(Self {
      file,
      pool,
      span: SegmentSpan::new(0, end),
    }))
  }

  /// 必需文件：编目在册而磁盘缺席即主端状态与文件不一致，中止下发
  /// （副本侧半截文件集由 meta 缺席拒绝导入）
  async fn open_required(path: &Path, pool: &'a Arc<BufferPool>) -> Result<Self, String> {
    Self::open(path, pool)
      .await?
      .ok_or_else(|| format!("IOERR checkpoint file missing: {}", path.display()))
  }
}

impl SnapshotDataSource for CheckpointFileSource<'_> {
  fn span(&self) -> &SegmentSpan {
    &self.span
  }

  async fn read_next_chunk(&mut self, max_len: usize) -> Result<AlignedBuf, String> {
    let offset = self.span.cursor;
    let want = self.span.want(max_len);
    // 池借出免清零读目的地（读区间整体覆写，对标 C# bufferPool.Get 的
    // clearOnReturn:false 读路径优化）；class 容量可大于本次请求，故按 want
    // 切片限定读取上界，杜绝越界多读把池尾残留送进段流
    let buf = self
      .pool
      .get_with_policy(want, false)
      .map_err(|e| format!("IOERR borrow chunk buffer at {offset}: {e}"))?;
    let BufResult(res, buf) = self.file.read_exact_at(buf.slice(..want), offset).await;
    let mut buf = buf.into_inner();
    res.map_err(|e| format!("IOERR checkpoint read at {offset}: {e}"))?;
    buf
      .set_len(want)
      .map_err(|e| format!("IOERR set chunk len at {offset}: {e}"))?;
    self.span.advance(want);
    Ok(buf)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    sync::{
      Arc,
      atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
  };

  use compio::{
    fs::{remove_file, write},
    runtime::{Runtime, spawn},
    time::sleep,
  };
  use wbase::align::DEFAULT_SECTOR_SIZE;

  use super::*;

  /// 探针节拍：远小于整段下发耗时，又不至于热自旋
  const PROBE_TICK: Duration = Duration::from_micros(50);

  /// 位置相关确定性图案（逐字节随偏移变化：错偏移、丢段、重复段必被捕获）
  fn pattern_bytes(len: usize) -> Vec<u8> {
    (0..len)
      .map(|i| ((i as u64 * 31 + 7) % 251) as u8)
      .collect()
  }

  /// 测试夹具落盘（与产品路径同一 compio 异步口径）
  async fn write_fixture(path: &Path, data: &[u8]) {
    let BufResult(res, _) = write(path, data.to_vec()).await;
    res.expect("write fixture");
  }

  /// 段流排空（与 [`send_file_chunks`] 的读循环同构，剥离网络帧）
  async fn drain<S: SnapshotDataSource>(src: &mut S, max_len: usize) -> Vec<(u64, Vec<u8>)> {
    let mut chunks = Vec::new();
    while src.has_next_chunk() {
      let start = src.span().cursor;
      let chunk = src.read_next_chunk(max_len).await.expect("chunk read");
      assert!(!chunk.is_empty(), "零字节块使游标停滞，段流永不收敛");
      assert!(chunk.len() <= max_len, "块长越过请求上界");
      chunks.push((start, chunk.to_vec()));
    }
    chunks
  }

  /// 段流读回的逐块断言：内容与写入逐字节一致、地址自起点连续、
  /// 块数与末尾短段由幅面和上界唯一决定
  fn assert_stream(chunks: &[(u64, Vec<u8>)], data: &[u8], start: u64, end: u64, max_len: usize) {
    let span = (end - start) as usize;
    assert_eq!(
      chunks.len(),
      span.div_ceil(max_len),
      "块数与段幅面/上界不符（span={span}, max_len={max_len}）"
    );
    let mut cursor = start;
    for (idx, (addr, chunk)) in chunks.iter().enumerate() {
      assert_eq!(*addr, cursor, "第 {idx} 块起始地址与游标不连续");
      let s = *addr as usize;
      assert_eq!(
        chunk.as_slice(),
        &data[s..s + chunk.len()],
        "第 {idx} 块内容与写入不符"
      );
      cursor += chunk.len() as u64;
    }
    assert_eq!(cursor, end, "读出总字节与段幅面不符");
    let tail = span % max_len;
    if tail != 0 {
      assert_eq!(
        chunks.last().expect("tail chunk").1.len(),
        tail,
        "末尾短段长度不符"
      );
    }
  }

  /// 段读回路与写入完全一致：空文件、末尾短段、整倍数无尾段、单段大于
  /// 读缓冲（幅面远超上界）、请求上界超出幅面（按幅面截断）、末尾短段
  /// 非扇区整数倍（池 class 容量大于本次请求，越界多读必被捕获）
  #[test]
  fn segment_stream_reads_back_written_bytes() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let dir = tempfile::tempdir()?;
      let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
      let cases: [(usize, usize); 6] = [
        (0, SNAPSHOT_CHUNK_SIZE),
        (SNAPSHOT_CHUNK_SIZE * 2 + 4096, SNAPSHOT_CHUNK_SIZE),
        (SNAPSHOT_CHUNK_SIZE * 3, SNAPSHOT_CHUNK_SIZE),
        (SNAPSHOT_CHUNK_SIZE * 4, 4096),
        (SNAPSHOT_CHUNK_SIZE, SNAPSHOT_CHUNK_SIZE * 4),
        (SNAPSHOT_CHUNK_SIZE * 2 + 100, SNAPSHOT_CHUNK_SIZE),
      ];
      for (idx, (len, max_len)) in cases.into_iter().enumerate() {
        let data = pattern_bytes(len);
        let path = dir.path().join(format!("case-{idx}"));
        write_fixture(&path, &data).await;
        let mut src = CheckpointFileSource::open_required(&path, &pool)
          .await
          .expect("open required");
        assert_eq!(src.span().end, len as u64, "幅面须由 open 期 fstat 定得");
        let chunks = drain(&mut src, max_len).await;
        assert_stream(&chunks, &data, 0, len as u64, max_len);
        assert_eq!(src.span().cursor, src.span().end, "排空后游标停在段流终点");
        assert!(!src.has_next_chunk(), "排空后不得再有下一块");
      }
      Ok(())
    })
  }

  /// 缺席文件按「无该源」跳过（STORE_INDEX 可缺），编目在册而磁盘缺席
  /// 必须中止下发；空幅面文件建得起源但零块（只发空载荷收尾帧）
  #[test]
  fn absent_and_empty_files_gate_the_source() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let dir = tempfile::tempdir()?;
      let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
      let missing = dir.path().join("index_absent");
      assert!(
        CheckpointFileSource::open(&missing, &pool)
          .await
          .expect("open absent must not error")
          .is_none(),
        "NotFound 须归一为无该源，而非 IOERR"
      );
      let err = CheckpointFileSource::open_required(&missing, &pool)
        .await
        .err()
        .expect("编目在册而磁盘缺席必须中止");
      assert!(
        err.contains("checkpoint file missing"),
        "中止原因须点名缺席文件: {err}"
      );

      let empty = dir.path().join("index_empty");
      write_fixture(&empty, &[]).await;
      let mut src = CheckpointFileSource::open_required(&empty, &pool)
        .await
        .expect("open empty");
      assert_eq!(src.span().end, 0);
      assert!(!src.has_next_chunk(), "零幅面段流不得读出任何块");
      assert!(drain(&mut src, SNAPSHOT_CHUNK_SIZE).await.is_empty());
      Ok(())
    })
  }

  /// 每文件一次 open、句柄跨块复用（C# RangeIndexFileDataSource 的
  /// `stream ??= new FileStream` 形态）：首块之后删除目录项，后续块仍须
  /// 读全——逐块重开的实现会在此以 NotFound 失败
  #[test]
  fn file_handle_is_reused_across_chunks() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let dir = tempfile::tempdir()?;
      let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
      let path = dir.path().join("tree.bftree");
      let len = SNAPSHOT_CHUNK_SIZE * 3 + 4096;
      let data = pattern_bytes(len);
      write_fixture(&path, &data).await;

      let mut src = CheckpointFileSource::open_required(&path, &pool)
        .await
        .expect("open");
      let mut joined = src
        .read_next_chunk(SNAPSHOT_CHUNK_SIZE)
        .await
        .expect("first chunk")
        .to_vec();
      remove_file(&path).await.expect("unlink fixture");
      while src.has_next_chunk() {
        joined.extend_from_slice(
          src
            .read_next_chunk(SNAPSHOT_CHUNK_SIZE)
            .await
            .expect("chunk after unlink")
            .as_slice(),
        );
      }
      assert_eq!(joined, data, "句柄复用下的按段读回须与写入完全一致");
      Ok(())
    })
  }

  /// 大文件段下发不独占 compio 线程：读段期间同线程上的其它异步任务
  /// 必须被驱动。若读段退回同步 syscall，主循环一次也不让出，探针计数
  /// 恒 0（同步 fs 读与 compio reactor 不可并存的正证伪判据）。判据只取
  /// 「让出发生过」这一不变量，不取节拍比例——页缓存命中时长随机器负载
  /// 浮动，任何绝对节拍阈值都会 flaky
  #[test]
  fn large_file_segments_yield_to_other_tasks() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let dir = tempfile::tempdir()?;
      let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
      let path = dir.path().join("large.bftree");
      let len = SNAPSHOT_CHUNK_SIZE * 128;
      let data = pattern_bytes(len);
      write_fixture(&path, &data).await;

      // 源先建、探针后派：探针拿到的每一次让出都只能来自读段
      let mut src = CheckpointFileSource::open_required(&path, &pool)
        .await
        .expect("open");
      let beats = Arc::new(AtomicU64::new(0));
      let stop = Arc::new(AtomicBool::new(false));
      let probe_beats = Arc::clone(&beats);
      let probe_stop = Arc::clone(&stop);
      spawn(async move {
        while !probe_stop.load(Ordering::Relaxed) {
          probe_beats.fetch_add(1, Ordering::Relaxed);
          sleep(PROBE_TICK).await;
        }
      })
      .detach();

      let mut cursor = 0usize;
      let mut previous_beats = 0u64;
      while src.has_next_chunk() {
        let chunk = src
          .read_next_chunk(SNAPSHOT_CHUNK_SIZE)
          .await
          .expect("chunk read");
        assert_eq!(
          chunk.as_slice(),
          &data[cursor..cursor + chunk.len()],
          "段内容错位"
        );
        cursor += chunk.len();
        let beats = beats.load(Ordering::Relaxed);
        assert!(
          beats >= previous_beats,
          "探针计数只增不减（{previous_beats} → {beats}）"
        );
        previous_beats = beats;
      }
      // 循环出口到此处无 await：读到的计数只能由读段期间的让出贡献
      let beats = beats.load(Ordering::Relaxed);
      stop.store(true, Ordering::Relaxed);

      assert_eq!(cursor, len, "整幅面须全部按段读出");
      assert!(
        beats > 0,
        "读段全程未让出 ⇒ 读源退回同步 syscall，独占当根 compio 线程"
      );
      Ok(())
    })
  }

  /// STORE_HLOG 设备段源：池化异步读按段回读与写入一致，起点非零时
  /// 只下发 [start, end) 区间（对标 C# hybridLogFileStart/EndAddress）
  #[test]
  fn hlog_segment_source_reads_back_device_bytes() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let dir = tempfile::tempdir()?;
      let path = dir.path().join("hlog.0");
      let len = SNAPSHOT_CHUNK_SIZE * 2 + 4096;
      let data = pattern_bytes(len);
      write_fixture(&path, &data).await;
      let device = SegmentedDevice::single_file(&path)?;

      let mut whole = HlogSegmentSource::new(&device, 0, len as u64);
      let chunks = drain(&mut whole, SNAPSHOT_CHUNK_SIZE).await;
      assert_stream(&chunks, &data, 0, len as u64, SNAPSHOT_CHUNK_SIZE);

      let start = SNAPSHOT_CHUNK_SIZE as u64;
      let mut tail = HlogSegmentSource::new(&device, start, len as u64);
      let chunks = drain(&mut tail, SNAPSHOT_CHUNK_SIZE).await;
      assert_stream(&chunks, &data, start, len as u64, SNAPSHOT_CHUNK_SIZE);
      Ok(())
    })
  }
}
