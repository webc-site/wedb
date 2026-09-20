//! 检查点网络接收面（全量同步第二段：主端快照流落盘）
//!
//! 对标 C# 三件套（libs/cluster/Server/Replication/ReplicaOps/
//! DiskbasedReplication/）：
//! - `ReceiveCheckpointHandler.cs`：activeSink 单文件状态机（顺序协议，
//!   一次一个活跃文件；空载荷 = 当前文件流收尾哨兵）；
//! - `FileDataSink.cs`：文件段设备写（C# 的 writeSemaphore/bufferPool/
//!   超时信号量是 Tsavorite 设备异步回调模型的桥接件，compio 读写天然
//!   异步直驱，无桥接语义，不做字段级转写）；
//! - `RangeIndexFileDataSink.cs`：RangeIndex 检查点快照树文件落盘（头帧
//!   元数据派生目标路径——rust 载荷为 key_id 16B LE，经
//!   [`wbftree::RangeIndexManager::checkpoint_snapshot_path_in`] 派生，
//!   与恢复端 `recover_all_trees_from_dir` 候选目录同构，落盘即收敛）；
//! - `MetadataDataSink.cs`：检查点元数据提交（单消息载荷整包落盘）。
//!
//! 接收文件布局（staging = 副本检查点目录，wcpr 命名零改名，重启恢复
//! `recover_latest(checkpoint_dir, 主数据文件)` 天然收敛）：
//! - STORE_HLOG 段流 → 在线引擎设备文件（[`wdev::Device::write_aligned`]，
//!   与引擎自身 I/O 模式同路，杜绝双句柄页缓存/O_DIRECT 不一致；对标 C#
//!   `CreateCheckpointDevice` STORE_HLOG → GetStoreHLogDevice 同源语义）；
//!   末块零填充扇区对齐（设备对齐契约），尾随零页由恢复期
//!   `[tail, page_end)` 清零语义吸收；
//! - STORE_INDEX 段流 → `checkpoint_dir/index_<token>.ckpt`
//!   （wcpr index_filename）；
//! - STORE_RANGEINDEX_SNAPSHOT（头帧 startAddress=-1 + key_id 16B LE →
//!   段帧 → 空收尾）→ `checkpoint_dir/{token_b32}/rangeindex/{key_id_b32}.bftree`；
//! - STORE_SNAPSHOT 元数据（startAddress=-1 单消息）→
//!   `checkpoint_dir/checkpoint_<token>.meta`（wcpr meta_filename；元数据
//!   最后落盘即提交标记，对齐 wcpr「meta 即发布」崩溃一致性协议）；
//! - STORE_INDEX 元数据：登记差异——C# CommitIndexCheckpoint 提交独立
//!   索引元数据文件；rust wcpr 索引快照自带头+CRC 自描述，无该文件概念，
//!   主端不发送，副本收到空载荷容忍（no-op）、非空载荷拒绝；
//! - STORE_RANGEINDEX_FLUSH：登记差异——C# 随检查点下发逐 flush 快照；
//!   rust 副本 AOF 直推自检查点覆盖位点全量重放 ri_set，flush 快照无
//!   恢复面消费，主端不发送，副本拒绝（协议违约即错，防静默丢帧）。

use std::{
  fs::{self, File, OpenOptions},
  io::{Seek, SeekFrom, Write},
  path::{Path, PathBuf},
  sync::Arc,
};

use parking_lot::Mutex;
use wbase::pool::AlignedBuf;
use wbftree::RangeIndexManager;
use wdev::{Device, SegmentedDevice};

use super::checkpoint_entry::CheckpointFileType;
use crate::server::replication::replication_manager::ReplicationManager;

/// 导入落盘依赖束（arm 侧自 ClusterProvider 现取现传，处理器不持引用，
/// 杜绝 provider → rm → handler → provider 循环持有）
#[derive(Clone)]
pub struct CheckpointImportCtx {
  /// 在线引擎设备（STORE_HLOG 段流落盘目标）
  pub store_device: Arc<SegmentedDevice>,
  /// 检查点目录（index/meta 文件落盘目标；对标 C# 副本本地
  /// ReplicationLogCheckpointManager 的目录）
  pub checkpoint_dir: PathBuf,
}

/// 单消息载荷哨兵地址（C# ProcessSnapshotData 约定：startAddress == -1
/// 表示整包元数据，随帧到达无需分段）
const SINGLE_MESSAGE_ADDRESS: i64 = -1;

/// 文件承载统一开口（create + truncate 写；控制面同步 syscall，见模块文档）
fn open_truncate(path: &Path) -> Result<File, String> {
  OpenOptions::new()
    .create(true)
    .write(true)
    .truncate(true)
    .open(path)
    .map_err(|e| format!("IOERR open checkpoint file {}: {e}", path.display()))
}

/// 活跃文件接收槽（FileDataSink 对标）
enum SinkTarget {
  /// 引擎设备段写（STORE_HLOG）
  Device(Arc<SegmentedDevice>),
  /// 独立文件定位写（STORE_INDEX）
  File(File),
}

/// 文件段接收槽（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/FileDataSink.cs:FileDataSink；
/// C# 的 Type 属性由开槽期类型分派消化，rust 槽体只保留 token 防串流）
struct FileDataSink {
  token: u128,
  target: SinkTarget,
}

impl FileDataSink {
  /// 开槽（对标 C# ProcessFileSegment 首段 CreateCheckpointDevice +
  /// FileDataSink 构造；重试重开已有文件安全——分块自起始地址顺序覆盖，
  /// C# 同款注释语义）
  async fn open(
    ctx: &CheckpointImportCtx,
    token: u128,
    file_type: CheckpointFileType,
  ) -> Result<Self, String> {
    let target = match file_type {
      // 主存 HybridLog：引擎设备直写（O_DIRECT/缓冲同路）
      CheckpointFileType::StoreHlog => SinkTarget::Device(Arc::clone(&ctx.store_device)),
      // 主存索引快照：wcpr index_filename 命名，恢复零改名。
      // 文件承载用 std::fs（控制面同步 syscall，见模块文档）：compio File
      // 为 !Send，不得驻留 ReplicationManager 的 Send 状态链
      CheckpointFileType::StoreIndex => {
        let path = ctx.checkpoint_dir.join(wcpr::index_filename(token));
        let file = open_truncate(&path)?;
        SinkTarget::File(file)
      }
      // OBJ/RI flush 变体 rust 统一检查点模型无对应文件集（见模块文档），协议违约
      CheckpointFileType::None
      | CheckpointFileType::StoreHlogObj
      | CheckpointFileType::StoreSnapshot
      | CheckpointFileType::StoreSnapshotObj
      | CheckpointFileType::StoreRangeindexFlush => {
        return Err(format!(
          "invalid checkpoint filetype {file_type:?} for file segment"
        ));
      }
      // RangeIndex 检查点快照：头帧元数据派生落盘路径（FromMetadata 同构），
      // 不经本构造（段帧惰性开槽无元数据可用，缺头帧即协议违约）
      CheckpointFileType::StoreRangeindexSnapshot => {
        return Err(
          "STORE_RANGEINDEX_SNAPSHOT requires a header frame (startAddress = -1)".to_string(),
        );
      }
    };
    Ok(Self { token, target })
  }

  /// RangeIndex 检查点快照槽（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/RangeIndexFileDataSink.cs:FromMetadata
  /// 与 libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/RangeIndexFileDataSink.cs:Dispose）：
  /// 头帧元数据（key_id 16B LE）派生落盘路径并打开截断写句柄
  fn open_rangeindex_snapshot(
    ctx: &CheckpointImportCtx,
    token: u128,
    key_id: u128,
  ) -> Result<Self, String> {
    let path = RangeIndexManager::checkpoint_snapshot_path_in(&ctx.checkpoint_dir, token, key_id);
    // token/rangeindex 目录随首文件惰性创建（C# 构造期 Directory.CreateDirectory 同款）
    if let Some(dir) = path.parent() {
      fs::create_dir_all(dir).map_err(|e| format!("IOERR create rangeindex snapshot dir: {e}"))?;
    }
    let file = open_truncate(&path)?;
    Ok(Self {
      token,
      target: SinkTarget::File(file),
    })
  }

  /// 定位写一个段（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/FileDataSink.cs:WriteChunk
  /// 与 libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/RangeIndexFileDataSink.cs:WriteChunk）
  async fn write_chunk(&mut self, start_address: u64, data: &[u8]) -> Result<(), String> {
    match &mut self.target {
      SinkTarget::Device(device) => {
        // 设备对齐契约：offset/len 均须扇区整数倍——段起始由发送侧扇区
        // 对齐切分保证，末块零填充至扇区边界（尾随零页由恢复期
        // [tail, page_end) 清零语义吸收）
        let sector = device.sector_size();
        let padded_len = data.len().next_multiple_of(sector).max(sector);
        let mut padded = vec![0u8; padded_len];
        padded[..data.len()].copy_from_slice(data);
        let buf = AlignedBuf::from_slice(&padded, sector)
          .map_err(|e| format!("IOERR alloc aligned buffer: {e}"))?;
        let buf_res = device.write_aligned(start_address, buf).await;
        buf_res
          .0
          .map_err(|e| format!("IOERR device write at {start_address}: {e}"))?;
        Ok(())
      }
      SinkTarget::File(file) => file
        .seek(SeekFrom::Start(start_address))
        .and_then(|_| file.write_all(data))
        .map_err(|e| format!("IOERR checkpoint file write at {start_address}: {e}")),
    }
  }

  /// 流收尾刷盘（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/FileDataSink.cs:Complete
  /// 与 libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/RangeIndexFileDataSink.cs:Complete）
  async fn complete(self) -> Result<(), String> {
    match self.target {
      SinkTarget::Device(device) => device
        .sync_data()
        .await
        .map_err(|e| format!("IOERR device sync: {e}")),
      SinkTarget::File(file) => file
        .sync_all()
        .map_err(|e| format!("IOERR index checkpoint sync: {e}")),
    }
  }
}

/// 元数据接收槽（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/MetadataDataSink.cs:MetadataDataSink）
///
/// 单消息整包写 + 收尾（C# WriteChunk + Complete using 作用域同构）
struct MetadataDataSink {
  path: PathBuf,
}

impl MetadataDataSink {
  /// 构造（对标 C# 构造的 type 分发：STORE_SNAPSHOT 承载 wcpr
  /// CheckpointMeta；STORE_INDEX 元数据 rust 无文件概念，由调用方短路）
  fn new(ctx: &CheckpointImportCtx, token: u128) -> Self {
    Self {
      path: ctx.checkpoint_dir.join(wcpr::meta_filename(token)),
    }
  }

  /// libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/MetadataDataSink.cs:WriteChunk
  ///
  /// libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/MetadataDataSink.cs:Complete
  ///
  /// 元数据整包落盘并收尾（C# WriteChunk + Complete using 作用域同构）：
  /// 元数据文件最后落盘 = 提交标记，崩溃时半截文件集绝不进入恢复视图
  fn write_chunk_and_complete(self, data: &[u8]) -> Result<(), String> {
    let file =
      File::create(&self.path).map_err(|e| format!("IOERR open checkpoint metadata file: {e}"))?;
    let mut file = file;
    file
      .write_all(data)
      .and_then(|_| file.sync_all())
      .map_err(|e| format!("IOERR checkpoint metadata write: {e}"))
  }
}

/// 检查点网络接收处理器（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:ReceiveCheckpointHandler）
///
/// activeSink 单文件状态机：顺序协议一次只开一个活跃文件，空载荷收尾后
/// 才允许下一个文件开槽。处理器挂在 ReplicationManager 上（对标 C#
/// recvCheckpointHandler 字段），同步尝试间的状态复位经 [`Self::reset`]
/// （对标 C# 每次 attach new ReceiveCheckpointHandler + finally Dispose）。
#[derive(Default)]
pub struct ReceiveCheckpointHandler {
  active_sink: Mutex<Option<FileDataSink>>,
}

impl ReceiveCheckpointHandler {
  /// libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:ReceiveCheckpointHandler（构造）
  pub fn new() -> Self {
    Self::default()
  }

  /// 同步尝试状态复位（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:Dispose
  /// 的 rust 对位：C# Dispose = cts 取消 + activeSink 收置 + 信号量/缓冲池
  /// 归还，rust 无回调桥接件（compio 直驱），活跃槽 Drop 即关句柄，故收置
  /// 收敛为本复位；调用点对标 C# TryReplicateDiskbasedSyncAsync 内
  /// `recvCheckpointHandler = new(...)` 与 finally Dispose 的置换语义：
  /// 新同步发起/恢复完成时弃置残留活跃槽，失败半截文件由
  /// BEGIN_REPLICA_RECOVER 失败路径清场）
  pub fn reset(&self) {
    *self.active_sink.lock() = None;
  }

  /// 统一接收入口（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:ProcessSnapshotData）
  ///
  /// 约定（C# 同款）：startAddress == -1 为单消息载荷（元数据整包直达）；
  /// 其余为流式文件段，空载荷收尾当前文件。
  pub async fn process_snapshot_data(
    &self,
    rm: &ReplicationManager,
    ctx: &CheckpointImportCtx,
    token: u128,
    file_type: CheckpointFileType,
    start_address: i64,
    data: &[u8],
  ) -> Result<(), String> {
    rm.update_last_primary_sync_time();

    // 单消息载荷分支（C# STORE_INDEX/STORE_SNAPSHOT → MetadataDataSink；
    // STORE_RANGEINDEX_SNAPSHOT 头帧 → RangeIndexFileDataSink 开槽挂起，
    // 后续段帧续写、空载荷收尾）
    if start_address == SINGLE_MESSAGE_ADDRESS {
      return match file_type {
        CheckpointFileType::StoreSnapshot => {
          MetadataDataSink::new(ctx, token).write_chunk_and_complete(data)
        }
        CheckpointFileType::StoreIndex => {
          if data.is_empty() {
            Ok(())
          } else {
            // rust wcpr 索引快照自带头+CRC 自描述，无独立索引元数据文件
            //（模块文档登记差异），非空载荷属协议违约
            Err(
              "STORE_INDEX metadata payload is not expected (unified checkpoint model)".to_string(),
            )
          }
        }
        // RangeIndex 检查点快照头帧（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/RangeIndexFileDataSink.cs:FromMetadata）：
        // 元数据 = key_id 16B LE，活跃槽必须已收尾（顺序协议：一次一个
        // 活跃文件，上一文件未收尾到达新头帧属违约）
        CheckpointFileType::StoreRangeindexSnapshot => {
          let mut guard = self.active_sink.lock();
          if guard.is_some() {
            return Err(
              "new checkpoint file header before end-of-stream of active sink".to_string(),
            );
          }
          let Some(bytes) = data.first_chunk::<{ size_of::<u128>() }>() else {
            return Err(format!(
              "RangeIndex snapshot header too short ({}, expected {})",
              data.len(),
              size_of::<u128>()
            ));
          };
          let key_id = u128::from_le_bytes(*bytes);
          let sink = FileDataSink::open_rangeindex_snapshot(ctx, token, key_id)?;
          *guard = Some(sink);
          Ok(())
        }
        // RI flush / 其余类型不接受单消息形态（C# EnableRangeIndexPreview=false
        // 抛异常分支同语义）
        CheckpointFileType::None
        | CheckpointFileType::StoreHlog
        | CheckpointFileType::StoreHlogObj
        | CheckpointFileType::StoreSnapshotObj
        | CheckpointFileType::StoreRangeindexFlush => Err(format!(
          "ProcessSnapshotData invalid startAddress for checkpoint type: {file_type:?}!"
        )),
      };
    }

    // 空载荷 = 当前文件流收尾哨兵
    if data.is_empty() {
      return self.complete_active_sink().await;
    }

    // 惰性开槽（顺序协议：上一文件未收尾前到达新文件段属协议违约）
    let needs_open = self.active_sink.lock().is_none();
    if needs_open {
      match file_type {
        CheckpointFileType::StoreHlog | CheckpointFileType::StoreIndex => {}
        _ => {
          return Err(format!(
            "ProcessSnapshotData invalid startAddress for checkpoint type: {file_type:?}!"
          ));
        }
      }
      let sink = FileDataSink::open(ctx, token, file_type).await?;
      *self.active_sink.lock() = Some(sink);
    }

    self.write_active_sink(token, start_address, data).await
  }

  /// 文件段接收（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:ProcessFileSegment）
  ///
  /// 旧形态命令（SEND_CKPT_FILE_SEGMENT）承载：无单消息约定，其余与
  /// [`Self::process_snapshot_data`] 段流分支同构
  pub async fn process_file_segment(
    &self,
    rm: &ReplicationManager,
    ctx: &CheckpointImportCtx,
    token: u128,
    file_type: CheckpointFileType,
    start_address: i64,
    data: &[u8],
  ) -> Result<(), String> {
    rm.update_last_primary_sync_time();

    if data.is_empty() {
      return self.complete_active_sink().await;
    }

    let needs_open = self.active_sink.lock().is_none();
    if needs_open {
      let sink = FileDataSink::open(ctx, token, file_type).await?;
      *self.active_sink.lock() = Some(sink);
    }

    self.write_active_sink(token, start_address, data).await
  }

  /// 活跃槽定位写：take 让出锁 → 脱锁 await 写入 → 回放槽体
  ///（串行协议下单写者，锁仅护状态检查，绝不横跨 await）
  async fn write_active_sink(
    &self,
    token: u128,
    start_address: i64,
    data: &[u8],
  ) -> Result<(), String> {
    let mut sink = {
      let mut guard = self.active_sink.lock();
      let sink = guard.take().ok_or("checkpoint sink vanished")?;
      if sink.token != token {
        *guard = Some(sink);
        return Err("checkpoint token changed before end-of-stream".to_string());
      }
      sink
    };
    let res = sink.write_chunk(start_address as u64, data).await;
    let mut guard = self.active_sink.lock();
    match res {
      Ok(()) => {
        *guard = Some(sink);
        Ok(())
      }
      // 写失败的槽体状态不可信，弃置由重试重开/复位面承接（C# 同款
      // 重开语义：分块自起始地址顺序覆盖）
      Err(msg) => Err(msg),
    }
  }

  /// 活跃槽收尾刷盘：take 让出锁后脱锁 await（match 暂设值存活至分支尾，
  /// 须先绑定再 await，锁绝不横跨 await）
  async fn complete_active_sink(&self) -> Result<(), String> {
    let sink = self.active_sink.lock().take();
    match sink {
      Some(sink) => sink.complete().await,
      None => Ok(()),
    }
  }

  /// 元数据接收（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:ProcessMetadata）
  ///
  /// 旧形态命令（SEND_CKPT_METADATA）承载：单消息整包写 + 收尾
  pub async fn process_metadata(
    &self,
    rm: &ReplicationManager,
    ctx: &CheckpointImportCtx,
    token: u128,
    file_type: CheckpointFileType,
    metadata: &[u8],
  ) -> Result<(), String> {
    rm.update_last_primary_sync_time();
    match file_type {
      CheckpointFileType::StoreSnapshot => {
        MetadataDataSink::new(ctx, token).write_chunk_and_complete(metadata)
      }
      // 索引元数据：rust 统一检查点模型无该文件（模块文档登记差异），容忍空载荷
      CheckpointFileType::StoreIndex => {
        if metadata.is_empty() {
          Ok(())
        } else {
          Err("STORE_INDEX metadata payload is not expected (unified checkpoint model)".to_string())
        }
      }
      _ => Err(format!("Invalid checkpoint filetype {file_type:?}")),
    }
  }
}
