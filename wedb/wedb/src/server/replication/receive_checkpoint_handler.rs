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

use core::sync::atomic::{AtomicBool, Ordering};
use std::{
  fs::{self, File, OpenOptions},
  io::{Seek, SeekFrom, Write},
  path::{Path, PathBuf},
  sync::Arc,
};

use parking_lot::Mutex;
use wbftree::RangeIndexManager;
use wdev::{Device, SegmentedDevice};
use wnode::resp::vector::vector_manager::VectorManager;

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
  /// 向量管理器（清理闸门联动）
  pub vector_manager: Option<Arc<VectorManager>>,
}

/// 单消息载荷哨兵地址（C# ProcessSnapshotData 约定：startAddress == -1
/// 表示整包元数据，随帧到达无需分段）
const SINGLE_MESSAGE_ADDRESS: i64 = -1;

/// 文件承载统一开口（create + truncate 写；控制面同步 syscall，见模块文档）
fn open_truncate(path: &Path) -> Result<File, String> {
  if let Some(parent) = path.parent() {
    let _ = fs::create_dir_all(parent);
  }
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
/// C# 的 Type 属性由开槽期类型分派消化，rust 槽体保留 token 和 file_type 防串流）
struct FileDataSink {
  token: u128,
  file_type: CheckpointFileType,
  target: SinkTarget,
  vector_manager: Option<Arc<VectorManager>>,
}

impl Drop for FileDataSink {
  fn drop(&mut self) {
    if let Some(vm) = self.vector_manager.take() {
      vm.resume_cleanup();
    }
  }
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
    if let Some(vm) = &ctx.vector_manager {
      vm.pause_cleanup_async();
    }
    Ok(Self {
      token,
      file_type,
      target,
      vector_manager: ctx.vector_manager.clone(),
    })
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
    if let Some(vm) = &ctx.vector_manager {
      vm.pause_cleanup_async();
    }
    Ok(Self {
      token,
      file_type: CheckpointFileType::StoreRangeindexSnapshot,
      target: SinkTarget::File(file),
      vector_manager: ctx.vector_manager.clone(),
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
        if (start_address & (sector as u64 - 1)) != 0 {
          return Err(format!(
            "start_address {start_address} is not aligned to device sector size {sector}"
          ));
        }
        let padded_len = data.len().next_multiple_of(sector).max(sector);
        let mut buf = device
          .pool()
          .get(padded_len)
          .map_err(|e| format!("IOERR alloc aligned buffer: {e}"))?;
        buf
          .set_len(padded_len)
          .map_err(|e| format!("IOERR set aligned buffer length: {e}"))?;
        let slice = buf.as_mut_slice();
        slice[..data.len()].copy_from_slice(data);
        slice[data.len()..].fill(0);
        let (buf_res, _buf) = device.write_aligned(start_address, buf).await;
        buf_res.map_err(|e| format!("IOERR device write at {start_address}: {e}"))?;
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
  async fn complete(mut self) -> Result<(), String> {
    if let Some(vm) = self.vector_manager.take() {
      vm.resume_cleanup();
    }
    match &self.target {
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
    if let Some(parent) = self.path.parent() {
      let _ = fs::create_dir_all(parent);
    }
    let file =
      File::create(&self.path).map_err(|e| format!("IOERR open checkpoint metadata file: {e}"))?;
    let mut file = file;
    file
      .write_all(data)
      .and_then(|_| file.sync_all())
      .map_err(|e| format!("IOERR checkpoint metadata write: {e}"))
  }
}

/// 接收入口共享元数据臂（process_snapshot_data 单消息分支与 process_metadata
/// 同形收口）：STORE_SNAPSHOT 整包落盘；STORE_INDEX 空载荷容忍、非空属协议
/// 违约（rust wcpr 索引快照自带头+CRC 自描述，无独立索引元数据文件，模块
/// 文档登记差异）；其余类型报非法 filetype
fn handle_metadata_payload(
  ctx: &CheckpointImportCtx,
  token: u128,
  file_type: CheckpointFileType,
  payload: &[u8],
) -> Result<(), String> {
  match file_type {
    CheckpointFileType::StoreSnapshot => {
      MetadataDataSink::new(ctx, token).write_chunk_and_complete(payload)
    }
    CheckpointFileType::StoreIndex => {
      if payload.is_empty() {
        Ok(())
      } else {
        Err("STORE_INDEX metadata payload is not expected (unified checkpoint model)".to_string())
      }
    }
    _ => Err(format!("Invalid checkpoint filetype {file_type:?}")),
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
  /// 本轮同步会话的接收入口闸门：StoreHlog 写在线设备半途失败即设备内容
  /// 已不可信，本轮余流须断（继续收会把半截覆盖伪装成完整快照喂给导入面）。
  /// 闸门随会话净态——复位点见 [`Self::reset`]（C# 每轮 attach 全新处理器），
  /// 跨会话的设备损坏屏障在 ClusterProvider 管理面，二者不得互串
  device_contaminated: AtomicBool,
  /// STORE_HLOG 段写脏标记：收到过 STORE_HLOG 段写，但在恢复成功前若被中断复位，判定设备已污染
  hlog_dirty: AtomicBool,
}

impl ReceiveCheckpointHandler {
  /// libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:ReceiveCheckpointHandler（构造）
  pub fn new() -> Self {
    Self::default()
  }

  /// 设备是否处于污染状态
  #[inline]
  pub fn is_device_contaminated(&self) -> bool {
    self.device_contaminated.load(Ordering::Acquire)
  }

  /// 标记本轮接收闸门关闭
  #[inline]
  pub fn mark_device_contaminated(&self) {
    self.device_contaminated.store(true, Ordering::Release);
  }

  /// 解除本轮接收闸门（一次成功的全量恢复收口，见 [`Self::on_recovery_success`]）
  #[inline]
  pub fn clear_device_contaminated(&self) {
    self.device_contaminated.store(false, Ordering::Release);
  }

  /// 三入口共用的本轮会话闸门：设备污染即拒收余流，放行则刷新主端同步时间
  /// （闸门只护本轮会话，复位点见 [`Self::reset`]）
  #[inline]
  fn session_gate(&self, rm: &ReplicationManager) -> Result<(), String> {
    if self.is_device_contaminated() {
      return Err(
        "StoreHlog receive failed in this sync attempt, refusing remaining checkpoint frames"
          .to_string(),
      );
    }
    rm.update_last_primary_sync_time();
    Ok(())
  }

  /// 是否有未提交的 StoreHlog 脏段写入
  #[inline]
  pub fn is_hlog_dirty(&self) -> bool {
    self.hlog_dirty.load(Ordering::Acquire)
  }

  /// 检查点成功导入收尾：活跃槽、脏标记与本轮接收闸门全净（在线引擎已
  /// 置换为快照视图，本轮设备事实自此不再存活）
  ///
  /// 接收闸门的清除与 [`Self::reset`] 同须成立，否则一次导入失败留下的
  /// 置位会跨过下一次收口永久闭锁（C# 无此形态：处理器字段随每次
  /// `new` 归零，见 ReplicaDiskbasedSync.cs:161）
  pub fn on_recovery_success(&self) {
    *self.active_sink.lock() = None;
    self.hlog_dirty.store(false, Ordering::Release);
    self.device_contaminated.store(false, Ordering::Release);
  }

  /// 同步尝试状态复位（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:Dispose
  /// 的 rust 对位：C# Dispose = cts 取消 + activeSink 收置 + 信号量/缓冲池
  /// 归还，rust 无回调桥接件（compio 直驱），活跃槽 Drop 即关句柄，故收置
  /// 收敛为本复位；调用点对标 C# TryReplicateDiskbasedSyncAsync:161 的
  /// `recvCheckpointHandler = new(...)`——全新处理器即零状态，本轮残留活跃槽
  /// 与接收闸门一并弃置：主端重连后自起始地址从头分块重推，顺序覆盖上轮
  /// 半写数据（ReceiveCheckpointHandler.cs:64-65 "On retry, this may reopen
  /// an existing file from a previous failed attempt. This is safe because
  /// chunks are streamed from the start, overwriting any partial data."），
  /// 故本复位绝不可把上轮故障闭锁成后续一切快照的拒收）
  ///
  /// 返回 true = 弃置了未成功恢复的 StoreHlog 半写：本轮接收面已净态可
  /// 续收，但设备损坏事实须由调用点升级到 ClusterProvider 的管理面屏障
  /// （本地 flush / take_checkpoint 在下一轮导入成功前拒绝服务——它们在
  /// C# 无对应闸门，是 rust 直写在线设备布局下自加的保护，保护窗口只到
  /// 下一轮恢复收口为止，不得反噬接收面）
  pub fn reset(&self) -> bool {
    let sink = self.active_sink.lock().take();
    let half_written_hlog = self.hlog_dirty.load(Ordering::Acquire)
      || sink
        .as_ref()
        .is_some_and(|sink| sink.file_type == CheckpointFileType::StoreHlog);
    drop(sink);
    self.hlog_dirty.store(false, Ordering::Release);
    self.device_contaminated.store(false, Ordering::Release);
    half_written_hlog
  }

  /// 统一接收入口（libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:ProcessSnapshotData）
  ///
  /// 约定（C# 同款）：startAddress == -1 为单消息载荷（元数据整包直达）；
  /// 其余为流式文件段，空载荷收尾当前文件。
  ///
  /// 入口闸门只护本轮会话：上轮半写不得继续收帧，但新一轮全量必须放行
  /// （主端从头重推覆盖），闸门开启点即 [`Self::reset`]
  pub async fn process_snapshot_data(
    &self,
    rm: &ReplicationManager,
    ctx: &CheckpointImportCtx,
    token: u128,
    file_type: CheckpointFileType,
    start_address: i64,
    data: &[u8],
  ) -> Result<(), String> {
    self.session_gate(rm)?;

    // 单消息载荷分支（C# STORE_INDEX/STORE_SNAPSHOT → MetadataDataSink；
    // STORE_RANGEINDEX_SNAPSHOT 头帧 → RangeIndexFileDataSink 开槽挂起，
    // 后续段帧续写、空载荷收尾）
    if start_address == SINGLE_MESSAGE_ADDRESS {
      return match file_type {
        // 元数据两臂与 process_metadata 同形，收口至共享函数
        CheckpointFileType::StoreSnapshot | CheckpointFileType::StoreIndex => {
          handle_metadata_payload(ctx, token, file_type, data)
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

    self
      .write_active_sink(token, file_type, start_address, data)
      .await
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
    self.session_gate(rm)?;

    if data.is_empty() {
      return self.complete_active_sink().await;
    }

    let needs_open = self.active_sink.lock().is_none();
    if needs_open {
      let sink = FileDataSink::open(ctx, token, file_type).await?;
      *self.active_sink.lock() = Some(sink);
    }

    self
      .write_active_sink(token, file_type, start_address, data)
      .await
  }

  /// 活跃槽定位写：take 让出锁 → 脱锁 await 写入 → 回放槽体
  ///（串行协议下单写者，锁仅护状态检查，绝不横跨 await）
  async fn write_active_sink(
    &self,
    token: u128,
    file_type: CheckpointFileType,
    start_address: i64,
    data: &[u8],
  ) -> Result<(), String> {
    let mut sink = {
      let mut guard = self.active_sink.lock();
      let sink = guard.take().ok_or("checkpoint sink vanished")?;
      if sink.token != token || sink.file_type != file_type {
        *guard = Some(sink);
        return Err("checkpoint token or file_type changed before end-of-stream".to_string());
      }
      sink
    };
    let start_address = u64::try_from(start_address)
      .map_err(|_| format!("start_address must be non-negative: {start_address}"))?;
    let res = sink.write_chunk(start_address, data).await;
    let mut guard = self.active_sink.lock();
    match res {
      Ok(()) => {
        if file_type == CheckpointFileType::StoreHlog {
          self.hlog_dirty.store(true, Ordering::Release);
        }
        *guard = Some(sink);
        Ok(())
      }
      // 写失败的槽体状态不可信，弃置由重试重开/复位面承接（C# 同款
      // 重开语义：分块自起始地址顺序覆盖）
      Err(msg) => {
        if file_type == CheckpointFileType::StoreHlog {
          self.device_contaminated.store(true, Ordering::Release);
        }
        Err(msg)
      }
    }
  }

  /// 活跃槽收尾刷盘：take 让出锁后脱锁 await（match 暂设值存活至分支尾，
  /// 须先绑定再 await，锁绝不横跨 await）
  async fn complete_active_sink(&self) -> Result<(), String> {
    let sink = self.active_sink.lock().take();
    match sink {
      Some(sink) => {
        let is_store_hlog = sink.file_type == CheckpointFileType::StoreHlog;
        let res = sink.complete().await;
        if res.is_err() && is_store_hlog {
          self.device_contaminated.store(true, Ordering::Release);
        }
        res
      }
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
    self.session_gate(rm)?;
    // 三臂 match（STORE_SNAPSHOT 落盘 / STORE_INDEX 容忍空载荷 / 其余非法）
    // 与快照入口单消息分支同形，收口至共享函数
    handle_metadata_payload(ctx, token, file_type, metadata)
  }
}
