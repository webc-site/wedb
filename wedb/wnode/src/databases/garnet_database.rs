//! 逻辑数据库容器（对标 libs/server/GarnetDatabase.cs:GarnetDatabase）
//!
//! C# 侧每个 GarnetDatabase 独占一套 TsavoriteKV + AOF + WatchVersionMap；
//! wkv 单库模型下 db 为会话前缀编号，多个 [`GarnetDatabase`] 共享同一
//! [`WedbStore`]（键经 ns/db 前缀物理隔离）与同一 AOF 域。

use std::{
  path::PathBuf,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
  },
};

use wdev::Device;
use wkv::WedbStore;
use wtxn::WatchVersionMap;

use crate::{
  aof::GarnetAppendOnlyFile, storage::sizetracker::cache_size_tracker::CacheSizeTracker,
};

/// WATCH 版本表默认桶数（libs/server/GarnetDatabase.cs:DefaultVersionMapSize）
pub const DEFAULT_VERSION_MAP_SIZE: u64 = 1 << 16;

/// 逻辑数据库
pub struct GarnetDatabase<D: Device> {
  /// 数据库编号
  pub id: i64,
  /// 存储引擎句柄（多库共享，前缀隔离）
  pub store: Arc<WedbStore<D>>,
  /// WATCH 版本表（libs/server/GarnetDatabase.cs:VersionMap；库内全部会话共享）
  pub version_map: Arc<WatchVersionMap>,
  /// 底层存储设备（检查点恢复重建存储所需）
  pub device: Arc<D>,
  /// 检查点落盘目录
  pub checkpoint_dir: PathBuf,
  /// AOF 域句柄（`enable_aof` 为假时为 None；全库共享同一实例）
  pub aof: Option<Arc<GarnetAppendOnlyFile>>,
  /// 上次保存时的写日志尾地址
  pub last_save_store_tail_address: AtomicU64,
  /// 上次保存时间（毫秒 Unix 时间戳，0 表示从未保存）
  pub last_save_ms: AtomicU64,
  /// 存储索引是否已达上限
  pub store_index_maxed_out: AtomicBool,
  /// 检查点暂停标志（对标 CheckpointingLock 写锁占位）
  pub checkpoint_paused: AtomicBool,
  /// 对象堆大小追踪器
  pub size_tracker: Arc<CacheSizeTracker>,
}

impl<D: Device> GarnetDatabase<D> {
  /// 创建逻辑数据库
  pub fn new(
    id: i64,
    store: Arc<WedbStore<D>>,
    device: Arc<D>,
    checkpoint_dir: PathBuf,
    aof: Option<Arc<GarnetAppendOnlyFile>>,
  ) -> Self {
    Self {
      id,
      store,
      version_map: Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE)),
      device,
      checkpoint_dir,
      aof,
      last_save_store_tail_address: AtomicU64::new(0),
      last_save_ms: AtomicU64::new(0),
      store_index_maxed_out: AtomicBool::new(false),
      checkpoint_paused: AtomicBool::new(false),
      size_tracker: Arc::new(CacheSizeTracker::new()),
    }
  }

  /// 基于具体 GarnetAppendOnlyFile 构建逻辑数据库（打通数据库与 AOF 无缝链接）
  pub fn with_garnet_aof(
    id: i64,
    store: Arc<WedbStore<D>>,
    device: Arc<D>,
    checkpoint_dir: PathBuf,
    aof: Option<Arc<GarnetAppendOnlyFile>>,
  ) -> Self {
    Self::new(id, store, device, checkpoint_dir, aof)
  }

  /// 记录一次成功保存（时间戳 + 存储尾地址）
  pub fn update_last_save(&self, now_ms: u64) {
    self.last_save_ms.store(now_ms, Relaxed);
    self
      .last_save_store_tail_address
      .store(self.store.tail_address(), Relaxed);
  }

  /// 上次保存时间（毫秒 Unix 时间戳）
  pub fn last_save_ms(&self) -> u64 {
    self.last_save_ms.load(Relaxed)
  }

  /// AOF 当前总字节数（未启用 AOF 时为 0；C# TotalSize = Tail - Begin 聚合）
  pub fn aof_size(&self) -> u64 {
    self
      .aof
      .as_ref()
      .map_or(0, |aof| aof.total_size().max(0) as u64)
  }
}
