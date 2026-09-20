//! 逻辑数据库容器（对标 libs/server/GarnetDatabase.cs:GarnetDatabase）
//!
//! ### 架构设计：节点级单 WatchVersionMap
//! C# Garnet 原实现中每个 GarnetDatabase 独占一套 TsavoriteKV + AOF + WatchVersionMap；
//! 而 WeDB 采用共享单存储模型（键经 ns/db 前缀物理隔离）与单 AOF 域。
//! 在 WATCH/MULTI/EXEC 事务机制上，WeDB 采用节点级唯一的 [`wtxn::WatchVersionMap`]
//! （由 `NodeService` 统一持有），统一接线到底层存储写钩子（`store.set_watch_hook`）
//! 及各会话事务管理器。因此 [`GarnetDatabase`] 不再各自分配未接线的 `version_map` 死字段。

use std::{
  path::PathBuf,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
  },
};

use event_listener::Event;
use wdev::Device;
use wkv::WedbStore;

use crate::aof::GarnetAppendOnlyFile;

/// 逻辑数据库
pub struct GarnetDatabase<D: Device> {
  /// 数据库编号
  pub id: i64,
  /// 存储引擎句柄（多库共享，前缀隔离）
  pub store: Arc<WedbStore<D>>,
  /// 底层存储设备（检查点恢复重建存储所需）
  pub device: Arc<D>,
  /// 检查点落盘目录
  pub checkpoint_dir: PathBuf,
  /// AOF 域门面（`enable_aof` 为假时为 None；全库共享同一实例）
  pub aof: Option<Arc<GarnetAppendOnlyFile>>,
  /// 上次保存时的写日志尾地址
  pub last_save_store_tail_address: AtomicU64,
  /// 上次保存时间（毫秒 Unix 时间戳，0 表示从未保存）
  pub last_save_ms: AtomicU64,
  /// 检查点暂停标志（对标 CheckpointingLock 写锁占位）
  pub checkpoint_paused: AtomicBool,
  /// 还闸通知（按需入口让渡等待的唤醒源：resume_checkpoints 还闸时精准
  /// 唤醒一个等待者，对标 C# TakeOnDemandCheckpointAsync 的 while +
  /// Task.Yield 让渡重试）
  pub checkpoint_gate_resume: Event,
  /// 索引是否已达上限（对标 libs/server/GarnetDatabase.cs:StoreIndexMaxedOut）
  pub store_index_maxed_out: AtomicBool,
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
      device,
      checkpoint_dir,
      aof,
      last_save_store_tail_address: AtomicU64::new(0),
      last_save_ms: AtomicU64::new(0),
      checkpoint_paused: AtomicBool::new(false),
      checkpoint_gate_resume: Event::new(),
      store_index_maxed_out: AtomicBool::new(false),
    }
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
}

impl<D: Device> GarnetDatabase<D> {
  /// libs/server/StoreWrapper.cs:AofSize
  ///
  /// AOF 当前总字节数（未启用 AOF 时为 0；C# TotalSize = Tail - Begin 聚合）
  pub fn aof_size(&self) -> u64 {
    self
      .aof
      .as_ref()
      .map_or(0, |aof| aof.total_size().max(0) as u64)
  }
}
