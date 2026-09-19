//! 副本背景重放任务（异步重放模式的应用链主体）
//!
//! 对标 C# 形态（libs/cluster/Server/Replication/ReplicaOps/AOFReplay/）
//! - ReplicaReplayDriver.cs:InitializeBackgroundReplayTask：幂等启动——首帧
//!   建 ScanSingle 迭代器并派生 BackgroundReplayTaskAsync 常驻消费；
//! - ReplicaReplayDriver.cs:BackgroundReplayTaskAsync：BulkConsumeAllAsync
//!   常驻循环，流耗尽以 replica-sync-delay 槽位现值空转等待新数据，每轮
//!   Throttle 消化时间脉冲，异常记警告后终止任务；
//! - ReplicaReplayDriver.cs:ConsumeDirect：批起点推进复制位点 → 逐记录
//!   ProcessAofRecordInternal 应用进存储 → 批终点推进位点（applied 语义，
//!   即 task/done/replica-offset-semantics.md 登记的位点收敛路径）。
//!
//! 架构折叠（登记差异）：
//! 1. 专用 OS 线程 + 自有 compio runtime：wnode 线程每核模型下会话消费面
//!    （MessageConsumerFace）为同步 trait，无法在会话线程内联驱动异步存储
//!    应用，且同步阻塞会饿死同 runtime 任务；独立线程使 ThrottlePrimary
//!    阻塞等位期间重放照常推进（线程回退形态先例：waof_sublog commit）。
//! 2. C# 页级双闸栏并行重放（AofReplayTaskCount > 1）折叠为顺序消费
//!    （对齐 recover_log_driver 既有先例）。
//! 3. C# 同步形态（AofReplayMaxLagBytes == 0）会话线程内联 ConsumeDirect
//!    折叠为「背景重放 + maxLag=0 阻塞至追平」：每帧 enqueue 后等应用
//!    追平再处理下一帧，锁步语义等价。

use std::{
  fmt,
  sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicI64, Ordering},
  },
  thread,
  time::Duration,
};

use compio::{runtime::Runtime, time::sleep};
use log::{error, warn};
use wconf::{RuntimeServerConfig, ServerConfigType};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  AofProcessor, AofReplayError,
  aof::{
    aof_processor::{ReplayTarget, ReplicaCheckpointHook},
    garnet_append_only_file::GarnetAppendOnlyFile,
  },
  rangeindex::range_index_manager_replication::RangeIndexManagerReplication,
  storage::session::storage_session::StorageSession,
};

use super::replication_manager::ReplicationManager;
use crate::server::replication::replica_replay_driver::ReplicaReplayDriver;

/// 空转等待周期兜底值（真值源为 wconf 槽位 replica-sync-delay，运行期
/// CONFIG SET 即时生效；本常量仅在重放 / 推流域无配置句柄的退化形态下
/// 兜底——C# defaults.conf ReplicaSyncDelayMs = 5，
/// ServerConfigType.REPLICA_SYNC_DELAY 的 Task.Delay 形态同源）
pub(crate) const DEFAULT_REPLICA_SYNC_DELAY: Duration = Duration::from_millis(5);

/// 现取流耗尽空转等待周期（libs/cluster/Server/Replication/ReplicaOps/
/// AOFReplay/ReplicaReplayDriver.cs:314 `BulkConsumeAllAsync(this,
/// runtimeConfig.GetInt(REPLICA_SYNC_DELAY), ...)` 的每轮现取对译）；
/// 无配置句柄形态回落兜底常量，负值钳 0（不延迟）
pub(crate) fn current_sync_delay(runtime_config: Option<&RuntimeServerConfig>) -> Duration {
  runtime_config
    .map(|rc| {
      Duration::from_millis(
        rc.get_milliseconds(ServerConfigType::ReplicaSyncDelay)
          .max(0) as u64,
      )
    })
    .unwrap_or(DEFAULT_REPLICA_SYNC_DELAY)
}

/// 单轮消费窗口上限（C# BulkConsumeAllAsync maxChunkSize: 1 << 20）
const REPLAY_CHUNK_BYTES: i64 = 1 << 20;

/// 副本重放应用资产（对标 C# rm 构造期 aofProcessor + clusterProvider.
/// storeWrapper 可达面的注入束；rust 装配期差异同 set_commit_channel 先例）
pub struct ReplayAssets {
  /// 追加日志门面（扫描 / tail / 读一致时间源，对标 appendOnlyFile）
  pub aof: Arc<GarnetAppendOnlyFile>,
  /// 存储引擎（重放应用目标，对标 storeWrapper 存储可达面）
  pub store: Arc<WedbStore<SegmentedDevice>>,
  /// 长生命周期重放处理器（事务/分块/模糊区状态跨记录，对标
  /// recordToAof: false 的 AofProcessor；仅背景重放线程触达）
  processor: AofProcessor,
  /// 运行时配置（对标 C# storeWrapper.runtimeConfig 可达面：重放空转周期
  /// 每轮现取 replica-sync-delay；None = 装配域无配置句柄的退化形态，
  /// 回落兜底常量）
  pub runtime_config: Option<Arc<RuntimeServerConfig>>,
}

impl fmt::Debug for ReplayAssets {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ReplayAssets").finish_non_exhaustive()
  }
}

impl ReplayAssets {
  /// 构建重放资产（AofProcessor 挂范围索引重放面与副本本地检查点钩子，
  /// 对标 replay_into_session 装配形态）。钩子 None = 无本地打点面的退化装配
  /// （测试 / 无库管理器场景），检查点结束臂据此维持仅退出模糊区 + 重放缓冲
  /// 条目的原语义；生产装配必注入（对标 C# AofProcessor.cs:302-319 触发
  /// storeWrapper.TakeCheckpointAsync）。
  pub fn new(
    aof: Arc<GarnetAppendOnlyFile>,
    store: Arc<WedbStore<SegmentedDevice>>,
    runtime_config: Option<Arc<RuntimeServerConfig>>,
    checkpoint_hook: Option<Arc<ReplicaCheckpointHook>>,
  ) -> Self {
    let mut processor = AofProcessor::new(Arc::clone(&aof));
    processor.set_range_index_manager(Arc::new(RangeIndexManagerReplication::new(Arc::clone(
      store.range_index(),
    ))));
    if let Some(hook) = checkpoint_hook {
      processor.set_checkpoint_hook(hook);
    }
    Self {
      aof,
      store,
      processor,
      runtime_config,
    }
  }
}

/// 启动背景重放线程（派生失败返回 false；线程体异常自终止并记警告）
pub(crate) fn spawn(
  driver: Arc<ReplicaReplayDriver>,
  assets: Arc<ReplayAssets>,
  rm: Weak<ReplicationManager>,
  start_address: i64,
  stop: Arc<AtomicBool>,
) -> bool {
  thread::Builder::new()
    .name("replica-replay".into())
    .spawn(move || {
      let Ok(rt) = Runtime::new() else {
        error!("背景重放线程 compio runtime 创建失败");
        return;
      };
      rt.block_on(run_replay_loop(driver, assets, rm, start_address, stop));
    })
    .is_ok()
}

/// 常驻重放循环（对标 BackgroundReplayTaskAsync + BulkConsumeAllAsync）
///
/// 从 start_address 起按窗口流式扫描本地日志，逐条应用进入存储；流耗尽空转
/// REPLICA_SYNC_DELAY 等新数据；处置标志置位 / 资产失效即退出。
async fn run_replay_loop(
  driver: Arc<ReplicaReplayDriver>,
  assets: Arc<ReplayAssets>,
  rm: Weak<ReplicationManager>,
  start_address: i64,
  stop: Arc<AtomicBool>,
) {
  // 独占纪元参与者的重放会话（对标 C# 重放任务自有 epoch 保护）
  let Ok(session) = assets.store.new_session() else {
    log::error!("背景重放任务存储会话创建失败，任务终止");
    return;
  };
  // recordToAof:false 形态：重放应用不镜像回写本地 wal（守卫随任务存活；
  // 复制期副本唯一存储写入源是重放链，全局暂停 = C# 处理器旗标语义）
  let _pause = assets.store.pause_aof_listeners();
  let sublog_idx = driver.physical_sublog_idx;
  let mut applied = start_address;

  while !stop.load(Ordering::Acquire) {
    let Some(rm) = rm.upgrade() else {
      break;
    };
    let tail = assets.aof.log().get_tail_address(sublog_idx);
    if tail <= applied {
      // 流耗尽：每轮 Throttle 消化时间脉冲（对标 BulkConsumeAllAsync
      // 每轮 consumer.Throttle），空转等待新数据（周期每轮现取槽位，
      // CONFIG SET replica-sync-delay 即时生效）
      driver.throttle();
      sleep(current_sync_delay(assets.runtime_config.as_deref())).await;
      continue;
    }
    // 窗口钳制（对标 maxChunkSize）；扫描起点 clamp 至日志 begin，
    // FastAofTruncate 跳跃重对齐后自然衔接跳跃点
    let end = tail.min(applied + REPLAY_CHUNK_BYTES);

    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    let target = ReplayTarget {
      session: &storage,
      store: Arc::clone(&assets.store),
      store_version: assets.store.current_version(),
    };
    let virtual_sublog_idx = assets.aof.get_virtual_sublog_idx(sublog_idx, 0);

    let current_applied = AtomicI64::new(applied);
    let scanned_any = AtomicBool::new(false);
    let assets_ref = &assets;
    let rm_ref = &rm;
    let target_ref = &target;
    let current_applied_ref = &current_applied;
    let scanned_any_ref = &scanned_any;

    let scan_res = assets
      .aof
      .log()
      .scan_single_async_with::<AofReplayError, _, _>(sublog_idx, applied, end, |record| {
        let record_address = record.address as i64;
        let next_address = record.next_address as i64;
        let payload = record.payload;
        if !scanned_any_ref.swap(true, Ordering::Relaxed) {
          // 批起点位点推进（对标 ConsumeDirect 首行 SetSublogReplicationOffset(currentAddress)）
          rm_ref.set_sublog_replication_offset(sublog_idx, record_address);
        }
        async move {
          let is_checkpoint_start = assets_ref
            .processor
            .process_aof_record_internal(
              virtual_sublog_idx,
              &payload,
              true,
              record_address,
              target_ref,
            )
            .await?;
          if is_checkpoint_start {
            // 检查点起始标记（对标 ReplicationCheckpointStartOffset[sublogIdx]
            // = replicationOffset：副本侧检查点截断点记账）
            rm_ref.set_sublog_checkpoint_start_offset(sublog_idx, record_address);
          }
          current_applied_ref.store(next_address, Ordering::Relaxed);
          Ok(true)
        }
      })
      .await;

    if !scanned_any.load(Ordering::Relaxed) {
      driver.throttle();
      sleep(current_sync_delay(assets.runtime_config.as_deref())).await;
      continue;
    }

    let next = current_applied.load(Ordering::Relaxed);
    applied = next;
    // 批终点推进（对标 ConsumeDirect 尾段 SetSublogReplicationOffset(replicationOffset)）
    rm.set_sublog_replication_offset(sublog_idx, next);
    driver.replayed_offset.fetch_max(next, Ordering::AcqRel);
    // 位点推进唤醒节流等待者（ThrottlePrimary 解除阻塞的信号源）
    driver.notify_progress();

    match scan_res {
      Ok(()) => {
        driver.throttle();
      }
      // C# ConsumeDirect 异常路径按已成功工作量推进位点后上抛，
      // BackgroundReplayTaskAsync catch 记警告终止任务
      Err(err) => {
        warn!("An exception occurred at ReplicationManager.ReplicaReplayTask - terminating: {err}");
        break;
      }
    }
  }
  driver.notify_progress();
}
