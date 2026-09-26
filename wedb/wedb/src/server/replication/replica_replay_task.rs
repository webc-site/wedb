//! 副本背景重放任务（异步重放模式的应用链主体）
//!
//! 文件本体对应 libs/cluster/Server/Replication/ReplicaOps/AOFReplay/
//! ReplicaReplayTask.cs（C# 重放任务工作体，经架构折叠 2 并作下方顺序
//! 常驻循环，锚挂 [`run_replay_loop`]）；同文件的驱动三件
//! （ReplicaReplayDriver 内，映射锚统一挂在下方真实承接点）：
//! - InitializeBackgroundReplayTask：幂等启动——首帧
//!   建 ScanSingle 迭代器并派生 BackgroundReplayTaskAsync 常驻消费；
//! - BackgroundReplayTaskAsync：BulkConsumeAllAsync
//!   常驻循环，流耗尽以 replica-sync-delay 槽位现值空转等待新数据，每轮
//!   Throttle 消化时间脉冲，异常记警告后终止任务；
//! - ConsumeDirect：批起点推进复制位点 → 逐记录分发：commit 元数据帧
//!   （payloadLength < 0）反序列化后 UnsafeCommitMetadataOnly 对齐本地提交
//!   边界、其余记录 ProcessAofRecordInternal 应用进存储 → 批终点推进位点
//!   （applied 语义，位点收敛至批终点）。
//!
//! 架构折叠（登记差异）：
//! 1. 专用 OS 线程 + 自有 compio runtime：wnode 线程每核模型下会话消费面
//!    （MessageConsumerFace）为同步 trait，无法在会话线程内联驱动异步存储
//!    应用，且同步阻塞会饿死同 runtime 任务；独立线程使 ThrottlePrimary
//!    阻塞等位期间重放照常推进（线程回退形态先例：waof_sublog commit）。
//! 2. C# 页级双闸栏并行重放（AofReplayTaskCount > 1）折叠为顺序消费
//!    （对齐 recover_log_driver 既有先例）。折叠只改并发形态、不改覆盖域：
//!    C# 一物理子日志配 R 条回放任务、每任务推自有虚拟子日志槽（页末
//!    UpdateVirtualSublogMaxSequenceNumber 与脉冲支 AdvanceVirtualSublogTime
//!    都按 GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx) 落槽），
//!    全槽并集即该物理子日志；本仓单消费者顺序消费覆盖该物理子日志全部条目，
//!    故批末读一致时间同样须推进该物理子日志的全部虚拟子日志（经
//!    ReadConsistencyManager::update_physical_sublog_max_sequence_number），
//!    否则停在非零槽的一致读等待者无法由真实回放链放行。
//! 3. C# 同步形态（AofReplayMaxLagBytes == 0）会话线程内联 ConsumeDirect
//!    折叠为「背景重放 + maxLag=0 阻塞至追平」：每帧 enqueue 后等应用
//!    追平再处理下一帧，锁步语义等价。

use std::{
  fmt,
  sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::Duration,
};

use compio::{runtime::Runtime, time::sleep};
use log::{error, warn};
use waof::Error;
use wconf::{RuntimeServerConfig, ServerConfigType};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  AofProcessor, AofReplayError,
  aof::{
    aof_processor::{ReplayTarget, ReplicaCheckpointHook},
    garnet_append_only_file::GarnetAppendOnlyFile,
  },
  range_index::range_index_manager_replication::RangeIndexManagerReplication,
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

/// 单轮消费窗口上限（C# BulkConsumeAllAsync maxChunkSize: 1 << 20）。
/// 副本重放与主端推流泵（aof_replication_pump 的单趟窗口）共用同一常量，
/// 对标 C# 两侧同取 maxChunkSize 的单源口径
pub(crate) const REPLAY_CHUNK_BYTES: i64 = 1 << 20;

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

/// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayTask.cs:FullPageBasedBackgroundReplayAsync
/// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:BackgroundReplayTaskAsync
/// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:ConsumeDirect
///
/// 常驻重放循环（本文件本体即 C# 重放任务：FullPageBasedBackgroundReplayAsync
/// 的页级双闸栏并行工作体按模块自述「架构折叠 2」并作顺序消费；驱动侧
/// BackgroundReplayTaskAsync + BulkConsumeAllAsync 的常驻迭代与
/// ConsumeDirect 的单条记录应用段亦折叠于此，逐记录应用语义不变）
///
/// 从 start_address 起按窗口流式扫描本地日志，逐条应用进入存储；流耗尽空转
/// REPLICA_SYNC_DELAY 等新数据；处置标志置位 / 资产失效即退出。
/// ConsumeDirect 的两处吞异常口径在此合一：批内失败按已成功工作量推进位点
/// （catch 段 SetSublogReplicationOffset）后由本循环 catch 记警告终止任务。
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
    // 驱动游标单调跃迁至日志 begin（对标 C# GetNextInternal 的
    // `if (currentAddress < allocator.BeginAddress) MonotonicUpdate(ref nextAddress,
    // allocator.BeginAddress)`，TsavoriteLogScanIterator.cs:752-757）：FastAofTruncate
    // 截断跳跃 / 从库全量快照恢复后 begin 可远超 applied。whlog 迭代器虽对内部
    // curr_addr 钳 begin（scan.rs 构造期 + next_ref 循环头），但那只救迭代器自身游标；
    // 本驱动若停留在旧 applied，窗口右界 end 仍按旧 applied 算，一旦 begin 跨过
    // applied + REPLAY_CHUNK_BYTES（>1MB 跳跃）便令传入的 end < begin，迭代器首轮
    // 即 curr_addr(=begin) >= effective_end(=end) 空返回，applied 永不前移 → 回放死锁
    // （replayed_offset 停摆、主端 ThrottlePrimary 永久挂起）。钳位于窗口计算之前，
    // 使 applied 与迭代器内部钳位同源同向，二者不再错配。
    let begin = assets.aof.log().get_begin_address(sublog_idx);
    applied = applied.max(begin);
    let tail = assets.aof.log().get_tail_address(sublog_idx);
    if tail <= applied {
      // 流耗尽：每轮 Throttle 消化时间脉冲（对标 BulkConsumeAllAsync
      // 每轮 consumer.Throttle），空转等待新数据（周期每轮现取槽位，
      // CONFIG SET replica-sync-delay 即时生效）
      driver.throttle();
      sleep(current_sync_delay(assets.runtime_config.as_deref())).await;
      continue;
    }
    // 窗口右界钳制（对标 maxChunkSize）：applied 已跃迁至 begin 之上，故
    // [applied, end) 合法非空，底层 scan_single_iter 的 start_addr > end_address
    // 区间反转不再触发；FastAofTruncate 跳跃重对齐后自此自然衔接跳跃点
    let end = tail.min(applied + REPLAY_CHUNK_BYTES);

    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    let target = ReplayTarget {
      session: &storage,
      store: Arc::clone(&assets.store),
      // 副本回放面位点域语义归复制域（重放游标由复制位点承担），不下沉
      // 恢复检查点覆盖边界；闸内 !as_replica 位判同此口径双保险
      aof_floor: vec![],
    };
    // 折叠道：单消费者以回放任务 0 的下标面记账（协调器上下文 / 检查点标记 /
    // key 草图入账槽），读一致时间的覆盖域补维在批末统一落全部槽
    let virtual_sublog_idx = assets.aof.get_virtual_sublog_idx(sublog_idx, 0);

    // 直驱迭代器扁平循环（对标 C# ConsumeDirect 页内 while 直调结构）：
    // 逐记录直 await 应用，消逐记录 async 闭包包装与跨闭包记账的原子操作，
    // 位点 / 批首标记为普通可变局部变量
    let mut scanned_any = false;
    let mut next = applied;
    let mut scan_err: Option<AofReplayError> = None;
    let mut iter = assets.aof.log().scan_single_iter(sublog_idx, applied, end);
    loop {
      match iter.next().await {
        Ok(Some(record)) => {
          let record_address = record.address as i64;
          if !scanned_any {
            scanned_any = true;
            // 批起点位点推进（对标 ConsumeDirect 首行 SetSublogReplicationOffset(currentAddress)）
            rm.set_sublog_replication_offset(sublog_idx, record_address);
          }
          // commit 元数据帧拦截（对标 ConsumeDirect 的 payloadLength < 0 分支，
          // ReplicaReplayDriver.cs:189-195；页级并行形态 replayTaskIdx == 0 才
          // 对齐的防重臂随「架构折叠 2」单消费者化而收敛为本唯一对齐点）：
          // 解出 CommitMeta 后把副本本地日志提交边界原子对齐主端帧位，不进
          // 数据条目回放。解码即判据单点（is_commit_frame 是其布尔视图），
          // 主端 pump 保真推流、会话 enqueue_raw 保真落盘的帧字节在此兑现
          if let Some(meta) = waof::decode_payload(&record.payload) {
            if let Err(err) = assets
              .aof
              .log()
              .get_sub_log(sublog_idx)
              .unsafe_commit_metadata_only(meta, record.next_address as i64)
              .await
            {
              scan_err = Some(err.into());
              break;
            }
            next = record.next_address as i64;
            continue;
          }
          match assets
            .processor
            .process_aof_record_internal(
              virtual_sublog_idx,
              &record.payload,
              true,
              record_address,
              &target,
            )
            .await
          {
            Ok(is_checkpoint_start) => {
              if is_checkpoint_start {
                // 检查点起始标记（对标 ReplicationCheckpointStartOffset[sublogIdx]
                // = replicationOffset：副本侧检查点截断点记账）
                rm.set_sublog_checkpoint_start_offset(sublog_idx, record_address);
              }
              next = record.next_address as i64;
            }
            // 处理器错误：next 停在最后成功记录的后继位点，批末按已成功
            // 工作量统一记账后终止任务
            Err(err) => {
              scan_err = Some(err);
              break;
            }
          }
        }
        Ok(None) => break,
        // 缺数据优于错数据：扫描 IO 异常显式报告并截断本批（已应用区间
        // 有效，不终止任务；对标 WaofSublog::scan_async_with 内嵌同语义分支）
        // 但 AOF 帧 CRC 失配（ChecksumMismatch）属数据失真确定性信号，
        // 须对接 divergent 断流重同步收敛通路（重置驱动仓使活跃复制流判定复位、
        // 终止重放任务并阻断主端推流），杜绝副本以「在同步」假象长期滞留
        Err(err) => {
          if matches!(err, Error::ChecksumMismatch { .. }) {
            error!(
              "副本重放 AOF 帧 CRC 校验失败 @ {addr:#x}: {err:?}，触发 divergent 断流重同步",
              addr = iter.current_address()
            );
            rm.reset_replica_replay_driver_store();
            scan_err = Some(err.into());
            break;
          }
          error!(
            "副本重放扫描磁盘段读取失败 @ {addr:#x}: {err:?}",
            addr = iter.current_address()
          );
          break;
        }
      }
    }
    if iter.overwritten_skips() > 0 {
      error!(
        "副本重放扫描中 {skipped} 条未提交记录被环形覆写，区间数据不完整",
        skipped = iter.overwritten_skips()
      );
    }

    if scanned_any {
      applied = next;
      // 读一致时间补维（对标 C# 回放任务页末 UpdateVirtualSublogMaxSequence
      // Number(virtualSublogIdx, nextAddress) 的全槽并集）：本消费者已把本批全
      // 部条目应用进存储，该物理子日志的每个虚拟子日志都可安全推进——停在非零
      // 虚拟子日志上的一致读等待者自此有了真实回放链放行源。
      // 前沿先行、位点后发：复制位点即「读一致时间已覆盖」的可观察凭证，读者
      // 不会落进位点已发而前沿未发的窗口。
      if let Some(rcm) = assets.aof.read_consistency_manager() {
        rcm.update_physical_sublog_max_sequence_number(sublog_idx, next);
      }
      // 批终点推进（对标 ConsumeDirect 尾段 SetSublogReplicationOffset(replicationOffset)）
      rm.set_sublog_replication_offset(sublog_idx, next);
      driver.replayed_offset.fetch_max(next, Ordering::AcqRel);
      // 位点推进唤醒节流等待者（ThrottlePrimary 解除阻塞的信号源）
      driver.notify_progress();
    }

    match scan_err {
      None => {
        if !scanned_any {
          driver.throttle();
          sleep(current_sync_delay(assets.runtime_config.as_deref())).await;
          continue;
        }
        driver.throttle();
      }
      // C# ConsumeDirect 异常路径按已成功工作量推进位点后上抛，
      // BackgroundReplayTaskAsync catch 记警告终止任务
      Some(err) => {
        warn!("An exception occurred at ReplicationManager.ReplicaReplayTask - terminating: {err}");
        break;
      }
    }
  }
  driver.notify_progress();
}
