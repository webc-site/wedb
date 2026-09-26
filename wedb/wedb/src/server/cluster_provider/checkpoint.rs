//! 纪元推进与检查点/恢复面：`garnet_current_epoch` 三件套（同步/异步两版静止
//! 等待）+ 按需快照、在线引擎置换、检查点导入依赖束与整体析构
//! （epoch 属副本读写一致性栅栏，与检查点/置换同生命周期，故并件）

use std::{
  fs::create_dir_all,
  ops::ControlFlow,
  sync::{Arc, atomic::Ordering},
};

use compio::time::sleep;
use wconf::ServerConfigType;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  cluster_session::ClusterSessionFace,
  servers::{ConsumerRegistry, ConsumerType},
  session_parse_state_extensions::ClientType,
};

use crate::{
  error,
  error::Error,
  server::{
    cluster_provider::ClusterProvider, replication::receive_checkpoint_handler::CheckpointImportCtx,
  },
};

impl ClusterProvider {
  /// 获取 Garnet 当前纪元（对标 C# GarnetCurrentEpoch）
  #[inline]
  pub fn current_epoch(&self) -> i64 {
    self.garnet_current_epoch.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/ClusterProvider.cs:BumpCurrentEpoch
  ///
  /// 推进 Garnet 集群纪元
  #[inline]
  pub fn bump_current_epoch(&self) -> i64 {
    self.garnet_current_epoch.fetch_add(1, Ordering::AcqRel) + 1
  }

  /// libs/cluster/Server/ClusterProvider.cs:BumpAndWaitForEpochTransitionAsync
  ///
  /// 推进集群纪元并自旋等待全部活跃集群会话批内纪元快照追平（C# 遍历
  /// storeWrapper.Servers → ActiveClusterSessions 逐会话重试至
  /// LocalCurrentEpoch 追平，快照 0 = 批外空闲放行；rust 每轮以
  /// yield_now 让步执行器，对标 C# await Task.Yield()）。以
  /// cluster_node_timeout() 为上限，超时返 false；None（0 = 无限）不设限，
  /// 与 C# 无限自旋一致（false 仅表达静止未达成，调用方按各自窗口不变量
  /// 裁决——无盘快照键门排空点 [`replication_snapshot_iterator`](crate::server::replication::diskless_replication::replication_snapshot_iterator)
  /// 返值承判、未达成即判败重拍）
  pub async fn bump_and_wait_for_epoch_transition_async(&self) -> bool {
    let current_epoch = self.bump_current_epoch();
    let limit = self.cluster_node_timeout();
    wepoch::wait_condition_async(
      None,
      false,
      || self.all_sessions_caught_up(current_epoch),
      |_| {},
      limit,
      sleep,
    )
    .await
  }

  /// 纪元推进全会话静止的命令批内同步形态（C# 命令侧
  /// `AsyncUtils.BlockingWait(BumpAndWaitForEpochTransitionAsync())` 的
  /// 语义：网络线程阻塞等待，见 RespClusterSlotManagementCommands.cs:493）
  ///
  /// compio 单线程每核下，发起会话所在线程的其余会话必处批外（快照 0），
  /// 阻塞自旋仅等他核会话收尾，无死锁；上限与追平判定同异步形态
  pub fn bump_and_wait_for_epoch_transition(&self) -> bool {
    let current_epoch = self.bump_current_epoch();
    let limit = self.cluster_node_timeout();
    wepoch::wait_condition_sync(
      None,
      false,
      || self.all_sessions_caught_up(current_epoch),
      |_| {},
      limit,
    )
  }

  /// 全部活跃集群会话纪元是否追平（ClusterProvider.cs:377
  /// ActiveClusterSessions 枚举的等价面；papaya 无锁快照枚举对标 C#
  /// ConcurrentDictionary 轻量迭代，静止等待不排阻会话注册注销；枚举时
  /// 顺带清扫过期弱引用）
  fn all_sessions_caught_up(&self, current_epoch: i64) -> bool {
    let pin = self.cluster_sessions.pin();
    pin
      .iter()
      .try_for_each(|(key, weak)| match weak.upgrade() {
        Some(s) => {
          let entry_epoch = s.local_current_epoch();
          // C# 判定取反：entryEpoch != 0 && entryEpoch < currentEpoch 才重试
          if entry_epoch == 0 || entry_epoch >= current_epoch {
            ControlFlow::Continue(())
          } else {
            ControlFlow::Break(())
          }
        }
        // 会话已亡：当场自清扫死弱引用，免注销钩子
        None => {
          pin.remove(key);
          ControlFlow::Continue(())
        }
      })
      .is_continue()
  }

  /// 按需拍摄快照（对标 C# StoreWrapper.TakeOnDemandCheckpointAsync）
  ///
  /// 检查点条目登记与截断唯一点在检查点内核
  /// （`wnode::database::DatabaseManagerBase::take_database_checkpoint_async`
  /// 的集群分支，对标 C# AddNewCheckpointEntry 仅在 InitiateCheckpointAsync
  /// 内调用一处），本入口不重复登记
  pub async fn take_on_demand_checkpoint(&self, entry_ms: u64) -> Result<bool, String> {
    if self.is_device_contaminated() {
      return Err(
        "Device is contaminated by a failed checkpoint receive, refusing checkpoint".to_string(),
      );
    }
    let Some(dm) = self.try_database_manager() else {
      return Ok(false);
    };
    let taken = dm
      .take_on_demand_checkpoint_async(entry_ms)
      .await
      .map_err(|e| format!("On-demand checkpoint failed: {e}"))?;
    Ok(taken)
  }

  /// 在线引擎置换（副本检查点导入闭环收口）：换入引擎先经宿主钩子束重挂
  /// 写面钩子再投槽——单次写本层引擎槽，宿主已采纳同槽时新引擎即对后续新
  /// 会话装配生效；随后联动换持已装配的逻辑数据库管理面引擎引用（对标 C#
  /// 全体调用方经 StoreWrapper.cs:41 单计算属性自动转发恢复后的引擎），
  /// 最后断开存量会话保「锁面==写面」不变量
  ///
  /// libs/server/StoreWrapper.cs:Reset
  ///（C# 副本全量同步前 `storeWrapper.Reset()` 拆除重建分配器准备接收主端
  /// 检查点：rust 无对位的分配器拆除——恢复出新引擎后经本口单次换指完成
  /// 「重置」，旧引擎随引用计数 Drop，见 replica_diskbased_sync 换库段）
  pub fn swap_online_store(&self, store: Arc<WedbStore<SegmentedDevice>>) {
    // 写面钩子先于投槽重挂（OnceLock 保首幂等）：C# 原位恢复的钩子接线跨
    // 恢复全程存活（functionsState.watchVersionMap / appendOnlyFile 与引擎
    // 同实例共享），rust 实例置换形态下换入引擎不经重挂则 WATCH 版本推进
    // 静默旁路、AOF per-op 镜像零条目、缺席删除登记观测断线（钩子束逐件
    // 承接 wkv::session::EngineHookSlots 全举三件）；投槽前挂载保证引擎
    // 可见即钩子在场，不存在无钩子写入窗口。宿主钩子束缺席（单测直驱形态）
    // 为无害空转
    if let Some(hooks) = self.try_engine_swap_hooks() {
      hooks(&store);
    }
    self.store_slot.read().swap(Arc::clone(&store));
    // 管理面联动（C# Store => databaseManager.Store 动态转发的 rust 对位）：
    // 换持 SingleDatabaseManager 持有的引擎引用，周期检查点/BGSAVE、
    // FLUSHDB/FLUSHALL、索引自适应扩容与 hlog 统计自此作用于新引擎，
    // 杜绝管理面持久化悬挂旧引擎致数据回滚
    if let Some(dm) = self.try_database_manager() {
      dm.swap_store(store);
    }
    // 断开存量客户端会话（C# 原位恢复不换实例，全体会话经单计算属性恒见
    // 同引擎的强一致语义；rust 实例置换形态下存量会话执行域在 get_session
    // 构造期钉死旧引擎，而事务锁表闭包现取当前引擎——不断开则 MULTI/EXEC
    // 屏障注册与桶闩落新引擎、读写落旧引擎，互斥双侧落空。断开令客户端重
    // 连即装配于新引擎：注册先于会话构造的时序保证置换前注册的消费者全部
    // 被本清扫命中，置换后才构造的消费者取口即新引擎，无双态残留）。
    // 射程收窄（r37-lockfix 发现 A，自 b092ece 起豁免互连）：
    // 仅清扫持有旧引擎执行域的客户端会话（ConsumerType::Client）。
    // 豁免复制与集群总线消费者（ConsumerType::Replication / Cluster），
    // 豁免 Master/Replica/Slave 互连与慢挂起会话，杜绝自断主从与集群拓扑。
    //
    // 射程实例级收口（r37-lockfix 发现 A 残留，txnfix2 第二节 6 条）：清扫
    // 面取本实例注册表（装配期 [`Self::set_consumer_registry`] 注入），多
    // 实例同进程形态（嵌入双实例等）下换引擎只断本实例的客户端会话，不越
    // ConsumerRegistry::global 进程级单槽误杀他实例全部 Normal 连接；未
    // 注入（单测直驱置换面）回退 global 兜底
    let registry = self
      .try_consumer_registry()
      .or_else(ConsumerRegistry::global);
    if let Some(registry) = registry {
      for entry in registry.active_consumers() {
        if entry.consumer_type() != ConsumerType::Client
          || matches!(
            entry.client_type(),
            ClientType::Master | ClientType::Replica | ClientType::Slave
          )
          || entry.is_in_slow_wait()
        {
          continue;
        }
        entry.kill_session();
      }
    }
  }

  /// 检查点导入落盘依赖束（三 arm 接收面现取现用；目录缺失时惰性创建）
  ///
  /// 错误两态分流（票 zcode-r135c 案一，复用 [`Error`] 既有变体零新增，对位
  /// C# ReceiveCheckpointHandler/RangeIndexFileDataSink 的类型化异常面）：
  /// 目录/引擎未接线属集群配置态（[`Error::ClusterNotInitialized`]，拓扑收敛
  /// 后可重试）；`create_dir_all` 失败属磁盘 IO 硬错（[`Error::Io`] 透明转发，
  /// 含 path/errno，需人工介入），唯一生产消费点
  /// [`execute_checkpoint_recv`](crate::server::cluster_session::replication)
  /// 据此分流应答帧，杜绝盘硬错坍缩为 CLUSTERNOTINIT 误导主端与运维
  pub fn checkpoint_import_ctx(&self) -> error::Result<CheckpointImportCtx> {
    let dir = self
      .try_checkpoint_dir()
      .ok_or(Error::ClusterNotInitialized)?;
    create_dir_all(&dir)?;
    let device = self
      .try_store()
      .ok_or(Error::ClusterNotInitialized)?
      .device
      .clone();
    Ok(CheckpointImportCtx {
      store_device: device,
      checkpoint_dir: dir,
      vector_manager: self.try_vector_manager(),
    })
  }

  /// 执行序列号生成器复位（故障转移触发时调用；对标 C#
  /// ReplicaFailoverSession.cs:154 经 storeWrapper.appendOnlyFile 直达
  /// GarnetAppendOnlyFile.ResetSequenceNumberGenerator，AOF 门面未装配
  /// 时空转——单物理日志模式 C# 侧同样短路）
  pub fn reset_sequence_number_generator(&self) {
    if let Some(aof) = self.try_aof() {
      aof.reset_sequence_number_generator();
    }
  }

  /// 注入副本重放最大滞后字节数（C# serverOptions.AofReplayMaxLagBytes 的
  /// 装配期注入；INFO 复制段直读）
  pub fn set_aof_replay_max_lag_bytes(&self, value: i32) {
    self
      .aof_replay_max_lag_bytes
      .store(value, Ordering::Relaxed);
  }

  /// 副本重放最大滞后字节数（优先从 runtime_config 实时读取，未注入回退原子槽；
  /// C# runtimeConfig.GetInt(AOF_REPLAY_MAX_LAG_BYTES) 读取面：-1 = 异步重放不节流，
  /// 0 = 同步重放（每帧锁步），>0 = 异步重放滞后超限阻塞推流；副本会话 ThrottlePrimary 门限源）
  #[inline]
  pub fn aof_replay_max_lag_bytes(&self) -> i32 {
    if let Some(cfg) = self.runtime_config.read().as_ref() {
      cfg.get_int(ServerConfigType::AofReplayMaxLagBytes)
    } else {
      self.aof_replay_max_lag_bytes.load(Ordering::Acquire)
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:Dispose
  pub fn dispose(&self) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.dispose();
    }
    if let Some(rm) = self.replication_manager() {
      rm.dispose();
    }
    if let Some(fm) = self.failover_manager() {
      fm.dispose();
    }
    if let Some(mm) = self.migration_manager() {
      mm.dispose();
    }
  }
}
