//! 装配期注入槽族：两阶段构造的自有形态集中处（store / primary_tasks /
//! vector_manager / pubsub / aof / runtime_config / wal / 副本接收会话 /
//! 主端推流装配面 / 检查点目录 / 置换槽 / 数据库管理器）与集群账号读取面
//! —— C# 侧这些依赖由 ClusterProvider 构造器注入，本层为装配期逐槽注入

use std::{path::PathBuf, sync::Arc};

use waof::WalLog;
use wconf::RuntimeServerConfig;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  PrimaryTasks, aof::garnet_append_only_file::GarnetAppendOnlyFile,
  database::SingleDatabaseManager, resp::vector::vector_manager::VectorManager,
  servers::ConsumerRegistry, service::StoreSwapSlot,
};
use wpubsub::subscribe_broker::SubscribeBroker;
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;

use crate::server::{
  cluster_provider::{ClusterProvider, EngineSwapHooks, PrimaryReplicationAssets},
  replication::{
    cluster_replication_session::ClusterReplicationSession, store_commit::StoreCommitFn,
  },
};

impl ClusterProvider {
  /// 集群检查点装配：注入 storeWrapper 提交标记写入回调（一次注入）
  ///
  /// 对标 C# ReplicationManager 构造内经 clusterProvider.storeWrapper 反查
  /// AOF 写入面（Rust 依赖方向反转，由装配层正向注入 GarnetLog 适配闭包）
  pub fn set_commit_channel(&self, commit: Option<StoreCommitFn>) {
    if let Some(rm) = self.replication_manager() {
      rm.set_commit_channel(commit);
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterUsername
  pub fn cluster_username(&self) -> Option<String> {
    self.auth_container.read().0.clone()
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterPassword
  pub fn cluster_password(&self) -> Option<String> {
    self.auth_container.read().1.clone()
  }

  /// 更新集群互信凭据（libs/cluster/Server/ClusterProvider.cs:UpdateClusterAuth
  /// 构造期注入与运行期 CONFIG SET 共用单点）：用户名 None 复用旧值（C#
  /// `clusterUsername ?? oldAuthContainer.ClusterUsername`），密码 None 即清空。
  /// 装配期 boot 以启动参数播种，运行期 CONFIG SET 热更，同一 auth_container
  /// RwLock 单源，gossip / 复制 / failover 五类出站握手经 cluster_username/
  /// password 访问器现取
  pub fn update_cluster_auth(
    &self,
    cluster_username: Option<String>,
    cluster_password: Option<String>,
  ) {
    let mut auth = self.auth_container.write();
    let old_user = auth.0.clone();
    *auth = (cluster_username.or(old_user), cluster_password);
  }

  /// 播种当前在线引擎（集群装配期一次调用；对标 C# 构造期经 storeWrapper
  /// 建立的存储可达面。写入本层引擎槽——宿主槽注入后与之同源，全链一份引擎
  /// 状态，无第二份拷贝）
  pub fn set_store(&self, store: Arc<WedbStore<SegmentedDevice>>) {
    self.store_slot.read().swap(store);
  }

  /// 当前在线引擎（唯一读取口，取自本层引擎槽；未播种时 None）
  pub fn try_store(&self) -> Option<Arc<WedbStore<SegmentedDevice>>> {
    self.store_slot.read().get()
  }

  /// 注入 Primary 类后台任务生命周期域（集群装配期一次调用；按当前角色
  /// 初始化挂起态——恢复态副本节点在此同步停 GC，对标 C# StoreWrapper.Start
  /// 按角色分派 StartPrimaryTasks / StartReplicaTasks）
  pub fn set_primary_tasks(&self, tasks: Arc<PrimaryTasks>) {
    if self.is_replica() {
      tasks.suspend();
      if let Some(store) = self.try_store() {
        store.stop_gc();
      }
    }
    *self.primary_tasks.write() = Some(tasks);
  }

  /// Primary 类后台任务生命周期域（未注入时 None）
  pub fn primary_tasks(&self) -> Option<Arc<PrimaryTasks>> {
    self.primary_tasks.read().clone()
  }

  /// 挂起 Primary 类后台任务（角色位翻转停周期任务轮次 + 停内置 GC 扫描/
  /// 紧缩；对标 libs/server/StoreWrapper.cs:SuspendPrimaryOnlyTasksAsync——
  /// 降副本 TryAddReplicaAsync、REPLICAOF 指向主端、全量同步 attach 前
  /// 调用。副本读路径惰性过期与确定性 TtlPurge 重放不受影响）
  ///
  /// 角色守卫（与 [`Self::resume_primary_tasks`] 成对的单一守卫结构）：仅
  /// 配置角色非主时生效——主态调用即 no-op（无集群管理器的单机形态恒主，
  /// 同 no-op），杜绝后续角色变更路径误挂起致 GC 与周期任务停摆
  pub fn suspend_primary_tasks(&self) {
    if self.is_primary() {
      return;
    }
    if let Some(tasks) = self.primary_tasks.read().as_ref() {
      tasks.suspend();
    }
    if let Some(store) = self.try_store() {
      store.stop_gc();
    }
  }

  /// 恢复 Primary 类后台任务（角色位翻转复跑周期任务 + 重拉周期对象收集 +
  /// 重启内置 GC 扫描/紧缩；对标 libs/server/StoreWrapper.cs:StartPrimaryTasks
  /// ——REPLICAOF NO ONE、failover 接管、attach 失败回滚臂调用）
  ///
  /// 角色守卫（单一守卫结构的恢复侧）：仅配置角色为主时生效——副本态恢复
  /// 会让 GC 与周期任务在副本角色下错误运转，no-op 挡下；恢复期
  ///（is_recovering）不参与判定——三处升主路径（NO ONE / failover 接管 /
  /// attach 失败回滚）均在恢复锁内调用本函数（C# StartPrimaryTasks 锁内
  /// 纪律），含恢复期的 [`Self::is_replica`] 在此必误判
  pub fn resume_primary_tasks(&self) {
    if !self.is_primary() {
      return;
    }
    if let Some(tasks) = self.primary_tasks.read().as_ref() {
      if let Some(store) = self.try_store() {
        tasks.resume(&store);
      } else {
        tasks.set_replica(false);
      }
    }
    if let Some(store) = self.try_store() {
      store.start_gc();
    }
  }

  /// 注入向量集合管理器（集群装配期一次调用；对标 C# RespServerSession
  /// 会话持有的 vectorManager——CLUSTER RESERVE 迁移预保留面）
  pub fn set_vector_manager(&self, vector_manager: Arc<VectorManager>) {
    *self.vector_manager.write() = Some(vector_manager);
  }

  /// 向量集合管理器（未注入时 None）
  pub fn try_vector_manager(&self) -> Option<Arc<VectorManager>> {
    self.vector_manager.read().clone()
  }

  /// 注入发布订阅中枢（集群装配期一次调用；对标 C# clusterProvider.storeWrapper.subscribeBroker）
  pub fn set_pubsub(&self, pubsub: Option<Arc<SubscribeBroker>>) {
    *self.pubsub.write() = pubsub;
  }

  /// 发布订阅中枢（未注入或 --disable-pubsub 时 None）
  pub fn subscribe_broker(&self) -> Option<Arc<SubscribeBroker>> {
    self.pubsub.read().clone()
  }

  /// 注入 AOF 门面（AOF 门控点亮时装配期一次调用；对标 C#
  /// storeWrapper.appendOnlyFile 可达面——MLOG_KEY_TIME 序列号读取）。
  /// 物理日志句柄同步注入复制域驱动仓库（C# AofSyncDriverStore 构造期
  /// 反查 appendOnlyFile.Log；Rust 装配期注入，见
  /// AofSyncDriverStore::attach_log——SafeTruncateAof 物理截断面，同时亦是
  /// 主端运行期位点的动态读日志尾源），并向复制管理器注入主端角色谓词
  /// （对标 C# ReplicationOffset getter 在 PRIMARY 角色动态读
  /// appendOnlyFile.Log.TailAddress——主端运行期位点靠该角色谓词 + 同一份
  /// 日志尾推进，INFO / gossip / failover 停写应答的位点单点收口于
  /// ReplicationManager::get_current_replication_offset）
  pub fn set_aof(&self, aof: Option<Arc<GarnetAppendOnlyFile>>) {
    if let Some(rm) = self.replication_manager() {
      rm.aof_sync_driver_store
        .attach_log(aof.as_ref().map(|a| Arc::clone(a.log())));
      rm.aof_sync_driver_store
        .attach_backpressure(aof.as_ref().and_then(|a| a.backpressure().cloned()));
      // 主端角色实时谓词：捕获自身弱引用回查 is_primary（避免 rm -> provider
      // 强引用成环），无 provider 时按主处理，对齐 is_primary 的 unwrap_or(true)
      let weak = self.self_weak.get().cloned();
      let primary_role: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
        weak
          .as_ref()
          .and_then(|w| w.upgrade())
          .is_none_or(|p| p.is_primary())
      });
      rm.set_primary_role_source(Some(primary_role));
    }
    *self.aof.write() = aof;
  }

  /// AOF 门面（AOF 门控未点亮时 None）
  pub fn try_aof(&self) -> Option<Arc<GarnetAppendOnlyFile>> {
    self.aof.read().clone()
  }

  /// 注入运行时配置（对标 C# ClusterProvider.cs 构造期传入的 serverOptions
  /// 可达面；装配期一次调用，全仓唯一
  /// [`RuntimeServerConfig`] 实例的薄克隆——热更槽值由 CONFIG SET 就地落
  /// 槽，消费方每轮实时读取即时生效，杜绝第二张配置表与装配期快照）
  pub fn set_runtime_config(&self, runtime_config: Arc<RuntimeServerConfig>) {
    *self.runtime_config.write() = Some(runtime_config);
  }

  /// 运行时配置可达面（装配期未注入时 None）
  pub fn try_runtime_config(&self) -> Option<Arc<RuntimeServerConfig>> {
    self.runtime_config.read().clone()
  }

  /// 注入集群出站 TLS 客户端配置（装配期自 provider.tls_config() 共享证书
  /// 源派生构造，五类出站消费点的单一配置源；映射口径见
  /// [`ClientTlsConfig::from_shared_source`]）
  #[cfg(feature = "tls")]
  pub fn set_cluster_tls_client(&self, tls: Option<Arc<ClientTlsConfig>>) {
    *self.cluster_tls_client.write() = tls;
  }

  /// 集群出站 TLS 客户端配置（未配置即 None = 明文集群）
  #[cfg(feature = "tls")]
  pub fn try_cluster_tls_client(&self) -> Option<Arc<ClientTlsConfig>> {
    self.cluster_tls_client.read().clone()
  }

  /// 注入本地物理日志句柄（AOF 门控点亮时装配期一次调用；副本发起同步的
  /// begin/tail 位点源与副本接收会话落盘目标共用同一实例）
  pub fn set_wal(&self, wal: Arc<WalLog<SegmentedDevice>>) {
    *self.wal.write() = Some(wal);
  }

  /// 本地物理日志句柄（AOF 门控未点亮时 None）
  pub fn try_wal(&self) -> Option<Arc<WalLog<SegmentedDevice>>> {
    self.wal.read().clone()
  }

  /// 注入副本接收面会话（AOF 门控点亮时装配期一次调用；CLUSTER APPENDLOG
  /// 记录帧经此落盘重放，对标 C# 会话侧 replicaReplaySession 可达面）
  pub fn set_replica_replication_session(
    &self,
    session: Option<Arc<ClusterReplicationSession<SegmentedDevice>>>,
  ) {
    *self.replica_replication.write() = session;
  }

  /// 副本接收面会话（未注入时 None）
  pub fn try_replica_replication_session(
    &self,
  ) -> Option<Arc<ClusterReplicationSession<SegmentedDevice>>> {
    self.replica_replication.read().clone()
  }

  /// 注入主端推流装配面（AOF 门控点亮时装配期一次调用；CLUSTER
  /// INITIATE_REPLICA_SYNC 发起面）
  pub fn set_primary_replication(&self, assets: Option<Arc<PrimaryReplicationAssets>>) {
    *self.primary_replication.write() = assets;
  }

  /// 主端推流装配面（未注入时 None）
  pub fn try_primary_replication(&self) -> Option<Arc<PrimaryReplicationAssets>> {
    self.primary_replication.read().clone()
  }

  /// 注入检查点目录（对标 C# clusterProvider 经 storeWrapper 反查
  /// CheckpointDir；快照发送源与副本接收落盘目标的公共根）
  pub fn set_checkpoint_dir(&self, dir: PathBuf) {
    *self.checkpoint_dir.write() = Some(dir);
  }

  /// 检查点目录（未注入时 None）
  pub fn try_checkpoint_dir(&self) -> Option<PathBuf> {
    self.checkpoint_dir.read().clone()
  }

  /// 采纳宿主引擎置换槽（对标 C# ClusterProvider.storeWrapper 装配注入）：
  /// 此后本层与宿主共读共写同一槽，一次置换两侧同时见新引擎；已播种的装配期
  /// 引擎随采纳迁入宿主槽，故与 [`Self::set_store`] 的先后次序无关
  pub fn set_store_swap_slot(&self, slot: StoreSwapSlot) {
    let mut current = self.store_slot.write();
    if let Some(seed) = current.get() {
      slot.swap(seed);
    }
    *current = slot;
  }

  /// 注入引擎在线置换写面钩子束（装配期一次注入；宿主构造单点为 wnode
  /// `StorageSessionProvider::engine_swap_hook_bundle`——WATCH 版本推进钩子
  /// 与 AOF per-op 事件汇随换机统一重挂，对标 C# 原位恢复的 functionsState
  /// 接线跨恢复全程存活。运行期唯一消费点 [`Self::swap_online_store`]）
  pub fn set_engine_swap_hooks(&self, hooks: EngineSwapHooks) {
    *self.engine_swap_hooks.write() = Some(hooks);
  }

  /// 注入本实例活跃消费者注册表（装配期一次调用，宿主
  /// StorageSessionProvider 的 registry 单点；置换清扫射程的实例级收口，
  /// 票 zcode-r37-lockfix 发现 A 残留——跨实例爆炸半径）
  pub fn set_consumer_registry(&self, registry: Arc<ConsumerRegistry>) {
    *self.consumer_registry.write() = Some(registry);
  }

  /// 本实例消费者注册表（未注入为 None；单测直驱置换面的退化形态，
  /// 清扫臂回退进程级 global 兜底）
  pub(crate) fn try_consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    self.consumer_registry.read().clone()
  }

  /// 引擎置换钩子束（未注入为 None；单测直驱置换面的退化形态）
  pub(crate) fn try_engine_swap_hooks(&self) -> Option<EngineSwapHooks> {
    self.engine_swap_hooks.read().clone()
  }

  /// 注入逻辑数据库管理器（装配期注入，对标 C# StoreWrapper.databaseManager）
  pub fn set_database_manager(&self, dm: Arc<SingleDatabaseManager<SegmentedDevice>>) {
    dm.attach_flush_gate(self.provider_handle());
    *self.database_manager.write() = Some(dm);
  }

  /// 逻辑数据库管理器句柄
  pub fn try_database_manager(&self) -> Option<Arc<SingleDatabaseManager<SegmentedDevice>>> {
    self.database_manager.read().clone()
  }

  /// 上次保存时间戳（毫秒）
  pub fn last_save_ms(&self) -> u64 {
    self
      .try_database_manager()
      .map(|dm| dm.last_save_ms())
      .unwrap_or(0)
  }
}
