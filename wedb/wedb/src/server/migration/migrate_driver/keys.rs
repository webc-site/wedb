//! 基于键清单的迁移流程与驱动循环 (KEYS 路径)
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs:TryStartMigrationTaskAsync
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionKeys.cs:MigrateKeysAsync

use std::{sync::Arc, time::Duration};

use compio::runtime::{Runtime, spawn};
use wbase::map::HashSet;
use wconn::record::{BatchItem, encode_migration_payload, send_chunked_record};
use wdev::{Device, SegmentedDevice};
use wkv::WedbStore;
use wnode::{
  resp::vector::vector_manager::VectorManager, storage::session::storage_session::StorageSession,
};

use super::{
  abandon_migration_session,
  keys_execute::execute_keys_migration,
  live_value::{LiveValue, probe_unsupported_keys, read_live_value},
  phase::{rollback_abandoned_remote, send_payload_and_wait},
};
use crate::{
  client::GarnetClient,
  error::{Error, Result},
  server::{
    cluster_provider::ClusterProvider,
    migration::{
      migrate_session::{MigrateSession, MigrateTaskSpec},
      migration_manager::MigrationManager,
      sketch::Sketch,
    },
    sync_transport::MAX_MIGRATION_BATCH_COUNT,
  },
};

/// 停等时长三态映射：spec.timeout (ms) 由 MIGRATE 命令第 5 参流入
/// （C# `TimeSpan.FromMilliseconds(_timeout)` 同源，`WaitAsync(_timeout)` 消费）：
/// timeout > 0 → `Some(限时)`；timeout == 0 → `Some(ZERO)`（基线快失败档，
/// 首次 [`super::phase::wait_remote`] deadline 检查即判超时）；timeout == -1 → `None`
/// （免超时档，对标 C# `Timeout.InfiniteTimeSpan`，停等仅随取消令牌收敛）。
/// 其余负值（< -1，C# 基线在首个 WaitAsync 抛 ArgumentOutOfRangeException
/// 致迁移运行期失败）已在命令解析期显式 ERR 拒收（见 cluster_session/
/// migrate.rs，偏差登记 doc/zh/deviations.md 86），本函数定义域内不可达
#[inline]
pub fn wait_dur(timeout_ms: i32) -> Option<Duration> {
  (timeout_ms >= 0).then(|| Duration::from_millis(timeout_ms as u64))
}

/// 迁移传输环境参数聚合
pub struct MigrateTransmitEnv<'a> {
  pub client: &'a GarnetClient,
  pub session: &'a MigrateSession,
  pub dur: Option<Duration>,
  pub spec: &'a MigrateTaskSpec,
  pub max_chunk: usize,
}

/// 键清单批量传输：逐键读活值（string / 对象信封）、按配置发送缓冲内容
/// 上限分批装帧停等 ACK，超大单记录切块传输；返回（已确认 ACK 的键, 确认
/// 条数, 清单内待带外传输的 wbftree 页存储集合键）。中途失败已 ACK 批次不
/// 回传（失败路径不删除任何键，键权保留源端，对标 C# 传输失败不
/// DeleteKeys）。
/// wbftree 页存储集合键（`LiveValue::TieredTree`，RangeIndex 与升阶分层
/// 集合共用）只登记于第三返回项、由调用方并入带外分块流通道传输；向量集
/// 键（`LiveValue::VectorSet`）由调用方收集后走向量集通道，此处跳过装帧；
/// `LiveValue::Unsupported`（仅未知信封内层类型 = 数据损坏可达）显式上抛
/// 拒绝，绝不静默跳键
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateOperation.cs:TransmitKeysAsync
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateOperation.cs:TransmitSlotsAsync
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionKeys.cs:MigrateKeysFromStoreAsync
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionKeys.cs:ShouldSkipKey
///
/// MigrateOperation 两件合一登记：C# `TransmitKeysAsync`（KEYS 链遍历
/// sketch.Keys、FOUND 者标记待删）与 `TransmitSlotsAsync`（SLOTS 链遍历
/// sketch.argSliceVector，含 hasNs 向量集分支）在本件合一——两件的逐记录
/// 装帧停等体同形，本件按调用方给定的键清单传输，「FOUND 即标记待删」折叠
/// 为返回值 transferred 清单（由调用方 DELETING 臂消费，见 keys_execute.rs
/// execute_keys_migration 与 slots.rs execute_slots_migration）；
/// TransmitSlotsAsync 的 hasNs 分支不在此，剥出为向量集带外通道
/// migrate_vector_set_keys_async（migrate_session_vector_set.rs）。
pub async fn transmit_keys<'a, D: Device, K: AsRef<[u8]>>(
  storage: &StorageSession<'_, D>,
  vm: Option<&VectorManager>,
  env: &MigrateTransmitEnv<'_>,
  keys: &'a [K],
) -> Result<(Vec<&'a [u8]>, usize, Vec<&'a [u8]>)> {
  let mut transferred = Vec::with_capacity(keys.len());
  let mut migrated_count = 0usize;
  let mut tree_keys: Vec<&'a [u8]> = Vec::new();
  let mut cur_batch: Vec<BatchItem<'a>> =
    Vec::with_capacity(MAX_MIGRATION_BATCH_COUNT.min(keys.len()));
  let mut cur_batch_bytes = 0usize;

  // 装批冲刷：整批编码一次停等
  macro_rules! flush_batch {
    () => {
      if !cur_batch.is_empty() {
        let payload = encode_migration_payload(&cur_batch);
        send_payload_and_wait(env.client, env.session, env.dur, env.spec, &payload).await?;
        migrated_count += cur_batch.len();
        transferred.extend(cur_batch.iter().map(|it| it.key));
        cur_batch.clear();
      }
    };
  }

  for key in keys {
    let key_ref = key.as_ref();
    match read_live_value(storage, vm, key_ref).await? {
      LiveValue::TieredTree => {
        // wbftree 页存储集合键（RangeIndex 与升阶分层集合）：登记待带外
        // 分块流传输，此处跳过装帧
        tree_keys.push(key_ref);
      }
      LiveValue::VectorSet => {
        // 向量集键由调用方收集后走向量集带外通道，此处跳过装帧
      }
      LiveValue::Unsupported(kind_label) => {
        // 未知信封内层类型（仅数据损坏可达）：载荷语义不可判定，显式上抛
        // 拒绝，绝不静默跳键（诚实发送端经入口预检不触达此臂）
        return Err(Error::InvalidArgument(format!(
          "键 {} 载荷类型不可迁移（信封类型 {kind_label}）",
          String::from_utf8_lossy(key_ref)
        )));
      }
      LiveValue::Gone => {
        // 竞态兜底：键被并发删除/过期/改写 → 不发帧、不计入删除清单，
        // 键权保留在源端（绝不波及未成功传输的键）
      }
      LiveValue::Migratable(val, expire_ticks) => {
        let item = BatchItem {
          key: key_ref,
          val,
          expire_ticks,
        };
        let frame_len = item.frame_len();
        if frame_len > env.max_chunk {
          // 超限单记录切块发送（对标 WriteOrSendChunkedRecordAsync）：
          // 先冲既有批（批字节计数一并复位），再流式逐块发送避免全帧物化
          flush_batch!();
          cur_batch_bytes = 0;
          send_chunked_record(&item, env.max_chunk, async |payload| {
            send_payload_and_wait(env.client, env.session, env.dur, env.spec, payload).await
          })
          .await?;
          migrated_count += 1;
          transferred.push(key_ref);
          continue;
        }
        if !cur_batch.is_empty()
          && (cur_batch.len() >= MAX_MIGRATION_BATCH_COUNT
            || cur_batch_bytes + frame_len > env.max_chunk)
        {
          flush_batch!();
          cur_batch_bytes = 0;
        }
        cur_batch_bytes += frame_len;
        cur_batch.push(item);
      }
    }
  }
  flush_batch!();
  Ok((transferred, migrated_count, tree_keys))
}

/// 执行 CLUSTER MIGRATE 发送驱动 (KEYS 路径)
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs:TryStartMigrationTaskAsync
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionKeys.cs:MigrateKeysAsync
///
/// 严格停等架构（全部远端 await 经 [`super::phase::wait_remote`] 限时并联控会话取消
/// 令牌，目标挂起不至永挂、dispose 即时收敛）：
/// 1. 暂不支持键预检（零副作用拒绝）→ 提取槽位 + sketch 收录 → 注册迁移任务
/// 2. 前置编排 [`super::phase::begin_migration_phase`]：远端 IMPORTING（自动下发的显式
///    文档差异）+ 本端 MIGRATING 容忍
/// 3. sketch 切 TRANSMITTING，逐批装帧发送 CLUSTER MIGRATE 停等 ACK；
///    wbftree 页存储集合键（RangeIndex 与升阶分层集合）逐键快照分块、
///    向量集键帧传输，均走带外通道且不触碰外层 sketch（门控外置）
/// 4. 收尾编排 [`super::phase::end_migration_phase`]：仅哨兵（KEYS 链不动槽位，绝不移交
///    槽属主，同槽未迁移键持续可源端访问，收口归运维 CLUSTER SETSLOT）
/// 5. 非 copy 删除「已确认 ACK」的键（DELETING 门控，RI/向量集键并入同一
///    收口清单），sketch 归位
/// 6. finally 移除迁移任务（对标 KEYS 分支 finally TryRemoveMigrationTask）
///
/// 迁移资格（禁止静默丢键）：string 记录与 Hash/Set/List/ZSet 对象信封
/// 记录可迁移，wbftree 页存储集合键（RangeIndex 与升阶分层集合）与向量集
/// 键走带外通道迁移（C# MigrateKeysFromStoreAsync 同款，rust 页存储层为
/// 本仓扩展）。请求键清单含未知信封等暂不支持键时，入口预检
/// [`probe_unsupported_keys`] 整体拒绝并列明键清单——此时尚未注册
/// 任务、未触达远端，源端零状态变更、零键删除。仅在全部批次 ACK 成功后，
/// 源端才对「已确认传输成功」的键执行删除，未传输键一律保留源端（属主
/// 未动，键持续可访问）。
pub async fn run_keys_migration_driver(
  cluster_provider: Arc<ClusterProvider>,
  store: Arc<WedbStore<SegmentedDevice>>,
  spec: MigrateTaskSpec,
  slots: &HashSet<i32>,
  keys: &[Vec<u8>],
) -> Result<usize> {
  let Some(migration_mgr) = cluster_provider.migration_manager() else {
    return Err(Error::ClusterNotInitialized);
  };

  // 0. 暂不支持键预检（显式拒绝，禁止静默丢键）：探测到即整体失败，
  //    错误中列明清单；失败点在注册任务/连接远端之前，零副作用可安全重试
  {
    let probe_session = store.new_session()?;
    let probe_batch = probe_session.enter_batch();
    let probe_storage = StorageSession::new_readonly(probe_batch);
    let unsupported = probe_unsupported_keys(
      &probe_storage,
      cluster_provider.try_vector_manager().as_deref(),
      keys,
    )
    .await?;
    if !unsupported.is_empty() {
      let mut labels: Vec<&str> = unsupported.iter().map(|u| u.kind_label).collect();
      labels.sort_unstable();
      labels.dedup();
      return Err(Error::InvalidArgument(format!(
        "MIGRATE 拒绝：{} 个键暂不支持迁移（{}），已整体取消迁移：{}",
        unsupported.len(),
        labels.join("/"),
        unsupported
          .iter()
          .map(|u| String::from_utf8_lossy(u.key))
          .collect::<Vec<_>>()
          .join(", ")
      )));
    }
  }

  // 1. 收录 sketch（库级定槽 doc/zh/db.md 4.1：槽位集由命令解析期显式
  //    传入（会话槽位单元素），不再逐键 HashSlot 收集；sketch 仅作键级
  //    门控 can_access_key，对标 MigrateCommand.cs 解析期 sketch.HashAndStore）
  let sketch = Sketch::new();
  for k in keys {
    sketch.hash_and_store(k);
  }

  // 2. 注册任务
  let session = migration_mgr
    .try_add_migration_task(spec.clone(), slots.clone(), sketch)
    .ok_or_else(|| Error::InvalidArgument("创建迁移任务失败 (槽位冲突或超限)".into()))?;

  // 3. 执行 + finally 移除任务（对标 KEYS 分支 finally TryRemoveMigrationTask）；
  //    取消安全：全程持 [`KeysDriverGuard`]，future 被泵侧丢弃时守卫取消臂
  //    补齐同步面清理与远端回滚；正常臂 disarm 后交还既有显式收口，行为零变化
  let mut guard = KeysDriverGuard::new(Arc::clone(&migration_mgr), Arc::clone(&session));
  let res = execute_keys_migration(&store, &spec, &session, keys).await;
  guard.disarm();
  migration_mgr.try_remove_migration_task_session(Arc::clone(&session));
  res
}

/// KEYS 驱动取消安全守卫（对标 replication_sync_manager 的 SyncBatchGuard/
/// PinDriversGuard 先例：Drop 内同步清理 + disarm 幂等共存）
///
/// 生产挂点：[`run_keys_migration_driver`] 注册任务后创建、正常臂（Ok/Err
/// 同达）`disarm` 后交还既有显式收口（失败 recover 已由驱动链内
/// try_recover_from_failure 收敛、finally TryRemoveMigrationTask 保持可见，
/// C# KEYS 分支 finally 同位）。
///
/// 取消臂背景：KEYS 同步形态挂慢路径（cluster_session/migrate.rs
/// pending_slow），发起客户端断连/CLIENT KILL/停机广播即被网络泵
/// RaceEnd::Disposed 丢弃 future（future drop 即取消），裸顺序收尾不可达
/// ——sketch 滞留 Transmitting/Deleting 源端键级写门关闭、任务表槽位泄漏
/// （同槽再迁移恒 IOERR）、远端自动下发的 IMPORTING 无 recover 回滚（目标端
/// 该槽持续 CLUSTERDOWN）、session.status 恒 Pending。C# 基线：BlockingWait
/// 阻塞网络线程，断连不取消迁移，finally TryRemoveMigrationTask + recover
/// 必达——本守卫即该必达面的 rust 投影：
/// - 同步面（[`abandon_migration_session`]，全为同步方法，Drop 内安全）；
/// - 异步面（[`rollback_abandoned_remote`]）：远端回滚是逐帧停等的网络操作，
///   不可入 Drop——spawn 独立任务补跑。spawn 上下文论证：future 仅在泵
///   poll（RaceEnd::Disposed 臂）或会话收口丢弃，均在 compio runtime worker
///   线程上、`Runtime::try_current` 必有；兜底缺席臂（运行时停机 clear 间
///   隙、测试裸 drop）仅留痕不 panic——同步面已收口，远端回滚属尽力而为旁路。
pub struct KeysDriverGuard {
  mgr: Arc<MigrationManager>,
  session: Arc<MigrateSession>,
  disarmed: bool,
}

impl KeysDriverGuard {
  /// 持守卫（生产挂点为 [`run_keys_migration_driver`]，本构造口供测试直构）
  pub fn new(mgr: Arc<MigrationManager>, session: Arc<MigrateSession>) -> Self {
    Self {
      mgr,
      session,
      disarmed: false,
    }
  }

  /// 正常收尾解除守卫（清理交还既有显式单点，防与失败 recover 双重收口）
  pub fn disarm(&mut self) {
    self.disarmed = true;
  }
}

impl Drop for KeysDriverGuard {
  fn drop(&mut self) {
    if self.disarmed {
      return;
    }
    // 取消臂：同步面单点收口（幂等：recover/正常收口已先达时零操作或假）
    abandon_migration_session(&self.mgr, &self.session);
    // 异步面：远端 IMPORTING 回滚 spawn 独立任务（上下文论证见类型文档）
    if Runtime::try_current().is_some() {
      spawn(rollback_abandoned_remote(Arc::clone(&self.session))).detach();
    } else {
      log::error!("KEYS 迁移驱动遗弃：无运行时上下文，远端 IMPORTING 回滚未补跑（同步面已收口）");
    }
  }
}

/// 发送缓冲内容上限读取：委派 cluster_provider 单点真源（迁移/无盘同源，
/// migration_manager 未装配时回退同源派生缺省）
pub(crate) fn max_chunk_of(session: &MigrateSession) -> usize {
  session.cluster_provider.max_send_buffer_content_size()
}
