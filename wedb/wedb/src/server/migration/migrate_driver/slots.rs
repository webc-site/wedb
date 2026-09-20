//! 基于槽位的全槽扫描与驱动循环 (SLOTS 路径)
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs:BeginAsyncMigrationTaskAsync
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionSlots.cs:MigrateSlotsDriverInlineAsync
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionSlots.cs:ScanStoreTaskAsync

use std::{collections::BTreeSet, sync::Arc};

use wbase::map::HashSet;
use wdev::{Device, SegmentedDevice};
use wkv::WedbStore;
use wnode::{
  range_index::range_index_manager_migration::RangeIndexManagerMigration,
  storage::session::storage_session::StorageSession,
};

use super::keys::{
  MigrateTransmitEnv, begin_migration_phase, connect_migrate_client, dispose_migration,
  end_migration_phase, is_timeout_err, max_chunk_of, transmit_keys, try_recover_from_failure,
  wait_dur,
};
use crate::{
  error::{Error, Result},
  server::{
    cluster_provider::ClusterProvider,
    migration::{
      migrate_session::{MigrateSession, MigrateTaskSpec},
      migrate_session_range_index::migrate_range_index_keys_async,
      migrate_session_vector_set::migrate_vector_set_keys_async,
      sketch::Sketch,
      sketch_status::SketchStatus,
      transfer_option::TransferOption,
    },
    sync_transport::MAX_MIGRATION_BATCH_COUNT,
  },
};

/// 注册 SLOTS 迁移任务（命令臂同步段，对标 MigrateCommand.cs 解析尾的
/// TryAddMigrationTask）；槽位冲突/超限显式失败
pub fn try_add_slots_migration_task(
  cluster_provider: &ClusterProvider,
  spec: MigrateTaskSpec,
  slots: &HashSet<i32>,
) -> Result<Arc<MigrateSession>> {
  let Some(migration_mgr) = cluster_provider.migration_manager() else {
    return Err(Error::ClusterNotInitialized);
  };
  migration_mgr
    .try_add_migration_task(spec, slots.clone(), Sketch::new())
    .ok_or_else(|| Error::InvalidArgument("创建迁移任务失败 (槽位冲突或超限)".into()))
}

/// 执行已注册的 SLOTS/SLOTSRANGE 迁移任务（对标
/// MigrationDriver.cs:BeginAsyncMigrationTaskAsync；命令臂以 spawn
/// detached 后台任务调用，finally 移除任务对标 TryStartMigrationTaskAsync
/// SLOTS 分支）
///
/// 前置编排 IMPORTING → MIGRATING → 纪元转换；驱动循环（对标
/// MigrateSessionSlots.cs:MigrateSlotsDriverInlineAsync / ScanStoreTaskAsync，
/// C# ParallelMigrateTaskCount 并行扫描投影为串行单任务，并行迁移任务
/// 明确不做）：逐槽游标推进——批量取键 → sketch 收录并切 TRANSMITTING
/// 分批停等传输 → 切 DELETING 删除已确认键 → 清 sketch，直到槽内无可迁
/// 键；copy 态删除环整体门控（源端键保留并登记不可迁移清单承接游标推进，
/// 对标 DeleteKeys 首行 `_copyOption` 早退）；收尾编排
/// 哨兵 → NODE → relinquish。
///
/// 删除游标只推进到「批次 ACK 成功」的键（对标 C# MigrateOperation.
/// DeleteKeys 只删 sketch 收录键；绝不使用 delete_slot_keys 全槽清除——
/// 槽内未传输键（并发改写落空键、未知信封损坏键）误删即丢数据）。
pub async fn run_slots_migration_task(
  store: Arc<WedbStore<SegmentedDevice>>,
  spec: MigrateTaskSpec,
  session: Arc<MigrateSession>,
) -> Result<usize> {
  let res = execute_slots_migration(&store, &spec, &session).await;
  if let Some(migration_mgr) = session.cluster_provider.migration_manager() {
    migration_mgr.try_remove_migration_task_session(Arc::clone(&session));
  }
  res
}

/// 复活暂停守卫（RAII 保证迁移搬迁窗口复活暂停与安全恢复）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs:BeginAsyncMigrationTaskAsync
pub struct RevivPauseGuard<'a, D: Device> {
  store: &'a WedbStore<D>,
}

impl<'a, D: Device> RevivPauseGuard<'a, D> {
  /// 构造守卫并暂停存储复活分配
  pub fn new(store: &'a WedbStore<D>) -> Self {
    log::info!("迁移槽位搬迁窗口：暂停存储复活分配");
    store.reviv_pool.pause();
    Self { store }
  }
}

impl<D: Device> Drop for RevivPauseGuard<'_, D> {
  fn drop(&mut self) {
    self.store.reviv_pool.resume();
    log::info!("迁移槽位搬迁窗口：恢复存储复活分配");
  }
}

/// SLOTS 驱动执行体（任务注册后调用；失败统一 recover，见各编排函数）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionSlots.cs:MigrateSlotsDriverInlineAsync
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionSlots.cs:ScanStoreTaskAsync
async fn execute_slots_migration(
  store: &Arc<WedbStore<SegmentedDevice>>,
  spec: &MigrateTaskSpec,
  session: &Arc<MigrateSession>,
) -> Result<usize> {
  let _reviv_guard = RevivPauseGuard::new(store);
  let client = connect_migrate_client(
    spec,
    session,
    #[cfg(feature = "tls")]
    session.cluster_provider.try_cluster_tls_client().as_ref(),
  );
  let ranges = session.get_ranges();
  let dur = wait_dur(spec.timeout);
  let max_chunk = max_chunk_of(session);

  begin_migration_phase(
    &client,
    session,
    &ranges,
    dur,
    spec.source_node_id,
    true,
    TransferOption::Slots,
  )
  .await?;

  let wkv_session = store.new_session()?;
  let batch = wkv_session.enter_batch();
  // 迁移源端同会话既有读（扫描/传输）又有写（DELETING 收口删键），走全功能
  // 会话，禁 new_readonly（写入路径绝不可用）
  let storage = StorageSession::new(batch);
  let vm = session.cluster_provider.try_vector_manager();

  let mut migrated_count = 0usize;
  // 槽序稳定推进（HashSet 迭代无序，排序后逐槽处理）
  let mut sorted_slots: Vec<i32> = session.get_slots().iter().copied().collect();
  sorted_slots.sort_unstable();
  for slot in sorted_slots {
    // 不可迁移键登记（暂不支持类型 / 并发改写键 / 已死键）：驱动游标每轮
    // 自槽头重扫，须剔除以收敛；键保留源端（孤儿键投影声明见模块注释）
    let mut untouchable: HashSet<Vec<u8>> = HashSet::default();
    loop {
      // INITIALIZING 扫描收键（对标 ScanStoreTaskAsync 的
      // SetStatus(INITIALIZING) → Scan）
      //
      // libs/cluster/Server/Migration/MigrateOperation.cs:Scan
      //
      // C# 该件为 BasicGarnetApi.IterateStore(ref storeScan, ref current,
      // endAddress, endAddress, includeTombstones: true) 的游标式全库迭代
      // （逐记录回调侧收键，见 MigrateScanFunctions StoreScan）；rust 无游标
      // 迭代面，采槽位索引枚举 get_keys_in_slot 批取键 + 本驱动循环的
      // untouchable 剔除实现同一「收键」职责，游标推进语义为「每轮自槽头
      // 重扫至无可迁键」（差异登记见上条不可迁移键注释）
      let keys = storage
        .get_keys_in_slot(slot as u16, MAX_MIGRATION_BATCH_COUNT)
        .await?;
      let mut work = keys;
      work.retain(|k| !untouchable.contains(k));
      if work.is_empty() {
        break;
      }

      let mut ri_keys =
        RangeIndexManagerMigration::get_range_index_keys_for_migration(&wkv_session, &work).await?;
      let normal_work: Vec<&[u8]> = work
        .iter()
        .filter(|k| !ri_keys.contains(*k))
        .map(|k| k.as_slice())
        .collect();

      let mut transferred = Vec::new();
      // 带内传输面单遍分类登记的页存储集合键（发现面本轮封窗跳过、但读活
      // 值判型为 wbftree 树的键）：并入 ri_keys 同走带外分块流通道
      let mut deferred_tree_keys: Vec<&[u8]> = Vec::new();

      if !normal_work.is_empty() {
        // TRANSMITTING：分批停等传输（对标 TRANSMITTING → TransmitSlotsAsync）
        session.sketch.set_status(SketchStatus::Transmitting);
        for k in &normal_work {
          session.sketch.hash_and_store(k);
        }
        // 纪元静止等待：等 TRANSMITTING 键门对全会话生效、批内在途操作排空后
        // 再传输（对标 MigrateSessionSlots.cs:236 WaitForConfigPropagationAsync；
        // 返值忽略：C# 无限自旋，rust 有界放行）
        let _ = session
          .cluster_provider
          .bump_and_wait_for_epoch_transition_async()
          .await;
        let env = MigrateTransmitEnv {
          client: &client,
          session,
          dur,
          spec,
          max_chunk,
        };

        let (t, moved, tree) =
          match transmit_keys(&storage, vm.as_deref(), &env, &normal_work).await {
            Ok(res) => res,
            Err(err) => {
              let poisoned = is_timeout_err(&err);
              try_recover_from_failure(
                &client,
                session,
                &ranges,
                dur,
                &err.to_string(),
                poisoned,
                TransferOption::Slots,
              )
              .await;
              return Err(err);
            }
          };
        transferred = t;
        migrated_count += moved;
        deferred_tree_keys = tree;

        // DELETING：删除已确认传输键 → 清 sketch 进入下一轮（对标
        // DELETING → DeleteKeys → sketch.Clear()）；copy 门置于删除环最外层
        // （对标首行 `if (session._copyOption) return;`，与
        // keys.rs/range_index/vector_set 三臂同形）：copy 态源端
        // 键保留不删、Deleting 门不落，sketch 照常复位推进下一轮
        //
        // libs/cluster/Server/Migration/MigrateOperation.cs:DeleteKeys
        //
        // C# 该件按 transferOption 分两臂（SLOTS 遍历 sketch.argSliceVector
        // 跳过 hasNs、KEYS 遍历 sketch.Keys 只删已标记 transmitted 者）；
        // rust 两臂各在其驱动内收口，本臂为 SLOTS 支（transferred 清单即
        // 「已确认 ACK」判据，与 C# Item2 标记同义），KEYS 支见 keys.rs
        // execute_keys_migration 的 DELETING 段（不另挂锚，避免同键重复登记）
        if !spec.copy_option {
          session.sketch.set_status(SketchStatus::Deleting);
          // 纪元静止等待：等 DELETING 键门对全会话生效后再落删除（对标
          // MigrateSessionSlots.cs:247；删除失败返值忽略口径同 keys.rs 收口）
          let _ = session
            .cluster_provider
            .bump_and_wait_for_epoch_transition_async()
            .await;
          for key in &transferred {
            let _ = storage.delete_string(key).await;
          }
        }
        session.sketch.clear();
      }

      // 发现面封窗跳过（MigrationBusy 瞬时窗）但带内读值判为树的键并入带外
      // 清单：快照阶段在独占锁下重读权威存根，仍封窗即显式判败 recover，
      // 绝不拉黑静默留键（拉黑即槽交换完成后源端孤儿键）
      for key in &deferred_tree_keys {
        ri_keys.insert(key.to_vec());
      }

      if !ri_keys.is_empty() {
        match migrate_range_index_keys_async(
          &client,
          session,
          &wkv_session,
          &storage,
          spec,
          &ri_keys,
          dur,
        )
        .await
        {
          Ok(true) => {
            migrated_count += ri_keys.len();
          }
          Ok(false) => {
            let err = Error::InvalidArgument("带外树流迁移失败 (slot)".into());
            try_recover_from_failure(
              &client,
              session,
              &ranges,
              dur,
              "带外树流迁移失败",
              false,
              TransferOption::Slots,
            )
            .await;
            return Err(err);
          }
          Err(err) => {
            let poisoned = is_timeout_err(&err);
            try_recover_from_failure(
              &client,
              session,
              &ranges,
              dur,
              &err.to_string(),
              poisoned,
              TransferOption::Slots,
            )
            .await;
            return Err(err);
          }
        }
      }

      // copy 态：源端键不消失，已确认传输键（含 RI 带外成功键）登记入不可
      // 迁移清单，承接 C# 扫描游标推进（cursor = current，
      // MigrateSessionSlots.cs:254——游标后键不再进入重扫）；本 rust 投影以
      // 槽头重扫替代地址游标，缺此登记则重扫反复重传已确认键、槽内循环永不
      // 收敛。键权保留源端，正是 COPY 语义
      if spec.copy_option {
        untouchable.extend(transferred.iter().map(|k| k.to_vec()));
        untouchable.extend(ri_keys.iter().map(|k| k.to_vec()));
      }

      // 快路径：正常键全量传输成功，跳过集合构建与残留键判定
      if transferred.len() == normal_work.len() {
        continue;
      }

      // 已不存在/被并发改写的键登记不可迁移（显式留痕，键权保留源端）；
      // wbftree 页存储集合键（RI 与升阶分层集合）与向量集键已走各自带外
      // 通道迁移，不在此列（否则每轮误拉黑、槽位永不收敛）
      let transferred_set: HashSet<&[u8]> = transferred.into_iter().collect();
      let stuck_keys: Vec<&[u8]> = work
        .iter()
        .map(|k| k.as_slice())
        .filter(|k| !ri_keys.contains(*k) && !transferred_set.contains(k))
        .collect();
      if !stuck_keys.is_empty() {
        let mut summary = String::new();
        for (i, k) in stuck_keys.iter().enumerate() {
          if i > 0 {
            summary.push_str(", ");
          }
          summary.push_str(&String::from_utf8_lossy(k));
        }
        log::error!(
          "槽 {slot} 有 {} 个键已不存在或被并发改写，保留源端: {summary}",
          stuck_keys.len()
        );
        for key in stuck_keys {
          untouchable.insert(key.to_vec());
        }
      }
    }
  }

  // 向量集收尾段（对标 C# CreateAndRunMigrateTasksAsync 扫描完成后的
  // Vector Set 统一迁移：GetNamespacesForHashSlots 发现 → RESERVE →
  // 帧传输 → 源端删除）。wkv 槽扫描对向量集键不可见（索引记录驻留
  // 向量管理器登记表），故按命名空间全集独立发现
  let slot_set: BTreeSet<i32> = session.get_slots().iter().copied().collect();
  let vector_sets = vm
    .as_deref()
    .map(|vm| vm.get_vector_set_keys_for_slots(&slot_set))
    .unwrap_or_default();
  if !vector_sets.is_empty() {
    match migrate_vector_set_keys_async(
      &client,
      session,
      vm.as_deref(),
      spec,
      &vector_sets,
      max_chunk,
      dur,
    )
    .await
    {
      Ok(true) => migrated_count += vector_sets.len(),
      Ok(false) => {
        let err = Error::InvalidArgument("SLOTS 向量集迁移失败".into());
        try_recover_from_failure(
          &client,
          session,
          &ranges,
          dur,
          "向量集迁移失败",
          false,
          TransferOption::Slots,
        )
        .await;
        return Err(err);
      }
      Err(err) => {
        let poisoned = is_timeout_err(&err);
        try_recover_from_failure(
          &client,
          session,
          &ranges,
          dur,
          &err.to_string(),
          poisoned,
          TransferOption::Slots,
        )
        .await;
        return Err(err);
      }
    }
  }

  end_migration_phase(&client, session, &ranges, dur, spec, TransferOption::Slots).await?;
  // 成功终态收口：取消令牌触发 + 在途客户端会话断开（对标 Dispose）
  dispose_migration(&client, session);
  Ok(migrated_count)
}
