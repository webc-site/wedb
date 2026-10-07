//! KEYS 驱动执行体（任务注册后的键迁移主状态机：扫描分类 / 批量传输 /
//! 带外通道 / DELETING 收口 / 槽位分相推进）
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionKeys.cs:DeleteKeysAsync
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs:TryStartMigrationTaskAsync

use std::{sync::Arc, time::Duration};

use wbftree::DEFAULT_MIGRATION_CHUNK_SIZE;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  resp::vector::vector_manager_locking::registry_user_key,
  storage::session::storage_session::StorageSession,
};

use super::{
  keys::{MigrateTransmitEnv, max_chunk_of, transmit_keys, wait_dur},
  live_value::collect_vector_set_keys,
  phase::{
    begin_migration_phase, connect_migrate_client, dispose_migration, end_migration_phase,
    is_timeout_err, send_payload_and_wait, try_recover_from_failure,
  },
  recover_and_fail,
};
use crate::{
  client::GarnetClient,
  error::{Error, Result},
  server::{
    migration::{
      ERR_MIGRATE_EPOCH_WAIT,
      migrate_session::{MigrateSession, MigrateTaskSpec},
      migrate_session_range_index::transmit_range_index_async,
      migrate_state::MigrateState,
      sketch_status::SketchStatus,
      transfer_option::TransferOption,
    },
    sync_transport::{reserve_vector_set_namespace_map, transmit_vector_set_frames},
  },
};

/// 向量集/树流带外传输判败壳单点（两段带外传输共用）：`Ok(true)` 判定归调用方
/// 计数；`Ok(false)`（业务判败：对端拒绝/管理器缺席）与 `Err`（传输/停等错误）
/// 双臂统一走 [`recover_and_fail!`] 收口——先 try_recover_from_failure 恢复
/// 再置败返回，调用方 `?` 上抛同款错误
async fn ensure_ok_or_recover(
  client: &GarnetClient,
  session: &MigrateSession,
  ranges: &[(i32, i32)],
  dur: Option<Duration>,
  res: Result<bool>,
  fail_msg: &'static str,
) -> Result<()> {
  match res {
    Ok(true) => Ok(()),
    Ok(false) => {
      let err = Error::InvalidArgument(fail_msg.into());
      recover_and_fail!(
        client,
        session,
        ranges,
        dur,
        TransferOption::Keys,
        false,
        fail_msg,
        err
      );
    }
    Err(err) => {
      recover_and_fail!(
        client,
        session,
        ranges,
        dur,
        TransferOption::Keys,
        is_timeout_err(&err),
        &err.to_string(),
        err
      );
    }
  }
}

/// KEYS 驱动执行体（任务注册后调用；失败统一 recover，见各编排函数）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionKeys.cs:DeleteKeysAsync
/// （此处为 DELETING 分相编排：仅删「已确认 ACK」键，MIGRATED 释放等待后归位）
pub(super) async fn execute_keys_migration(
  store: &Arc<WedbStore<SegmentedDevice>>,
  spec: &MigrateTaskSpec,
  session: &Arc<MigrateSession>,
  keys: &[Vec<u8>],
) -> Result<usize> {
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
    false,
    TransferOption::Keys,
  )
  .await?;

  // TRANSMITTING：载荷在途，源端对已收录键的写等待（对标
  // MigrateKeysFromStoreAsync 的 sketch.SetStatus(TRANSMITTING)）
  session.sketch.set_status(SketchStatus::Transmitting);
  // 纪元静止等待：等 TRANSMITTING 键门对全会话生效、批内在途操作排空后再
  // 传输，堵「写已过键门 → 删除落地后写才持久化 → 源端复活键」窗口（对标
  // MigrateSessionKeys.cs:35 WaitForConfigPropagationAsync；C# KEYS 链走
  // clusterSession.UnsafeBumpAndWaitForEpochTransitionAsync，rust 统一
  // clusterProvider 原语。返值承判：rust 有界化后静止未达成即判败 recover，
  // §95 迁移族收口口径）
  if !session
    .cluster_provider
    .bump_and_wait_for_epoch_transition_async()
    .await
  {
    recover_and_fail!(
      &client,
      session,
      &ranges,
      dur,
      TransferOption::Keys,
      ERR_MIGRATE_EPOCH_WAIT
    );
  }

  let wkv_session = store.new_session()?;
  let batch = wkv_session.enter_batch();
  // 迁移源端同会话既有读（传输）又有写（DELETING 收口删键），走全功能会话，
  // 禁 new_readonly（写入路径绝不可用）
  let storage = StorageSession::new(batch);
  let vm = session.cluster_provider.try_vector_manager();

  // wbftree 带外流键的清单即传输面单遍分类产物（tree_keys，见
  // transmit_keys）：读活值判型一处定分，不再前置独立发现（避免双判据）
  let vector_sets = collect_vector_set_keys(&storage, vm.as_deref(), keys).await?;

  let env = MigrateTransmitEnv {
    client: &client,
    session,
    dur,
    spec,
    max_chunk,
  };

  let (mut transferred, mut migrated_count, tree_keys) =
    match transmit_keys(&storage, vm.as_deref(), &env, keys).await {
      Ok(res) => res,
      Err(err) => {
        recover_and_fail!(
          &client,
          session,
          &ranges,
          dur,
          TransferOption::Keys,
          is_timeout_err(&err),
          &err.to_string(),
          err
        );
      }
    };

  // 向量集带外传输（门控外置：不 clear、不重建外层 sketch、不改状态——键已在
  // 外层 sketch 受 TRANSMITTING 门保护；对标 MigrateSessionKeys.cs:74-141 的
  // 传输段，源端删除并入末段统一 DELETING 收口）
  if !vector_sets.is_empty() {
    let vs_res = async {
      let Some(vm) = vm.as_deref() else {
        log::error!("向量集迁移需向量集合管理器装配，本次迁移拒绝");
        return Ok(false);
      };
      // 预留前置（SLOTS 链同款单点；预留失败与传输失败同走
      // ensure_ok_or_recover 判败，时序对位见 reserve 函数注释）
      let Some(namespace_map) = reserve_vector_set_namespace_map(&client, &vector_sets).await
      else {
        return Ok(false);
      };
      transmit_vector_set_frames(
        store,
        vm,
        &vector_sets,
        max_chunk,
        || session.is_cancelled(),
        &namespace_map,
        async |payload| send_payload_and_wait(&client, session, dur, spec, payload).await,
      )
      .await
    }
    .await;
    ensure_ok_or_recover(&client, session, &ranges, dur, vs_res, "向量集迁移失败").await?;
    migrated_count += vector_sets.len();
  }

  // wbftree 页存储集合键（RangeIndex 与升阶分层集合共用）带外传输（门控外
  // 置）：逐键快照分块停等（对标 MigrateSessionKeys.cs:151-158 逐键
  // TransmitRangeIndexAsync，rust 泛化到全部页存储树；键已在外层 sketch、
  // 受 TRANSMITTING 门保护，注释 :143-146 明言），不触碰 sketch；删除并入
  // 末段统一 DELETING 收口（对标标记后交 DeleteKeysAsync）
  if !tree_keys.is_empty() {
    let tree_res = async {
      for key in &tree_keys {
        if !transmit_range_index_async(
          &client,
          session,
          &wkv_session,
          spec,
          key,
          DEFAULT_MIGRATION_CHUNK_SIZE,
          dur,
        )
        .await?
        {
          return Ok(false);
        }
      }
      Ok(true)
    }
    .await;
    ensure_ok_or_recover(&client, session, &ranges, dur, tree_res, "带外树流迁移失败").await?;
    migrated_count += tree_keys.len();
  }

  end_migration_phase(&client, session, &ranges, dur, spec, TransferOption::Keys).await?;

  // 非 copy：删除「已确认传输成功」的键——未传输键（中途失败键/竞态改写
  // 键）一律保留在源端，杜绝静默丢键；DELETING 门控读写全等待
  // （对标 DeleteKeysAsync 的 DELETING → 删除 → MIGRATED 单点收口：wbftree
  // 带外流键并入 transferred 删除清单、向量集键与字符串键同走
  // storage.delete_string 统一删除通道，外层 sketch 全程驻留，键门对全部
  // 已迁键无空洞。C# MigrateOperation.DeleteKeys 的 KEYS 分支即本臂，其
  // 映射锚持于 slots.rs 的 DELETING 臂；本臂另有向量集删件对位，映射锚持
  // 于 migrate_session_vector_set.rs）
  transferred.extend(tree_keys.iter().copied());
  if !spec.copy_option {
    session.sketch.set_status(SketchStatus::Deleting);
    // 纪元静止等待：等 DELETING 键门对全会话生效后再落删除（对标
    // MigrateSessionKeys.cs:187；返值承判：未达成即判败，杜绝在途写在删除
    // 之后才持久化成源端复活键，§95 迁移族收口口径）
    if !session
      .cluster_provider
      .bump_and_wait_for_epoch_transition_async()
      .await
    {
      recover_and_fail!(
        &client,
        session,
        &ranges,
        dur,
        TransferOption::Keys,
        ERR_MIGRATE_EPOCH_WAIT
      );
    }
    // DELETING 收口只删不释（C# 对位 DeleteKeysAsync → MigrateOperation.DeleteKeys
    // 只删；本链零登记 claim，未持有即释会误删并发 RENAME/换入窗的同源键 claim，
    // 持有者纪律见 wkv migration.rs rename_range_index「仅限 try_claim 成功后的
    // 持有者调用」释放配对表锚）；删除失败留痕落源，持 claim 键 DEL 闸回
    // MigrationBusy 属正当封堵，禁静默排空他人在搬运的树
    for key in &transferred {
      if let Err(e) = storage.delete_string(key).await {
        log::error!(
          "ExecuteKeysMigration: failed to delete key {} after migration: {e}",
          String::from_utf8_lossy(key)
        );
      }
    }
    // 向量集源端删除与字符串键同通道：登记复合键剥域取用户键（与枚举产物
    // 同域）走 storage.delete_string 单点——经 wkv 用户键删除缺席观测钩子摘
    // 除登记表项，缺席键墓碑镜像入 AOF 复制链供源端副本同形收敛（对标 C#
    // DeleteVectorSet 经 BasicGarnetApi.DELETE 入日志，MigrateOperation.cs:
    // 269-277；副本重放经 GarnetRecordTriggers.cs:OnDispose Deleted 臂 →
    // RequestDeletion 收口）。禁直调登记表摘除旁路：旁路不入复制流，failover
    // 即复活幽灵集、主从永久发散。删除失败留痕落源（禁静默丢键），经
    // transferred 键门与 DELETING 纪律照旧
    for (rk, _) in &vector_sets {
      if let Err(e) = storage.delete_string(registry_user_key(rk)).await {
        log::error!(
          "ExecuteKeysMigration: failed to delete vector set key {} after migration: {e}",
          String::from_utf8_lossy(rk)
        );
      }
    }
  }
  // MIGRATED 释放等待操作后归位（对标 MigrateKeysAsync finally 的
  // INITIALIZING；两态对 can_access_key 均放行，连续设置无观察窗口）
  session.sketch.set_status(SketchStatus::Migrated);
  // 纪元静止等待：等 MIGRATED 释放门对全会话生效后再归位（对标
  // MigrateSessionKeys.cs:194）
  // 返值放行系本位点无数据收敛不变量（INITIALIZING 与 MIGRATED 两态对键门
  // 均放行，对位 C# finally 归位，与 §95 无盘快照键门同口径），非缺口
  let _ = session
    .cluster_provider
    .bump_and_wait_for_epoch_transition_async()
    .await;
  session.sketch.set_status(SketchStatus::Initializing);

  // 成功终态收口：取消令牌触发 + 在途客户端会话断开（对标 Dispose）
  dispose_migration(&client, session);
  *session.status.write() = MigrateState::Success;
  Ok(migrated_count)
}
