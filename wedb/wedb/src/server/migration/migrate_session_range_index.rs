//! wbftree 页存储集合键跨节点集群迁移发送驱动
//! （1:1 对标 libs/cluster/Server/Migration/MigrateSession.RangeIndex.cs，
//! rust 分层扩展面：RangeIndex 与升阶分层集合共用同一带外分块流通道，
//! 判别类型与 TTL 随流元携载）

use std::time::Duration;

use wbase::map::HashSet;
use wbftree::DEFAULT_MIGRATION_CHUNK_SIZE;
use wdev::Device;
use wkv::StoreSession;
use wnode::{range_index::MigrateActivity, storage::session::storage_session::StorageSession};

use crate::{
  client::GarnetClient,
  error::{Error, Result},
  server::{
    migration::{
      migrate_driver::send_payload_and_wait,
      migrate_session::{MigrateSession, MigrateTaskSpec},
      sketch_status::SketchStatus,
    },
    sync_transport::transmit_range_index_stream,
  },
};

/// libs/cluster/Server/Migration/MigrateSession.RangeIndex.cs:TransmitRangeIndexAsync
///
/// 源端快照并流式发送单个 wbftree 页存储集合键（RangeIndex 与升阶分层
/// 集合共用）的树快照分块（迁移停等门面：会话取消联动 + 停等限时帧发送）
pub async fn transmit_range_index_async<D: Device>(
  client: &GarnetClient,
  session: &MigrateSession,
  store_session: &StoreSession<D>,
  spec: &MigrateTaskSpec,
  key: &[u8],
  chunk_size: usize,
  timeout: Duration,
) -> Result<bool> {
  if session.is_cancelled() {
    return Err(Error::OperationCancelled);
  }
  transmit_range_index_stream(store_session, key, chunk_size, async |payload| {
    send_payload_and_wait(client, session, timeout, spec, payload).await
  })
  .await
}

/// libs/cluster/Server/Migration/MigrateSession.RangeIndex.cs:MigrateRangeIndexKeysAsync
///
/// 批量迁移 wbftree 页存储集合键（RangeIndex 与升阶分层集合共用；SLOTS 链
/// 专属带 sketch 重置形态，对标 C# 仅
/// MigrateSessionSlots.cs:123 调用）：Sketch 门控（INITIALIZING →
/// TRANSMITTING → DELETING），相位切换后纪元静止等待使门对全会话生效，
/// 确保写操作在快照传输期受阻，读写在删除期受阻；KEYS 链走门控外置路径
/// （逐键 [`transmit_range_index_async`] + 外层统一 DELETING 收口，见
/// migrate_driver/keys.rs）
pub async fn migrate_range_index_keys_async<D: Device>(
  client: &GarnetClient,
  session: &MigrateSession,
  store_session: &StoreSession<D>,
  storage: &StorageSession<'_, D>,
  spec: &MigrateTaskSpec,
  range_index_keys: &HashSet<Vec<u8>>,
  timeout: Duration,
) -> Result<bool> {
  if range_index_keys.is_empty() {
    return Ok(true);
  }

  let mut migrate_activity = MigrateActivity::start_activity(range_index_keys.len());
  log::warn!(
    "MigrateRangeIndexKeysAsync: migrating {} RangeIndex keys",
    range_index_keys.len()
  );

  session.sketch.clear();
  session.sketch.set_status(SketchStatus::Initializing);
  for key in range_index_keys {
    session.sketch.hash_and_store(key);
  }

  // TRANSMITTING：快照与传输期间阻止写操作
  session.sketch.set_status(SketchStatus::Transmitting);
  // 纪元静止等待：等 TRANSMITTING 键门对全会话生效后再传输（对标
  // MigrateSession.RangeIndex.cs:114 WaitForConfigPropagationAsync；
  // 返值忽略：C# 无限自旋，rust 有界放行）
  let _ = session
    .cluster_provider
    .bump_and_wait_for_epoch_transition_async()
    .await;
  migrate_activity.on_transmitting();

  for key in range_index_keys {
    if session.is_cancelled() {
      migrate_activity.on_error("OperationCancelled");
      session.sketch.clear();
      migrate_activity.end_and_log_activity();
      return Err(Error::OperationCancelled);
    }

    match transmit_range_index_async(
      client,
      session,
      store_session,
      spec,
      key,
      DEFAULT_MIGRATION_CHUNK_SIZE,
      timeout,
    )
    .await
    {
      Ok(true) => {}
      Ok(false) => {
        migrate_activity.on_error(&format!(
          "failed to transmit key {}",
          String::from_utf8_lossy(key)
        ));
        session.sketch.clear();
        migrate_activity.end_and_log_activity();
        return Ok(false);
      }
      Err(e) => {
        migrate_activity.on_error(&e.to_string());
        session.sketch.clear();
        migrate_activity.end_and_log_activity();
        return Err(e);
      }
    }
  }

  // DELETING：删除期间阻止读写操作
  if !spec.copy_option {
    session.sketch.set_status(SketchStatus::Deleting);
    // 纪元静止等待：等 DELETING 键门对全会话生效后再落删除（对标
    // MigrateSession.RangeIndex.cs:135；返值忽略口径同上）
    let _ = session
      .cluster_provider
      .bump_and_wait_for_epoch_transition_async()
      .await;
    migrate_activity.on_deleting();

    for key in range_index_keys {
      if let Err(e) = storage.delete_string(key).await {
        log::error!(
          "MigrateRangeIndexKeysAsync: failed to delete RangeIndex key {} after migration: {e}",
          String::from_utf8_lossy(key)
        );
      }
    }
  }

  session.sketch.clear();
  migrate_activity.end_and_log_activity();
  Ok(true)
}
