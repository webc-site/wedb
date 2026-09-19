//! 主端 diskless 快照共享迭代器与逐会话锁步扇出 (SnapshotIteratorManager)
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSnapshotIterator.cs
//!
//! 对标 C# `SnapshotIteratorManager`：单遍存储迭代产记录，逐记录经
//! FanOutRecordSpan / FanOutChunk 同步锁步写入本批全部会话（C# 逐会话迭代
//! 缓冲 + 缓冲满全批冲刷等齐重试；rust 停等批模型收敛为单共享攒批——锁步
//! 不变量「同一字节流写全部会话、任一满即全体冲刷」使各会话缓冲内容恒同，
//! 攒批与编码每批一次、发送逐会话并发停等，序列化一次广播 N 副本）。
//! C# `StoreSnapshotIterator`（同文件 :264 的 IStreamingSnapshotIterator-
//! Functions 消费者壳）在本模块收敛为 [`run_snapshot_fanout`] 的扫描主体；
//! OnStart 初始化逐会话迭代缓冲对应共享攒批的构造复位，OnStop 统一收尾
//! 对应末批冲刷 + 完成哨兵，OnException 失败收敛对应逐扇出点判败摘除。
//!
//! rust 快照源为活存储 live scan（get_keys_in_slot 库级单遍 + read_live_value
//! 活值分类），非 C# Tsavorite StreamingSnapshot 检查点迭代（位点锚定口径
//! 见 replication_sync_manager 批量共享锚，该缺口归已登记的 diskless 位点
//! 锚定条）。RangeIndex 树与向量集带外流为本仓相对 C# 的扩展面：RI 分块帧
//! 副本无关可整帧广播；向量集上下文按副本预留、索引重映射逐副本各异，故
//! 源端导出单遍、逐副本各自编码发送（不共享载荷）。

use std::{collections::BTreeSet, sync::Arc};

use compio::time::timeout;
use futures_util::future::join_all;
use gxhash::HashMap as GxHashMap;
use parking_lot::Mutex;
use wbase::{hash_slot::slot_of, hex::hex_str_u128};
use wbftree::DEFAULT_MIGRATION_CHUNK_SIZE;
use wconn::record::{
  BatchItem, MigrateVectorElement, encode_migration_payload, encode_vector_set_element_payload,
  encode_vector_set_index_payload, send_chunked_record,
};
use wnode::{
  StorageSession,
  resp::vector::{
    vector_manager::{INDEX_SIZE_BYTES, VectorManager},
    vector_manager_index::Index,
    vector_manager_locking::split_registry_key,
  },
};

use crate::{
  client::GarnetClient,
  error::Error,
  server::{
    cluster_provider::ClusterProvider,
    migration::migrate_driver::{LiveValue, read_live_value},
    replication::{
      diskless_replication::{replica_sync_session::DisklessSyncSession, sync_status::SyncStatus},
      replica_wire::REPLICA_SYNC_TIMEOUT,
    },
    sync_transport::{MAX_MIGRATION_BATCH_COUNT, transmit_range_index_stream},
  },
};

/// 单遍快照迭代 + 逐会话锁步扇出管理器
struct SnapshotIteratorManager {
  /// 扇出帧源节点 id（协议面渲染 hex，同原单副本路径）
  source_node_hex: String,
  /// 本批参与快照扇出的活跃全量会话（判败即摘除，对标 C# IsActive 收敛）
  sessions: Mutex<Vec<Arc<DisklessSyncSession>>>,
}

impl SnapshotIteratorManager {
  fn new(local_node_id: u128, sessions: Vec<Arc<DisklessSyncSession>>) -> Self {
    Self {
      source_node_hex: hex_str_u128(local_node_id),
      sessions: Mutex::new(sessions),
    }
  }

  /// 活跃会话快照（终态会话摘除后返回，对标 C# IsActive 逐位检查）
  fn active_sessions(&self) -> Vec<Arc<DisklessSyncSession>> {
    let mut guard = self.sessions.lock();
    guard.retain(|s| !s.is_terminal());
    guard.clone()
  }

  /// 同一载荷广播全部活跃会话（FanOutRecordSpan / FanOutChunk 的 rust
  /// 停等形态：逐会话并发发送、全批等齐返回；任一会话发送失败/超时/被拒
  /// 即判败摘除，其余会话继续——对标 C# SetFlushTask 失败收敛 +
  /// WaitForFlushAsync 内 `if (Sessions[i].Failed) Sessions[i] = null`）
  async fn fan_out_send(&self, payload: &[u8]) -> Result<(), String> {
    let targets = self.active_sessions();
    if targets.is_empty() {
      return Err(ALL_SESSIONS_FAILED.to_string());
    }
    let hex = &self.source_node_hex;
    join_all(targets.iter().map(|s| async {
      let client = s.client().ok_or("snapshot session client not connected");
      let outcome: Result<(), String> = match client {
        Err(msg) => Err(msg.to_string()),
        Ok(client) => {
          match timeout(
            REPLICA_SYNC_TIMEOUT,
            client.execute_cluster_sync(hex, payload),
          )
          .await
          {
            Err(_) => Err("CLUSTER SYNC timeout".to_string()),
            Ok(Err(e)) => Err(format!("CLUSTER SYNC failed: {e}")),
            Ok(Ok(false)) => Err("CLUSTER SYNC rejected by replica".to_string()),
            Ok(Ok(true)) => Ok(()),
          }
        }
      };
      if let Err(msg) = outcome {
        s.set_status(SyncStatus::Failed, Some(msg));
      }
    }))
    .await;
    if self.active_sessions().is_empty() {
      return Err(ALL_SESSIONS_FAILED.to_string());
    }
    Ok(())
  }

  /// 单遍库级活扫描快照扇出主体（对标 C# WriteRecord 消费循环 +
  /// MainStreamingSnapshotDriverAsync 内 TakeStreamingCheckpointAsync 触发）
  async fn run(&self, provider: &Arc<ClusterProvider>) -> Result<(), String> {
    let store = provider
      .try_store()
      .ok_or_else(|| "store not initialized".to_string())?;
    let session = store
      .new_session()
      .map_err(|e| format!("session error: {e}"))?;
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    let (namespace, db) = (session.namespace(), session.active_db());
    // 单块/装批内容上限：与迁移共用同一真源（对标 C#
    // NetworkBufferSettings.MaxSendBufferContentSize = sendBufferSize -
    // SendBufferOverheadReserve 公式；rust 只建一份发送缓冲规格，故走
    // cluster_provider 单点读取，migration_manager 未装配时回退同源缺省）
    let max_chunk = provider.max_send_buffer_content_size();

    // 全库活键快照（库级定槽 doc/zh/db.md 4.1：会话库全部键恒共会话槽位，
    // 单次列举覆盖整库；多库扇出的库枚举编排登记
    // task/ing/migrate-whole-db-nsdb-granularity.md）
    let keys = storage
      .get_keys_in_slot(slot_of(namespace, db), usize::MAX)
      .await
      .map_err(|e| format!("snapshot scan failed: {e}"))?;
    let mut cur_batch: Vec<BatchItem<'_>> = Vec::new();
    let mut cur_batch_bytes = 0usize;

    // 装批冲刷：整批一次编码广播全部活跃会话（序列化一次、发送逐会话；
    // 批字节计数复位由调用点按需跟进）
    macro_rules! flush_batch {
      () => {
        if !cur_batch.is_empty() {
          let payload = encode_migration_payload(&cur_batch);
          self.fan_out_send(&payload).await?;
          cur_batch.clear();
        }
      };
    }

    for key in &keys {
      // 活值分类读取：string/合规信封域自带惰性过期裁决，真实过期时间戳
      // 一并提取装帧（对标 C# 快照迭代整记录搬运含 RecordDataHeader.expiration）
      match read_live_value(&storage, None, key).await {
        Ok(LiveValue::Migratable(val, expire_unix_ms)) => {
          let item = BatchItem {
            key,
            val,
            expire_unix_ms,
          };
          let frame_len = item.frame_len();
          if frame_len > max_chunk {
            // 超大记录切块广播（分块帧副本无关，逐块锁步等齐，对标
            // FanOutChunk 的 moreChunksFollow 续块语义）
            flush_batch!();
            cur_batch_bytes = 0;
            send_chunked_record(&item, max_chunk, async |payload| {
              self
                .fan_out_send(payload)
                .await
                .map_err(Error::InvalidArgument)
            })
            .await
            .map_err(|e| e.to_string())?;
            continue;
          }
          if !cur_batch.is_empty()
            && (cur_batch.len() >= MAX_MIGRATION_BATCH_COUNT
              || cur_batch_bytes + frame_len > max_chunk)
          {
            flush_batch!();
            cur_batch_bytes = 0;
          }
          cur_batch_bytes += frame_len;
          cur_batch.push(item);
        }
        // RangeIndex 元记录键：树快照走 RangeIndexStream 带外分块帧，副本端
        // RI 接收会话流式落盘原子发布（对标 C# 快照迭代 RangeIndexRecordType
        // 分流 + RangeIndexMigrationReceiveSession 承接）；分块帧副本无关，
        // 整帧广播锁步发送，失败即中止全量同步，半快照副本绝不放行转入增量
        Ok(LiveValue::RangeIndex) => {
          flush_batch!();
          cur_batch_bytes = 0;
          let ok = transmit_range_index_stream(
            &session,
            key,
            DEFAULT_MIGRATION_CHUNK_SIZE,
            async |payload| {
              self
                .fan_out_send(payload)
                .await
                .map_err(Error::InvalidArgument)
            },
          )
          .await
          .map_err(|e| e.to_string())?;
          if !ok {
            return Err(format!(
              "failed to snapshot range index for key {}",
              String::from_utf8_lossy(key)
            ));
          }
        }
        // 不可迁移键与读取失败同态：跳过该键（与槽扫描失败容错口径一致）
        _ => continue,
      }
    }
    flush_batch!();

    // 向量集段：索引记录驻留向量管理器登记表、存储槽扫描不可见，按库槽
    // 发现面独立装帧（对标 C# MigrateOperation.EncounteredVectorSet 传输面
    // 分发）。上下文按副本预留、索引重映射逐副本各异——源端导出单遍，
    // 逐副本编码发送并发等齐；全体判败即中止
    if let Some(vm) = provider.try_vector_manager().as_deref() {
      let db_slots: BTreeSet<i32> = [i32::from(slot_of(namespace, db))].into_iter().collect();
      let vector_sets = vm.get_vector_set_keys_for_slots(&db_slots);
      if !vector_sets.is_empty() {
        // 帧口径恒为剥域用户键（枚举产物为登记表复合键，此处单点剥离）
        let snapshots: Vec<VectorSetSnapshot> = vector_sets
          .iter()
          .map(|(rk, src_index)| VectorSetSnapshot {
            key: split_registry_key(rk).map_or(rk.clone(), |(_, user_key)| user_key.to_vec()),
            src_index: *src_index,
            elements: vm
              .export_migration_elements(src_index)
              .into_iter()
              .map(|e| (e.element, e.values, e.attributes))
              .collect(),
          })
          .collect();
        let targets = self.active_sessions();
        join_all(targets.iter().map(|s| async {
          let outcome =
            transmit_vector_sets_to_session(s, vm, &snapshots, &self.source_node_hex, max_chunk)
              .await;
          if let Err(msg) = outcome {
            s.set_status(SyncStatus::Failed, Some(msg));
          }
        }))
        .await;
        if self.active_sessions().is_empty() {
          return Err(ALL_SESSIONS_FAILED.to_string());
        }
      }
    }

    // 发送完成哨兵空载荷 (recordCount = 0)（对标 C# OnStop 统一收尾冲刷）
    let sentinel_payload = encode_migration_payload(&[]);
    self.fan_out_send(&sentinel_payload).await?;
    Ok(())
  }
}

/// 全员判败统一文案
const ALL_SESSIONS_FAILED: &str = "all diskless sync snapshot sessions failed";

/// 向量集源端单遍导出快照（键 + 源索引字节 + 导出元素三段）
struct VectorSetSnapshot {
  key: Vec<u8>,
  src_index: [u8; INDEX_SIZE_BYTES],
  elements: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
}

/// 逐副本向量集装帧发送：目标端上下文预留 → 源→目标重映射 → 索引帧 +
/// 元素批帧停等（口径同 sync_transport::transmit_vector_set_frames，差异
/// 在源端导出已由调用方单遍完成，本函数只做逐副本编码与发送）
async fn transmit_vector_sets_to_session(
  session: &Arc<DisklessSyncSession>,
  vm: &VectorManager,
  snapshots: &[VectorSetSnapshot],
  source_node_hex: &str,
  max_chunk: usize,
) -> Result<(), String> {
  let client = session
    .client()
    .ok_or("snapshot session client not connected")?;
  // 目标端上下文预留 + 源→目标重映射表（一键一上下文；预留应答序与键序对齐）
  let reserved: Vec<u64> = match timeout(
    REPLICA_SYNC_TIMEOUT,
    client.reserve_vector_set_contexts_async(snapshots.len()),
  )
  .await
  {
    Err(_) => return Err("CLUSTER RESERVE timeout".to_string()),
    Ok(Err(e)) => return Err(format!("reserve destination vector sets failed: {e}")),
    Ok(Ok(r)) => r,
  };
  if reserved.len() != snapshots.len() {
    return Err(format!(
      "vector set context reserve mismatch (need {}, got {})",
      snapshots.len(),
      reserved.len()
    ));
  }
  let mut namespace_map: GxHashMap<u64, u64> = GxHashMap::default();
  for (snap, dst_ctx) in snapshots.iter().zip(&reserved) {
    if let Some(index) = Index::from_bytes(&snap.src_index) {
      namespace_map.insert(index.context, *dst_ctx);
    }
  }

  // 逐键：索引帧（停等）→ 元素分批帧（停等）
  for snap in snapshots {
    let Some(dst_index) = vm.remap_index_for_migration(&snap.src_index, &namespace_map) else {
      return Err(format!(
        "向量集键 {} 上下文无预留映射，快照中止",
        String::from_utf8_lossy(&snap.key)
      ));
    };
    let index_payload = encode_vector_set_index_payload(&snap.key, &dst_index);
    send_sync_frame(&client, source_node_hex, &index_payload).await?;

    let mut items: Vec<MigrateVectorElement> = Vec::new();
    let mut batch_bytes = 0usize;
    for (element, values, attributes) in &snap.elements {
      let item = MigrateVectorElement {
        key: snap.key.clone(),
        element: element.clone(),
        values: values.clone(),
        attributes: attributes.clone(),
      };
      if !items.is_empty()
        && (items.len() >= MAX_MIGRATION_BATCH_COUNT || batch_bytes + item.frame_len() > max_chunk)
      {
        let payload = encode_vector_set_element_payload(&items);
        send_sync_frame(&client, source_node_hex, &payload).await?;
        items.clear();
        batch_bytes = 0;
      }
      batch_bytes += item.frame_len();
      items.push(item);
    }
    if !items.is_empty() {
      let payload = encode_vector_set_element_payload(&items);
      send_sync_frame(&client, source_node_hex, &payload).await?;
    }
  }
  Ok(())
}

/// 单副本 CLUSTER SYNC 帧停等发送（vector 逐副本链专用；扇出广播链走
/// fan_out_send，停等口径一致）
async fn send_sync_frame(
  client: &GarnetClient,
  source_node_hex: &str,
  payload: &[u8],
) -> Result<(), String> {
  let ok = match timeout(
    REPLICA_SYNC_TIMEOUT,
    client.execute_cluster_sync(source_node_hex, payload),
  )
  .await
  {
    Err(_) => return Err("CLUSTER SYNC timeout".to_string()),
    Ok(Err(e)) => return Err(format!("CLUSTER SYNC failed: {e}")),
    Ok(Ok(ok)) => ok,
  };
  if !ok {
    return Err("CLUSTER SYNC rejected by replica".to_string());
  }
  Ok(())
}

/// 主端 diskless 快照扇出入口（批内单遍扫描；会话需已带快照客户端连接）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs:TakeStreamingCheckpointAsync
pub(crate) async fn run_snapshot_fanout(
  provider: &Arc<ClusterProvider>,
  local_node_id: u128,
  sessions: Vec<Arc<DisklessSyncSession>>,
) -> Result<(), String> {
  SnapshotIteratorManager::new(local_node_id, sessions)
    .run(provider)
    .await
}
