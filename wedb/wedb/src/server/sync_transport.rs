//! wbftree 带外流（RangeIndex 与升阶分层集合）与向量集共用的流式传输件
//! (SyncTransport)
//!
//! 树流与向量集的带外流式发送核心：源端快照/装配 → 帧编码 →
//! 逐块发送（帧发送经闭包注入，两链各自挂停等或直写出口）。二者是共享
//! 传输件而非迁移私件，故独立于 migration 目录、由迁移与复制各引一处。
//! 编排面（sketch 门控、会话取消、停等限时）留在各自的迁移门面文件。
//! 注：树流与向量集迁移编排是本仓相对 C# 的扩展面，C# 无同名函数，
//! 本模块不挂 cs 映射锚点。

use std::{result, slice::from_ref, sync::Arc};

use wbase::map::HashMap as GxHashMap;
use wconn::record::{
  MigrateVectorElement, encode_range_index_stream_payload_into, encode_vector_set_element_payload,
  encode_vector_set_index_payload,
};
use wdev::Device;
use wkv::{StoreSession, WedbStore};
use wnode::{
  range_index::{RangeIndexManagerMigration, TransmitActivity},
  resp::vector::{
    vector_manager::{INDEX_SIZE_BYTES, VectorManager},
    vector_manager_index::Index,
    vector_manager_locking::{registry_user_key, split_registry_key},
    vector_store_callbacks::OwnedActiveVectorSession,
  },
};

use crate::{
  client::GarnetClient,
  error::{Error, Result},
};

/// 停等单批最大记录条数 (64 条；rust 停等批模型特有条数上限，C# 由流式
/// client 迭代缓冲自然限批无对应物)。迁移驱动与 diskless 快照共用
pub const MAX_MIGRATION_BATCH_COUNT: usize = 64;

/// 源端快照单个 wbftree 树键（RangeIndex 或升阶分层集合）并分块流式发送
/// （帧发送经 `send` 注入：迁移停等链与 diskless 快照链共用；快照/读块失败
/// `Ok(false)`+活动日志，发送失败 `Err(E)` 上抛交调用方判 poison）。流元
/// （判别类型、成员 TTL 水位、键级 TTL）由快照内核从权威 MetaValue 与 TTL
/// 域单点派生，随每块帧携载
pub async fn transmit_range_index_stream<D: Device, E>(
  store_session: &StoreSession<D>,
  key: &[u8],
  chunk_size: usize,
  mut send: impl AsyncFnMut(&[u8]) -> result::Result<(), E>,
) -> result::Result<bool, E> {
  let mut transmit_activity = TransmitActivity::start_activity();
  let (mut reader, stream_meta) =
    match RangeIndexManagerMigration::snapshot_range_index_and_create_reader(store_session, key)
      .await
    {
      Ok(res) => res,
      Err(e) => {
        transmit_activity.on_error(&e.to_string());
        transmit_activity.end_and_log_activity(key);
        log::error!(
          "TransmitRangeIndexAsync: error during snapshot for key {}: {e}",
          String::from_utf8_lossy(key)
        );
        return Ok(false);
      }
    };

  transmit_activity.on_snapshot_completed(reader.total_file_bytes() as i64);

  let mut buffer = vec![0u8; chunk_size];
  let mut payload = Vec::with_capacity(4 + 1 + 4 + chunk_size);
  while !reader.is_complete() {
    let payload_len = match reader.read_next_chunk(&mut buffer) {
      Ok(len) => len,
      Err(e) => {
        transmit_activity.on_error(&e.to_string());
        transmit_activity.end_and_log_activity(key);
        log::error!(
          "TransmitRangeIndexAsync: reader error for key {}: {e}",
          String::from_utf8_lossy(key)
        );
        return Ok(false);
      }
    };

    if payload_len == 0 {
      transmit_activity.on_error("Zero-length payload from reader");
      transmit_activity.end_and_log_activity(key);
      log::error!(
        "TransmitRangeIndexAsync: reader returned zero-length payload with a {chunk_size}-byte buffer for key {}",
        String::from_utf8_lossy(key)
      );
      return Ok(false);
    }

    encode_range_index_stream_payload_into(
      &buffer[..payload_len],
      stream_meta.obj_type.as_u8(),
      stream_meta.next_expiry,
      stream_meta.expire_unix_ms,
      &mut payload,
    );
    send(&payload).await?;

    transmit_activity.on_chunk_sent(payload_len);
  }

  transmit_activity.end_and_log_activity(key);
  Ok(true)
}

/// 导出的向量集元素三元组（元素键, 向量值, 属性）
pub type ExportedVectorElement = (Vec<u8>, Vec<u8>, Vec<u8>);

/// 元素帧单批发送（批满/超限冲刷，对标迭代缓冲批量冲刷形态；帧发送经
/// `send` 注入）
async fn send_elements(
  key: &[u8],
  elements: Vec<ExportedVectorElement>,
  max_chunk: usize,
  mut send: impl AsyncFnMut(&[u8]) -> Result<()>,
) -> Result<()> {
  let mut items: Vec<MigrateVectorElement> = Vec::new();
  let mut batch_bytes = 0usize;

  macro_rules! flush {
    () => {
      if !items.is_empty() {
        let payload = encode_vector_set_element_payload(&items);
        send(&payload).await?;
        items.clear();
      }
    };
  }

  for (element, values, attributes) in elements {
    let item = MigrateVectorElement {
      key: key.to_vec(),
      element,
      values,
      attributes,
    };
    if item.frame_len() > max_chunk {
      // 超限单元素独立成批（帧结构固定四段，不切块；载荷上限内天然可行）。
      // 先冲既有批并复位批字节计数，杜绝陈旧计数提前触发下一轮冲刷
      flush!();
      batch_bytes = 0;
      let payload = encode_vector_set_element_payload(from_ref(&item));
      send(&payload).await?;
      continue;
    }
    if !items.is_empty()
      && (items.len() >= MAX_MIGRATION_BATCH_COUNT || batch_bytes + item.frame_len() > max_chunk)
    {
      flush!();
      batch_bytes = 0;
    }
    batch_bytes += item.frame_len();
    items.push(item);
  }
  flush!();
  Ok(())
}

/// 向量集源端导出单臂（迁移停等链与无盘快照链共用的唯一绑定落点）
///
/// 两条发送链的导出都跑在迁移驱动 / 快照后台线程，不经命令面
/// `StoreGarnetApi::exec` 的绑定段，而元素导出（`try_get_raw_embedding` 走
/// 存储回调）须有当前执行域绑定的会话——缺绑即 wnode
/// `vector_store_callbacks::report_missing_session` 的失败口径，向量静默导空。
/// 本臂按既有后台自持形态自备一份专用会话（`store.new_session` +
/// [`OwnedActiveVectorSession`]，对标 C# 迁移臂恒自建 StorageSession：
/// VectorManager.Migration.cs:328 `GetNamespacesForKeys` 的
/// `using var storageSession = new StorageSession(...)`），不新增第二处绑定
/// 实现，也不借用他链会话。
///
/// 域口径：元素记录键前缀随**绑定会话的域**落位（见 wnode
/// `vector_store_callbacks` 模块头「物理落位口径」），故会话直设为登记条目
/// 复合键自带的物理域（[`split_registry_key`] 单点解域）——跨域向量集各归
/// 各域导出，绝不以驱动会话默认域盲读他域记录。取会话失败上抛，严禁降级为
/// 空导出。
pub async fn export_vector_set_elements<D: Device>(
  store: &Arc<WedbStore<D>>,
  vm: &VectorManager,
  rk: &[u8],
  src_index: &[u8; INDEX_SIZE_BYTES],
) -> Result<Vec<ExportedVectorElement>> {
  let session = store.new_session()?;
  let (domain, _) = split_registry_key(rk);
  // 直设臂显式携逻辑域（版本轨=逻辑域种子契约）：导出读臂零 bump，换算仅
  // 满足逻辑槽一致性口径，死域回孤域替身属安全侧
  let (lns, ldb) = store.vdb.version_domain_of(domain.vns, domain.vdb);
  session.set_virtual_context(domain.vns, domain.vdb, lns, ldb);
  // 自持即绑定，离开本同步段（函数返回）先还原线程槽再销毁会话
  let _vector_domain = OwnedActiveVectorSession::new(session);
  Ok(
    vm.export_migration_elements(src_index)
      .await
      .into_iter()
      .map(|e| (e.element, e.values, e.attributes))
      .collect(),
  )
}

/// 向量集帧传输核心：预留 → 源→目标重映射 → 逐键索引帧 + 元素批帧（帧发送
/// 经 `send` 注入：迁移停等链与 diskless 快照链共用）。返回 false = 远端
/// 拒绝/装配缺失（判败走 recover）；Err 为停等超时/取消
///
/// `store` 供源端导出臂自备专用会话：本核跑在迁移驱动后台线程，不经命令面
/// `StoreGarnetApi::exec` 的绑定段，而元素导出经存储回调读线程槽绑定的会话
pub async fn transmit_vector_set_frames<D: Device>(
  store: &Arc<WedbStore<D>>,
  client: &GarnetClient,
  vm: &VectorManager,
  vector_set_keys: &[(Vec<u8>, [u8; INDEX_SIZE_BYTES])],
  max_chunk: usize,
  is_cancelled: impl Fn() -> bool,
  mut send: impl AsyncFnMut(&[u8]) -> Result<()>,
) -> Result<bool> {
  // 1. 目标端上下文预留 + 源→目标重映射表（一键一上下文；预留应答序与
  //    键序对齐）
  let reserved = match client
    .reserve_vector_set_contexts_async(vector_set_keys.len())
    .await
  {
    Ok(r) => r,
    Err(e) => {
      log::error!("ReserveDestinationVectorSetsAsync: 预留目标端上下文失败: {e}");
      return Ok(false);
    }
  };
  if reserved.len() != vector_set_keys.len() {
    log::error!(
      "ReserveDestinationVectorSetsAsync: 预留数不符 (need {}, got {})",
      vector_set_keys.len(),
      reserved.len()
    );
    return Ok(false);
  }
  let mut namespace_map: GxHashMap<u64, u64> = GxHashMap::default();
  for ((_, src_index), dst_ctx) in vector_set_keys.iter().zip(&reserved) {
    if let Some(index) = Index::from_bytes(src_index) {
      namespace_map.insert(index.context, *dst_ctx);
    }
  }

  // 2/3. 逐键：索引帧（停等）→ 元素分批帧（停等）。帧口径恒为剥域用户键
  //（C# 无域前缀，目标端按本端会话域重新复合）
  for (rk, src_index) in vector_set_keys {
    if is_cancelled() {
      return Err(Error::OperationCancelled);
    }
    let key = registry_user_key(rk);

    let Some(dst_index) = vm.remap_index_for_migration(src_index, &namespace_map) else {
      log::error!(
        "向量集键 {} 上下文无预留映射，迁移中止",
        String::from_utf8_lossy(key)
      );
      return Ok(false);
    };

    let index_payload = encode_vector_set_index_payload(key, &dst_index);
    if let Err(e) = send(&index_payload).await {
      log::error!(
        "向量集索引帧发送失败 key {}: {e}",
        String::from_utf8_lossy(key)
      );
      return Err(e);
    }

    let elements = match export_vector_set_elements(store, vm, rk, src_index).await {
      Ok(elements) => elements,
      Err(e) => {
        log::error!(
          "向量集导出自备专用会话失败 key {}: {e}，迁移中止",
          String::from_utf8_lossy(key)
        );
        return Ok(false);
      }
    };
    if let Err(e) = send_elements(key, elements, max_chunk, &mut send).await {
      log::error!(
        "向量集元素帧发送失败 key {}: {e}",
        String::from_utf8_lossy(key)
      );
      return Err(e);
    }
  }
  Ok(true)
}
