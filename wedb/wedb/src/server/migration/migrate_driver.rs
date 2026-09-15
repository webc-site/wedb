//! 集群键迁移协议帧编解码与发送驱动 (MigrateDriver)
//!
//! 对标 Garnet C# libs/cluster/Server/Migration/ClusterMigrateDriver.cs
//! 与 libs/cluster/Session/RespClusterMigrateCommands.cs。
//!
//! 帧格式定义 (M1/M2 单一真值源)：
//! `payload = [u32 LE recordCount][record]*`
//! `record = [u8 kind=1][u32 LE keyLen][key][u32 LE valLen][value][i64 LE expire_unix_ms(0=无TTL)]`
//!
//! 显式裁剪（禁止静默丢键）：本链路仅支持 string 记录 (kind=1)。Hash/Set/
//! ZSet/List/向量集等对象记录（KeyTag::ObjectEnvelope 域 / 集合元记录）
//! 不发帧，迁移入口对含对象键的请求整体拒绝并显式报错。对象/大值 chunk
//! 迁移属未实现（对标 C# ChunkedRecordReassembler 分相重组 + 全类型序列
//! 化），待 M 系列帧扩展立项补全。

use std::{io, io::ErrorKind, sync::Arc};

use gxhash::HashSet;
use wbase::{
  convert::{TICKS_PER_MILLISECOND, UNIX_EPOCH_TICKS},
  hash_slot::hash_slot as cluster_slot,
  time::now_ticks,
};
use wdev::{Device, SegmentedDevice};
use wkv::WedbStore;
use wnode::storage::session::storage_session::StorageSession;

use crate::{
  client::GarnetClient,
  error::{Error, Result},
  server::{
    cluster_provider::ClusterProvider,
    migration::{migrate_session::MigrateTaskSpec, sketch::Sketch},
  },
};

/// 迁移单批最大字节限制 (512KB)
pub const MAX_MIGRATION_BATCH_BYTES: usize = 512 * 1024;
/// 迁移单批最大记录条数 (64 条)
pub const MAX_MIGRATION_BATCH_COUNT: usize = 64;
/// 迁移帧记录类型：字符串记录（本迁移链路唯一支持类型，M1/M2 唯一合法 kind）
pub const MIGRATION_RECORD_KIND_STRING: u8 = 1;

/// 单条迁移记录解码视图
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationRecord<'a> {
  /// 记录类型 (1 = 字符串/标准日志记录)
  pub kind: u8,
  /// 键字节
  pub key: &'a [u8],
  /// 值字节
  pub val: &'a [u8],
  /// 绝对过期 Unix 时间戳 (毫秒，0 表示无 TTL)
  pub expire_unix_ms: i64,
}

/// 解析迁移载荷帧
pub fn parse_migration_payload(payload: &[u8]) -> Result<(u32, Vec<MigrationRecord<'_>>)> {
  if payload.len() < 4 {
    return Err(Error::InvalidArgument("载荷长度不足 4 字节".into()));
  }
  let record_count = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
  if record_count == 0 {
    return Ok((0, Vec::new()));
  }
  let mut records = Vec::with_capacity(record_count as usize);
  let mut cur = 4;
  for _ in 0..record_count {
    if cur + 5 > payload.len() {
      return Err(Error::InvalidArgument("载荷意外截断 (kind/key_len)".into()));
    }
    let kind = payload[cur];
    cur += 1;
    let key_len = u32::from_le_bytes([
      payload[cur],
      payload[cur + 1],
      payload[cur + 2],
      payload[cur + 3],
    ]) as usize;
    cur += 4;
    if cur + key_len + 4 > payload.len() {
      return Err(Error::InvalidArgument("载荷意外截断 (key/val_len)".into()));
    }
    let key = &payload[cur..cur + key_len];
    cur += key_len;
    let val_len = u32::from_le_bytes([
      payload[cur],
      payload[cur + 1],
      payload[cur + 2],
      payload[cur + 3],
    ]) as usize;
    cur += 4;
    if cur + val_len + 8 > payload.len() {
      return Err(Error::InvalidArgument("载荷意外截断 (val/expire)".into()));
    }
    let val = &payload[cur..cur + val_len];
    cur += val_len;
    let expire_unix_ms = i64::from_le_bytes([
      payload[cur],
      payload[cur + 1],
      payload[cur + 2],
      payload[cur + 3],
      payload[cur + 4],
      payload[cur + 5],
      payload[cur + 6],
      payload[cur + 7],
    ]);
    cur += 8;
    records.push(MigrationRecord {
      kind,
      key,
      val,
      expire_unix_ms,
    });
  }
  Ok((record_count, records))
}

/// 编码迁移载荷帧 (单趟流式序列化，零中间分配)
pub fn encode_migration_payload<'a, I>(records: I) -> Vec<u8>
where
  I: IntoIterator<Item = (&'a [u8], &'a [u8], i64)>,
{
  let iter = records.into_iter();
  let (lower, _) = iter.size_hint();
  let mut buf = Vec::with_capacity(4 + lower * 32);
  buf.extend_from_slice(&0u32.to_le_bytes());
  let mut count = 0u32;
  for (key, val, expire_ms) in iter {
    count += 1;
    buf.push(MIGRATION_RECORD_KIND_STRING);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
    buf.extend_from_slice(val);
    buf.extend_from_slice(&expire_ms.to_le_bytes());
  }
  buf[0..4].copy_from_slice(&count.to_le_bytes());
  buf
}

/// 预检键清单，返回其中「对象记录键」（非 string 记录）
///
/// 判定口径（双域探测）：String 域 `read_string` 未命中、且 `contains_key`
/// 命中（覆盖集合 Meta 元记录与 KeyTag::ObjectEnvelope 对象信封物理域，含
/// TTL 惰性过期裁决）——即键存活但并非 string 记录。纯 string 键、不存在
/// 键、已过期键均不入选。供迁移入口在触达远端前显式拒绝混合键请求。
pub async fn probe_object_keys<D: Device>(
  storage: &StorageSession<'_, D>,
  keys: &[Vec<u8>],
) -> wkv::Result<Vec<Vec<u8>>> {
  let mut object_keys = Vec::new();
  for key in keys {
    if storage.read_string(key).await?.is_none() && storage.batch.contains_key(key).await? {
      object_keys.push(key.clone());
    }
  }
  Ok(object_keys)
}

/// 执行 CLUSTER MIGRATE 发送驱动 (M2 KEYS 路径)
///
/// 严格停等架构：
/// 1. 注册迁移任务
/// 2. 远端置槽位为 IMPORTING
/// 3. 本端置槽位为 MIGRATING
/// 4. 逐批装帧并发送 CLUSTER MIGRATE，批次停等 ACK (+OK)
/// 5. 发送完成哨兵空载荷帧 (recordCount = 0)
/// 6. 远端与本端切换槽位归属为 NODE / 释放所有权
/// 7. 异常回滚 STABLE / reset_local_slot
///
/// 限制（显式裁剪，禁止静默丢键）：迁移帧仅支持 string 记录 (kind=1)。
/// 请求键清单含对象记录键（Hash/Set/ZSet/List/向量集等）时，入口预检
/// [`probe_object_keys`] 整体拒绝并列明键清单——此时尚未注册任务、未触达
/// 远端，源端零状态变更、零键删除，绝不静默跳过。仅在全部批次 ACK 成功
/// 后，源端才对「已确认传输成功」的键执行删除/交权，未传输键一律保留。
pub async fn run_keys_migration_driver(
  cluster_provider: Arc<ClusterProvider>,
  store: Arc<WedbStore<SegmentedDevice>>,
  spec: MigrateTaskSpec<'_>,
  keys: &[Vec<u8>],
) -> Result<usize> {
  let Some(migration_mgr) = cluster_provider.migration_manager() else {
    return Err(Error::ClusterNotInitialized);
  };

  // 0. 对象键预检（显式裁剪，禁止静默丢键）：探测到对象记录键即整体失败，
  //    错误中列明清单；失败点在注册任务/连接远端之前，零副作用可安全重试
  {
    let probe_session = store.new_session()?;
    let probe_batch = probe_session.enter_batch();
    let probe_storage = StorageSession::new_readonly(probe_batch);
    let object_keys = probe_object_keys(&probe_storage, keys).await?;
    if !object_keys.is_empty() {
      return Err(Error::InvalidArgument(format!(
        "MIGRATE 拒绝：{} 个键为对象记录（迁移帧仅支持 string 记录），已整体取消迁移：{}",
        object_keys.len(),
        object_keys
          .iter()
          .map(|k| String::from_utf8_lossy(k))
          .collect::<Vec<_>>()
          .join(", ")
      )));
    }
  }

  // 1. 提取槽位集合
  let mut slots = HashSet::default();
  for k in keys {
    slots.insert(cluster_slot(k) as i32);
  }

  // 2. 注册任务
  let session = migration_mgr
    .try_add_migration_task(spec, slots, Sketch::new())
    .ok_or_else(|| Error::InvalidArgument("创建迁移任务失败 (槽位冲突或超限)".into()))?;

  let client = GarnetClient::with_auth(
    format!("{}:{}", spec.target_address, spec.target_port),
    if spec.username.is_empty() {
      None
    } else {
      Some(spec.username.to_string())
    },
    if spec.passwd.is_empty() {
      None
    } else {
      Some(spec.passwd.to_string())
    },
  );
  client.connect_async().await;
  if !client.is_connected() {
    session.reset_local_slot();
    return Err(Error::Io(io::Error::new(
      ErrorKind::ConnectionRefused,
      "无法连接迁移目标节点",
    )));
  }

  let ranges = session.get_ranges();

  // 3. 远端置槽位 IMPORTING
  for &(start, end) in &ranges {
    let resp = client
      .set_slot_range_async("IMPORTING", start, end, Some(spec.source_node_id))
      .await?;
    if resp != "OK" {
      session.reset_local_slot();
      return Err(Error::InvalidArgument(
        "远端 SETSLOTSRANGE IMPORTING 失败".into(),
      ));
    }
  }

  // 4. 本端置槽位 MIGRATING
  if !session.try_prepare_local_for_migration() {
    session.reset_local_slot();
    for &(start, end) in &ranges {
      let _ = client
        .set_slot_range_async("STABLE", start, end, None)
        .await;
    }
    return Err(Error::InvalidArgument("本端准备迁移槽位失败".into()));
  }

  // 5. 逐键读取并分批发送
  let wkv_session = store.new_session()?;
  let batch = wkv_session.enter_batch();
  let storage = StorageSession::new_readonly(batch);

  let mut migrated_count = 0;
  // 已确认传输成功（批次 ACK +OK）的键：源端删除游标只允许推进到这里
  let mut transferred: Vec<Vec<u8>> = Vec::new();
  let mut cur_batch: Vec<(Vec<u8>, Vec<u8>, i64)> = Vec::new();
  let mut cur_batch_bytes = 0;

  for key in keys {
    let Some(val) = storage.read_string(key).await? else {
      // 预检后的竞态兜底：键被并发删除/过期/改写为对象记录 → 不发帧、
      // 不计入删除清单，键权保留在源端（绝不波及未成功传输的键）
      continue;
    };
    let ttl_ms = match storage.batch.ttl_of(key).await? {
      Some(exp) => {
        let now = now_ticks();
        if exp <= now {
          continue; // 已过期，跳过
        }
        (exp - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND
      }
      None => 0,
    };

    let item_bytes = key.len() + val.len() + 17;
    if !cur_batch.is_empty()
      && (cur_batch.len() >= MAX_MIGRATION_BATCH_COUNT
        || cur_batch_bytes + item_bytes > MAX_MIGRATION_BATCH_BYTES)
    {
      // 发送当前批次并停等 ACK
      let payload = encode_migration_payload(
        cur_batch
          .iter()
          .map(|(k, v, exp)| (k.as_slice(), v.as_slice(), *exp)),
      );
      let ok = client
        .execute_cluster_migrate_async(spec.source_node_id, spec.replace_option, &payload)
        .await?;
      if !ok {
        session.reset_local_slot();
        for &(start, end) in &ranges {
          let _ = client
            .set_slot_range_async("STABLE", start, end, None)
            .await;
        }
        return Err(Error::InvalidArgument(
          "远端 CLUSTER MIGRATE 拒绝数据".into(),
        ));
      }
      migrated_count += cur_batch.len();
      transferred.extend(cur_batch.iter().map(|(k, ..)| k.clone()));
      cur_batch.clear();
      cur_batch_bytes = 0;
    }

    cur_batch_bytes += item_bytes;
    cur_batch.push((key.clone(), val, ttl_ms));
  }

  if !cur_batch.is_empty() {
    let payload = encode_migration_payload(
      cur_batch
        .iter()
        .map(|(k, v, exp)| (k.as_slice(), v.as_slice(), *exp)),
    );
    let ok = client
      .execute_cluster_migrate_async(spec.source_node_id, spec.replace_option, &payload)
      .await?;
    if !ok {
      session.reset_local_slot();
      for &(start, end) in &ranges {
        let _ = client
          .set_slot_range_async("STABLE", start, end, None)
          .await;
      }
      return Err(Error::InvalidArgument(
        "远端 CLUSTER MIGRATE 拒绝数据".into(),
      ));
    }
    migrated_count += cur_batch.len();
    transferred.extend(cur_batch.iter().map(|(k, ..)| k.clone()));
  }

  // 6. 发送空载荷完成哨兵
  let empty_payload = encode_migration_payload([]);
  let _ = client
    .execute_cluster_migrate_async(spec.source_node_id, spec.replace_option, &empty_payload)
    .await;

  // 7. 远端置槽位 NODE，本端释放归属
  for &(start, end) in &ranges {
    let _ = client
      .set_slot_range_async("NODE", start, end, Some(spec.target_node_id))
      .await;
  }
  let _ = session.relinquish_ownership();

  // 8. 若非 copy 选项，仅删除「已确认传输成功」的键——未传输键（对象键/
  //    中途失败键/竞态改写键）一律保留在源端，杜绝静默丢键
  if !spec.copy_option {
    for key in &transferred {
      let _ = storage.delete_string(key).await;
    }
  }

  Ok(migrated_count)
}
