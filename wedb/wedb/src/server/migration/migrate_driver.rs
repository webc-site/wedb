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

use std::{future::Future, io, io::ErrorKind, sync::Arc, time::Duration};

use compio::time::timeout;
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
    migration::{
      migrate_session::{MigrateSession, MigrateTaskSpec},
      migrate_state::MigrateState,
      sketch::Sketch,
    },
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

/// 迁移停等默认超时毫秒：spec.timeout <= 0 时兜底（redis-cli MIGRATE 默认
/// 口径，防零值退化成立即超时）
const DEFAULT_MIGRATE_TIMEOUT_MS: u64 = 60_000;

/// 停等时长：spec.timeout (ms) 由 MIGRATE 命令第 5 参流入（C# _timeout 同源），
/// <= 0 取默认
#[inline]
fn wait_dur(timeout_ms: i32) -> Duration {
  if timeout_ms > 0 {
    Duration::from_millis(timeout_ms as u64)
  } else {
    Duration::from_millis(DEFAULT_MIGRATE_TIMEOUT_MS)
  }
}

/// 远端停等包装：对标 libs/cluster/Server/Migration/MigrationDriver.cs:TrySetSlotRangesAsync
/// 的 `WaitAsync(_timeout, _cts.Token)` —— 任一远端响应限时，目标挂起不至
/// 任务永挂；超时转 Err 交调用方走 recover 失败路径
async fn wait_remote<F, T>(dur: Duration, fut: F) -> Result<T>
where
  F: Future<Output = Result<T>>,
{
  match timeout(dur, fut).await {
    Ok(res) => res,
    Err(_) => Err(Error::Io(io::Error::new(
      ErrorKind::TimedOut,
      "迁移远端停等超时",
    ))),
  }
}

/// 判定错误是否为停等超时：超时意味着连接上残留未决响应（wconn 严格
/// 停等，后续帧将永远排队），恢复前必须弃连重连
#[inline]
fn is_timeout_err(err: &Error) -> bool {
  matches!(err, Error::Io(e) if e.kind() == ErrorKind::TimedOut)
}

/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs:TryRecoverFromFailureAsync
///
/// 迁移失败恢复：远端逐 range 置 STABLE（nodeid=None，失败仅留痕不阻断，
/// C# 同口径）→ 本端槽位状态回退 → 会话状态置 FAIL。只回滚槽位状态，
/// 远端已导入批次数据不回收（C# 同口径）。poisoned（停等超时）时先重连
/// 再发恢复帧（对标 C# recover → TrySetSlotRangesAsync → CheckConnectionAsync
/// 的 ReconnectAsync 保供语义），收尾弃连防迟到 ACK 错位（对标 C#
/// MigrateSession.Dispose 的 _cts.Cancel 断连）
async fn try_recover_from_failure(
  client: &GarnetClient,
  session: &MigrateSession,
  ranges: &[(i32, i32)],
  dur: Duration,
  why: &str,
  poisoned: bool,
) {
  log::error!("迁移失败，执行恢复: {why}");
  if poisoned {
    client.reconnect_async().await;
  }
  for &(start, end) in ranges {
    match wait_remote(dur, client.set_slot_range_async("STABLE", start, end, None)).await {
      Ok(resp) if resp == "OK" => {}
      Ok(resp) => log::error!("恢复远端槽位 STABLE 失败: {resp}"),
      Err(err) => log::error!("恢复远端槽位 STABLE 失败: {err}"),
    }
  }
  session.reset_local_slot();
  *session.status.write() = MigrateState::Fail;
  client.dispose();
}

/// 单批装帧发送 + 停等 ACK：对标
/// libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:HandleMigrateTaskResponseAsync
/// （`WaitAsync(_timeout)` + 非 OK 判败）；空批次即完成哨兵帧 (recordCount = 0)
async fn send_batch_and_wait(
  client: &GarnetClient,
  dur: Duration,
  source_node_id: &str,
  replace_option: bool,
  batch: &[(Vec<u8>, Vec<u8>, i64)],
) -> Result<()> {
  let payload = encode_migration_payload(
    batch
      .iter()
      .map(|(k, v, exp)| (k.as_slice(), v.as_slice(), *exp)),
  );
  match wait_remote(
    dur,
    client.execute_cluster_migrate_async(source_node_id, replace_option, &payload),
  )
  .await
  {
    Ok(true) => Ok(()),
    Ok(false) => Err(Error::InvalidArgument(
      "远端 CLUSTER MIGRATE 拒绝数据".into(),
    )),
    Err(err) => Err(err),
  }
}

/// 远端槽位状态切换 + 逐 range 停等校验：对标
/// libs/cluster/Server/Migration/MigrationDriver.cs:TrySetSlotRangesAsync
/// （`WaitAsync(_timeout)` + 非 "OK" 判败置 FAIL）
async fn set_slot_ranges_checked(
  client: &GarnetClient,
  dur: Duration,
  state: &str,
  ranges: &[(i32, i32)],
  node_id: Option<&str>,
) -> Result<()> {
  for &(start, end) in ranges {
    let resp = wait_remote(dur, client.set_slot_range_async(state, start, end, node_id)).await?;
    if resp != "OK" {
      return Err(Error::InvalidArgument(format!(
        "远端 SETSLOTSRANGE {state} 失败: {resp}"
      )));
    }
  }
  Ok(())
}

/// 执行 CLUSTER MIGRATE 发送驱动 (M2 KEYS 路径)
///
/// 严格停等架构（全部远端 await 经 [`wait_remote`] 限时，目标挂起不至永挂）：
/// 1. 注册迁移任务
/// 2. 远端置槽位为 IMPORTING
/// 3. 本端置槽位为 MIGRATING
/// 4. 逐批装帧并发送 CLUSTER MIGRATE，批次停等 ACK (+OK)
/// 5. 发送完成哨兵空载荷帧 (recordCount = 0)
/// 6. 远端与本端切换槽位归属为 NODE / 释放所有权
/// 7. 任一失败点统一 [`try_recover_from_failure`] 回滚（远端 STABLE +
///    本端回退 + FAIL 留痕 + 弃连）
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
  let dur = wait_dur(spec.timeout);

  // 3. 远端置槽位 IMPORTING（停等限时，失败 → recover）
  if let Err(err) = set_slot_ranges_checked(
    &client,
    dur,
    "IMPORTING",
    &ranges,
    Some(spec.source_node_id),
  )
  .await
  {
    try_recover_from_failure(
      &client,
      &session,
      &ranges,
      dur,
      &err.to_string(),
      is_timeout_err(&err),
    )
    .await;
    return Err(err);
  }

  // 4. 本端置槽位 MIGRATING（失败 → recover）
  if !session.try_prepare_local_for_migration() {
    try_recover_from_failure(
      &client,
      &session,
      &ranges,
      dur,
      "本端准备迁移槽位失败",
      false,
    )
    .await;
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
      // 发送当前批次并限时停等 ACK（超时/拒绝均判败 → recover）
      if let Err(err) = send_batch_and_wait(
        &client,
        dur,
        spec.source_node_id,
        spec.replace_option,
        &cur_batch,
      )
      .await
      {
        try_recover_from_failure(
          &client,
          &session,
          &ranges,
          dur,
          &err.to_string(),
          is_timeout_err(&err),
        )
        .await;
        return Err(err);
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
    if let Err(err) = send_batch_and_wait(
      &client,
      dur,
      spec.source_node_id,
      spec.replace_option,
      &cur_batch,
    )
    .await
    {
      try_recover_from_failure(
        &client,
        &session,
        &ranges,
        dur,
        &err.to_string(),
        is_timeout_err(&err),
      )
      .await;
      return Err(err);
    }
    migrated_count += cur_batch.len();
    transferred.extend(cur_batch.iter().map(|(k, ..)| k.clone()));
  }

  // 6. 发送空载荷完成哨兵：应答非 OK 即判败——吞没响应会让远端导入残缺
  //    而源端照常交权（对标 HandleMigrateTaskResponseAsync 应答校验）
  if let Err(err) =
    send_batch_and_wait(&client, dur, spec.source_node_id, spec.replace_option, &[]).await
  {
    try_recover_from_failure(
      &client,
      &session,
      &ranges,
      dur,
      &err.to_string(),
      is_timeout_err(&err),
    )
    .await;
    return Err(err);
  }

  // 7. 远端置槽位 NODE（失败 → recover，对标 BeginAsyncMigrationTaskAsync NODE 分支）
  if let Err(err) =
    set_slot_ranges_checked(&client, dur, "NODE", &ranges, Some(spec.target_node_id)).await
  {
    try_recover_from_failure(
      &client,
      &session,
      &ranges,
      dur,
      &err.to_string(),
      is_timeout_err(&err),
    )
    .await;
    return Err(err);
  }
  // 本端释放归属（失败 → recover，对标 BeginAsyncMigrationTaskAsync
  // RelinquishOwnership 分支）
  if !session.relinquish_ownership() {
    try_recover_from_failure(
      &client,
      &session,
      &ranges,
      dur,
      "本端释放槽位所有权失败",
      false,
    )
    .await;
    return Err(Error::InvalidArgument("本端释放槽位所有权失败".into()));
  }

  // 8. 若非 copy 选项，仅删除「已确认传输成功」的键——未传输键（对象键/
  //    中途失败键/竞态改写键）一律保留在源端，杜绝静默丢键
  if !spec.copy_option {
    for key in &transferred {
      let _ = storage.delete_string(key).await;
    }
  }

  Ok(migrated_count)
}
