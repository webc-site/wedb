//! 集群键迁移协议帧编解码与发送驱动 (MigrateDriver)
//!
//! 对标 Garnet C# libs/cluster/Server/Migration/ClusterMigrateDriver.cs、
//! libs/cluster/Server/Migration/MigrationDriver.cs（任务启动/恢复编排）
//! 与 libs/cluster/Session/RespClusterMigrateCommands.cs。
//!
//! 帧格式定义 (M1/M2 单一真值源)：
//! `payload = [u32 LE recordCount][record]*`
//! `record = [u8 kind=1][u32 LE keyLen][key][u32 LE valLen][value][i64 LE expire_unix_ms(0=无TTL)]`
//!
//! 显式裁剪（禁止静默丢键）：本链路仅支持 string 记录 (kind=1)。Hash/Set/
//! ZSet/List/向量集等对象记录（KeyTag::ObjectEnvelope 域 / 集合元记录）
//! 不发帧，KEYS 入口对含对象键的请求整体拒绝并显式报错；SLOTS 游标循环
//! 对对象键跳过传输、保留源端并留痕。对象/大值 chunk 迁移属未实现（对标
//! C# ChunkedRecordReassembler 分相重组 + 全类型序列化），待 M 系列帧扩展
//! 立项补全。
//!
//! 孤儿键投影（net.md 条 19，安全声明）：槽位移交（远端 NODE + gossip 传
//! 播）与源端物理删除存在交错窗口，窗口内按槽路由的读可能在源端命中尚未
//! 删除的旧键投影（孤儿键）。键不丢、最终一致；C# 的 DELETING 分相与
//! gossip 传播交错存在同形窗口，属双方共有的安全投影而非缺陷。

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
      sketch_status::SketchStatus,
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

/// 远端停等包装：C# 迁移会话对每个远端响应统一施加
/// `Task.WaitAsync(_timeout, _cts.Token)` 限时（时长即 MIGRATE 命令的
/// timeout 参数），本辅助承接同一机制——任一远端 await 限时，目标挂起
/// 不至任务永挂；超时转 Err 交调用方走 recover 失败路径
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

/// 迁移目标客户端构造（用户名/口令非空才携带，对标 MigrateSession.cs:
/// GetGarnetClient 的 authUsername/authPassword 透传）
fn connect_migrate_client(spec: &MigrateTaskSpec) -> GarnetClient {
  GarnetClient::with_auth(
    format!("{}:{}", spec.target_address, spec.target_port),
    (!spec.username.is_empty()).then(|| spec.username.to_string()),
    (!spec.passwd.is_empty()).then(|| spec.passwd.to_string()),
  )
}

/// 迁移前置编排（对标 MigrationDriver.cs:BeginAsyncMigrationTaskAsync 的
/// IMPORT / MIGRATING / 纪元转换段）：建立连接 → 远端置 IMPORTING →
/// 本端置 MIGRATING →（SLOTS 链）纪元转换等待；任一失败点统一 recover
async fn begin_migration_phase(
  client: &GarnetClient,
  session: &MigrateSession,
  ranges: &[(i32, i32)],
  dur: Duration,
  source_node_id: &str,
  epoch_gate: bool,
) -> Result<()> {
  client.connect_async().await;
  if !client.is_connected() {
    session.reset_local_slot();
    return Err(Error::Io(io::Error::new(
      ErrorKind::ConnectionRefused,
      "无法连接迁移目标节点",
    )));
  }

  // 远端置槽位 IMPORTING（停等限时，失败 → recover）
  if let Err(err) =
    set_slot_ranges_checked(client, dur, "IMPORTING", ranges, Some(source_node_id)).await
  {
    let poisoned = is_timeout_err(&err);
    try_recover_from_failure(client, session, ranges, dur, &err.to_string(), poisoned).await;
    return Err(err);
  }

  // 本端置槽位 MIGRATING（失败 → recover）
  if !session.try_prepare_local_for_migration() {
    try_recover_from_failure(client, session, ranges, dur, "本端准备迁移槽位失败", false).await;
    return Err(Error::InvalidArgument("本端准备迁移槽位失败".into()));
  }

  // 纪元转换等待（对标 BeginAsyncMigrationTaskAsync 的
  // BumpAndWaitForEpochTransitionAsync，仅 SLOTS 链调用；C# 失败静默
  // return 致任务与槽位状态悬挂，rust 显式 recover + Err 收敛）
  if epoch_gate
    && !session
      .cluster_provider
      .bump_and_wait_for_epoch_transition_async()
      .await
  {
    try_recover_from_failure(client, session, ranges, dur, "迁移纪元转换等待失败", false).await;
    return Err(Error::InvalidArgument("迁移纪元转换等待失败".into()));
  }
  Ok(())
}

/// 迁移收尾编排（对标 MigrationDriver.cs:BeginAsyncMigrationTaskAsync 的
/// 完成哨兵 / NODE / RelinquishOwnership 段）；任一失败点统一 recover。
/// C# 收尾段的 SuspendConfigMerge + TryMeetAsync gossip 汇聚未投影（依赖
/// gossip 会话基建，属范围外），槽位视图一致性由 gossip 周期汇聚兜底
async fn end_migration_phase(
  client: &GarnetClient,
  session: &MigrateSession,
  ranges: &[(i32, i32)],
  dur: Duration,
  spec: &MigrateTaskSpec,
) -> Result<()> {
  // 完成哨兵空载荷帧：应答非 OK 即判败——吞没响应会让远端导入残缺而
  // 源端照常交权（对标 HandleMigrateTaskResponseAsync 应答校验）
  if let Err(err) = send_batch_and_wait(
    client,
    dur,
    spec.source_node_id.as_str(),
    spec.replace_option,
    &[],
  )
  .await
  {
    let poisoned = is_timeout_err(&err);
    try_recover_from_failure(client, session, ranges, dur, &err.to_string(), poisoned).await;
    return Err(err);
  }
  // 远端置槽位 NODE（失败 → recover，对标 BeginAsyncMigrationTaskAsync NODE 分支）
  if let Err(err) = set_slot_ranges_checked(
    client,
    dur,
    "NODE",
    ranges,
    Some(spec.target_node_id.as_str()),
  )
  .await
  {
    let poisoned = is_timeout_err(&err);
    try_recover_from_failure(client, session, ranges, dur, &err.to_string(), poisoned).await;
    return Err(err);
  }
  // 本端释放归属（失败 → recover，对标 BeginAsyncMigrationTaskAsync
  // RelinquishOwnership 分支）
  if !session.relinquish_ownership() {
    try_recover_from_failure(
      client,
      session,
      ranges,
      dur,
      "本端释放槽位所有权失败",
      false,
    )
    .await;
    return Err(Error::InvalidArgument("本端释放槽位所有权失败".into()));
  }
  Ok(())
}

/// 读取单键活值与 TTL 毫秒：string 域未命中（不存在 / 已过期 / 竞态改写
/// 为对象记录）返回 None，调用方决定跳过或登记不可迁移
async fn read_live_string(
  storage: &StorageSession<'_, SegmentedDevice>,
  key: &[u8],
) -> Result<Option<(Vec<u8>, i64)>> {
  let Some(val) = storage.read_string(key).await? else {
    return Ok(None);
  };
  let ttl_ms = match storage.batch.ttl_of(key).await? {
    Some(exp) => {
      if exp <= now_ticks() {
        return Ok(None); // 已过期，跳过
      }
      (exp - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND
    }
    None => 0,
  };
  Ok(Some((val, ttl_ms)))
}

/// 键清单批量传输：逐键读活值、按字节/条数上限分批装帧停等 ACK，
/// 返回（已确认 ACK 的键, 确认条数）。中途失败已 ACK 批次不回传（失败
/// 路径不删除任何键，键权保留源端，对标 C# 传输失败不 DeleteKeys）
async fn transmit_keys(
  storage: &StorageSession<'_, SegmentedDevice>,
  client: &GarnetClient,
  dur: Duration,
  spec: &MigrateTaskSpec,
  keys: &[Vec<u8>],
) -> Result<(Vec<Vec<u8>>, usize)> {
  let mut transferred = Vec::new();
  let mut migrated_count = 0usize;
  let mut cur_batch: Vec<(Vec<u8>, Vec<u8>, i64)> = Vec::new();
  let mut cur_batch_bytes = 0usize;

  for key in keys {
    let Some((val, ttl_ms)) = read_live_string(storage, key).await? else {
      // 竞态兜底：键被并发删除/过期/改写为对象记录 → 不发帧、不计入
      // 删除清单，键权保留在源端（绝不波及未成功传输的键）
      continue;
    };

    let item_bytes = key.len() + val.len() + 17;
    if !cur_batch.is_empty()
      && (cur_batch.len() >= MAX_MIGRATION_BATCH_COUNT
        || cur_batch_bytes + item_bytes > MAX_MIGRATION_BATCH_BYTES)
    {
      send_batch_and_wait(
        client,
        dur,
        spec.source_node_id.as_str(),
        spec.replace_option,
        &cur_batch,
      )
      .await?;
      migrated_count += cur_batch.len();
      transferred.extend(cur_batch.iter().map(|(k, ..)| k.clone()));
      cur_batch.clear();
      cur_batch_bytes = 0;
    }

    cur_batch_bytes += item_bytes;
    cur_batch.push((key.clone(), val, ttl_ms));
  }

  if !cur_batch.is_empty() {
    send_batch_and_wait(
      client,
      dur,
      spec.source_node_id.as_str(),
      spec.replace_option,
      &cur_batch,
    )
    .await?;
    migrated_count += cur_batch.len();
    transferred.extend(cur_batch.iter().map(|(k, ..)| k.clone()));
  }
  Ok((transferred, migrated_count))
}

/// 执行 CLUSTER MIGRATE 发送驱动 (KEYS 路径，对标
/// MigrationDriver.cs:TryStartMigrationTaskAsync 的 KEYS 分支 +
/// MigrateSessionKeys.cs:MigrateKeysAsync)
///
/// 严格停等架构（全部远端 await 经 [`wait_remote`] 限时，目标挂起不至永挂）：
/// 1. 对象键预检（零副作用拒绝）→ 提取槽位 + sketch 收录 → 注册迁移任务
/// 2. 前置编排 [`begin_migration_phase`]：IMPORTING → MIGRATING
/// 3. sketch 切 TRANSMITTING，逐批装帧发送 CLUSTER MIGRATE 停等 ACK
/// 4. 收尾编排 [`end_migration_phase`]：哨兵 → NODE → relinquish
/// 5. 非 copy 删除「已确认 ACK」的键（DELETING 门控），sketch 归位
/// 6. finally 移除迁移任务（对标 KEYS 分支 finally TryRemoveMigrationTask）
///
/// 限制（显式裁剪，禁止静默丢键）：迁移帧仅支持 string 记录 (kind=1)。
/// 请求键清单含对象记录键（Hash/Set/ZSet/List/向量集等）时，入口预检
/// [`probe_object_keys`] 整体拒绝并列明键清单——此时尚未注册任务、未触达
/// 远端，源端零状态变更、零键删除，绝不静默跳过。仅在全部批次 ACK 成功
/// 后，源端才对「已确认传输成功」的键执行删除/交权，未传输键一律保留。
pub async fn run_keys_migration_driver(
  cluster_provider: Arc<ClusterProvider>,
  store: Arc<WedbStore<SegmentedDevice>>,
  spec: MigrateTaskSpec,
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

  // 1. 提取槽位集合并收录 sketch（对标 MigrateCommand.cs 解析期
  //    sketch.HashAndStore：键级门控 can_access_key 据此生效）
  let mut slots = HashSet::default();
  let sketch = Sketch::new();
  for k in keys {
    slots.insert(cluster_slot(k) as i32);
    sketch.hash_and_store(k);
  }

  // 2. 注册任务
  let session = migration_mgr
    .try_add_migration_task(spec.clone(), slots, sketch)
    .ok_or_else(|| Error::InvalidArgument("创建迁移任务失败 (槽位冲突或超限)".into()))?;

  // 3. 执行 + finally 移除任务
  let res = execute_keys_migration(&store, &spec, &session, keys).await;
  migration_mgr.try_remove_migration_task_session(Arc::clone(&session));
  res
}

/// KEYS 驱动执行体（任务注册后调用；失败统一 recover，见各编排函数）
async fn execute_keys_migration(
  store: &Arc<WedbStore<SegmentedDevice>>,
  spec: &MigrateTaskSpec,
  session: &Arc<MigrateSession>,
  keys: &[Vec<u8>],
) -> Result<usize> {
  let client = connect_migrate_client(spec);
  let ranges = session.get_ranges();
  let dur = wait_dur(spec.timeout);

  begin_migration_phase(
    &client,
    session,
    &ranges,
    dur,
    spec.source_node_id.as_str(),
    false,
  )
  .await?;

  // TRANSMITTING：载荷在途，源端对已收录键的写等待（对标
  // MigrateKeysFromStoreAsync 的 sketch.SetStatus(TRANSMITTING)）
  session.sketch.set_status(SketchStatus::Transmitting);

  let wkv_session = store.new_session()?;
  let batch = wkv_session.enter_batch();
  let storage = StorageSession::new_readonly(batch);

  let (transferred, migrated_count) = match transmit_keys(&storage, &client, dur, spec, keys).await
  {
    Ok(res) => res,
    Err(err) => {
      let poisoned = is_timeout_err(&err);
      try_recover_from_failure(&client, session, &ranges, dur, &err.to_string(), poisoned).await;
      return Err(err);
    }
  };

  end_migration_phase(&client, session, &ranges, dur, spec).await?;

  // 非 copy：删除「已确认传输成功」的键——未传输键（对象键/中途失败键/
  // 竞态改写键）一律保留在源端，杜绝静默丢键；DELETING 门控读写全等待
  // （对标 DeleteKeysAsync 的 DELETING → 删除 → MIGRATED 序列）
  if !spec.copy_option {
    session.sketch.set_status(SketchStatus::Deleting);
    for key in &transferred {
      let _ = storage.delete_string(key).await;
    }
  }
  // MIGRATED 释放等待操作后归位（对标 MigrateKeysAsync finally 的
  // INITIALIZING；两态对 can_access_key 均放行，连续设置无观察窗口）
  session.sketch.set_status(SketchStatus::Migrated);
  session.sketch.set_status(SketchStatus::Initializing);

  Ok(migrated_count)
}

/// 注册 SLOTS 迁移任务（命令臂同步段，对标 MigrateCommand.cs 解析尾的
/// TryAddMigrationTask）；槽位冲突/超限显式失败
pub fn try_add_slots_migration_task(
  cluster_provider: &Arc<ClusterProvider>,
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
/// 键；收尾编排哨兵 → NODE → relinquish。
///
/// 删除游标只推进到「批次 ACK 成功」的键（对标 C# MigrateOperation.
/// DeleteKeys 只删 sketch 收录键；绝不使用 delete_slot_keys 全槽清除——
/// 槽内对象信封键未传输，误删即丢数据）。
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

/// SLOTS 驱动执行体（任务注册后调用；失败统一 recover，见各编排函数）
async fn execute_slots_migration(
  store: &Arc<WedbStore<SegmentedDevice>>,
  spec: &MigrateTaskSpec,
  session: &Arc<MigrateSession>,
) -> Result<usize> {
  let client = connect_migrate_client(spec);
  let ranges = session.get_ranges();
  let dur = wait_dur(spec.timeout);

  begin_migration_phase(
    &client,
    session,
    &ranges,
    dur,
    spec.source_node_id.as_str(),
    true,
  )
  .await?;

  let wkv_session = store.new_session()?;
  let batch = wkv_session.enter_batch();
  let storage = StorageSession::new_readonly(batch);

  let mut migrated_count = 0usize;
  // 槽序稳定推进（HashSet 迭代无序，排序后逐槽处理）
  let mut sorted_slots: Vec<i32> = session.get_slots().iter().copied().collect();
  sorted_slots.sort_unstable();
  for slot in sorted_slots {
    // 不可迁移键登记（对象信封键 / 并发改写键 / 已死键）：驱动游标每轮
    // 自槽头重扫，须剔除以收敛；键保留源端（孤儿键投影声明见模块注释）
    let mut untouchable: HashSet<Vec<u8>> = HashSet::default();
    loop {
      // INITIALIZING 扫描收键（对标 ScanStoreTaskAsync 的
      // SetStatus(INITIALIZING) → Scan）
      let keys = storage
        .get_keys_in_slot(slot as u16, MAX_MIGRATION_BATCH_COUNT)
        .await?;
      let work: Vec<Vec<u8>> = keys
        .iter()
        .filter(|k| !untouchable.contains(*k))
        .cloned()
        .collect();
      if work.is_empty() {
        break;
      }

      // TRANSMITTING：分批停等传输（对标 TRANSMITTING → TransmitSlotsAsync）
      session.sketch.set_status(SketchStatus::Transmitting);
      for k in &work {
        session.sketch.hash_and_store(k);
      }
      let (transferred, moved) = match transmit_keys(&storage, &client, dur, spec, &work).await {
        Ok(res) => res,
        Err(err) => {
          let poisoned = is_timeout_err(&err);
          try_recover_from_failure(&client, session, &ranges, dur, &err.to_string(), poisoned)
            .await;
          return Err(err);
        }
      };
      migrated_count += moved;

      // DELETING：删除已确认传输键 → 清 sketch 进入下一轮（对标
      // DELETING → DeleteKeys → sketch.Clear()）
      session.sketch.set_status(SketchStatus::Deleting);
      for key in &transferred {
        let _ = storage.delete_string(key).await;
      }
      session.sketch.clear();

      // 活值未命中的键登记不可迁移（显式留痕，键权保留源端）
      let transferred_set: HashSet<&[u8]> = transferred.iter().map(|k| k.as_slice()).collect();
      let stuck: Vec<Vec<u8>> = work
        .iter()
        .filter(|k| !transferred_set.contains(k.as_slice()))
        .cloned()
        .collect();
      if !stuck.is_empty() {
        log::error!(
          "槽 {slot} 有 {} 个键不可迁移（对象记录或已不存在），保留源端: {}",
          stuck.len(),
          stuck
            .iter()
            .map(|k| String::from_utf8_lossy(k))
            .collect::<Vec<_>>()
            .join(", ")
        );
        for key in stuck {
          untouchable.insert(key);
        }
      }
    }
  }

  end_migration_phase(&client, session, &ranges, dur, spec).await?;
  Ok(migrated_count)
}
