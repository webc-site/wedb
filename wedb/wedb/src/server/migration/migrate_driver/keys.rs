//! 基于键清单的迁移流程与驱动循环 (KEYS 路径)
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs:TryStartMigrationTaskAsync
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionKeys.cs:MigrateKeysAsync

use std::{future::Future, io, io::ErrorKind, pin::pin, sync::Arc, time::Duration};

use coarsetime::Instant;
use compio::time::timeout;
use itoa::Buffer;
use wbase::{hex::hex_str_u128, map::HashSet};
use wbftree::DEFAULT_MIGRATION_CHUNK_SIZE;
use wconn::record::{BatchItem, encode_migration_payload, send_chunked_record};
use wdev::{Device, SegmentedDevice};
use wkv::WedbStore;
use wnode::{
  resp::vector::vector_manager::VectorManager, storage::session::storage_session::StorageSession,
};
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;

use super::{
  live_value::{LiveValue, collect_vector_set_keys, probe_unsupported_keys, read_live_value},
  recover_and_fail,
};
use crate::{
  client::GarnetClient,
  error::{Error, Result},
  server::{
    cluster_provider::ClusterProvider,
    migration::{
      migrate_session::{MigrateSession, MigrateTaskSpec},
      migrate_session_range_index::transmit_range_index_async,
      migrate_state::MigrateState,
      sketch::Sketch,
      sketch_status::SketchStatus,
      transfer_option::TransferOption,
    },
    sync_transport::{MAX_MIGRATION_BATCH_COUNT, transmit_vector_set_frames},
  },
};

/// 停等等待的取消轮询切片：等待期间周期性检查会话取消令牌，dispose 触发
/// 后在途停等即时收敛（对标 C# `WaitAsync(_timeout, _cts.Token)` 的令牌联动）
const CANCEL_POLL_SLICE: Duration = Duration::from_millis(25);

/// 停等时长三态映射：spec.timeout (ms) 由 MIGRATE 命令第 5 参流入
/// （C# `TimeSpan.FromMilliseconds(_timeout)` 同源，`WaitAsync(_timeout)` 消费）：
/// timeout > 0 → `Some(限时)`；timeout == 0 → `Some(ZERO)`（基线快失败档，
/// 首次 [`wait_remote`] deadline 检查即判超时）；timeout == -1 → `None`
/// （免超时档，对标 C# `Timeout.InfiniteTimeSpan`，停等仅随取消令牌收敛）。
/// 其余负值（< -1，C# 基线在首个 WaitAsync 抛 ArgumentOutOfRangeException
/// 致迁移运行期失败）已在命令解析期显式 ERR 拒收（见 cluster_session/
/// migrate.rs，偏差登记 doc/zh/deviations.md 86），本函数定义域内不可达
#[inline]
pub fn wait_dur(timeout_ms: i32) -> Option<Duration> {
  (timeout_ms >= 0).then(|| Duration::from_millis(timeout_ms as u64))
}

/// 停等超时错误
pub(crate) fn timeout_err() -> Error {
  Error::Io(io::Error::new(ErrorKind::TimedOut, "迁移远端停等超时"))
}

/// 取消令牌触发错误
pub(crate) fn cancelled_err() -> Error {
  Error::InvalidArgument("迁移已被取消 (MigrateSession disposed)".into())
}

/// 判定错误是否为停等超时：超时意味着连接上残留未决响应（wconn 严格
/// 停等，后续帧将永远排队），恢复前必须弃连重连
#[inline]
pub(crate) fn is_timeout_err(err: &Error) -> bool {
  matches!(err, Error::Io(e) if e.kind() == ErrorKind::TimedOut)
}

/// 远端停等包装：C# 迁移会话对每个远端响应统一施加
/// `Task.WaitAsync(_timeout, _cts.Token)` 限时（时长即 MIGRATE 命令的
/// timeout 参数经 [`wait_dur`] 的三态映射，令牌即会话取消令牌）。任一远端
/// await 限时或被 [`MigrateSession::dispose`] 取消，目标挂起不至任务永挂；
/// 超时/取消转 Err 交调用方走 recover 失败路径。`None` 为免超时档（对标
/// C# `Timeout.InfiniteTimeSpan`）：不挂 deadline，保留取消轮询循环节拍，
/// dispose 仍能即时收敛。截止时刻取全仓统一单调时钟源
/// `coarsetime::Instant`（与 replay_align_barrier、failover 会话、wtxn 各超时轮转同源，粗粒度域，
/// 非 TTL 判定路径；C# 迁移键驱动无超时计时面，此 deadline 为 rust 自有防护）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:CompletePending
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:RetryAsync
pub(crate) async fn wait_remote<F, T>(
  dur: Option<Duration>,
  session: &MigrateSession,
  fut: F,
) -> Result<T>
where
  F: Future<Output = Result<T>>,
{
  let mut fut = pin!(fut);
  let deadline = dur.map(|dur| Instant::now() + dur.into());
  loop {
    if session.is_cancelled() {
      return Err(cancelled_err());
    }
    let slice = match deadline {
      Some(deadline) => {
        let now = Instant::now();
        if now >= deadline {
          return Err(timeout_err());
        }
        CANCEL_POLL_SLICE.min((deadline - now).into())
      }
      None => CANCEL_POLL_SLICE,
    };
    match timeout(slice, fut.as_mut()).await {
      Ok(res) => return res,
      Err(_) if deadline.is_some_and(|deadline| Instant::now() >= deadline) => {
        return Err(timeout_err());
      }
      Err(_) => continue, // 切片到期：复查取消令牌后续等
    }
  }
}

/// 迁移终态收口：触发会话取消令牌并断开已打开的目标端客户端会话
/// （调用 MigrateSession dispose 取消令牌并释放 gcs 客户端）
///
/// libs/cluster/Server/Migration/MigrateOperation.cs:Dispose
///
/// C# 该件释放 operation 自有两资源：gcs（目标端客户端会话）与
/// localServerSession。rust 对位：客户端断连即本件 `client.dispose()`；
/// 本地会话侧为驱动栈帧内 `StorageSession`（批作用域 RAII，出作用域即释
/// 卷，无显式 Dispose）；`session.dispose()` 承接 C# 会话取消令牌
/// （`_cts.Cancel`，映射锚持于 migrate_session.rs 的 MigrateSession
/// Dispose 件）
pub(crate) fn dispose_migration(client: &GarnetClient, session: &MigrateSession) {
  session.dispose();
  client.dispose();
}

/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs:TryRecoverFromFailureAsync
///
/// 迁移失败恢复：远端逐 range 置 STABLE（nodeid=None，失败仅留痕不阻断，
/// C# 同口径）→（仅 SLOTS 链）本端槽位状态回退 → 会话状态置 FAIL。只回滚
/// 槽位状态，远端已导入批次数据不回收（C# 同口径）。KEYS 链仅回滚驱动自动
/// 下发的远端 IMPORTING→STABLE，本端 MIGRATING 为运维经 NOTMIGRATING 门
/// 手工前置，保持原状留运维 CLUSTER SETSLOT 收口（C# KEYS 臂无槽位回滚）。
/// poisoned（停等超时）时先重连再发恢复帧（对标 C# recover →
/// TrySetSlotRangesAsync → CheckConnectionAsync 的 ReconnectAsync 保供语义），
/// 收尾弃连防迟到 ACK 错位并触发会话取消（对标 C# MigrateSession.Dispose
/// 的 _cts.Cancel 断连）
pub(crate) async fn try_recover_from_failure(
  client: &GarnetClient,
  session: &MigrateSession,
  ranges: &[(i32, i32)],
  dur: Option<Duration>,
  why: &str,
  poisoned: bool,
  transfer: TransferOption,
) {
  log::error!("迁移失败，执行恢复: {why}");
  if poisoned {
    let _ = client.reconnect_async().await;
  }
  for &(start, end) in ranges {
    // 与全链停等同机制经 wait_remote 限时并联控取消令牌（免超时档 None
    // 仅随取消收敛）；恢复帧失败仅留痕不阻断（C# 同口径）
    match wait_remote(
      dur,
      session,
      client.set_slot_range_async(MigrateState::Stable.as_slot_state_str(), start, end, None),
    )
    .await
    {
      Ok(resp) if resp == "OK" => {}
      Ok(resp) => log::error!("恢复远端槽位 STABLE 失败: {resp}"),
      Err(err) if is_timeout_err(&err) => log::error!("恢复远端槽位 STABLE 超时"),
      Err(err) => log::error!("恢复远端槽位 STABLE 失败: {err}"),
    }
  }
  if transfer == TransferOption::Slots {
    session.reset_local_slot();
  }
  *session.status.write() = MigrateState::Fail;
  dispose_migration(client, session);
}

/// 单批载荷发送 + 停等 ACK：对标
/// libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:HandleMigrateTaskResponseAsync
/// （`WaitAsync(_timeout)` + 非 OK 判败）；空载荷即完成哨兵帧 (recordCount = 0)
///
/// 头显式携带槽位集（库级定槽 doc/zh/db.md 4.1：接收端改头级判槽，C# 头无
/// 此参数、接收端逐键 HashSlot 探测随键级哈希废除）
pub async fn send_payload_and_wait(
  client: &GarnetClient,
  session: &MigrateSession,
  dur: Option<Duration>,
  spec: &MigrateTaskSpec,
  payload: &[u8],
) -> Result<()> {
  // 槽集渲染为逗号分隔十进制（i32 全非负，HashSet 遍历顺序不影响语义）
  let mut slot_list = String::new();
  let mut buf = Buffer::new();
  for (i, slot) in session.get_slots().iter().enumerate() {
    if i > 0 {
      slot_list.push(',');
    }
    slot_list.push_str(buf.format(*slot));
  }
  match wait_remote(
    dur,
    session,
    client.execute_cluster_migrate_async(
      // 协议帧参数：节点 id 仅在跨节点命令面渲染 hex
      &hex_str_u128(spec.source_node_id),
      spec.replace_option,
      &slot_list,
      payload,
    ),
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
pub(crate) async fn set_slot_ranges_checked(
  client: &GarnetClient,
  session: &MigrateSession,
  dur: Option<Duration>,
  state: &str,
  ranges: &[(i32, i32)],
  node_id: Option<u128>,
) -> Result<()> {
  for &(start, end) in ranges {
    match wait_remote(
      dur,
      session,
      // 协议帧参数：SETSLOT <state> <node-id hex> 仅在命令面渲染
      client.set_slot_range_async(state, start, end, node_id.map(hex_str_u128).as_deref()),
    )
    .await
    {
      Ok(resp) if resp == "OK" => {}
      Ok(resp) => {
        return Err(Error::InvalidArgument(format!(
          "远端 SETSLOTSRANGE {state} 失败: {resp}"
        )));
      }
      Err(err) => {
        return Err(Error::InvalidArgument(format!(
          "远端 SETSLOTSRANGE {state} 失败: {err}"
        )));
      }
    }
  }
  Ok(())
}

/// 迁移目标客户端构造（用户名/口令非空才携带，对标 MigrateSession.cs:
/// GetGarnetClient 的 authUsername/authPassword 透传；迁移网络缓冲池同池
/// 注入，对标 GetGarnetClient 的 `networkPool: GetNetworkPool` 形参，经
/// migrationManager.GetNetworkPool 取 manager 持有池跨连接复用；出站 TLS
/// 单源透传，对标 MigrateSession.cs:172
/// `clusterProvider?.serverOptions.TlsOptions?.TlsClientOptions` 形参位）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSession.cs:GetGarnetClient
pub fn connect_migrate_client(
  spec: &MigrateTaskSpec,
  session: &Arc<MigrateSession>,
  #[cfg(feature = "tls")] tls: Option<&Arc<ClientTlsConfig>>,
) -> GarnetClient {
  let mut client = GarnetClient::with_auth(
    format!("{}:{}", spec.target_address, spec.target_port),
    (!spec.username.is_empty()).then(|| spec.username.to_string()),
    (!spec.passwd.is_empty()).then(|| spec.passwd.to_string()),
  );
  // 迁移 manager 未装配（理论上迁移会话必经装配态）即 None，建连走自建回退
  client.set_network_pool(
    session
      .cluster_provider
      .migration_manager()
      .map(|mm| mm.network_pool()),
  );
  #[cfg(feature = "tls")]
  client.set_tls(tls.cloned());
  client
}

/// 迁移前置编排（编排顺序对标 MigrationDriver 的 BeginAsyncMigrationTaskAsync
/// IMPORT / MIGRATING / 纪元转换段）：建立连接 → 远端置 IMPORTING →
/// 本端置 MIGRATING →（SLOTS 链）纪元转换等待；任一失败点统一 recover
/// （按 `transfer` 形态传导本端回退口径）。KEYS 链本端经
/// ensure_local_prepared_for_migration 容忍已手工前置的 MIGRATING 不再翻转
/// （C# MigrateKeysAsync 不触碰本端槽位状态）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSession.cs:CheckConnectionAsync
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionKeyAccess.cs:WaitForConfigPropagationAsync
pub(crate) async fn begin_migration_phase(
  client: &GarnetClient,
  session: &MigrateSession,
  ranges: &[(i32, i32)],
  dur: Option<Duration>,
  source_node_id: u128,
  epoch_gate: bool,
  transfer: TransferOption,
) -> Result<()> {
  let _ = client.connect_async().await;
  if !client.is_connected() {
    if transfer == TransferOption::Slots {
      session.reset_local_slot();
    }
    return Err(Error::Io(io::Error::new(
      ErrorKind::ConnectionRefused,
      "无法连接迁移目标节点",
    )));
  }

  // 远端置槽位 IMPORTING（停等限时，失败 → recover）
  if let Err(err) = set_slot_ranges_checked(
    client,
    session,
    dur,
    MigrateState::Import.as_slot_state_str(),
    ranges,
    Some(source_node_id),
  )
  .await
  {
    recover_and_fail!(
      client,
      session,
      ranges,
      dur,
      transfer,
      is_timeout_err(&err),
      &err.to_string(),
      err
    );
  }

  // 本端置槽位 MIGRATING（失败 → recover）
  let prepared = if transfer == TransferOption::Keys {
    session.ensure_local_prepared_for_migration()
  } else {
    session.try_prepare_local_for_migration()
  };
  if !prepared {
    recover_and_fail!(
      client,
      session,
      ranges,
      dur,
      transfer,
      "本端准备迁移槽位失败"
    );
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
    recover_and_fail!(
      client,
      session,
      ranges,
      dur,
      transfer,
      "迁移纪元转换等待失败"
    );
  }
  Ok(())
}

/// 迁移收尾编排（SLOTS 臂编排顺序对标 MigrationDriver 的 BeginAsyncMigrationTaskAsync
/// 完成哨兵 / SuspendConfigMerge + TryMeetAsync / NODE / RelinquishOwnership /
/// TryMeetAsync 段；KEYS 臂对标 TryStartMigrationTaskAsync 的 KEYS 臂——
/// MigrateKeysAsync 全程不动槽位，收尾仅发完成哨兵帧并校验应答，绝不移交
/// 槽属主，同槽未迁移键持续可源端访问，本端 MIGRATING 与远端收口归运维
/// CLUSTER SETSLOT，差异登记见模块头「槽位属主语义」）；任一失败点统一
/// recover（按 `transfer` 形态传导本端回退口径），配置合并挂起写锁为 RAII
/// 守卫，任一返回路径收口释放（对标 C# finally ResumeConfigMerge）
pub(crate) async fn end_migration_phase(
  client: &GarnetClient,
  session: &MigrateSession,
  ranges: &[(i32, i32)],
  dur: Option<Duration>,
  spec: &MigrateTaskSpec,
  transfer: TransferOption,
) -> Result<()> {
  // 完成哨兵空载荷帧：应答非 OK 即判败——吞没响应会让远端导入残缺而
  // 源端照常交权（对标 HandleMigrateTaskResponseAsync 应答校验）
  if let Err(err) =
    send_payload_and_wait(client, session, dur, spec, &encode_migration_payload(&[])).await
  {
    recover_and_fail!(
      client,
      session,
      ranges,
      dur,
      transfer,
      is_timeout_err(&err),
      &err.to_string(),
      err
    );
  }
  // KEYS 臂：按清单搬键，属主绝不移交（C# MigrateKeysAsync 无任何
  // SETSLOT/NODE/Relinquish 编排，NODE + Relinquish 属 SLOTS 链专属）。
  // 收尾仅哨兵帧即返回，成功不作任何槽位复位：本端 MIGRATING 与远端
  // IMPORTING 均为运维 SETSLOT 域，归运维 CLUSTER SETSLOT 收口（差异登记
  // 见模块头「槽位属主语义」）
  if transfer == TransferOption::Keys {
    return Ok(());
  }
  // SLOTS 臂：挂起配置合并并向目标 gossip 汇聚一次（对标
  // MigrationDriver.cs:188-191 SuspendConfigMerge + TryMeetAsync
  // acquireLock:false）：写锁挂起 gossip 周期合并，槽位交换期间配置视图
  // 冻结，防止后台纪元推进交错。窗口为异步写锁守卫，贯穿下方两次汇聚与
  // 各 await 持有（C# 同形态），等待合并的读锁方让出任务而非冻住本线程
  let cluster_mgr = session.cluster_provider.cluster_manager();
  let _merge_guard = match cluster_mgr.as_deref() {
    Some(cm) => Some(cm.suspend_config_merge().await),
    None => None,
  };
  meet_target_async(session, spec).await;
  // SLOTS 臂：远端置槽位 NODE（失败 → recover，对标 BeginAsyncMigrationTaskAsync NODE 分支）
  if let Err(err) = set_slot_ranges_checked(
    client,
    session,
    dur,
    MigrateState::Node.as_slot_state_str(),
    ranges,
    Some(spec.target_node_id),
  )
  .await
  {
    recover_and_fail!(
      client,
      session,
      ranges,
      dur,
      transfer,
      is_timeout_err(&err),
      &err.to_string(),
      err
    );
  }
  // SLOTS 臂：本端释放归属（失败 → recover，对标 BeginAsyncMigrationTaskAsync
  // RelinquishOwnership 分支）
  if !session.relinquish_ownership() {
    recover_and_fail!(
      client,
      session,
      ranges,
      dur,
      transfer,
      "本端释放槽位所有权失败"
    );
  }
  // 二次汇聚：源/目标就槽位交换达成一致后再放行配置合并（对标
  // MigrationDriver.cs:212 TryMeetAsync acquireLock:false）；挂起写锁随
  // 作用域收口释放（对标 finally ResumeConfigMerge）
  meet_target_async(session, spec).await;
  Ok(())
}

/// 向迁移目标发起一次 gossip 汇聚（对标 MigrationDriver.cs:191/:212
/// clusterManager.TryMeetAsync(_targetAddress, _targetPort, acquireLock:false)；
/// acquireLock:false 因调用方已持 suspend_config_merge 写锁）。汇聚为尽力
/// 而为：未装配 gossip 管理器或汇聚失败仅留痕不判败（C# TryMeetAsync 内部
/// 自吞异常不外抛）
pub(crate) async fn meet_target_async(session: &MigrateSession, spec: &MigrateTaskSpec) {
  let Some(gm) = session.cluster_provider.gossip_manager() else {
    return;
  };
  if let Err(err) = gm
    .try_meet_async(&spec.target_address, spec.target_port, false)
    .await
  {
    log::error!(
      "迁移收尾 gossip 汇聚 {}:{} 失败: {err}",
      spec.target_address,
      spec.target_port
    );
  }
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
/// 为返回值 transferred 清单（由调用方 DELETING 臂消费，见 keys.rs
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
      LiveValue::Migratable(val, ttl_ms) => {
        let item = BatchItem {
          key: key_ref,
          val,
          expire_unix_ms: ttl_ms,
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
/// 严格停等架构（全部远端 await 经 [`wait_remote`] 限时并联控会话取消
/// 令牌，目标挂起不至永挂、dispose 即时收敛）：
/// 1. 暂不支持键预检（零副作用拒绝）→ 提取槽位 + sketch 收录 → 注册迁移任务
/// 2. 前置编排 [`begin_migration_phase`]：远端 IMPORTING（自动下发的显式
///    文档差异）+ 本端 MIGRATING 容忍
/// 3. sketch 切 TRANSMITTING，逐批装帧发送 CLUSTER MIGRATE 停等 ACK；
///    wbftree 页存储集合键（RangeIndex 与升阶分层集合）逐键快照分块、
///    向量集键帧传输，均走带外通道且不触碰外层 sketch（门控外置）
/// 4. 收尾编排 [`end_migration_phase`]：仅哨兵（KEYS 链不动槽位，绝不移交
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

  // 3. 执行 + finally 移除任务
  let res = execute_keys_migration(&store, &spec, &session, keys).await;
  migration_mgr.try_remove_migration_task_session(Arc::clone(&session));
  res
}

/// KEYS 驱动执行体（任务注册后调用；失败统一 recover，见各编排函数）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionKeys.cs:DeleteKeysAsync
/// （此处为 DELETING 分相编排：仅删「已确认 ACK」键，MIGRATED 释放等待后归位）
async fn execute_keys_migration(
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
      "迁移纪元转换等待失败"
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
    let vs_res = match vm.as_deref() {
      Some(vm) => {
        transmit_vector_set_frames(
          store,
          &client,
          vm,
          &vector_sets,
          max_chunk,
          || session.is_cancelled(),
          async |payload| send_payload_and_wait(&client, session, dur, spec, payload).await,
        )
        .await
      }
      None => {
        log::error!("向量集迁移需向量集合管理器装配，本次迁移拒绝");
        Ok(false)
      }
    };
    match vs_res {
      Ok(true) => migrated_count += vector_sets.len(),
      Ok(false) => {
        let err = Error::InvalidArgument("向量集迁移失败".into());
        recover_and_fail!(
          &client,
          session,
          &ranges,
          dur,
          TransferOption::Keys,
          false,
          "向量集迁移失败",
          err
        );
      }
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
    }
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
    match tree_res {
      Ok(true) => {
        migrated_count += tree_keys.len();
      }
      Ok(false) => {
        let err = Error::InvalidArgument("带外树流迁移失败".into());
        recover_and_fail!(
          &client,
          session,
          &ranges,
          dur,
          TransferOption::Keys,
          false,
          "带外树流迁移失败",
          err
        );
      }
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
    }
  }

  end_migration_phase(&client, session, &ranges, dur, spec, TransferOption::Keys).await?;

  // 非 copy：删除「已确认传输成功」的键——未传输键（中途失败键/竞态改写
  // 键）一律保留在源端，杜绝静默丢键；DELETING 门控读写全等待
  // （对标 DeleteKeysAsync 的 DELETING → 删除 → MIGRATED 单点收口：wbftree
  // 带外流键并入 transferred 删除清单、向量集键携源端索引随收口登记清理，
  // 外层 sketch 全程驻留，键门对全部已迁键无空洞。C#
  // MigrateOperation.DeleteKeys 的 KEYS 分支即本臂，其映射锚持于
  // slots.rs 的 DELETING 臂；本臂另有向量集删件对位，映射锚持于
  // migrate_session_vector_set.rs）
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
        "迁移纪元转换等待失败"
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
    if let Some(vm) = vm.as_deref() {
      // 源端删除按复合键直删（含源端会话域，与枚举产物同域）
      for (rk, src_index) in &vector_sets {
        vm.delete_migrated_vector_set_of(rk, src_index).await;
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

/// 发送缓冲内容上限读取：委派 cluster_provider 单点真源（迁移/无盘同源，
/// migration_manager 未装配时回退同源派生缺省）
pub(crate) fn max_chunk_of(session: &MigrateSession) -> usize {
  session.cluster_provider.max_send_buffer_content_size()
}
