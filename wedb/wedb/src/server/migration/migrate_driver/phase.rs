//! 迁移停等原语与前置/收尾编排
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs
//! （CompletePending / RetryAsync / HandleMigrateTaskResponseAsync 停等原语）
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs
//! （TryRecoverFromFailureAsync / TrySetSlotRangesAsync 与 Begin/End 编排段）
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSession.cs
//! （GetGarnetClient / CheckConnectionAsync 客户端与会话编排）

use std::{future::Future, io, io::ErrorKind, pin::pin, sync::Arc, time::Duration};

use coarsetime::Instant;
use compio::time::timeout;
use itoa::Buffer;
use wbase::hex::hex_str_u128;
use wconn::record::encode_migration_payload;
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;

use super::recover_and_fail;
use crate::{
  client::GarnetClient,
  error::{Error, Result},
  server::migration::{
    migrate_session::{MigrateSession, MigrateTaskSpec},
    migrate_state::{MigrateState, SlotStateStr},
    transfer_option::TransferOption,
  },
};

/// 停等等待的取消轮询切片：等待期间周期性检查会话取消令牌，dispose 触发
/// 后在途停等即时收敛（对标 C# `WaitAsync(_timeout, _cts.Token)` 的令牌联动）
const CANCEL_POLL_SLICE: Duration = Duration::from_millis(25);

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
/// timeout 参数经 [`super::keys::wait_dur`] 的三态映射，令牌即会话取消令牌）。
/// 任一远端 await 限时或被 [`MigrateSession::dispose`] 取消，目标挂起不至任务永挂；
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
      client.set_slot_range_async(SlotStateStr::Stable.as_str(), start, end, None),
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
    None,
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
    SlotStateStr::Import.as_str(),
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
    SlotStateStr::Node.as_str(),
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
