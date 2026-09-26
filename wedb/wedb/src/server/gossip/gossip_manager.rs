use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{
  runtime::spawn,
  time::{sleep, timeout},
};
use log::{error, info, warn};
use wbase::{hex::hex_str_u128, supervise::supervise_task, time::now_ms};

/// 监督快照里的任务名（wbase::supervise 归组键，INFO bg_task_health 可见）
const GOSSIP_MAIN_TASK: &str = "gossip_main";

use crate::{
  error::{Error, Result},
  server::{
    cluster_config::{CLUSTER_CONFIG_VERSION, ClusterConfig},
    cluster_provider::ClusterProvider,
    gossip::{
      connection_store::ConnectionStore, gossip_stats::GossipStats, node_connection::NodeConnection,
    },
    wait_async,
  },
};

/// libs/cluster/Server/Gossip/Gossip.cs:ClusterManager
pub struct GossipManager {
  cluster_provider: Arc<ClusterProvider>,
  pub stats: Arc<GossipStats>,
  pub connection_store: Arc<ConnectionStore>,
  is_running: AtomicBool,
}

impl GossipManager {
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      stats: Arc::new(GossipStats::new()),
      connection_store: Arc::new(ConnectionStore::new()),
      is_running: AtomicBool::new(false),
    }
  }

  pub fn is_running(&self) -> bool {
    self.is_running.load(Ordering::Acquire)
  }

  /// gossip 周期（对标 clusterManager.gossipDelay =
  /// TimeSpan.FromSeconds(serverOptions.GossipDelay)，live 读装配槽位）
  #[inline]
  fn gossip_delay(&self) -> Duration {
    Duration::from_millis(self.cluster_provider.gossip_delay_ms())
  }

  /// gossip 抽样百分比（对标 ClusterManager.GossipSamplePercent）
  #[inline]
  fn gossip_sample_percent(&self) -> i32 {
    self.cluster_provider.gossip_sample_percent()
  }

  /// Gossip.cs TryStartGossipTasks 的实现体（C# 方法在 ClusterManager，
  /// 映射见 ClusterManager::try_start_gossip_tasks）
  ///
  /// 启动先对全部已知 worker 跑一轮 MEET（对标 RunMeetTask = Task.Run
  /// (TryMeetAsync)，恢复上线的节点据此尽快重入集群），再起 gossip 主循环。
  /// is_running 幂等 CAS 门把关全部启动入口（装配期 ClusterManager::Start
  /// 与 MEET / 入站 gossip 事件重拉）：存活轮次即拦零开销，panic 终局臂复位
  /// 门后由事件重拉放行
  pub fn start(self: &Arc<Self>) {
    if self.is_running.swap(true, Ordering::AcqRel) {
      return;
    }
    // 终局拆池后重拉必须先起池：上一轮终局（外部 dispose 或 panic Err 臂）
    // 已置 disposed 恒拒新增，不清位则重拉环面对死池（裁决码注见
    // ConnectionStore::revive）
    self.connection_store.revive();
    if let Some(cluster_mgr) = self.cluster_provider.cluster_manager() {
      let config = cluster_mgr.current_config();
      // worker id 自 2 起（0 保留 unassigned、1 本地），对标 for i = 2; i <= NumWorkers
      for worker_id in 2..=config.num_workers() {
        let (address, port) = config.get_worker_address(worker_id as u16);
        let this = Arc::clone(self);
        spawn(async move {
          if let Err(e) = this.try_meet_async(&address, port, true).await {
            warn!("Startup MEET {address}:{port} failed: {e:?}");
          }
        })
        .detach();
      }
    }
    let this = Arc::clone(self);
    spawn(async move {
      // 任务体经 wbase supervise_task 顶层监督（单点一次成型）：panic 臂以
      // dispose 单点收口——复位 is_running 幂等启动位 + 拆连接池，对标 C#
      // GossipMainAsync 终止必拆（Gossip.cs:369 finally →
      // GarnetClusterConnectionStore.Dispose 置 _disposed 恒拒新增并逐连接
      // Dispose），全部节点套接字随环死收口，杜绝「环死池漏」（publish/
      // flushall 扇出与 MEET 交接不再向死池塞入连接）；复位后 MEET 重入 /
      // gossip 入站链事件到达即经 try_start_gossip_tasks 重拉，start CAS 门
      // 放行、revive 起池后首轮 MEET 与 gossip_step 建连补员重建，杜绝
      // 「panic 后 is_running 恒真假装在跑、start() CAS 永拒重拉」的静默停摆
      if supervise_task(GOSSIP_MAIN_TASK, async {
        while this.is_running() {
          this.gossip_step_async().await;
          sleep(this.gossip_delay()).await;
        }
      })
      .await
      .is_err()
      {
        this.dispose();
      }
    })
    .detach();
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:TryMeetAsync
  ///
  /// 先按地址查已知 nodeId 复用池内现有连接（GetConnection），查不到才本地
  /// 新建未注册实例完成握手（对标 C# gsn 仅存局部变量，绝不以临时键预插
  /// 连接池）。created 语义：失败/超时由本调用 dispose 回收；成功后以应答
  /// nodeId 原位更新再 add_connection 移交连接池，交接后归 gossip 主循环
  /// 所有，握手好的 TCP 连接无缝复用。`acquire_lock` 透传 try_merge（C#
  /// TryMerge acquireLock 参数）：调用方已持 suspend_config_merge 挂起窗口
  /// 时必须传 false，否则同任务先写后读自死锁（异步读写锁同样不可重入）
  pub async fn try_meet_async(&self, address: &str, port: i32, acquire_lock: bool) -> Result<()> {
    self.stats.update_meet_requests_recv();
    let cluster_mgr = self
      .cluster_provider
      .cluster_manager()
      .ok_or_else(|| Error::Gossip("ClusterManager not initialized".into()))?;

    let config_bytes = {
      if let Some(rm) = self.cluster_provider.replication_manager() {
        cluster_mgr.lazy_update_local_replication_offset(rm.get_replication_offset(0));
      }
      let config = cluster_mgr.current_config();
      config.to_byte_array()
    };

    let known_id = {
      let config = cluster_mgr.current_config();
      config.get_worker_node_id_from_address(address, port)
    };
    let reused = known_id.and_then(|id| self.connection_store.get_connection(id));
    // 本地握手实例（对标 C# new GarnetServerNode 仅存局部变量）：入池前
    // node_id 占位 0，握手成功后原位更新为目标节点正式标识
    let mut local: Option<NodeConnection> = None;

    // meet 超时用集群节点超时（对标 GarnetServerNode.TryMeetAsync
    // WaitAsync(clusterTimeout)；0 = 无限，不挂计时器）
    // 应答验证（对标 Gossip.cs:196-205：先验线格式版本再反序列化；
    // Ok(None) = 空应答，不计成败）
    let validated: Result<Option<ClusterConfig>> = {
      let conn: &NodeConnection = match reused.as_ref() {
        Some(c) => c.as_ref(),
        None => local.get_or_insert(NodeConnection::new(
          0,
          address.to_string(),
          port,
          &self.cluster_provider,
        )),
      };
      match wait_async(
        self.cluster_provider.cluster_node_timeout(),
        conn.try_meet_async(&config_bytes, self.gossip_delay()),
      )
      .await
      {
        Some(Ok(resp)) if !resp.is_empty() => {
          if ClusterConfig::try_peek_version(&resp) != Some(CLUSTER_CONFIG_VERSION) {
            warn!("MEET response has incompatible config version from {address}:{port}");
            Err(Error::Gossip("incompatible config version".into()))
          } else {
            match ClusterConfig::from_byte_array(&resp) {
              Ok(other) => Ok(Some(other)),
              Err(e) => {
                warn!("MEET response deserialization failed from {address}:{port}: {e:?}");
                Err(e)
              }
            }
          }
        }
        Some(Ok(_)) => Ok(None),
        Some(Err(e)) => {
          error!("MEET {address}:{port} failed: {e:?}");
          Err(e)
        }
        None => {
          error!("MEET {address}:{port} timed out");
          Err(Error::Gossip(format!("MEET {address}:{port} timed out")))
        }
      }
    };

    match validated {
      Ok(Some(other)) => {
        let Some(target_id) = other.local_node_id() else {
          warn!("MEET response from {address}:{port} missing local node id");
          if let Some(gsn) = local.take() {
            gsn.dispose();
          }
          self.stats.update_meet_requests_failed();
          return Err(Error::Gossip(format!(
            "MEET {address}:{port} response missing node id"
          )));
        };

        info!(
          "MEET {} {address}:{port} successful",
          hex_str_u128(target_id)
        );
        // merge 返回值无需检查（meet 由管理员发起，节点可信；rust try_merge
        // 与 C# TryMerge 一致仍会检查 ban list，C# 原注释「Merge without a
        // check」指忽略返回值 _ = TryMerge(...)，并非跳过信任校验）
        cluster_mgr.try_merge(&other, acquire_lock).await;
        self.stats.update_meet_requests_succeed();

        // created 连接交接：node_id 原位更新为目标正式标识后整实例入池
        //（对标 gsn.NodeId = nodeId 后 AddConnectionAsync(gsn)，握手好的
        // TCP 连接无缝复用，绝不经 get_or_add 重建冷桩）；入库失败（并发
        // 同 nodeId 已入池或 store 已释放）→ 本方法 dispose 回收，入库后
        // 归 gossip 主循环所有。刻意差异声明：C# 对 reused 连接也无条件
        // 回写 NodeId，会使 connectionMap 键与实例 id 失配（随后
        // TryRemove 按 conn.NodeId 查表失配泄漏）；rust 保持键 = ID 不变式
        if let Some(mut gsn) = local.take() {
          gsn.node_id = target_id;
          let gsn = Arc::new(gsn);
          if !self.connection_store.add_connection(Arc::clone(&gsn)) {
            gsn.dispose();
          }
        }
        Ok(())
      }
      // 空应答：created 连接不入池即弃（C# 中未 Add 的 gsn 悬空由 GC 回收，
      // rust 侧显式 dispose 对齐确定性回收）
      Ok(None) => {
        if let Some(gsn) = local.take() {
          gsn.dispose();
        }
        Ok(())
      }
      Err(e) => {
        self.stats.update_meet_requests_failed();
        if let Some(gsn) = local.take() {
          gsn.dispose();
        }
        Err(e)
      }
    }
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:GossipMainAsync
  pub async fn gossip_step_async(&self) {
    let Some(cluster_mgr) = self.cluster_provider.cluster_manager() else {
      return;
    };

    // 1. 清理过期封禁节点连接并移除处于封禁中的连接（对标 C# DisposeBannedWorkerConnectionsAsync）
    cluster_mgr.cleanup_ban_list();
    {
      // 封禁表为无锁并发字典：一次迭代快照枚举在册节点 id（C#
      // DisposeBannedWorkerConnectionsAsync 的 `foreach (var w in
      // workerBanList)` 同形），逐连接摘除自身即单点原子操作，无需外层锁
      let ban_list = cluster_mgr.worker_ban_list.pin();
      for nid in ban_list.keys() {
        self.connection_store.try_remove(*nid);
      }
    }

    // 2. 同步当前已知的集群节点并建立连接
    //（对标 C# Gossip.cs:InitConnectionsAsync 经 GetWorkerInfoForGossip
    // 收集节点三元组；新建连接即刻 initialize_async，建连失败当轮即自连接池摘除）
    {
      let (local_id, workers) = {
        let config = cluster_mgr.current_config();
        (config.local_node_id(), config.get_worker_info_for_gossip())
      };
      for (nid, address, port) in workers {
        if !cluster_mgr.is_banned(nid) && local_id != Some(nid) {
          let (conn, is_new) =
            self
              .connection_store
              .get_or_add_entry(nid, &address, port, &self.cluster_provider);
          if is_new {
            conn.initialize_async(self.gossip_delay()).await;
            if !conn.is_connected() {
              self.connection_store.try_remove_if_current(nid, &conn);
            }
          }
        }
      }
    }

    // 3. 广播或者抽样 Gossip
    if self.gossip_sample_percent() >= 100 {
      self.broadcast_gossip_send();
    } else {
      self.sample_gossip_send();
    }
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:BroadcastGossipSendAsync
  ///
  /// 主循环只做非阻塞发起，瞬时完成（对标 C# TryGossip 不 await 收发）；
  /// 单节点挂起不影响本轮其余节点派发，挂起连接由下一轮 CAS 失败判超时移除
  ///
  /// 游标推进三态（对标 C# "成功 offset++、失败移除靠集合收缩下落"）：
  /// 派发成功 → 前进；失败但连接已被移除 → 原地不动等下落；失败且连接
  /// 仍在集合（cluster_manager 未就绪等不移除路径）→ 强制前进，杜绝
  /// 固定 offset 上同步自旋
  pub fn broadcast_gossip_send(&self) {
    let mut offset = 0;
    while let Some(conn) = self.connection_store.get_connection_at_offset(offset) {
      let sent = self.try_gossip(Arc::clone(&conn));
      if sent || self.connection_store.contains(conn.node_id) {
        offset += 1;
      }
    }
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:GossipSampleSendAsync
  ///
  /// 刻意差异声明（配额语义，行为不改）：C# 成功支（Gossip.cs:503-507）以
  /// continue 跳过循环尾 count--（Gossip.cs:520），成功不耗配额，终止靠
  /// Gossip.cs:496 的 currNode == null（成功节点 GossipSend 已推进过
  /// startTime 不再入选），全部成功时单轮净扇出可超出 fraction；此处采固定
  /// count 封顶，每轮扇出上界可预测，gossip 统计口径与 gossip_delay 周期解耦
  pub fn sample_gossip_send(&self) {
    let total = self.connection_store.count();
    if total == 0 {
      return;
    }
    let percent = self.gossip_sample_percent();
    let count = ((total as f64 * (percent as f64 / 100.0)).ceil() as usize).clamp(1, total);
    let start_time = now_ms() as i64;
    for _ in 0..count {
      let mut min_send = start_time;
      let mut curr_node = None;
      for _ in 0..3 {
        if let Some(conn) = self.connection_store.get_random_connection() {
          let send = conn.last_send.load(Ordering::Acquire);
          if send < min_send {
            min_send = send;
            curr_node = Some(conn);
          }
        }
      }
      match curr_node {
        Some(conn) => {
          self.try_gossip(conn);
        }
        None => break,
      }
    }
  }

  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:TryGossip
  ///
  /// 非阻塞发起单节点 gossip 探测：CAS 抢占 gossip_in_flight 成功即 spawn
  /// 独立任务派发（收发、应答 merge、失败清理都在任务内闭环），主循环瞬时
  /// 完成，各节点探测并发隔离；抢占成功即推进发送时间戳（对标 C# 派发分支
  /// 的 UpdateGossipSend：派发即刻新，同轮抽样不会重复选中同一节点）；
  /// 抢占失败即上一轮未完成（对标 C# gossipTask 非完成态轮询），记超时并
  /// 仅在连接仍是本实例时移除（try_remove_if_current，防止误删 ban 清理或
  /// MEET 流程重建的新连接，与任务内失败分支同一防误删语义）。本轮配置的取与
  /// 判（是否全量、序列化、版本比对）下沉至 conn.get_most_recent_config
  /// （对标 C# 派发分支调用 GarnetServerNode.GetMostRecentConfig），本函数只留
  /// 调度、统计与收发闭环
  fn try_gossip(&self, conn: Arc<NodeConnection>) -> bool {
    if conn
      .gossip_in_flight
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .is_err()
    {
      self
        .stats
        .gossip_timeout_count
        .fetch_add(1, Ordering::Relaxed);
      warn!(
        "GOSSIP to remote node {} timeout!",
        hex_str_u128(conn.node_id)
      );
      self
        .connection_store
        .try_remove_if_current(conn.node_id, &conn);
      return false;
    }

    // 集群管理器未就绪：拒绝派发且不移除连接（广播游标侧兜底强制推进）
    let Some(cluster_mgr) = self.cluster_provider.cluster_manager() else {
      conn.gossip_in_flight.store(false, Ordering::Release);
      return false;
    };

    let (config_bytes, config_version, is_full) = conn.get_most_recent_config(&cluster_mgr);

    if is_full {
      self.stats.gossip_full_send.fetch_add(1, Ordering::Relaxed);
      self
        .stats
        .update_gossip_bytes_send(config_bytes.len() as i64);
    } else {
      self.stats.gossip_empty_send.fetch_add(1, Ordering::Relaxed);
    }

    let store = Arc::clone(&self.connection_store);
    let stats = Arc::clone(&self.stats);
    let delay = Duration::from_millis(self.cluster_provider.gossip_delay_ms());
    // 派发即推进发送时间戳（对标 C# TryGossip 派发分支的 UpdateGossipSend，
    // 主循环同步推进、不依赖任务调度）：广播与抽样共用此口径，
    // GetConnectionInfo.ping 随派发如实刷新；抽样循环据此防同轮重复选中
    // （推进时刻 >= start_time，min_send 比较不再命中）
    conn.update_send_time();
    spawn(async move {
      match timeout(delay, conn.try_gossip_async(&config_bytes, delay)).await {
        Ok(Ok(resp)) => {
          conn
            .last_sent_config_version
            .store(config_version, Ordering::Relaxed);
          if !resp.is_empty() {
            stats.update_gossip_bytes_recv(resp.len() as i64);
            if ClusterConfig::try_peek_version(&resp) == Some(CLUSTER_CONFIG_VERSION) {
              if let Ok(other) = ClusterConfig::from_byte_array(&resp)
                && let Some(other_id) = other.local_node_id()
              {
                if cluster_mgr.current_config().is_known(other_id) {
                  cluster_mgr.try_merge(&other, true).await;
                } else {
                  warn!("Received gossip from unknown node: {other_id}");
                }
              }
            } else {
              warn!("Received gossip response with incompatible config version");
            }
          }
        }
        Ok(Err(e)) => {
          stats.gossip_failed_count.fetch_add(1, Ordering::Relaxed);
          warn!(
            "GOSSIP to remote node {} failed: {e:?}",
            hex_str_u128(conn.node_id)
          );
          store.try_remove_if_current(conn.node_id, &conn);
        }
        Err(_) => {
          stats.gossip_timeout_count.fetch_add(1, Ordering::Relaxed);
          warn!(
            "GOSSIP to remote node {} timeout!",
            hex_str_u128(conn.node_id)
          );
          store.try_remove_if_current(conn.node_id, &conn);
        }
      }
      // 复位放最后：先于 CAS(Acquire) 读侧建立 happens-before，
      // 保证下一轮派发能看到本任务的全部收尾写入
      conn.gossip_in_flight.store(false, Ordering::Release);
    })
    .detach();
    // 派发面计数（对标 Gossip.cs:455：TryGossip 返回 true 即记成功，
    // 不等应答；应答成败由 failed/timeout 单独统计）
    self
      .stats
      .gossip_success_count
      .fetch_add(1, Ordering::Relaxed);
    true
  }

  /// 终止 Gossip 轮询并断开所有节点连接（对标 C# GossipMainAsync finally 的
  /// 拆池收口：外部关停与主循环 panic 终局臂共用本单点，复位幂等启动位 +
  /// ConnectionStore::Dispose 恒拒新增；后续事件重拉经 start 的 CAS 门起池）
  pub fn dispose(&self) {
    self.is_running.store(false, Ordering::Release);
    self.connection_store.dispose();
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{Arc, atomic::Ordering};

  use super::GossipManager;
  use crate::server::{cluster_provider::ClusterProvider, gossip::node_connection::NodeConnection};

  /// CAS 失败分支防误删：已被移除的旧实例（上一轮任务仍飞行）抢占失败时，
  /// store 中同 node_id 重建的新连接不得被移除（与任务内 Ok(Err)/Err 分支
  /// 的 try_remove_if_current 同一防误删语义，对标 C# 摘除仅针对失联连接）
  #[test]
  fn cas_failure_keeps_rebuilt_connection() {
    let cp = ClusterProvider::new();
    let gm = GossipManager::new(Arc::clone(&cp));
    let node_id = 0xA11CE;
    let rebuilt = gm
      .connection_store
      .get_or_add(node_id, "127.0.0.1", 7000, &cp);

    // 旧实例：已从 store 移除、上一轮派发任务经 gossipDelay 仍未完成
    let stale = Arc::new(NodeConnection::new(node_id, "127.0.0.1".into(), 7000, &cp));
    stale.gossip_in_flight.store(true, Ordering::Release);

    assert!(!gm.try_gossip(Arc::clone(&stale)));
    assert_eq!(
      gm.stats.gossip_timeout_count.load(Ordering::Acquire),
      1,
      "旧实例抢占失败应计超时"
    );
    let cur = gm
      .connection_store
      .get_connection(node_id)
      .expect("重建连接不得被 CAS 失败路径误删");
    assert!(
      Arc::ptr_eq(&cur, &rebuilt),
      "store 中应仍是重建的新连接实例"
    );
  }
}
