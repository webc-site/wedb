//! gossip 管理器集成测试：配置演化增量判定、MEET 应答验证与失败清理、
//! 首轮 MEET、广播派发并发隔离（对标 Gossip.cs TryStartGossipTasks /
//! TryMeetAsync、GarnetServerNode.GetMostRecentConfig / TryGossip）
use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use aok::Void;
use compio::{
  runtime::{Runtime, spawn},
  time::timeout,
};
use wedb::server::{
  cluster_provider::ClusterProvider,
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wtest_base::{GossipNode, wait_for};

/// 初始化本地 worker 的提供者（worker 1 = local）
fn provider_with_local(node_id: u128, port: i32) -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  if let Some(cm) = cp.cluster_manager() {
    cm.init_local("127.0.0.1", port, false, "");
    let mut config = cm.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id,
      address: "127.0.0.1",
      port,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
  }
  cp
}

/// 对标 Gossip.cs 的 FlushConfig 调用域（配置演化统一出口）
///
/// flush_config 每次调用递增 config_version；无变化的 merge 不递增
#[test]
fn test_config_version_increments_on_config_evolution() -> Void {
  Runtime::new().unwrap().block_on(async {
    let cp = provider_with_local(0x10CA1, 7001);
    let cm = cp.cluster_manager().unwrap();
    let v0 = cm.config_version();

    // 本地 epoch 自增：配置演化 → 版本递增
    assert!(cm.try_bump_cluster_epoch());
    let v1 = cm.config_version();
    assert!(v1 > v0, "配置演化后版本应递增: {v0} -> {v1}");

    // merge 无变化（自身配置原样合并）不递增版本
    let config = cm.current_config().clone();
    assert!(!cm.try_merge(&config, true).await);
    assert_eq!(cm.config_version(), v1);

    // merge 带来新 worker 信息（本地 epoch 未变）也必须递增版本
    let peer = provider_with_local(0x2E71, 7002);
    let peer_config = peer.cluster_manager().unwrap().current_config().clone();
    assert!(cm.try_merge(&peer_config, true).await);
    assert!(cm.config_version() > v1);
    aok::OK
  })
}

/// 对标 GarnetServerNode.cs 的 GetMostRecentConfig
///
/// 配置未演化时空包 ping（empty_send），配置演化后（config_version 递增）
/// 下一轮发全量（full_send）——本地 epoch 不变的演化也必须触发。
/// 广播派发为非阻塞 spawn，每轮断言前等待派发任务收尾（in_flight 复位）
#[test]
fn test_gossip_incremental_judgement_by_config_version() -> Void {
  Runtime::new().unwrap().block_on(async {
    let fake = GossipNode::bind(Arc::new(Vec::new())).await; // 空应答靶端
    let cp = provider_with_local(0x10CA1, 7001);
    cp.set_gossip_delay_ms(500);
    let cm = cp.cluster_manager().unwrap();
    let gm = cp.gossip_manager().unwrap();

    gm.connection_store
      .get_or_add(0x2E71, "127.0.0.1", fake.port() as i32, &cp);

    // 派发任务收尾等待：健康靶端毫秒级应答，1s 足够宽裕
    let settle = || async {
      let conn = gm.connection_store.get_connection(0x2E71).unwrap();
      assert!(
        wait_for(
          || !conn.gossip_in_flight.load(Ordering::Acquire),
          Duration::from_secs(1)
        )
        .await
      );
    };

    // 第 1 轮：首发的全量
    gm.broadcast_gossip_send();
    // 派发面计数同步可见（对标 Gossip.cs:455：派发成功即计，不等应答）
    assert_eq!(gm.stats.gossip_success_count.load(Ordering::Acquire), 1);
    settle().await;
    assert_eq!(gm.stats.gossip_full_send.load(Ordering::Acquire), 1);
    assert_eq!(gm.stats.gossip_empty_send.load(Ordering::Acquire), 0);

    // 第 2 轮：配置未演化 → 空包 ping
    gm.broadcast_gossip_send();
    settle().await;
    assert_eq!(gm.stats.gossip_full_send.load(Ordering::Acquire), 1);
    assert_eq!(gm.stats.gossip_empty_send.load(Ordering::Acquire), 1);

    // 配置演化（bump epoch，本地 epoch 计数变化前旧行为判不出发散）→ 第 3 轮必须再发全量
    assert!(cm.try_bump_cluster_epoch());
    gm.broadcast_gossip_send();
    settle().await;
    assert_eq!(gm.stats.gossip_full_send.load(Ordering::Acquire), 2);
    aok::OK
  })
}

/// 对标 Gossip.cs 的 TryMeetAsync 空应答分支
///
/// 空应答不计成败，created 临时连接必须回收（不残留被广播遍历）
#[test]
fn test_meet_empty_reply_reclaims_temp_connection() -> Void {
  Runtime::new().unwrap().block_on(async {
    let fake = GossipNode::bind(Arc::new(Vec::new())).await;
    let cp = provider_with_local(0x10CA1, 7001);
    let gm = cp.gossip_manager().unwrap();

    assert!(
      gm.try_meet_async("127.0.0.1", fake.port() as i32, true)
        .await
        .is_ok()
    );
    assert_eq!(gm.connection_store.count(), 0, "空应答后临时连接应被回收");
    aok::OK
  })
}

/// 对标 Gossip.cs 的 TryMeetAsync 版本校验分支
///
/// 应答线格式版本不兼容：拒绝反序列化、记失败、回收 created 临时连接
#[test]
fn test_meet_incompatible_version_reclaims_temp_connection() -> Void {
  Runtime::new().unwrap().block_on(async {
    let fake = GossipNode::bind(Arc::new(vec![0xff, 0x00, 0x01])).await;
    let cp = provider_with_local(0x10CA1, 7001);
    let gm = cp.gossip_manager().unwrap();

    assert!(
      gm.try_meet_async("127.0.0.1", fake.port() as i32, true)
        .await
        .is_err()
    );
    assert_eq!(
      gm.connection_store.count(),
      0,
      "版本不兼容后临时连接应被回收"
    );
    assert!(
      gm.stats.meet_requests_failed.load(Ordering::Acquire) >= 1,
      "版本不兼容应记 meet 失败"
    );
    aok::OK
  })
}

/// 对标 Gossip.cs 的 TryMeetAsync 成功分支
///
/// 成功应答：merge 对方配置入本地、created 连接以正式 nodeId 交接入库；
/// 本地 epoch 不变的新节点信息也进入配置（版本号随之递增）
#[test]
fn test_meet_success_merges_and_hands_off_connection() -> Void {
  Runtime::new().unwrap().block_on(async {
    let peer = provider_with_local(0x2E71, 7002);
    let reply = peer
      .cluster_manager()
      .unwrap()
      .current_config()
      .to_byte_array();
    let fake = GossipNode::bind(Arc::new(reply)).await;
    let cp = provider_with_local(0x10CA1, 7001);
    let cm = cp.cluster_manager().unwrap();
    let gm = cp.gossip_manager().unwrap();
    let version_before = cm.config_version();

    assert!(
      gm.try_meet_async("127.0.0.1", fake.port() as i32, true)
        .await
        .is_ok()
    );
    assert_eq!(
      gm.connection_store.count(),
      1,
      "成功后应以正式 nodeId 持有一条连接"
    );
    assert!(gm.connection_store.get_connection(0x2E71).is_some());
    {
      let config = cm.current_config();
      assert!(config.is_known(0x2E71), "merge 应带入对方节点");
    }
    assert!(
      cm.config_version() > version_before,
      "merge 带来新节点后配置版本应递增"
    );
    aok::OK
  })
}

/// 对标 Gossip.cs 的 GossipSampleSendAsync 选取防重
///
/// 抽样模式（percent<100）单轮选取多连接：派发即推进 last_send（对标
/// TryGossip 派发分支 UpdateGossipSend），同轮不会重复选中同一节点触发
/// CAS 失败误判——零超时计数、零连接移除
#[test]
fn test_sample_round_no_duplicate_pick_removal() -> Void {
  Runtime::new().unwrap().block_on(async {
    let fake = GossipNode::bind(Arc::new(Vec::new())).await; // 空应答靶端
    let cp = provider_with_local(0x10CA1, 7001);
    cp.set_gossip_sample_percent(67); // 3 连接 → ceil(3*0.67) = 3 次选取
    let gm = cp.gossip_manager().unwrap();

    for id in [0x11, 0x12, 0x13] {
      gm.connection_store
        .get_or_add(id, "127.0.0.1", fake.port() as i32, &cp);
    }

    gm.sample_gossip_send();

    // 同轮重复选中若存在，第二次 try_gossip 必 CAS 失败：修复前按超时
    // 计数并无条件 try_remove，健康连接被误删；修复后两处都必须为零
    assert_eq!(
      gm.stats.gossip_timeout_count.load(Ordering::Acquire),
      0,
      "同轮抽样不得产生超时误判"
    );
    assert_eq!(gm.connection_store.count(), 3, "健康连接不得被同轮重选误删");
    let picked = gm.stats.gossip_success_count.load(Ordering::Acquire);
    assert!(
      (1..=3).contains(&picked),
      "至少派发一次且至多每连接一次: {picked}"
    );

    // 已派发连接的任务收尾：健康靶端毫秒级应答，1s 足够宽裕
    for id in [0x11, 0x12, 0x13] {
      if let Some(conn) = gm.connection_store.get_connection(id)
        && conn.last_send.load(Ordering::Acquire) > 0
      {
        assert!(
          wait_for(
            || !conn.gossip_in_flight.load(Ordering::Acquire),
            Duration::from_secs(1)
          )
          .await,
          "{id} 派发任务应收尾"
        );
      }
    }
    aok::OK
  })
}

/// 对标 Gossip.cs 广播路径 TryGossip 返回 false 的超时摘除分支
///
/// 上一轮任务未完成（in_flight 保持置位）时 CAS 抢占失败：按
/// gossip_timeout_count 计超时并移除 store 中的当前实例——真实失联摘除
/// 语义不得因防误删收紧而丢失
#[test]
fn test_gossip_cas_timeout_removes_current_connection() -> Void {
  Runtime::new().unwrap().block_on(async {
    let fake = GossipNode::bind(Arc::new(Vec::new())).await;
    let cp = provider_with_local(0x10CA1, 7001);
    let gm = cp.gossip_manager().unwrap();

    let conn = gm
      .connection_store
      .get_or_add(0x2E71, "127.0.0.1", fake.port() as i32, &cp);
    // 模拟上一轮派发任务经 gossipDelay 仍未完成
    conn.gossip_in_flight.store(true, Ordering::Release);

    gm.broadcast_gossip_send();

    assert_eq!(
      gm.stats.gossip_timeout_count.load(Ordering::Acquire),
      1,
      "CAS 抢占失败应计超时"
    );
    assert_eq!(
      gm.stats.gossip_success_count.load(Ordering::Acquire),
      0,
      "抢占失败不得计派发成功"
    );
    assert_eq!(gm.connection_store.count(), 0, "真实失联连接应被摘除");
    aok::OK
  })
}

/// 对标 MigrationDriver.cs:188-191 与 ClusterManagerWorkerState.cs:103-144
/// （SuspendConfigMerge 贯穿 await TryMeetAsync(acquireLock:false)）：挂起窗
/// 口为异步感知读写锁（C# Garnet.common.ReaderWriterLock 的 SemaphoreSlim
/// 读写门同形态），持锁跨 await 即被测语义。判式：① 窗口内 acquire_lock=
/// false 的直连 merge 即时完成（形参被忽略而取读锁即同任务自死锁，旧实现
/// 硬编码 true 正落于此）；② 窗口内后台 acquire_lock=true 的 merge 以 await
/// 让出且不得越窗取得读锁（同步锁下此处整线程冻死）；③ 窗口内
/// acquire_lock=false 的 meet 汇聚成功、恰计一次、配置版本推进；④ 窗口收口
/// 后读侧立即放行（丢唤醒即超时判败）
#[test]
fn test_meet_without_lock_succeeds_while_merge_suspended() -> Void {
  Runtime::new().unwrap().block_on(async {
    let peer = provider_with_local(0x2E71, 7002);
    let reply = peer
      .cluster_manager()
      .unwrap()
      .current_config()
      .to_byte_array();
    let fake = GossipNode::bind(Arc::new(reply)).await;
    let cp = provider_with_local(0x10CA1, 7001);
    let cm = cp.cluster_manager().unwrap();
    let gm = cp.gossip_manager().unwrap();
    let version_before = cm.config_version();
    let self_snapshot = cm.current_config().clone();

    // 挂起写锁贯穿下方全部 await（C# WriteLock 同样贯穿 TryMeetAsync），
    // 死锁即 timeout 判败
    let window = cm.suspend_config_merge().await;

    // ① 窗口内同任务直连 merge 走 false 路径即时返回（false 不触碰
    // active_merge_lock；若取读锁即同任务自死锁卡死于此）。无变化的自身配置
    // 合并返回 false 且版本不动，不扰动后续判定
    let merged = timeout(Duration::from_secs(10), cm.try_merge(&self_snapshot, false))
      .await
      .expect("挂起窗口内 acquire_lock=false 的 merge 应即时完成，不得死锁");
    assert!(!merged, "自身旧快照合并无变化，不得扰动后续判定");

    // ② 后台带锁合并（读锁侧）在窗口内只能让出等待：让出后本任务方能继续
    // 推进汇聚，同步锁下此处整线程冻死；started 置位即该任务已停在读锁等待
    // 点（同一轮 poll 内同步推进到 await），故无需额外轮询
    let bg_cm = Arc::clone(&cm);
    let bg_config = self_snapshot.clone();
    let bg_started = Arc::new(AtomicBool::new(false));
    let bg = spawn({
      let bg_started = Arc::clone(&bg_started);
      async move {
        bg_started.store(true, Ordering::Release);
        bg_cm.try_merge(&bg_config, true).await;
      }
    });
    assert!(
      wait_for(
        || bg_started.load(Ordering::Acquire),
        Duration::from_secs(5)
      )
      .await,
      "后台合并任务未被调度"
    );
    assert!(!bg.is_finished(), "挂起窗口未生效：写锁收口前即取得读锁");

    // ③ 汇聚：以 acquire_lock=false 的 meet（MigrationDriver 调用形态）
    // 成功合入对方配置，成功计数恰一次（不误计）
    let meet = gm.try_meet_async("127.0.0.1", fake.port() as i32, false);
    match timeout(Duration::from_secs(10), meet).await {
      Ok(res) => assert!(res.is_ok(), "挂起窗口内汇聚应成功: {res:?}"),
      Err(_) => panic!("汇聚在挂起写锁下死锁"),
    }
    assert_eq!(
      gm.stats.meet_requests_succeed.load(Ordering::Acquire),
      1,
      "汇聚成功应恰计一次，不得误计"
    );

    // 窗口收口（C# finally ResumeConfigMerge，rust 以守卫 drop 承载）
    drop(window);

    // ④ 收口后读侧立即放行：证等待方仅让出而非丢唤醒；后台任务内合并的是
    // 本节点旧快照，无新信息，其返回值不参与判定
    match timeout(Duration::from_secs(10), bg).await {
      Ok(res) => assert!(res.is_ok(), "后台合并任务应正常完成: {res:?}"),
      Err(_) => panic!("窗口收口后读锁未唤醒，挂起锁丢失唤醒"),
    }
    timeout(Duration::from_secs(10), cm.try_merge(&self_snapshot, true))
      .await
      .expect("窗口收口后 acquire_lock=true 的 merge 应恢复即时可用");

    {
      let config = cm.current_config();
      assert!(config.is_known(0x2E71), "merge 应带入对方节点");
    }
    assert!(
      cm.config_version() > version_before,
      "merge 带来新节点应递增配置版本"
    );
    aok::OK
  })
}

/// 对标 Gossip.cs 的 TryStartGossipTasks
///
/// start 对已恢复配置的全部已知 worker 先跑一轮 MEET：连接以正式 nodeId
/// 入库且 gossip 主循环点亮
#[test]
fn test_start_runs_initial_meet_round() -> Void {
  Runtime::new().unwrap().block_on(async {
    let peer = provider_with_local(0x2E71, 7002);
    let reply = peer
      .cluster_manager()
      .unwrap()
      .current_config()
      .to_byte_array();
    let fake = GossipNode::bind(Arc::new(reply)).await;

    let cp = provider_with_local(0x10CA1, 7001);
    let cm = cp.cluster_manager().unwrap();
    // 预置已恢复配置中的远端 worker（地址指向靶端）
    {
      let mut config = cm.current_config.write();
      config.workers.push(Worker {
        nodeid: Some(0x2E71),
        address: "127.0.0.1".into(),
        port: fake.port() as i32,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
    }

    let gm = cp.gossip_manager().unwrap();
    assert!(!gm.is_running());
    cm.start();
    assert!(gm.is_running(), "start 应点亮 gossip 主循环");

    let ok = wait_for(
      || gm.connection_store.get_connection(0x2E71).is_some(),
      Duration::from_secs(5),
    )
    .await;
    assert!(ok, "首轮 MEET 应完成连接交接入库");
    gm.dispose();
    aok::OK
  })
}
