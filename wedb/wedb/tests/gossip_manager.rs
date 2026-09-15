//! gossip 管理器集成测试：配置演化增量判定、MEET 应答验证与失败清理、
//! 首轮 MEET（对标 Gossip.cs TryStartGossipTasks / TryMeetAsync 与
//! GarnetServerNode.GetMostRecentConfig）
use std::{sync::Arc, time::Duration};

use aok::Void;
use compio::runtime::Runtime;
use wedb::server::{
  cluster_provider::ClusterProvider,
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::{GossipNode, wait_for};

/// 初始化本地 worker 的提供者（worker 1 = local）
fn provider_with_local(node_id: &str, port: i32) -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  if let Some(cm) = cp.cluster_manager() {
    cm.init_local("127.0.0.1", port, false);
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

/// libs/cluster/Server/Gossip/Gossip.cs:FlushConfig 调用域（配置演化统一出口）
///
/// flush_config 每次调用递增 config_version；无变化的 merge 不递增
#[test]
fn test_config_version_increments_on_config_evolution() -> Void {
  Runtime::new().unwrap().block_on(async {
    let cp = provider_with_local("local-node", 7001);
    let cm = cp.cluster_manager().unwrap();
    let v0 = cm.config_version();

    // 本地 epoch 自增：配置演化 → 版本递增
    assert!(cm.try_bump_cluster_epoch());
    let v1 = cm.config_version();
    assert!(v1 > v0, "配置演化后版本应递增: {v0} -> {v1}");

    // merge 无变化（自身配置原样合并）不递增版本
    let config = cm.current_config().clone();
    assert!(!cm.try_merge(&config, true));
    assert_eq!(cm.config_version(), v1);

    // merge 带来新 worker 信息（本地 epoch 未变）也必须递增版本
    let peer = provider_with_local("gossip-peer", 7002);
    let peer_config = peer.cluster_manager().unwrap().current_config().clone();
    assert!(cm.try_merge(&peer_config, true));
    assert!(cm.config_version() > v1);
    aok::OK
  })
}

/// libs/cluster/Server/Gossip/GarnetServerNode.cs:GetMostRecentConfig
///
/// 配置未演化时空包 ping（empty_send），配置演化后（config_version 递增）
/// 下一轮发全量（full_send）——本地 epoch 不变的演化也必须触发
#[test]
fn test_gossip_incremental_judgement_by_config_version() -> Void {
  Runtime::new().unwrap().block_on(async {
    let fake = GossipNode::bind(Arc::new(Vec::new())).await; // 空应答靶端
    let cp = provider_with_local("local-node", 7001);
    cp.set_gossip_delay_ms(500);
    let cm = cp.cluster_manager().unwrap();
    let gm = cp.gossip_manager().unwrap();

    gm.connection_store
      .get_or_add("gossip-peer", "127.0.0.1", fake.port() as i32);

    // 第 1 轮：首发的全量
    gm.broadcast_gossip_async().await;
    assert_eq!(
      gm.stats
        .gossip_full_send
        .load(std::sync::atomic::Ordering::Acquire),
      1
    );
    assert_eq!(
      gm.stats
        .gossip_empty_send
        .load(std::sync::atomic::Ordering::Acquire),
      0
    );

    // 第 2 轮：配置未演化 → 空包 ping
    gm.broadcast_gossip_async().await;
    assert_eq!(
      gm.stats
        .gossip_full_send
        .load(std::sync::atomic::Ordering::Acquire),
      1
    );
    assert_eq!(
      gm.stats
        .gossip_empty_send
        .load(std::sync::atomic::Ordering::Acquire),
      1
    );

    // 配置演化（bump epoch，本地 epoch 计数变化前旧行为判不出发散）→ 第 3 轮必须再发全量
    assert!(cm.try_bump_cluster_epoch());
    gm.broadcast_gossip_async().await;
    assert_eq!(
      gm.stats
        .gossip_full_send
        .load(std::sync::atomic::Ordering::Acquire),
      2
    );
    aok::OK
  })
}

/// libs/cluster/Server/Gossip/Gossip.cs:TryMeetAsync 空应答分支
///
/// 空应答不计成败，created 临时连接必须回收（不残留被广播遍历）
#[test]
fn test_meet_empty_reply_reclaims_temp_connection() -> Void {
  Runtime::new().unwrap().block_on(async {
    let fake = GossipNode::bind(Arc::new(Vec::new())).await;
    let cp = provider_with_local("local-node", 7001);
    let gm = cp.gossip_manager().unwrap();

    assert!(
      gm.try_meet_async("127.0.0.1", fake.port() as i32)
        .await
        .is_ok()
    );
    assert_eq!(gm.connection_store.count(), 0, "空应答后临时连接应被回收");
    aok::OK
  })
}

/// libs/cluster/Server/Gossip/Gossip.cs:TryMeetAsync 版本校验分支
///
/// 应答线格式版本不兼容：拒绝反序列化、记失败、回收 created 临时连接
#[test]
fn test_meet_incompatible_version_reclaims_temp_connection() -> Void {
  Runtime::new().unwrap().block_on(async {
    let fake = GossipNode::bind(Arc::new(vec![0xff, 0x00, 0x01])).await;
    let cp = provider_with_local("local-node", 7001);
    let gm = cp.gossip_manager().unwrap();

    assert!(
      gm.try_meet_async("127.0.0.1", fake.port() as i32)
        .await
        .is_err()
    );
    assert_eq!(
      gm.connection_store.count(),
      0,
      "版本不兼容后临时连接应被回收"
    );
    assert!(
      gm.stats
        .meet_requests_failed
        .load(std::sync::atomic::Ordering::Acquire)
        >= 1,
      "版本不兼容应记 meet 失败"
    );
    aok::OK
  })
}

/// libs/cluster/Server/Gossip/Gossip.cs:TryMeetAsync 成功分支
///
/// 成功应答：merge 对方配置入本地、created 连接以正式 nodeId 交接入库；
/// 本地 epoch 不变的新节点信息也进入配置（版本号随之递增）
#[test]
fn test_meet_success_merges_and_hands_off_connection() -> Void {
  Runtime::new().unwrap().block_on(async {
    let peer = provider_with_local("gossip-peer", 7002);
    let reply = peer
      .cluster_manager()
      .unwrap()
      .current_config()
      .to_byte_array();
    let fake = GossipNode::bind(Arc::new(reply)).await;
    let cp = provider_with_local("local-node", 7001);
    let cm = cp.cluster_manager().unwrap();
    let gm = cp.gossip_manager().unwrap();
    let version_before = cm.config_version();

    assert!(
      gm.try_meet_async("127.0.0.1", fake.port() as i32)
        .await
        .is_ok()
    );
    assert_eq!(
      gm.connection_store.count(),
      1,
      "成功后应以正式 nodeId 持有一条连接"
    );
    assert!(gm.connection_store.get_connection("gossip-peer").is_some());
    {
      let config = cm.current_config();
      assert!(config.is_known("gossip-peer"), "merge 应带入对方节点");
    }
    assert!(
      cm.config_version() > version_before,
      "merge 带来新节点后配置版本应递增"
    );
    aok::OK
  })
}

/// libs/cluster/Server/Gossip/Gossip.cs:TryStartGossipTasks
///
/// start 对已恢复配置的全部已知 worker 先跑一轮 MEET：连接以正式 nodeId
/// 入库且 gossip 主循环点亮
#[test]
fn test_start_runs_initial_meet_round() -> Void {
  Runtime::new().unwrap().block_on(async {
    let peer = provider_with_local("gossip-peer", 7002);
    let reply = peer
      .cluster_manager()
      .unwrap()
      .current_config()
      .to_byte_array();
    let fake = GossipNode::bind(Arc::new(reply)).await;

    let cp = provider_with_local("local-node", 7001);
    let cm = cp.cluster_manager().unwrap();
    // 预置已恢复配置中的远端 worker（地址指向靶端）
    {
      let mut config = cm.current_config.write();
      config.workers.push(Worker {
        nodeid: Some("gossip-peer".into()),
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
      || gm.connection_store.get_connection("gossip-peer").is_some(),
      Duration::from_secs(5),
    )
    .await;
    assert!(ok, "首轮 MEET 应完成连接交接入库");
    gm.dispose();
    aok::OK
  })
}
