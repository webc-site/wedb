//! 集群纪元推进等待的并发回归测试
//!
//! 对标 garnet/libs/cluster/Server/ClusterProvider.cs:BumpAndWaitForEpochTransitionAsync
//! 的静止等待语义：活跃集群会话表为 papaya 无锁并发表（对标
//! GarnetServerBase.activeHandlers 的 ConcurrentDictionary），纪元自旋枚举
//! 不排阻会话注册，死弱引用枚举时自清扫免注销钩子。

use std::{sync::Arc, thread::spawn};

use wedb::server::{
  cluster::IClusterProvider, cluster_provider::ClusterProvider, cluster_session::ClusterSession,
};

/// 批外空闲会话（纪元快照 0）放行：注册会话不阻塞追平；会话亡后死弱
/// 引用 upgrade 失败被跳过并自清扫，同样放行
#[test]
fn bump_waits_pass_with_registered_and_dropped_sessions() {
  let cp = ClusterProvider::new();
  let session: Arc<ClusterSession> = cp.create_cluster_session();
  assert!(cp.bump_and_wait_for_epoch_transition());
  drop(session);
  assert!(cp.bump_and_wait_for_epoch_transition());
}

/// 纪元自旋枚举与并发会话注册互不阻塞：注册方与静止等待方多线程同时
/// 推进，全部完成且纪元追平（写锁形态下注册方会与每轮自旋互斥排队）
#[test]
fn bump_wait_does_not_block_session_registration() {
  let cp = Arc::new(ClusterProvider::new());
  cp.set_cluster_node_timeout_ms(2000);
  cp.bump_current_epoch();

  let mut joins = Vec::new();
  for _ in 0..4 {
    let cp = Arc::clone(&cp);
    joins.push(spawn(move || {
      for _ in 0..200 {
        let _session: Arc<ClusterSession> = cp.create_cluster_session();
      }
    }));
  }
  let waiter = {
    let cp = Arc::clone(&cp);
    spawn(move || cp.bump_and_wait_for_epoch_transition())
  };
  for j in joins {
    j.join().unwrap();
  }
  assert!(waiter.join().unwrap());
}

/// 集群节点超时单点映射（next 条4）：默认 60 秒
/// （GarnetServerOptions.cs:251 ClusterTimeout）、0 = 无限哨兵 None
/// （RuntimeServerConfig.cs:314-321 非正值 → Timeout.InfiniteTimeSpan）、
/// 正值毫秒时长；无限槽位下纪元静止等待空闲放行，不得退化为 0ms 即超时
#[test]
fn cluster_node_timeout_zero_sentinel_is_infinite() {
  use std::time::Duration;
  let cp = ClusterProvider::new();
  assert_eq!(cp.cluster_node_timeout(), Some(Duration::from_secs(60)));
  cp.set_cluster_node_timeout_ms(0);
  assert_eq!(cp.cluster_node_timeout(), None);
  assert!(cp.bump_and_wait_for_epoch_transition());
  cp.set_cluster_node_timeout_ms(1500);
  assert_eq!(cp.cluster_node_timeout(), Some(Duration::from_millis(1500)));
}
