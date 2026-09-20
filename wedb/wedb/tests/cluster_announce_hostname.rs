//! 集群宣告主机名链集成测试（对标 garnet/libs/cluster/Server/ClusterManager.cs:InitLocal
//! 的 hostname 宣告：`string.IsNullOrEmpty(hostname) ? Format.GetHostName() : hostname`）
//!
//! 覆盖三处落地：
//! 1. 装配链：`--cluster-announce-hostname` 配置值经 ClusterProvider::initialize_cluster_config
//!    → ClusterManager::init_local 落入本地位 Worker.hostname（配置优先）；
//!    配置为空则回退一次 OS 主机名（C# Format.GetHostName）。
//! 2. CLUSTER SHARDS：preferred=hostname 时端点吐宣告主机名而非 IP（ClusterConfig
//!    序列化臂 AppendFormattedNodeInfo）。
//! 3. -MOVED：preferred=hostname 时远端槽重定向吐被宣告节点的主机名而非 IP
//!    （GetEndpointByPreferredType / GetEndpointFromSlot）。

use std::{path::Path, sync::Arc};

use wbase::{hash_slot::CLUSTER_SLOT_COUNT, map::new_concurrent_map};
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::{ClusterConfig, ClusterPreferredEndpointType},
  cluster_provider::ClusterProvider,
  hash_slot::{HashSlot, SlotState},
  worker::LOCAL_WORKER_ID,
};

/// 节点 A 宣告主机名（纯 ASCII，序列化 endpoint 长度可据此推算）
const ANNOUNCED_A: &str = "host-a.example.test";
/// 节点 B 宣告主机名（-MOVED 目标节点）
const ANNOUNCED_B: &str = "host-b.example.test";
/// 划归远端节点 B 的槽位（会话重定向用例消费）
const REMOTE_SLOT: u16 = 7777;
const _: () = assert!(REMOTE_SLOT < CLUSTER_SLOT_COUNT as u16, "槽位须在有效区间");

/// 纯内存装配一个集群节点（刷盘频率 -1 → recover_config 恒 false，走首启新建臂），
/// `announce` 即 C# serverOptions.ClusterAnnounceHostname 的 rust 配置面
fn boot_node(dir: &Path, address: &str, port: i32, announce: &str) -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  cp.initialize_cluster_config(address, port, &dir.join("nodes.conf"), -1, false, announce)
    .expect("集群拓扑装配");
  cp
}

/// 取本地位 hostname 克隆（避免长期持锁）
fn local_hostname(cp: &ClusterProvider) -> Option<String> {
  let cm = cp.cluster_manager().expect("集群管理器已装配");
  cm.current_config().workers[LOCAL_WORKER_ID]
    .hostname
    .as_ref()
    .map(|h| h.to_string())
}

/// 配置优先：`--cluster-announce-hostname` 值落入本地位 Worker.hostname
#[test]
fn announce_hostname_config_lands_on_local_worker() {
  let dir = tempfile::tempdir().unwrap().keep();
  let cp = boot_node(&dir, "127.0.0.1", 7300, ANNOUNCED_A);
  assert_eq!(
    local_hostname(&cp).as_deref(),
    Some(ANNOUNCED_A),
    "配置宣告主机名应落到本地位"
  );
}

/// 空配置回退：未宣告时 init_local 取一次 OS 主机名（C# Format.GetHostName 等价，
/// gethostname(2)），两臂共用单源不各调一次系统调用
#[test]
fn empty_announce_falls_back_to_os_hostname_once() {
  let dir = tempfile::tempdir().unwrap().keep();
  let cp = boot_node(&dir, "127.0.0.1", 7301, "");
  let host = local_hostname(&cp).expect("空配置应回退 OS 主机名而非 None");
  assert!(!host.is_empty(), "OS 主机名回退值应非空");
  // 与 gethostname(2) 单源探测一致（同一次系统调用语义，非二次拼装）
  let mut buf = [0u8; 256];
  let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast::<libc::c_char>(), buf.len()) };
  if rc == 0 {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let raw = String::from_utf8_lossy(&buf[..end]).to_string();
    assert_eq!(host, raw, "回退值应等于 OS 主机名");
  }
}

/// CLUSTER SHARDS：preferred=hostname 时本分片 endpoint 吐宣告主机名，
/// preferred=ip 时吐 IP（ip 字段两种偏好下恒保留）
#[test]
fn cluster_shards_emit_announced_hostname() {
  let dir = tempfile::tempdir().unwrap().keep();
  let cp = boot_node(&dir, "127.0.0.1", 7302, ANNOUNCED_A);
  let cm = cp.cluster_manager().unwrap();
  {
    let mut config = cm.current_config.write();
    for slot in config.slot_map.iter_mut() {
      *slot = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
  }

  // 偏好 hostname：endpoint 行即宣告主机名
  let shards = cm
    .current_config()
    .get_shards_info(None, ClusterPreferredEndpointType::Hostname);
  let want_host = format!(
    "$8\r\nendpoint\r\n${}\r\n{ANNOUNCED_A}\r\n",
    ANNOUNCED_A.len()
  );
  assert!(
    shards.contains(&want_host),
    "CLUSTER SHARDS endpoint 应为宣告主机名:\n{shards}"
  );

  // 偏好 ip：endpoint 行回退 IP，但 hostname 字段仍随节点元数据出现
  cp.set_preferred_endpoint_type(ClusterPreferredEndpointType::Ip);
  let shards_ip = cm
    .current_config()
    .get_shards_info(None, ClusterPreferredEndpointType::Ip);
  assert!(
    shards_ip.contains("$8\r\nendpoint\r\n$9\r\n127.0.0.1\r\n"),
    "IP 偏好 endpoint 应为 127.0.0.1:\n{shards_ip}"
  );
}

/// -MOVED：本节点经 gossip 合并感知远端节点后，preferred=hostname 下重定向吐
/// 被宣告节点的主机名而非 IP（C# GetEndpointFromSlot → GetEndpointByPreferredType）
#[test]
fn moved_redirect_emits_announced_hostname() {
  let dir = tempfile::tempdir().unwrap().keep();
  // A 与 B 各自经装配链宣告主机名（配置优先臂）
  let a = boot_node(&dir, "10.0.0.1", 7310, ANNOUNCED_A);
  let b = boot_node(&dir, "10.0.0.2", 7311, ANNOUNCED_B);

  // B 认领 REMOTE_SLOT（本地位即 LOCAL_WORKER_ID）
  let b_cm = b.cluster_manager().unwrap();
  b_cm.current_config.write().update_slot_state(
    REMOTE_SLOT as usize,
    LOCAL_WORKER_ID as u16,
    SlotState::Stable,
  );

  // A 经 gossip merge 把 B（含其宣告主机名）纳入自身配置
  let a_cm = a.cluster_manager().unwrap();
  let b_config: ClusterConfig = b_cm.current_config().clone();
  let merged = a_cm
    .current_config()
    .merge(&b_config, &new_concurrent_map())
    .expect("B 拓扑应可合并进 A");
  *a_cm.current_config.write() = merged;

  // 校验合并后 A 视 REMOTE_SLOT 属主为 B（非本地，故触发 MOVED 且携带 B 主机名）
  let cs = a.create_cluster_session();
  a.set_preferred_endpoint_type(ClusterPreferredEndpointType::Hostname);
  assert!(
    !cs.network_iterative_slot_verify(b"key", false, 0, false, REMOTE_SLOT),
    "远端槽应判非本地"
  );
  let mut out = Vec::new();
  cs.write_cached_slot_verification_message(&mut out);
  assert_eq!(
    out,
    format!("-MOVED {REMOTE_SLOT} {ANNOUNCED_B}:7311\r\n").into_bytes(),
    "-MOVED 应吐被宣告主机名而非 IP"
  );
}
