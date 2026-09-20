use std::fmt::Write as _;

use bitcode::{Decode, Encode};
use hipstr::HipStr;
use wbase::{hash_slot::CLUSTER_SLOT_COUNT, hex::hex_str_u128};

use super::ClusterConfig;
use crate::{
  error::{Error, Result},
  server::{
    cluster::ClusterPreferredEndpointType,
    cluster_provider::ClusterProvider,
    connection_info::ConnectionInfo,
    hash_slot::{HashSlot, SlotState},
    worker::{LOCAL_WORKER_ID, NodeRole, Worker},
  },
};

/// 集群配置线格式版本：v2 起由 .NET BinaryWriter 布局换为 bitcode 编码，
/// 无向下兼容负担，异版本载荷在解码前即被拒绝
pub const CLUSTER_CONFIG_VERSION: u8 = 2;

/// CLUSTER NODES 中 bus 端口偏移（garnet 语义：bus port = port + 10000）
const BUS_PORT_OFFSET: i32 = 10000;

/// 集群配置线格式（bitcode 编码）
///
/// 槽位图以 RLE 段传输（连续同 (worker_id, state) 的槽数远多于段数，
/// 典型集群个位数段即可覆盖 16384 槽），worker 自 1 号起序列化，
/// 0 号保留位反序列化时按 default 重建——与 C# 布局语义一致，但编码
/// 由 .NET BinaryWriter 的 7-bit 变长整数 hack 换为 bitcode 位压缩，
/// 且解码不再吞错（原实现对截断/越界静默补 0/空串，会产出损坏配置）
#[derive(Encode, Decode)]
struct ConfigWire<'a> {
  segments: Vec<SlotSegmentWire>,
  workers: Vec<WorkerWire<'a>>,
}

/// 一段连续同状态槽位
#[derive(Encode, Decode)]
struct SlotSegmentWire {
  count: u16,
  worker_id: u16,
  /// SlotState 的 u8 表示（显式字节而非枚举直编，状态含义不依赖位布局）
  state: u8,
}

/// Worker 线格式编解码视图（地址等非身份字段借用 &str 零堆分配；
/// 节点 id 按 transpile 规范为 u128 纯二进制，bitcode 定长编码）
#[derive(Encode, Decode)]
struct WorkerWire<'a> {
  nodeid: Option<u128>,
  address: &'a str,
  port: i32,
  config_epoch: i64,
  role: NodeRole,
  replica_of_node_id: Option<u128>,
  replication_offset: i64,
  hostname: Option<&'a str>,
}

impl<'a> From<&'a Worker> for WorkerWire<'a> {
  #[inline]
  fn from(w: &'a Worker) -> Self {
    Self {
      nodeid: w.nodeid,
      address: &w.address,
      port: w.port,
      config_epoch: w.config_epoch,
      role: w.role,
      replica_of_node_id: w.replica_of_node_id,
      replication_offset: w.replication_offset,
      hostname: w.hostname.as_deref(),
    }
  }
}

impl<'a> From<WorkerWire<'a>> for Worker {
  #[inline]
  fn from(w: WorkerWire<'a>) -> Self {
    Self {
      nodeid: w.nodeid,
      address: HipStr::from(w.address),
      replica_of_node_id: w.replica_of_node_id,
      hostname: w.hostname.map(HipStr::from),
      config_epoch: w.config_epoch,
      replication_offset: w.replication_offset,
      port: w.port,
      role: w.role,
    }
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfigSerializer.cs:TryPeekVersion
  ///
  /// 全量解码前快速校验版本号（gossip 接收端先用它拒绝异版本节点）
  #[inline]
  pub fn try_peek_version(data: &[u8]) -> Option<u8> {
    data.first().copied()
  }

  /// libs/cluster/Server/ClusterConfigSerializer.cs:ToByteArray
  pub fn to_byte_array(&self) -> Vec<u8> {
    let mut segments = Vec::with_capacity(16);
    let mut iter = self.slot_map.iter();
    if let Some(first) = iter.next() {
      let mut curr_worker_id = first.worker_id;
      let mut curr_state = first.state as u8;
      let mut curr_count = 1u16;

      for s in iter {
        let state = s.state as u8;
        if s.worker_id == curr_worker_id && state == curr_state {
          curr_count += 1;
        } else {
          segments.push(SlotSegmentWire {
            count: curr_count,
            worker_id: curr_worker_id,
            state: curr_state,
          });
          curr_worker_id = s.worker_id;
          curr_state = state;
          curr_count = 1;
        }
      }
      segments.push(SlotSegmentWire {
        count: curr_count,
        worker_id: curr_worker_id,
        state: curr_state,
      });
    }

    let wire_workers = self
      .workers
      .get(1..)
      .unwrap_or(&[])
      .iter()
      .map(WorkerWire::from)
      .collect();

    let wire = ConfigWire {
      segments,
      workers: wire_workers,
    };

    let encoded = bitcode::encode(&wire);
    let mut out = Vec::with_capacity(1 + encoded.len());
    out.push(CLUSTER_CONFIG_VERSION);
    out.extend_from_slice(&encoded);
    out
  }

  /// libs/cluster/Server/ClusterConfigSerializer.cs:FromByteArray
  pub fn from_byte_array(data: &[u8]) -> Result<Self> {
    let Some((&version, payload)) = data.split_first() else {
      return Err(Error::PayloadTooShort);
    };
    if version != CLUSTER_CONFIG_VERSION {
      return Err(Error::Version {
        got: version,
        expect: CLUSTER_CONFIG_VERSION,
      });
    }

    let wire: ConfigWire<'_> = bitcode::decode(payload)?;

    // 线格式自 1 号本地 worker 起序列化，空列表即结构损坏：放行会产出无本地
    // 位的配置，后续 LOCAL_WORKER_ID 索引 panic（C# 同场景在解码后首次访问
    // workers[1] 时才崩溃，此处前置为解码期 fail-loud）
    if wire.workers.is_empty() {
      return Err(Error::MissingWorkers);
    }

    let mut slot_map = Box::new([HashSlot::default(); CLUSTER_SLOT_COUNT]);
    // worker_id 越界校验先于槽位展开：越界 id 若入库，CLUSTER SLOTS 与
    // 副本读路径（is_local_expensive）按属主下标直取 workers 会 panic，
    // 恶意/损坏 gossip 载荷即可击穿节点进程
    let worker_limit = wire.workers.len();
    let mut offset = 0usize;
    for seg in &wire.segments {
      let state = SlotState::from_repr(seg.state).ok_or(Error::SlotState(seg.state))?;
      if seg.worker_id as usize > worker_limit {
        return Err(Error::SlotWorkerId(seg.worker_id));
      }
      let end = offset + seg.count as usize;
      if end > CLUSTER_SLOT_COUNT {
        return Err(Error::SlotOverflow);
      }
      for slot in &mut slot_map[offset..end] {
        slot.worker_id = seg.worker_id;
        slot.state = state;
      }
      offset = end;
    }

    // 0 号保留位不在线格式内，按 unassigned 重建（对应 C# skip(1) 布局）
    let mut workers = Vec::with_capacity(1 + wire.workers.len());
    workers.push(Worker::unassigned());
    workers.extend(wire.workers.into_iter().map(Worker::from));

    Ok(Self { slot_map, workers })
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:GetClusterInfo
  pub fn get_cluster_info(&self, cluster_provider: Option<&ClusterProvider>) -> String {
    let mut sb = String::new();
    for i in 1..=self.num_workers() {
      let info = if let Some(cp) = cluster_provider {
        if let Some(id) = self.workers[i].nodeid {
          cp.get_connection_info(id)
        } else {
          ConnectionInfo::default()
        }
      } else {
        ConnectionInfo::default()
      };
      self.append_node_info(i, &info, &mut sb);
    }
    sb
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetNodeInfo
  pub fn get_node_info(&self, worker_id: usize, info: &ConnectionInfo) -> String {
    let mut sb = String::new();
    self.append_node_info(worker_id, info, &mut sb);
    sb
  }

  fn append_node_info(&self, worker_id: usize, info: &ConnectionInfo, sb: &mut String) {
    let w = &self.workers[worker_id];
    // 节点 id 仅在 RESP 渲染点转 32 字符小写 hex（对标 C# 40 hex 字符串形态）
    let nodeid = w.nodeid.map_or(String::new(), hex_str_u128);
    let _ = write!(
      sb,
      "{nodeid} {}:{}@{}",
      w.address,
      w.port,
      w.port + BUS_PORT_OFFSET
    );

    if let Some(ref h) = w.hostname
      && !h.is_empty()
    {
      let _ = write!(sb, ",{}", h);
    }

    let _ = write!(
      sb,
      " {}{} {} {} {} {} {}",
      if worker_id == LOCAL_WORKER_ID {
        "myself,"
      } else {
        ""
      },
      if w.role == NodeRole::Primary {
        "master"
      } else {
        "slave"
      },
      if w.role == NodeRole::Replica {
        w.replica_of_node_id
          .map_or_else(|| "-".to_string(), hex_str_u128)
      } else {
        "-".to_string()
      },
      info.ping,
      info.pong,
      w.config_epoch,
      if info.connected || worker_id == LOCAL_WORKER_ID {
        "connected"
      } else {
        "disconnected"
      }
    );

    self.append_slot_range(sb, worker_id as u16);
    self.append_special_states(sb, worker_id as u16);
    sb.push('\n');
  }

  /// libs/cluster/Server/ClusterConfig.cs:AppendSlotRange
  fn append_slot_range(&self, sb: &mut String, worker_id: u16) {
    for (start, end) in self.get_shard_ranges(worker_id as usize) {
      if start == end {
        let _ = write!(sb, " {}", start);
      } else {
        let _ = write!(sb, " {}-{}", start, end);
      }
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:AppendSpecialStates
  fn append_special_states(&self, sb: &mut String, worker_id: u16) {
    if worker_id as usize != LOCAL_WORKER_ID {
      return;
    }
    for (slot, s) in self.slot_map.iter().enumerate() {
      let wid = s.worker_id as usize;
      let state = s.state;

      if state == SlotState::Stable || wid > self.num_workers() {
        continue;
      }

      if let Some(node_id) = self.workers[wid].nodeid {
        match state {
          SlotState::Migrating => {
            let _ = write!(sb, " [{}->-{}]", slot, hex_str_u128(node_id));
          }
          SlotState::Importing => {
            let _ = write!(sb, " [{}-<-{}]", slot, hex_str_u128(node_id));
          }
          _ => {}
        }
      }
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetReplicas
  pub fn get_replicas(
    &self,
    nodeid: u128,
    cluster_provider: Option<&ClusterProvider>,
  ) -> Vec<String> {
    let mut replicas = Vec::new();
    for (i, worker) in self.workers.iter().enumerate().skip(1) {
      if worker.replica_of_node_id == Some(nodeid) {
        let info = cluster_provider
          .and_then(|cp| worker.nodeid.map(|id| cp.get_connection_info(id)))
          .unwrap_or_default();
        replicas.push(self.get_node_info(i, &info));
      }
    }
    replicas
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:GetShardsInfo
  pub fn get_shards_info(
    &self,
    cluster_connection: Option<&ClusterProvider>,
    pref_type: ClusterPreferredEndpointType,
  ) -> String {
    let mut sb = String::new();
    let mut shard_count = 0;
    let mut shards_str = String::new();

    for i in 1..=self.num_workers() {
      if self.workers[i].role == NodeRole::Primary {
        let shard_ranges = self.get_shard_ranges(i);
        let replica_worker_ids = self.get_worker_replicas(i);
        self.append_formatted_shard_info(
          &mut shards_str,
          i,
          &shard_ranges,
          &replica_worker_ids,
          cluster_connection,
          pref_type,
        );
        shard_count += 1;
      }
    }
    let _ = write!(sb, "*{}\r\n{}", shard_count, shards_str);
    sb
  }

  /// libs/cluster/Server/ClusterConfig.cs:AppendFormattedShardInfo
  fn append_formatted_shard_info(
    &self,
    sb: &mut String,
    primary_worker_id: usize,
    shard_ranges: &[(u16, u16)],
    replica_worker_ids: &[usize],
    cluster_connection: Option<&ClusterProvider>,
    pref_type: ClusterPreferredEndpointType,
  ) {
    sb.push_str("*4\r\n");
    sb.push_str("$5\r\nslots\r\n");
    let _ = write!(sb, "*{}\r\n", shard_ranges.len() * 2);
    for range in shard_ranges {
      let _ = write!(sb, ":{}\r\n:{}\r\n", range.0, range.1);
    }

    sb.push_str("$5\r\nnodes\r\n");
    let _ = write!(sb, "*{}\r\n", 1 + replica_worker_ids.len());

    if primary_worker_id == LOCAL_WORKER_ID {
      self.append_formatted_node_info(sb, primary_worker_id, true, pref_type);
    } else {
      let connected = if let Some(cp) = cluster_connection {
        self.workers[primary_worker_id]
          .nodeid
          .is_some_and(|nid| cp.get_connection_info(nid).connected)
      } else {
        false
      };
      self.append_formatted_node_info(sb, primary_worker_id, connected, pref_type);
    }

    for &id in replica_worker_ids {
      let connected = if let Some(cp) = cluster_connection {
        self.workers[id]
          .nodeid
          .is_some_and(|nid| cp.get_connection_info(nid).connected)
      } else {
        false
      };
      self.append_formatted_node_info(sb, id, connected, pref_type);
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:AppendFormattedNodeInfo
  fn append_formatted_node_info(
    &self,
    sb: &mut String,
    worker_id: usize,
    connected: bool,
    pref_type: ClusterPreferredEndpointType,
  ) {
    let ip = &self.workers[worker_id].address;
    let hostname = self.workers[worker_id].hostname.as_deref().unwrap_or("");
    let has_hostname = !hostname.is_empty();
    let role = if self.workers[worker_id].role == NodeRole::Primary {
      "master"
    } else {
      "slave"
    };

    let endpoint = match pref_type {
      ClusterPreferredEndpointType::Hostname => {
        if has_hostname {
          hostname
        } else {
          "?"
        }
      }
      ClusterPreferredEndpointType::Unknown => "?",
      _ => ip,
    };

    let field_count = if has_hostname { 16 } else { 14 };

    let _ = write!(sb, "*{}\r\n", field_count);
    sb.push_str("$2\r\nid\r\n");
    // 节点 id 仅在 RESP 渲染点转 32 字符小写 hex
    let nodeid = self.workers[worker_id]
      .nodeid
      .map_or_else(String::new, hex_str_u128);
    let _ = write!(sb, "${}\r\n{}\r\n", nodeid.len(), nodeid);
    sb.push_str("$4\r\nport\r\n");
    let _ = write!(sb, ":{}\r\n", self.workers[worker_id].port);
    sb.push_str("$2\r\nip\r\n");
    let _ = write!(sb, "${}\r\n{}\r\n", ip.len(), ip);
    sb.push_str("$8\r\nendpoint\r\n");
    let _ = write!(sb, "${}\r\n{}\r\n", endpoint.len(), endpoint);
    if has_hostname {
      sb.push_str("$8\r\nhostname\r\n");
      let _ = write!(sb, "${}\r\n{}\r\n", hostname.len(), hostname);
    }
    sb.push_str("$4\r\nrole\r\n");
    let _ = write!(sb, "${}\r\n{}\r\n", role.len(), role);
    sb.push_str("$18\r\nreplication-offset\r\n");
    let _ = write!(sb, ":{}\r\n", self.workers[worker_id].replication_offset);
    sb.push_str("$6\r\nhealth\r\n");
    if connected {
      sb.push_str("$6\r\nonline\r\n");
    } else {
      sb.push_str("$7\r\noffline\r\n");
    }
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:GetSlotsInfo
  ///
  /// 对齐 C#（ClusterConfig.cs:890-894）按 eff 属主分段：Migrating 槽
  /// eff=LOCAL 随源节点区间上报（Redis 语义：迁移未完成前属主不变）
  pub fn get_slots_info(&self, pref_type: ClusterPreferredEndpointType) -> String {
    let mut sb = String::new();
    let mut slot_ranges = 0;
    let mut slot_start = 0;
    let mut slots_str = String::new();

    while slot_start < CLUSTER_SLOT_COUNT {
      if self.slot_map[slot_start].state == SlotState::Offline {
        slot_start += 1;
        continue;
      }

      let mut slot_end = slot_start;
      while slot_end < CLUSTER_SLOT_COUNT {
        if self.slot_map[slot_end].state == SlotState::Offline
          || self.slot_map[slot_start].eff_worker_id() != self.slot_map[slot_end].eff_worker_id()
        {
          break;
        }
        slot_end += 1;
      }

      slot_end -= 1;
      let curr_worker_id = self.slot_map[slot_start].eff_worker_id() as usize;
      // 区间属主以借用传递，免每区间 4 份字符串克隆
      let owner = &self.workers[curr_worker_id];
      let replica_worker_ids = self.get_worker_replicas(curr_worker_id);

      self.append_formatted_slot_info(
        &mut slots_str,
        slot_start,
        slot_end,
        owner,
        &replica_worker_ids,
        pref_type,
      );
      slot_ranges += 1;
      slot_start = slot_end + 1;
    }

    let _ = write!(sb, "*{}\r\n{}", slot_ranges, slots_str);
    sb
  }

  /// libs/cluster/Server/ClusterConfig.cs:AppendFormattedSlotInfo
  fn append_formatted_slot_info(
    &self,
    sb: &mut String,
    slot_start: usize,
    slot_end: usize,
    owner: &Worker,
    replica_worker_ids: &[usize],
    pref_type: ClusterPreferredEndpointType,
  ) {
    let count_a = 3 + replica_worker_ids.len();
    let _ = write!(sb, "*{}\r\n:{}\r\n:{}\r\n", count_a, slot_start, slot_end);

    self.append_node_networking_info(
      sb,
      &owner.address,
      owner.port,
      owner.nodeid,
      owner.hostname.as_deref(),
      pref_type,
    );

    for &id in replica_worker_ids {
      if let Some(w) = self.workers.get(id) {
        self.append_node_networking_info(
          sb,
          &w.address,
          w.port,
          w.nodeid,
          w.hostname.as_deref(),
          pref_type,
        );
      }
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:AppendNodeNetworkingInfo
  fn append_node_networking_info(
    &self,
    sb: &mut String,
    ip_address: &str,
    port: i32,
    nodeid: Option<u128>,
    hostname: Option<&str>,
    pref_type: ClusterPreferredEndpointType,
  ) {
    // 节点 id 仅在 RESP 渲染点转 hex；缺席身份渲染空串（对位 C# unwrap_or_default）
    let nodeid = nodeid.map_or_else(String::new, hex_str_u128);
    sb.push_str("*4\r\n");
    let is_null_or_empty_hostname = hostname.is_none_or(|h| h.is_empty());

    match pref_type {
      ClusterPreferredEndpointType::Ip => {
        self.append_value_or_null(sb, Some(ip_address));
        let _ = write!(
          sb,
          ":{}\r\n${}\r\n{}\r\n*{}\r\n",
          port,
          nodeid.len(),
          nodeid,
          if is_null_or_empty_hostname { 0 } else { 2 }
        );
        if !is_null_or_empty_hostname {
          sb.push_str("$8\r\nhostname\r\n");
          self.append_value_or_null(sb, hostname);
        }
      }
      ClusterPreferredEndpointType::Hostname => {
        let hostname_for_resp = if is_null_or_empty_hostname {
          "?"
        } else {
          hostname.unwrap()
        };
        self.append_value_or_null(sb, Some(hostname_for_resp));
        let _ = write!(
          sb,
          ":{}\r\n${}\r\n{}\r\n*2\r\n$2\r\nip\r\n",
          port,
          nodeid.len(),
          nodeid
        );
        self.append_value_or_null(sb, Some(ip_address));
      }
      ClusterPreferredEndpointType::Unknown => {
        self.append_value_or_null(sb, None);
        let _ = write!(
          sb,
          ":{}\r\n${}\r\n{}\r\n*{}\r\n$2\r\nip\r\n",
          port,
          nodeid.len(),
          nodeid,
          if is_null_or_empty_hostname { 2 } else { 4 }
        );
        self.append_value_or_null(sb, Some(ip_address));
        if !is_null_or_empty_hostname {
          sb.push_str("$8\r\nhostname\r\n");
          self.append_value_or_null(sb, hostname);
        }
      }
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:AppendValueOrNull
  fn append_value_or_null(&self, sb: &mut String, value: Option<&str>) {
    if let Some(v) = value {
      if v.is_empty() {
        sb.push_str("$-1\r\n");
      } else {
        let _ = write!(sb, "${}\r\n{}\r\n", v.len(), v);
      }
    } else {
      sb.push_str("$-1\r\n");
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::server::worker::LocalWorkerSpec;

  #[test]
  fn test_config_roundtrip() {
    let mut config = ClusterConfig::new();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x1234_5678_9abc_def0_1234_5678_9abc_def0,
      address: "127.0.0.1",
      port: 7001,
      config_epoch: 42,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: Some("node1.cluster"),
    });

    config.update_slot_state(100, LOCAL_WORKER_ID as u16, SlotState::Stable);
    config.update_slot_state(101, LOCAL_WORKER_ID as u16, SlotState::Migrating);
    config.update_slot_state(500, LOCAL_WORKER_ID as u16, SlotState::Importing);

    let bytes = config.to_byte_array();
    let decoded = ClusterConfig::from_byte_array(&bytes).expect("decode failed");

    assert_eq!(config.num_workers(), decoded.num_workers());
    assert_eq!(config.local_node_id(), decoded.local_node_id());
    assert_eq!(config.local_node_ip(), decoded.local_node_ip());
    assert_eq!(config.local_node_port(), decoded.local_node_port());
    assert_eq!(
      config.local_node_config_epoch(),
      decoded.local_node_config_epoch()
    );
    assert_eq!(config.local_node_role(), decoded.local_node_role());

    for i in 0..CLUSTER_SLOT_COUNT {
      assert_eq!(config.slot_map[i].worker_id, decoded.slot_map[i].worker_id);
      assert_eq!(config.slot_map[i].state, decoded.slot_map[i].state);
    }
  }

  /// 节点 id 线格式为 u128 纯二进制：roundtrip 后身份不变，
  /// RESP 渲染点输出 32 字符小写 hex（仅协议面转字符串）
  #[test]
  fn test_node_id_binary_wire_and_hex_render() {
    let mut config = ClusterConfig::new();
    let id = 0x0123_4567_89ab_cdef_0fed_cba9_8765_4321u128;
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: id,
      address: "127.0.0.1",
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });

    let decoded = ClusterConfig::from_byte_array(&config.to_byte_array()).expect("decode failed");
    assert_eq!(decoded.local_node_id(), Some(id));
    // CLUSTER NODES 渲染面：身份以 32 字符小写 hex 呈现
    let nodes = decoded.get_node_info(LOCAL_WORKER_ID, &ConnectionInfo::default());
    assert!(
      nodes.starts_with("0123456789abcdef0fedcba987654321 127.0.0.1:7001"),
      "hex 渲染不符: {nodes}"
    );
  }
}
