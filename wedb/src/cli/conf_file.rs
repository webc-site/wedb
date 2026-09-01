use serde::{Deserialize, Deserializer, Serialize, de};
use std::fs::read_to_string;

pub use wedb_cluster::conf::{FjallConf, TopologyConf};

/// NestedText 分块层级配置文件结构体定义 (极简分块层级设计)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfFile {
  /// 全局通用 IP (各子模块未单独配置 IP 时默认继承)
  pub ip: Option<String>,

  /// Redis 服务配置
  pub server: Option<ServerConfig>,

  /// 集群节点基础配置
  pub cluster: Option<ClusterConfig>,

  /// Raft 共识配置
  pub raft: Option<RaftConfig>,

  /// Fjall LSM-Tree 存储引擎配置 (直接复用 wedb_embed::Conf)
  pub fjall: Option<FjallConf>,

  /// 物理故障域拓扑配置 (直接复用 cluster::TopologyConf)
  pub topology: Option<TopologyConf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NetConfig {
  Section(NetSection),
  Port(u16),
  Addr(String),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct NetSection {
  pub ip: Option<String>,
  pub addr: Option<String>,
  #[serde(deserialize_with = "de_opt_u16", default)]
  pub port: Option<u16>,
}

impl NetConfig {
  pub fn resolve(&self, default_ip: &str) -> String {
    match self {
      Self::Section(sec) => {
        let actual_ip = sec.ip.as_deref().unwrap_or(default_ip);
        if let Some(ref addr) = sec.addr {
          resolve_endpoint(addr, sec.ip.as_deref(), default_ip)
        } else if let Some(port) = sec.port {
          format!("{actual_ip}:{port}")
        } else {
          format!("{actual_ip}:0")
        }
      }
      Self::Port(port) => format!("{default_ip}:{port}"),
      Self::Addr(addr) => resolve_endpoint(addr, None, default_ip),
    }
  }
}

pub type ServerConfig = NetConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RaftConfig {
  Section(RaftSection),
  Port(u16),
  Addr(String),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RaftSection {
  pub ip: Option<String>,
  pub addr: Option<String>,
  #[serde(deserialize_with = "de_opt_u16", default)]
  pub port: Option<u16>,
  pub join: Option<Vec<String>>,
  #[serde(deserialize_with = "de_opt_u64", default)]
  pub heartbeat: Option<u64>,
}

impl RaftConfig {
  pub fn resolve(&self, default_ip: &str) -> String {
    match self {
      Self::Section(sec) => {
        let actual_ip = sec.ip.as_deref().unwrap_or(default_ip);
        if let Some(ref addr) = sec.addr {
          resolve_endpoint(addr, sec.ip.as_deref(), default_ip)
        } else if let Some(port) = sec.port {
          format!("{actual_ip}:{port}")
        } else {
          format!("{actual_ip}:0")
        }
      }
      Self::Port(port) => format!("{default_ip}:{port}"),
      Self::Addr(addr) => resolve_endpoint(addr, None, default_ip),
    }
  }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
  #[serde(deserialize_with = "de_opt_u64", default)]
  pub node_id: Option<u64>,
  #[serde(deserialize_with = "de_opt_u32", default)]
  pub weight: Option<u32>,
}

macro_rules! impl_de_opt_num {
  ($fn_name:ident, $ty:ty, $desc:literal) => {
    fn $fn_name<'de, D>(deserializer: D) -> std::result::Result<Option<$ty>, D::Error>
    where
      D: Deserializer<'de>,
    {
      struct Visitor;
      impl<'de> de::Visitor<'de> for Visitor {
        type Value = Option<$ty>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
          formatter.write_str($desc)
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
          Ok(Some(v as $ty))
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
          Ok(Some(v as $ty))
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
          let trimmed = v.trim();
          if trimmed.is_empty() {
            return Ok(None);
          }
          trimmed
            .parse::<$ty>()
            .map(Some)
            .map_err(|e| de::Error::custom(format!("failed to parse integer: {e}")))
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
          Ok(None)
        }
        fn visit_some<D2: Deserializer<'de>>(
          self,
          deserializer: D2,
        ) -> Result<Self::Value, D2::Error> {
          deserializer.deserialize_any(Visitor)
        }
      }
      deserializer.deserialize_any(Visitor)
    }
  };
}

impl_de_opt_num!(de_opt_u16, u16, "an optional u16 integer or string");
impl_de_opt_num!(de_opt_u32, u32, "an optional u32 integer or string");
impl_de_opt_num!(de_opt_u64, u64, "an optional u64 integer or string");

#[inline]
fn resolve_endpoint(addr_or_port: &str, ip: Option<&str>, default_ip: &str) -> String {
  if addr_or_port.contains(':') {
    addr_or_port.to_string()
  } else if let Ok(p) = addr_or_port.parse::<u16>() {
    let actual_ip = ip.unwrap_or(default_ip);
    format!("{actual_ip}:{p}")
  } else {
    addr_or_port.to_string()
  }
}

impl ConfFile {
  /// 从 NestedText 字符串解析配置
  pub fn from_nested_text(content: &str) -> Result<Self, String> {
    nested_text::from_str(content).map_err(|e| e.to_string())
  }

  /// 从文件路径加载 NestedText 配置
  pub fn load_from_file(path: &str) -> aok::Result<Self> {
    let content = read_to_string(path)?;
    let conf: Self = Self::from_nested_text(&content)
      .map_err(|e| aok::anyhow!("Failed to parse config file '{path}': {e}"))?;
    Ok(conf)
  }

  /// 获取配置中复用的全局 IP
  pub fn get_ip(&self) -> Option<String> {
    self.ip.clone()
  }

  pub fn get_node_id(&self) -> Option<u64> {
    self.cluster.as_ref().and_then(|c| c.node_id)
  }

  pub fn get_addr(&self) -> Option<String> {
    let default_ip = self.ip.as_deref().unwrap_or("127.0.0.1");
    self.server.as_ref().map(|s| s.resolve(default_ip))
  }

  pub fn get_redis_addr(&self) -> Option<String> {
    self.get_addr()
  }

  pub fn get_raft_addr(&self) -> Option<String> {
    let default_ip = self.ip.as_deref().unwrap_or("127.0.0.1");
    self.raft.as_ref().map(|r| r.resolve(default_ip))
  }

  pub fn get_data_dir(&self) -> Option<String> {
    self.fjall.as_ref().map(|f| f.data_path.clone())
  }

  pub fn get_compression(&self) -> Option<String> {
    self.fjall.as_ref().and_then(|f| f.compression.clone())
  }

  pub fn get_journal_compression(&self) -> Option<String> {
    self
      .fjall
      .as_ref()
      .and_then(|f| f.journal_compression.clone())
  }

  pub fn get_manual_journal_persist(&self) -> Option<bool> {
    self.fjall.as_ref().and_then(|f| f.manual_journal_persist)
  }

  pub fn get_cache_size(&self) -> Option<usize> {
    self.fjall.as_ref().and_then(|f| f.cache_size)
  }

  pub fn get_worker_threads(&self) -> Option<usize> {
    self.fjall.as_ref().and_then(|f| f.worker_threads)
  }

  pub fn get_max_journaling_size(&self) -> Option<usize> {
    self.fjall.as_ref().and_then(|f| f.max_journaling_size)
  }

  pub fn get_join(&self) -> Option<Vec<String>> {
    self.raft.as_ref().and_then(|r| match r {
      RaftConfig::Section(sec) => sec.join.clone(),
      _ => None,
    })
  }

  pub fn get_heartbeat(&self) -> Option<u64> {
    self.raft.as_ref().and_then(|r| match r {
      RaftConfig::Section(sec) => sec.heartbeat,
      _ => None,
    })
  }

  pub fn get_region(&self) -> Option<String> {
    self.topology.as_ref().and_then(|t| t.region.clone())
  }

  pub fn get_zone(&self) -> Option<String> {
    self.topology.as_ref().and_then(|t| t.zone.clone())
  }

  pub fn get_rack(&self) -> Option<String> {
    self.topology.as_ref().and_then(|t| t.rack.clone())
  }

  pub fn get_host(&self) -> Option<String> {
    self.topology.as_ref().and_then(|t| t.host.clone())
  }

  pub fn get_weight(&self) -> Option<u32> {
    self
      .topology
      .as_ref()
      .and_then(|t| t.weight)
      .or_else(|| self.cluster.as_ref().and_then(|c| c.weight))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_nested_text_hierarchical_config() {
    let nt = r#"
ip: 10.0.1.10
cluster:
  node_id: 10
  weight: 200
server:
  port: 6379
raft:
  port: 4910
  heartbeat: 100
  join:
    - 10.0.1.11:4910
    - 10.0.1.12:4910
fjall:
  data_dir: /var/lib/wedb
  compression: lz4
  journal_compression: none
  manual_journal_persist: false
  cache_size: 67108864
topology:
  region: cn-beijing
  zone: zone-a
  rack: rack-01
  host: 10.0.1.10
"#;
    let conf = ConfFile::from_nested_text(nt).unwrap();
    assert_eq!(conf.get_node_id(), Some(10));
    assert_eq!(conf.get_addr().unwrap(), "10.0.1.10:6379");
    assert_eq!(conf.get_raft_addr().unwrap(), "10.0.1.10:4910");
    assert_eq!(conf.get_heartbeat(), Some(100));
    assert_eq!(conf.get_data_dir().unwrap(), "/var/lib/wedb");
    assert_eq!(conf.get_compression().unwrap(), "lz4");
    assert_eq!(conf.get_journal_compression().unwrap(), "none");
    assert_eq!(conf.get_manual_journal_persist(), Some(false));
    assert_eq!(conf.get_cache_size(), Some(67108864));
    assert_eq!(conf.get_region().unwrap(), "cn-beijing");
    assert_eq!(conf.get_zone().unwrap(), "zone-a");
    assert_eq!(conf.get_rack().unwrap(), "rack-01");
    assert_eq!(conf.get_host().unwrap(), "10.0.1.10");
    assert_eq!(conf.get_weight(), Some(200));
    assert_eq!(
      conf.get_join().unwrap(),
      vec!["10.0.1.11:4910".to_string(), "10.0.1.12:4910".to_string()]
    );
  }

  #[test]
  fn test_nested_text_hierarchical_ports_config() {
    let nt = r#"
ip: 0.0.0.0
cluster:
  node_id: 2
  weight: 150
server:
  port: 6379
raft:
  port: 4910
  heartbeat: 100
  join:
    - 192.168.1.1:4910
fjall:
  data_dir: /tmp/wedb_node2
  compression: none
  journal_compression: lz4
  manual_journal_persist: true
topology:
  region: us-west
  zone: us-west-1a
  rack: rack-99
  host: 192.168.1.2
"#;
    let conf = ConfFile::from_nested_text(nt).unwrap();
    assert_eq!(conf.get_node_id(), Some(2));
    assert_eq!(conf.get_addr().unwrap(), "0.0.0.0:6379");
    assert_eq!(conf.get_raft_addr().unwrap(), "0.0.0.0:4910");
    assert_eq!(conf.get_heartbeat(), Some(100));
    assert_eq!(conf.get_data_dir().unwrap(), "/tmp/wedb_node2");
    assert_eq!(conf.get_compression().unwrap(), "none");
    assert_eq!(conf.get_journal_compression().unwrap(), "lz4");
    assert_eq!(conf.get_manual_journal_persist(), Some(true));
    assert_eq!(conf.get_region().unwrap(), "us-west");
    assert_eq!(conf.get_zone().unwrap(), "us-west-1a");
    assert_eq!(conf.get_rack().unwrap(), "rack-99");
    assert_eq!(conf.get_host().unwrap(), "192.168.1.2");
    assert_eq!(conf.get_weight(), Some(150));
    assert_eq!(
      conf.get_join().unwrap(),
      vec!["192.168.1.1:4910".to_string()]
    );
  }
}
