//! 集群宣告端点解析：announce-ip/port 配置校验与 Any 绑定出口 IP 探测
//!
//! 两段 C# 对位：
//! - libs/host/Configuration/Options.cs:800-811（宣告端点与监听端点匹配校验）：
//!   announce-port 缺省随监听端口，显式值必须等于某监听端点端口，宣告 IP
//!   须等于某监听地址或监听地址为 Any/IPv6Any，否则拒启；
//! - libs/server/StoreWrapper.cs:GetClusterEndpoint（宣告地址取口）：未配
//!   announce-ip 取首个 TCP 监听端点；地址为 Any/IPv6Any 时经 UDP connect
//!   探测默认路由源地址后宣告，探测失败异常外抛拒启。
//!
//! rust 兜底语义选型（C# SocketException 外抛拒启的对位）：显式报错拒启，
//! 不做保护模式回退——回退会把 127.0.0.1 写进 nodes.conf 供 gossip 扩散，
//! 对端建连必败，故障比拒启更隐蔽；且拒启即暴露探测失败（无默认路由），
//! 运维可在启动期介入，与 C# 拒启行为等价。
//!
//! 结构性缺席登记：C# StoreWrapper.cs:353-354「无 TCP 监听端点即
//! GarnetException」臂在 rust 不可达——bind 缺省回退恒产 TCP 端点
//! （保护回环 / 非保护全接口），unixsocket 仅追加不替换监听列表。

use std::{
  io,
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs, UdpSocket},
};

use wconf::{DEFAULT_BIND, DEFAULT_BIND_ANY, NodeArgs, format_bind_endpoint};
use wnode::ServerEndpoint;

use crate::{
  error::{Error, Result},
  server::cluster_manager::os_hostname,
};

/// 出口探测对端端口（C# StoreWrapper.cs:360/369 硬编码 65530；UDP connect
/// 只做路由选路不发包，端口值不产生网络语义，仅为与 C# 同形取常量）
const PROBE_PORT: u16 = 65530;

/// 出口 IP 探测（libs/server/StoreWrapper.cs:GetClusterEndpoint 的
/// socket.Connect 同法：v4 连 8.8.8.8、v6 连 2001:4860:4860::8888，UDP
/// connect 不发包，`local_addr` 即默认路由源地址）
///
/// 在 garnet 中的相对路径:StoreWrapper.GetClusterEndpoint
pub fn probe_outbound_ip(v6: bool) -> io::Result<IpAddr> {
  let (bind, target): (IpAddr, IpAddr) = if v6 {
    (
      Ipv6Addr::UNSPECIFIED.into(),
      Ipv6Addr::new(0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888).into(),
    )
  } else {
    (
      Ipv4Addr::UNSPECIFIED.into(),
      Ipv4Addr::new(8, 8, 8, 8).into(),
    )
  };
  let sock = UdpSocket::bind(SocketAddr::new(bind, 0))?;
  sock.connect(SocketAddr::new(target, PROBE_PORT))?;
  Ok(sock.local_addr()?.ip())
}

/// 监听端点表（Format.TryParseAddressList 的 IP 投影：bind 缺省按
/// protected-mode 回退，逐条切分后解析为 SocketAddr。C# TryParseAddressList
/// 锚由配置层 `NodeArgs::endpoints` 单点持有——C# 唯一生产调用点即
/// Options.cs:795 配置面；本函数为节点启动监听投影，不重复挂锚）
fn listen_endpoints(node: &NodeArgs) -> Result<Vec<SocketAddr>> {
  let raw = node.bind.as_deref().unwrap_or_default().trim();
  let bind = if raw.is_empty() {
    if node.protected_mode {
      DEFAULT_BIND
    } else {
      DEFAULT_BIND_ANY
    }
  } else {
    raw
  };
  let mut endpoints = Vec::new();
  for a in bind
    .split([',', ' '])
    .map(str::trim)
    .filter(|a| !a.is_empty())
  {
    let eps = ServerEndpoint::parse_many(&format_bind_endpoint(a, node.port)).map_err(|_| {
      Error::InvalidArgument(format!("Invalid endpoint format {bind} {}.", node.port))
    })?;
    for ep in eps {
      match ep {
        ServerEndpoint::Tcp(addr) => endpoints.push(addr),
        ServerEndpoint::Unix(_) => {
          return Err(Error::InvalidArgument(format!(
            "Invalid endpoint format {bind} {}.",
            node.port
          )));
        }
      }
    }
  }
  // C# Options.cs:796-797 端点列表为空即拒启（bind 全空白条目被剔除）
  if endpoints.is_empty() {
    return Err(Error::InvalidArgument(format!(
      "Invalid endpoint format {bind} {}.",
      node.port
    )));
  }
  Ok(endpoints)
}

/// 宣告 IP 解析（C# Format.TryCreateEndpoint 投影）：IP 字面量直取；
/// `localhost` 取回环（C# defaultBindLoopBack 首成员 127.0.0.1）；主机名
/// 仅当与本机主机名一致时按 getaddrinfo 解析取首地址（C# Dns.GetHostAddresses
/// 前的机器名等价门），其余拒启——主机名宣告走 cluster-announce-hostname 面。
/// C# 的前导 `-` 剥除臂不随入：clap 解析期已消化该形态
fn parse_announce_ip(input: &str) -> Option<IpAddr> {
  if let Ok(ip) = input.parse::<IpAddr>() {
    return Some(ip);
  }
  if input.eq_ignore_ascii_case("localhost") {
    return Some(Ipv4Addr::LOCALHOST.into());
  }
  if input.eq_ignore_ascii_case(&os_hostname()) {
    return (input, 0).to_socket_addrs().ok()?.map(|a| a.ip()).next();
  }
  None
}

/// 集群宣告端点解析（产出直传 init_local 的本地位：任何路径都不产
/// unspecified 地址——Any 绑定一律经探测折算为出口 IP，探测失败即拒启，
/// 杜绝 0.0.0.0 进 nodes.conf 供 gossip 扩散）
///
/// `probe` 形参注入出口探测（生产传 [`probe_outbound_ip`]，测试注入假探针）
///
/// 在 garnet 中的相对路径:StoreWrapper.GetClusterEndpoint
pub fn resolve_cluster_announce(
  node: &NodeArgs,
  announce_ip: Option<&str>,
  announce_port: u16,
  probe: impl Fn(bool) -> io::Result<IpAddr>,
) -> Result<(String, i32)> {
  let listen = listen_endpoints(node)?;
  // C# :334-345 未配 announce-ip 取首个 TCP 监听端点；监听端口恒为
  // node.port（监听列表单端口展开），即 C# localEndPoint.Port
  let mut addr = listen[0].ip();
  let mut port = node.port;
  if let Some(ip) = announce_ip {
    // C# :802 announce-port 缺省随监听端口
    port = if announce_port == 0 {
      node.port
    } else {
      announce_port
    };
    // C# :803-811 宣告端点解析 + 匹配校验：端口须等某监听端点端口，
    // 地址须等某监听地址或监听为 Any（unspecified 覆盖 v4/v6 双 Any）
    let Some(parsed) = parse_announce_ip(ip) else {
      return Err(Error::AnnounceMismatch);
    };
    let matched = listen
      .iter()
      .any(|ep| ep.port() == port && (ep.ip() == parsed || ep.ip().is_unspecified()));
    if !matched {
      return Err(Error::AnnounceMismatch);
    }
    addr = parsed;
  }
  // C# :356-373 Any/IPv6Any 绑定按地址族探测出口 IP，端口保持监听值
  if addr.is_unspecified() {
    addr = probe(addr.is_ipv6()).map_err(Error::AnnounceProbe)?;
  }
  Ok((addr.to_string(), i32::from(port)))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_listen_endpoints_defaults() {
    let node = NodeArgs::default();
    let eps = listen_endpoints(&node).unwrap();
    assert_eq!(eps.len(), 2);
    assert_eq!(eps[0].to_string(), "127.0.0.1:6379");
    assert_eq!(eps[1].to_string(), "[::1]:6379");
  }

  #[test]
  fn test_listen_endpoints_localhost() {
    let node = NodeArgs {
      bind: Some("localhost".to_string()),
      ..Default::default()
    };
    let eps = listen_endpoints(&node).unwrap();
    assert_eq!(eps.len(), 2);
    assert_eq!(eps[0].to_string(), "127.0.0.1:6379");
    assert_eq!(eps[1].to_string(), "[::1]:6379");
  }

  #[test]
  fn test_listen_endpoints_dual_any() {
    let node = NodeArgs {
      protected_mode: false,
      ..Default::default()
    };
    let eps = listen_endpoints(&node).unwrap();
    assert_eq!(eps.len(), 2);
    assert_eq!(eps[0].to_string(), "0.0.0.0:6379");
    assert_eq!(eps[1].to_string(), "[::]:6379");
  }

  #[test]
  fn test_listen_endpoints_rejects_uds() {
    let node = NodeArgs {
      bind: Some("/tmp/wedb.sock".to_string()),
      ..Default::default()
    };
    assert!(listen_endpoints(&node).is_err());
  }
}
