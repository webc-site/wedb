use std::{
  io,
  net::{IpAddr, Ipv4Addr},
  sync::atomic::{AtomicBool, Ordering},
};

use wconf::NodeArgs;
use wedb::{error::Error, server::announce::resolve_cluster_announce};

/// 定版参数基线（端口 7000，bind 与保护档按入参）
fn node(bind: Option<&str>, protected_mode: bool) -> NodeArgs {
  NodeArgs {
    bind: bind.map(ToOwned::to_owned),
    port: 7000,
    protected_mode,
    ..NodeArgs::default()
  }
}

/// 恒败假探针：被调用即断言失败（校验「无需探测」路径零网络触达）
fn no_probe(v6: bool) -> io::Result<IpAddr> {
  panic!("unexpected probe v6:{v6}");
}

/// 定值假探针
fn fake_probe(ip: IpAddr) -> impl Fn(bool) -> io::Result<IpAddr> {
  move |_| Ok(ip)
}

fn v6_addr() -> IpAddr {
  IpAddr::from([0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

#[test]
fn loopback_listen_announced_verbatim() {
  let (addr, port) =
    resolve_cluster_announce(&node(Some("127.0.0.1"), true), None, 0, no_probe).unwrap();
  assert_eq!(addr, "127.0.0.1");
  assert_eq!(port, 7000);
}

#[test]
fn protected_default_announces_loopback_without_probe() {
  let (addr, port) = resolve_cluster_announce(&node(None, true), None, 0, no_probe).unwrap();
  assert_eq!(addr, "127.0.0.1");
  assert_eq!(port, 7000);
}

#[test]
fn non_protected_default_probes_outbound_ip() {
  // 硬判据：非保护缺省（bind 回退 0.0.0.0）必须探测折算，绝不产 0.0.0.0
  let probed = IpAddr::from(Ipv4Addr::new(10, 1, 2, 3));
  let (addr, port) =
    resolve_cluster_announce(&node(None, false), None, 0, fake_probe(probed)).unwrap();
  assert_eq!(addr, "10.1.2.3");
  assert_eq!(port, 7000);
}

#[test]
fn explicit_any_bind_probes_per_family() {
  let probed = IpAddr::from(Ipv4Addr::new(192, 168, 1, 9));
  let (addr, _) =
    resolve_cluster_announce(&node(Some("0.0.0.0"), false), None, 0, fake_probe(probed)).unwrap();
  assert_eq!(addr, "192.168.1.9");

  // v6 Any 全段展开与 :: 简写，断言按 v6 地址族探测
  let probed_v6 = AtomicBool::new(false);
  let (addr, _) = resolve_cluster_announce(&node(Some("0:0:0:0:0:0:0:0"), false), None, 0, |v6| {
    probed_v6.store(v6, Ordering::Relaxed);
    Ok(v6_addr())
  })
  .unwrap();
  assert_eq!(addr, v6_addr().to_string());
  assert!(probed_v6.load(Ordering::Relaxed));

  let probed_v6_short = AtomicBool::new(false);
  let (addr, _) = resolve_cluster_announce(&node(Some("::"), false), None, 0, |v6| {
    probed_v6_short.store(v6, Ordering::Relaxed);
    Ok(v6_addr())
  })
  .unwrap();
  assert_eq!(addr, v6_addr().to_string());
  assert!(probed_v6_short.load(Ordering::Relaxed));
}

#[test]
fn explicit_v6_bind_resolves_announce() {
  let (addr, port) =
    resolve_cluster_announce(&node(Some("::1"), false), None, 0, no_probe).unwrap();
  assert_eq!(addr, "::1");
  assert_eq!(port, 7000);

  let (addr, port) =
    resolve_cluster_announce(&node(Some("[::1]"), false), None, 0, no_probe).unwrap();
  assert_eq!(addr, "::1");
  assert_eq!(port, 7000);
}

#[test]
fn probe_failure_refuses_startup() {
  let err = resolve_cluster_announce(&node(None, false), None, 0, |_| {
    Err(io::Error::new(
      io::ErrorKind::NetworkUnreachable,
      "no route",
    ))
  })
  .unwrap_err();
  assert!(matches!(err, Error::AnnounceProbe(_)));
}

#[test]
fn announce_ip_matches_any_listen_with_default_port() {
  let (addr, port) = resolve_cluster_announce(
    &node(None, false),
    Some("10.0.0.9"),
    0,
    fake_probe(IpAddr::from(Ipv4Addr::new(10, 0, 0, 9))),
  )
  .unwrap();
  assert_eq!(addr, "10.0.0.9");
  assert_eq!(port, 7000);
}

#[test]
fn announce_ip_equal_listen_ok() {
  let (addr, port) = resolve_cluster_announce(
    &node(Some("192.168.1.5"), false),
    Some("192.168.1.5"),
    7000,
    no_probe,
  )
  .unwrap();
  assert_eq!(addr, "192.168.1.5");
  assert_eq!(port, 7000);
}

#[test]
fn announce_ip_mismatch_refuses() {
  let err = resolve_cluster_announce(
    &node(Some("127.0.0.1"), true),
    Some("10.0.0.9"),
    0,
    no_probe,
  )
  .unwrap_err();
  assert!(matches!(err, Error::AnnounceMismatch));
}

#[test]
fn announce_port_mismatch_refuses() {
  // C# :805 校验端口必须等于某监听端点端口，监听端点恒持 node.port
  let err = resolve_cluster_announce(
    &node(None, false),
    Some("10.0.0.9"),
    7001,
    fake_probe(IpAddr::from(Ipv4Addr::new(10, 0, 0, 9))),
  )
  .unwrap_err();
  assert!(matches!(err, Error::AnnounceMismatch));
}

#[test]
fn announce_hostname_gate_refuses_unknown_host() {
  // 非本机主机名不做 DNS 直拒（C# TryCreateEndpoint 机器名等价门）
  let err = resolve_cluster_announce(
    &node(None, false),
    Some("definitely-not-this-host"),
    0,
    no_probe,
  )
  .unwrap_err();
  assert!(matches!(err, Error::AnnounceMismatch));
}

#[test]
fn invalid_bind_entry_refuses_with_csharp_message() {
  // UDS 形态条目混入 bind 列表即拒启（C# TryParseAddressList 对非 IP
  // 非本机主机名的 null 臂同桶）；选 uds 前缀确保不依赖网络/DNS
  let err = resolve_cluster_announce(&node(Some("./x.sock"), true), None, 0, no_probe).unwrap_err();
  assert!(matches!(err, Error::InvalidArgument(_)));
}
