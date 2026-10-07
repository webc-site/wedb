//! 端点形态判定测试（UDS 路径规则 / 回环地址判定，自 tests/main.rs 迁入）

/// 端点形态判定单源规则（入站监听解析与出站建连共读此一条）
#[test]
fn test_endpoint_uds_path_rule() {
  use std::path::Path;

  use wbase::endpoint::uds_path;

  // 显式前缀形态：剥 `unix:` 取其后的路径
  assert_eq!(
    uds_path("unix:/var/run/wedb.sock"),
    Some(Path::new("/var/run/wedb.sock"))
  );
  assert_eq!(uds_path("unix:"), Some(Path::new("")));
  // 裸路径三形态：绝对路径、相对路径、`.sock` 后缀
  assert_eq!(
    uds_path("/tmp/wedb.sock"),
    Some(Path::new("/tmp/wedb.sock"))
  );
  assert_eq!(uds_path("/tmp/wedb"), Some(Path::new("/tmp/wedb")));
  assert_eq!(
    uds_path("./run/wedb.sock"),
    Some(Path::new("./run/wedb.sock"))
  );
  assert_eq!(
    uds_path("we/call/wedb.sock"),
    Some(Path::new("we/call/wedb.sock"))
  );
  // 前后空白归一
  assert_eq!(
    uds_path("  unix:/tmp/a.sock  "),
    Some(Path::new("/tmp/a.sock"))
  );
  // TCP 形态一律 None（含 `:port` 简写与裸主机串）
  assert_eq!(uds_path("127.0.0.1:6379"), None);
  assert_eq!(uds_path("[::1]:6379"), None);
  assert_eq!(uds_path(":6379"), None);
  assert_eq!(uds_path("localhost"), None);
  assert_eq!(uds_path(""), None);
}

/// typed 回环判定（对位 C# IPAddress.IsLoopback：127.0.0.0/8 全段、::1、
/// v4-mapped IPv6 先解映射再按 IPv4 口径判）
#[test]
fn test_endpoint_ip_is_loopback() {
  use std::net::SocketAddr;

  use wbase::endpoint::ip_is_loopback;

  let addr = |s: &str| s.parse::<SocketAddr>().unwrap();
  // IPv4 回环全段
  assert!(ip_is_loopback(addr("127.0.0.1:6379")));
  assert!(ip_is_loopback(addr("127.1.2.3:1")));
  // IPv6 回环
  assert!(ip_is_loopback(addr("[::1]:6379")));
  // v4-mapped IPv6：解映射后按 IPv4 口径（旧字符串前缀判据的漏判态）
  assert!(ip_is_loopback(addr("[::ffff:127.0.0.1]:6379")));
  assert!(ip_is_loopback(addr("[::ffff:127.9.9.9]:1")));
  // 非回环全拒（拒向语义不扩大敞口）
  assert!(!ip_is_loopback(addr("10.0.0.9:1234")));
  assert!(!ip_is_loopback(addr("0.0.0.0:1")));
  assert!(!ip_is_loopback(addr("[::ffff:10.0.0.1]:6379")));
  assert!(!ip_is_loopback(addr("[fe80::1]:6379")));
  assert!(!ip_is_loopback(addr("[::2]:1")));
}
