#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 服务器监听端点解析集成测（TCP / UDS / localhost 双回环 / IPv6 规范化）
//!
//! 自 `wnode/src/endpoint.rs` 内联测模块迁入（纯 pub API 面：`ServerEndpoint::parse_many`）。

use wnode::ServerEndpoint;

#[test]
fn test_parse_endpoints() {
  let ep_tcp = ServerEndpoint::parse_many("127.0.0.1:6379")
    .unwrap()
    .remove(0);
  assert!(matches!(ep_tcp, ServerEndpoint::Tcp(_)));
  assert_eq!(ep_tcp.to_string(), "127.0.0.1:6379");

  let ep_tcp_port_only = ServerEndpoint::parse_many(":6379").unwrap().remove(0);
  assert!(matches!(ep_tcp_port_only, ServerEndpoint::Tcp(_)));
  assert_eq!(ep_tcp_port_only.to_string(), "0.0.0.0:6379");

  let ep_v6 = ServerEndpoint::parse_many("[::1]:6379").unwrap().remove(0);
  assert!(matches!(ep_v6, ServerEndpoint::Tcp(_)));
  assert_eq!(ep_v6.to_string(), "[::1]:6379");

  let ep_v6_unbracketed = ServerEndpoint::parse_many("::1:6379").unwrap().remove(0);
  assert!(matches!(ep_v6_unbracketed, ServerEndpoint::Tcp(_)));
  assert_eq!(ep_v6_unbracketed.to_string(), "[::1]:6379");

  let ep_v6_any = ServerEndpoint::parse_many(":::6379").unwrap().remove(0);
  assert!(matches!(ep_v6_any, ServerEndpoint::Tcp(_)));
  assert_eq!(ep_v6_any.to_string(), "[::]:6379");

  let ep_v6_full = ServerEndpoint::parse_many("0:0:0:0:0:0:0:1:6379")
    .unwrap()
    .remove(0);
  assert!(matches!(ep_v6_full, ServerEndpoint::Tcp(_)));
  assert_eq!(ep_v6_full.to_string(), "[::1]:6379");

  let ep_uds = ServerEndpoint::parse_many("/tmp/wedb.sock")
    .unwrap()
    .remove(0);
  assert!(matches!(ep_uds, ServerEndpoint::Unix(_)));
  assert_eq!(ep_uds.to_string(), "unix:/tmp/wedb.sock");

  let ep_prefix = ServerEndpoint::parse_many("unix:/var/run/test.sock")
    .unwrap()
    .remove(0);
  assert_eq!(ep_prefix.to_string(), "unix:/var/run/test.sock");
}

#[test]
fn test_parse_many_localhost() {
  let eps = ServerEndpoint::parse_many("localhost:6379").unwrap();
  assert_eq!(eps.len(), 2);
  assert_eq!(eps[0].to_string(), "127.0.0.1:6379");
  assert_eq!(eps[1].to_string(), "[::1]:6379");
}
