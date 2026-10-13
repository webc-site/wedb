#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 客户端配置与 SNI 主机名解析集成测试

use wtls::client::server_name;

/// server_name 解析：目标优先、空回落 endpoint host 段、IPv6 剥方括号、
/// IP 字面量与非法主机
#[test]
fn server_name_resolution() {
  assert_eq!(
    server_name("node1.cluster", "10.0.0.1:7000").unwrap(),
    "node1.cluster"
  );
  assert_eq!(server_name("", "10.0.0.1:7000").unwrap(), "10.0.0.1");
  assert_eq!(server_name("", "[::1]:7000").unwrap(), "::1");
  assert_eq!(server_name("", "host.example:443").unwrap(), "host.example");
  assert!(server_name("", "").is_err(), "双空无可回落目标必拒");
}
