//! GarnetServer 构造面 + start 失败臂资源回收集成测（自 src/server.rs 内联测
//! 迁入合并：真 TCP 绑定 + 目录路径 UDS 绑定必败形态，断言与覆盖原样保留）
//!
//! 观测面：`worker_thread_count` / `endpoints` 为 #[doc(hidden)] 测试专用口
//!（见定义处注）；`tcp_addrs` 空 / `shutdown_coordinator` 停止两断言改走既有
//! pub 面 `local_addr()`（空表 → NotConnected Err）与 `shutdown_coordinator()`。
//! TLS 参数拒启两测经 #[doc(hidden)] 测试专用口 `guard_no_tls` /
//! `tls_config_from_node` 直驱（各自随 tls 特性门互斥编译）。

use std::{mem::take, num::NonZeroUsize, sync::Arc};

use wconf::NodeArgs;
use wnode::{
  Error, GarnetServer,
  traits::{MessageConsumerFace, SessionProviderFace, WireFormat},
};

/// 最小哑消费者：仅满足端点解析拒启断言，不触网络
struct NullConsumer(Vec<u8>);

impl MessageConsumerFace for NullConsumer {
  fn try_consume_messages_into(&mut self, _resp_buf: &mut Vec<u8>) -> Option<usize> {
    Some(0)
  }
  fn take_recv_scratch(&mut self) -> Vec<u8> {
    take(&mut self.0)
  }
  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.0 = buf;
  }
  fn dispose(&mut self) {}
}

struct NullProvider;

impl SessionProviderFace for NullProvider {
  type Consumer = NullConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<NullConsumer> {
    Some(NullConsumer(Vec::new()))
  }
}

/// 非法端点拒启且错误信息含原字符串（对标 C# Options.cs:795-797 抛
/// GarnetException，禁静默回退默认端点）
#[test]
fn new_rejects_invalid_endpoint() {
  let err = match GarnetServer::new(&["1.2.3.4:65536".to_string()], 4096, Arc::new(NullProvider)) {
    Ok(_) => panic!("端口越界端点必须拒启"),
    Err(e) => e,
  };
  assert!(matches!(err, Error::AddrParse(_)), "实际错误: {err:?}");
  assert!(
    err.to_string().contains("1.2.3.4:65536"),
    "错误信息须点名原字符串，实际: {err}"
  );
}

/// 空端点列表拒启（C# Options.cs:796 `endpoints.Length == 0` 臂）
#[test]
fn new_rejects_empty_endpoints() {
  assert!(GarnetServer::new(&[], 4096, Arc::new(NullProvider)).is_err());
}

/// localhost 端点自动展开为 IPv4 + IPv6 双回环端点（对标 C# Format.cs:99-100）
#[test]
fn new_expands_localhost_to_dual_endpoints() {
  let server = GarnetServer::new(
    &["localhost:6379".to_string()],
    4096,
    Arc::new(NullProvider),
  )
  .unwrap();
  assert_eq!(server.endpoints.len(), 2);
  assert_eq!(server.endpoints[0].to_string(), "127.0.0.1:6379");
  assert_eq!(server.endpoints[1].to_string(), "[::1]:6379");
}

/// start 失败臂（第二端点失败）必须回收已 spawn worker 并清空监听，杜绝幽灵监听
#[test]
fn start_failure_reclaims_all_workers() {
  let temp_dir = tempfile::tempdir().unwrap();
  // 目录路径做套接字绑定必定失败
  let invalid_uds_path = temp_dir.path().to_path_buf();

  let server = GarnetServer::new(
    &[
      "127.0.0.1:0".to_string(),
      format!("unix:{}", invalid_uds_path.display()),
    ],
    4096,
    Arc::new(NullProvider),
  )
  .unwrap();

  let start_res = server.start(NonZeroUsize::new(2));
  assert!(
    start_res.is_err(),
    "第二端点 UDS 绑定失败必须导致 start 返回 Err"
  );

  // 断言资源干净回收：零存活幽灵监听（worker 句柄清空 + TCP 绑定表清空 +
  // 停机协调器已停）
  assert_eq!(server.worker_thread_count(), 0);
  assert!(
    server.local_addr().is_err(),
    "tcp_addrs 已清空，local_addr 必报未绑定"
  );
  assert!(server.shutdown_coordinator().is_stopped());
}

/// 未启用 TLS 特性时配置 TLS 相关参数拒启（防静默降级为明文）
#[cfg(not(feature = "tls"))]
#[test]
fn test_guard_no_tls_rejects_tls_args() {
  use wnode::server::guard_no_tls;

  let mut args = NodeArgs::default();
  assert!(guard_no_tls(&args).is_ok());

  args.tls_cert = Some("/tmp/cert.pem".into());
  assert!(guard_no_tls(&args).is_err());
  args.tls_cert = None;

  args.tls_key = Some("/tmp/key.pem".into());
  assert!(guard_no_tls(&args).is_err());
  args.tls_key = None;

  args.tls_issuer_cert = Some("/tmp/ca.pem".into());
  assert!(guard_no_tls(&args).is_err());
  args.tls_issuer_cert = None;

  args.tls_client_target_host = Some("example.com".into());
  assert!(guard_no_tls(&args).is_err());
}

/// TLS 证书对 + --unixsocket 组合拒启三臂锁测（对齐 guard_no_tls 形制）：
/// 证书对与 UDS 端点组合 → Err（UDS 接入环无握手位，防静默明文端点绕开
/// mTLS 身份门）；证书对仅 TCP → Ok；unixsocket 无证书 → Ok。差分变量
/// 仅 unixsocket / 证书字段，证书用 rcgen 真签真读（from_pem_files 真装载，
/// 禁假路径假过门）
#[cfg(feature = "tls")]
#[test]
fn tls_config_rejects_unixsocket_cert_combo() {
  use std::fs;

  use rcgen::generate_simple_self_signed;
  use wnode::server::tls_config_from_node;

  let dir = tempfile::tempdir().unwrap();
  let cert_path = dir.path().join("cert.pem");
  let key_path = dir.path().join("key.pem");
  let ck = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
  fs::write(&cert_path, ck.cert.pem()).unwrap();
  fs::write(&key_path, ck.signing_key.serialize_pem()).unwrap();

  // 臂一：证书对 + UDS 组合拒启，文案点名 --unixsocket 与静默明文红线
  let mut args = NodeArgs {
    tls_cert: Some(cert_path.clone()),
    tls_key: Some(key_path.clone()),
    ..Default::default()
  };
  args.unixsocket = Some(dir.path().join("wedb.sock").to_string_lossy().to_string());
  let err = match tls_config_from_node(&args) {
    Ok(_) => panic!("证书对 + unixsocket 组合必须拒启"),
    Err(e) => e,
  };
  assert!(
    matches!(err, Error::InvalidArgument(_)),
    "实际错误: {err:?}"
  );
  assert!(
    err.to_string().contains("--unixsocket") && err.to_string().contains("静默降级为明文"),
    "错误须点名 unixsocket 与静默明文红线，实际: {err}"
  );

  // 臂二：同证书对仅 TCP 端点放行（真装载 Ok(Some)）
  args.unixsocket = None;
  assert!(
    tls_config_from_node(&args).is_ok_and(|c| c.is_some()),
    "证书对无 unixsocket 组合应正常装配 TLS 配置"
  );

  // 臂三：unixsocket 无证书即明文 UDS 单端点，合法形态放行
  args.tls_cert = None;
  args.tls_key = None;
  args.unixsocket = Some(dir.path().join("wedb.sock").to_string_lossy().to_string());
  assert!(
    tls_config_from_node(&args).is_ok_and(|c| c.is_none()),
    "unixsocket 无证书应装配 None（明文 UDS 合法）"
  );
}
