#![recursion_limit = "256"]
#![cfg(not(feature = "tls"))]

use wconf::ConfigFileArgs;
use wedb::{ClusterArgs, Error as WedbError, run_cluster_server};
use wnode::Error as WnodeError;

/// 无 tls 特性时 --tls-* 旗标拒启端到端单册（原 error_sink_tests.rs boot
/// 用例的 Node 变体路由断言并于此）：全量 InvalidArgument + 「未启用 tls
/// 特性」文案，boot 错误经 [`WedbError::Node`] 透明变体透传可 match
#[test]
fn test_run_cluster_server_rejects_tls_args_without_feature() {
  let tmp = tempfile::tempdir().unwrap();
  let dir = tmp.path().to_str().unwrap();
  for flag in [
    "--tls-cert",
    "--tls-key",
    "--tls-issuer-cert",
    "--tls-client-target-host",
  ] {
    let args = ClusterArgs::from_args_iter(["wedb", "--dir", dir, flag, "test_val"]).unwrap();
    let res = run_cluster_server(args, None);
    assert!(res.is_err(), "flag {flag} 必须拒绝启动");
    match res {
      Err(WedbError::Node(WnodeError::InvalidArgument(msg))) => {
        assert!(
          msg.contains("未启用 tls 特性"),
          "错误信息应包含未启用 tls 说明: {msg}"
        );
      }
      other => panic!("预期 wedb::Error::Node(wnode::Error::InvalidArgument)，实际为: {other:?}"),
    }

    // Node 变体路由断言（error_sink_tests.rs boot 用例并入的同款夹具：
    // 无 --dir 形态；guard_no_tls 在 wnode run_async 首步，先于目录装配，
    // 同门拒启且错误经 Node 变体透传）
    let args = ClusterArgs::from_args_iter(["wedb", flag, "test_val"]).unwrap();
    let err = run_cluster_server(args, None).expect_err("无 dir 形态须同样拒绝启动");
    assert!(
      matches!(err, WedbError::Node(WnodeError::InvalidArgument(_))),
      "TLS 门禁拒启错误须经 Node 变体透传: {err:?}"
    );
  }
}
