#![cfg(not(feature = "tls"))]

use wconf::ConfigFileArgs;
use wedb::{ClusterArgs, Error as WedbError, run_cluster_server};
use wnode::Error as WnodeError;

#[test]
fn test_run_cluster_server_rejects_tls_args_without_feature() {
  for flag in [
    "--tls-cert",
    "--tls-key",
    "--tls-issuer-cert",
    "--tls-client-target-host",
  ] {
    let args = ClusterArgs::from_args_iter(["wedb", flag, "test_val"]).unwrap();
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
  }
}
