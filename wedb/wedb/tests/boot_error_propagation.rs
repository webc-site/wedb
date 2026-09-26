//! 验证 run_cluster_server 启动错误向 wedb::Error::Node 透传契约

use tempfile::tempdir;
use wconf::ConfigFileArgs;
use wedb::{ClusterArgs, Error, run_cluster_server};
use wnode::Error as WnodeError;

/// 验证非法端点解析错误透明透传为 Error::Node(wnode::Error::AddrParse)
#[test]
fn test_boot_error_propagation_empty_endpoint() {
  let dir = tempdir().unwrap();
  let args = ClusterArgs::from_args_iter([
    "wedb",
    "--port",
    "0",
    "--bind",
    " , ",
    "--dir",
    &dir.path().to_string_lossy(),
  ])
  .unwrap();

  let res = run_cluster_server(args, None);
  assert!(res.is_err(), "空端点必须拒启");
  match res {
    Err(Error::Node(WnodeError::AddrParse(msg))) => {
      assert!(
        msg.contains("监听端点列表为空"),
        "错误信息应指明端点为空: {msg}"
      );
    }
    other => panic!("预期 Error::Node(wnode::Error::AddrParse)，实际为: {other:?}"),
  }
}

/// 验证非法 gossip 参数错误透明透传为 Error::Node(wnode::Error::InvalidArgument)
#[test]
fn test_boot_error_propagation_invalid_gossip_fraction() {
  let dir = tempdir().unwrap();
  let args = ClusterArgs::from_args_iter([
    "wedb",
    "--port",
    "0",
    "--dir",
    &dir.path().to_string_lossy(),
    "--gossip-sample-percent",
    "150",
  ])
  .unwrap();

  let res = run_cluster_server(args, None);
  assert!(res.is_err(), "非法 gossip 百分比必须拒启");
  match res {
    Err(Error::Node(WnodeError::InvalidArgument(msg))) => {
      assert!(
        msg.contains("Gossip sample fraction"),
        "错误信息应指明 gossip 参数越界: {msg}"
      );
    }
    other => panic!("预期 Error::Node(wnode::Error::InvalidArgument)，实际为: {other:?}"),
  }
}

/// 验证 wnode::Error 到 wedb::Error 的 From 转换链编译期与运行期契约
#[test]
fn test_wedb_error_from_wnode_error() {
  let node_err = WnodeError::Stopped;
  let wedb_err: Error = node_err.into();
  match wedb_err {
    Error::Node(WnodeError::Stopped) => {}
    other => panic!("预期 Error::Node(wnode::Error::Stopped)，实际为: {other:?}"),
  }
}
