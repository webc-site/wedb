//! 集群互信凭据启动接线集成测试（案四）
//!
//! 对标 garnet/libs/host/Configuration/Options.cs:177-182
//! `--cluster-username` / `[HiddenOption] --cluster-password` →
//! GarnetServerOptions.cs:266/:271 → ClusterProvider.cs:56-57 构造期注入
//! AuthContainer。覆盖两处落地：
//! 1. CLI 参数暴露：ClusterArgs（flatten NodeArgs）解析二旋钮进启动配置；
//! 2. 启动装配注入：boot 构造阶段以启动参数经 ClusterProvider::update_cluster_auth
//!    播种 AuthContainer，gossip / 复制 / failover 出站握手经 cluster_username/
//!    cluster_password 访问器即取到有效凭据（单源 auth_container，无第二张表）。

use std::sync::Arc;

use clap::Parser;
use wedb::{ClusterArgs, server::cluster_provider::ClusterProvider};

/// CLI 二旋钮暴露：`--cluster-username` / `--cluster-password` 落入 NodeArgs
/// 真源（ClusterArgs flatten node 段），未配置为 None（明文集群行为不变）
#[test]
fn cluster_auth_cli_flags_land_on_node_args() {
  let args = ClusterArgs::try_parse_from([
    "wedb",
    "--cluster-username",
    "wedb",
    "--cluster-password",
    "secret",
  ])
  .expect("parse cluster auth args");
  assert_eq!(args.node.cluster_username.as_deref(), Some("wedb"));
  assert_eq!(args.node.cluster_password.as_deref(), Some("secret"));

  // 缺省：二旋钮均未配置即 None
  let default = ClusterArgs::try_parse_from(["wedb"]).expect("parse default");
  assert_eq!(default.node.cluster_username, None);
  assert_eq!(default.node.cluster_password, None);
}

/// 启动装配注入：boot 构造阶段调用的 update_cluster_auth（与运行期 CONFIG SET
/// 共用单点）将启动凭据播种进 AuthContainer，出站访问器即时取到
#[test]
fn cluster_auth_startup_seeds_auth_container() {
  let provider = Arc::new(ClusterProvider::new());
  // 初始明文集群：二访问器恒 None
  assert_eq!(provider.cluster_username(), None);
  assert_eq!(provider.cluster_password(), None);

  // 模拟 boot.rs 装配尾段以启动参数播种（对位 C# 构造期注入 AuthContainer）
  provider.update_cluster_auth(Some("wedb".to_owned()), Some("secret".to_owned()));
  assert_eq!(provider.cluster_username().as_deref(), Some("wedb"));
  assert_eq!("secret", provider.cluster_password().as_deref().unwrap());

  // 仅改密码沿用旧用户名（C# `clusterUsername ?? oldAuthContainer.ClusterUsername`）
  provider.update_cluster_auth(None, Some("rotated".to_owned()));
  assert_eq!(provider.cluster_username().as_deref(), Some("wedb"));
  assert_eq!(
    provider.cluster_password().as_deref(),
    Some("rotated"),
    "只给 password 须复用旧 username"
  );
}
