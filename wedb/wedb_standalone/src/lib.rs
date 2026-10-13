use clap::{ArgMatches, Parser};
use wconf::{ConfigFileArgs, NodeArgs, NodeOptionsError, ServerArgs};

/// WeDB 单机参数配置
#[derive(Debug, Parser)]
#[command(author, version, about = "WeDB Standalone 单机服务节点")]
pub struct StandaloneArgs {
  /// 节点通用参数（端口、工作目录、线程、WAL路径等）
  #[command(flatten)]
  pub node: NodeArgs,
}

impl ServerArgs for StandaloneArgs {
  #[inline]
  fn node_args(&self) -> &NodeArgs {
    &self.node
  }
}

impl ConfigFileArgs for StandaloneArgs {
  fn from_layered_matches(matches: &ArgMatches) -> Result<Self, NodeOptionsError> {
    Ok(Self {
      node: NodeArgs::from_layered_matches(matches)?,
    })
  }
}
