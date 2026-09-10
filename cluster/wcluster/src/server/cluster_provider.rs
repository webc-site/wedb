use crate::server::connection_info::ConnectionInfo;

/// 在 garnet 中的相对路径:Server:ClusterProvider
#[derive(Clone)]
pub struct ClusterProvider {
  // fields will be populated later
}

impl ClusterProvider {
  pub fn get_connection_info(&self, _node_id: &str) -> ConnectionInfo {
    // stub
    ConnectionInfo::default()
  }
}
