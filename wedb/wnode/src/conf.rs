//! 统一节点服务配置（兼容别名门面）

pub use crate::args::{
  DEFAULT_BIND, DEFAULT_DIR, DEFAULT_PORT, NodeArgs, NodeArgs as Conf, ServerArgs,
};

#[cfg(test)]
mod tests {
  use std::path::PathBuf;

  use super::*;

  #[test]
  fn test_node_config_defaults() {
    let cfg = Conf::default();
    assert_eq!(cfg.bind, "127.0.0.1");
    assert_eq!(cfg.port, 6379);
    assert_eq!(cfg.dir, PathBuf::from("./data"));
    assert_eq!(cfg.endpoints(), vec!["127.0.0.1:6379"]);
  }

  #[test]
  fn test_node_config_with_unixsocket() {
    let cfg = Conf {
      unixsocket: Some("/tmp/wedb.sock".to_string()),
      ..Default::default()
    };
    assert_eq!(
      cfg.endpoints(),
      vec!["127.0.0.1:6379", "unix:/tmp/wedb.sock"]
    );
  }

  #[test]
  fn test_nested_text_parse() {
    let nt = r#"
bind: 192.168.1.100
port: 6380
dir: /tmp/wedb_data
"#;
    let conf = Conf::from_nested_text_str(nt).unwrap();
    assert_eq!(conf.bind, "192.168.1.100");
    assert_eq!(conf.port, 6380);
    assert_eq!(conf.dir, PathBuf::from("/tmp/wedb_data"));
  }
}
