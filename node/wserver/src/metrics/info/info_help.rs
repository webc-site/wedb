/// INFO 帮助文本（对标 libs/server/Metrics/Info/InfoHelp.cs:InfoHelp）。
pub struct InfoHelp;

impl InfoHelp {
  /// C# 常量 HELP。
  pub const HELP: &'static str = "HELP";
  /// C# 常量 ALL。
  pub const ALL: &'static str = "ALL";
  /// C# 常量 DEFAULT。
  pub const DEFAULT: &'static str = "DEFAULT";
  /// C# 常量 EVERYTHING。
  pub const EVERYTHING: &'static str = "EVERYTHING";
  /// C# 常量 RESET。
  pub const RESET: &'static str = "RESET";

  /// libs/server/Metrics/Info/InfoHelp.cs:GetInfoTypeHelpMessage
  pub fn get_info_type_help_message() -> Vec<String> {
    vec![
      "# Info options".into(),
      "SERVER: General information about the Garnet instance.".into(),
      "MEMORY: Server memory usage information.".into(),
      "CLUSTER: Cluster instance specific operational info.".into(),
      "REPLICATION: Replication info.".into(),
      "STATS: General server operational stats.".into(),
      "STORE: Main store operational information.".into(),
      "STOREHASHTABLE: Hash table distribution info for main store (expensive, not returned by default).".into(),
      "STOREREVIV: Revivification info for deleted records in main store (not returned by default).".into(),
      "PERSISTENCE: Persistence related information (i.e. Checkpoint and AOF).".into(),
      "CLIENTS: Information related to client connections.".into(),
      "KEYSPACE: Database related statistics.".into(),
      "MODULES: Information related to loaded modules.".into(),
      "HLOGSCAN: Distribution of records in main store's hybrid log in-memory portion.".into(),
      "ALL: Return all informational sections (excluding module generated ones).".into(),
      "DEFAULT: Return the default set of informational sections.".into(),
      "EVERYTHING: Return all informational sections including module generated ones.".into(),
      "HELP: Print this help message.".into(),
      "RESET: Reset stats.".into(),
      "\r\n".into(),
    ]
  }
}

#[cfg(test)]
mod tests {
  #[test]
  fn help_message_contents() {
    let lines = super::InfoHelp::get_info_type_help_message();
    assert_eq!(lines.len(), 20);
    assert_eq!(lines[0], "# Info options");
    assert!(lines.last().unwrap() == "\r\n");
    assert_eq!(super::InfoHelp::RESET, "RESET");
  }
}
