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
  pub const HELP_LINES: &'static [&'static str] = &[
    "# Info options",
    "SERVER: General information about the Garnet instance.",
    "MEMORY: Server memory usage information.",
    "CLUSTER: Cluster instance specific operational info.",
    "REPLICATION: Replication info.",
    "STATS: General server operational stats.",
    "STORE: Main store operational information.",
    "STOREHASHTABLE: Hash table distribution info for main store (expensive, not returned by default).",
    "STOREREVIV: Revivification info for deleted records in main store (not returned by default).",
    "PERSISTENCE: Persistence related information (i.e. Checkpoint and AOF).",
    "CLIENTS: Information related to client connections.",
    "KEYSPACE: Database related statistics.",
    "MODULES: Information related to loaded modules.",
    "HLOGSCAN: Distribution of records in main store's hybrid log in-memory portion.",
    "ALL: Return all informational sections (excluding module generated ones).",
    "DEFAULT: Return the default set of informational sections.",
    "EVERYTHING: Return all informational sections including module generated ones.",
    "HELP: Print this help message.",
    "RESET: Reset stats.",
    "\r\n",
  ];

  #[inline]
  pub fn get_info_type_help_message() -> &'static [&'static str] {
    Self::HELP_LINES
  }
}

#[cfg(test)]
mod tests {
  #[test]
  fn help_message_contents() {
    let lines = super::InfoHelp::get_info_type_help_message();
    assert_eq!(lines.len(), 20);
    assert_eq!(lines[0], "# Info options");
    assert_eq!(*lines.last().unwrap(), "\r\n");
  }
}
