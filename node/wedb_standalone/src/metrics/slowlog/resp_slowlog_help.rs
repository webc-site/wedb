/// 支持的 SLOWLOG 命令及简述
///（对标 libs/server/Metrics/Slowlog/RespSlowlogHelp.cs:RespSlowlogHelp）。
pub struct RespSlowlogHelp;

impl RespSlowlogHelp {
  /// libs/server/Metrics/Slowlog/RespSlowlogHelp.cs:GetSlowLogCommands
  pub fn get_slow_log_commands() -> Vec<&'static str> {
    vec![
      "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
      "GET [<count>]",
      "\tReturn top <count> entries from the slowlog (default: 10, -1 mean all).",
      "\tEntries are made of:",
      "\tid, timestamp, time in microseconds, arguments array, client IP and port,",
      "\tclient name",
      "LEN",
      "\tReturn the length of the slowlog.",
      "RESET",
      "\tReset the slowlog.",
      "HELP",
      "\tPrints this help",
    ]
  }
}

#[cfg(test)]
mod tests {
  #[test]
  fn help_lists_eleven_lines() {
    let lines = super::RespSlowlogHelp::get_slow_log_commands();
    assert_eq!(lines.len(), 12);
    assert!(lines[0].starts_with("SLOWLOG <subcommand>"));
  }
}
