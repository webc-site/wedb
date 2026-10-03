/// 支持的 LATENCY 命令及简述
///（对标 libs/server/Metrics/Latency/RespLatencyHelp.cs:RespLatencyHelp）。
pub struct RespLatencyHelp;

impl RespLatencyHelp {
  /// 支持的子命令帮助行
  pub const COMMANDS: [&'static str; 9] = [
    "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "HISTOGRAM [EVENT [EVENT...]]",
    "\tReturn latency histogram of one or more <event> classes.",
    "\tIf no commands are specified then all histograms are replied",
    "RESET [EVENT [EVENT...]]",
    "\tReset latency data of one or more <event> classes.",
    "\t(default: reset all data for all event classes).",
    "HELP",
    "\tPrints this help",
  ];

  /// libs/server/Metrics/Latency/RespLatencyHelp.cs:GetLatencyCommands
  #[inline]
  pub const fn get_latency_commands() -> &'static [&'static str] {
    &Self::COMMANDS
  }
}
