use super::{
  garnet_info_metrics::{ALL_INFO_SET, DEFAULT_INFO, GarnetInfoMetrics, InfoProvider},
  info_help::InfoHelp,
};
use crate::metrics::{info_metrics_type::InfoMetricsType, resp_write_utils::RespWriteUtils};

/// INFO 命令的响应编码（对标
/// libs/server/Metrics/Info/InfoCommand.cs:InfoCommand，
/// C# 内嵌于 RespServerSession partial）。
///
/// 会话缓冲管理（`SendAndReset` 循环）与 activeDbId 取值属会话域；
/// 此处以 `(参数切片, dbId, provider, 复位回调)` 的纯函数形式承接语义。
pub struct InfoCommand;

impl InfoCommand {
  /// libs/server/Metrics/Info/InfoCommand.cs:NetworkINFO
  ///
  /// 段解析：RESET / HELP / ALL / DEFAULT / EVERYTHING / 具体段名
  ///（ASCII 大小写不敏感）；非法段整批报错。无参数 = DEFAULT 集合。
  /// `set_reset_flag` 承接 monitor.resetEventFlags[STATS] 置位。
  pub fn network_info(
    args: &[&[u8]],
    db_id: i32,
    provider: &dyn InfoProvider,
    info: &mut GarnetInfoMetrics,
    set_reset_flag: &mut impl FnMut(InfoMetricsType),
    output: &mut String,
  ) {
    let mut sections: Vec<InfoMetricsType> = Vec::new();
    let mut reset = false;
    let mut help = false;
    let mut invalid_section: Option<String> = None;

    for arg in args {
      if arg.eq_ignore_ascii_case(InfoHelp::RESET.as_bytes()) {
        reset = true;
      } else if arg.eq_ignore_ascii_case(InfoHelp::HELP.as_bytes()) {
        help = true;
      } else if arg.eq_ignore_ascii_case(InfoHelp::ALL.as_bytes()) {
        let merged: Vec<_> = ALL_INFO_SET
          .iter()
          .copied()
          .filter(|s| !sections.contains(s))
          .collect();
        sections.extend(merged);
      } else if arg.eq_ignore_ascii_case(InfoHelp::DEFAULT.as_bytes())
        || arg.eq_ignore_ascii_case(InfoHelp::EVERYTHING.as_bytes())
      {
        let merged: Vec<_> = DEFAULT_INFO
          .iter()
          .copied()
          .filter(|s| !sections.contains(s))
          .collect();
        sections.extend(merged);
      } else if let Some(section_type) = Self::try_get_info_metrics_type(arg) {
        if !sections.contains(&section_type) {
          sections.push(section_type);
        }
      } else {
        invalid_section = Some(String::from_utf8_lossy(arg).into_owned());
        break;
      }
    }

    if let Some(invalid) = invalid_section {
      output.push_str(&format!(
        "-ERR Invalid section {invalid}. Try INFO HELP\r\n"
      ));
      return;
    }

    if help {
      Self::get_help_message(output);
    } else if reset {
      set_reset_flag(InfoMetricsType::Stats);
      output.push_str(&RespWriteUtils::simple_string("OK"));
    } else {
      let sections = if sections.is_empty() {
        DEFAULT_INFO.to_vec()
      } else {
        sections
      };
      let info_text = info.get_resp_info(&sections, db_id, provider);
      if info_text.is_empty() {
        output.push_str("$-1\r\n");
      } else {
        output.push_str(&format!("${}\r\n{info_text}\r\n", info_text.len()));
      }
    }
  }

  /// libs/server/Metrics/Info/InfoCommand.cs:GetHelpMessage
  ///
  /// 输出 INFO 帮助文本数组（批量串形式）。
  pub fn get_help_message(output: &mut String) {
    let sections_help = InfoHelp::get_info_type_help_message();
    output.push_str(&RespWriteUtils::array_length(sections_help.len()));
    for section_info in sections_help {
      output.push_str(&RespWriteUtils::bulk_string(&section_info));
    }
  }

  /// 段名解析（ASCII 大小写不敏感；对齐
  /// SessionParseStateExtensions.TryGetInfoMetricsType 的解析语义）。
  fn try_get_info_metrics_type(arg: &[u8]) -> Option<InfoMetricsType> {
    InfoMetricsType::ALL
      .iter()
      .copied()
      .find(|t| t.as_cs_name().as_bytes().eq_ignore_ascii_case(arg))
  }
}

#[cfg(test)]
mod tests {
  use std::cell::Cell;

  use super::{InfoCommand, InfoMetricsType};
  use crate::metrics::{
    info::garnet_info_metrics::{
      DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot, InfoProvider, ServerFacts,
    },
    metrics_item::MetricsItem,
  };

  /// 最小 provider 桩：单库、无监视器。
  struct MockProvider;

  impl InfoProvider for MockProvider {
    fn server_facts(&self) -> ServerFacts {
      ServerFacts {
        version: "1.0.0".into(),
        run_id: "run123".into(),
        redis_protocol_version: "7.0".into(),
        enable_cluster: false,
        enable_aof: false,
        metrics_sampling_frequency: 10,
        latency_monitor: false,
        command_stats_monitor: false,
        startup_timestamp_unix_secs: 0,
        log_dir: "/tmp/log".into(),
      }
    }

    fn databases(&self) -> Vec<DbSnapshot> {
      vec![DbSnapshot {
        id: 0,
        current_version: 7,
        ..DbSnapshot::default()
      }]
    }

    fn max_database_id(&self) -> i32 {
      0
    }

    fn global_metrics(&self) -> Option<GlobalMetricsSnapshot> {
      None
    }

    fn command_stats(&self) -> Vec<(String, u64, u64)> {
      Vec::new()
    }

    fn keyspace_stats(&self, _db_id: i32) -> (u64, u64) {
      (0, 0)
    }

    fn replication_info(&self) -> Option<Vec<MetricsItem>> {
      None
    }

    fn gossip_stats(&self, _metrics_disabled: bool) -> Vec<MetricsItem> {
      Vec::new()
    }

    fn buffer_pool_stats(&self) -> Vec<(String, String)> {
      Vec::new()
    }

    fn checkpoint_info(&self) -> Option<Vec<MetricsItem>> {
      None
    }

    fn hlog_scan_dump(&self) -> Vec<(String, String)> {
      Vec::new()
    }

    fn safe_aof_address(&self) -> i64 {
      0
    }
  }

  #[test]
  fn help_renders_bulk_array() {
    let mut out = String::new();
    let mut info = GarnetInfoMetrics::new();
    let mut reset_flag = |_| {};
    InfoCommand::network_info(
      &[b"HELP"],
      0,
      &MockProvider,
      &mut info,
      &mut reset_flag,
      &mut out,
    );
    assert!(out.starts_with("*20\r\n$14\r\n# Info options\r\n"));
  }

  #[test]
  fn invalid_section_errors() {
    let mut out = String::new();
    let mut info = GarnetInfoMetrics::new();
    let mut reset_flag = |_| {};
    InfoCommand::network_info(
      &[b"BOGUS"],
      0,
      &MockProvider,
      &mut info,
      &mut reset_flag,
      &mut out,
    );
    assert_eq!(out, "-ERR Invalid section BOGUS. Try INFO HELP\r\n");
  }

  #[test]
  fn reset_replies_ok_and_sets_flag() {
    let mut out = String::new();
    let mut info = GarnetInfoMetrics::new();
    let flag = Cell::new(false);
    let mut reset_flag = |t: InfoMetricsType| {
      assert_eq!(t, InfoMetricsType::Stats);
      flag.set(true);
    };
    InfoCommand::network_info(
      &[b"reset"],
      0,
      &MockProvider,
      &mut info,
      &mut reset_flag,
      &mut out,
    );
    assert_eq!(out, "+OK\r\n");
    assert!(flag.get());
  }

  #[test]
  fn default_sections_render_server_info() {
    let mut out = String::new();
    let mut info = GarnetInfoMetrics::new();
    let mut reset_flag = |_| {};
    InfoCommand::network_info(&[], 0, &MockProvider, &mut info, &mut reset_flag, &mut out);
    assert!(out.contains("# Server\r\n"));
    assert!(out.contains("garnet_version:1.0.0\r\n"));
    assert!(out.contains("server_name:garnet\r\n"));
  }

  #[test]
  fn single_section_case_insensitive() {
    let mut out = String::new();
    let mut info = GarnetInfoMetrics::new();
    let mut reset_flag = |_| {};
    InfoCommand::network_info(
      &[b"server"],
      0,
      &MockProvider,
      &mut info,
      &mut reset_flag,
      &mut out,
    );
    assert!(out.contains("# Server\r\n"));
    assert!(!out.contains("# Memory\r\n"));
  }
}
