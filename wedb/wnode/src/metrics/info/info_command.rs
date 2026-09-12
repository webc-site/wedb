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
  /// `set_reset_flag` 承接 `monitor.resetEventFlags[STATS]` 置位。
  pub fn network_info(
    args: &[&[u8]],
    db_id: i32,
    provider: &impl InfoProvider,
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
