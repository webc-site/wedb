use wresp::{
  cmd_strings as cs,
  ext::{RespVecExt, sanitize_error_str},
  metrics::InfoMetricsType,
};

use super::{
  garnet_info_metrics::{ALL_INFO_SET, DEFAULT_INFO, GarnetInfoMetrics, InfoProvider},
  info_help::InfoHelp,
};

/// INFO 命令的响应编码（对标
/// libs/server/Metrics/Info/InfoCommand.cs，
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
    output: &mut Vec<u8>,
  ) {
    let mut sections: Vec<InfoMetricsType> = Vec::new();
    let mut reset = false;
    let mut help = false;
    let mut invalid_section: Option<&[u8]> = None;

    for arg in args {
      if arg.eq_ignore_ascii_case(InfoHelp::RESET.as_bytes()) {
        reset = true;
      } else if arg.eq_ignore_ascii_case(InfoHelp::HELP.as_bytes()) {
        help = true;
      } else if arg.eq_ignore_ascii_case(InfoHelp::ALL.as_bytes()) {
        for &s in ALL_INFO_SET {
          if !sections.contains(&s) {
            sections.push(s);
          }
        }
      } else if arg.eq_ignore_ascii_case(InfoHelp::DEFAULT.as_bytes())
        || arg.eq_ignore_ascii_case(InfoHelp::EVERYTHING.as_bytes())
      {
        for &s in DEFAULT_INFO {
          if !sections.contains(&s) {
            sections.push(s);
          }
        }
      } else if let Some(section_type) = InfoMetricsType::from_name(arg) {
        if !sections.contains(&section_type) {
          sections.push(section_type);
        }
      } else {
        invalid_section = Some(arg);
        break;
      }
    }

    if let Some(invalid) = invalid_section {
      // 段名是客户原始 arg（bulk string 内的 CRLF 由解析器原样保留）：回显段
      // 先过门面净化单点 sanitize_error_str（CRLF 切断 + MAX_PARAM_NAME_LEN
      // 长度帽），整帧由 cs::abort_with_error_message 单点成帧，对标 C#
      // InfoCommand.cs:NetworkINFO 的 TryWriteError 单口
      let section_name = String::from_utf8_lossy(invalid);
      let clean = sanitize_error_str(&section_name, cs::MAX_PARAM_NAME_LEN);
      cs::abort_with_error_message(
        output,
        &format!("ERR Invalid section {clean}. Try INFO HELP"),
      );
      return;
    }

    if help {
      Self::get_help_message(output);
    } else if reset {
      set_reset_flag(InfoMetricsType::Stats);
      output.write_resp_simple_string("OK");
    } else {
      let sections_slice = if sections.is_empty() {
        DEFAULT_INFO
      } else {
        &sections[..]
      };
      let info_text = info.get_resp_info(sections_slice, db_id, provider);
      if info_text.is_empty() {
        // C# InfoCommand.cs:NetworkINFO 空段回 CmdStrings.RESP_EMPTY（
        // 协议恒定的空批量串，非 nil）
        output.extend_from_slice(cs::RESP_EMPTY);
      } else {
        output.write_resp_bulk_string(info_text.as_bytes());
      }
    }
  }

  /// libs/server/Metrics/Info/InfoCommand.cs:GetHelpMessage
  ///
  /// 输出 INFO 帮助文本数组（批量串形式）。
  pub fn get_help_message(output: &mut Vec<u8>) {
    let sections_help = InfoHelp::get_info_type_help_message();
    output.write_resp_array_len(sections_help.len());
    for section_info in sections_help {
      output.write_resp_bulk_string(section_info.as_bytes());
    }
  }
}
