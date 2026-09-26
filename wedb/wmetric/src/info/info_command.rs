use wresp::{
  cmd_strings as cs,
  ext::{RespVecExt, is_resp3, sanitize_error_str},
  metrics::InfoMetricsType,
  resp_memory_writer::{Resp3, RespProtocol},
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

/// INFO 段集解析四态（对标 C# InfoCommand.cs:NetworkINFO 段解析循环的
/// sections/reset/help/invalid 四个累计状态；`sections` 为请求序去重后的
/// 段集，ALL/DEFAULT/EVERYTHING 关键字展开为各自段集）。
pub struct InfoSections<'a> {
  /// 已解析段集（请求序去重；无参为空 = 渲染面按 DEFAULT 呈现）。
  pub sections: Vec<InfoMetricsType>,
  /// 出现 RESET 词。
  pub reset: bool,
  /// 出现 HELP 词。
  pub help: bool,
  /// 首个非法段词前的末个非法段（C# 覆写不中断、扫尽回显末个），None = 全合法。
  pub invalid: Option<&'a [u8]>,
}

impl InfoCommand {
  /// libs/server/Metrics/Info/InfoCommand.cs:NetworkINFO 段解析臂（:31-46 段集
  /// 解析单点定义：同步渲染面与会话侧降级门共用同一判据，杜绝两处解析分叉）
  pub fn parse_sections<'a>(args: &[&'a [u8]]) -> InfoSections<'a> {
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
        // 对标 C# InfoCommand.cs:43-44：置位覆写不中断，扫尽后回显末个非法段
        invalid_section = Some(arg);
      }
    }

    InfoSections {
      sections,
      reset,
      help,
      invalid: invalid_section,
    }
  }

  /// libs/server/Metrics/Info/InfoCommand.cs:NetworkINFO 渲染出帧臂（:69-80
  /// 单点定义：同步面与含扫描段的慢路径拼装面共用同一帧型——非空段 RESP3
  /// verbatim（`txt:` 前缀）/ RESP2 bulk string，空段 RESP_EMPTY；
  /// 段序与段间分隔逻辑零改动保字节形）
  pub fn write_info_reply(
    sections: &[InfoMetricsType],
    db_id: i32,
    provider: &impl InfoProvider,
    info: &mut GarnetInfoMetrics,
    resp_protocol_version: u8,
    output: &mut Vec<u8>,
  ) {
    let info_text = info.get_resp_info(sections, db_id, provider);
    if info_text.is_empty() {
      // C# InfoCommand.cs:NetworkINFO 空段回 CmdStrings.RESP_EMPTY（
      // 协议恒定的空批量串，非 nil）
      output.extend_from_slice(cs::RESP_EMPTY);
    } else if is_resp3(resp_protocol_version) {
      Resp3::write_verbatim_string(output, info_text.as_bytes(), b"txt");
    } else {
      output.write_resp_bulk_string(info_text.as_bytes());
    }
  }

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
    resp_protocol_version: u8,
    output: &mut Vec<u8>,
  ) {
    let parsed = Self::parse_sections(args);

    if let Some(invalid) = parsed.invalid {
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

    if parsed.help {
      Self::get_help_message(output);
    } else if parsed.reset {
      set_reset_flag(InfoMetricsType::Stats);
      output.write_resp_simple_string("OK");
    } else {
      let sections = if parsed.sections.is_empty() {
        DEFAULT_INFO
      } else {
        &parsed.sections[..]
      };
      Self::write_info_reply(
        sections,
        db_id,
        provider,
        info,
        resp_protocol_version,
        output,
      );
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
