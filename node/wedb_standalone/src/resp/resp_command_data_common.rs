//! 命令元数据公共导入（对标 libs/server/Resp/RespCommandDataCommon.cs）
//!
//! C# 从嵌入 JSON 导入命令元数据到静态提供者（与 RespCommandDataProvider
//! 同族）；命令元数据域（resp_command_docs / resp_commands_info）为并行
//! 推进面，本入口保持"未导入"语义返回。

/// 命令元数据公共导入
pub struct RespCommandDataCommon;

impl RespCommandDataCommon {
  /// libs/server/Resp/RespCommandDataCommon.cs:TryImportRespCommandsData
  ///
  /// 缺口说明：C# 从 JSON 导入命令元数据；rust 命令元数据域并行推进，
  /// 未导入即按 C# 导入失败口径返回 false（豁免登记见 js/check/ignore/
  /// libs/server/Resp/RespCommandDataCommon.yml）
  pub fn try_import_resp_commands_data() -> bool {
    false
  }
}
