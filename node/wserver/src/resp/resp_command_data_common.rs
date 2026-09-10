//! 命令元数据导入入口（对标 libs/server/Resp/RespCommandDataCommon.cs）
//!
//! C# 从 Garnet.resources 内嵌资源读取 JSON 后交默认供给解析；Rust 侧
//! 调用方直接传入 `include_str!` 的内嵌文本，校验链一致。

use serde::de::DeserializeOwned;

use super::resp_command_data_provider::{IRespCommandData, get_resp_commands_data_provider};

/// 安全导入命令元数据（空名 / 重名 / 反序列化失败一律 None）
///
/// libs/server/Resp/RespCommandDataCommon.cs:TryImportRespCommandsData
pub(crate) fn try_import_resp_commands_data<T: IRespCommandData + DeserializeOwned>(
  json: &str,
) -> Option<Vec<T>> {
  get_resp_commands_data_provider().try_import_resp_commands_data(json)
}
