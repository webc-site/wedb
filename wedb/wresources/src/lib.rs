//! 内嵌命令资源（对标 garnet/libs/resources/，C# Garnet.resources 程序集
//! 以 EmbeddedResource 承载的两份 JSON；rust 侧单一真值文件，解析方各自
//! include 本 crate 常量，杜绝副本）

/// 命令元数据（C# Garnet.resources:RespCommandsInfo.json）
pub const RESP_COMMANDS_INFO_JSON: &str = include_str!("../RespCommandsInfo.json");

/// 命令文档（C# Garnet.resources:RespCommandsDocs.json）
pub const RESP_COMMANDS_DOCS_JSON: &str = include_str!("../RespCommandsDocs.json");
