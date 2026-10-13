//! 命令元数据 JSON 供给（对标 libs/server/Resp/RespCommandDataProvider.cs
//! 与 RespCommandDataCommon.cs）
//!
//! C# 以 `IRespCommandsDataProvider` 抽象 JSON 数组的导入 / 导出（默认实现
//! `DefaultRespCommandsDataProvider` 单例，`RespCommandDataCommon` 为便捷
//! 入口）；Rust 侧折叠为泛型自由函数单点，导入校验（空名 / 重名）与 C#
//! 逐条对应。[`crate::catalog`] 的命令目录与命令文档均经此单点导入内嵌
//! JSON。导出口（C# TryExportRespCommandsData）不复刻：rust 命令目录为
//! 编译期内嵌 JSON 单向导入，无落盘导出需求（形状差异，非缺实现）。
//!
//! 自研依据: 命令目录数据供给（编译期生成，C# 对应静态目录初始化）

use serde::de::DeserializeOwned;

/// libs/server/Resp/RespCommandDataProvider.cs:IRespCommandData
///
/// 命令元数据的最小面（C# IRespCommandData）
pub trait IRespCommandData: Sized {
  /// 命令名（C# Name；子命令为 `ACL|CAT` 形式）
  fn name(&self) -> &str;
}

/// 导入 JSON 数组并做名称校验，返回全量条目（保持声明序）
///
/// libs/server/Resp/RespCommandDataCommon.cs:TryImportRespCommandsData
/// libs/server/Resp/RespCommandDataProvider.cs:TryImportRespCommandsData
///（C# 经 `IRespCommandsDataProvider` 单例转发，TryImportRespCommandsData；
/// Rust 折叠工厂与默认实现为自由函数单点）。C# 侧返回以 Name
///（OrdinalIgnoreCase）为键的只读字典，消费方仅按值迭代；Rust 侧直接给出
/// 有序 Vec，名称唯一性校验保持一致。
pub fn try_import_resp_commands_data<T: IRespCommandData + DeserializeOwned>(
  json: &str,
) -> Option<Vec<T>> {
  let entries: Vec<T> = sonic_rs::from_str(json).ok()?;

  // 名称为空 / 重复即导入失败（C# LogError + return false）
  let mut seen: Vec<&str> = Vec::with_capacity(entries.len());
  for entry in &entries {
    let name = entry.name();
    if name.trim().is_empty() {
      return None;
    }
    // 重名按大小写不敏感判定（C# OrdinalIgnoreCase 字典语义；一次性
    // 导入，条目量数百级，线性扫零分配）
    if seen.iter().any(|k| k.eq_ignore_ascii_case(name)) {
      return None;
    }
    seen.push(name);
  }

  Some(entries)
}
