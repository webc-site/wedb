//! 主存变长输入信息（对标 libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs）
//!
//! C# 侧为 Tsavorite 变长分配回调（计算 RMW 初始/修改值与 Upsert 的物理
//! 长度）；wkv 值定界由记录头承担，此处提供字节数估计供配额与统计复用。

/// RMW 初始值物理长度估计（值长 + 1 字节信封标签余量）
///
/// libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs:GetRMWInitialFieldInfo
pub fn get_rmw_initial_field_info(value_len: usize) -> usize {
  value_len + 1
}

/// RMW 修改值物理长度估计
///
/// libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs:GetRMWModifiedFieldInfo
pub fn get_rmw_modified_field_info(value_len: usize) -> usize {
  value_len + 1
}

/// Upsert 值物理长度估计
///
/// libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs:GetUpsertFieldInfo
pub fn get_upsert_field_info(value_len: usize) -> usize {
  value_len + 1
}

/// 是否为合法 i64 数字文本
///
/// libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs:IsValidNumber
pub fn is_valid_number(bytes: &[u8]) -> bool {
  super::super::mainstore::private_methods::is_valid_number(bytes)
}

/// 是否为合法有限双精度文本
///
/// libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs:IsValidDouble
pub fn is_valid_double(bytes: &[u8]) -> bool {
  super::super::mainstore::private_methods::is_valid_double(bytes)
}
