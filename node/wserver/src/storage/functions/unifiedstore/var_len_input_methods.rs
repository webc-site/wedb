//! 统一存变长输入信息（对标 libs/server/Storage/Functions/UnifiedStore/VarLenInputMethods.cs）

/// RMW 修改值物理长度估计
///
/// libs/server/Storage/Functions/UnifiedStore/VarLenInputMethods.cs:GetRMWModifiedFieldInfo
pub fn get_rmw_modified_field_info(value_len: usize) -> usize {
  value_len + 1
}

/// RMW 初始值物理长度估计
///
/// libs/server/Storage/Functions/UnifiedStore/VarLenInputMethods.cs:GetRMWInitialFieldInfo
pub fn get_rmw_initial_field_info(value_len: usize) -> usize {
  value_len + 1
}

/// Upsert 值物理长度估计
///
/// libs/server/Storage/Functions/UnifiedStore/VarLenInputMethods.cs:GetUpsertFieldInfo
pub fn get_upsert_field_info(value_len: usize) -> usize {
  value_len + 1
}
