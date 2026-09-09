//! 对象存变长输入信息（对标 libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs）

/// RMW 初始对象物理长度估计（信封：1 字节标签 + bitcode 载荷）
///
/// libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs:GetRMWInitialFieldInfo
pub fn get_rmw_initial_field_info(payload_len: usize) -> usize {
  payload_len + 1
}

/// RMW 修改对象物理长度估计
///
/// libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs:GetRMWModifiedFieldInfo
pub fn get_rmw_modified_field_info(payload_len: usize) -> usize {
  payload_len + 1
}

/// Upsert 对象物理长度估计
///
/// libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs:GetUpsertFieldInfo
pub fn get_upsert_field_info(payload_len: usize) -> usize {
  payload_len + 1
}
