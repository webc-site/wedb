//! 向量存会话函数（对标 libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs）
//!
//! 缺口总述：C# 侧向量函数挂接 VectorManager 专用存储上下文；向量引擎域为
//! 并行转写域（见 mainstore/vector_store_ops 模块级缺口总述），wkv 无向量
//! 原语，本域函数以一致性缺省返回并注明缺口。

/// 读完成回调（向量上下文未接线，恒 false：无挂起可完成）
///
/// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:ReadCompletionCallback
pub fn read_completion_callback() -> bool {
  false
}

/// RMW 修改值长度（向量域缺口，返回 0）
///
/// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:GetRMWModifiedFieldInfo
pub fn get_rmw_modified_field_info() -> usize {
  0
}

/// RMW 初始值长度（向量域缺口，返回 0）
///
/// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:GetRMWInitialFieldInfo
pub fn get_rmw_initial_field_info() -> usize {
  0
}

/// Upsert 值长度（向量域缺口，返回 0）
///
/// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:GetUpsertFieldInfo
pub fn get_upsert_field_info() -> usize {
  0
}

/// RMW 完成回调（向量上下文未接线）
///
/// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:RMWCompletionCallback
pub fn rmw_completion_callback() -> bool {
  false
}

/// 对象操作不应出现（向量存只承载记录，遇对象操作返回 false 断言位）
///
/// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:ObjectOperationsNotExpected
pub fn object_operations_not_expected() -> bool {
  false
}

/// 日志记录操作不应出现（断言位）
///
/// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:LogRecordOperationsNotExpected
pub fn log_record_operations_not_expected() -> bool {
  false
}

/// 地址对齐或钉选（wkv 记录定界内部化，恒返回 true）
///
/// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:AlignOrPin
pub fn align_or_pin() -> bool {
  true
}

/// 对齐断言（wkv 记录对齐由引擎保证，恒通过）
///
/// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:AssertAlignment
pub fn assert_alignment() -> bool {
  true
}
