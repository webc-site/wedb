//! 主存读取函数（对标 libs/server/Storage/Functions/MainStore/ReadMethods.cs）

/// 记录类型不匹配检查（信封首字节与期望标签比对）
///
/// `raw` 为完整信封载荷；`expected_tag` 取 objectstore::common 的 OBJ_TAG_*。
/// 空记录视为字符串语义（标签 0）恒匹配。
///
/// libs/server/Storage/Functions/MainStore/ReadMethods.cs:CheckRecordTypeMismatch
pub fn check_record_type_mismatch(raw: &[u8], expected_tag: u8) -> bool {
  raw.first().is_none_or(|&t| t != expected_tag)
}
