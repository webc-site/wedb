//! 对象存函数私有辅助（对标 libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs）

/// 获取自定义对象命令处理函数
///
/// 缺口说明：依赖 custom 域（libs/server/Custom/CustomObjectFactory.cs）
/// 转写完成后的句柄接线；未挂载时返回 None。
///
/// libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs:GetCustomObjectCommand
pub fn get_custom_object_command() -> Option<()> {
  None
}

/// 对象类型不匹配错误构造（WRONGTYPE 语义的纯判定）
///
/// `actual_tag` 为信封实际类型标签，`expected_tag` 为期望标签。
///
/// libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs:IncorrectObjectType
pub fn incorrect_object_type(actual_tag: u8, expected_tag: u8) -> bool {
  actual_tag != expected_tag
}
