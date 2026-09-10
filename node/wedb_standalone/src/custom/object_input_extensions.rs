//! 对象输入扩展（对标 libs/server/Custom/ObjectInputExtensions.cs）
//!
//! C# 扩展方法解析 ObjectInput 的 EXIST 选项（装箱参数管道）；rust 输入
//! 参数面统一为切片参数（parse_state），custom 域并行推进面。

/// 对象输入扩展
pub struct ObjectInputExtensions;

impl ObjectInputExtensions {
  /// libs/server/Custom/ObjectInputExtensions.cs:TryGetExistOption
  ///
  /// 缺口说明：EXIST 选项解析依赖 ObjectInput 装箱输入管道；rust 参数面
  /// 为切片形态（豁免登记见 js/check/ignore/libs/server/Custom/
  /// ObjectInputExtensions.yml）
  pub fn try_get_exist_option() {}
}
