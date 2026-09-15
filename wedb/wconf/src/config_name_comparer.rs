/// 配置参数名（原始字节）的 ASCII 大小写不敏感比较
/// （对标 libs/server/Config/ConfigNameComparer.cs:ConfigNameComparer）。
///
/// 无状态：比较为纯函数，C# 的 `Instance` 单例在此退化为关联函数。
/// C# 的 GetHashCode/ToUpperAscii 不转写：rust 配置名匹配走 equals 线性
/// 比较，无哈希表消费面（见 js/check/ignore/server.yml 登记）。
pub struct ConfigNameComparer;

impl ConfigNameComparer {
  /// libs/server/Config/ConfigNameComparer.cs:Equals
  ///
  /// 按字节逐位比较两个参数名，忽略 ASCII 大小写。
  #[inline]
  pub fn equals(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
  }
}
