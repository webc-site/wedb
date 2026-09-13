//! 存储面分类枚举（对标 libs/server/Storage/Common/StoreType.cs）

/// 存储类型枚举（Main / Object / None / All）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum StoreType {
  /// 主键值存储（String / Hash / List / Set / ZSet / Stream 等单键或内联对象）
  #[default]
  Main,
  /// 外部大对象 / 专用对象存储
  Object,
  /// 未指定或无作用存储
  None,
  /// 全部存储
  All,
}

impl StoreType {
  /// 从成员名解析（零堆分配，大小写不敏感匹配）
  pub fn from_member_name(name: &str) -> Option<Self> {
    if name.eq_ignore_ascii_case("MAIN") {
      Some(Self::Main)
    } else if name.eq_ignore_ascii_case("OBJECT") {
      Some(Self::Object)
    } else if name.eq_ignore_ascii_case("ALL") {
      Some(Self::All)
    } else {
      None
    }
  }

  /// 对应成员名称字面量
  #[inline]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Main => "Main",
      Self::Object => "Object",
      Self::None => "None",
      Self::All => "All",
    }
  }
}
