//! 存储面分类枚举（对标 libs/server/Cluster/StoreType.cs:StoreType）
//!
//! 跨 crate 基础协议与事务分类公用枚举：
//! - wresp 协议命令目录分类使用
//! - wtxn 事务存储面与锁计划使用
//! - wnode / wedb 存储与集群面统一收口

use core::{fmt, str::FromStr};

/// libs/server/AOF/AofHeader.cs:AofShardedLogTransactionHeader.ReplayTaskAccessVectorBytes
///
/// 协调操作（事务 / 存储过程）的重放任务位图字节数：每物理子日志最多
/// 256 个回放任务，故位图定宽 32 字节。
pub const REPLAY_TASK_ACCESS_VECTOR_BYTES: usize = 32;

/// 存储类型枚举（对标 libs/server/Cluster/StoreType.cs:byte）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum StoreType {
  /// 未指定或无作用存储
  None = 0,
  /// 主键值存储（String / Hash / List / Set / ZSet / Stream 等单键或内联对象）
  #[default]
  Main = 1,
  /// 外部大对象 / 专用对象存储
  Object = 2,
  /// 全部存储
  All = 3,
}

impl StoreType {
  /// 从成员名解析（零堆分配，大小写不敏感匹配）
  pub fn from_member_name(name: &str) -> Option<Self> {
    match name.len() {
      3 => {
        if name.eq_ignore_ascii_case("ALL") {
          Some(Self::All)
        } else {
          None
        }
      }
      4 => {
        if name.eq_ignore_ascii_case("MAIN") {
          Some(Self::Main)
        } else if name.eq_ignore_ascii_case("NONE") {
          Some(Self::None)
        } else {
          None
        }
      }
      6 => {
        if name.eq_ignore_ascii_case("OBJECT") {
          Some(Self::Object)
        } else {
          None
        }
      }
      _ => None,
    }
  }

  /// 从原生数值解析（对标 libs/server/Cluster/StoreType.cs:byte）
  #[inline]
  pub const fn from_u8(val: u8) -> Option<Self> {
    match val {
      0 => Some(Self::None),
      1 => Some(Self::Main),
      2 => Some(Self::Object),
      3 => Some(Self::All),
      _ => None,
    }
  }

  /// 对应原生数值
  #[inline]
  pub const fn as_u8(self) -> u8 {
    self as u8
  }

  /// 对应成员名称字面量
  #[inline]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::None => "None",
      Self::Main => "Main",
      Self::Object => "Object",
      Self::All => "All",
    }
  }
}

impl AsRef<str> for StoreType {
  #[inline]
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl fmt::Display for StoreType {
  #[inline]
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

impl FromStr for StoreType {
  type Err = ();

  #[inline]
  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Self::from_member_name(s).ok_or(())
  }
}

impl TryFrom<u8> for StoreType {
  type Error = ();

  #[inline]
  fn try_from(val: u8) -> Result<Self, Self::Error> {
    Self::from_u8(val).ok_or(())
  }
}

#[cfg(test)]
mod tests {
  use super::StoreType;

  #[test]
  fn test_store_type_roundtrip() {
    assert_eq!(StoreType::from_member_name("Main"), Some(StoreType::Main));
    assert_eq!(StoreType::from_member_name("main"), Some(StoreType::Main));
    assert_eq!(StoreType::from_member_name("MAIN"), Some(StoreType::Main));
    assert_eq!(
      StoreType::from_member_name("Object"),
      Some(StoreType::Object)
    );
    assert_eq!(
      StoreType::from_member_name("object"),
      Some(StoreType::Object)
    );
    assert_eq!(StoreType::from_member_name("All"), Some(StoreType::All));
    assert_eq!(StoreType::from_member_name("all"), Some(StoreType::All));
    assert_eq!(StoreType::from_member_name("None"), Some(StoreType::None));
    assert_eq!(StoreType::from_member_name("none"), Some(StoreType::None));
    assert_eq!(StoreType::from_member_name("unknown"), None);
    assert_eq!(StoreType::from_member_name(""), None);

    for s in [
      StoreType::None,
      StoreType::Main,
      StoreType::Object,
      StoreType::All,
    ] {
      assert_eq!(StoreType::from_member_name(s.as_str()), Some(s));
      assert_eq!(s.as_str(), s.to_string());
      assert_eq!(s.as_str(), s.as_ref());
      assert_eq!(s.as_str().parse::<StoreType>(), Ok(s));
      assert_eq!(StoreType::from_u8(s.as_u8()), Some(s));
      assert_eq!(StoreType::try_from(s.as_u8()), Ok(s));
    }

    assert_eq!(StoreType::Main.as_str(), "Main");
    assert_eq!(StoreType::Object.as_str(), "Object");
    assert_eq!(StoreType::None.as_str(), "None");
    assert_eq!(StoreType::All.as_str(), "All");
    assert_eq!(StoreType::default(), StoreType::Main);

    assert_eq!(StoreType::None.as_u8(), 0);
    assert_eq!(StoreType::Main.as_u8(), 1);
    assert_eq!(StoreType::Object.as_u8(), 2);
    assert_eq!(StoreType::All.as_u8(), 3);
    assert_eq!(StoreType::from_u8(99), None);
    assert_eq!(StoreType::try_from(99), Err(()));
  }
}
