//! 节点与服务通用类型定义（对标 libs/server/Storage/Common/StoreType.cs）

use strum::{Display, EnumString};
pub use wresp::RespCommand;
pub use wval::GarnetObjectType;

/// 存储类型枚举（Main / Object / None / All）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Display, EnumString)]
#[strum(ascii_case_insensitive)]
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
  /// 从成员名解析
  pub fn from_member_name(name: &str) -> Option<Self> {
    match name.to_ascii_uppercase().as_str() {
      "MAIN" => Some(Self::Main),
      "OBJECT" => Some(Self::Object),
      "ALL" => Some(Self::All),
      _ => None,
    }
  }
}

/// 存储 API 调用返回状态（libs/server/API/GarnetStatus.cs:GarnetStatus）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum GarnetStatus {
  /// 成功
  Ok = 0,
  /// 未找到
  NotFound = 1,
  /// 已迁移（集群 MOVED 重定向）
  Moved = 2,
  /// 对持有错误类型值的键执行操作
  WrongType = 3,
}

impl GarnetStatus {
  /// 是否成功
  #[inline]
  pub const fn is_ok(self) -> bool {
    matches!(self, Self::Ok)
  }
}

bitflags::bitflags! {
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
  pub struct RespInputFlags: u8 {
    const SET_GET = 32;
    const DETERMINISTIC = 64;
    const EXPIRED = 128;
  }
}
