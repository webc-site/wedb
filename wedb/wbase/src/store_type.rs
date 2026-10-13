//! 存储面分类枚举（对标 libs/server/Cluster/StoreType.cs:StoreType）
//!
//! 跨 crate 基础协议与事务分类公用枚举：
//! - wresp 协议命令目录分类使用
//! - wtxn 事务存储面与锁计划使用
//! - wnode / wedb 存储与集群面统一收口
//!
//! 自研依据: StoreType 枚举（Main/Object 双存储形态，对标 C# StoreType）

use core::str::FromStr;

use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum::{AsRefStr, Display, EnumString, IntoStaticStr};

/// libs/server/AOF/AofHeader.cs:AofShardedLogTransactionHeader.ReplayTaskAccessVectorBytes
///
/// 协调操作（事务 / 存储过程）的重放任务位图字节数：每物理子日志最多
/// 256 个回放任务，故位图定宽 32 字节。
pub const REPLAY_TASK_ACCESS_VECTOR_BYTES: usize = 32;

/// 存储类型枚举（对标 libs/server/Cluster/StoreType.cs:byte）
#[derive(
  Debug,
  Clone,
  Copy,
  PartialEq,
  Eq,
  Hash,
  Default,
  Display,
  EnumString,
  AsRefStr,
  IntoStaticStr,
  TryFromPrimitive,
  IntoPrimitive,
)]
#[strum(ascii_case_insensitive)]
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
  #[inline]
  pub fn from_member_name(name: &str) -> Option<Self> {
    Self::from_str(name).ok()
  }

  /// 从原生数值解析（对标 libs/server/Cluster/StoreType.cs:byte）
  #[inline]
  pub fn from_u8(val: u8) -> Option<Self> {
    Self::try_from(val).ok()
  }

  /// 对应原生数值
  #[inline]
  pub const fn as_u8(self) -> u8 {
    self as u8
  }

  /// 对应成员名称字面量
  #[inline]
  pub fn as_str(self) -> &'static str {
    self.into()
  }
}
