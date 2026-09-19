//! 扩展对象静态描述单点（信封标签 + TYPE 注册名 + 按名命令解析入口）
//!
//! 对标 C# libs/server/Custom/CustomCommandManager.cs 的集中分配面
//! （CustomObjectTypeMinId 标签分配 + :406 `(GarnetObjectType)(CustomObjectTypeMinId + id)`
//! + CustomObjectFactory 的 Create/Deserialize 工厂集中持有类型与序列化能力）。
//!
//! 动态注册管理层按转写规范删除后，集中分配这件事本身由本模块以编译期
//! 静态描述承接——每个扩展 crate 以 const [`CustomObjectEntry`] 提供自身
//! 那一条，server 层（wnode）以 const 清单按特性组装，零运行时注册、
//! 零锁、零分配。
//!
//! 标签值统一取自 [`wval::CustomObjectType`]（wval 分配单点的投影），
//! 全仓严禁 `CUSTOM_OBJECT_TYPE_BASE + n` 裸偏移。

use wval::CustomObjectType;

use crate::custom_object_fns::{CommandType, CustomObjectFns};

/// 按名解析出的自定义命令元数据（执行面四元组）
///
/// 信封标签不随命令走：一个扩展对象类型一个标签，由清单项描述单点
/// 持有（[`CustomObjectEntry::tag`]），命令元数据只承载执行面。
pub struct CustomCommandMeta {
  /// 命令名（静态清单规范形，零分配）
  pub name: &'static str,
  /// 命令类型（Read / ReadModifyWrite）
  pub command_type: CommandType,
  /// arity（0 = 不校验；负值 = 至少 -arity-1 个参数）
  pub arity: i32,
  /// 静态执行体（编译期函数指针集）
  pub fns: CustomObjectFns,
}

/// 扩展对象静态描述清单项（一个扩展对象类型一条）
///
/// server 层的扩展对象分发面（TYPE 注册名、扩展命令解析落槽）统一以
/// 本描述清单为准，不再手写标签字面比对臂；堆内存估算不入描述——
/// C# 侧仅部分扩展对象维护 `IHeapObject.HeapMemorySize`
/// （RoaringBitmapObject.cs:33 维护，GarnetJsonObject.cs:350 备注明确
/// 未更新），各扩展 crate 的对象级记账入口自持，server 消费点按现状
/// 直呼，不另立第二口径。
#[derive(Clone, Copy)]
pub struct CustomObjectEntry {
  /// 信封内层类型标签（[`wval::CustomObjectType`] 分配单点）
  pub tag: CustomObjectType,
  /// Redis TYPE 应答类型串（C# modules 注册名，如 "GarnetRoaringBitmap"）
  pub type_name: &'static str,
  /// 按名解析命令（大小写不敏感；None = 名称不在本类型命令清单）
  pub match_command: fn(&[u8]) -> Option<CustomCommandMeta>,
}
