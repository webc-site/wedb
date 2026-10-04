//! 节点与服务通用类型定义（对标 libs/server/API/GarnetStatus.cs 等）
//!
//! `StoreType` 统一由 [`wbase::store_type`] 提供（对标
//! libs/server/Cluster/StoreType.cs），`RespCommand` 由 `wresp` 提供，
//! `GarnetObjectType` 由 `wval` 提供，均一处定义、直接引用，不经本模块转导。

/// 存储 API 调用返回状态（libs/server/API/GarnetStatus.cs:GarnetStatus）
///
/// 无 `Moved` 变体：集群 MOVED 重定向由 RESP 层字节直写，不经本枚举构造。
#[derive(Debug, Copy, PartialEq, Eq, Clone)]
#[repr(u8)]
pub enum GarnetStatus {
  /// 成功
  Ok = 0,
  /// 未找到
  NotFound = 1,
  /// 对持有错误类型值的键执行操作（判别值沿用上游 GarnetStatus.cs 编号）
  WrongType = 3,
}
