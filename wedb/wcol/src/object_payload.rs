//! 集合对象二进制载荷编解码辅助（AOF 回放与会话共用）
//!
//! 成员级过期队列单源在 [`crate::types::expiration_queue`]（Hash/SortedSet 共用）。
//! 对象信封记录挂 `KeyTag::ObjectEnvelope` 物理键（带外类型通道，对标 C#
//! LogRecord.DataHeader.ValueIsObject 位），值为 `[1B GarnetObjectType 标签]
//! [4B count LE][bitcode 载荷]`——内层标签区分子类型，4B 小端整数直读计数，
//! 用户字符串值内容任意、互不干扰。
//!
//! 内层标签是单一 u8 线域：标准段（[`GarnetObjectType`]）与扩展段
//! （`wval::CustomObjectType`，自 `wval::CUSTOM_OBJECT_TYPE_BASE` 起）在同字节
//! 上连续分配，对标 C# `(GarnetObjectType)(CustomObjectTypeMinId + id)` 的单
//! 枚举域（libs/server/Custom/CustomCommandManager.cs:406）。故本模块只保留
//! 一对收 u8 线标签的编解码口（`obj_encode_custom` / `obj_decode_custom`），
//! 标准段另给 `GarnetObjectType` 类型化薄口转发——不再为扩展段立第二个 typed
//! 入口，同一线域上的同义双口即重复机制。标签的类型安全在其产出与消费侧由
//! `wval::CustomObjectType` 枚举保证（会话 parse→exec 全程持枚举，仅跨本模块
//! 边界收窄为 u8）。

use wval::GarnetObjectType;

use super::{
  hash::hash_object::HashObject, list::list_object::ListObject, set::set_object::SetObject,
  zset::sorted_set_object::SortedSetObject,
};

/// 对象存载荷通用行为契约（对标 Garnet IGarnetObject 与 ObjectStoreRMW 抽象）
///
/// 在 garnet 中的相对路径:libs/server/Objects/Types/IGarnetObject.cs:IGarnetObject
/// 在 garnet 中的相对路径:libs/server/Objects/Types/GarnetObjectSerializer.cs:DeserializeInternal
pub trait GarnetObjectPayload: Sized + Default {
  /// 权威对象类型标识（对标 GarnetObjectType）
  const OBJECT_TAG: GarnetObjectType;

  /// 从对象信封载荷切片反序列化对象（单格式：载荷恒带 `[4B count LE]` 头）
  ///
  /// 畸形/损坏载荷显式失败返回 `None`，严禁静默回退空对象（对标 C#
  /// DeserializeInternal 抛异常 fail-fast，杜绝把损坏折成空对象后回写/删键销毁数据）
  ///
  /// 在 garnet 中的相对路径:libs/server/Objects/Types/GarnetObjectSerializer.cs:DeserializeInternal
  fn from_blob(raw: &[u8]) -> Option<Self>;

  /// 序列化对象为信封载荷字节向量
  ///
  /// 在 garnet 中的相对路径:libs/server/Objects/Types/GarnetObjectSerializer.cs:Serialize
  fn to_blob(&self) -> Vec<u8>;

  /// 检查集合对象是否为空
  fn is_empty(&self) -> bool;
}

/// 从对象信封载荷（`[4B count LE][bitcode 载荷]`）中直读元素数量（O(1) 零反序列化）
#[inline]
pub fn count_of_blob(payload: &[u8]) -> Option<usize> {
  payload
    .first_chunk::<4>()
    .map(|&b| u32::from_le_bytes(b) as usize)
}

/// 将对象值信封直接编码写入调用方缓冲区（减少堆分配）
#[inline]
pub fn obj_encode_into(tag: GarnetObjectType, payload: &[u8], buf: &mut Vec<u8>) {
  obj_encode_custom_into(tag as u8, payload, buf);
}

/// 自定义对象值信封直接编码写入调用方缓冲区
#[inline]
pub fn obj_encode_custom_into(tag: u8, payload: &[u8], buf: &mut Vec<u8>) {
  buf.reserve(payload.len() + 1);
  buf.push(tag);
  buf.extend_from_slice(payload);
}

/// 自定义对象值信封编码（支持任意 u8 扩展标签）
#[inline]
pub fn obj_encode_custom(tag: u8, payload: &[u8]) -> Vec<u8> {
  let mut out = Vec::with_capacity(payload.len() + 1);
  obj_encode_custom_into(tag, payload, &mut out);
  out
}

/// 对象值信封解码：校验类型标签后返回载荷切片（信封记录挂 KeyTag::ObjectEnvelope 物理键）
#[inline]
pub fn obj_decode(raw: &[u8], want: GarnetObjectType) -> Option<&[u8]> {
  obj_decode_custom(raw, want as u8)
}

/// 自定义对象值信封解码：校验任意 u8 类型标签后返回载荷切片
#[inline]
pub fn obj_decode_custom(raw: &[u8], want: u8) -> Option<&[u8]> {
  raw
    .split_first()
    .filter(|(t, _)| **t == want)
    .map(|(_, p)| p)
}

/// 对象信封堆内存估算：GarnetObjectType 标准段（信封 raw = `[1B 类型标签][4B count LE][bitcode 载荷]`）
///
/// MEMORY USAGE 统计内层对象用：反序列化后取各对象 heap_memory_size
/// 增量记账（对标 C# ReadMethods 的 `ValueObject.HeapMemorySize` 对象堆估算段，
/// 记账单点为 [`crate::types::GarnetObjectBase::heap_memory_size`] 契约与各对象字段）。
/// 只覆盖标准段，非标准标签（含 wval::CUSTOM_OBJECT_TYPE_BASE 起的自定义扩展
/// 对象、保留段与畸形信封）回 `None`，由持有扩展 crate 编译期接线的 server 层
/// 分派——对标 C# 的 `IHeapObject.HeapMemorySize` 多态读：估算由对象自身记账
/// （libs/server/Objects 各对象、modules 各对象），消费点在 libs/server 读路径，
/// Garnet.server 工程不引用 modules，本 crate 也不该有扩展 crate 可见性
pub fn object_heap_estimate(raw: &[u8]) -> Option<i64> {
  let (&tag, payload) = raw.split_first()?;
  // C# 侧 MEMORY USAGE 持有已反序列化的 IGarnetObject 直接取 HeapMemorySize；
  // Rust 信封存存储，经各对象 from_blob 统一剥离 4B 计数头后提取堆估算（from_u8 单点分派）
  match GarnetObjectType::from_u8(tag) {
    // 载荷畸形（估算不可得）与未知标签同口径回 None，由消费方按扩展/失败处理
    Some(GarnetObjectType::SortedSet) => {
      SortedSetObject::from_blob(payload).map(|o| o.heap_memory_size)
    }
    Some(GarnetObjectType::List) => ListObject::from_blob(payload).map(|o| o.heap_memory_size),
    Some(GarnetObjectType::Hash) => HashObject::from_blob(payload).map(|o| o.heap_memory_size),
    Some(GarnetObjectType::Set) => SetObject::from_blob(payload).map(|o| o.heap_memory_size),
    _ => None,
  }
}

/// 对象键装载/读写结果状态四态（跨 storage 与 resp 全链路统一收敛）
///
/// 对标 C# 全链单枚举 GarnetStatus（OK/NOTFOUND/WRONGTYPE），结合 Rust 异步引擎必要之磁盘候选降级（Degrade）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjLoad<T> {
  /// 磁盘候选：命令须降级异步重放（未写任何输出）
  Degrade,
  /// 键存在但信封类型不符（WRONGTYPE）
  WrongType,
  /// 键缺失（未找到/无候选/放弃写入）
  Missing,
  /// 命中/执行完成并产出载荷或结果
  Present(T),
}

impl<T> ObjLoad<T> {
  /// 结果提取（非 Present 分支返回 `fallback`）
  #[inline]
  pub fn unwrap_or(self, fallback: T) -> T {
    match self {
      Self::Present(v) => v,
      _ => fallback,
    }
  }

  /// 映射内部载荷
  #[inline]
  pub fn map<U>(self, f: impl FnOnce(T) -> U) -> ObjLoad<U> {
    match self {
      Self::Present(v) => ObjLoad::Present(f(v)),
      Self::Degrade => ObjLoad::Degrade,
      Self::WrongType => ObjLoad::WrongType,
      Self::Missing => ObjLoad::Missing,
    }
  }

  /// 转换为 Option
  #[inline]
  pub fn ok(self) -> Option<T> {
    match self {
      Self::Present(v) => Some(v),
      _ => None,
    }
  }

  /// 借用转换
  #[inline]
  pub fn as_ref(&self) -> ObjLoad<&T> {
    match self {
      Self::Present(v) => ObjLoad::Present(v),
      Self::Degrade => ObjLoad::Degrade,
      Self::WrongType => ObjLoad::WrongType,
      Self::Missing => ObjLoad::Missing,
    }
  }

  /// 可变借用转换
  #[inline]
  pub fn as_mut(&mut self) -> ObjLoad<&mut T> {
    match self {
      Self::Present(v) => ObjLoad::Present(v),
      Self::Degrade => ObjLoad::Degrade,
      Self::WrongType => ObjLoad::WrongType,
      Self::Missing => ObjLoad::Missing,
    }
  }

  /// 提取内部值或默认值
  #[inline]
  pub fn unwrap_or_default(self) -> T
  where
    T: Default,
  {
    self.unwrap_or_else(T::default)
  }

  /// 延迟求值提取内部值
  #[inline]
  pub fn unwrap_or_else(self, f: impl FnOnce() -> T) -> T {
    match self {
      Self::Present(v) => v,
      _ => f(),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_count_of_blob() {
    assert_eq!(count_of_blob(&[]), None);
    assert_eq!(count_of_blob(&[1, 2, 3]), None);
    let mut buf = vec![42, 0, 0, 0];
    buf.extend_from_slice(b"payload");
    assert_eq!(count_of_blob(&buf), Some(42));

    let mut buf2 = vec![0, 1, 0, 0];
    buf2.extend_from_slice(b"payload");
    assert_eq!(count_of_blob(&buf2), Some(256));
  }

  #[test]
  fn test_obj_encode_into() {
    let mut buf = Vec::new();
    obj_encode_into(GarnetObjectType::Hash, b"test_data", &mut buf);
    assert_eq!(buf[0], GarnetObjectType::Hash as u8);
    assert_eq!(&buf[1..], b"test_data");

    let mut custom_buf = Vec::new();
    obj_encode_custom_into(0xFE, b"custom_data", &mut custom_buf);
    assert_eq!(custom_buf[0], 0xFE);
    assert_eq!(&custom_buf[1..], b"custom_data");
  }
}
