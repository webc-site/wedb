//! 集合对象二进制载荷编解码与输入构造辅助（AOF 回放与会话共用）
//!
//! 成员级过期队列单源在 [`crate::types::expiration_queue`]（Hash/SortedSet 共用）。
//! 对象信封记录挂 `KeyTag::ObjectEnvelope` 物理键（带外类型通道，对标 C#
//! LogRecord.DataHeader.ValueIsObject 位），值为 `[1B GarnetObjectType 标签]
//! [bitcode 载荷]`——内层标签区分子类型，用户字符串值内容任意、互不干扰。

use wresp::{ArgSlice, SessionParseState};
use wval::GarnetObjectType;

use super::{
  hash::hash_object::HashObject, list::list_object::ListObject, set::set_object::SetObject,
  zset::sorted_set_object::SortedSetObject,
};
use crate::types::{ObjectInput, RespInputFlags, RespInputHeader};

/// 构造零拷贝 ObjectInput（借用 args 的字节指针，无需克隆任何参数与解析状态）
pub fn make_object_input<T: AsRef<[u8]>>(
  obj_type: GarnetObjectType,
  sub_id: u8,
  args: &[T],
  arg1: i32,
  arg2: i32,
) -> ObjectInput {
  let slices: Vec<ArgSlice> = args
    .iter()
    .map(|a| {
      let b = a.as_ref();
      ArgSlice::new(b.as_ptr(), b.len())
    })
    .collect();
  let mut parse_state = SessionParseState::new();
  parse_state.initialize_with_args(&slices);

  let mut header = RespInputHeader::new_with_type(obj_type, RespInputFlags::empty());
  header.set_sub_id(sub_id);
  ObjectInput {
    header,
    arg1,
    arg2,
    parse_state,
  }
}

/// 从对象信封载荷（KeyTag::ObjectEnvelope 记录值，剥壳后）装载哈希对象
#[inline]
pub fn hash_from_blob(raw: &[u8]) -> HashObject {
  HashObject::deserialize_from_slice(raw).unwrap_or_default()
}

/// 序列化哈希对象为信封载荷（挂 KeyTag::ObjectEnvelope 记录）
#[inline]
pub fn hash_to_blob(obj: &HashObject) -> Vec<u8> {
  obj.serialize_to_vec()
}

/// 从对象信封载荷（KeyTag::ObjectEnvelope 记录值，剥壳后）装载集合对象
#[inline]
pub fn set_from_blob(raw: &[u8]) -> SetObject {
  SetObject::deserialize_from_slice(raw).unwrap_or_default()
}

/// 序列化集合对象为信封载荷（挂 KeyTag::ObjectEnvelope 记录）
#[inline]
pub fn set_to_blob(obj: &SetObject) -> Vec<u8> {
  obj.serialize_to_vec()
}

/// 从对象信封载荷（KeyTag::ObjectEnvelope 记录值，剥壳后）装载列表对象
#[inline]
pub fn list_from_blob(raw: &[u8]) -> ListObject {
  ListObject::deserialize_from_slice(raw).unwrap_or_default()
}

/// 序列化列表对象为信封载荷（挂 KeyTag::ObjectEnvelope 记录）
#[inline]
pub fn list_to_blob(obj: &ListObject) -> Vec<u8> {
  obj.serialize_to_vec()
}

/// 从对象信封载荷（KeyTag::ObjectEnvelope 记录值，剥壳后）装载有序集合对象
#[inline]
pub fn zset_from_blob(raw: &[u8]) -> SortedSetObject {
  SortedSetObject::deserialize_from_slice(raw).unwrap_or_default()
}

/// 序列化有序集合对象为信封载荷（挂 KeyTag::ObjectEnvelope 记录）
#[inline]
pub fn zset_to_blob(obj: &SortedSetObject) -> Vec<u8> {
  obj.serialize_to_vec()
}

/// 对象值信封编码：[类型标签][bitcode 载荷]（信封记录挂 KeyTag::ObjectEnvelope 物理键）
///
/// C# 侧对象存值由 Tsavorite 经对象序列化器落盘（GarnetObjectSerializer.cs）；
/// Rust 单库 wkv 模型下以值内信封承载，此处为信封编解码单点
#[inline]
pub fn obj_encode(tag: u8, payload: &[u8]) -> Vec<u8> {
  let mut out = Vec::with_capacity(payload.len() + 1);
  out.push(tag);
  out.extend_from_slice(payload);
  out
}

/// 对象值信封解码：校验类型标签后返回载荷切片（信封记录挂 KeyTag::ObjectEnvelope 物理键）
#[inline]
pub fn obj_decode(raw: &[u8], want: u8) -> Option<&[u8]> {
  raw
    .split_first()
    .filter(|(t, _)| **t == want)
    .map(|(_, p)| p)
}

/// 对象信封堆内存估算（信封 raw = `[1B 类型标签][bitcode 载荷]`）
///
/// MEMORY USAGE 统计内层对象用：反序列化后取各对象 heap_memory_size
/// 增量记账（对标 C# ReadMethods 的 `ValueObject.HeapMemorySize` 对象堆估算段，
/// 记账单点为 [`crate::types::GarnetObjectBase::heap_memory_size`] 契约与各对象字段）。
/// 格式损坏/未知标签（含自定义类型段与保留段）按 0 计——信封记录物理尺寸
/// 已含序列化载荷本体，不重复计
pub fn object_heap_estimate(raw: &[u8]) -> i64 {
  let Some((&tag, payload)) = raw.split_first() else {
    return 0;
  };
  // C# 侧 MEMORY USAGE 持有已反序列化的 IGarnetObject 直接取 HeapMemorySize；
  // Rust 信封存存储，须先按类型标签剥壳反序列化（from_u8 单点分派）
  match GarnetObjectType::from_u8(tag) {
    Some(GarnetObjectType::SortedSet) => {
      SortedSetObject::deserialize_from_slice(payload).map_or(0, |o| o.heap_memory_size)
    }
    Some(GarnetObjectType::List) => {
      ListObject::deserialize_from_slice(payload).map_or(0, |o| o.heap_memory_size)
    }
    Some(GarnetObjectType::Hash) => {
      HashObject::deserialize_from_slice(payload).map_or(0, |o| o.heap_memory_size)
    }
    Some(GarnetObjectType::Set) => {
      SetObject::deserialize_from_slice(payload).map_or(0, |o| o.heap_memory_size)
    }
    _ => 0,
  }
}
