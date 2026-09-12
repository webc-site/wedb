//! 集合对象二进制载荷编解码与输入构造辅助（AOF 回放与会话共用）

use std::io::Cursor;

use wresp::ArgSlice;
use wval::GarnetObjectType;

use super::{
  hash::hash_object::HashObject, list::list_object::ListObject, set::set_object::SetObject,
  sortedset::sorted_set_object::SortedSetObject,
};
use crate::{
  input_header::RespInputHeader, inputs::ObjectInput, session_parse_state::SessionParseState,
  types::RespInputFlags,
};

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

/// 获取对象信封对应的类型名（"hash"/"list"/"set"/"zset"）；若非对象信封返回 None
pub fn object_type_name(val: &[u8]) -> Option<&'static str> {
  let &tag_byte = val.first()?;
  match tag_byte {
    1 => Some("zset"),
    2 => Some("list"),
    3 => Some("hash"),
    4 => Some("set"),
    _ => None,
  }
}

/// 判定载荷是否为集合对象信封格式
#[inline]
pub fn is_object_envelope(val: &[u8]) -> bool {
  val.first().is_some_and(|&tag| (1..=4).contains(&tag))
}

/// 从 wkv 信封载荷装载哈希对象
#[inline]
pub fn hash_from_blob(raw: &[u8]) -> HashObject {
  HashObject::deserialize(&mut Cursor::new(raw)).unwrap_or_default()
}

/// 序列化哈希对象为 wkv 信封载荷
#[inline]
pub fn hash_to_blob(obj: &HashObject) -> Vec<u8> {
  let mut out = Vec::new();
  if let Err(e) = obj.serialize(&mut out) {
    log::error!("哈希对象序列化失败: {e}");
  }
  out
}

/// 从 wkv 信封载荷装载集合对象
#[inline]
pub fn set_from_blob(raw: &[u8]) -> SetObject {
  SetObject::deserialize(&mut Cursor::new(raw)).unwrap_or_default()
}

/// 序列化集合对象为 wkv 信封载荷
#[inline]
pub fn set_to_blob(obj: &SetObject) -> Vec<u8> {
  let mut out = Vec::new();
  if let Err(e) = obj.serialize(&mut out) {
    log::error!("集合对象序列化失败: {e}");
  }
  out
}

/// 从 wkv 信封载荷装载列表对象
#[inline]
pub fn list_from_blob(raw: &[u8]) -> ListObject {
  ListObject::deserialize(&mut Cursor::new(raw)).unwrap_or_default()
}

/// 序列化列表对象为 wkv 信封载荷
#[inline]
pub fn list_to_blob(obj: &ListObject) -> Vec<u8> {
  let mut out = Vec::new();
  if let Err(e) = obj.serialize(&mut out) {
    log::error!("列表对象序列化失败: {e}");
  }
  out
}

/// 从 wkv 信封载荷装载有序集合对象
#[inline]
pub fn zset_from_blob(raw: &[u8]) -> SortedSetObject {
  SortedSetObject::deserialize(&mut Cursor::new(raw)).unwrap_or_default()
}

/// 序列化有序集合对象为 wkv 信封载荷
#[inline]
pub fn zset_to_blob(obj: &SortedSetObject) -> Vec<u8> {
  let mut out = Vec::new();
  if let Err(e) = obj.serialize(&mut out) {
    log::error!("有序集合对象序列化失败: {e}");
  }
  out
}
