//! Garnet 对象序列化器（对标 libs/server/Objects/Types/GarnetObjectSerializer.cs）
//!
//! 线格式：1 字节类型标记 + 各对象自有载荷。有序集合/列表/集合/哈希
//! 均经原生 bitcode 载荷承载。自定义类型（CustomDeserialize）以
//! 自定义类型起点校验 + 原样载荷透传（C# 经 CustomCommandManager 工厂）。

use std::io::{self, Cursor, Read, Write};

use crate::{
  objects::{
    hash::hash_object::HashObject, list::list_object::ListObject, set::set_object::SetObject,
    sortedset::sorted_set_object::SortedSetObject,
  },
  types::GarnetObjectType,
};

/// 自定义对象类型起点（C# CustomCommandManager.CustomTypeIdStartOffset）
pub const CUSTOM_TYPE_ID_START: u8 = 0x10;

/// 可序列化的 Garnet 对象值
///
/// libs/server/Objects/Types/GarnetObjectSerializer.cs 承载的 IGarnetObject 判别联合
pub enum GarnetObjectValue {
  Null,
  SortedSet(SortedSetObject),
  List(ListObject),
  Hash(HashObject),
  Set(SetObject),
  /// 自定义对象：类型 id + 原样载荷（工厂反序列化由宿主注册表承担）
  Custom {
    type_id: u8,
    payload: Vec<u8>,
  },
}

pub struct GarnetObjectSerializer;

impl GarnetObjectSerializer {
  /// 线格式序列化（Null → 单类型字节；其余 → 类型字节 + 载荷）
  ///
  /// libs/server/Objects/Types/GarnetObjectSerializer.cs:SerializeInternal
  pub fn serialize_internal(writer: &mut impl Write, obj: &GarnetObjectValue) -> io::Result<()> {
    match obj {
      GarnetObjectValue::Null => writer.write_all(&[GarnetObjectType::Null as u8]),
      GarnetObjectValue::SortedSet(obj) => {
        writer.write_all(&(GarnetObjectType::SortedSet as u8).to_le_bytes())?;
        obj.serialize(writer)
      }
      GarnetObjectValue::List(obj) => {
        writer.write_all(&(GarnetObjectType::List as u8).to_le_bytes())?;
        obj.serialize(writer)
      }
      GarnetObjectValue::Hash(obj) => {
        writer.write_all(&(GarnetObjectType::Hash as u8).to_le_bytes())?;
        obj.serialize(writer)
      }

      GarnetObjectValue::Set(obj) => {
        writer.write_all(&(GarnetObjectType::Set as u8).to_le_bytes())?;
        obj.serialize(writer)
      }
      GarnetObjectValue::Custom { type_id, payload } => {
        writer.write_all(&[*type_id])?;
        writer.write_all(&(payload.len() as u32).to_le_bytes())?;
        writer.write_all(payload)
      }
    }
  }

  /// 线格式反序列化（首字节为类型标记；保留段 0xFB..0xFF 视为不支持的格式版本）
  ///
  /// libs/server/Objects/Types/GarnetObjectSerializer.cs:DeserializeInternal
  pub fn deserialize_internal(reader: &mut impl Read) -> io::Result<Option<GarnetObjectValue>> {
    let mut first = [0_u8; 1];
    reader.read_exact(&mut first)?;
    let first = first[0];

    // 0xFC..0xFF 为保留格式版本/转义字节（GarnetObjectType::All = 0xFB 之后）
    if first >= GarnetObjectType::All as u8 {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "unsupported object serialization format marker",
      ));
    }

    let mut payload = Vec::new();
    reader.read_to_end(&mut payload)?;

    // 类型字节手工匹配（GarnetObjectType 无 TryFrom<u8> 派生）
    match first {
      x if x == GarnetObjectType::Null as u8 => Ok(None),
      x if x == GarnetObjectType::SortedSet as u8 => Ok(Some(GarnetObjectValue::SortedSet(
        SortedSetObject::deserialize(&mut Cursor::new(&payload))?,
      ))),
      x if x == GarnetObjectType::List as u8 => Ok(Some(GarnetObjectValue::List(
        ListObject::deserialize(&mut Cursor::new(&payload))?,
      ))),
      x if x == GarnetObjectType::Hash as u8 => Ok(Some(GarnetObjectValue::Hash(
        HashObject::deserialize(&mut Cursor::new(&payload))?,
      ))),
      x if x == GarnetObjectType::Set as u8 => Ok(Some(GarnetObjectValue::Set(
        SetObject::deserialize(&mut Cursor::new(&payload))?,
      ))),
      _ => Self::custom_deserialize(first, &payload).map(Some),
    }
  }

  /// 自定义对象反序列化：内建保留段快速失败，其余原样载荷承载
  ///
  /// libs/server/Objects/Types/GarnetObjectSerializer.cs:CustomDeserialize
  pub fn custom_deserialize(type_id: u8, buf: &[u8]) -> io::Result<GarnetObjectValue> {
    if type_id < CUSTOM_TYPE_ID_START {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "unsupported object type id (legacy built-in band)",
      ));
    }
    // 载荷带 u32 长度前缀（serialize_internal 的对称格式）
    if buf.len() < 4 {
      return Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "custom payload length",
      ));
    }
    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    if buf.len() < 4 + len {
      return Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "custom payload",
      ));
    }
    Ok(GarnetObjectValue::Custom {
      type_id,
      payload: buf[4..4 + len].to_vec(),
    })
  }

  /// 全量字节 → 对象（C# Deserialize(byte[])）
  pub fn deserialize_bytes(data: &[u8]) -> io::Result<Option<GarnetObjectValue>> {
    Self::deserialize_internal(&mut Cursor::new(data))
  }

  /// 对象 → 全量字节（C# Serialize(IGarnetObject, out byte[])）
  pub fn serialize_bytes(obj: &GarnetObjectValue) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    Self::serialize_internal(&mut out, obj)?;
    Ok(out)
  }
}
