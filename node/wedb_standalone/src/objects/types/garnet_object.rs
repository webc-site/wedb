//! Garnet 对象扩展（对标 libs/server/Objects/Types/GarnetObject.cs）
//!
//! NeedToCreate 判定：只读/删除类操作在键缺失时无需创建新对象
//! （C# 经 Tsavorite RMW 的 InitialValue 入口避免空对象落库）。

use crate::{
  input_header::RespInputHeader, objects::sortedset::sorted_set_object::SortedSetOperation,
  types::GarnetObjectType,
};

/// 有序集合操作码：直接取 SortedSetOperation 判别式（一处定义，数值与 C# 一致）
const Z_REM: u8 = SortedSetOperation::Zrem as u8;
const Z_POPMAX: u8 = SortedSetOperation::Zpopmax as u8;
const Z_POPMIN: u8 = SortedSetOperation::Zpopmin as u8;
const Z_REMRANGEBYLEX: u8 = SortedSetOperation::Zremrangebylex as u8;
const Z_REMRANGEBYSCORE: u8 = SortedSetOperation::Zremrangebyscore as u8;
const Z_REMRANGEBYRANK: u8 = SortedSetOperation::Zremrangebyrank as u8;
const Z_EXPIRE: u8 = SortedSetOperation::Zexpire as u8;
const Z_COLLECT: u8 = SortedSetOperation::Zcollect as u8;

/// 列表操作码（C# ListOperation）
const L_POP: u8 = 0;
const L_PUSHX: u8 = 2;
const R_POP: u8 = 3;
const R_PUSHX: u8 = 5;
const L_RANGE: u8 = 8;
const L_INDEX: u8 = 9;
const L_TRIM: u8 = 7;
const L_REM: u8 = 11;
const L_INSERT: u8 = 10;

/// 集合操作码（C# SetOperation）
const S_CARD: u8 = 13;
const S_MEMBERS: u8 = 5;
const S_REM: u8 = 1;
const S_POP: u8 = 2;

/// 哈希操作码（C# HashOperation：HCOLLECT=0、HEXPIRE=1）
const H_EXPIRE: u8 = 1;
const H_COLLECT: u8 = 0;

pub struct GarnetObject;

impl GarnetObject {
  /// 判断是否需要创建对象初值
  ///
  /// libs/server/Objects/Types/GarnetObject.cs:NeedToCreate
  pub fn need_to_create(header: &RespInputHeader) -> bool {
    // 类型字节手工匹配（GarnetObjectType 无 TryFrom<u8> 派生，types.rs 不在本周期改动范围）
    match header.data[0] {
      x if x == GarnetObjectType::SortedSet as u8 => !matches!(
        header.data[1],
        Z_REM
          | Z_POPMAX
          | Z_POPMIN
          | Z_REMRANGEBYLEX
          | Z_REMRANGEBYSCORE
          | Z_REMRANGEBYRANK
          | Z_EXPIRE
          | Z_COLLECT
      ),
      x if x == GarnetObjectType::List as u8 => !matches!(
        header.data[1],
        L_POP | R_POP | L_RANGE | L_INDEX | L_TRIM | L_REM | L_INSERT | L_PUSHX | R_PUSHX
      ),
      x if x == GarnetObjectType::Set as u8 => {
        !matches!(header.data[1], S_CARD | S_MEMBERS | S_REM | S_POP)
      }
      x if x == GarnetObjectType::Hash as u8 => !matches!(header.data[1], H_EXPIRE | H_COLLECT),
      _ => true,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::types::RespInputFlags;

  fn header(obj_type: GarnetObjectType, op: u8) -> RespInputHeader {
    let mut h = RespInputHeader::new_with_type(obj_type, RespInputFlags::empty());
    h.set_sub_id(op);
    h
  }

  #[test]
  fn sorted_set_create_matrix() {
    // 读/删类：不创建
    for op in [
      Z_REM,
      Z_POPMAX,
      Z_POPMIN,
      Z_REMRANGEBYLEX,
      Z_REMRANGEBYSCORE,
      Z_REMRANGEBYRANK,
      Z_EXPIRE,
      Z_COLLECT,
    ] {
      assert!(
        !GarnetObject::need_to_create(&header(GarnetObjectType::SortedSet, op)),
        "op = {op}"
      );
    }
    // 写类：创建（ZADD = 0）
    assert!(GarnetObject::need_to_create(&header(
      GarnetObjectType::SortedSet,
      0
    )));
  }

  #[test]
  fn list_set_hash_create_matrix() {
    // List 读类不创建（LPOP/LRANGE/LINDEX/LTRIM/LREM/LINSERT/LPUSHX/RPUSHX/RPOP）
    for op in [
      L_POP, R_POP, L_RANGE, L_INDEX, L_TRIM, L_REM, L_INSERT, L_PUSHX, R_PUSHX,
    ] {
      assert!(
        !GarnetObject::need_to_create(&header(GarnetObjectType::List, op)),
        "op = {op}"
      );
    }
    // List 写类创建（LPUSH = 1）
    assert!(GarnetObject::need_to_create(&header(
      GarnetObjectType::List,
      1
    )));

    // Set：SCARD/SMEMBERS/SREM/SPOP 不创建
    for op in [S_CARD, S_MEMBERS, S_REM, S_POP] {
      assert!(
        !GarnetObject::need_to_create(&header(GarnetObjectType::Set, op)),
        "op = {op}"
      );
    }
    assert!(GarnetObject::need_to_create(&header(
      GarnetObjectType::Set,
      0
    )));

    // Hash：HEXPIRE/HCOLLECT 不创建，其余创建
    assert!(!GarnetObject::need_to_create(&header(
      GarnetObjectType::Hash,
      H_EXPIRE
    )));
    assert!(!GarnetObject::need_to_create(&header(
      GarnetObjectType::Hash,
      H_COLLECT
    )));
    // C# 编号下 0 为 HCOLLECT（不创建），4 为 HGET（创建）
    assert!(!GarnetObject::need_to_create(&header(
      GarnetObjectType::Hash,
      0
    )));
    assert!(GarnetObject::need_to_create(&header(
      GarnetObjectType::Hash,
      4
    )));

    // 未知类型一律创建
    assert!(GarnetObject::need_to_create(&header(
      GarnetObjectType::Null,
      0
    )));
  }
}
