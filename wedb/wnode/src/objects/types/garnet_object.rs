//! Garnet 对象扩展（对标 libs/server/Objects/Types/GarnetObject.cs）
//!
//! NeedToCreate 判定：只读/删除类操作在键缺失时无需创建新对象
//! （C# 经 Tsavorite RMW 的 InitialValue 入口避免空对象落库）。

use crate::{
  input_header::RespInputHeader,
  objects::{
    list::list_object::ListOperation, set::set_object::SetOperation,
    sortedset::sorted_set_object::SortedSetOperation,
  },
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

/// 列表操作码：直接取 ListOperation 判别式（C# ListOperation 编号）
const L_POP: u8 = ListOperation::Lpop as u8;
const L_PUSHX: u8 = ListOperation::Lpushx as u8;
const R_POP: u8 = ListOperation::Rpop as u8;
const R_PUSHX: u8 = ListOperation::Rpushx as u8;
const L_RANGE: u8 = ListOperation::Lrange as u8;
const L_INDEX: u8 = ListOperation::Lindex as u8;
const L_TRIM: u8 = ListOperation::Ltrim as u8;
const L_REM: u8 = ListOperation::Lrem as u8;
const L_INSERT: u8 = ListOperation::Linsert as u8;

/// 集合操作码：直接取 SetOperation 判别式（C# SetOperation：SCARD=4、SMEMBERS=3）
const S_CARD: u8 = SetOperation::Scard as u8;
const S_MEMBERS: u8 = SetOperation::Smembers as u8;
const S_REM: u8 = SetOperation::Srem as u8;
const S_POP: u8 = SetOperation::Spop as u8;

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
