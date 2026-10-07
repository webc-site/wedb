//! 列表 RESP 语义操作（对标 libs/server/Objects/List/ListObjectImpl.cs，
//! C# 为 ListObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。
//! 刻意差异：C# RespMemoryWriter 的 ResetPosition/DecreaseArrayLength
//! （LPOS COUNT 形态预写数组头再回退）以"先收集命中、后统一输出"等价表达；
//! LPOS 缺省形态（C# 命中即直写整数、无数组头）以栈上标量槽承接，免收集免分配。
//!
//! 在 garnet 中的相对路径: libs/server/Storage/Session/ObjectStore/ListObject.cs（List 原语）

use std::mem::replace;

use wbase::{eq_ascii_case_const, num::strict_i32};
use wresp::{
  cmd_strings::{
    COUNT, MAXLEN, RANK, RESP_ERR_GENERIC_INDEX_OUT_RANGE, RESP_ERR_GENERIC_NOSUCHKEY,
    RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_OK,
  },
  resp_memory_writer::RespWriter,
};

use super::list_object::ListObject;
use crate::{
  resp::output::write_null,
  types::{ObjectOutput, norm},
};

impl ListObject {
  /// LREM：按计数方向移除元素
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListRemove
  pub(crate) fn list_remove(&mut self, args: &[&[u8]], arg1: i32, output: &mut ObjectOutput<'_>) {
    let count = arg1;

    // get the source string to remove
    let item_span = args[0];

    let mut removed_count = 0_i64;
    output.result1 = 0;

    //remove all equals to item
    if count == 0 {
      let mut i = 0;
      while i < self.list.len() {
        if self.list[i].as_slice() == item_span {
          let value = self.list.remove(i).unwrap();
          self.update_size(&value, false);
          removed_count += 1;
        } else {
          i += 1;
        }
      }
    } else {
      let from_head_to_tail = count > 0;
      // |int.MinValue| 不适配 i64 取绝对值路径：钳制为 i32::MAX（迭代上限受
      // 列表长度约束，钳制后移除数与 C# Math.Abs 一致）
      let count = if count == i32::MIN {
        i32::MAX as i64
      } else {
        (count).abs() as i64
      };

      let mut idx = if from_head_to_tail {
        0
      } else {
        self.list.len() as i64 - 1
      };

      while removed_count < count && (0..self.list.len() as i64).contains(&idx) {
        let matches = self.list[idx as usize].as_slice() == item_span;
        if matches {
          let value = self.list.remove(idx as usize).unwrap();
          self.update_size(&value, false);
          removed_count += 1;
          if !from_head_to_tail {
            idx -= 1;
          }
        } else {
          idx += if from_head_to_tail { 1 } else { -1 };
        }
      }
    }
    output.result1 = removed_count;
  }

  /// LINSERT：在首个 pivot 前后插入
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListInsert
  pub(crate) fn list_insert(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    //indicates partial execution
    output.result1 = i32::MIN as i64;

    if !self.list.is_empty() {
      // figure out where to insert BEFORE or AFTER
      let position = args[0];

      // get the source string
      let pivot = args[1];

      let insert_before = position.eq_ignore_ascii_case(b"BEFORE");

      output.result1 = -1;

      // find the first ocurrence of the pivot element
      if let Some(pos) = self.list.iter().position(|v| v.as_slice() == pivot) {
        let item = args[2].to_vec();
        self.update_size(&item, true);
        let at = if insert_before { pos } else { pos + 1 };
        self.list.insert(at, item);
        output.result1 = self.list.len() as i64;
      }
    }
  }

  /// LINDEX：按下标取元素
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListIndex
  pub(crate) fn list_index(&mut self, _args: &[&[u8]], arg1: i32, output: &mut ObjectOutput<'_>) {
    output.result1 = -1;

    let index = norm(i64::from(arg1), self.list.len() as i64);

    // C# ElementAtOrDefault 越界回 null 项（item == default），此处以无负载表达
    if let Some(item) = self.list.get(index as usize) {
      RespWriter::new_ref(output.payload).write_bulk_string(item);
      output.result1 = 1;
    }
  }

  /// LRANGE：闭区间取片段
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListRange
  pub(crate) fn list_range(
    &mut self,
    _args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
  ) {
    if self.list.is_empty() {
      // write empty list
      RespWriter::new_ref(output.payload).write_empty_array();
      return;
    }

    let len = self.list.len() as i64;
    // 闭区间裁剪：负下标换算后 start 钳下界 0、stop 钳上界 len-1
    let start = norm(i64::from(arg1), len).max(0);
    let stop = norm(i64::from(arg2), len).min(len - 1);

    if start > stop {
      RespWriter::new_ref(output.payload).write_empty_array();
      return;
    }

    let count = (stop - start + 1) as usize;
    RespWriter::new_ref(output.payload).write_array_length(count);

    for item in self.list.iter().skip(start as usize).take(count) {
      RespWriter::new_ref(output.payload).write_bulk_string(item);
    }

    output.result1 = count as i64;
  }

  /// LTRIM：区间裁剪（保留 [start, stop]）
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListTrim
  pub(crate) fn list_trim(
    &mut self,
    _args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
  ) {
    if !self.list.is_empty() {
      let len = self.list.len() as i64;
      let mut start = norm(i64::from(arg1), len);
      let mut end = norm(i64::from(arg2), len);

      if start > end || start >= len || end < 0 {
        let removed: Vec<Vec<u8>> = self.list.drain(..).collect();
        for value in removed {
          self.update_size(&value, false);
        }
      } else {
        start = start.max(0);
        end = if end >= len { len } else { end + 1 };

        // Only the first end elements will remain
        if start == 0 {
          let num_deletes = len - end;
          for _ in 0..num_deletes {
            if let Some(value) = self.list.pop_back() {
              self.update_size(&value, false);
            }
          }
          output.result1 = num_deletes;
        } else {
          // 保留 [start, end)：先收集后删除（C# 经只读快照迭代原表删除）
          let doomed: Vec<usize> = (0..start as usize)
            .chain(end as usize..len as usize)
            .collect();
          for (offset, i) in doomed.iter().enumerate() {
            let value = self.list.remove(i - offset).unwrap();
            self.update_size(&value, false);
          }
          output.result1 = len;
        }
      }
    }
  }

  /// LLEN：长度
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListLength
  pub(crate) fn list_length(&mut self, output: &mut ObjectOutput<'_>) {
    output.result1 = self.list.len() as i64;
  }

  /// LPUSH / RPUSH / LPUSHX / RPUSHX：批量推入
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListPush
  pub(crate) fn list_push(
    &mut self,
    args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
    f_add_at_head: bool,
  ) {
    for &arg in args {
      self.update_size(arg, true);
      let value = arg.to_vec();

      // Add the value to the top of the list
      if f_add_at_head {
        self.list.push_front(value);
      } else {
        self.list.push_back(value);
      }
    }
    output.result1 = self.list.len() as i64;
  }

  /// LPOP / RPOP（含 count 形态）
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListPop
  pub(crate) fn list_pop(
    &mut self,
    _args: &[&[u8]],
    arg1: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
    f_del_at_head: bool,
  ) {
    let mut count = i64::from(arg1);

    if (self.list.len() as i64) < count {
      count = self.list.len() as i64;
    }

    if self.list.is_empty() {
      write_null(output, resp_protocol_version);
      count = 0;
    } else if count <= 0 {
      // LPOP/RPOP with an explicit count of 0 replies with an empty array.
      RespWriter::new_ref(output.payload).write_empty_array();
    } else if count > 1 {
      RespWriter::new_ref(output.payload).write_array_length(count as usize);
    }

    let mut removed = 0_i64;

    while count > 0 && !self.list.is_empty() {
      let value = if f_del_at_head {
        self.list.pop_front()
      } else {
        self.list.pop_back()
      };

      if let Some(value) = value {
        self.update_size(&value, false);
        RespWriter::new_ref(output.payload).write_bulk_string(&value);
      }

      count -= 1;

      removed += 1;
    }

    output.result1 = removed;
  }

  /// LSET：按下标覆写
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListSet
  pub(crate) fn list_set(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    if self.list.is_empty() {
      RespWriter::new_ref(output.payload).write_error_bytes(RESP_ERR_GENERIC_NOSUCHKEY.as_bytes());
      return;
    }

    // index
    let Some(index) = strict_i32(args[0]) else {
      RespWriter::new_ref(output.payload)
        .write_error_bytes(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      return;
    };

    let len = self.list.len() as i64;
    let index = norm(i64::from(index), len);

    if index > len - 1 || index < 0 {
      RespWriter::new_ref(output.payload)
        .write_error_bytes(RESP_ERR_GENERIC_INDEX_OUT_RANGE.as_bytes());
      return;
    }

    // element
    let element = args[1].to_vec();

    // 记账换值零拷贝（对位 C# 节点值引用直传）：element 局部先记新值，mem::replace 整包
    // 移出旧值免 clone；记账为 i64 加减可换序，先加后减使 false 臂 debug_assert 底线不减穿
    self.update_size(&element, true);
    let old = replace(&mut self.list[index as usize], element);
    self.update_size(&old, false);

    // C# writer.WriteDirect(CmdStrings.RESP_OK)
    output.payload.extend_from_slice(RESP_OK);
    output.result1 = 1;
  }

  /// LPOS：定位元素第 rank 次出现（count/maxlen 可选）
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListPosition
  pub(crate) fn list_position(
    &mut self,
    args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    let element = args[0];

    // 词元解析 + 三门拒绝单源（与分层态树内臂同一份码，见
    // [`read_list_position_params`]）
    let params = match read_list_position_params(args) {
      Ok(params) => params,
      Err(error) => {
        RespWriter::new_ref(output.payload).write_error_bytes(error);
        return;
      }
    };

    let count = if params.count == 0 {
      self.list.len() as i64
    } else {
      params.count
    };

    // 只在「记结果」一点上分形：默认形态栈上标量槽（C# count 恒 1、命中即
    // WriteInt32 后 break，零中间容器），显式 COUNT 形态保留动态数组收集
    let mut hits = Hits::new(params.is_default_count);

    if params.rank > 0 {
      let mut rank = params.rank;
      let len = self.list.len() as i64;
      let maxlen_index = if params.maxlen == 0 {
        len
      } else {
        params.maxlen
      };

      for (current_index, item) in self.list.iter().enumerate().take(maxlen_index as usize) {
        if item.as_slice() == element {
          if rank == 1 {
            if hits.record(current_index as i64, count) {
              break;
            }
          } else {
            rank -= 1;
          }
        }
      }
    } else {
      // rank < 0：自尾向头
      let mut rank = params.rank.unsigned_abs() as i64;
      let len = self.list.len() as i64;
      let maxlen_index = if params.maxlen == 0 {
        0
      } else {
        len - params.maxlen
      };

      let mut current_index = len - 1;
      while current_index >= maxlen_index && current_index >= 0 {
        if self.list[current_index as usize].as_slice() == element {
          if rank == 1 {
            if hits.record(current_index, count) {
              break;
            }
          } else {
            rank -= 1;
          }
        }
        current_index -= 1;
      }
    }

    // 出帧单点（C# ResetPosition/DecreaseArrayLength 的等价形态：先记命中、后统一成帧）
    match &hits {
      Hits::One(hit) => match *hit {
        Some(index) => RespWriter::new_ref(output.payload).write_int64(index),
        // C# RespMemoryWriter.WriteNull 按会话协商版本写 null
        None => write_null(output, resp_protocol_version),
      },
      Hits::Many(indexes) => {
        if indexes.is_empty() {
          RespWriter::new_ref(output.payload).write_empty_array();
        } else {
          RespWriter::new_ref(output.payload).write_array_length(indexes.len());
          for index in indexes {
            RespWriter::new_ref(output.payload).write_int64(*index);
          }
        }
      }
    }

    output.result1 = hits.len() as i64;
  }
}

/// LPOS 命中容器：两支输出共用一处出帧逻辑，仅在记结果处分标量/数组
///
/// C# (`ListObjectImpl.cs:340-461`) 命中即 `writer.WriteInt32` 直写、批尾按
/// `noOfFoundItem` 复位或缩头，全程零中间容器；rust 侧统一为「先记命中、后
/// 成帧」单点（见 [`ListObject::list_position`]），故缺省形态（无 COUNT，count
/// 恒 1、首命中即止）以栈上 `Option<i64>` 承接，显式 COUNT 形态（COUNT 可达
/// 上千）保留动态数组
enum Hits {
  /// 默认形态：首个命中下标
  One(Option<i64>),
  /// 显式 COUNT 形态：命中下标序列
  Many(Vec<i64>),
}

impl Hits {
  #[inline]
  fn new(is_default_count: bool) -> Self {
    if is_default_count {
      Self::One(None)
    } else {
      Self::Many(Vec::new())
    }
  }

  /// 记一次命中，返回是否已满 count（C# `noOfFoundItem == count` 的 break 判据）
  #[inline]
  fn record(&mut self, index: i64, count: i64) -> bool {
    match self {
      // 默认形态 count 恒 1：首命中即满
      Self::One(hit) => {
        *hit = Some(index);
        true
      }
      Self::Many(indexes) => {
        indexes.push(index);
        indexes.len() as i64 == count
      }
    }
  }

  /// 命中数（C# `noOfFoundItem` → `output.result1`）
  #[inline]
  fn len(&self) -> usize {
    match self {
      Self::One(hit) => usize::from(hit.is_some()),
      Self::Many(indexes) => indexes.len(),
    }
  }
}

/// LPOS 解析产物
///
/// C# ReadListPositionInput 臂的出参束（同名对位锚留 [`read_list_position_input`] 一处）
#[derive(Debug, Clone, Copy)]
pub struct ListPositionParams {
  pub rank: i64,
  pub count: i64,
  pub is_default_count: bool,
  pub maxlen: i64,
}

impl Default for ListPositionParams {
  fn default() -> Self {
    // By default, LPOS takes first match element; return 1 element; iterate to all the item
    Self {
      rank: 1,
      count: 1,
      is_default_count: true,
      maxlen: 0,
    }
  }
}

/// LPOS 入参单源：RANK/COUNT/MAXLEN 词元解析 + 三门拒绝（rank==0 / count<0 /
/// maxlen<0），`Err` 携带待写出的错误帧字节（not-an-integer / syntax）
///
/// 信封态 [`ListObject::list_position`] 与分层态树内臂
///（wnode `tiered_collection_ops/list.rs` Lpos 臂）共用本函数，双态词元、
/// 三门与 isDefaultCount 判定同一份码，严禁第二解析器。
///
/// 对位 C# ReadListPositionInput + ListPosition 三门（词元解析末端锚留
/// [`read_list_position_input`] 一处）
pub fn read_list_position_params(args: &[&[u8]]) -> Result<ListPositionParams, &'static [u8]> {
  // 默认形态：rank=1、count=1（缺省）、maxlen=0（不限）
  let mut params = ListPositionParams::default();
  read_list_position_input(args, &mut params)?;

  // 入参边界三门拒绝（rank==0 / count<0 / maxlen<0），回标准 not-an-integer
  // 错误，与 C# ListObjectImpl.cs:355-371 的 count/maxlen/rank 三门同形收敛。
  // 历史上 C# 参考形态无此防线（静默回 null / 负数畸形帧），rust 先单侧拦截
  // 修复；后 C# 上游于 LPOS 引入提交同批写入三门收敛，单侧修复偏差随之消亡，
  // 此处为双侧同帧的对齐防线，非单侧修复面，无在册条目。
  if params.count < 0 || params.maxlen < 0 || params.rank == 0 {
    return Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
  }
  Ok(params)
}

/// 解析 LPOS 的 RANK/COUNT/MAXLEN 词元
///
/// 参数匹配大小写不敏感（eq_ascii_case_const），统一 Redis 命令选项的容忍语义
///
/// libs/server/Objects/List/ListObjectImpl.cs:ReadListPositionInput
fn read_list_position_input(
  args: &[&[u8]],
  params: &mut ListPositionParams,
) -> Result<(), &'static [u8]> {
  let count = args.len();
  let mut curr_token_idx = 1;

  let parse_i32_arg = |idx: &mut usize| -> Result<i64, &'static [u8]> {
    if *idx >= count {
      return Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
    }
    let val = strict_i32(args[*idx]).ok_or(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes())?;
    *idx += 1;
    Ok(i64::from(val))
  };

  while curr_token_idx < count {
    let sb_param = args[curr_token_idx];
    curr_token_idx += 1;

    // 词元按长度分派（房内范式 parse_utils.rs / bitfield/parse.rs）：长度不匹配
    // 直接落语法错误臂，免逐词元全量比较；未识别词元同样回语法错误
    match sb_param.len() {
      4 if eq_ascii_case_const(sb_param, RANK) => {
        params.rank = parse_i32_arg(&mut curr_token_idx)?;
      }
      5 => {
        if eq_ascii_case_const(sb_param, COUNT) {
          params.count = parse_i32_arg(&mut curr_token_idx)?;
          params.is_default_count = false;
        } else {
          return Err(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
        }
      }
      6 if eq_ascii_case_const(sb_param, MAXLEN) => {
        params.maxlen = parse_i32_arg(&mut curr_token_idx)?;
      }
      _ => return Err(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes()),
    }
  }

  Ok(())
}
