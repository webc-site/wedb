//! 列表 RESP 语义操作（对标 libs/server/Objects/List/ListObjectImpl.cs，
//! C# 为 ListObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。
//! 刻意差异：C# RespMemoryWriter 的 ResetPosition/DecreaseArrayLength
//! （LPOS 预写数组头再回退）以"先收集命中、后统一输出"等价表达。

use crate::{
  inputs::ObjectInput,
  objects::{
    list::list_object::ListObject, parse_utils::try_get_int, types::object_output::ObjectOutput,
  },
};

// ---- CmdStrings 中列表域专用错误串（cmd_strings.rs 不在本周期改动范围） ----

/// ERR index out of range
const RESP_ERR_GENERIC_INDEX_OUT_RANGE: &[u8] = b"ERR index out of range";

use crate::resp::cmd_strings::{
  RESP_ERR_GENERIC_NOSUCHKEY, RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
};

/// 取第 i 个参数字节
///
#[inline]
fn arg<'a>(input: &ObjectInput, i: usize) -> &'a [u8] {
  input.parse_state.get_arg_slice_by_ref(i).as_slice()
}

impl ListObject {
  /// LREM：按计数方向移除元素
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListRemove
  pub(crate) fn list_remove(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    let count = input.arg1;

    //indicates partial execution
    output.result1 = i32::MIN as i64;

    // get the source string to remove
    let item_span = arg(input, 0);

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
  pub(crate) fn list_insert(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    //indicates partial execution
    output.result1 = i32::MIN as i64;

    if !self.list.is_empty() {
      // figure out where to insert BEFORE or AFTER
      let position = arg(input, 0);

      // get the source string
      let pivot = arg(input, 1);

      // get the string to INSERT into the list
      let item = arg(input, 2).to_vec();

      let insert_before = position.eq_ignore_ascii_case(b"BEFORE");

      output.result1 = -1;

      // find the first ocurrence of the pivot element
      if let Some(pos) = self.list.iter().position(|v| v.as_slice() == pivot) {
        let at = if insert_before { pos } else { pos + 1 };
        self.list.insert(at, item.clone());
        self.update_size(&item, true);
        output.result1 = self.list.len() as i64;
      }
    }
  }

  /// LINDEX：按下标取元素
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListIndex
  pub(crate) fn list_index(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    let index = input.arg1;

    output.result1 = -1;

    let len = self.list.len() as i64;
    let index = if index < 0 {
      len + i64::from(index)
    } else {
      i64::from(index)
    };

    if let Some(item) = self.list.get(index as usize) {
      output.write_bulk_string(item);
      output.result1 = 1;
    }
    // C# ElementAtOrDefault 越界回 null 项（item == default），此处以无负载表达
    let _ = resp_protocol_version;
  }

  /// LRANGE：闭区间取片段
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListRange
  pub(crate) fn list_range(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    let start = input.arg1;
    let stop = input.arg2;

    if self.list.is_empty() {
      // write empty list
      output.write_empty_array();
      return;
    }

    let len = self.list.len() as i64;
    let mut start = i64::from(start);
    let mut stop = i64::from(stop);

    start = if start < 0 { len + start } else { start };
    if start < 0 {
      start = 0;
    }

    stop = if stop < 0 { len + stop } else { stop };
    if stop >= len {
      stop = len - 1;
    }

    if start > stop {
      output.write_empty_array();
      return;
    }

    let count = (stop - start + 1) as usize;
    output.write_array_length(count);

    for item in self.list.iter().skip(start as usize).take(count) {
      output.write_bulk_string(item);
    }

    output.result1 = count as i64;
  }

  /// LTRIM：区间裁剪（保留 [start, stop]）
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListTrim
  pub(crate) fn list_trim(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    let start = input.arg1;
    let end = input.arg2;

    if !self.list.is_empty() {
      let len = self.list.len() as i64;
      let mut start = i64::from(start);
      let mut end = i64::from(end);

      start = if start < 0 { len + start } else { start };
      end = if end < 0 { len + end } else { end };

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
          let doomed: Vec<usize> = (0..len as usize)
            .filter(|i| !(*i >= start as usize && *i < end as usize))
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
  pub(crate) fn list_length(&mut self, output: &mut ObjectOutput) {
    output.result1 = self.list.len() as i64;
  }

  /// LPUSH / RPUSH / LPUSHX / RPUSHX：批量推入
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListPush
  pub(crate) fn list_push(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    f_add_at_head: bool,
  ) {
    output.result1 = 0;
    for i in 0..input.parse_state.count {
      let value = arg(input, i).to_vec();

      // Add the value to the top of the list
      if f_add_at_head {
        self.list.push_front(value.clone());
      } else {
        self.list.push_back(value.clone());
      }

      self.update_size(&value, true);
    }
    output.result1 = self.list.len() as i64;
  }

  /// LPOP / RPOP（含 count 形态）
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListPop
  pub(crate) fn list_pop(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
    f_del_at_head: bool,
  ) {
    let mut count = i64::from(input.arg1);

    if (self.list.len() as i64) < count {
      count = self.list.len() as i64;
    }

    if self.list.is_empty() {
      output.write_null(resp_protocol_version);
      count = 0;
    } else if count <= 0 {
      // LPOP/RPOP with an explicit count of 0 replies with an empty array.
      output.write_empty_array();
    } else if count > 1 {
      output.write_array_length(count as usize);
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
        output.write_bulk_string(&value);
      }

      count -= 1;

      removed += 1;
    }

    output.result1 = removed;
  }

  /// LSET：按下标覆写
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListSet
  pub(crate) fn list_set(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    if self.list.is_empty() {
      output.write_error(RESP_ERR_GENERIC_NOSUCHKEY.as_bytes());
      return;
    }

    // index
    let Some(index) = try_get_int(arg(input, 0)) else {
      output.write_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      return;
    };

    let len = self.list.len() as i64;
    let index = if index < 0 {
      len + i64::from(index)
    } else {
      i64::from(index)
    };

    if index > len - 1 || index < 0 {
      output.write_error(RESP_ERR_GENERIC_INDEX_OUT_RANGE);
      return;
    }

    // element
    let element = arg(input, 1).to_vec();

    let old = self.list[index as usize].clone();
    self.update_size(&old, false);
    self.update_size(&element, true);
    self.list[index as usize] = element;

    // C# writer.WriteDirect(CmdStrings.RESP_OK)
    output.payload.extend_from_slice(b"+OK\r\n");
    output.result1 = 1;
  }

  /// LPOS：定位元素第 rank 次出现（count/maxlen 可选）
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListPosition
  pub(crate) fn list_position(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    let element = arg(input, 0);

    // 默认形态：rank=1、count=1（缺省）、maxlen=0（不限）
    let mut params = ListPositionParams::default();

    if let Err(error) = read_list_position_input(input, &mut params) {
      output.write_error(error);
      return;
    }

    if params.count < 0 || params.maxlen < 0 || params.rank == 0 {
      output.write_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      return;
    }

    let count = if params.count == 0 {
      self.list.len() as i64
    } else {
      params.count
    };

    let mut found: Vec<i64> = Vec::new();

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
            found.push(current_index as i64);
            if found.len() as i64 == count {
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
            found.push(current_index);
            if found.len() as i64 == count {
              break;
            }
          } else {
            rank -= 1;
          }
        }
        current_index -= 1;
      }
    }

    // C# ResetPosition/DecreaseArrayLength 的等价形态：先收集命中，后统一输出
    let found_len = found.len();
    if params.is_default_count {
      if found.is_empty() {
        output.write_null(2);
      } else {
        output.write_int64(found[0]);
      }
    } else if found.is_empty() {
      output.write_empty_array();
    } else {
      output.write_array_length(found_len);
      for index in found {
        output.write_int64(index);
      }
    }

    output.result1 = found_len as i64;
  }
}

/// LPOS 解析产物
///
/// libs/server/Objects/List/ListObjectImpl.cs:ReadListPositionInput 出参束
#[derive(Debug, Clone, Copy)]
struct ListPositionParams {
  rank: i64,
  count: i64,
  is_default_count: bool,
  maxlen: i64,
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

/// 解析 LPOS 的 RANK/COUNT/MAXLEN 词元（C# 仅识别全大写/全小写两种词形）
///
/// libs/server/Objects/List/ListObjectImpl.cs:ReadListPositionInput
fn read_list_position_input(
  input: &ObjectInput,
  params: &mut ListPositionParams,
) -> Result<(), &'static [u8]> {
  let mut curr_token_idx = 1;

  while curr_token_idx < input.parse_state.count {
    let sb_param = arg(input, curr_token_idx);
    curr_token_idx += 1;

    if sb_param == b"RANK" || sb_param == b"rank" {
      let Some(rank) = try_get_int(arg(input, curr_token_idx)) else {
        return Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      };
      curr_token_idx += 1;
      params.rank = i64::from(rank);
    } else if sb_param == b"COUNT" || sb_param == b"count" {
      let Some(count) = try_get_int(arg(input, curr_token_idx)) else {
        return Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      };
      curr_token_idx += 1;
      params.count = i64::from(count);
      params.is_default_count = false;
    } else if sb_param == b"MAXLEN" || sb_param == b"maxlen" {
      let Some(maxlen) = try_get_int(arg(input, curr_token_idx)) else {
        return Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      };
      curr_token_idx += 1;
      params.maxlen = i64::from(maxlen);
    } else {
      return Err(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
    }
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    arg_slice::ArgSlice,
    input_header::RespInputHeader,
    objects::list::list_object::ListOperation,
    session_parse_state::SessionParseState,
    types::{GarnetObjectType, RespInputFlags},
  };

  /// 构造 ObjectInput（backing 须与 input 同生命周期存活）
  fn make_input(
    op: ListOperation,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
  ) -> (ObjectInput, Vec<Vec<u8>>) {
    let backing: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
    let slices: Vec<ArgSlice> = backing
      .iter()
      .map(|b| ArgSlice::new(b.as_ptr(), b.len()))
      .collect();
    let mut parse_state = SessionParseState::new();
    parse_state.initialize_with_args(&slices);

    let mut header =
      RespInputHeader::new_with_type(GarnetObjectType::List, RespInputFlags::empty());
    header.set_sub_id(op as u8);
    (
      ObjectInput::new_with_state(header, &mut parse_state, arg1, arg2),
      backing,
    )
  }

  fn obj_with(items: &[&str]) -> ListObject {
    let mut obj = ListObject::new();
    for item in items {
      obj.list.push_back(item.as_bytes().to_vec());
    }
    obj
  }

  /// LPUSH/RPUSH 头尾次序 + LPOP/RPOP + LLEN
  #[test]
  fn push_pop_len() {
    let mut obj = ListObject::new();

    // RPUSH a b c
    let (input, _b) = make_input(ListOperation::Rpush, &[b"a", b"b", b"c"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_push(&input, &mut out, false);
    assert_eq!(out.result1, 3);

    // LPUSH head
    let (input, _b) = make_input(ListOperation::Lpush, &[b"H"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_push(&input, &mut out, true);
    assert_eq!(out.result1, 4);
    assert_eq!(
      obj.to_items(),
      [b"H".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );

    // LPOP 无 count：取头
    let (input, _b) = make_input(ListOperation::Lpop, &[], 1, 0);
    let mut out = ObjectOutput::new();
    obj.list_pop(&input, &mut out, 2, true);
    assert_eq!(out.payload, b"$1\r\nH\r\n");
    assert_eq!(out.result1, 1);

    // RPOP count 2：取尾两枚
    let (input, _b) = make_input(ListOperation::Rpop, &[], 2, 0);
    let mut out = ObjectOutput::new();
    obj.list_pop(&input, &mut out, 2, false);
    assert_eq!(out.payload, b"*2\r\n$1\r\nc\r\n$1\r\nb\r\n");

    // LLEN
    let (_input, _b) = make_input(ListOperation::Llen, &[], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_length(&mut out);
    assert_eq!(out.result1, 1);

    // 弹空 → null + REMOVE_KEY
    let (input, _b) = make_input(ListOperation::Lpop, &[], 1, 0);
    let mut out = ObjectOutput::new();
    obj.list_pop(&input, &mut out, 2, true);
    assert_eq!(out.payload, b"$1\r\na\r\n");
    let (input, _b) = make_input(ListOperation::Lpop, &[], 1, 0);
    let mut out = ObjectOutput::new();
    obj.operate(&input, &mut out, 2);
    assert_eq!(out.payload, b"$-1\r\n");
    assert!(out.has_remove_key());

    // 显式 count=0 → 空数组
    let mut obj = obj_with(&["x"]);
    let (input, _b) = make_input(ListOperation::Lpop, &[], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_pop(&input, &mut out, 2, true);
    assert_eq!(out.payload, b"*0\r\n");
    assert_eq!(out.result1, 0);
    assert_eq!(obj.list.len(), 1);
  }

  /// LRANGE / LINDEX / LTRIM 边界
  #[test]
  fn range_index_trim() {
    let mut obj = obj_with(&["a", "b", "c", "d"]);

    // LRANGE 1 2
    let (input, _b) = make_input(ListOperation::Lrange, &[], 1, 2);
    let mut out = ObjectOutput::new();
    obj.list_range(&input, &mut out, 2);
    assert_eq!(out.payload, b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n");
    assert_eq!(out.result1, 2);

    // LRANGE -2 -1
    let (input, _b) = make_input(ListOperation::Lrange, &[], -2, -1);
    let mut out = ObjectOutput::new();
    obj.list_range(&input, &mut out, 2);
    assert_eq!(out.payload, b"*2\r\n$1\r\nc\r\n$1\r\nd\r\n");

    // LRANGE start > stop → 空数组
    let (input, _b) = make_input(ListOperation::Lrange, &[], 2, 1);
    let mut out = ObjectOutput::new();
    obj.list_range(&input, &mut out, 2);
    assert_eq!(out.payload, b"*0\r\n");

    // LINDEX -1
    let (input, _b) = make_input(ListOperation::Lindex, &[], -1, 0);
    let mut out = ObjectOutput::new();
    obj.list_index(&input, &mut out, 2);
    assert_eq!(out.payload, b"$1\r\nd\r\n");
    assert_eq!(out.result1, 1);

    // LINDEX 越界 → 无负载 + result1 -1
    let (input, _b) = make_input(ListOperation::Lindex, &[], 9, 0);
    let mut out = ObjectOutput::new();
    obj.list_index(&input, &mut out, 2);
    assert!(out.payload.is_empty());
    assert_eq!(out.result1, -1);

    // LTRIM 1 2
    let (input, _b) = make_input(ListOperation::Ltrim, &[], 1, 2);
    let mut out = ObjectOutput::new();
    obj.list_trim(&input, &mut out);
    assert_eq!(obj.to_items(), [b"b".to_vec(), b"c".to_vec()]);

    // LTRIM 全域外 → 清空（C# list.Clear()）
    let (input, _b) = make_input(ListOperation::Ltrim, &[], 5, 9);
    let mut out = ObjectOutput::new();
    obj.list_trim(&input, &mut out);
    assert!(obj.list.is_empty());

    // LTRIM start==0：尾部收缩
    let mut obj = obj_with(&["a", "b", "c"]);
    let (input, _b) = make_input(ListOperation::Ltrim, &[], 0, 1);
    let mut out = ObjectOutput::new();
    obj.list_trim(&input, &mut out);
    assert_eq!(out.result1, 1);
    assert_eq!(obj.to_items(), [b"a".to_vec(), b"b".to_vec()]);

    // LTRIM 中段：快照删除形态，result1 = 原长度
    let mut obj = obj_with(&["a", "b", "c", "d"]);
    let (input, _b) = make_input(ListOperation::Ltrim, &[], 1, 2);
    let mut out = ObjectOutput::new();
    obj.list_trim(&input, &mut out);
    assert_eq!(out.result1, 4);
    assert_eq!(obj.to_items(), [b"b".to_vec(), b"c".to_vec()]);

    // 空列表 LTRIM：无操作
    let mut obj = ListObject::new();
    let (input, _b) = make_input(ListOperation::Ltrim, &[], 0, 1);
    let mut out = ObjectOutput::new();
    obj.list_trim(&input, &mut out);
    assert!(out.payload.is_empty());
  }

  /// LINSERT / LREM 语义
  #[test]
  fn insert_remove() {
    let mut obj = obj_with(&["a", "b", "c", "b"]);

    // LINSERT BEFORE b X：首个 pivot 前
    let (input, _b) = make_input(ListOperation::Linsert, &[b"BEFORE", b"b", b"X"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_insert(&input, &mut out);
    assert_eq!(out.result1, 5);
    assert_eq!(
      obj.to_items(),
      ["a", "X", "b", "c", "b"]
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect::<Vec<_>>()
    );

    // LINSERT AFTER b Y
    let (input, _b) = make_input(ListOperation::Linsert, &[b"after", b"b", b"Y"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_insert(&input, &mut out);
    assert_eq!(out.result1, 6);
    assert_eq!(
      obj.to_items(),
      ["a", "X", "b", "Y", "c", "b"]
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect::<Vec<_>>()
    );

    // pivot 缺失 → -1
    let (input, _b) = make_input(ListOperation::Linsert, &[b"BEFORE", b"zz", b"Q"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_insert(&input, &mut out);
    assert_eq!(out.result1, -1);

    // 空列表 → 部分执行标记
    let mut empty = ListObject::new();
    let (input, _b) = make_input(ListOperation::Linsert, &[b"BEFORE", b"b", b"X"], 0, 0);
    let mut out = ObjectOutput::new();
    empty.list_insert(&input, &mut out);
    assert_eq!(out.result1, i32::MIN as i64);

    // LREM 0 b：全删
    let mut obj = obj_with(&["b", "a", "b", "c", "b"]);
    let (input, _b) = make_input(ListOperation::Lrem, &[b"b"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_remove(&input, &mut out);
    assert_eq!(out.result1, 3);
    assert_eq!(obj.to_items(), [b"a".to_vec(), b"c".to_vec()]);

    // LREM 1 a：从头删 1 枚
    let mut obj = obj_with(&["b", "a", "b"]);
    let (input, _b) = make_input(ListOperation::Lrem, &[b"b"], 1, 0);
    let mut out = ObjectOutput::new();
    obj.list_remove(&input, &mut out);
    assert_eq!(out.result1, 1);
    assert_eq!(obj.to_items(), [b"a".to_vec(), b"b".to_vec()]);

    // LREM -2 b：从尾删 2 枚
    let mut obj = obj_with(&["b", "a", "b", "c", "b"]);
    let (input, _b) = make_input(ListOperation::Lrem, &[b"b"], -2, 0);
    let mut out = ObjectOutput::new();
    obj.list_remove(&input, &mut out);
    assert_eq!(out.result1, 2);
    assert_eq!(
      obj.to_items(),
      [b"b".to_vec(), b"a".to_vec(), b"c".to_vec()]
    );
  }

  /// LSET 边界
  #[test]
  fn set_index() {
    let mut obj = obj_with(&["a", "b", "c"]);

    let (input, _b) = make_input(ListOperation::Lset, &[b"1", b"B"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_set(&input, &mut out, 2);
    assert_eq!(out.payload, b"+OK\r\n");
    assert_eq!(out.result1, 1);
    assert_eq!(
      obj.to_items(),
      [b"a".to_vec(), b"B".to_vec(), b"c".to_vec()]
    );

    // 负下标
    let (input, _b) = make_input(ListOperation::Lset, &[b"-1", b"C"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_set(&input, &mut out, 2);
    assert_eq!(obj.to_items()[2], b"C".to_vec());

    // 越界
    let (input, _b) = make_input(ListOperation::Lset, &[b"9", b"Z"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_set(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR index out of range\r\n");

    // 非整数
    let (input, _b) = make_input(ListOperation::Lset, &[b"x", b"Z"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_set(&input, &mut out, 2);
    assert_eq!(
      out.payload,
      b"-ERR value is not an integer or out of range.\r\n"
    );

    // 空列表 → no such key
    let mut empty = ListObject::new();
    let (input, _b) = make_input(ListOperation::Lset, &[b"0", b"Z"], 0, 0);
    let mut out = ObjectOutput::new();
    empty.list_set(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR no such key\r\n");
  }

  /// LPOS：rank/count/maxlen 全矩阵
  #[test]
  fn position_matrix() {
    let mut obj = obj_with(&["a", "b", "a", "c", "a", "b"]);

    // 缺省：第一个 a
    let (input, _b) = make_input(ListOperation::Lpos, &[b"a"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_position(&input, &mut out, 2);
    assert_eq!(out.payload, b":0\r\n");
    assert_eq!(out.result1, 1);

    // RANK 2：第二个 a
    let (input, _b) = make_input(ListOperation::Lpos, &[b"a", b"RANK", b"2"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_position(&input, &mut out, 2);
    assert_eq!(out.payload, b":2\r\n");

    // RANK -1：最后一个 a
    let (input, _b) = make_input(ListOperation::Lpos, &[b"a", b"rank", b"-1"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_position(&input, &mut out, 2);
    assert_eq!(out.payload, b":4\r\n");

    // COUNT 0：全部命中
    let (input, _b) = make_input(ListOperation::Lpos, &[b"a", b"COUNT", b"0"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_position(&input, &mut out, 2);
    assert_eq!(out.payload, b"*3\r\n:0\r\n:2\r\n:4\r\n");

    // COUNT 2：前两个命中
    let (input, _b) = make_input(ListOperation::Lpos, &[b"a", b"count", b"2"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_position(&input, &mut out, 2);
    assert_eq!(out.payload, b"*2\r\n:0\r\n:2\r\n");

    // MAXLEN 2：只扫前两枚（无命中 → 缺省 count → null）
    let (input, _b) = make_input(ListOperation::Lpos, &[b"c", b"MAXLEN", b"2"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_position(&input, &mut out, 2);
    assert_eq!(out.payload, b"$-1\r\n");

    // 未命中 + 显式 COUNT → 空数组
    let (input, _b) = make_input(ListOperation::Lpos, &[b"zz", b"COUNT", b"2"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_position(&input, &mut out, 2);
    assert_eq!(out.payload, b"*0\r\n");

    // RANK 0 → 错误
    let (input, _b) = make_input(ListOperation::Lpos, &[b"a", b"RANK", b"0"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_position(&input, &mut out, 2);
    assert_eq!(
      out.payload,
      b"-ERR value is not an integer or out of range.\r\n"
    );

    // 未知词元 → syntax error（大小写混排不可识别，1:1 对齐 C#）
    let (input, _b) = make_input(ListOperation::Lpos, &[b"a", b"Rank", b"1"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.list_position(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR syntax error\r\n");

    // RANK -2 自尾向头 + COUNT：尾侧首个命中（4）仅推进 rank，从次个命中起记录
    let (input, _b) = make_input(
      ListOperation::Lpos,
      &[b"a", b"RANK", b"-2", b"COUNT", b"2"],
      0,
      0,
    );
    let mut out = ObjectOutput::new();
    obj.list_position(&input, &mut out, 2);
    assert_eq!(out.payload, b"*2\r\n:2\r\n:0\r\n");
  }

  /// operate 分派冒烟：LPUSH 新建 → 删空 REMOVE_KEY；阻塞族缺省分支
  #[test]
  fn operate_dispatch_smoke() {
    let mut obj = ListObject::new();
    let (input, _b) = make_input(ListOperation::Lpush, &[b"x"], 0, 0);
    let mut out = ObjectOutput::new();
    assert!(obj.operate(&input, &mut out, 2));
    assert_eq!(out.result1, 1);

    let (input, _b) = make_input(ListOperation::Lpop, &[], 1, 0);
    let mut out = ObjectOutput::new();
    obj.operate(&input, &mut out, 2);
    assert!(out.has_remove_key());

    // LMOVE 不经对象层 operate（C# switch default 抛 GarnetException）
    let (input, _b) = make_input(ListOperation::Lmove, &[], 0, 0);
    let mut out = ObjectOutput::new();
    obj.operate(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR unsupported operation\r\n");

    // 类型不符 → WRONGTYPE
    let (input, _b) = make_input(ListOperation::Llen, &[], 0, 0);
    let mut input = input;
    input.header.data[0] = GarnetObjectType::Hash as u8;
    let mut out = ObjectOutput::new();
    obj.operate(&input, &mut out, 2);
    assert!(out.has_wrong_type());
  }
}
