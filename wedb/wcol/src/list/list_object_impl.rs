//! 列表 RESP 语义操作（对标 libs/server/Objects/List/ListObjectImpl.cs，
//! C# 为 ListObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。
//! 刻意差异：C# RespMemoryWriter 的 ResetPosition/DecreaseArrayLength
//! （LPOS COUNT 形态预写数组头再回退）以"先收集命中、后统一输出"等价表达；
//! LPOS 缺省形态（C# 命中即直写整数、无数组头）以栈上标量槽承接，免收集免分配。

use wbase::num::strict_i32;
use wresp::{
  cmd_strings::{
    COUNT, MAXLEN, RANK, RESP_ERR_GENERIC_INDEX_OUT_RANGE, RESP_ERR_GENERIC_NOSUCHKEY,
    RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_OK,
  },
  resp_memory_writer::RespWriter,
};

use super::list_object::ListObject;
use crate::{resp::output::write_null, types::ObjectOutput};

impl ListObject {
  /// LREM：按计数方向移除元素
  ///
  /// libs/server/Objects/List/ListObjectImpl.cs:ListRemove
  pub(crate) fn list_remove(&mut self, args: &[&[u8]], arg1: i32, output: &mut ObjectOutput<'_>) {
    let count = arg1;

    //indicates partial execution
    output.result1 = i32::MIN as i64;

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

      // get the string to INSERT into the list
      let item = args[2].to_vec();

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
  pub(crate) fn list_index(&mut self, _args: &[&[u8]], arg1: i32, output: &mut ObjectOutput<'_>) {
    let index = arg1;

    output.result1 = -1;

    let len = self.list.len() as i64;
    let index = if index < 0 {
      len + i64::from(index)
    } else {
      i64::from(index)
    };

    if let Some(item) = self.list.get(index as usize) {
      RespWriter::new_ref(output.payload).write_bulk_string(item);
      output.result1 = 1;
    }
    // C# ElementAtOrDefault 越界回 null 项（item == default），此处以无负载表达
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
    let start = arg1;
    let stop = arg2;

    if self.list.is_empty() {
      // write empty list
      RespWriter::new_ref(output.payload).write_empty_array();
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
    let start = arg1;
    let end = arg2;

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
    let index = if index < 0 {
      len + i64::from(index)
    } else {
      i64::from(index)
    };

    if index > len - 1 || index < 0 {
      RespWriter::new_ref(output.payload)
        .write_error_bytes(RESP_ERR_GENERIC_INDEX_OUT_RANGE.as_bytes());
      return;
    }

    // element
    let element = args[1].to_vec();

    let old = self.list[index as usize].clone();
    self.update_size(&old, false);
    self.update_size(&element, true);
    self.list[index as usize] = element;

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

    // 默认形态：rank=1、count=1（缺省）、maxlen=0（不限）
    let mut params = ListPositionParams::default();

    if let Err(error) = read_list_position_input(args, &mut params) {
      RespWriter::new_ref(output.payload).write_error_bytes(error);
      return;
    }

    if params.count < 0 || params.maxlen < 0 || params.rank == 0 {
      RespWriter::new_ref(output.payload)
        .write_error_bytes(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      return;
    }

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

/// 解析 LPOS 的 RANK/COUNT/MAXLEN 词元
///
/// 参数匹配大小写不敏感（eq_ignore_ascii_case），统一 Redis 命令选项的容忍语义
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

    if sb_param.eq_ignore_ascii_case(RANK) {
      params.rank = parse_i32_arg(&mut curr_token_idx)?;
    } else if sb_param.eq_ignore_ascii_case(COUNT) {
      params.count = parse_i32_arg(&mut curr_token_idx)?;
      params.is_default_count = false;
    } else if sb_param.eq_ignore_ascii_case(MAXLEN) {
      params.maxlen = parse_i32_arg(&mut curr_token_idx)?;
    } else {
      return Err(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
    }
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_read_list_position_input() {
    // 词元大小写不敏感（全大写、全小写、混合形态）
    let mut params = ListPositionParams::default();
    assert!(
      read_list_position_input(
        &[
          b"elem".as_slice(),
          b"RANK",
          b"2",
          b"count",
          b"5",
          b"MAXLEN",
          b"100",
        ],
        &mut params,
      )
      .is_ok()
    );
    assert_eq!(params.rank, 2);
    assert_eq!(params.count, 5);
    assert!(!params.is_default_count);
    assert_eq!(params.maxlen, 100);

    // 缺少参数值边界守卫（防止越界 panic）
    for opt in [
      b"RANK".as_slice(),
      b"COUNT",
      b"maxlen",
      b"Rank",
      b"Count",
      b"MaxLen",
    ] {
      let mut p = ListPositionParams::default();
      assert_eq!(
        read_list_position_input(&[b"elem".as_slice(), opt], &mut p),
        Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes())
      );
    }

    // 非整数值
    let mut p = ListPositionParams::default();
    assert_eq!(
      read_list_position_input(&[b"elem".as_slice(), b"rank", b"abc"], &mut p),
      Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes())
    );

    // 未知选项
    let mut p = ListPositionParams::default();
    assert_eq!(
      read_list_position_input(&[b"elem".as_slice(), b"UNKNOWN"], &mut p),
      Err(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes())
    );

    // 混合大小写词元匹配（eq_ignore_ascii_case）
    for (opt, val_str, expect_rank, expect_count, expect_maxlen) in [
      (b"Rank".as_slice(), b"2".as_slice(), 2, 1, 0),
      (b"rAnK", b"-2", -2, 1, 0),
      (b"CounT", b"4", 1, 4, 0),
      (b"cOuNt", b"5", 1, 5, 0),
      (b"MaxLen", b"50", 1, 1, 50),
      (b"mAxLeN", b"60", 1, 1, 60),
    ] {
      let mut p = ListPositionParams::default();
      assert!(read_list_position_input(&[b"elem".as_slice(), opt, val_str], &mut p).is_ok());
      assert_eq!(p.rank, expect_rank);
      assert_eq!(p.count, expect_count);
      assert_eq!(p.maxlen, expect_maxlen);
    }

    // 未知/畸变选项仍报语法错误
    for opt in [b"Rankk".as_slice(), b"CountX", b"Max_Len", b"UNKNOWN"] {
      let mut p = ListPositionParams::default();
      assert_eq!(
        read_list_position_input(&[b"elem".as_slice(), opt, b"1"], &mut p),
        Err(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes())
      );
    }
  }

  /// 分配计数探针（仅测试构建）：LPOS 默认形态零堆分配判据的观测口
  ///
  /// 转调 [`std::alloc::System`] 只追加计数，不改变分配语义；nextest 逐用例
  /// 独立进程执行，故计数差即本用例该段代码的分配次数。
  mod probe {
    use std::{
      alloc::{GlobalAlloc, Layout, System},
      sync::atomic::{AtomicUsize, Ordering},
    };

    static ALLOCS: AtomicUsize = AtomicUsize::new(0);

    struct Counting;

    // SAFETY: alloc/dealloc 原样转调 System，计数仅为无副作用的原子自增；
    // realloc/alloc_zeroed 走 trait 默认实现（即本 alloc/dealloc 组合）
    unsafe impl GlobalAlloc for Counting {
      unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
      }

      unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
      }
    }

    #[global_allocator]
    static COUNTING: Counting = Counting;

    /// 至今累计的分配次数
    pub(super) fn allocs() -> usize {
      ALLOCS.load(Ordering::Relaxed)
    }
  }

  /// 样本表 `[a, c, b, c, d]`（C# RespListTests.cs:LPOSWithOptions 的列表形态）
  fn sample_list() -> ListObject {
    let mut obj = ListObject::default();
    for value in [b"a", b"c", b"b", b"c", b"d"] {
      obj.list.push_back(value.to_vec());
    }
    obj
  }

  /// 跑一枪 LPOS，回（应答字节, result1）
  fn lpos(obj: &mut ListObject, args: &[&[u8]], resp_protocol_version: u8) -> (Vec<u8>, i64) {
    let mut payload = Vec::new();
    let result1;
    {
      let mut out = ObjectOutput::mount(&mut payload);
      obj.list_position(args, &mut out, resp_protocol_version);
      result1 = out.result1;
    }
    (payload, result1)
  }

  /// 默认形态（无 COUNT）：命中直写整数、未命中 null，且全程零堆分配
  ///
  /// 证伪判据：两支共用无条件 `Vec<i64>` 收集时，本用例至少付一次 8 字节分配。
  #[test]
  fn lpos_default_form_scalar_is_zero_alloc() {
    let mut obj = sample_list();
    // 出帧缓冲预留足量容量，隔离应答字节本身的扩容，使计数只反映命中容器
    let mut payload = Vec::with_capacity(64);

    // 命中：首个出现下标
    let before = probe::allocs();
    {
      let mut out = ObjectOutput::mount(&mut payload);
      obj.list_position(&[b"c"], &mut out, 2);
      assert_eq!(out.payload_view(), b":1\r\n");
      assert_eq!(out.result1, 1);
    }
    assert_eq!(probe::allocs() - before, 0, "默认形态命中路径不得堆分配");

    // 未命中出口：RESP2 bulk null / RESP3 null，同样零分配
    for (version, expect) in [(2u8, &b"$-1\r\n"[..]), (3u8, &b"_\r\n"[..])] {
      payload.clear();
      let before = probe::allocs();
      {
        let mut out = ObjectOutput::mount(&mut payload);
        obj.list_position(&[b"nx", b"RANK", b"-1"], &mut out, version);
        assert_eq!(out.payload_view(), expect);
        assert_eq!(out.result1, 0);
      }
      assert_eq!(probe::allocs() - before, 0, "默认形态未命中路径不得堆分配");
    }

    // 混合大小写选项仍保持零堆分配
    for opt in [b"Rank".as_slice(), b"rAnK"] {
      payload.clear();
      let before = probe::allocs();
      {
        let mut out = ObjectOutput::mount(&mut payload);
        obj.list_position(&[b"c", opt, b"-2"], &mut out, 2);
        assert_eq!(out.payload_view(), b":1\r\n");
        assert_eq!(out.result1, 1);
      }
      assert_eq!(
        probe::allocs() - before,
        0,
        "混合大小写选项命中路径不得堆分配"
      );
    }

    // rank 越界：缺省形态无第 3 次出现 → null
    let (payload, result1) = lpos(&mut obj, &[b"c", b"RANK", b"3"], 2);
    assert_eq!(payload, b"$-1\r\n");
    assert_eq!(result1, 0);

    // rank 反向命中 + maxlen 截断（缺省 COUNT 仍标量直写）
    let (payload, _) = lpos(&mut obj, &[b"c", b"RANK", b"-2"], 2);
    assert_eq!(payload, b":1\r\n");
    let (payload, _) = lpos(&mut obj, &[b"c", b"MAXLEN", b"1"], 2);
    assert_eq!(payload, b"$-1\r\n");
  }

  /// 显式 COUNT 形态：保留收集，数组应答与 C# 预写数组头/缩头逐字节全等
  #[test]
  fn lpos_count_form_collects_array() {
    let mut obj = sample_list();

    // 多命中数组（正向）
    let (payload, result1) = lpos(&mut obj, &[b"c", b"COUNT", b"2"], 2);
    assert_eq!(payload, b"*2\r\n:1\r\n:3\r\n");
    assert_eq!(result1, 2);

    // 词元全形态（大写/小写/混合大小写）+ count=0 全量扫描（C# count = list.Count，尾段缩头）
    for token in [b"COUNT".as_slice(), b"count", b"Count", b"cOuNt"] {
      let (payload, result1) = lpos(&mut obj, &[b"c", token, b"0"], 2);
      assert_eq!(payload, b"*2\r\n:1\r\n:3\r\n");
      assert_eq!(result1, 2);
    }

    // 反向：自尾向头的命中序
    let (payload, result1) = lpos(&mut obj, &[b"c", b"RANK", b"-1", b"COUNT", b"2"], 2);
    assert_eq!(payload, b"*2\r\n:3\r\n:1\r\n");
    assert_eq!(result1, 2);

    // 命中数不足 count：数组头按实际命中收缩
    let (payload, result1) = lpos(&mut obj, &[b"c", b"RANK", b"2", b"COUNT", b"5"], 2);
    assert_eq!(payload, b"*1\r\n:3\r\n");
    assert_eq!(result1, 1);

    // maxlen 截断（正向限前 3 项 / 反向限后 2 项）
    let (payload, result1) = lpos(&mut obj, &[b"c", b"COUNT", b"0", b"MAXLEN", b"3"], 2);
    assert_eq!(payload, b"*1\r\n:1\r\n");
    assert_eq!(result1, 1);
    let (payload, result1) = lpos(
      &mut obj,
      &[b"c", b"RANK", b"-1", b"COUNT", b"0", b"MAXLEN", b"2"],
      2,
    );
    assert_eq!(payload, b"*1\r\n:3\r\n");
    assert_eq!(result1, 1);

    // 混合大小写词元组合（Rank + Count + MaxLen）
    let (payload, result1) = lpos(
      &mut obj,
      &[b"c", b"Rank", b"-1", b"cOuNt", b"0", b"MaxLen", b"2"],
      2,
    );
    assert_eq!(payload, b"*1\r\n:3\r\n");
    assert_eq!(result1, 1);

    // 未命中出口：空数组（RESP2/RESP3 同形，C# WriteEmptyArray）
    for version in [2u8, 3u8] {
      let (payload, result1) = lpos(&mut obj, &[b"nx", b"COUNT", b"3"], version);
      assert_eq!(payload, b"*0\r\n");
      assert_eq!(result1, 0);
    }

    // 显式 COUNT 1：与缺省形态计数同、应答仍为单元素数组
    let (payload, result1) = lpos(&mut obj, &[b"c", b"COUNT", b"1"], 2);
    assert_eq!(payload, b"*1\r\n:1\r\n");
    assert_eq!(result1, 1);
  }
}
