//! 集合 RESP 语义操作（对标 libs/server/Objects/Set/SetObjectImpl.cs，
//! C# 为 SetObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。

use crate::{
  inputs::ObjectInput,
  objects::{
    hash::hash_object::{pick_k_random_indexes, pick_random_index},
    set::set_object::SetObject,
    types::object_output::ObjectOutput,
  },
};

/// 取第 i 个参数字节
///
/// libs/server/Resp/Parser/SessionParseState.cs:GetArgSliceByRef
#[inline]
fn arg<'a>(input: &ObjectInput, i: usize) -> &'a [u8] {
  input.parse_state.get_arg_slice_by_ref(i).as_slice()
}

/// SPOP 无 count 形态标记（C# ObjectInput.arg1 缺省值）
///
/// libs/server/Resp/Objects/SetCommands.cs:SetPop（countParameter = int.MinValue）
pub(crate) const NO_COUNT: i32 = i32::MIN;

impl SetObject {
  /// SADD：批量添加成员
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetAdd
  pub(crate) fn set_add(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    let mut added = 0_i64;

    for i in 0..input.parse_state.count {
      let member = arg(input, i);
      if self.set.insert(member.to_vec()) {
        added += 1;
        self.update_size(member, true);
      }
    }

    output.result1 = added;
  }

  /// SMEMBERS：全量成员
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetMembers
  pub(crate) fn set_members(&mut self, output: &mut ObjectOutput, resp_protocol_version: u8) {
    write_set_length(output, self.set.len(), resp_protocol_version);

    let mut written = 0_i64;

    for item in &self.set {
      output.write_bulk_string(item);
      written += 1;
    }

    output.result1 = written;
  }

  /// SISMEMBER：单成员存在性
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetIsMember
  pub(crate) fn set_is_member(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    let member = arg(input, 0);
    let is_member = self.set.contains(member);
    output.write_int64(i64::from(is_member));
    output.result1 = 1;
  }

  /// SMISMEMBER：多成员存在性
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetMultiIsMember
  pub(crate) fn set_multi_is_member(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    output.write_array_length(input.parse_state.count);

    for i in 0..input.parse_state.count {
      let member = arg(input, i);
      output.write_int64(i64::from(self.set.contains(member)));
    }

    output.result1 = input.parse_state.count as i64;
  }

  /// SREM：批量移除成员
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetRemove
  pub(crate) fn set_remove(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    let mut removed = 0_i64;

    for i in 0..input.parse_state.count {
      let member = arg(input, i);
      if self.set.remove(member) {
        removed += 1;
        self.update_size(member, false);
      }
    }

    output.result1 = removed;
  }

  /// SCARD：基数
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetLength
  pub(crate) fn set_length(&mut self, output: &mut ObjectOutput) {
    output.result1 = self.set.len() as i64;
  }

  /// SPOP：随机弹出（count >= 1 批量；NO_COUNT 单枚）
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetPop
  ///
  /// 刻意差异（对照 C#）：C# SPOP 经 RandomNumberGenerator（密码学随机）取下标，
  /// Rust 以 fastrand 非种子化抽取等价表达
  pub(crate) fn set_pop(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    // SPOP key [count]
    let count = input.arg1;
    let mut count_done = 0_i64;

    // key [count]
    if count >= 1 {
      // POP this number of random fields
      let count_parameter = (count as usize).min(self.set.len());
      let mut rng = fastrand::Rng::new();

      // Write the size of the array reply
      write_set_length(output, count_parameter, resp_protocol_version);

      for _ in 0..count_parameter {
        // Generate a new index based on the elements left in the set
        if self.set.is_empty() {
          break;
        }
        let index = rng.usize(..self.set.len());
        let Some(item) = self.set.iter().nth(index).cloned() else {
          break;
        };
        self.set.remove(&item);
        self.update_size(&item, false);
        output.write_bulk_string(&item);
        count_done += 1;
      }

      // C#: countDone += count - countDone → result1 恒为 count
      count_done += i64::from(count) - count_done;
    } else if count == NO_COUNT {
      // no count parameter is present, we just pop and return a random item of the set
      if !self.set.is_empty() {
        let item = self.set.iter().next().cloned().unwrap();
        self.set.remove(&item);
        self.update_size(&item, false);
        output.write_bulk_string(&item);
      } else {
        // If set empty return nil
        output.write_null(resp_protocol_version);
      }
      count_done += 1;
    }

    output.result1 = count_done;
  }

  /// SRANDMEMBER：随机取样（不弹出；正数去重 / 负数可重复 / NO_COUNT 单枚）
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetRandomMember
  pub(crate) fn set_random_member(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    let count = input.arg1;
    let seed = input.arg2;

    let mut count_done = 0_i64;

    if count > 0 {
      // Return an array of distinct elements
      let count_parameter = (count as usize).min(self.set.len());

      // The order of fields in the reply is not truly random
      let indexes = pick_k_random_indexes(self.set.len(), count_parameter, seed, true);

      // Write the size of the array reply
      write_set_length(output, count_parameter, resp_protocol_version);

      for index in indexes {
        let Some(element) = self.set.iter().nth(index).cloned() else {
          continue;
        };
        output.write_bulk_string(&element);
        count_done += 1;
      }
      count_done += i64::from(count) - count_parameter as i64;
    } else if count == NO_COUNT {
      // Return a single random element from the set
      if !self.set.is_empty() {
        let index = pick_random_index(self.set.len(), seed);
        if let Some(item) = self.set.iter().nth(index).cloned() {
          output.write_bulk_string(&item);
        }
      } else {
        // If set is empty, return nil
        output.write_null(resp_protocol_version);
      }
      count_done += 1;
    } else {
      // count < 0: Return an array with potentially duplicate elements
      let count_parameter = count.unsigned_abs() as usize;

      let indexes = pick_k_random_indexes(self.set.len(), count_parameter, seed, false);

      if !self.set.is_empty() {
        // Write the size of the array reply
        output.write_array_length(count_parameter);

        for index in indexes {
          let Some(element) = self.set.iter().nth(index).cloned() else {
            continue;
          };
          output.write_bulk_string(&element);
          count_done += 1;
        }
      } else {
        // If set is empty, return nil（C# 空集上 Random.Next(0) 抛异常，按 nil 处理）
        output.write_null(resp_protocol_version);
      }
    }

    output.result1 = count_done;
  }
}

/// RESP2 口径写 set 头（RESP3 `~n`，RESP2 退化为等长数组）
///
/// libs/common/RespMemoryWriter.cs:WriteSetLength
fn write_set_length(output: &mut ObjectOutput, len: usize, resp_protocol_version: u8) {
  if resp_protocol_version >= 3 {
    output.payload.push(b'~');
    let mut buf = itoa::Buffer::new();
    output.payload.extend_from_slice(buf.format(len).as_bytes());
    output.payload.extend_from_slice(b"\r\n");
  } else {
    output.write_array_length(len);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    arg_slice::ArgSlice,
    input_header::RespInputHeader,
    objects::set::set_object::SetOperation,
    session_parse_state::SessionParseState,
    types::{GarnetObjectType, RespInputFlags},
  };

  /// 构造 ObjectInput（backing 须与 input 同生命周期存活）
  fn make_input(
    op: SetOperation,
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

    let mut header = RespInputHeader::new_with_type(GarnetObjectType::Set, RespInputFlags::empty());
    header.set_sub_id(op as u8);
    (
      ObjectInput::new_with_state(header, &mut parse_state, arg1, arg2),
      backing,
    )
  }

  fn obj_with(members: &[&str]) -> SetObject {
    let mut obj = SetObject::new();
    for m in members {
      obj.set.insert(m.as_bytes().to_vec());
    }
    obj
  }

  /// SADD / SREM / SCARD / SISMEMBER / SMISMEMBER / SMEMBERS
  #[test]
  fn membership_ops() {
    let mut obj = SetObject::new();

    // SADD a b a：a 重复只计一次
    let (input, _b) = make_input(SetOperation::Sadd, &[b"a", b"b", b"a"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.set_add(&input, &mut out);
    assert_eq!(out.result1, 2);

    // SISMEMBER
    let (input, _b) = make_input(SetOperation::Sismember, &[b"a"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.set_is_member(&input, &mut out, 2);
    assert_eq!(out.payload, b":1\r\n");
    assert_eq!(out.result1, 1);
    let (input, _b) = make_input(SetOperation::Sismember, &[b"nx"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.set_is_member(&input, &mut out, 2);
    assert_eq!(out.payload, b":0\r\n");

    // SMISMEMBER a nx b
    let (input, _b) = make_input(SetOperation::Smismember, &[b"a", b"nx", b"b"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.set_multi_is_member(&input, &mut out, 2);
    assert_eq!(out.payload, b"*3\r\n:1\r\n:0\r\n:1\r\n");
    assert_eq!(out.result1, 3);

    // SMEMBERS：RESP2 数组形态
    let (_input, _b) = make_input(SetOperation::Smembers, &[], 0, 0);
    let mut out = ObjectOutput::new();
    obj.set_members(&mut out, 2);
    assert!(out.payload.starts_with(b"*2\r\n"));
    assert_eq!(out.result1, 2);

    // SCARD
    let (_input, _b) = make_input(SetOperation::Scard, &[], 0, 0);
    let mut out = ObjectOutput::new();
    obj.set_length(&mut out);
    assert_eq!(out.result1, 2);

    // SREM a nx
    let (input, _b) = make_input(SetOperation::Srem, &[b"a", b"nx"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.set_remove(&input, &mut out);
    assert_eq!(out.result1, 1);
    assert_eq!(obj.set.len(), 1);
  }

  /// SPOP：批量 / 单枚 / 空集
  #[test]
  fn pop_forms() {
    let mut obj = obj_with(&["a", "b", "c"]);

    // SPOP 2：数组头 + 两枚，result1 = count（C# countDone += count - countDone）
    let (input, _b) = make_input(SetOperation::Spop, &[], 2, 0);
    let mut out = ObjectOutput::new();
    obj.set_pop(&input, &mut out, 2);
    assert!(
      out.payload.starts_with(b"*2\r\n"),
      "{}",
      String::from_utf8_lossy(&out.payload)
    );
    assert_eq!(out.result1, 2);
    assert_eq!(obj.set.len(), 1);

    // 弹空
    let (input, _b) = make_input(SetOperation::Spop, &[], 5, 0);
    let mut out = ObjectOutput::new();
    obj.set_pop(&input, &mut out, 2);
    assert!(out.payload.starts_with(b"*1\r\n"));
    assert_eq!(out.result1, 5); // C# result1 = count 形态
    assert!(obj.set.is_empty());

    // 无 count 形态（NO_COUNT）：空集 → nil，result1 = 1
    let (input, _b) = make_input(SetOperation::Spop, &[], NO_COUNT, 0);
    let mut out = ObjectOutput::new();
    obj.set_pop(&input, &mut out, 2);
    assert_eq!(out.payload, b"$-1\r\n");
    assert_eq!(out.result1, 1);

    // 无 count 形态：单 bulk 弹出
    let mut obj = obj_with(&["x"]);
    let (input, _b) = make_input(SetOperation::Spop, &[], NO_COUNT, 0);
    let mut out = ObjectOutput::new();
    obj.set_pop(&input, &mut out, 2);
    assert_eq!(out.payload, b"$1\r\nx\r\n");
    assert!(obj.set.is_empty());
  }

  /// SRANDMEMBER：正/负/NO_COUNT 三形态（不弹出）
  #[test]
  fn random_member_forms() {
    let mut obj = obj_with(&["a", "b", "c", "d"]);

    // count > 0：去重取样（arg1=2）
    let (input, _b) = make_input(SetOperation::Srandmember, &[], 2, 42);
    let mut out = ObjectOutput::new();
    obj.set_random_member(&input, &mut out, 2);
    assert!(out.payload.starts_with(b"*2\r\n"));
    assert_eq!(out.result1, 2); // countDone += count - countParameter → = count
    assert_eq!(obj.set.len(), 4); // 不弹出

    // count 超集：钳制到 4，result1 = 6（count）
    let (input, _b) = make_input(SetOperation::Srandmember, &[], 6, 42);
    let mut out = ObjectOutput::new();
    obj.set_random_member(&input, &mut out, 2);
    assert!(out.payload.starts_with(b"*4\r\n"));
    assert_eq!(out.result1, 6);

    // count < 0：可重复，|count| 项
    let (input, _b) = make_input(SetOperation::Srandmember, &[], -3, 42);
    let mut out = ObjectOutput::new();
    obj.set_random_member(&input, &mut out, 2);
    assert!(out.payload.starts_with(b"*3\r\n"));
    assert_eq!(out.result1, 3);

    // NO_COUNT：单枚 bulk
    let (input, _b) = make_input(SetOperation::Srandmember, &[], NO_COUNT, 1);
    let mut out = ObjectOutput::new();
    obj.set_random_member(&input, &mut out, 2);
    assert!(out.payload.starts_with(b"$1\r\n"));
    assert_eq!(out.result1, 1);

    // NO_COUNT + 空集 → nil
    let mut empty = SetObject::new();
    let (input, _b) = make_input(SetOperation::Srandmember, &[], NO_COUNT, 1);
    let mut out = ObjectOutput::new();
    empty.set_random_member(&input, &mut out, 2);
    assert_eq!(out.payload, b"$-1\r\n");

    // count < 0 + 空集 → nil（C# 抛异常处按 nil 表达）
    let (input, _b) = make_input(SetOperation::Srandmember, &[], -3, 42);
    let mut out = ObjectOutput::new();
    empty.set_random_member(&input, &mut out, 2);
    assert_eq!(out.payload, b"$-1\r\n");
  }

  /// operate 分派冒烟：SADD 新建 → 删空 REMOVE_KEY；SMOVE 走缺省分支
  #[test]
  fn operate_dispatch_smoke() {
    let mut obj = SetObject::new();
    let (input, _b) = make_input(SetOperation::Sadd, &[b"m"], 0, 0);
    let mut out = ObjectOutput::new();
    assert!(obj.operate(&input, &mut out, 2));
    assert_eq!(out.result1, 1);

    let (input, _b) = make_input(SetOperation::Srem, &[b"m"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.operate(&input, &mut out, 2);
    assert!(out.has_remove_key());

    // SMOVE 不经对象层 operate（C# switch default 抛 GarnetException）
    let (input, _b) = make_input(SetOperation::Smove, &[b"s", b"d", b"m"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.operate(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR unsupported operation\r\n");

    // 类型不符 → WRONGTYPE
    let (input, _b) = make_input(SetOperation::Scard, &[], 0, 0);
    let mut input = input;
    input.header.data[0] = GarnetObjectType::List as u8;
    let mut out = ObjectOutput::new();
    obj.operate(&input, &mut out, 2);
    assert!(out.has_wrong_type());
  }

  /// SSCAN 经 operate 分派（RESP2 口径）
  #[test]
  fn sscan_flow() {
    let mut obj = obj_with(&["one", "two", "three"]);

    let (input, _b) = make_input(SetOperation::Sscan, &[b"0", b"MATCH", b"t*"], 0, 0);
    let mut out = ObjectOutput::new();
    assert!(obj.operate(&input, &mut out, 2));
    let payload = String::from_utf8_lossy(&out.payload);
    assert!(payload.starts_with("*2\r\n$1\r\n0\r\n"), "{payload}");
    assert!(payload.contains("$3\r\ntwo\r\n"), "{payload}");
    assert!(payload.contains("$5\r\nthree\r\n"), "{payload}");

    // 非法光标
    let (input, _b) = make_input(SetOperation::Sscan, &[b"-1"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.scan_operate(&input, &mut out);
    assert_eq!(out.payload, b"-ERR invalid cursor\r\n");
  }
}
