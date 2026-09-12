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
#[inline]
fn arg<'a>(input: &ObjectInput, i: usize) -> &'a [u8] {
  input.parse_state.get_arg_slice_by_ref(i).as_slice()
}

/// SPOP 无 count 形态标记（C# ObjectInput.arg1 缺省值）
///
/// libs/server/Resp/Objects/SetCommands.cs:SetPop（countParameter = int.MinValue）
pub const NO_COUNT: i32 = i32::MIN;

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
  pub(crate) fn set_is_member(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    let member = arg(input, 0);
    let is_member = self.set.contains(member);
    output.write_int64(i64::from(is_member));
    output.result1 = 1;
  }

  /// SMISMEMBER：多成员存在性
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetMultiIsMember
  pub(crate) fn set_multi_is_member(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
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
        // 随机下标取样（C# RandomNumberGenerator.GetInt32(0, Set.Count) 的
        // fastrand 非种子化等价形态）
        let index = fastrand::usize(..self.set.len());
        let item = self.set.iter().nth(index).cloned().unwrap();
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
// C# libs/common/RespMemoryWriter.cs:WriteSetLength
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
