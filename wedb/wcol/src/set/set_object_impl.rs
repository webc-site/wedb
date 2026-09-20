//! 集合 RESP 语义操作（对标 libs/server/Objects/Set/SetObjectImpl.cs，
//! C# 为 SetObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。

use fastrand::Rng;
use wresp::resp_memory_writer::RespWriter;

use super::set_object::SetObject;
use crate::{
  hash::hash_object::{pick_k_random_indexes, pick_random_index},
  resp::output::{write_null, write_set_length},
  types::ObjectOutput,
};

/// SPOP 无 count 形态标记（C# input.Arg1 缺省值）
///
/// libs/server/Resp/Objects/SetCommands.cs:SetPop（countParameter = int.MinValue）
pub const NO_COUNT: i32 = i32::MIN;

impl SetObject {
  /// SADD：批量添加成员
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetAdd
  pub(crate) fn set_add(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    let mut added = 0_i64;

    for &member in args {
      if !self.set.contains(member) {
        self.set.insert(member.to_vec());
        added += 1;
        self.update_size(member, true);
      }
    }

    output.result1 = added;
  }

  /// SMEMBERS：全量成员
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetMembers
  pub(crate) fn set_members(&mut self, output: &mut ObjectOutput<'_>, resp_protocol_version: u8) {
    write_set_length(output, self.set.len(), resp_protocol_version);

    let mut written = 0_i64;

    for item in &self.set {
      RespWriter::new_ref(output.payload).write_bulk_string(item);
      written += 1;
    }

    output.result1 = written;
  }

  /// SISMEMBER：单成员存在性
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetIsMember
  pub(crate) fn set_is_member(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    let member = args[0];
    let is_member = self.set.contains(member);
    RespWriter::new_ref(output.payload).write_int64(i64::from(is_member));
    output.result1 = 1;
  }

  /// SMISMEMBER：多成员存在性
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetMultiIsMember
  pub(crate) fn set_multi_is_member(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    RespWriter::new_ref(output.payload).write_array_length(args.len());

    for &member in args {
      RespWriter::new_ref(output.payload).write_int64(i64::from(self.set.contains(member)));
    }

    output.result1 = args.len() as i64;
  }

  /// SREM：批量移除成员
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetRemove
  pub(crate) fn set_remove(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    let mut removed = 0_i64;

    for &member in args {
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
  pub(crate) fn set_length(&mut self, output: &mut ObjectOutput<'_>) {
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
    _args: &[&[u8]],
    arg1: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    // SPOP key [count]
    let count = arg1;
    let mut count_done = 0_i64;

    // key [count]
    if count >= 1 {
      // POP this number of random fields
      let count_parameter = (count as usize).min(self.set.len());
      let mut rng = Rng::new();

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
        RespWriter::new_ref(output.payload).write_bulk_string(&item);
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
        if let Some(item) = self.set.iter().nth(index).cloned() {
          self.set.remove(&item);
          self.update_size(&item, false);
          RespWriter::new_ref(output.payload).write_bulk_string(&item);
          count_done += 1;
        } else {
          output.result1 = 0;
          return;
        }
      } else {
        // If set empty return nil
        write_null(output, resp_protocol_version);
        count_done += 1;
      }
    }

    output.result1 = count_done;
  }

  /// SRANDMEMBER：随机取样（不弹出；正数去重 / 负数可重复 / NO_COUNT 单枚）
  ///
  /// libs/server/Objects/Set/SetObjectImpl.cs:SetRandomMember
  pub(crate) fn set_random_member(
    &mut self,
    _args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    let count = arg1;
    let seed = arg2;

    let mut count_done = 0_i64;

    if count > 0 {
      // Return an array of distinct elements
      let count_parameter = (count as usize).min(self.set.len());

      // The order of fields in the reply is not truly random
      let indexes = pick_k_random_indexes(self.set.len(), count_parameter, seed, true);

      // Write the size of the array reply
      write_set_length(output, count_parameter, resp_protocol_version);

      for index in indexes {
        let Some(element) = self.set.iter().nth(index) else {
          continue;
        };
        RespWriter::new_ref(output.payload).write_bulk_string(element);
        count_done += 1;
      }
      count_done += i64::from(count) - count_parameter as i64;
    } else if count == NO_COUNT {
      // Return a single random element from the set
      if !self.set.is_empty() {
        let index = pick_random_index(self.set.len(), seed);
        if let Some(item) = self.set.iter().nth(index) {
          RespWriter::new_ref(output.payload).write_bulk_string(item);
        }
      } else {
        // If set is empty, return nil
        write_null(output, resp_protocol_version);
      }
      count_done += 1;
    } else {
      // count < 0: Return an array with potentially duplicate elements
      let count_parameter = count.unsigned_abs() as usize;

      let indexes = pick_k_random_indexes(self.set.len(), count_parameter, seed, false);

      if !self.set.is_empty() {
        // Write the size of the array reply
        RespWriter::new_ref(output.payload).write_array_length(count_parameter);

        for index in indexes {
          let Some(element) = self.set.iter().nth(index) else {
            continue;
          };
          RespWriter::new_ref(output.payload).write_bulk_string(element);
          count_done += 1;
        }
      } else {
        // If set is empty, return nil（C# 空集上 Random.Next(0) 抛异常，按 nil 处理）
        write_null(output, resp_protocol_version);
      }
    }

    output.result1 = count_done;
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_set_pop_no_count() {
    let mut set = SetObject::default();
    let mut payload = Vec::new();

    // 空集单枚弹出：写 null，result1 为 1
    {
      let mut output = ObjectOutput::mount(&mut payload);
      set.set_pop(&[], NO_COUNT, &mut output, 2);
      assert_eq!(output.result1, 1);
    }
    assert_eq!(payload, b"$-1\r\n");

    // 填充元素并同步更新内存记账
    set.set.insert(b"elem1".to_vec());
    set.update_size(b"elem1", true);
    set.set.insert(b"elem2".to_vec());
    set.update_size(b"elem2", true);

    // 弹出单枚
    payload.clear();
    {
      let mut output = ObjectOutput::mount(&mut payload);
      set.set_pop(&[], NO_COUNT, &mut output, 2);
      assert_eq!(output.result1, 1);
    }
    assert_eq!(set.set.len(), 1);

    // 再次弹出单枚
    payload.clear();
    {
      let mut output = ObjectOutput::mount(&mut payload);
      set.set_pop(&[], NO_COUNT, &mut output, 2);
      assert_eq!(output.result1, 1);
    }
    assert_eq!(set.set.len(), 0);

    // 弹空后再弹出：写 null，result1 为 1
    payload.clear();
    {
      let mut output = ObjectOutput::mount(&mut payload);
      set.set_pop(&[], NO_COUNT, &mut output, 2);
      assert_eq!(output.result1, 1);
    }
    assert_eq!(payload, b"$-1\r\n");
  }

  #[test]
  fn test_set_pop_with_count() {
    let mut set = SetObject::default();
    set.set.insert(b"a".to_vec());
    set.update_size(b"a", true);
    set.set.insert(b"b".to_vec());
    set.update_size(b"b", true);
    set.set.insert(b"c".to_vec());
    set.update_size(b"c", true);

    let mut payload = Vec::new();
    {
      let mut output = ObjectOutput::mount(&mut payload);
      set.set_pop(&[], 2, &mut output, 2);
      assert_eq!(output.result1, 2);
    }
    assert_eq!(set.set.len(), 1);

    payload.clear();
    {
      let mut output = ObjectOutput::mount(&mut payload);
      // count 超过当前剩余量
      set.set_pop(&[], 5, &mut output, 2);
      assert_eq!(output.result1, 5);
    }
    assert_eq!(set.set.len(), 0);
  }
}
