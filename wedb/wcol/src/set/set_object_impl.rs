//! 集合 RESP 语义操作（对标 libs/server/Objects/Set/SetObjectImpl.cs，
//! C# 为 SetObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。
//!
//! 在 garnet 中的相对路径: libs/server/Storage/Session/ObjectStore/SetObject.cs（Set 原语与随机成员）

use wresp::resp_memory_writer::RespWriter;

use super::set_object::SetObject;
use crate::{
  resp::output::{write_null, write_set_length},
  types::{ObjectOutput, pick_k_random_indexes, pick_random_index},
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
      if self.add(member) {
        added += 1;
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

      // Write the size of the array reply
      write_set_length(output, count_parameter, resp_protocol_version);

      // 采样域借用视图一次构建 + pick_k_random_indexes(distinct) 一次产出互异
      // 下标：消除 C# 形态「每弹一次 Set.ElementAt(index)」的 O(count·n) 线性
      // 扫描（compio thread-per-core 下单命令独占工作核）；不放回均匀采样与
      // 逐弹抽取同为均匀 k-子集，分布等价。k 已钳 min(count, n)，待删收集与
      // 对象本体同阶（与原逐弹 cloned 同量）
      let view: Vec<_> = self.set.iter().collect();
      let mut popped: Vec<Vec<u8>> = Vec::with_capacity(count_parameter);

      pick_k_random_indexes(
        self.set.len(),
        count_parameter,
        fastrand::i32(..),
        true,
        |index| {
          let Some(item) = view.get(index) else {
            return;
          };
          RespWriter::new_ref(output.payload).write_bulk_string(item);
          popped.push((*item).clone());
        },
      );

      // 释放视图后统一剔除（互异下标 → 互异成员，remove 全命中）
      for item in &popped {
        self.set.remove(item);
        self.update_size(item, false);
      }

      // C#: countDone += count - countDone → result1 恒为 count
      count_done = i64::from(count);
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
  ///
  /// 刻意差异（对照 C#）：原型 Set 侧正数 count 臂把钳制后的 countParameter（＝k）
  /// 当抽样域 n 传入 PickKRandomIndexes，n＝k 时返回下标恒为 0..k-1 的置换，
  /// 成员集合退化为集合前 k 个；此处以 self.set.len()（全集基数）为 n 采真全集
  /// 抽样，系对原型笔误的修正（Hash/ZSet 两面原型本就传全集），详见
  /// doc/zh/deviations.md 第 12 条，严禁回改
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
      // Write the size of the array reply
      write_set_length(output, count_parameter, resp_protocol_version);

      // 采样域借用视图一次构建（n 长度、与对象本体同阶，不随客户端 k 增长）：
      // 消除逐下标 iter().nth 的 O(index) 线性扫描（k 个下标合计 O(k·n)，
      // compio thread-per-core 下单命令独占工作核），sink 内 O(1) 直取；
      // 下标流式 sink 直写应答（头已声明，逐下标产出零存储）
      let view: Vec<_> = self.set.iter().collect();
      pick_k_random_indexes(self.set.len(), count_parameter, seed, true, |index| {
        let Some(element) = view.get(index) else {
          return;
        };
        RespWriter::new_ref(output.payload).write_bulk_string(element);
        count_done += 1;
      });
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

      if !self.set.is_empty() {
        // Write the size of the array reply
        RespWriter::new_ref(output.payload).write_array_length(count_parameter);

        // 采样域借用视图一次构建（同正 count 臂，消除逐下标 nth 的 O(k·n) 放大）；
        // 放回臂下标流式产出零存储：|count| 与集合基数脱钩，预分配即 GB 级
        // 单命令分配面（C# new int[countParameter] 为连接级 OOM），严禁回改
        let view: Vec<_> = self.set.iter().collect();
        pick_k_random_indexes(self.set.len(), count_parameter, seed, false, |index| {
          let Some(element) = view.get(index) else {
            return;
          };
          RespWriter::new_ref(output.payload).write_bulk_string(element);
          count_done += 1;
        });
      } else {
        // If set is empty, return nil（C# 空集上 Random.Next(0) 抛异常，按 nil 处理）
        write_null(output, resp_protocol_version);
      }
    }

    output.result1 = count_done;
  }
}
