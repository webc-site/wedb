//! 哈希 RESP 语义操作（对标 libs/server/Objects/Hash/HashObjectImpl.cs，
//! C# 为 HashObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。

use std::sync::Arc;

use itoa::Buffer;
use wbase::{
  heap::round_up_ptr,
  num::{strict_f64, try_parse},
  time::now_ticks,
};
use wresp::{
  cmd_strings::{
    RESP_ERR_GENERIC_NAN_INFINITY, RESP_ERR_GENERIC_NAN_INFINITY_INCR,
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_HASH_VALUE_IS_NOT_FLOAT,
    RESP_ERR_HASH_VALUE_IS_NOT_INTEGER, RESP_ERR_NOT_VALID_FLOAT,
  },
  ext::is_resp3,
  options::ExpirationWithOption,
  resp_memory_writer::{RespWriter, format_double as wresp_format_double},
};
use zmij::Buffer as ZmijBuffer;

use super::hash_object::{EXPIRY_FLOOR, HashObject, HashOperation};
use crate::{
  resp::output::{write_map_length, write_null},
  types::{
    ObjectOutput, format_member_ttl, pick_k_random_indexes, pick_random_index, scan_operate_shared,
  },
};

/// HINCRBY 族解析判据段：入参增量与存量旧值两态（错误文案不同）
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum HashIncrStage {
  /// 入参增量
  Incr,
  /// 存量旧值
  Stock,
}

/// HINCRBY 族整数侧单点判据（信封态 `hash_increment` 与 wnode 分层臂共用，杜绝
/// 两层口径分叉）：基座为 C# HashObjectImpl.cs HashIncrement 两调用点共用的
/// NumUtils.TryParse(long) 语义对位 = wbase::num::try_parse（FromStr 整体消费，
/// 接受前导零与 `+` 号——"007"/"+7" 为合法增量与可累加存量；拒空串、前后空白、
/// 尾随垃圾与溢出）。与 INCR/INCRBY 族的档位分界：INCR 族实参在 C# 走 TryGetLong
/// → ParseUtils.cs TryReadLong(allowLeadingZeros:false)，对位 wbase::num::strict_i64
/// （"007" 拒绝），两族锚点不同、勿互相顺手改。错误文案分态：增量失败回
/// RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER、存量非数字回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER
#[inline]
pub fn parse_hash_incr(raw: &[u8], stage: HashIncrStage) -> Result<i64, &'static str> {
  let mut value = 0i64;
  if try_parse(raw, &mut value) {
    return Ok(value);
  }
  Err(match stage {
    HashIncrStage::Incr => RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
    HashIncrStage::Stock => RESP_ERR_HASH_VALUE_IS_NOT_INTEGER,
  })
}

/// HINCRBYFLOAT 族浮点侧单点判据（信封态 `hash_increment_float` 与 wnode 分层臂
/// 共用）：基座为 wbase::num::strict_f64(raw, true)，对位 C# 增量侧 TryGetDouble
/// (canBeInfinite:true) 与存量侧 TryParseWithInfinity 共用的「Utf8Parser ∨
/// RespReadUtils.cs TryReadInfinity 词形白名单」口径——"inf"/"+inf"/"-inf" 可解析、
/// 连同纯数值溢出的 ±inf 一律落无穷门；两层对增量与存量同判据点。错误文案四态：
/// 增量不可解析回 RESP_ERR_NOT_VALID_FLOAT、增量落无穷门回
/// RESP_ERR_GENERIC_NAN_INFINITY、存量非浮点回 RESP_ERR_HASH_VALUE_IS_NOT_FLOAT、
/// 存量落无穷门回 RESP_ERR_GENERIC_NAN_INFINITY_INCR。"nan" 词形两层恒拒，对标 C# HashCommands.cs 与 RespReadUtils.cs 语义
#[inline]
pub fn parse_hash_incr_float(raw: &[u8], stage: HashIncrStage) -> Result<f64, &'static str> {
  match strict_f64(raw, true) {
    Some(value) if value.is_infinite() => Err(match stage {
      HashIncrStage::Incr => RESP_ERR_GENERIC_NAN_INFINITY,
      HashIncrStage::Stock => RESP_ERR_GENERIC_NAN_INFINITY_INCR,
    }),
    Some(value) => Ok(value),
    None => Err(match stage {
      HashIncrStage::Incr => RESP_ERR_NOT_VALID_FLOAT,
      HashIncrStage::Stock => RESP_ERR_HASH_VALUE_IS_NOT_FLOAT,
    }),
  }
}

impl HashObject {
  /// HGET：单字段取值
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashGet
  pub(crate) fn hash_get(
    &mut self,
    args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    let key = args[0];
    match self.try_get_value(key) {
      Some(hash_value) => RespWriter::new_ref(output.payload).write_bulk_string(hash_value),
      None => write_null(output, resp_protocol_version),
    }

    output.result1 = 1;
  }

  /// HMGET：多字段取值
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashMultipleGet
  pub(crate) fn hash_multiple_get(
    &mut self,
    args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    RespWriter::new_ref(output.payload).write_array_length(args.len());

    for &key in args {
      match self.try_get_value(key) {
        Some(hash_value) => RespWriter::new_ref(output.payload).write_bulk_string(hash_value),
        None => write_null(output, resp_protocol_version),
      }
    }

    output.result1 = args.len() as i64;
  }

  /// HGETALL：全量字段值对（RESP2 扁平数组 / RESP3 map）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashGetAll
  pub(crate) fn hash_get_all(&mut self, output: &mut ObjectOutput<'_>, resp_protocol_version: u8) {
    let count = self.purge_expired_len();
    write_map_length(output, count, resp_protocol_version);

    // purge 已物理摘除全部 expiry<采样时刻 项，主容器余下项在声明时刻均存活；
    // 迭代臂严禁重采样过滤——重采样会在「purge 后至写出前」窗口把恰到期字段
    // 计入头部声明却跳过写出，声明数大于实际项数即 RESP 流永久错位
    //（C# WriteMapLength(Count()) + foreach IsExpired 双采样同窗，属原型缺陷，
    // 修复型偏离见 doc/zh/deviations.md）
    for (key, value) in self.hash.iter() {
      RespWriter::new_ref(output.payload).write_bulk_string(key);
      RespWriter::new_ref(output.payload).write_bulk_string(value);
    }

    // result1 回填存活字段数（C# HashGetAll 不设 result1，此处对齐同文件
    // hash_get_keys_or_values 的 written 口径补齐同族读臂计数）
    output.result1 = count as i64;
  }

  /// HDEL：批量删除字段
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashDelete
  pub(crate) fn hash_delete(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    let mut removed = 0_i64;

    for &key in args {
      if self.remove(key).is_some() {
        removed += 1;
      }
    }

    output.result1 = removed;
  }

  /// HSTRLEN：字段值长度
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashStrLength
  pub(crate) fn hash_str_length(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    let key = args[0];
    output.result1 = match self.try_get_value(key) {
      Some(hash_value) => hash_value.len() as i64,
      None => 0,
    };
  }

  /// HEXISTS：字段存在性
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashExists
  pub(crate) fn hash_exists(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    let field = args[0];
    output.result1 = i64::from(self.contains_key(field));
  }

  /// HRANDFIELD：随机字段（arg1 打包 count/withValues/includedCount，arg2 为种子）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashRandomField
  pub(crate) fn hash_random_field(
    &mut self,
    _args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    // HRANDFIELD key [count [WITHVALUES]]
    let mut count_parameter = (arg1 >> 2) as i64;
    let with_values = (arg1 & 1) == 1;
    let included_count = ((arg1 >> 1) & 1) == 1;
    let seed = arg2;

    let mut count_done = 0_i64;

    if included_count {
      let count = self.purge_expired_len();

      if count == 0 {
        // This can happen because of expiration but RMW operation haven't applied yet
        RespWriter::new_ref(output.payload).write_empty_array();
        output.result1 = 0;
        return;
      }

      if count_parameter > 0 && count_parameter > count as i64 {
        count_parameter = count as i64;
      }

      let index_count = count_parameter.unsigned_abs() as usize;

      // Write the size of the array reply
      RespWriter::new_ref(output.payload).write_array_length(
        if with_values && resp_protocol_version == 2 {
          index_count * 2
        } else {
          index_count
        },
      );

      // 下标流式 sink 直写应答：负 count 的 |k| 与基数脱钩，放回臂零存储
      //（C# new int[indexCount] 为连接级 OOM 面，预分配即 GB 级单命令分配）
      pick_k_random_indexes(count, index_count, seed, count_parameter > 0, |index| {
        let Some((key, value)) = self.element_at(index) else {
          return;
        };

        if is_resp3(resp_protocol_version) && with_values {
          RespWriter::new_ref(output.payload).write_array_length(2);
        }

        RespWriter::new_ref(output.payload).write_bulk_string(key);

        if with_values {
          RespWriter::new_ref(output.payload).write_bulk_string(value);
        }

        count_done += 1;
      });
    } else {
      // No count parameter is present, we just return a random field
      let count = self.purge_expired_len();
      if count == 0 {
        // This can happen because of expiration but RMW operation haven't applied yet
        write_null(output, resp_protocol_version);
        output.result1 = 0;
        return;
      }

      let index = pick_random_index(count, seed);
      if let Some((key, _)) = self.element_at(index) {
        RespWriter::new_ref(output.payload).write_bulk_string(key);
      }
      count_done = 1;
    }

    output.result1 = count_done;
  }

  /// HSET / HMSET / HSETNX：批量设置字段
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashSet
  pub(crate) fn hash_set(&mut self, sub_id: u8, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    self.delete_expired_items();

    let mut set = 0_i64;

    let hash_op = sub_id;
    for chunk in args.as_chunks::<2>().0 {
      let key = chunk[0];
      let value = chunk[1];

      // 到期守卫窄窗：delete_expired_items 与字段判定各自采样 now_ticks，跨
      // tick 恰到期字段漏摘；对位 C# HashSet 第一臂 `!exists || IsExpired`——
      // 到期字段视同缺席，先摘（remove 含过期账清理与记账，不复刻 C# 第一臂
      // 残留 expirationTimes 的缺陷）再按缺席臂插入并计数，同文件
      // hash_increment / hash_increment_float 同口径
      if self.is_expired(key) {
        self.remove(key);
      }

      if matches!(
        HashOperation::from_repr(hash_op),
        Some(HashOperation::Hset) | Some(HashOperation::Hmset)
      ) {
        match self.hash.get_mut(key) {
          None => {
            self.hash.insert(Arc::from(key.to_vec()), value.to_vec());
            self.update_size(key, value, true);
            set += 1;
          }
          Some(old_val) => {
            Self::replace_value_slice(&mut self.heap_memory_size, old_val, value);

            // To persist the key, if it has an expiration（摘旧过期项 + 退账 + 空回收）
            self
              .ledger
              .remove_expiration(&mut self.heap_memory_size, EXPIRY_FLOOR, key);
          }
        }
      } else if !self.contains_key(key) {
        // HSETNX 判定走宿主 contains_key（含到期过滤）：到期字段视同缺席，
        // 走插入臂回 1 并写入新值
        self.hash.insert(Arc::from(key.to_vec()), value.to_vec());
        self.update_size(key, value, true);
        set += 1;
      }
    }

    output.result1 = set;
  }

  /// HCOLLECT：占位收集操作（清除过期后确认存活）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashCollect
  pub fn hash_collect(&mut self, output: &mut ObjectOutput<'_>) {
    self.delete_expired_items();
    output.result1 = 1;
  }

  /// HKEYS / HVALS：全量字段或全量值
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashGetKeysOrValues
  pub(crate) fn hash_get_keys_or_values(
    &mut self,
    sub_id: u8,
    _args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
  ) {
    let count = self.purge_expired_len();
    let Some(op) = HashOperation::from_repr(sub_id) else {
      return;
    };

    RespWriter::new_ref(output.payload).write_array_length(count);

    // purge 已物理摘除全部 expiry<采样时刻 项，与 hash_get_all 同窗同理，
    // 迭代臂严禁重采样过滤（声明数恒等写出数，deviations 登记条同源）
    let mut written = 0_i64;

    for (key, value) in self.hash.iter() {
      if op == HashOperation::Hkeys {
        RespWriter::new_ref(output.payload).write_bulk_string(key);
      } else {
        RespWriter::new_ref(output.payload).write_bulk_string(value);
      }

      written += 1;
    }

    output.result1 = written;
  }

  /// HINCRBY：整型增量（值存原始文本，读回再解析）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrement
  pub(crate) fn hash_increment(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    // 部分执行哨兵（对位 C# result1 = int.MinValue，命令臂尾回填真值）
    output.result1 = i32::MIN as i64;

    let key = args[0];
    let incr_slice = args[1];

    let incr = match parse_hash_incr(incr_slice, HashIncrStage::Incr) {
      Ok(v) => v,
      Err(msg) => {
        RespWriter::new_ref(output.payload).write_error(msg);
        return;
      }
    };

    self.delete_expired_items();
    // 到期守卫窄窗：delete_expired_items 与 is_expired 各自采样 now_ticks，
    // 跨 tick 时恰到期成员漏摘，此处对位 C# HashIncrement IsExpired 臂视同
    // 不存在——remove 后落新插臂（存原文/回原文），size 由 remove 出账归位
    if self.is_expired(key) {
      self.remove(key);
    }

    if let Some(val) = self.hash.get_mut(key) {
      let result = match parse_hash_incr(val, HashIncrStage::Stock) {
        Ok(v) => v,
        Err(msg) => {
          RespWriter::new_ref(output.payload).write_error(msg);
          return;
        }
      };

      let result = result.wrapping_add(incr);
      let mut buf = Buffer::new();
      let formatted_value = buf.format(result).as_bytes();
      Self::replace_value_slice(&mut self.heap_memory_size, val, formatted_value);

      RespWriter::new_ref(output.payload).write_integer_from_bytes(val);
    } else {
      self.add(key, incr_slice.to_vec());
      RespWriter::new_ref(output.payload).write_integer_from_bytes(incr_slice);
    }

    output.result1 = 1;
  }

  /// HINCRBYFLOAT：浮点增量（值存最短往返文本）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrementFloat
  pub(crate) fn hash_increment_float(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    // 部分执行哨兵（对位 C# result1 = int.MinValue，命令臂尾回填真值）
    output.result1 = i32::MIN as i64;

    let key = args[0];
    let incr_slice = args[1];

    let incr = match parse_hash_incr_float(incr_slice, HashIncrStage::Incr) {
      Ok(v) => v,
      Err(msg) => {
        RespWriter::new_ref(output.payload).write_error(msg);
        return;
      }
    };

    self.delete_expired_items();
    // 到期守卫窄窗：与 hash_increment 同一窗口成因与视同不存在语义（对位
    // C# HashIncrementFloat IsExpired 臂）
    if self.is_expired(key) {
      self.remove(key);
    }

    if let Some(val) = self.hash.get_mut(key) {
      let result = match parse_hash_incr_float(val, HashIncrStage::Stock) {
        Ok(v) => v,
        Err(msg) => {
          RespWriter::new_ref(output.payload).write_error(msg);
          return;
        }
      };

      // 求和不设结果门（deviations §1 尾回指注／§80 同族第二消费位）：和逾 DBL_MAX 恒经
      // format_double 单源落 "inf"/"-inf" 三字节刻形，系在册刻意形、禁按 C# TryFormat G
      // "Infinity" 词形回改，亦禁补门（真 Redis would-produce 拒改形不复刻）；无穷存量门
      // 在上游 parse_hash_incr_float(Stock) 既有档，此处复检即与分层臂及在册锁面发散
      let result = result + incr;
      let mut buf = ZmijBuffer::new();
      let formatted_value = wresp_format_double(result, &mut buf).as_bytes();
      Self::replace_value_slice(&mut self.heap_memory_size, val, formatted_value);

      RespWriter::new_ref(output.payload).write_bulk_string(val);
    } else {
      self.add(key, incr_slice.to_vec());
      RespWriter::new_ref(output.payload).write_bulk_string(incr_slice);
    }

    output.result1 = 1;
  }

  /// HEXPIRE：批量设置成员过期（arg1/arg2 为 ExpirationWithOption 压缩字的高低半部）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashExpire
  pub(crate) fn hash_expire(
    &mut self,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
  ) {
    self.delete_expired_items();

    let expiration_with_option = ExpirationWithOption::from_word_head_tail(arg1, arg2);

    RespWriter::new_ref(output.payload).write_array_length(args.len());

    for &field in args {
      let result = self.set_expiration(
        field,
        expiration_with_option.expiration_time_in_ticks(),
        expiration_with_option.expire_option(),
      );
      RespWriter::new_ref(output.payload).write_int64(i64::from(result as i32));
    }

    output.result1 = args.len() as i64;
  }

  /// HTTL / HEXPIRETIME（arg1 = 毫秒标记，arg2 = 时间戳标记）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashTimeToLive
  pub(crate) fn hash_time_to_live(
    &mut self,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
  ) {
    self.delete_expired_items();

    let is_milliseconds = arg1 == 1;
    let is_timestamp = arg2 == 1;
    let num_fields = args.len();

    RespWriter::new_ref(output.payload).write_array_length(num_fields);

    let now = now_ticks();
    for &field in args {
      let result = format_member_ttl(
        self.get_expiration(field),
        is_timestamp,
        is_milliseconds,
        now,
      );
      RespWriter::new_ref(output.payload).write_int64(result);
    }

    output.result1 = num_fields as i64;
  }

  /// HPERSIST：批量清除成员过期
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashPersist
  pub(crate) fn hash_persist(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    self.delete_expired_items();

    let num_fields = args.len();

    RespWriter::new_ref(output.payload).write_array_length(num_fields);

    for &field in args {
      let result = self.persist(field);
      RespWriter::new_ref(output.payload).write_int64(i64::from(result));
    }

    output.result1 = num_fields as i64;
  }

  /// 就地覆写字段值并调整记账（同长复用槽位不调整，异长按 RoundUp 差额调整）
  ///
  /// 对应 C# HashSet / HashIncrement 的 formattedValue.Length == hashValueRef.Length 分支合并形态
  #[inline]
  fn replace_value_slice(heap_memory_size: &mut i64, val: &mut Vec<u8>, new_value: &[u8]) {
    // i64 差额：新值 round_up_ptr 短于旧值时 usize 减法会下溢
    *heap_memory_size += round_up_ptr(new_value.len()) as i64 - round_up_ptr(val.len()) as i64;
    if val.len() == new_value.len() {
      val.copy_from_slice(new_value);
    } else {
      val.clear();
      val.extend_from_slice(new_value);
    }
  }

  /// HSCAN 的对象层入口，转发至 [`scan_operate_shared`]（总量即 hash.len()，
  /// 供游标/条目帧位预留估宽）。
  pub(crate) fn scan_operate(&mut self, args: &[&[u8]], limit: i32, output: &mut ObjectOutput<'_>) {
    scan_operate_shared(
      args,
      limit,
      output,
      self.hash.len(),
      |cursor, count, pattern, is_no_value, sink| {
        self.scan(cursor, count, pattern, is_no_value, |key, value| {
          sink(key);
          if is_no_value {
            1
          } else {
            sink(value);
            2
          }
        })
      },
    );
  }
}
