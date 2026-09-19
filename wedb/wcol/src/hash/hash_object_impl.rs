//! 哈希 RESP 语义操作（对标 libs/server/Objects/Hash/HashObjectImpl.cs，
//! C# 为 HashObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。

use itoa::Buffer;
use wbase::{
  convert::{
    milliseconds_from_diff_utc_now_ticks, seconds_from_diff_utc_now_ticks,
    unix_time_in_milliseconds_from_ticks, unix_time_in_seconds_from_ticks,
  },
  heap::{SLOT, round_up_ptr},
  num::{try_parse_f64, try_parse_i64},
};
use wresp::{
  cmd_strings::{
    RESP_ERR_GENERIC_NAN_INFINITY, RESP_ERR_GENERIC_NAN_INFINITY_INCR,
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_HASH_VALUE_IS_NOT_FLOAT,
    RESP_ERR_HASH_VALUE_IS_NOT_INTEGER, RESP_ERR_NOT_VALID_FLOAT,
  },
  options::ExpirationWithOption,
  resp_memory_writer::{RespWriter, format_double as wresp_format_double},
};
use zmij::Buffer as ZmijBuffer;

use super::hash_object::{
  HashObject, HashOperation, pick_k_random_indexes, pick_random_index, scan_operate_shared,
};
use crate::{
  parse_utils::try_parse_with_infinity,
  resp::output::{write_map_length, write_null},
  types::ObjectOutput,
};

/// 取第 i 个参数字节切片（对齐 C# GetByteSpanFromInput 的固定入参形态）
///
/// libs/server/Objects/Hash/HashObjectImpl.cs:GetByteSpanFromInput
#[inline]
fn get_byte_span_from_input<'a>(args: &[&'a [u8]], index: usize) -> &'a [u8] {
  args[index]
}

/// 解析 i64（调用 `wbase::num::try_parse_i64`）
fn num_utils_try_parse_long(v: &[u8]) -> Option<i64> {
  let mut value = 0;
  try_parse_i64(v, &mut value).then_some(value)
}

/// 解析 f64（调用 `wbase::num::try_parse_f64`）
fn num_utils_try_parse_double(v: &[u8]) -> Option<f64> {
  let mut value = 0.0;
  try_parse_f64(v, &mut value).then_some(value)
}

/// 最短往返双精度文本（对标 double.TryFormat 默认 G 形态）。格式化单点在
/// wresp::resp_memory_writer::format_double，此处直取字节视图，避免经 String 中转的堆分配
#[inline]
fn format_double(value: f64) -> Vec<u8> {
  let mut buf = ZmijBuffer::new();
  wresp_format_double(value, &mut buf).as_bytes().to_vec()
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
    let key = get_byte_span_from_input(args, 0);
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
    write_map_length(output, self.purge_expired_len(), resp_protocol_version);

    let is_expirable = self.has_expirable_items();

    for (key, value) in self.hash.iter() {
      if is_expirable && self.is_expired(key) {
        continue;
      }

      RespWriter::new_ref(output.payload).write_bulk_string(key);
      RespWriter::new_ref(output.payload).write_bulk_string(value);
    }
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

  /// HLEN：字段计数
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashLength
  pub(crate) fn hash_length(&mut self, output: &mut ObjectOutput<'_>) {
    output.result1 = self.purge_expired_len() as i64;
  }

  /// HSTRLEN：字段值长度
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashStrLength
  pub(crate) fn hash_str_length(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    let key = get_byte_span_from_input(args, 0);
    output.result1 = match self.try_get_value(key) {
      Some(hash_value) => hash_value.len() as i64,
      None => 0,
    };
  }

  /// HEXISTS：字段存在性
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashExists
  pub(crate) fn hash_exists(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    let field = get_byte_span_from_input(args, 0);
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

      let indexes = pick_k_random_indexes(count, index_count, seed, count_parameter > 0);

      // Write the size of the array reply
      RespWriter::new_ref(output.payload).write_array_length(
        if with_values && resp_protocol_version == 2 {
          index_count * 2
        } else {
          index_count
        },
      );

      for index in indexes {
        let Some((key, value)) = self.element_at(index) else {
          continue;
        };

        if resp_protocol_version >= 3 && with_values {
          RespWriter::new_ref(output.payload).write_array_length(2);
        }

        RespWriter::new_ref(output.payload).write_bulk_string(&key);

        if with_values {
          RespWriter::new_ref(output.payload).write_bulk_string(&value);
        }

        count_done += 1;
      }
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
        RespWriter::new_ref(output.payload).write_bulk_string(&key);
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

      if matches!(
        HashOperation::try_from(hash_op),
        Ok(HashOperation::Hset) | Ok(HashOperation::Hmset)
      ) {
        match self.hash.get_mut(key) {
          None => {
            self.hash.insert(key.to_vec(), value.to_vec());
            self.update_size(key, value, true);
            set += 1;
          }
          Some(old_val) => {
            let old_len = old_val.len();
            *old_val = value.to_vec();
            self.heap_memory_size +=
              round_up_ptr(value.len()) as i64 - round_up_ptr(old_len) as i64;

            // To persist the key, if it has an expiration
            if self.has_expirable_items()
              && self
                .expiration_times
                .as_mut()
                .unwrap()
                .remove(key)
                .is_some()
            {
              self.heap_memory_size -= SLOT * 2;
              self.cleanup_expiration_structures_if_empty();
            }
          }
        }
      } else if !self.hash.contains_key(key) {
        self.hash.insert(key.to_vec(), value.to_vec());
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
    let Ok(op) = HashOperation::try_from(sub_id) else {
      return;
    };

    RespWriter::new_ref(output.payload).write_array_length(count);

    let is_expirable = self.has_expirable_items();

    let mut written = 0_i64;

    for (key, value) in self.hash.iter() {
      if is_expirable && self.is_expired(key) {
        continue;
      }

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
    // This value is used to indicate partial command execution
    output.result1 = i32::MIN as i64;

    let key = get_byte_span_from_input(args, 0);
    let incr_slice = args[1];

    let Some(incr) = num_utils_try_parse_long(incr_slice) else {
      RespWriter::new_ref(output.payload)
        .write_error_bytes(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      return;
    };

    self.delete_expired_items();

    match self.hash.get(key).cloned() {
      // 新字段：直接存增量原文
      None => {
        self.add(key, incr_slice.to_vec());
        RespWriter::new_ref(output.payload).write_integer_from_bytes(incr_slice);
      }
      Some(hash_value) => {
        let Some(result) = num_utils_try_parse_long(&hash_value) else {
          RespWriter::new_ref(output.payload)
            .write_error_bytes(RESP_ERR_HASH_VALUE_IS_NOT_INTEGER.as_bytes());
          return;
        };

        let result = result.wrapping_add(incr);
        let formatted_value = format_i64(result);
        self.replace_value(key, &hash_value, &formatted_value);

        RespWriter::new_ref(output.payload).write_integer_from_bytes(&formatted_value);
      }
    }

    output.result1 = 1;
  }

  /// HINCRBYFLOAT：浮点增量（值存最短往返文本）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrementFloat
  pub(crate) fn hash_increment_float(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    // This value is used to indicate partial command execution
    output.result1 = i32::MIN as i64;

    let key = get_byte_span_from_input(args, 0);
    let incr_slice = args[1];

    let Some(incr) = num_utils_try_parse_double(incr_slice) else {
      RespWriter::new_ref(output.payload).write_error_bytes(RESP_ERR_NOT_VALID_FLOAT.as_bytes());
      return;
    };

    if incr.is_infinite() {
      RespWriter::new_ref(output.payload)
        .write_error_bytes(RESP_ERR_GENERIC_NAN_INFINITY.as_bytes());
      return;
    }

    self.delete_expired_items();

    match self.hash.get(key).cloned() {
      // 新字段：直接存增量原文
      None => {
        self.add(key, incr_slice.to_vec());
        RespWriter::new_ref(output.payload).write_bulk_string(incr_slice);
      }
      Some(hash_value) => {
        let Some(result) = try_parse_with_infinity(&hash_value) else {
          RespWriter::new_ref(output.payload)
            .write_error_bytes(RESP_ERR_HASH_VALUE_IS_NOT_FLOAT.as_bytes());
          return;
        };

        if result.is_infinite() {
          RespWriter::new_ref(output.payload)
            .write_error_bytes(RESP_ERR_GENERIC_NAN_INFINITY_INCR.as_bytes());
          return;
        }

        let result = result + incr;
        let formatted_value = format_double(result);
        self.replace_value(key, &hash_value, &formatted_value);

        RespWriter::new_ref(output.payload).write_bulk_string(&formatted_value);
      }
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

    for &field in args {
      let mut result = self.get_expiration(field);

      if result >= 0 {
        if is_timestamp && is_milliseconds {
          result = unix_time_in_milliseconds_from_ticks(result);
        } else if is_timestamp && !is_milliseconds {
          result = unix_time_in_seconds_from_ticks(result);
        } else if !is_timestamp && is_milliseconds {
          result = milliseconds_from_diff_utc_now_ticks(result);
        } else if !is_timestamp && !is_milliseconds {
          result = seconds_from_diff_utc_now_ticks(result);
        }
      }

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
  fn replace_value(&mut self, key: &[u8], old_value: &[u8], new_value: &[u8]) {
    // i64 差额：新值 round_up_ptr 短于旧值时 usize 减法会下溢
    self.heap_memory_size +=
      round_up_ptr(new_value.len()) as i64 - round_up_ptr(old_value.len()) as i64;
    if let Some(val) = self.hash.get_mut(key) {
      *val = new_value.to_vec();
    } else {
      self.hash.insert(key.to_vec(), new_value.to_vec());
    }
  }

  /// HSCAN 的对象层入口，转发至 [`scan_operate_shared`]。
  pub(crate) fn scan_operate(&mut self, args: &[&[u8]], limit: i32, output: &mut ObjectOutput<'_>) {
    scan_operate_shared(
      args,
      limit,
      output,
      |cursor, count, pattern, is_no_value| self.scan(cursor, count, pattern, is_no_value),
    );
  }
}

/// i64 → 最短文本
///
/// libs/common/NumUtils.cs:MaximumFormatInt64Length + long.TryFormat
#[inline]
fn format_i64(value: i64) -> Vec<u8> {
  let mut buf = Buffer::new();
  buf.format(value).as_bytes().to_vec()
}
