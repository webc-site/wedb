//! 哈希 RESP 语义操作（对标 libs/server/Objects/Hash/HashObjectImpl.cs，
//! C# 为 HashObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。

use wbase::{
  convert::{
    milliseconds_from_diff_utc_now_ticks, seconds_from_diff_utc_now_ticks,
    unix_time_in_milliseconds_from_ticks, unix_time_in_seconds_from_ticks,
  },
  num::{try_parse_f64, try_parse_i64},
};

use crate::{
  inputs::ObjectInput,
  objects::{
    hash::hash_object::{
      HashObject, HashOperation, pick_k_random_indexes, pick_random_index, scan_operate_shared,
    },
    parse_utils::try_parse_with_infinity,
    sortedset::sorted_set_object::ExpirationWithOption,
    types::object_output::ObjectOutput,
  },
  resp::cmd_strings::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
};

/// ERR hash value is not an integer.
pub(crate) const RESP_ERR_HASH_VALUE_IS_NOT_INTEGER: &[u8] = b"ERR hash value is not an integer.";
/// ERR hash value is not a float.
pub(crate) const RESP_ERR_HASH_VALUE_IS_NOT_FLOAT: &[u8] = b"ERR hash value is not a float.";
/// ERR value is NaN or Infinity
pub(crate) const RESP_ERR_GENERIC_NAN_INFINITY: &[u8] = b"ERR value is NaN or Infinity";

use crate::resp::cmd_strings::{RESP_ERR_GENERIC_NAN_INFINITY_INCR, RESP_ERR_NOT_VALID_FLOAT};

/// 取第 i 个参数字节
///
/// libs/server/Resp/Parser/SessionParseState.cs:GetArgSliceByRef
#[inline]
fn arg<'a>(input: &ObjectInput, i: usize) -> &'a [u8] {
  input.parse_state.get_arg_slice_by_ref(i).as_slice()
}

/// 取第 i 个参数字节切片（对齐 C# GetByteSpanFromInput 的固定入参形态）
///
/// libs/server/Objects/Hash/HashObjectImpl.cs:GetByteSpanFromInput
#[inline]
fn get_byte_span_from_input<'a>(input: &ObjectInput, index: usize) -> &'a [u8] {
  arg(input, index)
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

/// 最短往返双精度文本（对标 double.TryFormat 默认 G 形态；±∞/NaN 记法差异
/// 见 ObjectOutput::format_double 说明）
#[inline]
fn format_double(value: f64) -> Vec<u8> {
  ObjectOutput::format_double(value).into_bytes()
}

impl HashObject {
  /// HGET：单字段取值
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashGet
  pub(crate) fn hash_get(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    let key = get_byte_span_from_input(input, 0);
    match self.try_get_value(key) {
      Some(hash_value) => output.write_bulk_string(hash_value),
      None => output.write_null(resp_protocol_version),
    }

    output.result1 = 1;
  }

  /// HMGET：多字段取值
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashMultipleGet
  pub(crate) fn hash_multiple_get(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    output.write_array_length(input.parse_state.count);

    for i in 0..input.parse_state.count {
      let key = get_byte_span_from_input(input, i);
      match self.try_get_value(key) {
        Some(hash_value) => output.write_bulk_string(hash_value),
        None => output.write_null(resp_protocol_version),
      }
    }

    output.result1 = input.parse_state.count as i64;
  }

  /// HGETALL：全量字段值对（RESP2 扁平数组 / RESP3 map）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashGetAll
  pub(crate) fn hash_get_all(&mut self, output: &mut ObjectOutput, resp_protocol_version: u8) {
    write_map_length(output, self.count(), resp_protocol_version);

    let is_expirable = self.has_expirable_items();

    for (key, value) in self.hash.iter() {
      if is_expirable && self.is_expired(key) {
        continue;
      }

      output.write_bulk_string(key);
      output.write_bulk_string(value);
    }
  }

  /// HDEL：批量删除字段
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashDelete
  pub(crate) fn hash_delete(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    let mut removed = 0_i64;

    for i in 0..input.parse_state.count {
      let key = get_byte_span_from_input(input, i);
      if self.remove(key).is_some() {
        removed += 1;
      }
    }

    output.result1 = removed;
  }

  /// HLEN：字段计数
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashLength
  pub(crate) fn hash_length(&mut self, output: &mut ObjectOutput) {
    output.result1 = self.count() as i64;
  }

  /// HSTRLEN：字段值长度
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashStrLength
  pub(crate) fn hash_str_length(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    let key = get_byte_span_from_input(input, 0);
    output.result1 = match self.try_get_value(key) {
      Some(hash_value) => hash_value.len() as i64,
      None => 0,
    };
  }

  /// HEXISTS：字段存在性
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashExists
  pub(crate) fn hash_exists(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    let field = get_byte_span_from_input(input, 0);
    output.result1 = i64::from(self.contains_key(field));
  }

  /// HRANDFIELD：随机字段（arg1 打包 count/withValues/includedCount，arg2 为种子）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashRandomField
  pub(crate) fn hash_random_field(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    // HRANDFIELD key [count [WITHVALUES]]
    let mut count_parameter = (input.arg1 >> 2) as i64;
    let with_values = (input.arg1 & 1) == 1;
    let included_count = ((input.arg1 >> 1) & 1) == 1;
    let seed = input.arg2;

    let mut count_done = 0_i64;

    if included_count {
      let count = self.count();

      if count == 0 {
        // This can happen because of expiration but RMW operation haven't applied yet
        output.write_empty_array();
        output.result1 = 0;
        return;
      }

      if count_parameter > 0 && count_parameter > count as i64 {
        count_parameter = count as i64;
      }

      let index_count = count_parameter.unsigned_abs() as usize;

      let indexes = pick_k_random_indexes(count, index_count, seed, count_parameter > 0);

      // Write the size of the array reply
      output.write_array_length(if with_values && resp_protocol_version == 2 {
        index_count * 2
      } else {
        index_count
      });

      for index in indexes {
        let Some((key, value)) = self.element_at(index) else {
          continue;
        };

        if resp_protocol_version >= 3 && with_values {
          output.write_array_length(2);
        }

        output.write_bulk_string(&key);

        if with_values {
          output.write_bulk_string(&value);
        }

        count_done += 1;
      }
    } else {
      // No count parameter is present, we just return a random field
      let count = self.count();
      if count == 0 {
        // This can happen because of expiration but RMW operation haven't applied yet
        output.write_null(resp_protocol_version);
        output.result1 = 0;
        return;
      }

      let index = pick_random_index(count, seed);
      if let Some((key, _)) = self.element_at(index) {
        output.write_bulk_string(&key);
      }
      count_done = 1;
    }

    output.result1 = count_done;
  }

  /// HSET / HMSET / HSETNX：批量设置字段
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashSet
  pub(crate) fn hash_set(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    self.delete_expired_items();

    let mut set = 0_i64;

    let hash_op = input.header.sub_id();
    let mut i = 0;
    while i < input.parse_state.count {
      let key = get_byte_span_from_input(input, i);
      let value = arg(input, i + 1);

      match self.hash.get(key) {
        // 新字段（或已过期字段，DeleteExpiredItems 后一般不可达，防御性保留）
        None => {
          self.hash.insert(key.to_vec(), value.to_vec());
          self.update_size(key, value, true);
          set += 1;
        }
        Some(old_value) => {
          let old_value = old_value.clone();
          // HSETNX：字段已存在则无效果
          if matches!(
            HashOperation::try_from(hash_op),
            Ok(HashOperation::Hset) | Ok(HashOperation::Hmset)
          ) {
            // 覆写：同长复用槽位不调整记账，异长按 RoundUp 差额调整
            // （i64 差额：短值覆写长值时 usize 减法会下溢）
            self.heap_memory_size +=
              value.len().div_ceil(8) as i64 * 8 - old_value.len().div_ceil(8) as i64 * 8;
            self.hash.insert(key.to_vec(), value.to_vec());

            // To persist the key, if it has an expiration
            if self.has_expirable_items()
              && self
                .expiration_times
                .as_mut()
                .unwrap()
                .remove(key)
                .is_some()
            {
              self.heap_memory_size -= 16 + 16;
              self.cleanup_expiration_structures_if_empty();
            }
          } else {
            let _ = old_value;
          }
        }
      }

      i += 2;
    }

    output.result1 = set;
  }

  /// HCOLLECT：占位收集操作（清除过期后确认存活）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashCollect
  pub(crate) fn hash_collect(&mut self, output: &mut ObjectOutput) {
    self.delete_expired_items();
    output.result1 = 1;
  }

  /// HKEYS / HVALS：全量字段或全量值
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashGetKeysOrValues
  pub(crate) fn hash_get_keys_or_values(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    let count = self.count();
    let Ok(op) = HashOperation::try_from(input.header.sub_id()) else {
      return;
    };

    output.write_array_length(count);

    let is_expirable = self.has_expirable_items();

    let mut written = 0_i64;

    for (key, value) in self.hash.iter() {
      if is_expirable && self.is_expired(key) {
        continue;
      }

      if op == HashOperation::Hkeys {
        output.write_bulk_string(key);
      } else {
        output.write_bulk_string(value);
      }

      written += 1;
    }

    output.result1 = written;
  }

  /// HINCRBY：整型增量（值存原始文本，读回再解析）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrement
  pub(crate) fn hash_increment(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    // This value is used to indicate partial command execution
    output.result1 = i32::MIN as i64;

    let key = get_byte_span_from_input(input, 0);
    let incr_slice = arg(input, 1);

    let Some(incr) = num_utils_try_parse_long(incr_slice) else {
      output.write_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      return;
    };

    self.delete_expired_items();

    match self.hash.get(key).cloned() {
      // 新字段：直接存增量原文
      None => {
        self.add(key, incr_slice.to_vec());
        write_integer_from_bytes(output, incr_slice);
      }
      Some(hash_value) => {
        let Some(result) = num_utils_try_parse_long(&hash_value) else {
          output.write_error(RESP_ERR_HASH_VALUE_IS_NOT_INTEGER);
          return;
        };

        let result = result.wrapping_add(incr);
        let formatted_value = format_i64(result);
        self.replace_value(key, &hash_value, &formatted_value);

        write_integer_from_bytes(output, &formatted_value);
      }
    }

    output.result1 = 1;
  }

  /// HINCRBYFLOAT：浮点增量（值存最短往返文本）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrementFloat
  pub(crate) fn hash_increment_float(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    // This value is used to indicate partial command execution
    output.result1 = i32::MIN as i64;

    let key = get_byte_span_from_input(input, 0);
    let incr_slice = arg(input, 1);

    let Some(incr) = num_utils_try_parse_double(incr_slice) else {
      output.write_error(RESP_ERR_NOT_VALID_FLOAT.as_bytes());
      return;
    };

    if incr.is_infinite() {
      output.write_error(RESP_ERR_GENERIC_NAN_INFINITY);
      return;
    }

    self.delete_expired_items();

    match self.hash.get(key).cloned() {
      // 新字段：直接存增量原文
      None => {
        self.add(key, incr_slice.to_vec());
        output.write_bulk_string(incr_slice);
      }
      Some(hash_value) => {
        let Some(result) = try_parse_with_infinity(&hash_value) else {
          output.write_error(RESP_ERR_HASH_VALUE_IS_NOT_FLOAT);
          return;
        };

        if result.is_infinite() {
          output.write_error(RESP_ERR_GENERIC_NAN_INFINITY_INCR.as_bytes());
          return;
        }

        let result = result + incr;
        let formatted_value = format_double(result);
        self.replace_value(key, &hash_value, &formatted_value);

        output.write_bulk_string(&formatted_value);
      }
    }

    output.result1 = 1;
  }

  /// HEXPIRE：批量设置成员过期（arg1/arg2 为 ExpirationWithOption 压缩字的高低半部）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashExpire
  pub(crate) fn hash_expire(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    self.delete_expired_items();

    let expiration_with_option = ExpirationWithOption::from_word_head_tail(input.arg1, input.arg2);

    output.write_array_length(input.parse_state.count);

    for i in 0..input.parse_state.count {
      let result = self.set_expiration(
        get_byte_span_from_input(input, i),
        expiration_with_option.expiration_time_in_ticks(),
        expiration_with_option.expire_option(),
      );
      output.write_int64(i64::from(result as i32));
    }

    output.result1 = input.parse_state.count as i64;
  }

  /// HTTL / HEXPIRETIME（arg1 = 毫秒标记，arg2 = 时间戳标记）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashTimeToLive
  pub(crate) fn hash_time_to_live(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    self.delete_expired_items();

    let is_milliseconds = input.arg1 == 1;
    let is_timestamp = input.arg2 == 1;
    let num_fields = input.parse_state.count;

    output.write_array_length(num_fields);

    for i in 0..num_fields {
      let mut result = self.get_expiration(get_byte_span_from_input(input, i));

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

      output.write_int64(result);
    }

    output.result1 = num_fields as i64;
  }

  /// HPERSIST：批量清除成员过期
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashPersist
  pub(crate) fn hash_persist(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    self.delete_expired_items();

    let num_fields = input.parse_state.count;

    output.write_array_length(num_fields);

    for i in 0..num_fields {
      let result = self.persist(get_byte_span_from_input(input, i));
      output.write_int64(i64::from(result));
    }

    output.result1 = num_fields as i64;
  }

  /// 就地覆写字段值并调整记账（同长复用槽位不调整，异长按 RoundUp 差额调整）
  ///
  /// libs/server/Objects/Hash/HashObjectImpl.cs:HashSet/HashIncrement 的
  /// formattedValue.Length == hashValueRef.Length 分支合并形态
  fn replace_value(&mut self, key: &[u8], old_value: &[u8], new_value: &[u8]) {
    // i64 差额：新值 RoundUp 短于旧值时 usize 减法会下溢
    self.heap_memory_size +=
      new_value.len().div_ceil(8) as i64 * 8 - old_value.len().div_ceil(8) as i64 * 8;
    self.hash.insert(key.to_vec(), new_value.to_vec());
  }

  /// HSCAN 的对象层入口（解析光标/MATCH/COUNT/NOVALUES 后走 [`Self::scan`]）
  ///
  /// libs/server/Objects/Types/GarnetObjectBase.cs:Scan(ref ObjectInput, ...)
  pub(crate) fn scan_operate(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
    scan_operate_shared(input, output, |cursor, count, pattern, is_no_value| {
      self.scan(cursor, count, pattern, is_no_value)
    });
  }
}

/// RESP2 口径写 map 头（RESP3 `%n`，RESP2 退化为双倍长度数组）
///
/// libs/common/RespMemoryWriter.cs:WriteMapLength
fn write_map_length(output: &mut ObjectOutput, len: usize, resp_protocol_version: u8) {
  if resp_protocol_version >= 3 {
    output.payload.push(b'%');
    let mut buf = itoa::Buffer::new();
    output.payload.extend_from_slice(buf.format(len).as_bytes());
    output.payload.extend_from_slice(b"\r\n");
  } else {
    output.write_array_length(len * 2);
  }
}

/// 以既有整数字节文本原样写整数回复（不做规范化，":<bytes>\r\n"）
///
/// libs/common/RespWriteUtils.cs:TryWriteIntegerFromBytes
#[inline]
fn write_integer_from_bytes(output: &mut ObjectOutput, value: &[u8]) {
  output.payload.push(b':');
  output.payload.extend_from_slice(value);
  output.payload.extend_from_slice(b"\r\n");
}

/// i64 → 最短文本
///
/// libs/common/NumUtils.cs:MaximumFormatInt64Length + long.TryFormat
#[inline]
fn format_i64(value: i64) -> Vec<u8> {
  let mut buf = itoa::Buffer::new();
  buf.format(value).as_bytes().to_vec()
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    arg_slice::ArgSlice,
    input_header::RespInputHeader,
    objects::{
      hash::hash_object::{ExpireOption, HashOperation},
      parse_utils::now_ticks,
    },
    session_parse_state::SessionParseState,
    types::{GarnetObjectType, RespInputFlags},
  };

  /// 构造 ObjectInput（backing 须与 input 同生命周期存活）
  fn make_input(
    op: HashOperation,
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
      RespInputHeader::new_with_type(GarnetObjectType::Hash, RespInputFlags::empty());
    header.set_sub_id(op as u8);
    (
      ObjectInput::new_with_state(header, &mut parse_state, arg1, arg2),
      backing,
    )
  }

  fn seed(obj: &mut HashObject, fields: &[(&str, &str)]) {
    for (k, v) in fields {
      obj
        .hash
        .insert(k.as_bytes().to_vec(), v.as_bytes().to_vec());
    }
  }

  fn payload_str(out: &ObjectOutput) -> String {
    String::from_utf8_lossy(&out.payload).into_owned()
  }

  /// 解析 bulk string 序列负载（散列迭代序无关比对辅助）
  fn parse_bulk_items(frame: &[u8]) -> Vec<Vec<u8>> {
    let mut items = Vec::new();
    let mut pos = 0;
    while pos < frame.len() {
      if frame[pos] != b'$' {
        pos += 1;
        continue;
      }
      let Some(line_end) = frame[pos..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|p| p + pos)
      else {
        break;
      };
      let Ok(len) = str::from_utf8(&frame[pos + 1..line_end - 1])
        .unwrap_or("")
        .parse::<usize>()
      else {
        break;
      };
      let start = line_end + 1;
      items.push(frame[start..start + len].to_vec());
      pos = start + len + 2;
    }
    items
  }

  /// HGET / HMGET / HGETALL / HKEYS / HVALS / HLEN / HSTRLEN / HEXISTS
  #[test]
  fn read_ops() {
    let mut obj = HashObject::new();
    seed(&mut obj, &[("a", "1"), ("b", "22")]);

    let (input, _b) = make_input(HashOperation::Hget, &[b"a"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_get(&input, &mut out, 2);
    assert_eq!(out.payload, b"$1\r\n1\r\n");
    assert_eq!(out.result1, 1);

    let (input, _b) = make_input(HashOperation::Hmget, &[b"a", b"nx", b"b"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_multiple_get(&input, &mut out, 2);
    assert_eq!(out.payload, b"*3\r\n$1\r\n1\r\n$-1\r\n$2\r\n22\r\n");
    assert_eq!(out.result1, 3);

    // HGETALL：RESP2 扁平（迭代序随散列，解析后排序比对）
    let (_input, _b) = make_input(HashOperation::Hgetall, &[], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_get_all(&mut out, 2);
    let payload = payload_str(&out);
    assert!(payload.starts_with("*4\r\n"), "{payload}");
    for tok in ["1", "22", "a", "b"] {
      assert!(
        payload.contains(&format!("${}\r\n{}\r\n", tok.len(), tok)),
        "{payload}"
      );
    }

    // HKEYS / HVALS（散列迭代序不定，排序比对）
    let (input, _b) = make_input(HashOperation::Hkeys, &[], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_get_keys_or_values(&input, &mut out, 2);
    let mut keys = parse_bulk_items(&out.payload);
    keys.sort();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    assert_eq!(out.result1, 2);

    let (input, _b) = make_input(HashOperation::Hvals, &[], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_get_keys_or_values(&input, &mut out, 2);
    let mut vals = parse_bulk_items(&out.payload);
    vals.sort();
    assert_eq!(vals, vec![b"1".to_vec(), b"22".to_vec()]);

    let (_input, _b) = make_input(HashOperation::Hlen, &[], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_length(&mut out);
    assert_eq!(out.result1, 2);

    let (input, _b) = make_input(HashOperation::Hstrlen, &[b"b"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_str_length(&input, &mut out);
    assert_eq!(out.result1, 2);
    let (input, _b) = make_input(HashOperation::Hstrlen, &[b"nx"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_str_length(&input, &mut out);
    assert_eq!(out.result1, 0);

    let (input, _b) = make_input(HashOperation::Hexists, &[b"a"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_exists(&input, &mut out);
    assert_eq!(out.result1, 1);
    let (input, _b) = make_input(HashOperation::Hexists, &[b"nx"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_exists(&input, &mut out);
    assert_eq!(out.result1, 0);
  }

  /// HSET / HMSET / HSETNX 语义矩阵（新增计数、覆写不计数、HSETNX 不覆写）
  #[test]
  fn set_ops() {
    let mut obj = HashObject::new();

    // HSET 两对新字段
    let (input, _b) = make_input(HashOperation::Hset, &[b"f1", b"v1", b"f2", b"v2"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_set(&input, &mut out);
    assert_eq!(out.result1, 2);

    // HSET 覆写既有字段：不计入新增
    let (input, _b) = make_input(HashOperation::Hset, &[b"f1", b"v9"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_set(&input, &mut out);
    assert_eq!(out.result1, 0);
    assert_eq!(obj.try_get_value(b"f1"), Some(&b"v9".to_vec()));

    // HMSET 与 HSET 同语义
    let (input, _b) = make_input(HashOperation::Hmset, &[b"f3", b"v3"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_set(&input, &mut out);
    assert_eq!(out.result1, 1);

    // HSETNX：已存在字段不动
    let (input, _b) = make_input(HashOperation::Hsetnx, &[b"f1", b"other"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_set(&input, &mut out);
    assert_eq!(out.result1, 0);
    assert_eq!(obj.try_get_value(b"f1"), Some(&b"v9".to_vec()));

    // HSETNX：新字段照常新增
    let (input, _b) = make_input(HashOperation::Hsetnx, &[b"f4", b"v4"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_set(&input, &mut out);
    assert_eq!(out.result1, 1);

    // HSET 覆写清除成员过期（persist 语义）
    let future = now_ticks() + 10_000_000;
    let _ = obj.set_expiration(b"f2", future, ExpireOption::NONE);
    let (input, _b) = make_input(HashOperation::Hset, &[b"f2", b"vv"], 0, 0);
    obj.hash_set(&input, &mut ObjectOutput::new());
    assert_eq!(obj.get_expiration(b"f2"), -1);
  }

  /// HDEL / HCOLLECT
  #[test]
  fn delete_and_collect() {
    let mut obj = HashObject::new();
    seed(&mut obj, &[("a", "1"), ("b", "2")]);

    let (input, _b) = make_input(HashOperation::Hdel, &[b"a", b"nx"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_delete(&input, &mut out);
    assert_eq!(out.result1, 1);
    assert_eq!(obj.count(), 1);

    let (_input, _b) = make_input(HashOperation::Hcollect, &[], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_collect(&mut out);
    assert_eq!(out.result1, 1);
  }

  /// HINCRBY：新字段存原文、读回求和、非整型报错
  #[test]
  fn incr_int() {
    let mut obj = HashObject::new();

    // 新字段：存原文并回显
    let (input, _b) = make_input(HashOperation::Hincrby, &[b"f", b"10"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment(&input, &mut out, 2);
    assert_eq!(out.payload, b":10\r\n");
    assert_eq!(out.result1, 1);
    // 值以原文形式存储
    assert_eq!(obj.try_get_value(b"f"), Some(&b"10".to_vec()));

    // 累加
    let (input, _b) = make_input(HashOperation::Hincrby, &[b"f", b"-3"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment(&input, &mut out, 2);
    assert_eq!(out.payload, b":7\r\n");
    assert_eq!(obj.try_get_value(b"f"), Some(&b"7".to_vec()));

    // 存量非整型
    seed(&mut obj, &[("s", "abc")]);
    let (input, _b) = make_input(HashOperation::Hincrby, &[b"s", b"1"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR hash value is not an integer.\r\n");

    // 增量非整型
    let (input, _b) = make_input(HashOperation::Hincrby, &[b"f", b"1.5"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment(&input, &mut out, 2);
    assert_eq!(
      out.payload,
      b"-ERR value is not an integer or out of range.\r\n"
    );

    // 前导零与 + 号（Utf8Parser 宽松形态；存量值走格式化文本 → 规范化）
    let (input, _b) = make_input(HashOperation::Hincrby, &[b"f", b"+007"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment(&input, &mut out, 2);
    assert_eq!(out.payload, b":14\r\n");

    // 新字段原文直存（TryWriteIntegerFromBytes 原样拷贝，不规范化）
    let (input, _b) = make_input(HashOperation::Hincrby, &[b"g", b"+007"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment(&input, &mut out, 2);
    assert_eq!(out.payload, b":+007\r\n");
    assert_eq!(obj.try_get_value(b"g"), Some(&b"+007".to_vec()));
  }

  /// HINCRBYFLOAT：浮点文本语义（新字段存原文、累加、∞/NaN 边界）
  #[test]
  fn incr_float() {
    let mut obj = HashObject::new();

    // 新字段：存原文
    let (input, _b) = make_input(HashOperation::Hincrbyfloat, &[b"f", b"10.5"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment_float(&input, &mut out, 2);
    assert_eq!(out.payload, b"$4\r\n10.5\r\n");
    assert_eq!(obj.try_get_value(b"f"), Some(&b"10.5".to_vec()));

    // 累加：最短往返文本
    let (input, _b) = make_input(HashOperation::Hincrbyfloat, &[b"f", b"0.1"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment_float(&input, &mut out, 2);
    assert_eq!(out.payload, b"$4\r\n10.6\r\n", "{}", payload_str(&out));

    // 增量为 inf 词形 → 拒绝（RESP_ERR_NOT_VALID_FLOAT，Utf8Parser 不识别）
    let (input, _b) = make_input(HashOperation::Hincrbyfloat, &[b"f", b"inf"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment_float(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR value is not a valid float\r\n");

    // 增量数值溢出为 ±inf → NAN_INFINITY 错误
    let (input, _b) = make_input(HashOperation::Hincrbyfloat, &[b"f", b"1e400"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment_float(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR value is NaN or Infinity\r\n");

    // 存量非浮点
    seed(&mut obj, &[("s", "abc")]);
    let (input, _b) = make_input(HashOperation::Hincrbyfloat, &[b"s", b"1.5"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment_float(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR hash value is not a float.\r\n");

    // 存量为 inf 文本（TryParseWithInfinity 认可）→ 拒绝再增
    seed(&mut obj, &[("i", "inf")]);
    let (input, _b) = make_input(HashOperation::Hincrbyfloat, &[b"i", b"1.5"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment_float(&input, &mut out, 2);
    assert_eq!(
      out.payload,
      b"-ERR increment would produce NaN or Infinity\r\n"
    );

    // 存量 "Infinity" 扩展词形 → 非浮点
    seed(&mut obj, &[("w", "Infinity")]);
    let (input, _b) = make_input(HashOperation::Hincrbyfloat, &[b"w", b"1.5"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment_float(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR hash value is not a float.\r\n");
  }

  /// HEXPIRE / HTTL / HPERSIST 家族（ExpirationWithOption 压缩字）
  #[test]
  fn expire_family() {
    let mut obj = HashObject::new();
    seed(&mut obj, &[("a", "1"), ("b", "2")]);

    // HEXPIRE：expiration+option 压缩字
    let exp = now_ticks() + 1_000_000;
    let e = ExpirationWithOption::new(exp, ExpireOption::NONE);
    let (input, _b) = make_input(
      HashOperation::Hexpire,
      &[b"a", b"zz"],
      (e.word() >> 32) as i32,
      e.word() as i32,
    );
    let mut out = ObjectOutput::new();
    obj.hash_expire(&input, &mut out, 2);
    assert_eq!(out.payload, b"*2\r\n:1\r\n:-2\r\n");
    assert_eq!(out.result1, 2);

    // HTTL：剩余毫秒（arg1=1）
    let (input, _b) = make_input(HashOperation::Httl, &[b"a"], 1, 0);
    let mut out = ObjectOutput::new();
    obj.hash_time_to_live(&input, &mut out, 2);
    let payload = payload_str(&out);
    let ttl: i64 = payload
      .lines()
      .nth(1)
      .and_then(|l| l.trim_start_matches(':').parse().ok())
      .unwrap_or(-999);
    assert!((90..=100).contains(&ttl), "{payload}");

    // HTTL：时间戳形态（arg2=1）
    let (input, _b) = make_input(HashOperation::Httl, &[b"a"], 0, 1);
    let mut out = ObjectOutput::new();
    obj.hash_time_to_live(&input, &mut out, 2);
    let payload = payload_str(&out);
    let ts: i64 = payload
      .lines()
      .nth(1)
      .and_then(|l| l.trim_start_matches(':').parse().ok())
      .unwrap_or(-999);
    const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;
    let expected = (exp - UNIX_EPOCH_TICKS) / 10_000_000;
    assert!((ts - expected).abs() <= 1, "{payload} vs {expected}");

    // HPERSIST
    let (input, _b) = make_input(HashOperation::Hpersist, &[b"a", b"b"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_persist(&input, &mut out, 2);
    assert_eq!(out.payload, b"*2\r\n:1\r\n:-1\r\n");
    assert!(!obj.has_expirable_items());
  }

  /// HRANDFIELD：无 count / 带 count / WITHVALUES
  #[test]
  fn random_field() {
    let mut obj = HashObject::new();
    seed(&mut obj, &[("a", "1"), ("b", "2"), ("c", "3")]);

    // 无 count（includedCount=false）：单 bulk
    let (input, _b) = make_input(HashOperation::Hrandfield, &[], 0, 7);
    let mut out = ObjectOutput::new();
    obj.hash_random_field(&input, &mut out, 2);
    assert!(out.payload.starts_with(b"$1\r\n"), "{}", payload_str(&out));
    assert_eq!(out.result1, 1);

    // count=2（arg1 = (2<<1|1)<<1 = 10）
    let (input, _b) = make_input(HashOperation::Hrandfield, &[], ((2 << 1) | 1) << 1, 42);
    let mut out = ObjectOutput::new();
    obj.hash_random_field(&input, &mut out, 2);
    assert!(out.payload.starts_with(b"*2\r\n"), "{}", payload_str(&out));

    // count=2 + WITHVALUES（RESP2 扁平 4 项；arg1 = ((2<<1|1)<<1)|1 = 11）
    let (input, _b) = make_input(
      HashOperation::Hrandfield,
      &[],
      (((2 << 1) | 1) << 1) | 1,
      42,
    );
    let mut out = ObjectOutput::new();
    obj.hash_random_field(&input, &mut out, 2);
    assert!(out.payload.starts_with(b"*4\r\n"), "{}", payload_str(&out));

    // count 大于集合：钳制到 3（arg1 = (5<<1|1)<<1 = 22）
    let (input, _b) = make_input(HashOperation::Hrandfield, &[], ((5 << 1) | 1) << 1, 42);
    let mut out = ObjectOutput::new();
    obj.hash_random_field(&input, &mut out, 2);
    assert!(out.payload.starts_with(b"*3\r\n"), "{}", payload_str(&out));

    // count=0：命令层拦截不触达对象层；includedCount 且空集 → 空数组
    let mut empty = HashObject::new();
    let (input, _b) = make_input(HashOperation::Hrandfield, &[], ((1 << 1) | 1) << 1, 1);
    let mut out = ObjectOutput::new();
    empty.hash_random_field(&input, &mut out, 2);
    assert_eq!(out.payload, b"*0\r\n");
    assert_eq!(out.result1, 0);

    // 空集无 count → null
    let (input, _b) = make_input(HashOperation::Hrandfield, &[], 0, 1);
    let mut out = ObjectOutput::new();
    empty.hash_random_field(&input, &mut out, 2);
    assert_eq!(out.payload, b"$-1\r\n");
  }

  /// HSCAN：光标 + MATCH + NOVALUES（经 operate 分派）
  #[test]
  fn scan_flow() {
    let mut obj = HashObject::new();
    seed(&mut obj, &[("one", "1"), ("two", "2"), ("three", "3")]);

    let (input, _b) = make_input(HashOperation::Hscan, &[b"0", b"MATCH", b"t*"], 0, 0);
    let mut out = ObjectOutput::new();
    assert!(obj.operate(&input, &mut out, 2));
    let payload = payload_str(&out);
    assert!(payload.starts_with("*2\r\n$1\r\n0\r\n*4\r\n"), "{payload}");
    for (field, value) in [("two", "2"), ("three", "3")] {
      assert!(
        payload.contains(&format!("${}\r\n{}\r\n", field.len(), field)),
        "{payload}"
      );
      assert!(
        payload.contains(&format!("$1\r\n{}\r\n", value)),
        "{payload}"
      );
    }

    // NOVALUES：只回字段
    let (input, _b) = make_input(HashOperation::Hscan, &[b"0", b"NOVALUES"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.scan_operate(&input, &mut out, 2);
    let payload = payload_str(&out);
    assert!(payload.starts_with("*2\r\n$1\r\n0\r\n*3\r\n"), "{payload}");

    // 非法光标
    let (input, _b) = make_input(HashOperation::Hscan, &[b"-1"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.scan_operate(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR invalid cursor\r\n");

    // 类型不符 → WRONGTYPE
    let (input, _b) = make_input(HashOperation::Hget, &[b"a"], 0, 0);
    let mut input = input;
    input.header.data[0] = GarnetObjectType::List as u8;
    let mut out = ObjectOutput::new();
    obj.operate(&input, &mut out, 2);
    assert!(out.has_wrong_type());
  }

  /// operate 分派冒烟：HSET 新增 → 删空 REMOVE_KEY
  #[test]
  fn operate_dispatch_smoke() {
    let mut obj = HashObject::new();
    let (input, _b) = make_input(HashOperation::Hset, &[b"f", b"v"], 0, 0);
    let mut out = ObjectOutput::new();
    assert!(obj.operate(&input, &mut out, 2));
    assert_eq!(out.result1, 1);

    let (input, _b) = make_input(HashOperation::Hdel, &[b"f"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.operate(&input, &mut out, 2);
    assert!(out.has_remove_key());
  }

  /// 溢出回绕与覆写记账（C# unchecked 语义 / usize 下溢防线的回归锁定）
  #[test]
  fn incr_overflow_and_reaccount() {
    // HINCRBY 溢出回绕（C# unchecked `result += incr`，不得 panic）
    let mut obj = HashObject::new();
    seed(&mut obj, &[("m", "9223372036854775807")]);
    let (input, _b) = make_input(HashOperation::Hincrby, &[b"m", b"1"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment(&input, &mut out, 2);
    assert_eq!(out.payload, b":-9223372036854775808\r\n");
    assert_eq!(
      obj.try_get_value(b"m"),
      Some(&b"-9223372036854775808".to_vec())
    );

    // 短值覆写长值：记账差额按 i64 计算（usize 下溢防线；
    // seed 直插 hash 未记账，8 - 32 = -24 的相对差额）
    let mut obj = HashObject::new();
    seed(&mut obj, &[("k", "0123456789ABCDEF0123456789")]);
    let before = obj.heap_memory_size;
    let (input, _b) = make_input(HashOperation::Hset, &[b"k", b"xy"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_set(&input, &mut out);
    assert_eq!(out.result1, 0);
    assert_eq!(obj.try_get_value(b"k"), Some(&b"xy".to_vec()));
    assert_eq!(obj.heap_memory_size, before - 24);

    // HINCRBY 缩位覆写（19 位 → 20 位负数，RoundUp 同档 → 差额 0）
    let mut obj = HashObject::new();
    seed(&mut obj, &[("n", "9223372036854775807")]);
    let before = obj.heap_memory_size;
    let (input, _b) = make_input(HashOperation::Hincrby, &[b"n", b"-1"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.hash_increment(&input, &mut out, 2);
    assert_eq!(out.payload, b":9223372036854775806\r\n");
    assert_eq!(obj.heap_memory_size, before);
  }

  /// num_utils_try_parse_long：Utf8Parser 宽松形态矩阵
  #[test]
  fn parse_long_matrix() {
    assert_eq!(num_utils_try_parse_long(b"42"), Some(42));
    assert_eq!(num_utils_try_parse_long(b"-7"), Some(-7));
    assert_eq!(num_utils_try_parse_long(b"+7"), Some(7));
    assert_eq!(num_utils_try_parse_long(b"007"), Some(7));
    assert_eq!(num_utils_try_parse_long(b""), None);
    assert_eq!(num_utils_try_parse_long(b"-"), None);
    assert_eq!(num_utils_try_parse_long(b"1.5"), None);
    assert_eq!(num_utils_try_parse_long(b"1x"), None);
    assert_eq!(
      num_utils_try_parse_long(i64::MIN.to_string().as_bytes()),
      Some(i64::MIN)
    );
    assert_eq!(num_utils_try_parse_long(b"99999999999999999999"), None);
  }

  /// num_utils_try_parse_double：Utf8Parser 不识别词形、溢出保留
  #[test]
  fn parse_double_matrix() {
    assert_eq!(num_utils_try_parse_double(b"1.5"), Some(1.5));
    assert_eq!(num_utils_try_parse_double(b"1e400"), Some(f64::INFINITY));
    assert_eq!(
      num_utils_try_parse_double(b"-1e400"),
      Some(f64::NEG_INFINITY)
    );
    assert_eq!(num_utils_try_parse_double(b"inf"), None);
    assert_eq!(num_utils_try_parse_double(b"INF"), None);
    assert_eq!(num_utils_try_parse_double(b"-inf"), None);
    assert_eq!(num_utils_try_parse_double(b"nan"), None);
    assert_eq!(num_utils_try_parse_double(b"Infinity"), None);
    assert_eq!(num_utils_try_parse_double(b""), None);
    assert_eq!(num_utils_try_parse_double(b"1.5x"), None);
  }
}
