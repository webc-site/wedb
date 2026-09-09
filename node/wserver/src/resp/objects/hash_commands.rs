//! 哈希命令（HGET/HSET/HGETALL/HMGET/HLEN/HDEL/HEXISTS/HKEYS/HVALS）
//!
//! 同步快路径：经 [`super::object_store_utils`] 信封读写 wobject [`HashObject`]，
//! 磁盘候选等须异步裁决时返回 `Ok(false)` 交调用方降级。

use std::io::Cursor;

use wobject::hash::hash_object::{HashObject, HashOperation};

use super::object_store_utils::{
  OBJ_TAG_HASH, SyncObj, obj_load_sync, obj_save_or_gc_sync, obj_save_sync, read_object_or_reply,
};
use crate::resp::{
  cmd_strings as cs,
  cmd_strings::{abort_with_wrong_number_of_arguments, write_error_raw},
  parser::resp_ext::RespVecExt,
  resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// libs/server/Resp/Objects/HashCommands.cs:HashGet
  pub fn hash_get<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "HGET");
      return Ok(true);
    }
    let key = parse_state[0];
    let field = parse_state[1];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_HASH,
      output,
      |o| o.write_resp_null(),
      |p, o| {
        let obj = HashObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        match obj.operate(HashOperation::HGET, field, b"") {
          Some(v) => o.write_resp_bulk_string(&v),
          None => o.write_resp_null(),
        }
      },
    ))
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashSet
  ///
  /// 返回新增字段数（对齐 C#：仅原先不存在的字段计入）
  pub fn hash_set<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // key + 偶数个 field/value
    if parse_state.len() < 3 || parse_state.len() % 2 != 1 {
      abort_with_wrong_number_of_arguments(output, "HSET");
      return Ok(true);
    }
    let key = parse_state[0];

    let obj = match obj_load_sync(store, key, OBJ_TAG_HASH) {
      Ok(None) => return Ok(false),
      Ok(Some(SyncObj::Missing)) => HashObject::new(),
      Ok(Some(SyncObj::WrongType)) => {
        write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(Some(SyncObj::Present(p))) => {
        HashObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default()
      }
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    // 与 storage 层 hash_set 同法：先查后写，单字段单次判定新增
    let mut added = 0i64;
    for i in (1..parse_state.len()).step_by(2) {
      let field = parse_state[i];
      let value = parse_state[i + 1];
      let existed = obj.operate(HashOperation::HGET, field, b"").is_some();
      obj.operate(HashOperation::HSET, field, value);
      if !existed {
        added += 1;
      }
    }

    let mut payload = Vec::new();
    if obj.serialize(&mut payload).is_err() {
      output.write_resp_error("generic error");
      return Ok(true);
    }
    match obj_save_sync(store, key, OBJ_TAG_HASH, &payload) {
      Ok(true) => output.write_resp_int(added),
      Ok(false) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashGetAll
  pub fn hash_get_all<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "HGETALL");
      return Ok(true);
    }
    let key = parse_state[0];

    // RESP2 口径：map 退化为 field/value 交错的扁平数组
    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_HASH,
      output,
      |o| o.write_resp_array_len(0),
      |p, o| {
        let obj = HashObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        let all = obj.hash_get_all();
        o.write_resp_array_len(all.len() * 2);
        for (k, v) in all {
          o.write_resp_bulk_string(&k);
          o.write_resp_bulk_string(&v);
        }
      },
    ))
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashGetMultiple
  pub fn hash_get_multiple<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      abort_with_wrong_number_of_arguments(output, "HMGET");
      return Ok(true);
    }
    let key = parse_state[0];
    let fields = &parse_state[1..];

    // 键缺失：逐字段 nil 占位
    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_HASH,
      output,
      |o| write_null_array(o, fields.len()),
      |p, o| {
        let obj = HashObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        o.write_resp_array_len(fields.len());
        for field in fields {
          match obj.operate(HashOperation::HGET, field, b"") {
            Some(v) => o.write_resp_bulk_string(&v),
            None => o.write_resp_null(),
          }
        }
      },
    ))
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashLength
  pub fn hash_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "HLEN");
      return Ok(true);
    }
    let key = parse_state[0];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_HASH,
      output,
      |o| o.write_resp_int(0),
      |p, o| {
        let obj = HashObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        o.write_resp_int(obj.hash.pin().len() as i64);
      },
    ))
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashDelete
  ///
  /// 删空后整键回收（对齐 storage 层 hash_gc_if_empty）
  pub fn hash_delete<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      abort_with_wrong_number_of_arguments(output, "HDEL");
      return Ok(true);
    }
    let key = parse_state[0];
    let fields = &parse_state[1..];

    let obj = match obj_load_sync(store, key, OBJ_TAG_HASH) {
      Ok(None) => return Ok(false),
      Ok(Some(SyncObj::Missing)) => {
        output.write_resp_int(0);
        return Ok(true);
      }
      Ok(Some(SyncObj::WrongType)) => {
        write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(Some(SyncObj::Present(p))) => {
        HashObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default()
      }
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let mut deleted = 0i64;
    for field in fields {
      if obj.operate(HashOperation::HDEL, field, b"").is_some() {
        deleted += 1;
      }
    }
    if deleted > 0 {
      let mut payload = Vec::new();
      if obj.serialize(&mut payload).is_err() {
        output.write_resp_error("generic error");
        return Ok(true);
      }
      match obj_save_or_gc_sync(
        store,
        key,
        OBJ_TAG_HASH,
        &payload,
        obj.hash.pin().is_empty(),
      ) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => output.write_resp_error("generic error"),
      }
    }
    output.write_resp_int(deleted);
    Ok(true)
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashExists
  pub fn hash_exists<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "HEXISTS");
      return Ok(true);
    }
    let key = parse_state[0];
    let field = parse_state[1];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_HASH,
      output,
      |o| o.write_resp_int(0),
      |p, o| {
        let obj = HashObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        let exists = obj
          .operate(HashOperation::HEXISTS, field, b"")
          .is_some_and(|v| v == b"1");
        o.write_resp_int(i64::from(exists));
      },
    ))
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashKeys
  pub fn hash_keys<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "HKEYS");
      return Ok(true);
    }
    let key = parse_state[0];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_HASH,
      output,
      |o| o.write_resp_array_len(0),
      |p, o| {
        let obj = HashObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        let keys = obj.get_keys();
        o.write_resp_array_len(keys.len());
        for k in keys {
          o.write_resp_bulk_string(&k);
        }
      },
    ))
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashVals
  pub fn hash_vals<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "HVALS");
      return Ok(true);
    }
    let key = parse_state[0];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_HASH,
      output,
      |o| o.write_resp_array_len(0),
      |p, o| {
        let obj = HashObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        let vals = obj.get_values();
        o.write_resp_array_len(vals.len());
        for v in vals {
          o.write_resp_bulk_string(&v);
        }
      },
    ))
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashRandomField
  pub fn hash_random_field() {
    unimplemented!()
  }
  /// libs/server/Resp/Objects/HashCommands.cs:HashStrLength
  pub fn hash_str_length() {
    unimplemented!()
  }
  /// libs/server/Resp/Objects/HashCommands.cs:HashIncrement
  pub fn hash_increment() {
    unimplemented!()
  }
  /// libs/server/Resp/Objects/HashCommands.cs:HashTimeToLive
  pub fn hash_time_to_live() {
    unimplemented!()
  }
}

/// nil 元素数组应答（HMGET 键缺失时的逐字段占位）
pub(super) fn write_null_array(output: &mut Vec<u8>, len: usize) {
  output.write_resp_array_len(len);
  for _ in 0..len {
    output.write_resp_null();
  }
}

#[cfg(test)]
mod tests {
  use std::{io::Cursor, str};

  use super::{
    super::{
      super::batch_harness::with_batch,
      object_store_utils::{OBJ_TAG_HASH, obj_decode, obj_encode},
    },
    *,
  };

  /// WRONGTYPE 错误应答帧
  const WRONGTYPE: &[u8] =
    b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";

  /// 解析 RESP 批量字符串数组帧为 Vec<Vec<u8>>（测试辅助）
  fn parse_bulk_array(frame: &[u8]) -> Vec<Vec<u8>> {
    let mut items = Vec::new();
    let mut pos = frame.iter().position(|&b| b == b'\n').unwrap() + 1;
    while pos < frame.len() {
      assert_eq!(frame[pos], b'$');
      let len_end = frame[pos..].iter().position(|&b| b == b'\n').unwrap() + pos;
      let len: usize = str::from_utf8(&frame[pos + 1..len_end - 1])
        .unwrap()
        .parse()
        .unwrap();
      let start = len_end + 1;
      items.push(frame[start..start + len].to_vec());
      pos = start + len + 2;
    }
    items
  }

  #[test]
  fn hset_counts_new_fields_and_hget_reads() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s
        .hash_set(&[b"h", b"f1", b"v1", b"f2", b"v2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":2\r\n");

      // 已存在字段覆写不计入新增
      let mut out = Vec::new();
      let _ = s.hash_set(&[b"h", b"f1", b"v9"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");

      let mut out = Vec::new();
      let _ = s.hash_get(&[b"h", b"f1"], batch, &mut out).unwrap();
      assert_eq!(out, b"$2\r\nv9\r\n");
      let mut out = Vec::new();
      let _ = s.hash_get(&[b"h", b"nx"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");
      // 键缺失 → nil
      let mut out = Vec::new();
      let _ = s.hash_get(&[b"nokey", b"f"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");
    });
  }

  #[test]
  fn hset_wrongtype_on_string_key() {
    with_batch(|s, batch| {
      let _ = s
        .network_set(&[b"str", b"plain"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s.hash_set(&[b"str", b"f", b"v"], batch, &mut out).unwrap();
      // 错误类型键不被覆写（与 storage 层 WRONGTYPE 传播一致）
      assert_eq!(out, WRONGTYPE);
      let mut out = Vec::new();
      let _ = s.hash_get(&[b"str", b"f"], batch, &mut out).unwrap();
      assert_eq!(out, WRONGTYPE);
    });
  }

  #[test]
  fn envelope_interops_with_storage_layer() {
    with_batch(|s, batch| {
      // RESP 层写入 → 信封格式可被 storage 层解码
      let _ = s
        .hash_set(&[b"h", b"f", b"v"], batch, &mut Vec::new())
        .unwrap();
      let raw = batch
        .try_read_sync(b"h", |v| v.to_vec())
        .unwrap()
        .unwrap()
        .unwrap();
      assert_eq!(raw.first(), Some(&OBJ_TAG_HASH));
      let payload = obj_decode(&raw, OBJ_TAG_HASH).unwrap();
      let obj = HashObject::deserialize(&mut Cursor::new(payload)).unwrap();
      assert_eq!(
        obj.operate(HashOperation::HGET, b"f", b""),
        Some(b"v".to_vec())
      );

      // storage 层信封 → RESP 层可读
      let ext = HashObject::new();
      ext.operate(HashOperation::HSET, b"ext", b"pv");
      let mut buf = Vec::new();
      ext.serialize(&mut buf).unwrap();
      assert!(
        batch
          .try_upsert_sync(b"h2", &obj_encode(OBJ_TAG_HASH, &buf))
          .unwrap()
          .is_ok()
      );
      let mut out = Vec::new();
      let _ = s.hash_get(&[b"h2", b"ext"], batch, &mut out).unwrap();
      assert_eq!(out, b"$2\r\npv\r\n");
    });
  }

  #[test]
  fn hdel_recycles_empty_hash() {
    with_batch(|s, batch| {
      let _ = s
        .hash_set(&[b"h", b"f", b"v"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s.hash_delete(&[b"h", b"f"], batch, &mut out).unwrap();
      assert_eq!(out, b":1\r\n");
      // 删空 → 整键回收
      assert_eq!(batch.try_read_sync(b"h", |_| ()).unwrap(), Some(None::<()>));
      // 再删 → :0
      let mut out = Vec::new();
      let _ = s.hash_delete(&[b"h", b"f"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");
    });
  }

  #[test]
  fn hgetall_hkeys_hvals_hmget_hlen() {
    with_batch(|s, batch| {
      let _ = s
        .hash_set(&[b"h", b"a", b"1", b"b", b"2"], batch, &mut Vec::new())
        .unwrap();

      let mut out = Vec::new();
      let _ = s.hash_length(&[b"h"], batch, &mut out).unwrap();
      assert_eq!(out, b":2\r\n");

      // papaya 迭代序不定：解析为集合后排序比对
      let mut out = Vec::new();
      let _ = s.hash_get_all(&[b"h"], batch, &mut out).unwrap();
      let mut items = parse_bulk_array(&out);
      items.sort();
      assert_eq!(
        items,
        vec![b"1".to_vec(), b"2".to_vec(), b"a".to_vec(), b"b".to_vec()]
      );

      let mut out = Vec::new();
      let _ = s.hash_keys(&[b"h"], batch, &mut out).unwrap();
      let mut keys = parse_bulk_array(&out);
      keys.sort();
      assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

      let mut out = Vec::new();
      let _ = s.hash_vals(&[b"h"], batch, &mut out).unwrap();
      let mut vals = parse_bulk_array(&out);
      vals.sort();
      assert_eq!(vals, vec![b"1".to_vec(), b"2".to_vec()]);

      // 键缺失 HMGET → 逐字段 nil 占位；HGETALL/HKEYS/HVALS → 空数组
      let mut out = Vec::new();
      let _ = s
        .hash_get_multiple(&[b"nx", b"f1", b"f2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*2\r\n$-1\r\n$-1\r\n");

      let mut out = Vec::new();
      let _ = s.hash_get_all(&[b"nx"], batch, &mut out).unwrap();
      assert_eq!(out, b"*0\r\n");
      let mut out = Vec::new();
      let _ = s.hash_keys(&[b"nx"], batch, &mut out).unwrap();
      assert_eq!(out, b"*0\r\n");
      let mut out = Vec::new();
      let _ = s.hash_vals(&[b"nx"], batch, &mut out).unwrap();
      assert_eq!(out, b"*0\r\n");
    });
  }

  #[test]
  fn hexists_and_hmget_hit() {
    with_batch(|s, batch| {
      let _ = s
        .hash_set(&[b"h", b"f", b"v"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s.hash_exists(&[b"h", b"f"], batch, &mut out).unwrap();
      assert_eq!(out, b":1\r\n");
      let mut out = Vec::new();
      let _ = s.hash_exists(&[b"h", b"nx"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");
      let mut out = Vec::new();
      let _ = s.hash_exists(&[b"nk", b"f"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");

      let mut out = Vec::new();
      let _ = s
        .hash_get_multiple(&[b"h", b"f", b"nx"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*2\r\n$1\r\nv\r\n$-1\r\n");
    });
  }
}
