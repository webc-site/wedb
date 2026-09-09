//! 列表命令（LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE/LINDEX/LTRIM）
//!
//! 同步快路径：经 [`super::object_store_utils`] 信封读写 wobject [`ListObject`]，
//! 磁盘候选等须异步裁决时返回 `Ok(false)` 交调用方降级。

use std::io::Cursor;

use wobject::list::list_object::{ListObject, ListOperation};

use super::object_store_utils::{
  OBJ_TAG_LIST, SyncObj, obj_load_sync, obj_save_or_gc_sync, obj_save_sync, read_object_or_reply,
};
use crate::resp::{
  cmd_strings as cs,
  cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments, write_error_raw},
  parser::resp_ext::{RespSliceExt, RespVecExt},
  resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// libs/server/Resp/Objects/ListCommands.cs:ListPush（LPUSH/RPUSH 共体）
  ///
  /// 应答推入后的列表长度
  pub fn list_push<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_left: bool,
  ) -> wresp::Result<bool> {
    let cmd_name = if is_left { "LPUSH" } else { "RPUSH" };
    if parse_state.len() < 2 {
      abort_with_wrong_number_of_arguments(output, cmd_name);
      return Ok(true);
    }
    let key = parse_state[0];
    let items = &parse_state[1..];

    let list_obj = match obj_load_sync(store, key, OBJ_TAG_LIST) {
      Ok(None) => return Ok(false),
      Ok(Some(SyncObj::Missing)) => ListObject::new(),
      Ok(Some(SyncObj::WrongType)) => {
        write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(Some(SyncObj::Present(p))) => {
        ListObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default()
      }
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let op = if is_left {
      ListOperation::Lpush
    } else {
      ListOperation::Rpush
    };
    for item in items {
      list_obj.operate(op, item);
    }

    let mut payload = Vec::new();
    if list_obj.serialize(&mut payload).is_err() {
      output.write_resp_error("generic error");
      return Ok(true);
    }
    match obj_save_sync(store, key, OBJ_TAG_LIST, &payload) {
      Ok(true) => output.write_resp_int(list_obj.count() as i64),
      Ok(false) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  /// libs/server/Resp/Objects/ListCommands.cs:ListPop（LPOP/RPOP 共体）
  ///
  /// 弹出末元素后整键回收；键缺失回 nil
  pub fn list_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_left: bool,
  ) -> wresp::Result<bool> {
    let cmd_name = if is_left { "LPOP" } else { "RPOP" };
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, cmd_name);
      return Ok(true);
    }
    let key = parse_state[0];

    let list_obj = match obj_load_sync(store, key, OBJ_TAG_LIST) {
      Ok(None) => return Ok(false),
      Ok(Some(SyncObj::Missing)) => {
        output.write_resp_null();
        return Ok(true);
      }
      Ok(Some(SyncObj::WrongType)) => {
        write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(Some(SyncObj::Present(p))) => {
        ListObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default()
      }
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let op = if is_left {
      ListOperation::Lpop
    } else {
      ListOperation::Rpop
    };
    let Some(popped) = list_obj.operate(op, &[]) else {
      // 空列表（信封在但无元素）：与键缺失同应答
      output.write_resp_null();
      return Ok(true);
    };

    let mut payload = Vec::new();
    if list_obj.serialize(&mut payload).is_err() {
      output.write_resp_error("generic error");
      return Ok(true);
    }
    match obj_save_or_gc_sync(store, key, OBJ_TAG_LIST, &payload, list_obj.count() == 0) {
      Ok(true) => output.write_resp_bulk_string(&popped),
      Ok(false) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  /// libs/server/Resp/Objects/ListCommands.cs:ListLength
  pub fn list_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "LLEN");
      return Ok(true);
    }
    let key = parse_state[0];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_LIST,
      output,
      |o| o.write_resp_int(0),
      |p, o| {
        let list_obj = ListObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        o.write_resp_int(list_obj.count() as i64);
      },
    ))
  }

  /// libs/server/Resp/Objects/ListCommands.cs:ListRange
  pub fn list_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      abort_with_wrong_number_of_arguments(output, "LRANGE");
      return Ok(true);
    }
    let key = parse_state[0];
    // C#：start/end 非整数报错（不再静默取缺省）
    let (Some(start), Some(stop)) = (
      parse_state[1].try_parse_i64(),
      parse_state[2].try_parse_i64(),
    ) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_LIST,
      output,
      |o| o.write_resp_array_len(0),
      |p, o| {
        let list_obj = ListObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        let items = list_obj.range(start as isize, stop as isize);
        o.write_resp_array_len(items.len());
        for item in items {
          o.write_resp_bulk_string(&item);
        }
      },
    ))
  }

  /// libs/server/Resp/Objects/ListCommands.cs:ListIndex
  pub fn list_index<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "LINDEX");
      return Ok(true);
    }
    let key = parse_state[0];
    let Some(idx) = parse_state[1].try_parse_i64() else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_LIST,
      output,
      |o| o.write_resp_null(),
      |p, o| {
        let list_obj = ListObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        match list_obj.index(idx as isize) {
          Some(v) => o.write_resp_bulk_string(&v),
          None => o.write_resp_null(),
        }
      },
    ))
  }

  /// libs/server/Resp/Objects/ListCommands.cs:ListTrim
  ///
  /// 键缺失仍回 OK；裁剪至空后整键回收
  pub fn list_trim<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      abort_with_wrong_number_of_arguments(output, "LTRIM");
      return Ok(true);
    }
    let key = parse_state[0];
    let (Some(start), Some(stop)) = (
      parse_state[1].try_parse_i64(),
      parse_state[2].try_parse_i64(),
    ) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    let list_obj = match obj_load_sync(store, key, OBJ_TAG_LIST) {
      Ok(None) => return Ok(false),
      // C# NOTFOUND → OK
      Ok(Some(SyncObj::Missing)) => {
        output.write_resp_simple_string("OK");
        return Ok(true);
      }
      Ok(Some(SyncObj::WrongType)) => {
        write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(Some(SyncObj::Present(p))) => {
        ListObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default()
      }
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    list_obj.trim(start as isize, stop as isize);
    let mut payload = Vec::new();
    if list_obj.serialize(&mut payload).is_err() {
      output.write_resp_error("generic error");
      return Ok(true);
    }
    match obj_save_or_gc_sync(store, key, OBJ_TAG_LIST, &payload, list_obj.count() == 0) {
      Ok(true) => output.write_resp_simple_string("OK"),
      Ok(false) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  pub fn list_position() {
    unimplemented!()
  }
  pub fn list_pop_multiple() {
    unimplemented!()
  }
  pub fn list_blocking_pop() {
    unimplemented!()
  }
  pub fn list_blocking_move() {
    unimplemented!()
  }
  pub fn list_blocking_pop_push() {
    unimplemented!()
  }
  pub fn list_insert() {
    unimplemented!()
  }
  pub fn list_remove() {
    unimplemented!()
  }
  pub fn list_move() {
    unimplemented!()
  }
  pub fn list_right_pop_left_push() {
    unimplemented!()
  }
  pub fn list_set() {
    unimplemented!()
  }
  pub fn list_blocking_pop_multiple() {
    unimplemented!()
  }
}

#[cfg(test)]
mod tests {
  use super::super::super::batch_harness::with_batch;

  /// WRONGTYPE 错误应答帧
  const WRONGTYPE: &[u8] =
    b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";

  #[test]
  fn lpush_lpop_lpush_order_and_recycle() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s
        .list_push(&[b"l", b"a", b"b"], batch, &mut out, true)
        .unwrap();
      assert_eq!(out, b":2\r\n");

      // LPUSH 后 LPOP 取头（b 后进先出）
      let mut out = Vec::new();
      let _ = s.list_pop(&[b"l"], batch, &mut out, true).unwrap();
      assert_eq!(out, b"$1\r\nb\r\n");
      // RPUSH 后 RPOP 取尾
      let _ = s
        .list_push(&[b"l", b"c"], batch, &mut Vec::new(), false)
        .unwrap();
      let mut out = Vec::new();
      let _ = s.list_pop(&[b"l"], batch, &mut out, false).unwrap();
      assert_eq!(out, b"$1\r\nc\r\n");

      // 弹出末元素 → 整键回收
      let mut out = Vec::new();
      let _ = s.list_pop(&[b"l"], batch, &mut out, true).unwrap();
      assert_eq!(out, b"$1\r\na\r\n");
      assert_eq!(batch.try_read_sync(b"l", |_| ()).unwrap(), Some(None::<()>));

      // 键缺失 → nil；arity 带命令名
      let mut out = Vec::new();
      let _ = s.list_pop(&[b"nk"], batch, &mut out, true).unwrap();
      assert_eq!(out, b"$-1\r\n");
      let mut out = Vec::new();
      let _ = s.list_pop(&[], batch, &mut out, true).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'LPOP' command\r\n"
      );

      // 字符串键 → WRONGTYPE 不覆写
      let _ = s
        .network_set(&[b"str", b"v"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s.list_push(&[b"str", b"m"], batch, &mut out, true).unwrap();
      assert_eq!(out, WRONGTYPE);
    });
  }

  #[test]
  fn llen_lrange_lindex_and_strict_parse() {
    with_batch(|s, batch| {
      let _ = s
        .list_push(&[b"l", b"a", b"b", b"c"], batch, &mut Vec::new(), false)
        .unwrap();

      let mut out = Vec::new();
      let _ = s.list_length(&[b"l"], batch, &mut out).unwrap();
      assert_eq!(out, b":3\r\n");

      let mut out = Vec::new();
      let _ = s.list_range(&[b"l", b"0", b"-1"], batch, &mut out).unwrap();
      assert_eq!(out, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
      let mut out = Vec::new();
      let _ = s
        .list_range(&[b"nk", b"0", b"-1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*0\r\n");

      let mut out = Vec::new();
      let _ = s.list_index(&[b"l", b"-1"], batch, &mut out).unwrap();
      assert_eq!(out, b"$1\r\nc\r\n");
      let mut out = Vec::new();
      let _ = s.list_index(&[b"nk", b"0"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");

      // 非整数 start/end → 报错（对标 C# TryGetInt）
      let mut out = Vec::new();
      let _ = s.list_range(&[b"l", b"x", b"1"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
      let mut out = Vec::new();
      let _ = s.list_index(&[b"l", b"y"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
    });
  }

  #[test]
  fn ltrim_clamps_and_recycles() {
    with_batch(|s, batch| {
      let _ = s
        .list_push(&[b"l", b"a", b"b", b"c"], batch, &mut Vec::new(), false)
        .unwrap();
      let mut out = Vec::new();
      let _ = s.list_trim(&[b"l", b"1", b"1"], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");
      let mut out = Vec::new();
      let _ = s.list_length(&[b"l"], batch, &mut out).unwrap();
      assert_eq!(out, b":1\r\n");

      // 裁剪至空 → 整键回收
      let mut out = Vec::new();
      let _ = s.list_trim(&[b"l", b"5", b"9"], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");
      assert_eq!(batch.try_read_sync(b"l", |_| ()).unwrap(), Some(None::<()>));

      // 键缺失 → OK；非整数 → 报错
      let mut out = Vec::new();
      let _ = s.list_trim(&[b"nk", b"0", b"1"], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");
      let mut out = Vec::new();
      let _ = s.list_trim(&[b"nk", b"x", b"1"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
    });
  }
}
