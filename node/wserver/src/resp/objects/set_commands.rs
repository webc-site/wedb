//! 集合命令（SADD/SREM/SCARD/SMEMBERS/SISMEMBER/SPOP/SRANDMEMBER）
//!
//! 同步快路径：经 [`super::object_store_utils`] 信封读写 wobject [`SetObject`]，
//! 磁盘候选等须异步裁决时返回 `Ok(false)` 交调用方降级。

use std::io::Cursor;

use wobject::set::set_object::{SetObject, SetOperation};

use super::object_store_utils::{
  OBJ_TAG_SET, SyncObj, obj_load_sync, obj_save_or_gc_sync, obj_save_sync, read_object_or_reply,
};
use crate::resp::{
  cmd_strings as cs,
  cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments, write_error_raw},
  parser::resp_ext::{RespSliceExt, RespVecExt},
  resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// libs/server/Resp/Objects/SetCommands.cs:SetAdd
  ///
  /// 返回新增成员数（已存在成员不计入）
  pub fn set_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      abort_with_wrong_number_of_arguments(output, "SADD");
      return Ok(true);
    }
    let key = parse_state[0];
    let members = &parse_state[1..];

    let set_obj = match obj_load_sync(store, key, OBJ_TAG_SET) {
      Ok(None) => return Ok(false),
      Ok(Some(SyncObj::Missing)) => SetObject::new(),
      Ok(Some(SyncObj::WrongType)) => {
        write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(Some(SyncObj::Present(p))) => {
        SetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default()
      }
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let mut added = 0i64;
    for member in members {
      if set_obj.operate(SetOperation::Sadd, member) {
        added += 1;
      }
    }

    if added > 0 {
      let mut payload = Vec::new();
      if set_obj.serialize(&mut payload).is_err() {
        output.write_resp_error("generic error");
        return Ok(true);
      }
      match obj_save_sync(store, key, OBJ_TAG_SET, &payload) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    }
    output.write_resp_int(added);
    Ok(true)
  }

  /// libs/server/Resp/Objects/SetCommands.cs:SetRemove
  ///
  /// 删空后整键回收（对齐 storage 层 finalize_removal）
  pub fn set_remove<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      abort_with_wrong_number_of_arguments(output, "SREM");
      return Ok(true);
    }
    let key = parse_state[0];
    let members = &parse_state[1..];

    let set_obj = match obj_load_sync(store, key, OBJ_TAG_SET) {
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
        SetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default()
      }
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let mut removed = 0i64;
    for member in members {
      if set_obj.operate(SetOperation::Srem, member) {
        removed += 1;
      }
    }
    if removed > 0 {
      let mut payload = Vec::new();
      if set_obj.serialize(&mut payload).is_err() {
        output.write_resp_error("generic error");
        return Ok(true);
      }
      match obj_save_or_gc_sync(store, key, OBJ_TAG_SET, &payload, set_obj.count() == 0) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => output.write_resp_error("generic error"),
      }
    }
    output.write_resp_int(removed);
    Ok(true)
  }

  /// libs/server/Resp/Objects/SetCommands.cs:SetLength
  pub fn set_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "SCARD");
      return Ok(true);
    }
    let key = parse_state[0];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_SET,
      output,
      |o| o.write_resp_int(0),
      |p, o| {
        let set_obj = SetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        o.write_resp_int(set_obj.count() as i64);
      },
    ))
  }

  /// libs/server/Resp/Objects/SetCommands.cs:SetMembers
  pub fn set_members<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "SMEMBERS");
      return Ok(true);
    }
    let key = parse_state[0];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_SET,
      output,
      |o| o.write_resp_array_len(0),
      |p, o| {
        let set_obj = SetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        let members = set_obj.get_keys();
        o.write_resp_array_len(members.len());
        for m in members {
          o.write_resp_bulk_string(&m);
        }
      },
    ))
  }

  /// libs/server/Resp/Objects/SetCommands.cs:SetIsMember
  pub fn set_is_member<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "SISMEMBER");
      return Ok(true);
    }
    let key = parse_state[0];
    let member = parse_state[1];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_SET,
      output,
      |o| o.write_resp_int(0),
      |p, o| {
        let set_obj = SetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        let is_member = set_obj.operate(SetOperation::Sismember, member);
        o.write_resp_int(i64::from(is_member));
      },
    ))
  }

  /// libs/server/Resp/Objects/SetCommands.cs:SetPop
  ///
  /// 无 count：单成员 bulk 应答（键缺失 nil）；带 count：成员数组应答。
  /// 弹空后整键回收
  pub fn set_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() || parse_state.len() > 2 {
      abort_with_wrong_number_of_arguments(output, "SPOP");
      return Ok(true);
    }
    let key = parse_state[0];

    // C#：count 解析失败或负数 → 非整数错误；0 → 空数组
    let mut count = 1usize;
    if parse_state.len() == 2 {
      match parse_state[1].try_parse_i64() {
        Some(c) if c >= 0 => count = c as usize,
        _ => {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(true);
        }
      }
      if count == 0 {
        output.write_resp_array_len(0);
        return Ok(true);
      }
    }

    let set_obj = match obj_load_sync(store, key, OBJ_TAG_SET) {
      Ok(None) => return Ok(false),
      // C# NOTFOUND → nil
      Ok(Some(SyncObj::Missing)) => {
        output.write_resp_null();
        return Ok(true);
      }
      Ok(Some(SyncObj::WrongType)) => {
        write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(Some(SyncObj::Present(p))) => {
        SetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default()
      }
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let mut popped = Vec::new();
    for _ in 0..count {
      match set_obj.pop() {
        Some(m) => popped.push(m),
        None => break,
      }
    }

    if !popped.is_empty() {
      let mut payload = Vec::new();
      if set_obj.serialize(&mut payload).is_err() {
        output.write_resp_error("generic error");
        return Ok(true);
      }
      match obj_save_or_gc_sync(store, key, OBJ_TAG_SET, &payload, set_obj.count() == 0) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => output.write_resp_error("generic error"),
      }
    }

    // 无 count 形态：单 bulk；带 count 形态：数组
    if parse_state.len() == 2 {
      output.write_resp_array_len(popped.len());
      for m in popped {
        output.write_resp_bulk_string(&m);
      }
    } else {
      match popped.into_iter().next() {
        Some(m) => output.write_resp_bulk_string(&m),
        None => output.write_resp_null(),
      }
    }
    Ok(true)
  }

  /// libs/server/Resp/Objects/SetCommands.cs:SetRandomMember
  ///
  /// 仅单成员形态（count 形态未实现）；随机采样不弹出
  pub fn set_random_member<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "SRANDMEMBER");
      return Ok(true);
    }
    let key = parse_state[0];

    Ok(read_object_or_reply(
      store,
      key,
      OBJ_TAG_SET,
      output,
      |o| o.write_resp_null(),
      |p, o| {
        let set_obj = SetObject::deserialize(&mut Cursor::new(&p)).unwrap_or_default();
        match set_obj.random_member() {
          Some(m) => o.write_resp_bulk_string(&m),
          None => o.write_resp_null(),
        }
      },
    ))
  }

  pub fn set_intersect() {
    unimplemented!()
  }
  pub fn set_intersect_store() {
    unimplemented!()
  }
  pub fn set_intersect_length() {
    unimplemented!()
  }
  pub fn set_union() {
    unimplemented!()
  }
  pub fn set_union_store() {
    unimplemented!()
  }
  pub fn set_move() {
    unimplemented!()
  }
  pub fn set_diff() {
    unimplemented!()
  }
  pub fn set_diff_store() {
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
  fn sadd_srem_scard_sismember() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s
        .set_add(&[b"st", b"a", b"b", b"a"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":2\r\n");

      let mut out = Vec::new();
      let _ = s.set_length(&[b"st"], batch, &mut out).unwrap();
      assert_eq!(out, b":2\r\n");
      let mut out = Vec::new();
      let _ = s.set_is_member(&[b"st", b"a"], batch, &mut out).unwrap();
      assert_eq!(out, b":1\r\n");
      let mut out = Vec::new();
      let _ = s.set_is_member(&[b"st", b"nx"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");

      let mut out = Vec::new();
      let _ = s
        .set_remove(&[b"st", b"a", b"nx"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");

      // 字符串键 → WRONGTYPE
      let _ = s
        .network_set(&[b"str", b"v"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s.set_add(&[b"str", b"m"], batch, &mut out).unwrap();
      assert_eq!(out, WRONGTYPE);
    });
  }

  #[test]
  fn srem_recycles_empty_set() {
    with_batch(|s, batch| {
      let _ = s.set_add(&[b"st", b"m"], batch, &mut Vec::new()).unwrap();
      let mut out = Vec::new();
      let _ = s.set_remove(&[b"st", b"m"], batch, &mut out).unwrap();
      assert_eq!(out, b":1\r\n");
      assert_eq!(
        batch.try_read_sync(b"st", |_| ()).unwrap(),
        Some(None::<()>)
      );
    });
  }

  #[test]
  fn spop_forms_and_validation() {
    with_batch(|s, batch| {
      let _ = s
        .set_add(&[b"st", b"a", b"b"], batch, &mut Vec::new())
        .unwrap();

      // count=0 → 空数组；负 count / 非整数 → 错误
      let mut out = Vec::new();
      let _ = s.set_pop(&[b"st", b"0"], batch, &mut out).unwrap();
      assert_eq!(out, b"*0\r\n");
      let mut out = Vec::new();
      let _ = s.set_pop(&[b"st", b"-1"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

      // 带 count：数组形态；弹空整键回收
      let mut out = Vec::new();
      let _ = s.set_pop(&[b"st", b"10"], batch, &mut out).unwrap();
      assert!(out.starts_with(b"*2\r\n"));
      assert_eq!(
        batch.try_read_sync(b"st", |_| ()).unwrap(),
        Some(None::<()>)
      );

      // 键缺失：无 count / 带 count 均 nil（C# NOTFOUND → WriteNull）
      let mut out = Vec::new();
      let _ = s.set_pop(&[b"nk"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");
      let mut out = Vec::new();
      let _ = s.set_pop(&[b"nk", b"5"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");
    });
  }

  #[test]
  fn srandmember_missing_key_nil() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.set_random_member(&[b"nk"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");
    });
  }
}
