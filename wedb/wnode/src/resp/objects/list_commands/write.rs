//! 列表修改命令实现（LPUSH, RPUSH, LPUSHX, RPUSHX, LPOP, RPOP, LTRIM, LINSERT, LREM, LMOVE, RPOPLPUSH, LSET）

use wbase::num::strict_i32;
use wcol::list::list_object::{ListObject, ListOperation, OperationDirection};
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
  ext::RespVecExt,
};

use super::{ListLoad, Rmw, list_load_sync, list_save_or_gc, parse_i32_pair_args, run_operate};
use crate::{
  resp::{objects::object_store_utils::RespRmwDone, resp_server_session::RespServerSession},
  session_parse_state_extensions::operation_direction_from_token as parse_direction,
};

impl RespServerSession {
  /// LPUSH key element [element ...] / RPUSH key element [element ...]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListPush（LPUSH/RPUSH 共体）
  pub fn list_push<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_left: bool,
  ) -> wresp::Result<bool> {
    let (op, name) = if is_left {
      (ListOperation::Lpush, "LPUSH")
    } else {
      (ListOperation::Rpush, "RPUSH")
    };
    self.list_push_by_op(parse_state, store, output, op, name)
  }

  /// C# ListPush 共用体（LPUSHX/RPUSHX 命令分支；精确锚点见本文件 20 行）
  ///
  /// LPUSHX/RPUSHX 共体入口（键缺失不物化空列表，回复 0）：
  pub fn list_push_x<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_left: bool,
  ) -> wresp::Result<bool> {
    let (op, name) = if is_left {
      (ListOperation::Lpushx, "LPUSHX")
    } else {
      (ListOperation::Rpushx, "RPUSHX")
    };
    self.list_push_by_op(parse_state, store, output, op, name)
  }

  /// LPUSH/RPUSH/LPUSHX/RPUSHX 内部实现公共体
  fn list_push_by_op<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    op: ListOperation,
    cmd_name: &str,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, cmd_name);

    let key = parse_state[0];

    // LPUSHX/RPUSHX：键缺失不创建（对齐 GarnetObject.NeedToCreate 矩阵）
    if matches!(op, ListOperation::Lpushx | ListOperation::Rpushx) {
      match list_load_sync(store, key, output) {
        ListLoad::Degrade => return Ok(false),
        ListLoad::WrongType => {}
        ListLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
        ListLoad::Present(mut obj) => {
          // LPUSH 族仅回填 result1（无负载段），整数应答在写回成功后落帧
          let result1 = run_operate(
            &mut obj,
            op,
            &parse_state[1..],
            0,
            0,
            self.resp_protocol_version,
            output,
          )
          .result1;
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => {
              output.write_resp_int(result1);
              self.notify_collection_update(&**store, &**store, key);
            }
            Ok(false) => return Ok(false),
            Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
          }
        }
      }
      return Ok(true);
    }

    match self.list_rmw(store, key, op, &parse_state[1..], (0, 0), output) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType | Rmw::Missing => {}
      // C# 仅回填 result1，整数回复由 RESP 层写出
      Rmw::Present(RespRmwDone {
        result1,
        payload_written,
      }) => {
        if !payload_written {
          output.write_resp_int(result1);
        }
        self.notify_collection_update(&**store, &**store, key);
      }
    }
    Ok(true)
  }

  /// LPOP key \[count\] / RPOP key \[count\]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListPop
  pub fn list_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_left: bool,
  ) -> wresp::Result<bool> {
    let cmd_name = if is_left { "LPOP" } else { "RPOP" };
    check_arg_count!(parse_state, 1.., output, cmd_name);

    let key = parse_state[0];

    // Read count
    let pop_count = if parse_state.len() == 2 {
      match strict_i32(parse_state[1]) {
        Some(c) if c >= 0 => c,
        _ => {
          cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
          return Ok(true);
        }
      }
    } else {
      1
    };

    let op = if is_left {
      ListOperation::Lpop
    } else {
      ListOperation::Rpop
    };

    if let Rmw::Degrade = self.list_rmw(store, key, op, &[], (pop_count, 0), output) {
      return Ok(false);
    }
    Ok(true)
  }

  /// LTRIM key start stop
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListTrim
  pub fn list_trim<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // start/stop 参数推导单源（快慢共用，失败帧已写出）
    let Some((start, stop)) = parse_i32_pair_args("LTRIM", parse_state, output) else {
      return Ok(true);
    };
    let key = parse_state[0];

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::WrongType => {}
      // C# NOTFOUND → OK（无对象可裁剪，仍回 OK）
      ListLoad::Missing => output.extend_from_slice(cs::RESP_OK),
      ListLoad::Present(mut obj) => {
        run_operate(
          &mut obj,
          ListOperation::Ltrim,
          &[],
          start,
          stop,
          self.resp_protocol_version,
          output,
        );
        match list_save_or_gc(store, key, &obj) {
          Ok(true) => output.extend_from_slice(cs::RESP_OK),
          Ok(false) => return Ok(false),
          Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
        }
      }
    }
    Ok(true)
  }

  /// LINSERT key BEFORE|AFTER pivot element
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListInsert
  pub fn list_insert<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 4, output, "LINSERT");
    let key = parse_state[0];

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::WrongType => {}
      // C# NOTFOUND → :0
      ListLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      ListLoad::Present(mut obj) => {
        let mut obj_out = run_operate(
          &mut obj,
          ListOperation::Linsert,
          &parse_state[1..],
          0,
          0,
          self.resp_protocol_version,
          output,
        );
        let result1 = obj_out.result1;
        if result1 > 0 {
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => self.notify_collection_update(&**store, &**store, key),
            Ok(false) => {
              obj_out.reset();
              return Ok(false);
            }
            Err(_) => {
              obj_out.reset();
              output.write_resp_error(RESP_ERR_GENERIC);
              return Ok(true);
            }
          }
        }
        // C# 仅回填 result1（pivot 缺失 → -1），整数回复由 RESP 层写出
        output.write_resp_int(result1);
      }
    }
    Ok(true)
  }

  /// LREM key count element
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListRemove
  pub fn list_remove<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "LREM");
    let key = parse_state[0];
    let Some(count) = strict_i32(parse_state[1]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::WrongType => {}
      // C# NOTFOUND → :0
      ListLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      ListLoad::Present(mut obj) => {
        let mut obj_out = run_operate(
          &mut obj,
          ListOperation::Lrem,
          &[parse_state[2]],
          count,
          0,
          self.resp_protocol_version,
          output,
        );
        let result1 = obj_out.result1;
        if result1 > 0 {
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => {}
            Ok(false) => {
              obj_out.reset();
              return Ok(false);
            }
            Err(_) => {
              obj_out.reset();
              output.write_resp_error(RESP_ERR_GENERIC);
              return Ok(true);
            }
          }
        }
        // C# 仅回填 result1，整数回复由 RESP 层写出
        output.write_resp_int(result1);
      }
    }
    Ok(true)
  }

  /// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListMove
  pub fn list_move<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 4, output, "LMOVE");

    let (Some(src_dir), Some(dst_dir)) = (
      parse_direction(parse_state[2]),
      parse_direction(parse_state[3]),
    ) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    };

    self.list_move_core(
      parse_state[0],
      parse_state[1],
      src_dir,
      dst_dir,
      store,
      output,
    )
  }

  /// RPOPLPUSH source destination
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListRightPopLeftPush
  pub fn list_right_pop_left_push<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "RPOPLPUSH");

    self.list_move_core(
      parse_state[0],
      parse_state[1],
      OperationDirection::Right,
      OperationDirection::Left,
      store,
      output,
    )
  }

  /// LMOVE/RPOPLPUSH 公共体（同键同向退化为此前 C# 的 rotation/peek 语义）
  ///
  /// 对齐 C# ListOps.ListMove 语义
  pub(crate) fn list_move_core<'a, D: wdev::Device>(
    &mut self,
    src_key: &[u8],
    dst_key: &[u8],
    src_dir: OperationDirection,
    dst_dir: OperationDirection,
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let mut src = match list_load_sync(store, src_key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::WrongType => return Ok(true),
      // C# src 缺失/空 → OK + null element
      ListLoad::Missing => {
        output.write_resp_null_ver(self.resp_protocol_version);
        return Ok(true);
      }
      ListLoad::Present(o) => o,
    };
    if src.list.is_empty() {
      output.write_resp_null_ver(self.resp_protocol_version);
      return Ok(true);
    }

    let same_key = src_key == dst_key;
    if same_key && (src_dir == dst_dir || src.list.len() == 1) {
      // C# 同键同向 或 单元素：旋转为 no-op，直接窥视元素返回，严禁先 pop 再 push（否则会触发列表暂空导致 TTL 丢失）
      let item = if src_dir == OperationDirection::Right {
        src.list.back()
      } else {
        src.list.front()
      };
      match item {
        Some(item) => output.write_resp_bulk_string(item),
        None => output.write_resp_null_ver(self.resp_protocol_version),
      }
      return Ok(true);
    }

    // 异键移动：先预检目标键类型（对标 C# GET(destinationKey) WRONGTYPE 拦截，防误删源列表）
    let mut dst_loaded = ListObject::new();
    if !same_key {
      match list_load_sync(store, dst_key, output) {
        ListLoad::Degrade => return Ok(false),
        ListLoad::WrongType => return Ok(true),
        ListLoad::Missing => {}
        ListLoad::Present(o) => {
          dst_loaded = o;
        }
      }
    }

    // 弹出源端
    let popped = if src_dir == OperationDirection::Right {
      src.list.pop_back()
    } else {
      src.list.pop_front()
    };
    let Some(element) = popped else {
      output.write_resp_null_ver(self.resp_protocol_version);
      return Ok(true);
    };

    // 推入目标端（同键即源对象自身：rotation 形态，仅一次落库）
    if same_key {
      if dst_dir == OperationDirection::Left {
        src.list.push_front(element);
      } else {
        src.list.push_back(element);
      }

      match list_save_or_gc(store, src_key, &src) {
        Ok(true) => self.notify_collection_update(&**store, &**store, src_key),
        Ok(false) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
      let elem_ref = if dst_dir == OperationDirection::Left {
        src.list.front()
      } else {
        src.list.back()
      };
      if let Some(elem) = elem_ref {
        output.write_resp_bulk_string(elem);
      }
      return Ok(true);
    }

    src.update_size(&element, false);
    dst_loaded.update_size(&element, true);
    if dst_dir == OperationDirection::Left {
      dst_loaded.list.push_front(element);
    } else {
      dst_loaded.list.push_back(element);
    }

    // 源空 → 整键回收（C# EXPIRE TimeSpan.Zero）
    match list_save_or_gc(store, src_key, &src) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    match list_save_or_gc(store, dst_key, &dst_loaded) {
      Ok(true) => self.notify_collection_update(&**store, &**store, dst_key),
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }

    let elem_ref = if dst_dir == OperationDirection::Left {
      dst_loaded.list.front()
    } else {
      dst_loaded.list.back()
    };
    if let Some(elem) = elem_ref {
      output.write_resp_bulk_string(elem);
    }
    Ok(true)
  }

  /// LSET key index element
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListSet
  pub fn list_set<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "LSET");
    let key = parse_state[0];

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::WrongType => {}
      // C# NOTFOUND → ERR no such key
      ListLoad::Missing => {
        cs::write_error_raw(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
      }
      ListLoad::Present(mut obj) => {
        let mut obj_out = run_operate(
          &mut obj,
          ListOperation::Lset,
          &[parse_state[1], parse_state[2]],
          0,
          0,
          self.resp_protocol_version,
          output,
        );
        if obj_out.payload_view().first() == Some(&b'+') {
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => {}
            Ok(false) => {
              obj_out.reset();
              return Ok(false);
            }
            Err(_) => {
              obj_out.reset();
              output.write_resp_error(RESP_ERR_GENERIC);
              return Ok(true);
            }
          }
        }
        // +OK / 对象层错误负载均已直写会话输出尾段
      }
    }
    Ok(true)
  }
}
