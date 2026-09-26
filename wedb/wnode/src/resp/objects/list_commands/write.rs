//! 列表修改命令实现（LPUSH, RPUSH, LPUSHX, RPUSHX, LPOP, RPOP, LTRIM, LINSERT, LREM, LMOVE, RPOPLPUSH, LSET）

use wbase::num::strict_i32;
use wcol::list::list_object::{ListObject, ListOperation, OperationDirection};
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, parse_i32_arg},
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
  ext::RespVecExt,
};

use super::{ListLoad, Rmw, list_load_sync, list_save_or_gc, parse_i32_pair_args, run_operate};
use crate::{
  resp::{
    objects::object_store_utils::{
      obj_writeback_recheck_sync, try_sync_rmw_window_pair, write_rmw_aof_fail_frame,
      write_rmw_reply,
    },
    resp_server_session::RespServerSession,
  },
  session_parse_state_extensions::operation_direction_from_token as parse_direction,
};

impl RespServerSession {
  /// LPUSH key element [element ...] / RPUSH key element [element ...]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListPush（LPUSH/RPUSH 共体）
  pub fn list_push<'a, D: Device>(
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
  pub fn list_push_x<'a, D: Device>(
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
  fn list_push_by_op<'a, D: Device>(
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
      // 落笔前复验域归属，杜绝窗口期并发 RPUSH/DEL/SET 交错丢写
      list_windowed_load!(store, key, output, mut obj, {
        output.extend_from_slice(cs::RESP_RETURN_VAL_0);
        return Ok(true);
      });
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
      if !obj_writeback_recheck_sync(store, key, true) {
        return Ok(false);
      }
      match list_save_or_gc(store, key, &obj) {
        Ok(true) => {
          output.write_resp_int(result1);
          self.notify_collection_update(key);
        }
        Ok(false) => return Ok(false),
        Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
      }
      return Ok(true);
    }

    match self.list_rmw(store, key, op, &parse_state[1..], (0, 0), output) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType | Rmw::Missing => {}
      // C# 仅回填 result1，整数回复由 RESP 层写出（转调单源 write_rmw_reply）
      Rmw::Present(done) => {
        write_rmw_reply(done, output);
        self.notify_collection_update(key);
      }
      // AofFail：写已生效、入账失败，落错误帧拒绝（AofEnqueue 契约）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        return Ok(true);
      }
    }
    Ok(true)
  }

  /// LPOP key \[count\] / RPOP key \[count\]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListPop
  pub fn list_pop<'a, D: Device>(
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

    match self.list_rmw(store, key, op, &[], (pop_count, 0), output) {
      Rmw::Degrade => return Ok(false),
      // AofFail：弹出写已生效、入账失败，落错误帧拒绝；不回滚内存（弹出项
      // 不回插，发散可见），严禁转异步重放二次弹出（AofEnqueue 契约）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        return Ok(true);
      }
      _ => {}
    }
    Ok(true)
  }

  /// LTRIM key start stop
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListTrim
  pub fn list_trim<'a, D: Device>(
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

    // 双保护同步档（取窗单源见宏 doc）：丢写必现形——LTRIM 旧快照顶掉窗口期已 ACK 的元素
    list_windowed_load!(store, key, output, mut obj, {
      // C# NOTFOUND → OK（无对象可裁剪，仍回 OK）
      output.extend_from_slice(cs::RESP_OK);
      return Ok(true);
    });
    let mut obj_out = run_operate(
      &mut obj,
      ListOperation::Ltrim,
      &[],
      start,
      stop,
      self.resp_protocol_version,
      output,
    );
    if !obj_writeback_recheck_sync(store, key, true) {
      obj_out.reset();
      return Ok(false);
    }
    match list_save_or_gc(store, key, &obj) {
      Ok(true) => output.extend_from_slice(cs::RESP_OK),
      Ok(false) => {
        obj_out.reset();
        return Ok(false);
      }
      Err(_) => {
        obj_out.reset();
        output.write_resp_error(RESP_ERR_GENERIC);
      }
    }
    Ok(true)
  }

  /// LINSERT key BEFORE|AFTER pivot element
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListInsert
  pub fn list_insert<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 4, output, "LINSERT");
    let key = parse_state[0];

    // C# NOTFOUND → :0
    list_windowed_load!(store, key, output, mut obj, {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    });
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
      if !obj_writeback_recheck_sync(store, key, true) {
        obj_out.reset();
        return Ok(false);
      }
      match list_save_or_gc(store, key, &obj) {
        Ok(true) => self.notify_collection_update(key),
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
    Ok(true)
  }

  /// LREM key count element
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListRemove
  pub fn list_remove<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "LREM");
    let key = parse_state[0];
    let Some(count) = parse_i32_arg(parse_state[1], output) else {
      return Ok(true);
    };

    // C# NOTFOUND → :0
    list_windowed_load!(store, key, output, mut obj, {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    });
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
      if !obj_writeback_recheck_sync(store, key, true) {
        obj_out.reset();
        return Ok(false);
      }
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
    Ok(true)
  }

  /// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListMove
  pub fn list_move<'a, D: Device>(
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
  pub fn list_right_pop_left_push<'a, D: Device>(
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
  pub(crate) fn list_move_core<'a, D: Device>(
    &mut self,
    src_key: &[u8],
    dst_key: &[u8],
    src_dir: OperationDirection,
    dst_dir: OperationDirection,
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 双键装载型写臂双保护·同步档：键组桶升序单机制双窗取闩（全仓多键臂
    // 唯一取闩序，与 wtxn/EXEC 锁表天然同序，票 zcode-r135c-lockorder 案一），
    // 任一窗未取到整体走既有 Ok(false) 异步重放通道；各键落笔前按各自装载态
    // 复验域归属
    let Some(_windows) = try_sync_rmw_window_pair(store, src_key, dst_key) else {
      return Ok(false);
    };
    // C# src 缺失/空 → OK + null element
    let mut src = list_load_or_bail!(store, src_key, output, {
      output.write_resp_null_ver(self.resp_protocol_version);
      return Ok(true);
    });
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
    let mut dst_existed = false;
    if !same_key {
      match list_load_sync(store, dst_key, output) {
        ListLoad::Degrade => return Ok(false),
        ListLoad::WrongType => return Ok(true),
        ListLoad::Missing => {}
        ListLoad::Present(o) => {
          dst_loaded = o;
          dst_existed = true;
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

      if !obj_writeback_recheck_sync(store, src_key, true) {
        return Ok(false);
      }
      match list_save_or_gc(store, src_key, &src) {
        Ok(true) => self.notify_collection_update(src_key),
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

    // 写回序先目标后源（对齐 set_commands/write.rs set_move 成文纪律：本仓无
    // 事务打包机制，以倒序加重放补偿）：目标写回失败（升阶门/超页门 Ok(false)
    // 降级、IO 错误）时源零变异零写入，慢路径重放自完整初态整体执行，杜绝旧序
    // 「src 已持久弹出 → dst 门降级 → 整命令重放」的非幂等——弹空即键回收、
    // 重放 Missing 分支元素蒸发，尚余则重放弹出下一个元素、首个永久丢失且应答
    // 错位。列表侧与集合侧差异：列表 push 非幂等，目标成功而源失败时重放残留
    // 为「元素双份」（与经纪臂 collection_item_source.rs 头注自陈取舍同口径，
    // 较丢失无害且客户端重读可察；集合侧 insert 幂等自收敛），非「member 丢失」
    if !obj_writeback_recheck_sync(store, dst_key, dst_existed) {
      return Ok(false);
    }
    match list_save_or_gc(store, dst_key, &dst_loaded) {
      Ok(true) => self.notify_collection_update(dst_key),
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    // 源空 → 整键回收（C# EXPIRE TimeSpan.Zero）
    if !obj_writeback_recheck_sync(store, src_key, true) {
      return Ok(false);
    }
    match list_save_or_gc(store, src_key, &src) {
      Ok(true) => {}
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
  pub fn list_set<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "LSET");
    let key = parse_state[0];

    // C# NOTFOUND → ERR no such key
    list_windowed_load!(store, key, output, mut obj, {
      cs::write_error_raw(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
      return Ok(true);
    });
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
      if !obj_writeback_recheck_sync(store, key, true) {
        obj_out.reset();
        return Ok(false);
      }
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
    Ok(true)
  }
}
