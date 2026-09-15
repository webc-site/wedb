//! 列表命令（对标 libs/server/Resp/Objects/ListCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`wcol::list::list_object::ListObject`] 的 operate/ObjectInput
//! 通道（与 C# GarnetObjectBase.Operate 分层一致），存取经与 storage 会话域
//! 共享的 `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。
//!
//! 阻塞族（BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP）：经纪注入时登记观察者
//! 后挂起会话（pending_block），等待由网络泵 await BlockedWait 驱动
//!（C# 网络线程 BlockingWait 的 compio 挂起等价物）；未注入经纪的独立
//! 会话域（单测/脚本）保留立即可取路径。

use wbase::num::strict_i32;
use wcol::{
  itembroker::collection_item_observer::CollectionItemResult,
  list::list_object::{ListObject, ListOperation, OperationDirection},
  types::object_output::ObjectOutput,
};
use wresp::{
  RespCommand, RespSliceExt, RespVecExt, check_arg_count,
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
};
use wval::GarnetObjectType;

use crate::{
  resp::{
    objects::object_store_utils::{
      ObjLoad, RespRmwOutcome, SyncRmwCmd, SyncRmwHandlers, list_from_blob, list_to_blob,
      make_object_input, obj_load_typed_sync, obj_save_or_gc, run_sync_rmw,
    },
    resp_server_session::RespServerSession,
  },
  session_parse_state_extensions::{
    operation_direction_from_token as parse_direction, try_get_timeout_bytes,
  },
};

pub(crate) type ListLoad = ObjLoad<ListObject>;
type Rmw = RespRmwOutcome;

/// 经对象层 operate 通道执行操作，返回结构化输出
///（协议版本按会话协商版本透传，C# respProtocolVersion）
fn run_operate(
  obj: &mut ListObject,
  op: ListOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  resp_version: u8,
) -> ObjectOutput {
  let input = make_object_input(GarnetObjectType::List, op as u8, args, arg1, arg2);
  let mut obj_out = ObjectOutput::new();
  obj.operate(&input, &mut obj_out, resp_version);
  obj_out
}

/// 同步装载列表（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
#[inline]
pub(crate) fn list_load_sync(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> ListLoad {
  obj_load_typed_sync(
    store,
    key,
    GarnetObjectType::List as u8,
    output,
    list_from_blob,
  )
}

/// 变更回写：空列表整键回收（对齐 storage 层 finalize_removal 与命令域收尾）
#[inline]
pub(crate) fn list_save_or_gc(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  obj: &ListObject,
) -> wkv::Result<bool> {
  obj_save_or_gc(
    store,
    key,
    GarnetObjectType::List as u8,
    obj,
    obj.list.is_empty(),
    list_to_blob,
  )
}

/// rmw 回写判定
///
/// - 只读操作不落库；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）无状态变更，不落库（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的操作以变更计数为准；LSET 以 +OK 负载为准。
fn should_write_back(
  op: ListOperation,
  out: &ObjectOutput,
  obj: &ListObject,
  existed: bool,
) -> bool {
  if is_read_only(op)
    || out.has_wrong_type()
    || out.payload.first() == Some(&b'-')
    || (!existed && obj.list.is_empty())
  {
    return false;
  }
  match op {
    ListOperation::Lrem => out.result1 > 0,
    ListOperation::Linsert => out.result1 > 0,
    ListOperation::Lset => out.payload.first() == Some(&b'+'),
    _ => true,
  }
}

/// 只读操作（rmw 不落库）
fn is_read_only(op: ListOperation) -> bool {
  matches!(
    op,
    ListOperation::Llen | ListOperation::Lrange | ListOperation::Lindex | ListOperation::Lpos
  )
}

impl RespServerSession {
  /// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
  ///（协议版本取会话协商版本，C# respProtocolVersion）
  #[inline]
  fn list_rmw(
    &self,
    store: &wkv::BatchStoreSession<impl wdev::Device>,
    key: &[u8],
    op: ListOperation,
    args: &[&[u8]],
    args12: (i32, i32),
    output: &mut Vec<u8>,
  ) -> Rmw {
    let (arg1, arg2) = args12;
    let resp_version = self.resp_protocol_version;
    run_sync_rmw(
      store,
      SyncRmwCmd {
        key,
        tag: GarnetObjectType::List as u8,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        list_from_blob,
        ListObject::new,
        |o: &ListObject| o.list.is_empty(),
        list_to_blob,
        |obj, op, args| run_operate(obj, op, args, arg1, arg2, resp_version),
        should_write_back,
      ),
    )
  }
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
    let op = if is_left {
      ListOperation::Lpush
    } else {
      ListOperation::Rpush
    };
    self.list_push_by_op(parse_state, store, output, op, "LPUSH")
  }

  /// libs/server/Resp/Objects/ListCommands.cs:ListPushX
  ///
  /// LPUSHX/RPUSHX 共体入口（键缺失不物化空列表，回复 0）：
  pub fn list_push_x<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_left: bool,
  ) -> wresp::Result<bool> {
    let op = if is_left {
      ListOperation::Lpushx
    } else {
      ListOperation::Rpushx
    };
    self.list_push_by_op(parse_state, store, output, op, "LPUSHX")
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
    check_arg_count!(parse_state, >= 2, output, cmd_name);

    let key = parse_state[0];

    // LPUSHX/RPUSHX：键缺失不创建（对齐 GarnetObject.NeedToCreate 矩阵）
    if matches!(op, ListOperation::Lpushx | ListOperation::Rpushx) {
      match list_load_sync(store, key, output) {
        ListLoad::Degrade => return Ok(false),
        ListLoad::Error => {}
        ListLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
        ListLoad::Present(mut obj) => {
          let obj_out = run_operate(
            &mut obj,
            op,
            &parse_state[1..],
            0,
            0,
            self.resp_protocol_version,
          );
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => {
              output.write_resp_int(obj_out.result1);
              self.notify_collection_update(key);
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
      Rmw::Error => {}
      // C# 仅回填 result1，整数回复由 RESP 层写出
      Rmw::Done {
        result1,
        payload_written,
      } => {
        if !payload_written {
          output.write_resp_int(result1);
        }
        self.notify_collection_update(key);
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
    check_arg_count!(parse_state, !empty, output, "LPOP");

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

  /// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListPosition
  pub fn list_position<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 2, output, "LPOS");

    let key = parse_state[0];

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
      ListLoad::Missing => {
        // C# NOTFOUND：参数中含 COUNT → 空数组，否则 null（eq_ignore_ascii_case 本身大小写不敏感）
        let count = parse_state[2..]
          .iter()
          .any(|t| t.eq_ignore_ascii_case(b"COUNT"));
        if count {
          output.extend_from_slice(cs::RESP_EMPTYLIST);
        } else {
          output.write_resp_null();
        }
      }
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          ListOperation::Lpos,
          &parse_state[1..],
          0,
          0,
          self.resp_protocol_version,
        );
        // result1 == -1 时对象层未写负载（C# ProcessOutput + WriteNull）
        if obj_out.result1 != -1 {
          output.extend_from_slice(&obj_out.payload);
        } else {
          output.write_resp_null();
        }
      }
    }
    Ok(true)
  }

  /// LMPOP numkeys key [key ...] LEFT | RIGHT [COUNT count]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListPopMultiple
  pub fn list_pop_multiple<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 3, output, "LMPOP");

    let Some(num_keys) = parse_state[0].try_parse_i64() else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_NUMKEYS);
      return Ok(true);
    };
    if num_keys < 1 {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_NUMKEYS);
      return Ok(true);
    }
    if parse_state.len() != num_keys as usize + 2 && parse_state.len() != num_keys as usize + 4 {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    }

    let keys = &parse_state[1..=num_keys as usize];

    let Some(pop_direction) = parse_direction(parse_state[num_keys as usize + 1]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    };

    let mut pop_count = 1_i64;
    if parse_state.len() == num_keys as usize + 4 {
      if !parse_state[num_keys as usize + 2].eq_ignore_ascii_case(b"COUNT") {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      match parse_state[num_keys as usize + 3].try_parse_i64() {
        Some(c) if c >= 1 => pop_count = c,
        _ => {
          cs::abort_with_error_message(output, "ERR count should be greater than 0");
          return Ok(true);
        }
      }
    }

    // 逐键弹出第一个非空列表（LMPOP/BLMPOP 立即可取路径公共体复用）
    if let Some(done) = pop_first_nonempty(
      keys,
      store,
      pop_direction,
      pop_count as i32,
      self.resp_protocol_version,
      output,
    ) {
      return Ok(done);
    }

    // C# NOTFOUND → WriteNullArray
    output.extend_from_slice(b"*-1\r\n");
    Ok(true)
  }

  /// BLPOP key [key ...] timeout / BRPOP key [key ...] timeout
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListBlockingPop
  ///
  /// 经纪注入时登记观察者并挂起会话（等待由网络泵 await BlockedWait 驱动，
  /// C# 网络线程 BlockingWait 的 compio 挂起等价物）；未注入经纪的独立会话
  /// 域保留立即可取路径
  pub fn list_blocking_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_left: bool,
  ) -> wresp::Result<bool> {
    let command = if is_left {
      RespCommand::Blpop
    } else {
      RespCommand::Brpop
    };
    let cmd_name = if is_left { "BLPOP" } else { "BRPOP" };
    check_arg_count!(parse_state, >= 2, output, cmd_name);

    let timeout = match try_get_timeout_bytes(parse_state[parse_state.len() - 1]) {
      Ok(timeout) => timeout,
      Err(error) => {
        cs::abort_with_error_message(output, error);
        return Ok(true);
      }
    };

    // 经纪挂起路径（C# GetCollectionItemAsync；立即试取由经纪主循环
    // InitializeObserver 承担，FIFO 与 WRONGTYPE 语义同 C#）
    if self.park_broker_wait(
      command,
      timeout,
      || {
        parse_state[..parse_state.len() - 1]
          .iter()
          .map(|k| k.to_vec())
          .collect()
      },
      Vec::new,
    ) {
      return Ok(true);
    }

    // ---- 立即可取路径（经纪未注入的独立会话域）----
    for key in &parse_state[..parse_state.len() - 1] {
      let mut obj = match list_load_sync(store, key, output) {
        ListLoad::Degrade => return Ok(false),
        ListLoad::Error => return Ok(true),
        ListLoad::Missing => continue,
        ListLoad::Present(o) => o,
      };

      let item = if is_left {
        obj.list.pop_front()
      } else {
        obj.list.pop_back()
      };
      let Some(item) = item else {
        continue;
      };
      obj.update_size(&item, false);

      match list_save_or_gc(store, key, &obj) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }

      // 回复：[key, item]
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      output.write_resp_bulk_string(&item);
      return Ok(true);
    }

    // C# !result.Found → WriteNullArray
    output.extend_from_slice(b"*-1\r\n");
    Ok(true)
  }

  /// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListBlockingMove
  ///
  /// 经纪注入时挂起等待（cmd_args = [dstKey, srcDir(1B), dstDir(1B)]，
  /// C# ListBlockingMove(srcKey, dstKey, srcDir, dstDir, timeout)）；未注入
  /// 经纪保留立即可取路径
  pub fn list_blocking_move<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 5, output, "BLMOVE");

    let src_key = parse_state[0];
    let dst_key = parse_state[1];

    let (Some(src_dir), Some(dst_dir)) = (
      parse_direction(parse_state[2]),
      parse_direction(parse_state[3]),
    ) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    };

    let timeout = match try_get_timeout_bytes(parse_state[4]) {
      Ok(timeout) => timeout,
      Err(error) => {
        cs::abort_with_error_message(output, error);
        return Ok(true);
      }
    };

    // 经纪挂起路径：方向编码进 cmd_args（C# PinnedSpanByte 单字节指针形态）
    if self.park_broker_wait(
      RespCommand::Blmove,
      timeout,
      || vec![src_key.to_vec()],
      || vec![dst_key.to_vec(), vec![src_dir as u8], vec![dst_dir as u8]],
    ) {
      return Ok(true);
    }

    self.list_move_core(src_key, dst_key, src_dir, dst_dir, store, output)
  }

  /// BRPOPLPUSH source destination timeout
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListBlockingPopPush
  ///
  /// 同 BLMOVE 的 Right→Left 定式（C# ListBlockingMove(Right, Left)）
  pub fn list_blocking_pop_push<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "BRPOPLPUSH");

    let timeout = match try_get_timeout_bytes(parse_state[2]) {
      Ok(timeout) => timeout,
      Err(error) => {
        cs::abort_with_error_message(output, error);
        return Ok(true);
      }
    };

    // 经纪挂起路径：Right→Left 定式编码（C# ListBlockingMove 定式同源）
    if self.park_broker_wait(
      RespCommand::Blmove,
      timeout,
      || vec![parse_state[0].to_vec()],
      || {
        vec![
          parse_state[1].to_vec(),
          vec![OperationDirection::Right as u8],
          vec![OperationDirection::Left as u8],
        ]
      },
    ) {
      return Ok(true);
    }

    self.list_move_core(
      parse_state[0],
      parse_state[1],
      OperationDirection::Right,
      OperationDirection::Left,
      store,
      output,
    )
  }

  /// LLEN key
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListLength
  pub fn list_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1, output, "LLEN");
    let key = parse_state[0];
    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
      // C# NOTFOUND → :0
      ListLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          ListOperation::Llen,
          &[],
          0,
          0,
          self.resp_protocol_version,
        );
        output.write_resp_int(obj_out.result1);
      }
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
    check_arg_count!(parse_state, 3, output, "LTRIM");
    let key = parse_state[0];
    // C#：start/end 非整数报错
    let (Some(start), Some(stop)) = (strict_i32(parse_state[1]), strict_i32(parse_state[2])) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
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

  /// LRANGE key start stop
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListRange
  pub fn list_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "LRANGE");
    let key = parse_state[0];
    let (Some(start), Some(stop)) = (strict_i32(parse_state[1]), strict_i32(parse_state[2])) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
      // C# NOTFOUND → RESP_EMPTYLIST
      ListLoad::Missing => output.extend_from_slice(cs::RESP_EMPTYLIST),
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          ListOperation::Lrange,
          &[],
          start,
          stop,
          self.resp_protocol_version,
        );
        output.extend_from_slice(&obj_out.payload);
      }
    }
    Ok(true)
  }

  /// LINDEX key index
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListIndex
  pub fn list_index<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "LINDEX");
    let key = parse_state[0];
    let Some(index) = strict_i32(parse_state[1]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
      // C# NOTFOUND → null
      ListLoad::Missing => output.write_resp_null(),
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          ListOperation::Lindex,
          &[],
          index,
          0,
          self.resp_protocol_version,
        );
        if obj_out.result1 == -1 {
          output.write_resp_null();
        } else {
          output.extend_from_slice(&obj_out.payload);
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
      ListLoad::Error => {}
      // C# NOTFOUND → :0
      ListLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          ListOperation::Linsert,
          &parse_state[1..],
          0,
          0,
          self.resp_protocol_version,
        );
        if obj_out.result1 > 0 {
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => self.notify_collection_update(key),
            Ok(false) => return Ok(false),
            Err(_) => {
              output.write_resp_error(RESP_ERR_GENERIC);
              return Ok(true);
            }
          }
        }
        // C# 仅回填 result1（pivot 缺失 → -1），整数回复由 RESP 层写出
        output.write_resp_int(obj_out.result1);
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
      ListLoad::Error => {}
      // C# NOTFOUND → :0
      ListLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          ListOperation::Lrem,
          &[parse_state[2]],
          count,
          0,
          self.resp_protocol_version,
        );
        if obj_out.result1 > 0 {
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(_) => {
              output.write_resp_error(RESP_ERR_GENERIC);
              return Ok(true);
            }
          }
        }
        // C# 仅回填 result1，整数回复由 RESP 层写出
        output.write_resp_int(obj_out.result1);
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
  fn list_move_core<'a, D: wdev::Device>(
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
      ListLoad::Error => return Ok(true),
      // C# src 缺失/空 → OK + null element
      ListLoad::Missing => {
        output.write_resp_null();
        return Ok(true);
      }
      ListLoad::Present(o) => o,
    };
    if src.list.is_empty() {
      output.write_resp_null();
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
        None => output.write_resp_null(),
      }
      return Ok(true);
    }

    // 异键移动：先预检目标键类型（对标 C# GET(destinationKey) WRONGTYPE 拦截，防误删源列表）
    let mut dst_loaded = ListObject::new();
    if !same_key {
      match list_load_sync(store, dst_key, output) {
        ListLoad::Degrade => return Ok(false),
        ListLoad::Error => return Ok(true),
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
      output.write_resp_null();
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
      Ok(true) => self.notify_collection_update(dst_key),
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
      ListLoad::Error => {}
      // C# NOTFOUND → ERR no such key
      ListLoad::Missing => {
        cs::write_error_raw(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
      }
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          ListOperation::Lset,
          &[parse_state[1], parse_state[2]],
          0,
          0,
          self.resp_protocol_version,
        );
        if obj_out.payload.first() == Some(&b'+') {
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(_) => {
              output.write_resp_error(RESP_ERR_GENERIC);
              return Ok(true);
            }
          }
        }
        output.extend_from_slice(&obj_out.payload);
      }
    }
    Ok(true)
  }

  /// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListBlockingPopMultiple
  ///
  /// 经纪注入时挂起等待（cmd_args = [popDir(1B), popCount(i32 LE 4B)]，
  /// C# ListBlockingPopMultiple 同编码）；未注入经纪保留立即可取路径
  pub fn list_blocking_pop_multiple<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 4, output, "BLMPOP");

    let timeout = match try_get_timeout_bytes(parse_state[0]) {
      Ok(timeout) => timeout,
      Err(error) => {
        cs::abort_with_error_message(output, error);
        return Ok(true);
      }
    };

    let Some(num_keys) = parse_state[1].try_parse_i64() else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_NUMKEYS);
      return Ok(true);
    };
    if num_keys < 1 {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_NUMKEYS);
      return Ok(true);
    }
    if parse_state.len() != num_keys as usize + 3 && parse_state.len() != num_keys as usize + 5 {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    }

    let keys: Vec<&[u8]> = parse_state[2..=num_keys as usize + 1].to_vec();

    let Some(pop_direction) = parse_direction(parse_state[num_keys as usize + 2]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    };

    let mut pop_count = 1_i64;
    if parse_state.len() == num_keys as usize + 5 {
      if !parse_state[num_keys as usize + 3].eq_ignore_ascii_case(b"COUNT") {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      match parse_state[num_keys as usize + 4].try_parse_i64() {
        Some(c) if c >= 1 => pop_count = c,
        _ => {
          cs::abort_with_error_message(output, "ERR count should be greater than 0");
          return Ok(true);
        }
      }
    }

    // 经纪挂起路径（方向 + count 编码进 cmd_args）
    if self.park_broker_wait(
      RespCommand::Blmpop,
      timeout,
      || keys.iter().map(|k| k.to_vec()).collect(),
      || {
        vec![
          vec![pop_direction as u8],
          (pop_count as i32).to_le_bytes().to_vec(),
        ]
      },
    ) {
      return Ok(true);
    }

    // ---- 立即可取路径（经纪未注入的独立会话域）----
    if let Some(done) = pop_first_nonempty(
      &keys,
      store,
      pop_direction,
      pop_count as i32,
      self.resp_protocol_version,
      output,
    ) {
      return Ok(done);
    }

    // C# !result.Found → WriteNull
    output.write_resp_null();
    Ok(true)
  }
}

/// 阻塞命令完成后的应答写出（单函数承接 C# 六个阻塞命令尾部
/// BlockingWait 之后的 switch 应答段：ListBlockingPop / ListBlockingMove /
/// ListBlockingPopPush / ListBlockingPopMultiple / SortedSetBlockingPop /
/// SortedSetBlockingMPop）
///
/// - 强制解除（CLIENT UNBLOCK）→ UNBLOCKED 错误行；
/// - 类型不符 → WRONGTYPE；
/// - 未取到 → BLPOP/BRPOP 空数组 `*-1`，其余空值 `$-1`（C# WriteNullArray
///   / WriteNull 均不随 RESP 版本变形）；
/// - 取到 → 按命令族帧型展开。
pub(crate) fn write_collection_item_result(
  cmd: RespCommand,
  result: &CollectionItemResult,
  output: &mut Vec<u8>,
) {
  if result.is_force_unblocked {
    cs::write_error_raw(output, cs::RESP_UNBLOCKED_CLIENT_VIA_CLIENT_UNBLOCK);
    return;
  }
  if result.is_type_mismatch {
    cs::write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
    return;
  }

  // 借用展开：免 key/items/scores 的逐份 clone
  let Some(key) = &result.key else {
    // 未取到项：BLPOP/BRPOP 空数组，其余空值
    if matches!(cmd, RespCommand::Blpop | RespCommand::Brpop) {
      output.extend_from_slice(b"*-1\r\n");
    } else {
      output.extend_from_slice(b"$-1\r\n");
    }
    return;
  };

  match cmd {
    RespCommand::Blpop | RespCommand::Brpop => {
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      output.write_resp_bulk_string(result.item.as_deref().unwrap_or(&[]));
    }
    RespCommand::Blmove => {
      output.write_resp_bulk_string(result.item.as_deref().unwrap_or(&[]));
    }
    RespCommand::Blmpop => {
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      let items = result.items.as_deref().unwrap_or(&[]);
      output.write_resp_array_len(items.len());
      for item in items {
        output.write_resp_bulk_string(item);
      }
    }
    RespCommand::Bzpopmin | RespCommand::Bzpopmax => {
      output.write_resp_array_len(3);
      output.write_resp_bulk_string(key);
      output.write_resp_bulk_string(result.item.as_deref().unwrap_or(&[]));
      let s = ObjectOutput::format_double(result.score.unwrap_or_default());
      output.write_resp_bulk_string(s.as_bytes());
    }
    RespCommand::Bzmpop => {
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      let items = result.items.as_deref().unwrap_or(&[]);
      let scores = result.scores.as_deref().unwrap_or(&[]);
      output.write_resp_array_len(items.len());
      for (i, item) in items.iter().enumerate() {
        output.write_resp_array_len(2);
        output.write_resp_bulk_string(item);
        let s = ObjectOutput::format_double(scores.get(i).copied().unwrap_or_default());
        output.write_resp_bulk_string(s.as_bytes());
      }
    }
    _ => {}
  }
}

/// 逐键弹出第一个非空列表并写回复（LMPOP/BLMPOP 立即可取路径公共体）
///
/// 返回 Some(true) 已命中并写回复；Some(false) 磁盘候选降级 / 回写降级；
/// None 全部键缺失或空列表（调用方写未取到应答）
fn pop_first_nonempty(
  keys: &[&[u8]],
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  pop_direction: OperationDirection,
  pop_count: i32,
  _resp_version: u8,
  output: &mut Vec<u8>,
) -> Option<bool> {
  let is_left = pop_direction == OperationDirection::Left;
  for key in keys {
    let mut obj = match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Some(false),
      ListLoad::Error => return Some(true),
      ListLoad::Missing => continue,
      ListLoad::Present(o) => o,
    };
    if obj.list.is_empty() {
      continue;
    }

    let count = (pop_count as usize).min(obj.list.len());
    let mut popped = Vec::with_capacity(count);
    for _ in 0..count {
      let item = if is_left {
        obj.list.pop_front()
      } else {
        obj.list.pop_back()
      };
      if let Some(item) = item {
        obj.update_size(&item, false);
        popped.push(item);
      } else {
        break;
      }
    }

    match list_save_or_gc(store, key, &obj) {
      Ok(true) => {}
      Ok(false) => return Some(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Some(true);
      }
    }

    // 回复：[key, [element, ...]]
    output.write_resp_array_len(2);
    output.write_resp_bulk_string(key);
    output.write_resp_array_len(popped.len());
    for element in &popped {
      output.write_resp_bulk_string(element);
    }
    return Some(true);
  }
  None
}
