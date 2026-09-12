//! 列表命令（对标 libs/server/Resp/Objects/ListCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`crate::objects::list::list_object::ListObject`] 的 operate/ObjectInput
//! 通道（与 C# GarnetObjectBase.Operate 分层一致），存取经与 storage 会话域
//! 共享的 `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。
//!
//! 阻塞族（BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP）刻意差异：本层仅实现
//! "立即可取"路径（等价 timeout=0 立即返回）；真正的阻塞等待由
//! CollectionItemBroker 承担（见 objects/itembroker 与汇报接线项）。

use wresp::{RespSliceExt, RespVecExt, cmd_strings as cs};

use crate::{
  objects::{
    list::list_object::{ListObject, ListOperation, OperationDirection},
    parse_utils::try_get_int,
    types::object_output::ObjectOutput,
  },
  resp::{
    objects::object_store_utils::{
      ObjLoad, RmwOutcome, SyncRmwCmd, SyncRmwHandlers, list_from_blob, list_to_blob,
      make_object_input, obj_load_typed_sync, obj_save_or_gc, run_sync_rmw,
    },
    resp_server_session::RespServerSession,
  },
  types::GarnetObjectType,
};

/// 本命令面统一按 RESP2 协议输出（C# respProtocolVersion 由会话下发，
/// 会话层接线时替换为实际协商版本）
const RESP_VERSION: u8 = 2;

pub(crate) type ListLoad = ObjLoad<ListObject>;
type Rmw = RmwOutcome;

/// 经对象层 operate 通道执行操作，返回结构化输出
fn run_operate(
  obj: &mut ListObject,
  op: ListOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
) -> ObjectOutput {
  let input = make_object_input(GarnetObjectType::List, op as u8, args, arg1, arg2);
  let mut obj_out = ObjectOutput::new();
  obj.operate(&input, &mut obj_out, RESP_VERSION);
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

/// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
#[inline]
fn rmw(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  op: ListOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  output: &mut Vec<u8>,
) -> Rmw {
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
      |obj, op, args| run_operate(obj, op, args, arg1, arg2),
      should_write_back,
    ),
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

use crate::session_parse_state_extensions::operation_direction_from_token as parse_direction;

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
    let op = if is_left {
      ListOperation::Lpush
    } else {
      ListOperation::Rpush
    };
    self.list_push_by_op(parse_state, store, output, op, "LPUSH")
  }

  /// LPUSHX key element [element ...] / RPUSHX key element [element ...]
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
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, cmd_name);
      return Ok(true);
    }

    let key = parse_state[0];

    // LPUSHX/RPUSHX：键缺失不创建（对齐 GarnetObject.NeedToCreate 矩阵）
    if matches!(op, ListOperation::Lpushx | ListOperation::Rpushx) {
      match list_load_sync(store, key, output) {
        ListLoad::Degrade => return Ok(false),
        ListLoad::Error => {}
        ListLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
        ListLoad::Present(mut obj) => {
          let obj_out = run_operate(&mut obj, op, &parse_state[1..], 0, 0);
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => output.write_resp_int(obj_out.result1),
            Ok(false) => return Ok(false),
            Err(_) => output.write_resp_error("generic error"),
          }
        }
      }
      return Ok(true);
    }

    match rmw(store, key, op, &parse_state[1..], 0, 0, output) {
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
    if parse_state.is_empty() {
      cs::abort_with_wrong_number_of_arguments(output, "LPOP");
      return Ok(true);
    }

    let key = parse_state[0];

    // Read count
    let pop_count = if parse_state.len() == 2 {
      match try_get_int(parse_state[1]) {
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

    if let Rmw::Degrade = rmw(store, key, op, &[], pop_count, 0, output) {
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
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "LPOS");
      return Ok(true);
    }

    let key = parse_state[0];

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
      ListLoad::Missing => {
        // C# NOTFOUND：参数中含 COUNT → 空数组，否则 null
        let count = parse_state[2..]
          .iter()
          .any(|t| t.eq_ignore_ascii_case(b"COUNT") || t.eq_ignore_ascii_case(b"count"));
        if count {
          output.extend_from_slice(cs::RESP_EMPTYLIST);
        } else {
          output.write_resp_null();
        }
      }
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, ListOperation::Lpos, &parse_state[1..], 0, 0);
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
    if parse_state.len() < 3 {
      cs::abort_with_wrong_number_of_arguments(output, "LMPOP");
      return Ok(true);
    }

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

    // 逐键弹出第一个非空列表
    for key in keys {
      let mut obj = match list_load_sync(store, key, output) {
        ListLoad::Degrade => return Ok(false),
        ListLoad::Error => return Ok(true),
        ListLoad::Missing => continue,
        ListLoad::Present(o) => o,
      };
      if obj.list.is_empty() {
        continue;
      }

      let op = if pop_direction == OperationDirection::Left {
        ListOperation::Lpop
      } else {
        ListOperation::Rpop
      };
      let obj_out = run_operate(&mut obj, op, &[], pop_count as i32, 0);
      match list_save_or_gc(store, key, &obj) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }

      // 回复：[key, [element, ...]]
      let elements = parse_bulk_payload(&obj_out.payload);
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      output.write_resp_array_len(elements.len());
      for element in elements {
        output.write_resp_bulk_string(&element);
      }
      return Ok(true);
    }

    // C# NOTFOUND → WriteNullArray
    output.extend_from_slice(b"*-1\r\n");
    Ok(true)
  }

  /// BLPOP key [key ...] timeout / BRPOP key [key ...] timeout
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListBlockingPop
  ///
  /// 刻意差异：仅实现立即可取路径（等价 timeout=0 立即返回）；
  /// 真正的阻塞等待由 CollectionItemBroker 承担
  pub fn list_blocking_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_left: bool,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "BLPOP");
      return Ok(true);
    }

    if str::from_utf8(parse_state[parse_state.len() - 1])
      .unwrap_or("")
      .parse::<f64>()
      .is_err()
    {
      cs::abort_with_error_message(output, cs::RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
      return Ok(true);
    }

    for key in &parse_state[..parse_state.len() - 1] {
      let mut obj = match list_load_sync(store, key, output) {
        ListLoad::Degrade => return Ok(false),
        ListLoad::Error => return Ok(true),
        ListLoad::Missing => continue,
        ListLoad::Present(o) => o,
      };

      let op = if is_left {
        ListOperation::Lpop
      } else {
        ListOperation::Rpop
      };
      let obj_out = run_operate(&mut obj, op, &[], 1, 0);
      if obj_out.result1 <= 0 {
        continue;
      }

      match list_save_or_gc(store, key, &obj) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }

      // 回复：[key, item]
      let elements = parse_bulk_payload(&obj_out.payload);
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      if let Some(item) = elements.first() {
        output.write_resp_bulk_string(item);
      }
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
  /// 刻意差异：仅实现立即可取路径；阻塞等待由 CollectionItemBroker 承担
  pub fn list_blocking_move<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 5 {
      cs::abort_with_wrong_number_of_arguments(output, "BLMOVE");
      return Ok(true);
    }

    let src_key = parse_state[0];
    let dst_key = parse_state[1];

    let (Some(src_dir), Some(dst_dir)) = (
      parse_direction(parse_state[2]),
      parse_direction(parse_state[3]),
    ) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    };

    if str::from_utf8(parse_state[4])
      .unwrap_or("")
      .parse::<f64>()
      .is_err()
    {
      cs::abort_with_error_message(output, cs::RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
      return Ok(true);
    }

    self.list_move_core(src_key, dst_key, src_dir, dst_dir, store, output)
  }

  /// BRPOPLPUSH source destination timeout
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListBlockingPopPush
  ///
  /// 刻意差异：仅实现立即可取路径；阻塞等待由 CollectionItemBroker 承担
  pub fn list_blocking_pop_push<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      cs::abort_with_wrong_number_of_arguments(output, "BRPOPLPUSH");
      return Ok(true);
    }

    if str::from_utf8(parse_state[2])
      .unwrap_or("")
      .parse::<f64>()
      .is_err()
    {
      cs::abort_with_error_message(output, cs::RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
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
    if parse_state.len() != 1 {
      cs::abort_with_wrong_number_of_arguments(output, "LLEN");
      return Ok(true);
    }
    let key = parse_state[0];
    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
      // C# NOTFOUND → :0
      ListLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, ListOperation::Llen, &[], 0, 0);
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
    if parse_state.len() != 3 {
      cs::abort_with_wrong_number_of_arguments(output, "LTRIM");
      return Ok(true);
    }
    let key = parse_state[0];
    // C#：start/end 非整数报错
    let (Some(start), Some(stop)) = (try_get_int(parse_state[1]), try_get_int(parse_state[2]))
    else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
      // C# NOTFOUND → OK（无对象可裁剪，仍回 OK）
      ListLoad::Missing => output.extend_from_slice(cs::RESP_OK),
      ListLoad::Present(mut obj) => {
        run_operate(&mut obj, ListOperation::Ltrim, &[], start, stop);
        match list_save_or_gc(store, key, &obj) {
          Ok(true) => output.extend_from_slice(cs::RESP_OK),
          Ok(false) => return Ok(false),
          Err(_) => output.write_resp_error("generic error"),
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
    if parse_state.len() != 3 {
      cs::abort_with_wrong_number_of_arguments(output, "LRANGE");
      return Ok(true);
    }
    let key = parse_state[0];
    let (Some(start), Some(stop)) = (try_get_int(parse_state[1]), try_get_int(parse_state[2]))
    else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
      // C# NOTFOUND → RESP_EMPTYLIST
      ListLoad::Missing => output.extend_from_slice(cs::RESP_EMPTYLIST),
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, ListOperation::Lrange, &[], start, stop);
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
    if parse_state.len() != 2 {
      cs::abort_with_wrong_number_of_arguments(output, "LINDEX");
      return Ok(true);
    }
    let key = parse_state[0];
    let Some(index) = try_get_int(parse_state[1]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
      // C# NOTFOUND → null
      ListLoad::Missing => output.write_resp_null(),
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, ListOperation::Lindex, &[], index, 0);
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
    if parse_state.len() != 4 {
      cs::abort_with_wrong_number_of_arguments(output, "LINSERT");
      return Ok(true);
    }
    let key = parse_state[0];

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
      // C# NOTFOUND → :0
      ListLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, ListOperation::Linsert, &parse_state[1..], 0, 0);
        if obj_out.result1 > 0 {
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(_) => {
              output.write_resp_error("generic error");
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
    if parse_state.len() != 3 {
      cs::abort_with_wrong_number_of_arguments(output, "LREM");
      return Ok(true);
    }
    let key = parse_state[0];
    let Some(count) = try_get_int(parse_state[1]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::Error => {}
      // C# NOTFOUND → :0
      ListLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      ListLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, ListOperation::Lrem, &[parse_state[2]], count, 0);
        if obj_out.result1 > 0 {
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(_) => {
              output.write_resp_error("generic error");
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
    if parse_state.len() != 4 {
      cs::abort_with_wrong_number_of_arguments(output, "LMOVE");
      return Ok(true);
    }

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
    if parse_state.len() != 2 {
      cs::abort_with_wrong_number_of_arguments(output, "RPOPLPUSH");
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
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
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
        output.write_resp_error("generic error");
        return Ok(true);
      }
    }
    match list_save_or_gc(store, dst_key, &dst_loaded) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
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
    if parse_state.len() != 3 {
      cs::abort_with_wrong_number_of_arguments(output, "LSET");
      return Ok(true);
    }
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
        );
        if obj_out.payload.first() == Some(&b'+') {
          match list_save_or_gc(store, key, &obj) {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(_) => {
              output.write_resp_error("generic error");
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
  /// 刻意差异：仅实现立即可取路径；阻塞等待由 CollectionItemBroker 承担
  pub fn list_blocking_pop_multiple<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 4 {
      cs::abort_with_wrong_number_of_arguments(output, "BLMPOP");
      return Ok(true);
    }

    if str::from_utf8(parse_state[0])
      .unwrap_or("")
      .parse::<f64>()
      .is_err()
    {
      cs::abort_with_error_message(output, cs::RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
      return Ok(true);
    }

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

    for key in keys {
      let mut obj = match list_load_sync(store, key, output) {
        ListLoad::Degrade => return Ok(false),
        ListLoad::Error => return Ok(true),
        ListLoad::Missing => continue,
        ListLoad::Present(o) => o,
      };
      if obj.list.is_empty() {
        continue;
      }

      let op = if pop_direction == OperationDirection::Left {
        ListOperation::Lpop
      } else {
        ListOperation::Rpop
      };
      let obj_out = run_operate(&mut obj, op, &[], pop_count as i32, 0);
      match list_save_or_gc(store, key, &obj) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }

      // 回复：[key, [element, ...]]
      let elements = parse_bulk_payload(&obj_out.payload);
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      output.write_resp_array_len(elements.len());
      for element in elements {
        output.write_resp_bulk_string(&element);
      }
      return Ok(true);
    }

    // C# !result.Found → WriteNull
    output.write_resp_null();
    Ok(true)
  }
}

/// 对象层产出的 bulk string 序列负载 → 元素数组（LMPOP/BLPOP 族回读）
fn parse_bulk_payload(payload: &[u8]) -> Vec<Vec<u8>> {
  let mut items = Vec::new();
  let mut pos = 0;

  // 跳过外层数组头 *<n>\r\n
  if payload.first() == Some(&b'*')
    && let Some(line_end) = payload[pos..]
      .iter()
      .position(|&b| b == b'\n')
      .map(|p| p + pos)
  {
    pos = line_end + 1;
  }

  while pos < payload.len() {
    if payload[pos] != b'$' {
      break;
    }
    let Some(line_end) = payload[pos..]
      .iter()
      .position(|&b| b == b'\n')
      .map(|p| p + pos)
    else {
      break;
    };
    let Ok(len) = str::from_utf8(&payload[pos + 1..line_end - 1])
      .unwrap_or("")
      .parse::<usize>()
    else {
      break;
    };
    let start = line_end + 1;
    let end = start + len;
    if end + 2 > payload.len() {
      break;
    }
    items.push(payload[start..end].to_vec());
    pos = end + 2;
  }
  items
}
