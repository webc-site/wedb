//! 列表命令（对标 libs/server/Resp/Objects/ListCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`crate::objects::list::list_object::ListObject`] 的 operate/ObjectInput
//! 通道（与 C# GarnetObjectBase.Operate 分层一致），存取经与 storage 会话域
//! 共享的 `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]），
//! 载荷为 wobject bitcode `Vec<u8>` 数组。
//!
//! 阻塞族（BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP）刻意差异：本层仅实现
//! "立即可取"路径（等价 timeout=0 立即返回）；真正的阻塞等待由
//! CollectionItemBroker 承担（见 objects/itembroker 与汇报接线项）。

use std::io::Cursor;

use wobject::list::list_object::ListObject as WoListObject;

use crate::{
  arg_slice::ArgSlice,
  input_header::RespInputHeader,
  inputs::ObjectInput,
  objects::{
    list::list_object::{ListObject, ListOperation, OperationDirection},
    parse_utils::try_get_int,
    types::object_output::ObjectOutput,
  },
  resp::{
    cmd_strings as cs,
    cmd_strings::write_error_raw,
    objects::object_store_utils::{OBJ_TAG_LIST, SyncObj, obj_load_sync, obj_save_or_gc_sync},
    parser::resp_ext::{RespSliceExt, RespVecExt},
    resp_server_session::RespServerSession,
  },
  session_parse_state::SessionParseState,
  types::{GarnetObjectType, RespInputFlags},
};

/// 本命令面统一按 RESP2 协议输出（C# respProtocolVersion 由会话下发，
/// 会话层接线时替换为实际协商版本）
const RESP_VERSION: u8 = 2;

/// 从 wkv 信封载荷装载列表对象
///
/// 载荷双格式：默认 wobject bitcode（与 storage 会话域兼容）；C# BinaryWriter
/// 线格式（count 前缀 + 定长条目）作回退
pub(crate) fn list_from_blob(raw: &[u8]) -> ListObject {
  if let Ok(wo) = WoListObject::deserialize(&mut Cursor::new(raw)) {
    let list = wo.list.lock();
    return ListObject::from_items(list.iter().cloned().collect());
  }
  ListObject::deserialize(&mut Cursor::new(raw)).unwrap_or_default()
}

/// 序列化回 wkv 信封载荷
pub(crate) fn list_to_blob(obj: &ListObject) -> Vec<u8> {
  let wo = WoListObject::new();
  {
    let mut list = wo.list.lock();
    *list = obj.to_items().into();
  }
  let mut out = Vec::new();
  if wo.serialize(&mut out).is_err() {
    for item in obj.to_items() {
      out.extend_from_slice(&(item.len() as u32).to_le_bytes());
      out.extend_from_slice(&item);
    }
  }
  out
}

/// 构造 ObjectInput（backing 与 input 同生命周期存活）
fn make_input(
  op: ListOperation,
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

  let mut header = RespInputHeader::new_with_type(GarnetObjectType::List, RespInputFlags::empty());
  header.set_sub_id(op as u8);
  (
    ObjectInput::new_with_state(header, &mut parse_state, arg1, arg2),
    backing,
  )
}

/// 经对象层 operate 通道执行操作，返回结构化输出
fn run_operate(
  obj: &mut ListObject,
  op: ListOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
) -> ObjectOutput {
  let (input, _backing) = make_input(op, args, arg1, arg2);
  let mut obj_out = ObjectOutput::new();
  obj.operate(&input, &mut obj_out, RESP_VERSION);
  obj_out
}

/// 列表键同步装载结果
pub(crate) enum ListLoad {
  /// 磁盘候选：命令须降级异步重放（未写任何输出）
  Degrade,
  /// WrongType / 存储错误（错误行已写入输出）
  Error,
  /// 键缺失（可按空对象求值，但不得落库创建）
  Missing,
  /// 命中（信封载荷已解码）
  Present(ListObject),
}

/// 同步装载列表（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
pub(crate) fn list_load_sync(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> ListLoad {
  match obj_load_sync(store, key, OBJ_TAG_LIST) {
    Ok(None) => ListLoad::Degrade,
    Ok(Some(SyncObj::Missing)) => ListLoad::Missing,
    Ok(Some(SyncObj::WrongType)) => {
      write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
      ListLoad::Error
    }
    Ok(Some(SyncObj::Present(p))) => ListLoad::Present(list_from_blob(&p)),
    Err(_) => {
      output.write_resp_error("generic error");
      ListLoad::Error
    }
  }
}

/// 变更回写：空列表整键回收（对齐 storage 层 finalize_removal 与命令域收尾）
///
/// 返回 `Ok(false)` 表示磁盘侧须降级异步重放；`Err(())` 为存储层错误（由调用方写错误行）
pub(crate) fn list_save_or_gc(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  obj: &ListObject,
) -> Result<bool, ()> {
  let payload = list_to_blob(obj);
  obj_save_or_gc_sync(store, key, OBJ_TAG_LIST, &payload, obj.list.is_empty()).map_err(|_| ())
}

/// rmw 结果
enum Rmw {
  /// 磁盘候选降级（未写任何输出）
  Degrade,
  /// 错误行已写出，调用方不得追加回复
  Error,
  /// 已闭环：RESP 负载已随 rmw 写出；payload_written=false 时 result1 供调用方回执
  Done { result1: i64, payload_written: bool },
}

/// 读-改-写骨架：装载 → operate → 变更回写 → 负载输出
fn rmw(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  op: ListOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  output: &mut Vec<u8>,
) -> Rmw {
  let (mut obj, existed) = match list_load_sync(store, key, output) {
    ListLoad::Degrade => return Rmw::Degrade,
    ListLoad::Error => return Rmw::Error,
    ListLoad::Missing => (ListObject::new(), false),
    ListLoad::Present(o) => (o, true),
  };

  let obj_out = run_operate(&mut obj, op, args, arg1, arg2);
  let result1 = obj_out.result1;

  // 回写须先于回复输出：降级时保持输出零污染，交由异步重放整体重写
  if should_write_back(op, &obj_out, &obj, existed) {
    match list_save_or_gc(store, key, &obj) {
      Ok(true) => {}
      Ok(false) => return Rmw::Degrade,
      Err(()) => {
        output.write_resp_error("generic error");
        return Rmw::Error;
      }
    }
  }
  output.extend_from_slice(&obj_out.payload);

  Rmw::Done {
    result1,
    payload_written: !obj_out.payload.is_empty(),
  }
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
            Err(()) => output.write_resp_error("generic error"),
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

  /// LPOP key [count] / RPOP key [count]
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
      cs::abort_with_error_message(output, "ERR numkeys should be greater than 0");
      return Ok(true);
    };
    if num_keys < 1 {
      cs::abort_with_error_message(output, "ERR numkeys should be greater than 0");
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
        Err(()) => {
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
        Err(()) => {
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
        let obj_out = run_operate(&mut obj, ListOperation::Ltrim, &[], start, stop);
        match list_save_or_gc(store, key, &obj) {
          Ok(true) => output.extend_from_slice(cs::RESP_OK),
          Ok(false) => return Ok(false),
          Err(()) => output.write_resp_error("generic error"),
        }
        let _ = obj_out;
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
            Err(()) => {
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
            Err(()) => {
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
    if same_key && src_dir == dst_dir {
      // C# 同键同向：仅窥视不移动
      let item = if src_dir == OperationDirection::Right {
        src.list.back().cloned()
      } else {
        src.list.front().cloned()
      };
      match item {
        Some(item) => output.write_resp_bulk_string(&item),
        None => output.write_resp_null(),
      }
      return Ok(true);
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
    src.update_size(&element, false);

    // 推入目标端（同键即源对象自身：rotation 形态，仅一次落库）
    if same_key {
      if dst_dir == OperationDirection::Left {
        src.list.push_front(element.clone());
      } else {
        src.list.push_back(element.clone());
      }
      src.update_size(&element, true);

      match list_save_or_gc(store, src_key, &src) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(()) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
      output.write_resp_bulk_string(&element);
      return Ok(true);
    }

    let mut dst_loaded = ListObject::new();
    {
      let dst: &mut ListObject = match list_load_sync(store, dst_key, output) {
        ListLoad::Degrade => return Ok(false),
        ListLoad::Error => return Ok(true),
        ListLoad::Missing => &mut dst_loaded,
        ListLoad::Present(o) => {
          dst_loaded = o;
          &mut dst_loaded
        }
      };

      if dst_dir == OperationDirection::Left {
        dst.list.push_front(element.clone());
      } else {
        dst.list.push_back(element.clone());
      }
      dst.update_size(&element, true);
    }

    // 源空 → 整键回收（C# EXPIRE TimeSpan.Zero）
    match list_save_or_gc(store, src_key, &src) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(()) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    }
    match list_save_or_gc(store, dst_key, &dst_loaded) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(()) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    }

    output.write_resp_bulk_string(&element);
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
        write_error_raw(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
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
            Err(()) => {
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
      cs::abort_with_error_message(output, "ERR numkeys should be greater than 0");
      return Ok(true);
    };
    if num_keys < 1 {
      cs::abort_with_error_message(output, "ERR numkeys should be greater than 0");
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
        Err(()) => {
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

#[cfg(test)]
mod tests {
  use std::{io::Cursor, str, sync::Arc};

  use tempfile::{TempDir, tempdir};
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};
  use wobject::list::list_object::ListObject as WoListObject;

  use super::{
    super::object_store_utils::{OBJ_TAG_LIST, obj_encode},
    *,
  };

  type TestSession = wkv::StoreSession<SegmentedDevice>;

  fn fixture(tag: &str) -> (TempDir, Arc<WedbStore<SegmentedDevice>>, TestSession) {
    let dir = tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
    let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let session = store.new_session().unwrap();
    (dir, store, session)
  }

  /// WRONGTYPE 错误应答帧
  const WRONGTYPE: &[u8] =
    b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";

  /// rmw 回写契约 + LPUSH/LPOP 头尾次序 + 删空回收 + 幻键防护
  #[test]
  fn push_pop_contract() {
    let (_dir, _store, session) = fixture("listwb.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    // RPUSH a b c
    sess
      .list_push(&[b"l", b"a", b"b", b"c"], &batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    // LPUSH H → 头插
    out.clear();
    sess
      .list_push(&[b"l", b"H"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":4\r\n");
    out.clear();
    sess
      .list_range(&[b"l", b"0", b"-1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*4\r\n$1\r\nH\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");

    // LPOP 无 count：取头
    out.clear();
    sess.list_pop(&[b"l"], &batch, &mut out, true).unwrap();
    assert_eq!(out, b"$1\r\nH\r\n");

    // RPOP count 2：取尾两枚
    out.clear();
    sess
      .list_pop(&[b"l", b"2"], &batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nc\r\n$1\r\nb\r\n");

    // LPOP 弹空 → 整键回收
    out.clear();
    sess.list_pop(&[b"l"], &batch, &mut out, true).unwrap();
    assert_eq!(out, b"$1\r\na\r\n");
    out.clear();
    sess.list_pop(&[b"l"], &batch, &mut out, true).unwrap();
    assert_eq!(out, b"$-1\r\n");
    assert!(
      batch
        .try_read_sync(b"l", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );

    // LPUSH count 形态负数 → 错误；缺失键 → nil
    out.clear();
    sess
      .list_pop(&[b"nk", b"-1"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"-ERR value is out of range, must be positive.\r\n");
    out.clear();
    sess.list_pop(&[b"nk"], &batch, &mut out, true).unwrap();
    assert_eq!(out, b"$-1\r\n");

    // 字符串键 → WRONGTYPE 不覆写
    let _ = batch.try_upsert_sync(b"str", b"plain-value");
    out.clear();
    sess
      .list_push(&[b"str", b"m"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, WRONGTYPE);
    assert_eq!(
      batch
        .try_read_sync(b"str", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten(),
      Some(b"plain-value".to_vec())
    );

    // LPUSHX 缺失键 → :0 且不建键；命中 → 新长度
    out.clear();
    sess
      .list_push_x(&[b"nxl", b"v"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":0\r\n");
    assert!(
      batch
        .try_read_sync(b"nxl", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );
    sess
      .list_push(&[b"xl", b"seed"], &batch, &mut Vec::new(), false)
      .unwrap();
    out.clear();
    sess
      .list_push_x(&[b"xl", b"v"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // 信封互通：RESP 写入 = [OBJ_TAG_LIST][wobject bitcode]
    sess
      .list_push(&[b"env", b"x"], &batch, &mut Vec::new(), false)
      .unwrap();
    let raw = batch
      .try_read_sync(b"env", |v| v.to_vec())
      .ok()
      .flatten()
      .flatten()
      .expect("envelope value");
    assert_eq!(raw[0], OBJ_TAG_LIST);
    let from_storage = WoListObject::deserialize(&mut Cursor::new(&raw[1..])).unwrap();
    assert_eq!(
      from_storage.list.lock().back().cloned(),
      Some(b"x".to_vec())
    );

    // 反向：storage 会话域信封 RESP 层可读
    let ext = WoListObject::new();
    ext.list.lock().push_back(b"pv".to_vec());
    let mut payload = Vec::new();
    ext.serialize(&mut payload).unwrap();
    let _ = batch.try_upsert_sync(b"fromstore", &obj_encode(OBJ_TAG_LIST, &payload));
    out.clear();
    sess
      .list_index(&[b"fromstore", b"0"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$2\r\npv\r\n");
  }

  /// LLEN / LRANGE / LINDEX / LTRIM 端到端
  #[test]
  fn read_commands() {
    let (_dir, _store, session) = fixture("listread.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .list_push(&[b"l", b"a", b"b", b"c"], &batch, &mut Vec::new(), false)
      .unwrap();

    // LLEN
    out.clear();
    sess.list_length(&[b"l"], &batch, &mut out).unwrap();
    assert_eq!(out, b":3\r\n");
    out.clear();
    sess.list_length(&[b"nk"], &batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    // LRANGE
    out.clear();
    sess
      .list_range(&[b"l", b"0", b"-1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
    out.clear();
    sess
      .list_range(&[b"nk", b"0", b"-1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");
    out.clear();
    sess
      .list_range(&[b"l", b"x", b"1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    // LINDEX
    out.clear();
    sess.list_index(&[b"l", b"-1"], &batch, &mut out).unwrap();
    assert_eq!(out, b"$1\r\nc\r\n");
    out.clear();
    sess.list_index(&[b"l", b"9"], &batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
    out.clear();
    sess.list_index(&[b"nk", b"0"], &batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");

    // LTRIM
    out.clear();
    sess
      .list_trim(&[b"l", b"1", b"1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    out.clear();
    sess.list_length(&[b"l"], &batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");
    // 裁剪至空 → 整键回收
    out.clear();
    sess
      .list_trim(&[b"l", b"5", b"9"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    assert!(
      batch
        .try_read_sync(b"l", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );
    // 键缺失 → OK
    out.clear();
    sess
      .list_trim(&[b"nk", b"0", b"1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
  }

  /// LINSERT / LREM / LSET 端到端
  #[test]
  fn mutation_commands() {
    let (_dir, _store, session) = fixture("listmut.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .list_push(&[b"l", b"a", b"b", b"c"], &batch, &mut Vec::new(), false)
      .unwrap();

    // LINSERT BEFORE b X
    out.clear();
    sess
      .list_insert(&[b"l", b"BEFORE", b"b", b"X"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":4\r\n");
    out.clear();
    sess
      .list_range(&[b"l", b"0", b"-1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*4\r\n$1\r\na\r\n$1\r\nX\r\n$1\r\nb\r\n$1\r\nc\r\n");

    // pivot 缺失 → -1
    out.clear();
    sess
      .list_insert(&[b"l", b"AFTER", b"zz", b"Q"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-1\r\n");
    // 键缺失 → :0
    out.clear();
    sess
      .list_insert(&[b"nk", b"BEFORE", b"b", b"Q"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // LREM -1 b：从尾删 1 枚
    out.clear();
    sess
      .list_remove(&[b"l", b"-1", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    sess
      .list_range(&[b"l", b"0", b"-1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*3\r\n$1\r\na\r\n$1\r\nX\r\n$1\r\nc\r\n");
    // 键缺失 → :0
    out.clear();
    sess
      .list_remove(&[b"nk", b"0", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // LSET
    out.clear();
    sess
      .list_set(&[b"l", b"1", b"Y"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    // 越界
    out.clear();
    sess
      .list_set(&[b"l", b"9", b"Z"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR index out of range\r\n");
    // 键缺失 → no such key
    out.clear();
    sess
      .list_set(&[b"nk", b"0", b"Z"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR no such key\r\n");
  }

  /// LPOS / LMOVE / RPOPLPUSH / LMPOP 端到端
  #[test]
  fn position_move_pop_multiple() {
    let (_dir, _store, session) = fixture("listmv.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .list_push(&[b"l", b"a", b"b", b"a"], &batch, &mut Vec::new(), false)
      .unwrap();

    // LPOS 缺省
    out.clear();
    sess.list_position(&[b"l", b"a"], &batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
    // LPOS COUNT 0：全部命中
    out.clear();
    sess
      .list_position(&[b"l", b"a", b"COUNT", b"0"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:0\r\n:2\r\n");
    // LPOS 未命中 + COUNT → 空数组
    out.clear();
    sess
      .list_position(&[b"l", b"zz", b"COUNT", b"2"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");
    // 键缺失 → nil
    out.clear();
    sess
      .list_position(&[b"nk", b"a"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // LMOVE R L：跨键搬移
    out.clear();
    sess
      .list_move(&[b"l", b"dst", b"RIGHT", b"LEFT"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$1\r\na\r\n");
    out.clear();
    sess
      .list_range(&[b"dst", b"0", b"-1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n$1\r\na\r\n");

    // LMOVE 缺失源 → nil
    out.clear();
    sess
      .list_move(&[b"nk", b"dst", b"LEFT", b"RIGHT"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // 方向词错误
    out.clear();
    sess
      .list_move(&[b"l", b"dst", b"UP", b"LEFT"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR syntax error\r\n");

    // RPOPLPUSH（等价 LMOVE R L）
    sess
      .list_push(&[b"r", b"1", b"2"], &batch, &mut Vec::new(), false)
      .unwrap();
    out.clear();
    sess
      .list_right_pop_left_push(&[b"r", b"dst"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$1\r\n2\r\n");
    out.clear();
    sess
      .list_range(&[b"dst", b"0", b"-1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\n2\r\n$1\r\na\r\n");

    // 同键同向：仅窥视
    out.clear();
    sess
      .list_move(&[b"dst", b"dst", b"RIGHT", b"RIGHT"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$1\r\na\r\n");
    out.clear();
    sess.list_length(&[b"dst"], &batch, &mut out).unwrap();
    assert_eq!(out, b":2\r\n");

    // LMPOP：首键非空弹出
    out.clear();
    sess
      .list_pop_multiple(&[b"2", b"nk", b"r", b"LEFT"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nr\r\n*1\r\n$1\r\n1\r\n");
    // 全缺失 → null 数组
    out.clear();
    sess
      .list_pop_multiple(&[b"1", b"nk", b"LEFT"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*-1\r\n");
    // COUNT 形态
    sess
      .list_push(&[b"m", b"1", b"2", b"3"], &batch, &mut Vec::new(), false)
      .unwrap();
    out.clear();
    sess
      .list_pop_multiple(&[b"1", b"m", b"LEFT", b"COUNT", b"2"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nm\r\n*2\r\n$1\r\n1\r\n$1\r\n2\r\n");
    // 非法 numkeys
    out.clear();
    sess
      .list_pop_multiple(&[b"0", b"m", b"LEFT"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR numkeys should be greater than 0\r\n");
  }

  /// 阻塞族立即可取路径（BLPOP/BLMOVE/BRPOPLPUSH/BLMPOP）
  #[test]
  fn blocking_immediate_path() {
    let (_dir, _store, session) = fixture("listblk.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .list_push(&[b"bl", b"x"], &batch, &mut Vec::new(), false)
      .unwrap();

    // BLPOP 立即可取：[key, item]
    out.clear();
    sess
      .list_blocking_pop(&[b"bl", b"0.1"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"*2\r\n$2\r\nbl\r\n$1\r\nx\r\n");

    // 空集合 → null 数组
    out.clear();
    sess
      .list_blocking_pop(&[b"bl", b"0.1"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"*-1\r\n");

    // 非法 timeout
    out.clear();
    sess
      .list_blocking_pop(&[b"bl", b"abc"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"-ERR timeout is not a float or out of range\r\n");

    // BLMOVE 立即可取
    sess
      .list_push(&[b"src", b"v"], &batch, &mut Vec::new(), false)
      .unwrap();
    out.clear();
    sess
      .list_blocking_move(
        &[b"src", b"bdst", b"RIGHT", b"LEFT", b"0.1"],
        &batch,
        &mut out,
      )
      .unwrap();
    assert_eq!(out, b"$1\r\nv\r\n");

    // BRPOPLPUSH 立即可取
    sess
      .list_push(&[b"src2", b"w"], &batch, &mut Vec::new(), false)
      .unwrap();
    out.clear();
    sess
      .list_blocking_pop_push(&[b"src2", b"bdst", b"0.1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$1\r\nw\r\n");

    // BLMPOP 立即可取
    sess
      .list_push(&[b"bm", b"1", b"2"], &batch, &mut Vec::new(), false)
      .unwrap();
    out.clear();
    sess
      .list_blocking_pop_multiple(&[b"0.1", b"1", b"bm", b"RIGHT"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$2\r\nbm\r\n*1\r\n$1\r\n2\r\n");

    // BLMPOP 全缺失 → null
    out.clear();
    sess
      .list_blocking_pop_multiple(&[b"0.1", b"1", b"nk", b"LEFT"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");
  }

  /// 解析 RESP 批量字符串数组帧（测试辅助）
  #[allow(dead_code)]
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
}
