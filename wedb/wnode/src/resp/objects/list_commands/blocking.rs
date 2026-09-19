//! 列表阻塞弹出与多键弹出命令实现（BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, LMPOP, BLMPOP）

use wbase::num::strict_i32;
use wcol::{
  itembroker::collection_item_observer::CollectionItemResult, list::list_object::OperationDirection,
};
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
  command::RespCommand,
  ext::RespVecExt,
  resp_memory_writer::format_double,
};
use wval::GarnetObjectType;
use zmij::Buffer;

use super::{ListLoad, list_load_sync, list_save_or_gc};
use crate::{
  resp::{
    objects::object_store_utils::obj_load_sync_degrades, resp_server_session::RespServerSession,
  },
  session_parse_state_extensions::{
    operation_direction_from_token as parse_direction, try_get_timeout_bytes,
  },
};

/// 阻塞族 park 前预探：任一键同步不可出件（活跃分层键 / 磁盘候选）即真，
/// 命令整体不挂经纪、路由慢路径异步臂闭环（判定单源见 obj_load_sync_degrades）
fn any_sync_degrade(store: &wkv::BatchStoreSession<impl wdev::Device>, keys: &[&[u8]]) -> bool {
  keys
    .iter()
    .any(|k| obj_load_sync_degrades(store, k, GarnetObjectType::List))
}

impl RespServerSession {
  /// LMPOP numkeys key [key ...] LEFT | RIGHT [COUNT count]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListPopMultiple
  pub fn list_pop_multiple<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3.., output, "LMPOP");

    // C# TryGetInt（int32）：非整数（含溢出）与 <1 同报
    // GenericErrShouldBeGreaterThanZero "numkeys"（ListCommands.cs:198）
    let Some(num_keys) = strict_i32(parse_state[0]) else {
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

    let mut pop_count = 1_i32;
    if parse_state.len() == num_keys as usize + 4 {
      if !parse_state[num_keys as usize + 2].eq_ignore_ascii_case(b"COUNT") {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      // C# TryGetInt（int32）：非整数（含溢出）与 <1 同报
      // GenericErrShouldBeGreaterThanZero "count"（ListCommands.cs:228）
      match strict_i32(parse_state[num_keys as usize + 3]) {
        Some(c) if c >= 1 => pop_count = c,
        _ => {
          cs::abort_with_error_message(output, "ERR count should be greater than 0");
          return Ok(true);
        }
      }
    }

    // 逐键弹出第一个非空列表（LMPOP/BLMPOP 立即可取路径公共体复用）
    if let Some(done) = pop_first_nonempty(keys, store, pop_direction, pop_count, output) {
      return Ok(done);
    }

    // C# NOTFOUND → WriteNullArray（会话版本分派，RESP3 为 `_\r\n`）
    output.write_resp_null_array_ver(self.resp_protocol_version);
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
    check_arg_count!(parse_state, 2.., output, cmd_name);

    let timeout = match try_get_timeout_bytes(parse_state[parse_state.len() - 1]) {
      Ok(timeout) => timeout,
      Err(error) => {
        cs::abort_with_error_message(output, error);
        return Ok(true);
      }
    };

    // 经纪挂起路径（C# GetCollectionItemAsync；立即试取由经纪主循环
    // InitializeObserver 承担，FIFO 与 WRONGTYPE 语义同 C#）：park 前任一键
    // 同步不可出件（分层 / 冷候选）则整体路由慢路径异步臂
    if any_sync_degrade(store, &parse_state[..parse_state.len() - 1]) {
      return Ok(false);
    }
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
        ListLoad::WrongType => return Ok(true),
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

    // C# !result.Found → WriteNullArray（会话版本分派，RESP3 为 `_\r\n`）
    output.write_resp_null_array_ver(self.resp_protocol_version);
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

    // 经纪挂起路径：方向编码进 cmd_args（C# PinnedSpanByte 单字节指针形态）；
    // 源/目标任一键同步不可出件则整体路由慢路径（move_core 异步臂分层感知）
    if any_sync_degrade(store, &[src_key, dst_key]) {
      return Ok(false);
    }
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

    // 经纪挂起路径：Right→Left 定式编码（C# ListBlockingMove 定式同源）；
    // 源/目标预探同 BLMOVE
    if any_sync_degrade(store, &[parse_state[0], parse_state[1]]) {
      return Ok(false);
    }
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
    check_arg_count!(parse_state, 4.., output, "BLMPOP");

    let timeout = match try_get_timeout_bytes(parse_state[0]) {
      Ok(timeout) => timeout,
      Err(error) => {
        cs::abort_with_error_message(output, error);
        return Ok(true);
      }
    };

    // C# GenericParamShouldBeGreaterThanZero 替换 {0}="numkeys"（注意与 LMPOP 的
    // 无 Parameter 前缀版文案不同源）
    let err_numkeys = cs::GENERIC_PARAM_SHOULD_BE_GREATER_THAN_ZERO.replace("{0}", "numkeys");
    // C# TryGetInt（int32）：非整数（含溢出）与 <1 同报 Parameter 版（ListCommands.cs:866）
    let Some(num_keys) = strict_i32(parse_state[1]) else {
      cs::abort_with_error_message(output, &err_numkeys);
      return Ok(true);
    };
    if num_keys < 1 {
      cs::abort_with_error_message(output, &err_numkeys);
      return Ok(true);
    }
    if parse_state.len() != num_keys as usize + 3 && parse_state.len() != num_keys as usize + 5 {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    }

    let keys = &parse_state[2..=num_keys as usize + 1];

    let Some(pop_direction) = parse_direction(parse_state[num_keys as usize + 2]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    };

    let mut pop_count = 1_i32;
    if parse_state.len() == num_keys as usize + 5 {
      if !parse_state[num_keys as usize + 3].eq_ignore_ascii_case(b"COUNT") {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      // C# TryGetInt（int32）：非整数（含溢出）与 <1 同报 Parameter 版（ListCommands.cs:903）
      match strict_i32(parse_state[num_keys as usize + 4]) {
        Some(c) if c >= 1 => pop_count = c,
        _ => {
          // C# GenericParamShouldBeGreaterThanZero 替换 {0}="count"
          let err_count = cs::GENERIC_PARAM_SHOULD_BE_GREATER_THAN_ZERO.replace("{0}", "count");
          cs::abort_with_error_message(output, &err_count);
          return Ok(true);
        }
      }
    }

    // 经纪挂起路径（方向 + count 编码进 cmd_args）；park 前预探同 BLPOP
    if any_sync_degrade(store, keys) {
      return Ok(false);
    }
    if self.park_broker_wait(
      RespCommand::Blmpop,
      timeout,
      || keys.iter().map(|k| k.to_vec()).collect(),
      || vec![vec![pop_direction as u8], pop_count.to_le_bytes().to_vec()],
    ) {
      return Ok(true);
    }

    // ---- 立即可取路径（经纪未注入的独立会话域）----
    if let Some(done) = pop_first_nonempty(keys, store, pop_direction, pop_count, output) {
      return Ok(done);
    }

    // C# !result.Found → WriteNull（会话版本分派，RESP3 为 `_\r\n`）
    output.write_resp_null_ver(self.resp_protocol_version);
    Ok(true)
  }
}

/// 逐键弹出第一个非空列表并写回复（LMPOP/BLMPOP 立即可取路径公共体）
///
/// 返回 Some(true) 已命中并写回复；Some(false) 磁盘候选降级 / 回写降级；
/// None 全部键缺失或空列表（调用方写未取到应答）
pub(crate) fn pop_first_nonempty(
  keys: &[&[u8]],
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  pop_direction: OperationDirection,
  pop_count: i32,
  output: &mut Vec<u8>,
) -> Option<bool> {
  let is_left = pop_direction == OperationDirection::Left;
  for key in keys {
    let mut obj = match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Some(false),
      ListLoad::WrongType => return Some(true),
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

/// 阻塞命令完成后的应答写出（单函数承接 C# 六个阻塞命令尾部
/// BlockingWait 之后的 switch 应答段：ListBlockingPop / ListBlockingMove /
/// ListBlockingPopPush / ListBlockingPopMultiple / SortedSetBlockingPop /
/// SortedSetBlockingMPop）
///
/// - 强制解除（CLIENT UNBLOCK）→ UNBLOCKED 错误行；
/// - 类型不符 → WRONGTYPE；
/// - 未取到 → BLPOP/BRPOP 空数组，其余空值（均按会话 RESP 版本分派：
///   RESP3 为 `_\r\n`，RESP2 为 `*-1`/`$-1`，对位 C# WriteNullArray /
///   WriteNull 的版本感知形态，RespServerSessionOutput.cs:WriteNull/
///   WriteNullArray）；
/// - 取到 → 按命令族帧型展开。
pub(crate) fn write_collection_item_result(
  cmd: RespCommand,
  result: &CollectionItemResult,
  resp_version: u8,
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
    // 未取到项：BLPOP/BRPOP 空数组，其余空值（版本感知单源）
    if matches!(cmd, RespCommand::Blpop | RespCommand::Brpop) {
      output.write_resp_null_array_ver(resp_version);
    } else {
      output.write_resp_null_ver(resp_version);
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
      let mut fbuf = Buffer::new();
      output.write_resp_bulk_string(
        format_double(result.score.unwrap_or_default(), &mut fbuf).as_bytes(),
      );
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
        let mut fbuf = Buffer::new();
        output.write_resp_bulk_string(
          format_double(scores.get(i).copied().unwrap_or_default(), &mut fbuf).as_bytes(),
        );
      }
    }
    _ => {}
  }
}
