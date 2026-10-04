//! 列表阻塞弹出与多键弹出命令实现（BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, LMPOP, BLMPOP）

use wcol::{
  itembroker::collection_item_observer::CollectionItemResult, list::list_object::OperationDirection,
};
use wdev::Device;
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
  command::RespCommand,
  ext::RespVecExt,
};
use wval::GarnetObjectType;

use super::{
  ListLoad, list_load_sync, list_save_or_gc, parse_lmpop_args, parse_move_dirs, write::ListMove,
};
use crate::{
  resp::{
    objects::object_store_utils::{
      BlockingPopHead, any_sync_degrade, blocking_pop_head, obj_writeback_recheck_sync,
    },
    resp_server_session::RespServerSession,
  },
  session_parse_state_extensions::try_get_timeout_bytes,
};

impl RespServerSession {
  /// LMPOP numkeys key [key ...] LEFT | RIGHT [COUNT count]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListPopMultiple
  pub fn list_pop_multiple<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((keys, pop_direction, pop_count)) = parse_lmpop_args(parse_state, false, output)
    else {
      return Ok(true);
    };

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
  /// 域保留立即可取路径。入口前奏（超时臂/降级门/事务直取判定）单点见
  /// [`blocking_pop_head`]
  pub fn list_blocking_pop<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_left: bool,
  ) -> wresp::Result<bool> {
    let (command, cmd_name) = if is_left {
      (RespCommand::Blpop, "BLPOP")
    } else {
      (RespCommand::Brpop, "BRPOP")
    };
    check_arg_count!(parse_state, 2.., output, cmd_name);
    let BlockingPopHead {
      keys,
      timeout,
      txn_direct,
      ..
    } = match blocking_pop_head(
      parse_state,
      parse_state.len() - 1,
      store,
      GarnetObjectType::List,
      output,
      |ps, _| Some(((), &ps[..ps.len() - 1])),
    ) {
      // Err(true) 错误帧已落闭环；Err(false) park 前预探降级整体转异步重放
      Ok(head) => head,
      Err(done) => return Ok(done),
    };
    crate::park_broker_arm!(self, !txn_direct, command, timeout, keys, Vec::new);

    // ---- 立即可取路径（经纪未注入的独立会话域 / 事务重放段让闩直取域）----
    for key in keys {
      // 装载型取件臂双保护·同步档：弹出候选键即写回候选键（取窗单源见宏 doc）
      list_windowed_load!(store, key, output, mut obj, { continue });

      let item = if is_left {
        obj.list.pop_front()
      } else {
        obj.list.pop_back()
      };
      let Some(item) = item else {
        continue;
      };
      obj.update_size(&item, false);

      if !obj_writeback_recheck_sync(store, key, true) {
        return Ok(false);
      }
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

    // 事务直取未果（空/缺键）：照常挂经纪等外域写入（blocking_pop_head 头注）
    crate::park_broker_arm!(self, txn_direct, command, timeout, keys, Vec::new);

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
  /// 经纪保留立即可取路径。方向对解析单点见 [`super::parse_move_dirs`]
  pub fn list_blocking_move<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 5, output, "BLMOVE");

    let src_key = parse_state[0];
    let dst_key = parse_state[1];

    let Some((src_dir, dst_dir)) = parse_move_dirs(parse_state, output) else {
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
    // 源/目标任一键同步不可出件则整体路由慢路径（move_core 异步臂分层感知）。
    // 事务重放段外层直取臂同 blocking_pop_head 事务直取判定（头注同源，
    // 不复挂），直取复用 list_move_core，空源臂 SrcEmpty 分流回挂经纪
    if any_sync_degrade(store, &[src_key, dst_key], GarnetObjectType::List) {
      return Ok(false);
    }
    let txn_direct = store.session_locking().is_transactional();
    // 两臂共用：差异段闭包收口为局部绑定（捕获 dst_key/src_dir/dst_dir 均为
    // Copy 借用，各臂 FnOnce 消费一次；方向字节编码形态不变）
    let move_args = || vec![dst_key.to_vec(), vec![src_dir as u8], vec![dst_dir as u8]];
    crate::park_broker_arm!(
      self,
      !txn_direct,
      RespCommand::Blmove,
      timeout,
      &[src_key],
      move_args
    );
    match self.list_move_core(src_key, dst_key, src_dir, dst_dir, store, output)? {
      ListMove::Done => return Ok(true),
      ListMove::Degrade => return Ok(false),
      ListMove::SrcEmpty => {}
    }
    // 事务直取未果（空/缺源键）：照常挂经纪等外域写入（blocking_pop_head 头注）
    crate::park_broker_arm!(
      self,
      txn_direct,
      RespCommand::Blmove,
      timeout,
      &[src_key],
      move_args
    );
    output.write_resp_null_ver(self.resp_protocol_version);
    Ok(true)
  }

  /// BRPOPLPUSH source destination timeout
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListBlockingPopPush
  ///
  /// 同 BLMOVE 的 Right→Left 定式（C# ListBlockingMove(Right, Left)）
  pub fn list_blocking_pop_push<'a, D: Device>(
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
    // 源/目标预探同 BLMOVE，事务重放段外层直取臂同 BLMOVE（SrcEmpty 回挂）
    if any_sync_degrade(
      store,
      &[parse_state[0], parse_state[1]],
      GarnetObjectType::List,
    ) {
      return Ok(false);
    }
    let txn_direct = store.session_locking().is_transactional();
    // Right→Left 定式方向字节为编译期常量切片（免每次 vec! 重建）；差异段
    // 闭包收口为局部绑定（闭包捕获 parse_state 均为 Copy 借用，两臂各 FnOnce
    // 消费一次）
    const RIGHT_DIR_ARG: &[u8] = &[OperationDirection::Right as u8];
    const LEFT_DIR_ARG: &[u8] = &[OperationDirection::Left as u8];
    let move_args = || {
      vec![
        parse_state[1].to_vec(),
        RIGHT_DIR_ARG.to_vec(),
        LEFT_DIR_ARG.to_vec(),
      ]
    };
    crate::park_broker_arm!(
      self,
      !txn_direct,
      RespCommand::Blmove,
      timeout,
      &[parse_state[0]],
      move_args
    );
    match self.list_move_core(
      parse_state[0],
      parse_state[1],
      OperationDirection::Right,
      OperationDirection::Left,
      store,
      output,
    )? {
      ListMove::Done => return Ok(true),
      ListMove::Degrade => return Ok(false),
      ListMove::SrcEmpty => {}
    }
    // 事务直取未果（空/缺源键）：照常挂经纪等外域写入（blocking_pop_head 头注）
    crate::park_broker_arm!(
      self,
      txn_direct,
      RespCommand::Blmove,
      timeout,
      &[parse_state[0]],
      move_args
    );
    output.write_resp_null_ver(self.resp_protocol_version);
    Ok(true)
  }

  /// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListBlockingPopMultiple
  ///
  /// 经纪注入时挂起等待（cmd_args = [popDir(1B), popCount(i32 LE 4B)]，
  /// C# ListBlockingPopMultiple 同编码）；未注入经纪保留立即可取路径。
  /// 入口前奏单点见 [`blocking_pop_head`]（timeout 词元在首、键段经
  /// parse_lmpop_args numkeys 推导，方向与 count 随 `extra` 带出）
  pub fn list_blocking_pop_multiple<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 4.., output, "BLMPOP");

    let BlockingPopHead {
      keys,
      timeout,
      txn_direct,
      extra: (pop_direction, pop_count),
    } = match blocking_pop_head(
      parse_state,
      0,
      store,
      GarnetObjectType::List,
      output,
      |ps, out| parse_lmpop_args(ps, true, out).map(|(keys, dir, count)| ((dir, count), keys)),
    ) {
      // Err(true) 错误帧已落闭环；Err(false) park 前预探降级整体转异步重放
      Ok(head) => head,
      Err(done) => return Ok(done),
    };
    // 两臂共用：差异段闭包收口为局部绑定（捕获方向/count 均为 Copy 值，
    // 各臂 FnOnce 消费一次；方向 + count 编码形态不变）
    let pop_args = || vec![vec![pop_direction as u8], pop_count.to_le_bytes().to_vec()];
    crate::park_broker_arm!(
      self,
      !txn_direct,
      RespCommand::Blmpop,
      timeout,
      keys,
      pop_args
    );

    // ---- 立即可取路径（经纪未注入的独立会话域 / 事务重放段让闩直取域）----
    if let Some(done) = pop_first_nonempty(keys, store, pop_direction, pop_count, output) {
      return Ok(done);
    }

    // 事务直取未果（空/缺键）：照常挂经纪等外域写入（blocking_pop_head 头注）
    crate::park_broker_arm!(
      self,
      txn_direct,
      RespCommand::Blmpop,
      timeout,
      keys,
      pop_args
    );

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
  store: &wkv::BatchStoreSession<impl Device>,
  pop_direction: OperationDirection,
  pop_count: i32,
  output: &mut Vec<u8>,
) -> Option<bool> {
  let is_left = pop_direction == OperationDirection::Left;
  for key in keys {
    // 装载型取件臂双保护·同步档（同 list_blocking_pop 立即可取路径口径）
    let Some(_window) = store.try_rmw_window(key) else {
      return Some(false);
    };
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

    if !obj_writeback_recheck_sync(store, key, true) {
      return Some(false);
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
///   WriteNull 的版本感知形态）；
/// - 取到 → 按命令族帧型展开；BZPOPMIN/BZPOPMAX/BZMPOP 分值字段亦按会话
///   RESP 版本分派（RESP3 `,` double / RESP2 bulk，对位 C# 会话尾码
///   WriteDoubleNumeric，SortedSetCommands.cs:1619/:1734）。
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
      // 分值版本分派单源（对位 C#:1619 WriteDoubleNumeric）
      cs::write_double_numeric(output, result.score.unwrap_or_default(), resp_version);
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
        // 逐对分值同款版本分派（对位 C#:1734 WriteDoubleNumeric）
        cs::write_double_numeric(
          output,
          scores.get(i).copied().unwrap_or_default(),
          resp_version,
        );
      }
    }
    _ => {}
  }
}
