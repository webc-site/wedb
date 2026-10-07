//! 有序集合阻塞与弹出命令（ZPOPMIN/ZPOPMAX/ZMPOP/BZPOPMIN/BZPOPMAX/BZMPOP，
//! 对标 libs/server/Resp/Objects/SortedSetCommands.cs 弹出命令段）

use std::sync::Arc;

use wbase::num::strict_i32;
use wcol::zset::sorted_set_object::{SortedSetObject, SortedSetOperation};
use wdev::Device;
use wresp::{
  check_args::check_arg_count, cmd_strings as cs, command::RespCommand, ext::RespVecExt,
};
use wval::GarnetObjectType;

use super::{Rmw, ZsetLoad, parse_zmpop_args, rmw_writeback_sync, zset_load_sync};
use crate::resp::{
  objects::object_store_utils::{BlockingPopHead, blocking_pop_head, rmw_aof_fail_ok},
  resp_server_session::RespServerSession,
};

/// 弹出族成对回复写出：*2 + key + *n + 逐条 [*2 + member + 分值]
///（分值走 cs::write_double_numeric 版本分派：RESP2 bulk / RESP3 `,num`，
/// 对标 SortedSetMPop :493-517 / BlockingMPop :1719-1734 的 WriteDoubleNumeric；
/// *2/*n 包装 RESP2/3 同形）
pub fn write_popped_pairs(
  key: &[u8],
  popped: &[(f64, Arc<[u8]>)],
  output: &mut Vec<u8>,
  resp_version: u8,
) {
  output.write_resp_array_len(2);
  output.write_resp_bulk_string(key);
  output.write_resp_array_len(popped.len());
  for (score, member) in popped {
    output.write_resp_array_len(2);
    output.write_resp_bulk_string(member);
    cs::write_double_numeric(output, *score, resp_version);
  }
}

/// 逐次弹出至多 `count` 个成员（负数按 0、以存活数钳制，弹出落空即止；快慢路径共用）
pub(crate) fn pop_up_to(
  obj: &mut SortedSetObject,
  pop_max: bool,
  count: i32,
) -> Vec<(f64, Arc<[u8]>)> {
  let n = (count.max(0) as usize).min(obj.purge_expired_len());
  let mut popped = Vec::with_capacity(n);
  popped.extend((0..n).map_while(|_| obj.pop_min_or_max(pop_max)));
  popped
}

/// 逐键尝试从第一个非空有序集合弹出指定方向与数量的成员（ZMPOP/BZMPOP 共用内核）
///
/// 返回：
/// - `Some(true)`：已命中并写出响应（含 WRONGTYPE 报错或写入错误）；
/// - `Some(false)`：遇到磁盘页面未就绪需要降级异步；
/// - `None`：全部候选键均不存在或为空集合。
fn zset_pop_first_nonempty(
  keys: &[&[u8]],
  store: &wkv::BatchStoreSession<impl Device>,
  low_scores_first: bool,
  pop_count: i32,
  output: &mut Vec<u8>,
  resp_version: u8,
) -> Option<bool> {
  for key in keys {
    // 装载型取件臂双保护·同步档（票 load-type-rmw-window zset 留尾，与 list
    // pop_first_nonempty 同核）：逐键装载前取 rmw 窗跨「装载 → 弹出 → 写回」，
    // 未取到走既有 Some(false) 异步重放通道；落笔前按装载态复验域归属
    let Some(_window) = store.try_rmw_window(key) else {
      return Some(false);
    };
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Some(false),
      ZsetLoad::WrongType => return Some(true),
      ZsetLoad::Missing => continue,
      ZsetLoad::Present(o) => o,
    };
    if obj.purge_expired_len() == 0 {
      // 删空自愈（对标 C# SortedSetObject.Operate :452-453 REMOVE_KEY）：
      // 全成员到期剔空时须原子写删空墓碑清退幽灵空键，否则旧信封与 TTL 滞留
      if obj.mutated_by_ttl()
        && let Some(handled) = rmw_writeback_sync(store, key, &obj, output).terminal()
      {
        return Some(handled);
      }
      continue;
    }

    let popped = pop_up_to(&mut obj, !low_scores_first, pop_count);
    if let Some(handled) = rmw_writeback_sync(store, key, &obj, output).terminal() {
      return Some(handled);
    }
    write_popped_pairs(key, &popped, output, resp_version);
    return Some(true);
  }
  None
}

impl RespServerSession {
  /// ZPOPMIN / ZPOPMAX key \[count\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetPop
  pub fn sorted_set_pop<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_min: bool,
  ) -> wresp::Result<bool> {
    check_arg_count!(
      parse_state,
      1..=2,
      output,
      if is_min { "ZPOPMIN" } else { "ZPOPMAX" }
    );

    let key = parse_state[0];
    let op = if is_min {
      SortedSetOperation::Zpopmin
    } else {
      SortedSetOperation::Zpopmax
    };

    // count 缺省形态传 -1（无外层数组头）
    let arg1: i32 = match parse_state.get(1) {
      None => -1,
      Some(c) => match strict_i32(c) {
        Some(v) if v >= 0 => v,
        _ => {
          // C# popCount < 0 → RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE（含句点）
          cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
          return Ok(true);
        }
      },
    };

    match self.zset_rmw(store, key, op, &[], (arg1, 0), output) {
      Rmw::Degrade => Ok(false),
      // AofFail：弹出写已生效、入账失败，撤帧落错误帧拒绝（AofEnqueue 契约，
      // ZPOPMIN/ZPOPMAX 非幂等禁重放，禁 `_` 吞态假成功）
      Rmw::AofFail => rmw_aof_fail_ok(output),
      _ => Ok(true),
    }
  }

  /// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetMPop
  pub fn sorted_set_m_pop<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((keys, low_scores_first, count)) = parse_zmpop_args(parse_state, false, output) else {
      return Ok(true);
    };

    // 逐键尝试弹出第一个非空集合
    if let Some(handled) = zset_pop_first_nonempty(
      keys,
      store,
      low_scores_first,
      count,
      output,
      self.resp_protocol_version,
    ) {
      return Ok(handled);
    }

    // C# 空弹出 → WriteNull（会话版本分派，RESP3 为 `_\r\n`）
    output.write_resp_null_ver(self.resp_protocol_version);
    Ok(true)
  }

  /// BZPOPMIN / BZPOPMAX key [key ...] timeout
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetBlockingPop
  ///
  /// 经纪注入时挂起等待（立即试取由经纪主循环 InitializeObserver 承担）；
  /// 未注入经纪保留立即可取路径。入口前奏（超时臂/降级门/事务直取判定）
  /// 单点见 [`blocking_pop_head`]
  pub fn sorted_set_blocking_pop<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_min: bool,
  ) -> wresp::Result<bool> {
    let (command, cmd_name) = if is_min {
      (RespCommand::Bzpopmin, "BZPOPMIN")
    } else {
      (RespCommand::Bzpopmax, "BZPOPMAX")
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
      GarnetObjectType::SortedSet,
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
      // 装载型取件臂双保护·同步档（票 load-type-rmw-window zset 留尾，与
      // list_blocking_pop 立即可取路径同核）：逐键装载前取 rmw 窗跨全程，
      // 未取到走既有 Ok(false) 异步重放通道；落笔前按装载态复验域归属
      crate::obj_windowed_load!(zset_load_sync, store, key, output, mut obj, {
        continue;
      });
      match obj.pop_min_or_max(!is_min) {
        Some((score, member)) => {
          if let Some(ret) = rmw_writeback_sync(store, key, &obj, output).terminal() {
            return Ok(ret);
          }
          output.write_resp_array_len(3);
          output.write_resp_bulk_string(key);
          output.write_resp_bulk_string(&member);
          // 分值版本分派（C# SortedSetBlockingPop :1619 WriteDoubleNumeric）
          cs::write_double_numeric(output, score, self.resp_protocol_version);
          return Ok(true);
        }
        // 删空自愈（对标 C# SortedSetObject.Operate :452-453 REMOVE_KEY）：
        // 弹出落空但全成员已到期剔空时，须写删空墓碑清退幽灵空键
        None => {
          if obj.mutated_by_ttl()
            && let Some(ret) = rmw_writeback_sync(store, key, &obj, output).terminal()
          {
            return Ok(ret);
          }
        }
      }
    }

    // 事务直取未果（空/缺键）：照常挂经纪等外域写入（blocking_pop_head 头注）
    crate::park_broker_arm!(self, txn_direct, command, timeout, keys, Vec::new);

    output.write_resp_null_ver(self.resp_protocol_version);
    Ok(true)
  }

  /// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetBlockingMPop
  ///
  /// 经纪注入时挂起等待（cmd_args = [lowScoresFirst(1B), popCount(i32 LE 4B)]，
  /// C# SortedSetBlockingMPop 同编码）；未注入经纪保留立即可取路径。
  /// 入口前奏单点见 [`blocking_pop_head`]（timeout 词元在首、键段经
  /// parse_zmpop_args numkeys 推导，低分优先位与 count 随 `extra` 带出；
  /// 让闩直取先行，空/缺键直取未果后才挂经纪）
  pub fn sorted_set_blocking_m_pop<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 4.., output, "BZMPOP");

    let BlockingPopHead {
      keys,
      timeout,
      txn_direct,
      extra: (low_scores_first, count),
    } = match blocking_pop_head(
      parse_state,
      0,
      store,
      GarnetObjectType::SortedSet,
      output,
      |ps, out| {
        parse_zmpop_args(ps, true, out)
          .map(|(keys, low_scores_first, count)| ((low_scores_first, count), keys))
      },
    ) {
      // Err(true) 错误帧已落闭环；Err(false) park 前预探降级整体转异步重放
      Ok(head) => head,
      Err(done) => return Ok(done),
    };
    // 两臂共用：差异段闭包收口为局部绑定（捕获低分优先位/count 均为 Copy 值，
    // 各臂 FnOnce 消费一次；MIN/MAX 布尔 + count 编码形态不变）
    let zmpop_args = || {
      vec![
        vec![u8::from(low_scores_first)],
        count.to_le_bytes().to_vec(),
      ]
    };
    crate::park_broker_arm!(
      self,
      !txn_direct,
      RespCommand::Bzmpop,
      timeout,
      keys,
      zmpop_args
    );

    // ---- 立即可取路径（经纪未注入的独立会话域 / 事务重放段让闩直取域）----
    if let Some(handled) = zset_pop_first_nonempty(
      keys,
      store,
      low_scores_first,
      count,
      output,
      self.resp_protocol_version,
    ) {
      return Ok(handled);
    }

    // 事务直取未果（空/缺键）：照常挂经纪等外域写入（blocking_pop_head 头注）
    crate::park_broker_arm!(
      self,
      txn_direct,
      RespCommand::Bzmpop,
      timeout,
      keys,
      zmpop_args
    );

    // C# !result.Found → WriteNull（会话版本分派，RESP3 为 `_\r\n`）
    output.write_resp_null_ver(self.resp_protocol_version);
    Ok(true)
  }
}
