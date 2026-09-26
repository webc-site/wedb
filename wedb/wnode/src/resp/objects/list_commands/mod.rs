//! 列表命令（对标 libs/server/Resp/Objects/ListCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`wcol::list::list_object::ListObject`] 的 operate 通道
//! （与 C# GarnetObjectBase.Operate 分层一致），存取经与 storage 会话域
//! 共享的 `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。
//!
//! 目录化拆分：[`read`] 读命令、[`write`] 写命令、[`blocking`] 阻塞与多键弹出命令、[`slow`] 慢路径执行臂。

// ============ 族内装载收尾宏单源（read / write / blocking 子域共用，须先于 mod 声明定义） ============

macro_rules! list_load_or_bail {
  ($store:expr, $key:expr, $output:expr, $missing:expr) => {
    $crate::obj_load_or_bail!(list_load_sync, $store, $key, $output, $missing)
  };
}

macro_rules! list_windowed_load {
  ($store:expr, $key:expr, $output:expr, mut $name:ident, $missing:expr) => {
    $crate::obj_windowed_load!(list_load_sync, $store, $key, $output, mut $name, $missing);
  };
}

mod blocking;
mod read;
pub(crate) mod slow;
mod write;

use wcol::{
  ObjectOutput,
  list::list_object::{ListObject, ListOperation, OperationDirection},
};
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, parse_i32_arg},
  cmd_strings as cs,
};
use wval::GarnetObjectType;

pub(crate) use self::blocking::write_collection_item_result;
use crate::{
  resp::{
    objects::object_store_utils::{
      GarnetObjectPayload, ObjLoad, SyncRmwCmd, SyncRmwHandlers, SyncRmwOutcome,
      obj_load_typed_sync, obj_save_or_gc, run_sync_rmw,
    },
    resp_server_session::RespServerSession,
  },
  session_parse_state_extensions::operation_direction_from_token as parse_direction,
};

// ============ 族内参数推导单源（快慢路径共用） ============
// 推导体为无 IO 纯解析 + 失败帧直写 output（同输入快慢应答逐字节一致），
// 慢分派不再对快路径已校验参数做第二份推导。

/// LMPOP / BLMPOP 参数推导单源（快慢路径共用；解析失败时已写出错误应答并
/// 返回 None），返回 (键切片, 弹出方向, count)
///
/// LMPOP: numkeys key \[key ...\] LEFT|RIGHT \[COUNT count\]
/// BLMPOP: timeout numkeys key \[key ...\] LEFT|RIGHT \[COUNT count\]
///（timeout 词元不在本内核射程，由调用方先行解析与校验）
///
/// 判定序对标 C# ListCommands.cs（ListPopMultiple 与 ListBlockingPopMultiple）：
/// numkeys 非整数（含溢出）与 <1 同报 → 定长形态 LEFT|RIGHT 必带、COUNT 形态
/// 恰多 2 参 → 方向词元 → COUNT 词元大小写门 → count 非整数与 <1 同报。
/// 错误帧两命令不同源：LMPOP 为无前缀版，BLMPOP 为 `Parameter` 反引号版
pub(crate) fn parse_lmpop_args<'a>(
  parse_state: &'a [&'a [u8]],
  is_blocking: bool,
  output: &mut Vec<u8>,
) -> Option<(&'a [&'a [u8]], OperationDirection, i32)> {
  super::shared_object_commands::parse_mpop_args(
    parse_state,
    is_blocking,
    super::shared_object_commands::MpopArgsConfig {
      cmd_name: if is_blocking { "BLMPOP" } else { "LMPOP" },
      num_keys_non_blocking_err: cs::RESP_ERR_GENERIC_NUMKEYS,
      count_non_blocking_err: cs::RESP_ERR_COUNT_GREATER_THAN_ZERO,
    },
    parse_direction,
    output,
  )
}

/// LRANGE / LTRIM 的 start/stop 参数推导单源（快慢路径共用；解析失败时已
/// 写出错误应答并返回 None）
///
/// 判定序对标 C# ListCommands.cs（ListRange 与 ListTrim）：arity 恰 3 →
/// start/stop 均须为 i32（溢出走 not-integer 对齐 C# TryGetInt；前导零拒收系 rust 严格收口，见 doc/zh/deviations.md §32），非整数同报 NOT_INTEGER
pub(crate) fn parse_i32_pair_args(
  cmd_name: &'static str,
  parse_state: &[&[u8]],
  output: &mut Vec<u8>,
) -> Option<(i32, i32)> {
  check_arg_count!(parse_state, 3, output, cmd_name, return None);
  let start = parse_i32_arg(parse_state[1], output)?;
  let stop = parse_i32_arg(parse_state[2], output)?;
  Some((start, stop))
}

pub(crate) type ListLoad = ObjLoad<ListObject>;
pub(crate) type Rmw = SyncRmwOutcome;

/// 经对象层 operate 通道执行操作，返回结构化输出
///（协议版本按会话协商版本透传，C# respProtocolVersion）
/// 同步装载列表（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
#[inline]
pub(crate) fn list_load_sync(
  store: &wkv::BatchStoreSession<impl Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> ListLoad {
  obj_load_typed_sync(
    store,
    key,
    GarnetObjectType::List,
    output,
    ListObject::from_blob,
  )
}

/// 变更回写：空列表整键回收（对齐 storage 层 finalize_removal 与命令域收尾）
#[inline]
pub(crate) fn list_save_or_gc(
  store: &wkv::BatchStoreSession<impl Device>,
  key: &[u8],
  obj: &ListObject,
) -> wkv::Result<bool> {
  obj_save_or_gc(
    store,
    key,
    GarnetObjectType::List,
    obj,
    obj.list.is_empty(),
    |o| o.to_blob(),
  )
}

/// rmw 回写判定
///
/// - 只读操作不落库；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）无状态变更，不落库（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的操作以变更计数为准；LSET 以 +OK 负载为准。
pub(crate) fn should_write_back(
  op: ListOperation,
  out: &ObjectOutput<'_>,
  obj: &ListObject,
  existed: bool,
) -> bool {
  if is_read_only(op)
    || out.payload_view().first() == Some(&b'-')
    || (!existed && obj.list.is_empty())
  {
    return false;
  }
  match op {
    ListOperation::Lrem => out.result1 > 0,
    ListOperation::Linsert => out.result1 > 0,
    ListOperation::Lset => out.payload_view().first() == Some(&b'+'),
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
  pub(crate) fn list_rmw(
    &self,
    store: &wkv::BatchStoreSession<impl Device>,
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
        tag: GarnetObjectType::List,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        ListObject::from_blob,
        ListObject::new,
        |o: &ListObject| o.list.is_empty(),
        |o: &ListObject| o.to_blob(),
        |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
        should_write_back,
      ),
    )
  }
}

// operate 通道执行单源已收敛至 rmw_helpers（原本地副本删除）
pub(crate) use crate::resp::objects::object_store_utils::run_operate;
