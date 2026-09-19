//! 列表命令（对标 libs/server/Resp/Objects/ListCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`wcol::list::list_object::ListObject`] 的 operate 通道
//! （与 C# GarnetObjectBase.Operate 分层一致），存取经与 storage 会话域
//! 共享的 `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。
//!
//! 目录化拆分：[`read`] 读命令、[`write`] 写命令、[`blocking`] 阻塞与多键弹出命令、[`slow`] 慢路径执行臂。

mod blocking;
mod read;
pub(crate) mod slow;
mod write;

use wcol::{
  ObjectOutput,
  list::list_object::{ListObject, ListOperation},
};
use wval::GarnetObjectType;

pub(crate) use self::blocking::write_collection_item_result;
use crate::resp::{
  objects::object_store_utils::{
    GarnetObjectPayload, ObjLoad, RespRmwDone, SyncRmwCmd, SyncRmwHandlers, obj_load_typed_sync,
    obj_save_or_gc, run_sync_rmw,
  },
  resp_server_session::RespServerSession,
};

pub(crate) type ListLoad = ObjLoad<ListObject>;
pub(crate) type Rmw = ObjLoad<RespRmwDone>;

/// 经对象层 operate 通道执行操作，返回结构化输出
///（协议版本按会话协商版本透传，C# respProtocolVersion）
pub(crate) fn run_operate<'o>(
  obj: &mut ListObject,
  op: ListOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  resp_version: u8,
  output: &'o mut Vec<u8>,
) -> ObjectOutput<'o> {
  let mut obj_out = ObjectOutput::mount(output);
  obj.operate(op as u8, args, arg1, arg2, &mut obj_out, resp_version);
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
    GarnetObjectType::List,
    output,
    ListObject::from_blob,
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
