//! 集合命令（对标 libs/server/Resp/Objects/SetCommands.cs）
//!
//! 命令层只做参数校验与编解码：单键语义全部下沉到
//! [`wcol::set::set_object::SetObject`] 的 operate 通道
//! 通道（与 C# GarnetObjectBase.Operate 分层一致）；SINTER/SUNION/SDIFF
//! 族为多键聚合，对标 libs/server/Storage/Session/ObjectStore/SetOps.cs
//! 的装载-折叠语义在命令层就地求值。存取经与 storage 会话域共享的
//! `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。
//!
//! 目录化拆分：[`read`] 读命令、[`write`] 写命令与集合运算、[`slow`] 慢路径执行臂。

mod read;
pub(crate) mod slow;
mod write;

use wbase::num::strict_i32;
use wcol::{
  ObjectOutput,
  set::{
    set_object::{SetObject, SetOperation},
    set_object_impl::NO_COUNT,
  },
};
use wresp::{check_args::check_arg_count, cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

use crate::resp::{
  objects::object_store_utils::{
    GarnetObjectPayload, ObjLoad, RespRmwDone, SyncRmwCmd, SyncRmwHandlers, obj_load_typed_sync,
    obj_save_or_gc, run_sync_rmw,
  },
  resp_server_session::RespServerSession,
};

pub(crate) type SetLoad = ObjLoad<SetObject>;
type Rmw = ObjLoad<RespRmwDone>;

/// SPOP key \[count\] 参数推导单源（快慢路径共用；解析失败时已写出错误应答
/// 并返回 None），返回 (key, count；缺省 NO_COUNT)
///
/// 判定序对标 C# SetCommands.cs 的 SetPop：arity 1..=2 → count 非整数
/// （含溢出）或负数同报 NOT_INTEGER
pub(crate) fn parse_set_pop_args<'a>(
  parse_state: &'a [&'a [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'a [u8], i32)> {
  check_arg_count!(parse_state, 1..=2, output, "SPOP", return None);
  let count = match parse_state.get(1) {
    None => NO_COUNT,
    // C#：非整数（含溢出）或负数 → VALUE_IS_NOT_INTEGER
    Some(raw) => match strict_i32(raw) {
      Some(c) if c >= 0 => c,
      _ => {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return None;
      }
    },
  };
  Some((parse_state[0], count))
}

/// 经对象层 operate 通道执行操作，返回结构化输出
///（协议版本按会话协商版本透传，C# respProtocolVersion）
fn run_operate<'o>(
  obj: &mut SetObject,
  op: SetOperation,
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

/// 同步装载集合（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
#[inline]
pub(crate) fn set_load_sync(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> SetLoad {
  obj_load_typed_sync(
    store,
    key,
    GarnetObjectType::Set,
    output,
    SetObject::from_blob,
  )
}

/// 变更回写：空集合整键回收（对齐 storage 层 finalize_removal 与命令域收尾）
#[inline]
pub(crate) fn set_save_or_gc(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  obj: &SetObject,
) -> wkv::Result<bool> {
  obj_save_or_gc(
    store,
    key,
    GarnetObjectType::Set,
    obj,
    obj.set.is_empty(),
    |o| o.to_blob(),
  )
}

/// rmw 回写判定
///
/// - 只读操作不落库；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）无状态变更，不落库（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的删除类操作（SREM）以移除计数为准。
fn should_write_back(
  op: SetOperation,
  out: &ObjectOutput<'_>,
  obj: &SetObject,
  existed: bool,
) -> bool {
  if is_read_only(op)
    || out.payload_view().first() == Some(&b'-')
    || (!existed && obj.set.is_empty())
  {
    return false;
  }
  match op {
    SetOperation::Srem => out.result1 > 0,
    _ => true,
  }
}

/// 只读操作（rmw 不落库）
fn is_read_only(op: SetOperation) -> bool {
  matches!(
    op,
    SetOperation::Scard
      | SetOperation::Smembers
      | SetOperation::Sismember
      | SetOperation::Smismember
      | SetOperation::Srandmember
      | SetOperation::Sscan
  )
}

impl RespServerSession {
  /// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
  ///（协议版本取会话协商版本，C# respProtocolVersion）
  #[inline]
  fn set_rmw(
    &self,
    store: &wkv::BatchStoreSession<impl wdev::Device>,
    key: &[u8],
    op: SetOperation,
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
        tag: GarnetObjectType::Set,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        SetObject::from_blob,
        SetObject::new,
        |o: &SetObject| o.set.is_empty(),
        |o: &SetObject| o.to_blob(),
        |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
        should_write_back,
      ),
    )
  }
}

/// 结果集合的 RESP 输出（集合头版本分派：RESP2 *N / RESP3 ~N）
///
/// 写出 set 成员列表（内部调用 cs::write_set_len 单点）
fn write_set_members(result: &SetObject, output: &mut Vec<u8>, resp_version: u8) {
  let members = result.to_members();
  cs::write_set_len(output, members.len(), resp_version);
  for member in members {
    output.write_resp_bulk_string(&member);
  }
}

#[cfg(test)]
mod write_set_members_tests {
  use wcol::set::set_object::SetObject;

  use super::write_set_members;

  /// SINTER/SUNION/SDIFF 结果集合头版本分派测试（RESP2 *N / RESP3 ~N）
  #[test]
  fn set_head_resp2_array_resp3_set() {
    let mut obj = SetObject::new();
    obj.set.insert(b"a".to_vec());

    let mut out2 = Vec::new();
    write_set_members(&obj, &mut out2, 2);
    assert_eq!(out2, b"*1\r\n$1\r\na\r\n");

    let mut out3 = Vec::new();
    write_set_members(&obj, &mut out3, 3);
    assert_eq!(out3, b"~1\r\n$1\r\na\r\n");
  }

  /// 空集位点（SMEMBERS 缺键 / SPOP count==0 / SPOP 缺键带 count）
  /// 对位 C# RespServerSessionOutput.cs:100 WriteEmptySet
  #[test]
  fn empty_set_resp2_star_resp3_tilde() {
    let obj = SetObject::new();

    let mut out2 = Vec::new();
    write_set_members(&obj, &mut out2, 2);
    assert_eq!(out2, b"*0\r\n");

    let mut out3 = Vec::new();
    write_set_members(&obj, &mut out3, 3);
    assert_eq!(out3, b"~0\r\n");
  }
}
