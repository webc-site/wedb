//! 哈希命令（对标 libs/server/Resp/Objects/HashCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`wcol::hash::hash_object::HashObject`] 的 operate 通道
//! 通道（与 C# GarnetObjectBase.Operate 分层一致），存取经与 storage 会话域
//! 共享的 `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。
//!
//! 目录化拆分：[`read`] 读命令、[`write`] 写命令、[`slow`] 慢路径执行臂。

mod read;
pub(crate) mod slow;
mod write;

use wcol::{
  ObjectOutput, ObjectOutputFlags,
  hash::hash_object::{HashObject, HashOperation},
};
use wval::GarnetObjectType;

use crate::resp::{
  objects::object_store_utils::{
    GarnetObjectPayload, ObjLoad, RespRmwDone, SyncRmwCmd, SyncRmwHandlers, obj_load_typed_sync,
    run_sync_rmw,
  },
  resp_server_session::RespServerSession,
};

/// 本命令面统一按会话协商协议版本输出（C# respProtocolVersion 由会话
/// `UpdateRespProtocolVersion` 下发，命令层经 `resp_protocol_version` 透传）
///
/// 经对象层 operate 通道执行操作，返回结构化输出
fn run_operate<'o>(
  obj: &mut HashObject,
  op: HashOperation,
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

pub(crate) type HashLoad = ObjLoad<HashObject>;
type Rmw = ObjLoad<RespRmwDone>;

/// 同步装载哈希（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
#[inline]
pub(crate) fn hash_load_sync(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> HashLoad {
  obj_load_typed_sync(
    store,
    key,
    GarnetObjectType::Hash,
    output,
    HashObject::from_blob,
  )
}

/// rmw 回写判定
///
/// - 只读操作不落库；例外：对象层已发生 TTL 惰性剔除（mutated_by_ttl）时
///   升格写回——C# 对象常驻 Tsavorite 对象缓存（HashOps.HashTimeToLive →
///   ReadObjectStoreOperation 的就地剔除经 checkpoint 序列化落盘），Rust 信封
///   无常驻对象层，以剔除后回写等价闭环，杜绝已剔除字段重装载复活；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）无状态变更，不落库（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的删除类操作（HDEL）以移除计数为准。
fn should_write_back(
  op: HashOperation,
  out: &ObjectOutput,
  obj: &HashObject,
  existed: bool,
) -> bool {
  if out.payload_view().first() == Some(&b'-') || (!existed && obj.hash.is_empty()) {
    return false;
  }
  match op {
    HashOperation::Hdel => out.result1 > 0,
    // HLEN 物化矫正臂（信封水位越线承接，唯一携带写面的只读操作）：剔除实际
    // 发生（mutated_by_ttl）升格写回一次矫正；全字段到期剔空（REMOVE_KEY）
    // 即删空自愈——计数矫正后水位前移，回归 O(1) 快道（collection.md §6.3）
    HashOperation::Hlen => {
      obj.mutated_by_ttl() || out.output_flags.contains(ObjectOutputFlags::REMOVE_KEY)
    }
    // 只读操作零状态变更不落库（mutated_by_ttl 剔除升格写回维持原判定）
    _ => !is_read_only(op) || obj.mutated_by_ttl(),
  }
}

/// 只读操作（rmw 不落库）
fn is_read_only(op: HashOperation) -> bool {
  matches!(
    op,
    HashOperation::Hget
      | HashOperation::Hmget
      | HashOperation::Hgetall
      | HashOperation::Hlen
      | HashOperation::Hstrlen
      | HashOperation::Hexists
      | HashOperation::Hkeys
      | HashOperation::Hvals
      | HashOperation::Hrandfield
      | HashOperation::Httl
      | HashOperation::Hscan
  )
}

impl RespServerSession {
  /// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
  ///（协议版本取会话协商版本，C# respProtocolVersion）
  #[inline]
  fn hash_rmw(
    &self,
    store: &wkv::BatchStoreSession<impl wdev::Device>,
    key: &[u8],
    op: HashOperation,
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
        tag: GarnetObjectType::Hash,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        HashObject::from_blob,
        HashObject::new,
        |o: &HashObject| o.is_empty(),
        |o: &HashObject| o.to_blob(),
        |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
        should_write_back,
      ),
    )
  }
}
