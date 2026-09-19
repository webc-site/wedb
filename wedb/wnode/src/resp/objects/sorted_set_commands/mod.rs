//! 有序集合 RESP 命令（对标 libs/server/Resp/Objects/SortedSetCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`wcol::zset::sorted_set_object::SortedSetObject`] 的
//! operate 直收切片通道（与 C# GarnetObjectBase.Operate 分层一致），
//! 存取经与 storage 会话域共享的 `[类型标签][载荷]` 信封
//! （见 [`crate::resp::objects::object_store_utils`]）。
//!
//! 目录化拆分：
//! - [`read`]：ZRANGE, ZRANK, ZSCORE, ZCOUNT, ZLEXCOUNT, ZMSCORE, ZCARD, ZSCAN 等读命令；
//! - [`write`]：ZADD, ZINCRBY, ZREM, ZREMRANGEBYSCORE, ZREMRANGEBYRANK, ZREMRANGEBYLEX,
//!   ZDIFF, ZINTER, ZUNION, ZRANDMEMBER 等写与集合运算命令；
//! - [`blocking`]：BZPOPMIN, BZPOPMAX, BZMPOP 等阻塞与弹出命令；
//! - [`slow`]：慢路径执行臂。

mod blocking;
mod read;
pub(crate) mod slow;
mod write;

use memchr::memmem;
use wbase::num::{strict_f64, strict_i32};
use wcol::{
  ObjectOutput,
  zset::sorted_set_object::{SortedSetObject, SortedSetOperation},
};
use wval::GarnetObjectType;

pub use self::write::RemoveRangeKind;
pub(crate) use self::{
  blocking::write_popped_pairs,
  write::{
    CombineKind, combine_sets, diff_sets, parse_combine_args, parse_diff_args, write_zset_entries,
  },
};
use crate::resp::{
  objects::object_store_utils::{
    GarnetObjectPayload, ObjLoad, RespRmwDone, SyncRmwCmd, SyncRmwHandlers, obj_load_typed_sync,
    obj_save_or_gc, run_sync_rmw,
  },
  resp_server_session::RespServerSession,
};

pub(crate) type ZsetLoad = ObjLoad<SortedSetObject>;
pub(crate) type Rmw = ObjLoad<RespRmwDone>;

/// 经对象层 operate 通道执行操作，返回结构化输出
///（协议版本按会话协商版本透传，C# respProtocolVersion）
#[inline]
pub(crate) fn run_operate<'o>(
  obj: &mut SortedSetObject,
  op: SortedSetOperation,
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

/// 同步装载有序集合（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
#[inline]
pub(crate) fn zset_load_sync(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> ZsetLoad {
  obj_load_typed_sync(
    store,
    key,
    GarnetObjectType::SortedSet,
    output,
    SortedSetObject::from_blob,
  )
}

/// 变更回写：空集合整键回收（对齐 storage 层 finalize_removal 与 set 命令域收尾）
#[inline]
pub(crate) fn zset_save_or_gc(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  obj: &SortedSetObject,
) -> wkv::Result<bool> {
  obj_save_or_gc(
    store,
    key,
    GarnetObjectType::SortedSet,
    obj,
    obj.sorted_set_dict.is_empty(),
    |o| o.to_blob(),
  )
}

/// rmw 回写判定
///
/// - 只读操作不落库；例外：对象层已发生 TTL 惰性剔除（mutated_by_ttl）时
///   升格写回——C# 对象常驻 Tsavorite 对象缓存，ZCARD 读路径的
///   DeleteExpiredItems 就地剔除经 checkpoint 序列化落盘，Rust 信封无常驻
///   对象层，以剔除后回写等价闭环，杜绝已剔除成员重装载复活；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）无状态变更，不落库（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的操作（ZREM/ZREMRANGEBYLEX）以移除计数为准。
pub(crate) fn should_write_back(
  op: SortedSetOperation,
  out: &ObjectOutput<'_>,
  obj: &SortedSetObject,
  existed: bool,
) -> bool {
  if (is_read_only(op) && !obj.mutated_by_ttl())
    || out.payload_view().first() == Some(&b'-')
    || (!existed && obj.sorted_set_dict.is_empty())
  {
    return false;
  }
  match op {
    SortedSetOperation::Zrem | SortedSetOperation::Zremrangebylex => {
      out.result1 > 0 && out.result1 != i32::MAX as i64
    }
    _ => out.written(),
  }
}

/// 只读操作（rmw 不落库）
pub(crate) fn is_read_only(op: SortedSetOperation) -> bool {
  matches!(
    op,
    SortedSetOperation::Zcard
      | SortedSetOperation::Zscore
      | SortedSetOperation::Zmscore
      | SortedSetOperation::Zcount
      | SortedSetOperation::Zrange
      | SortedSetOperation::Zrank
      | SortedSetOperation::Zrevrank
      | SortedSetOperation::Zlexcount
      | SortedSetOperation::Zrandmember
      | SortedSetOperation::Zttl
      | SortedSetOperation::Zscan
  )
}

impl RespServerSession {
  /// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
  ///（协议版本取会话协商版本，C# respProtocolVersion）
  #[inline]
  pub(crate) fn zset_rmw(
    &self,
    store: &wkv::BatchStoreSession<impl wdev::Device>,
    key: &[u8],
    op: SortedSetOperation,
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
        tag: GarnetObjectType::SortedSet,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        SortedSetObject::from_blob,
        SortedSetObject::new,
        |o: &SortedSetObject| o.sorted_set_dict.is_empty(),
        |o: &SortedSetObject| o.to_blob(),
        |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
        should_write_back,
      ),
    )
  }
}

/// 成对负载解析（ZRANGESTORE/GEOSEARCHSTORE 回读：member score member score ...）
///
/// 兼容两种形态：RESP2 扁平序列；GEOSEARCHSTORE 的每项前置 `*2` 嵌套数组头
pub(crate) fn parse_pairs_payload(payload: &[u8]) -> Vec<(Vec<u8>, f64)> {
  let mut pairs = Vec::new();
  let mut pos = 0;

  // 跳过外层数组头 *<n>
  if payload.first() == Some(&b'*')
    && let Some(line_end) = find_crlf(payload, 0)
  {
    pos = line_end + 2;
  }

  while pos < payload.len() {
    // 项间嵌套数组头跳过（*<n>\r\n）
    if payload[pos] == b'*'
      && let Some(end) = find_crlf(payload, pos)
    {
      pos = end + 2;
    }
    // $<len>\r\n<bytes>\r\n
    if pos >= payload.len() || payload[pos] != b'$' {
      break;
    }
    let Some(line_end) = find_crlf(payload, pos) else {
      break;
    };
    let Some(len) = strict_i32(&payload[pos + 1..line_end])
      .filter(|&v| v >= 0)
      .map(|v| v as usize)
    else {
      break;
    };
    let start = line_end + 2;
    let end = start + len;
    if end + 2 > payload.len() {
      break;
    }
    let member = payload[start..end].to_vec();
    pos = end + 2;

    // 第二项：bulk string 形式的分值（前置嵌套数组头同样跳过）
    if pos < payload.len()
      && payload[pos] == b'*'
      && let Some(end) = find_crlf(payload, pos)
    {
      pos = end + 2;
    }
    if pos >= payload.len() || payload[pos] != b'$' {
      break;
    }
    let Some(line_end) = find_crlf(payload, pos) else {
      break;
    };
    let Some(len) = strict_i32(&payload[pos + 1..line_end])
      .filter(|&v| v >= 0)
      .map(|v| v as usize)
    else {
      break;
    };
    let start = line_end + 2;
    let end = start + len;
    if end + 2 > payload.len() {
      break;
    }
    let score = strict_f64(&payload[start..end], true).unwrap_or(0.0);
    pos = end + 2;

    pairs.push((member, score));
  }
  pairs
}

fn find_crlf(payload: &[u8], from: usize) -> Option<usize> {
  let slice = payload.get(from..)?;
  memmem::find(slice, b"\r\n").map(|pos| from + pos)
}
