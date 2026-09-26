//! 哈希命令（对标 libs/server/Resp/Objects/HashCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`wcol::hash::hash_object::HashObject`] 的 operate 通道
//! 通道（与 C# GarnetObjectBase.Operate 分层一致），存取经与 storage 会话域
//! 共享的 `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。
//!
//! 目录化拆分：[`read`] 读命令、[`write`] 写命令、[`slow`] 慢路径执行臂。

macro_rules! hash_load_or_bail {
  ($store:expr, $key:expr, $output:expr, $missing:expr) => {
    $crate::obj_load_or_bail!(hash_load_sync, $store, $key, $output, $missing)
  };
}

mod read;
pub(crate) mod slow;
mod write;

use wcol::{
  ObjectOutput,
  hash::hash_object::{HashObject, HashOperation},
};
use wdev::Device;
use wresp::cmd_strings as cs;
use wval::GarnetObjectType;

use crate::resp::{
  objects::object_store_utils::{
    GarnetObjectPayload, ObjLoad, SyncRmwCmd, SyncRmwHandlers, SyncRmwOutcome, obj_load_typed_sync,
    run_sync_rmw,
  },
  resp_server_session::RespServerSession,
};

/// 本命令面统一按会话协商协议版本输出（C# respProtocolVersion 由会话
/// `UpdateRespProtocolVersion` 下发，命令层经 `resp_protocol_version` 透传）
///
/// 经对象层 operate 通道执行操作，返回结构化输出
pub(crate) type HashLoad = ObjLoad<HashObject>;
type Rmw = SyncRmwOutcome;

/// 同步装载哈希（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
#[inline]
pub(crate) fn hash_load_sync(
  store: &wkv::BatchStoreSession<impl Device>,
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
///   例外：`-` 行且 existed 键装载/操作期已发生 TTL 物理剔除（mutated_by_ttl）
///   时豁免拒写——剔除矫正必固化（HINCRBY 族存量解析错臂；本豁免为 hash 面
///   自有裁决，zset 面同款门已裁无需固化，见 deviations §142），错误帧应答
///   字节不变；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的删除/排他写入操作（HDEL/HSETNX）以变更计数为准：HSETNX
///   字段已存在（set == 0）零变异不落库不广播，杜绝全量重序列化写放大与 AOF
///   增量污染；TTL 惰性剔除（装载即剔除或操作前堆序剔除，mutated_by_ttl）
///   升格写回固化矫正。
fn should_write_back(
  op: HashOperation,
  out: &ObjectOutput,
  obj: &HashObject,
  existed: bool,
) -> bool {
  // '-' 错误帧拒写门增 mutated_by_ttl 豁免支（hash 面自有裁决，勿按 zset 面
  // 对位门误读为同款——zset 面已裁无需固化，见 deviations §142）：HINCRBY 族存量解析错臂
  //（wcol hash_object_impl.rs:hash_increment/:hash_increment_float）出 '-' 前
  // 已发生装载期＋操作期物理剔除（delete_expired_items/窄窗 remove 置标），
  // 整臂拒写使已剔字段下次装载复活；existed 键的剔除矫正必落盘。可达性注：
  // '-' 且非空 obj 仅存量错形可至，豁免不引入新键；防幻键第二门保持
  //（!existed 且空对象仍拒）。错误帧应答字节零变化，仅持久化面放行
  if out.payload_view().first() == Some(&b'-') && !(existed && obj.mutated_by_ttl()) {
    return false;
  }
  if !existed && obj.hash.is_empty() {
    return false;
  }
  match op {
    HashOperation::Hdel | HashOperation::Hsetnx => out.result1 > 0 || obj.mutated_by_ttl(),
    // 只读操作零状态变更不落库（mutated_by_ttl 剔除升格写回维持原判定；
    // Hgetall/Hkeys/Hvals 读臂固化通道即落此臂——剔除发生才升格，无剔除时
    // 零落库零 notify 增量，票 wnode-hgetall-envelope-ttl-purge-not-solidified）
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
      | HashOperation::Hstrlen
      | HashOperation::Hexists
      | HashOperation::Hkeys
      | HashOperation::Hvals
      | HashOperation::Hrandfield
      | HashOperation::Httl
      | HashOperation::Hscan
  )
}

pub(crate) struct HashRmwParams<'a> {
  pub key: &'a [u8],
  pub op: HashOperation,
  pub args: &'a [&'a [u8]],
  pub args12: (i32, i32),
}

impl RespServerSession {
  /// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
  ///（协议版本取会话协商版本，C# respProtocolVersion）
  #[inline]
  fn hash_rmw(
    &self,
    store: &wkv::BatchStoreSession<impl Device>,
    key: &[u8],
    op: HashOperation,
    args: &[&[u8]],
    args12: (i32, i32),
    output: &mut Vec<u8>,
  ) -> Rmw {
    self.hash_rmw_missing(
      store,
      HashRmwParams {
        key,
        op,
        args,
        args12,
      },
      output,
      None,
    )
  }

  /// [`Self::hash_rmw`] 的 Missing 短路钩子变体：`on_missing` 为 `Some` 且
  /// 键缺失时经骨架直出常量帧（跳过空对象求值与写回判定），快慢双臂同钩
  /// 同帧（票 wnode-hgetall-envelope-ttl-purge-not-solidified，形制复用
  /// rmw_helpers.rs slow_load_eval 既有 on_missing 钩子）
  #[inline]
  pub(crate) fn hash_rmw_missing(
    &self,
    store: &wkv::BatchStoreSession<impl Device>,
    params: HashRmwParams<'_>,
    output: &mut Vec<u8>,
    on_missing: Option<fn(&mut Vec<u8>)>,
  ) -> Rmw {
    let (arg1, arg2) = params.args12;
    let resp_version = self.resp_protocol_version;
    run_sync_rmw(
      store,
      SyncRmwCmd {
        key: params.key,
        tag: GarnetObjectType::Hash,
        op: params.op,
        args: params.args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        HashObject::from_blob,
        HashObject::new,
        |o: &HashObject| o.is_empty(),
        |o: &HashObject| o.to_blob(),
        move |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
        should_write_back,
      )
      .with_on_missing(on_missing),
    )
  }
}

/// HGETALL/HKEYS/HVALS 缺键短路常量帧：RESP 空数组
///
/// C# HashCommands.cs:HashGetAll(:148)/HashKeys(:513) NOTFOUND →
/// RESP_EMPTYLIST 恒 `*0`（协议恒定，RESP3 下亦不出 `%0`，应答字节零变化；
/// 与 Present 全剔空载荷帧 `%0` 相异）
fn write_missing_empty_array(output: &mut Vec<u8>) {
  output.extend_from_slice(cs::RESP_EMPTYLIST);
}

// operate 通道执行单源已收敛至 rmw_helpers（原本地副本删除）
pub(crate) use crate::resp::objects::object_store_utils::run_operate;
