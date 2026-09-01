pub mod conn;
pub mod ext;
pub mod hash;
pub mod key;
pub mod list;
pub mod resp_util;
pub mod set;
pub mod string;
pub mod zset;

use std::mem;
use std::sync::Arc;

use self::conn::handle_conn;
use self::ext::handle_ext;
use self::hash::handle_hash;
use self::key::handle_key;
use self::list::handle_list;
use self::set::handle_set;
use self::string::handle_string;
use self::zset::handle_zset;
use webc_cmd::{Cmd, CmdHandler, ConnectionContext};
use wedb_embed::{Result, WeDb};
use wedb_resp::RespValue;

/// 单机 Redis 命令处理器
pub struct StandaloneHandler {
  pub db: Arc<WeDb>,
}

impl StandaloneHandler {
  pub fn new(db: Arc<WeDb>) -> Self {
    Self { db }
  }
}

impl CmdHandler for StandaloneHandler {
  async fn handle(&self, ctx: &mut ConnectionContext, cmd: Cmd) -> RespValue {
    handle_cmd_with_ctx(&self.db, ctx, cmd).await
  }
}

/// 处理从 RESP 解析出的强类型 Cmd（默认上下文）
pub async fn handle_cmd(db: &Arc<WeDb>, cmd: Cmd) -> RespValue {
  let mut ctx = ConnectionContext::default();
  handle_cmd_with_ctx(db, &mut ctx, cmd).await
}

/// 带连接上下文处理强类型 Cmd（支持事务队列控制）
pub async fn handle_cmd_with_ctx(
  db: &Arc<WeDb>,
  ctx: &mut ConnectionContext,
  cmd: Cmd,
) -> RespValue {
  // 事务控制与监听处理
  if matches!(cmd, Cmd::Multi) {
    if ctx.in_multi {
      return RespValue::error("ERR MULTI calls can not be nested");
    }
    ctx.in_multi = true;
    ctx.multi_queue.clear();
    return RespValue::ok();
  }

  if let Cmd::Watch(keys) = cmd {
    if ctx.in_multi {
      return RespValue::error("ERR WATCH inside MULTI is not allowed");
    }
    ctx
      .watched_keys
      .extend(keys.into_iter().map(hipstr::HipStr::from));
    return RespValue::ok();
  }

  if matches!(cmd, Cmd::Unwatch) {
    ctx.watched_keys.clear();
    return RespValue::ok();
  }

  if ctx.in_multi {
    match cmd {
      Cmd::Exec => {
        ctx.in_multi = false;
        let queue = mem::take(&mut ctx.multi_queue);
        let mut replies = Vec::with_capacity(queue.len());
        for queued_cmd in queue {
          let res = Box::pin(handle_cmd_with_ctx(db, ctx, queued_cmd)).await;
          replies.push(res);
        }
        return RespValue::Arr(replies);
      }
      Cmd::Discard => {
        ctx.in_multi = false;
        ctx.multi_queue.clear();
        return RespValue::ok();
      }
      Cmd::Quit | Cmd::Reset => {
        ctx.in_multi = false;
        ctx.multi_queue.clear();
      }
      _ => {
        ctx.multi_queue.push(cmd);
        return RespValue::queued();
      }
    }
  }

  match dispatch(db, ctx, cmd).await {
    Ok(val) => val,
    Err(e) => RespValue::error(format!("ERR {e}")),
  }
}

async fn dispatch(db: &Arc<WeDb>, ctx: &mut ConnectionContext, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    // ================= 1. 连接、认证、命名空间与系统服务 =================
    Cmd::Ping(_)
    | Cmd::Echo(_)
    | Cmd::Hello(_)
    | Cmd::Quit
    | Cmd::Select(_)
    | Cmd::Auth { .. }
    | Cmd::NamespaceAdd(_, _)
    | Cmd::NamespaceSet(_, _)
    | Cmd::NamespaceDel(_)
    | Cmd::NamespaceGet(_)
    | Cmd::NamespaceCurrent
    | Cmd::Command
    | Cmd::ConfigGet(_)
    | Cmd::ConfigSet(_, _)
    | Cmd::Time
    | Cmd::ClientId
    | Cmd::ClientGetName
    | Cmd::ClientSetName(_)
    | Cmd::ClientList
    | Cmd::ClientInfo
    | Cmd::ClientKill(_)
    | Cmd::ClientPause(_)
    | Cmd::ClientUnpause
    | Cmd::ClientUnblock { .. }
    | Cmd::ClientTracking { .. }
    | Cmd::ClientTrackingInfo
    | Cmd::ClientGetRedir
    | Cmd::ClientSetInfo(_, _)
    | Cmd::ClientNoTouch(_)
    | Cmd::ClientNoEvict(_)
    | Cmd::ClientReply(_)
    | Cmd::ClientHelp
    | Cmd::Info(_)
    | Cmd::Role
    | Cmd::Slowlog
    | Cmd::MemoryUsage(_)
    | Cmd::Reset
    | Cmd::Stats => handle_conn(db, ctx, cmd).await,

    // ================= 2. 字符串 (String) 与位图 (Bitmap) =================
    Cmd::Get(_)
    | Cmd::Set { .. }
    | Cmd::SetNx(_, _)
    | Cmd::SetEx(_, _, _)
    | Cmd::PSetEx(_, _, _)
    | Cmd::GetSet(_, _)
    | Cmd::GetDel(_)
    | Cmd::GetEx { .. }
    | Cmd::MGet(_)
    | Cmd::MSet(_)
    | Cmd::MSetNx(_)
    | Cmd::MSetEx { .. }
    | Cmd::Incr(_)
    | Cmd::Decr(_)
    | Cmd::IncrBy(_, _)
    | Cmd::DecrBy(_, _)
    | Cmd::IncrByFloat(_, _)
    | Cmd::IncrEx { .. }
    | Cmd::Digest(_)
    | Cmd::DelEx { .. }
    | Cmd::Cas { .. }
    | Cmd::Cad { .. }
    | Cmd::Lcs { .. }
    | Cmd::BitField { .. }
    | Cmd::BitFieldRo { .. }
    | Cmd::StrLen(_)
    | Cmd::Append(_, _)
    | Cmd::GetRange(_, _, _)
    | Cmd::SetRange(_, _, _)
    | Cmd::SetBit(_, _, _)
    | Cmd::GetBit(_, _)
    | Cmd::BitCount { .. }
    | Cmd::BitPos { .. }
    | Cmd::BitOp { .. } => handle_string(db, cmd).await,

    // ================= 3. Key 生命周期、TTL、过期与排序 =================
    Cmd::Del(_)
    | Cmd::Unlink(_)
    | Cmd::Exists(_)
    | Cmd::FlushAll
    | Cmd::FlushDb
    | Cmd::Type(_)
    | Cmd::Ttl(_)
    | Cmd::Pttl(_)
    | Cmd::ExpireTime(_)
    | Cmd::PExpireTime(_)
    | Cmd::Expire(_, _)
    | Cmd::PExpire(_, _)
    | Cmd::ExpireAt(_, _)
    | Cmd::PExpireAt(_, _)
    | Cmd::Persist(_)
    | Cmd::Keys(_)
    | Cmd::Scan { .. }
    | Cmd::ScanPrefix { .. }
    | Cmd::DbSize
    | Cmd::Rename(_, _)
    | Cmd::RenameNx(_, _)
    | Cmd::Copy { .. }
    | Cmd::Touch(_)
    | Cmd::RandomKey
    | Cmd::Object { .. }
    | Cmd::KMetaData(_)
    | Cmd::Sort { .. }
    | Cmd::SortRo { .. } => handle_key(db, cmd).await,

    // ================= 4. 哈希 (Hash) =================
    Cmd::HSet(_, _)
    | Cmd::HSetNx(_, _, _)
    | Cmd::HGet(_, _)
    | Cmd::HMGet(_, _)
    | Cmd::HGetAll(_)
    | Cmd::HDel(_, _)
    | Cmd::HGetDel { .. }
    | Cmd::HLen(_)
    | Cmd::HKeys(_)
    | Cmd::HVals(_)
    | Cmd::HExists(_, _)
    | Cmd::HIncrBy(_, _, _)
    | Cmd::HIncrByFloat(_, _, _)
    | Cmd::HStrLen(_, _)
    | Cmd::HRandField { .. }
    | Cmd::HExpire { .. }
    | Cmd::HPExpire { .. }
    | Cmd::HExpireAt { .. }
    | Cmd::HPExpireAt { .. }
    | Cmd::HTtl { .. }
    | Cmd::HPTtl { .. }
    | Cmd::HExpireTime { .. }
    | Cmd::HPExpireTime { .. }
    | Cmd::HPersist { .. }
    | Cmd::HSetExpire { .. }
    | Cmd::HGetEx { .. }
    | Cmd::HRangeByLex { .. }
    | Cmd::HScan { .. } => handle_hash(db, cmd).await,

    // ================= 5. 列表 (List) =================
    Cmd::LPush(_, _)
    | Cmd::RPush(_, _)
    | Cmd::LPushX(_, _)
    | Cmd::RPushX(_, _)
    | Cmd::LPop(_, _)
    | Cmd::RPop(_, _)
    | Cmd::LLen(_)
    | Cmd::LRange(_, _, _)
    | Cmd::LIndex(_, _)
    | Cmd::LSet(_, _, _)
    | Cmd::LTrim(_, _, _)
    | Cmd::LRem(_, _, _)
    | Cmd::LInsert { .. }
    | Cmd::LMove { .. }
    | Cmd::LMoveM { .. }
    | Cmd::RPopLPush(_, _)
    | Cmd::LPos { .. }
    | Cmd::BLPop(_, _)
    | Cmd::BRPop(_, _)
    | Cmd::BLMove { .. }
    | Cmd::BLMoveM { .. }
    | Cmd::LMPop { .. }
    | Cmd::BLMPop { .. } => handle_list(db, cmd).await,

    // ================= 6. 集合 (Set) =================
    Cmd::SAdd(_, _)
    | Cmd::SRem(_, _)
    | Cmd::SCard(_)
    | Cmd::SMembers(_)
    | Cmd::SIsMember(_, _)
    | Cmd::SMIsMember(_, _)
    | Cmd::SPop(_, _)
    | Cmd::SRandMember(_, _)
    | Cmd::SMove { .. }
    | Cmd::SDiff(_)
    | Cmd::SUnion(_)
    | Cmd::SInter(_)
    | Cmd::SInterCard { .. }
    | Cmd::SDiffCard { .. }
    | Cmd::SUnionCard { .. }
    | Cmd::SDiffStore(_, _)
    | Cmd::SUnionStore(_, _)
    | Cmd::SInterStore(_, _)
    | Cmd::SScan { .. } => handle_set(db, cmd).await,

    // ================= 7. 有序集合 (ZSet) =================
    Cmd::ZAdd { .. }
    | Cmd::ZScore(_, _)
    | Cmd::ZMScore(_, _)
    | Cmd::ZCard(_)
    | Cmd::ZRem(_, _)
    | Cmd::ZIncrBy(_, _, _)
    | Cmd::ZRank { .. }
    | Cmd::ZRevRank { .. }
    | Cmd::ZRange { .. }
    | Cmd::ZRevRange(_, _, _, _)
    | Cmd::ZRangeByScore { .. }
    | Cmd::ZRevRangeByScore { .. }
    | Cmd::ZRangeByLex { .. }
    | Cmd::ZRevRangeByLex { .. }
    | Cmd::ZCount(_, _, _)
    | Cmd::ZLexCount(_, _, _)
    | Cmd::ZPopMin(_, _)
    | Cmd::ZPopMax(_, _)
    | Cmd::BZPopMin(_, _)
    | Cmd::BZPopMax(_, _)
    | Cmd::ZMPop { .. }
    | Cmd::BZMPop { .. }
    | Cmd::ZInter { .. }
    | Cmd::ZInterStore { .. }
    | Cmd::ZInterCard { .. }
    | Cmd::ZUnion { .. }
    | Cmd::ZUnionStore { .. }
    | Cmd::ZDiff { .. }
    | Cmd::ZDiffStore { .. }
    | Cmd::ZRemRangeByRank(_, _, _)
    | Cmd::ZRemRangeByScore(_, _, _)
    | Cmd::ZRemRangeByLex(_, _, _)
    | Cmd::ZRandMember { .. }
    | Cmd::ZRangeStore { .. }
    | Cmd::ZScan { .. } => handle_zset(db, cmd).await,

    // ================= 8. 扩展数据结构与高级模块 =================
    _ => handle_ext(db, ctx, cmd).await,
  }
}
