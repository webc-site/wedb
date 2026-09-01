pub mod admin;
pub mod bloom;
pub mod conn;
pub mod context;
pub mod geo;
pub mod hash;
pub mod hll;
pub mod json;
pub mod key;
pub mod list;
pub mod search;
pub mod set;
pub mod string;
pub mod tdigest;
pub mod timeseries;
pub mod zset;

pub use context::{
  BloomChainMeta, ConnectionContext, CuckooChainMeta, FIELD_EXPIRE_PREFIX_LEN, HashMeta,
  HashSubkeyEncodingMode, HyperLogLogMeta, JsonMeta, JsonStorageFormat, KeyComposer, KeyMeta,
  ListMeta, RedisType, SetMeta, SortedintMeta, StreamConsumerGroupMeta, StreamConsumerMeta,
  StreamMeta, StreamPelEntry, ZSetMeta, bit_op_exec, decode_hash_value, decode_sortable_f64,
  encode_hash_value, encode_sortable_f64, get_bit_from_bytes, is_field_expired,
  normalize_bit_range_to_byte_mask, normalize_range, raw_bitpos, raw_popcount, set_bit_in_bytes,
};

use std::mem::take;
use std::sync::Arc;

use self::admin::handle_admin;
use self::bloom::handle_bloom;
use self::conn::handle_conn;
use self::geo::handle_geo;
use self::hash::handle_hash;
use self::hll::handle_hll;
use self::json::handle_json;
use self::key::handle_key;
use self::list::handle_list;
use self::search::handle_search;
use self::set::handle_set;
use self::string::handle_string;
use self::tdigest::handle_tdigest;
use self::timeseries::handle_timeseries;
use self::zset::handle_zset;
use crate::error::Result;
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::protocol::RespValue;
use crate::redis::sortedint::handle_sortedint;
use crate::redis::stream::handle_stream;

/// Redis 命令处理器主调度器
pub struct RedisHandler;

impl RedisHandler {
  pub async fn handle(
    node: &Arc<RaftNode>,
    ctx: &mut ConnectionContext,
    cmd: RedisCommand,
  ) -> Result<RespValue> {
    // 事务与管道控制处理
    if matches!(cmd, RedisCommand::Multi) {
      if ctx.in_multi {
        return Ok(RespValue::error("ERR MULTI calls can not be nested"));
      }
      ctx.in_multi = true;
      ctx.multi_queue.clear();
      return Ok(RespValue::ok());
    }

    if let RedisCommand::Watch(keys) = cmd {
      if ctx.in_multi {
        return Ok(RespValue::error("ERR WATCH inside MULTI is not allowed"));
      }
      ctx
        .watched_keys
        .extend(keys.into_iter().map(hipstr::HipStr::from));
      return Ok(RespValue::ok());
    }

    if matches!(cmd, RedisCommand::Unwatch) {
      ctx.watched_keys.clear();
      return Ok(RespValue::ok());
    }

    if ctx.in_multi {
      match cmd {
        RedisCommand::Exec => {
          ctx.in_multi = false;
          let queue = take(&mut ctx.multi_queue);
          let mut replies = Vec::with_capacity(queue.len());
          for queued_cmd in queue {
            let res = Self::exec_cmd(node, ctx, queued_cmd).await?;
            replies.push(res);
          }
          return Ok(RespValue::Arr(replies));
        }
        RedisCommand::Discard => {
          ctx.in_multi = false;
          ctx.multi_queue.clear();
          return Ok(RespValue::ok());
        }
        RedisCommand::Quit | RedisCommand::Reset => {
          ctx.in_multi = false;
          ctx.multi_queue.clear();
        }
        _ => {
          ctx.multi_queue.push(cmd);
          return Ok(RespValue::queued());
        }
      }
    }

    Self::exec_cmd(node, ctx, cmd).await
  }

  pub async fn exec_cmd(
    node: &Arc<RaftNode>,
    ctx: &mut ConnectionContext,
    cmd: RedisCommand,
  ) -> Result<RespValue> {
    match cmd {
      // 1. 连接、认证与系统服务
      RedisCommand::Ping(_)
      | RedisCommand::Echo(_)
      | RedisCommand::Hello(_)
      | RedisCommand::Quit
      | RedisCommand::Auth { .. }
      | RedisCommand::Select(_)
      | RedisCommand::NamespaceAdd(_, _)
      | RedisCommand::NamespaceSet(_, _)
      | RedisCommand::NamespaceDel(_)
      | RedisCommand::NamespaceGet(_)
      | RedisCommand::NamespaceCurrent
      | RedisCommand::SwapDb(_, _)
      | RedisCommand::Move(_, _)
      | RedisCommand::MoveX { .. }
      | RedisCommand::Command
      | RedisCommand::ConfigGet(_)
      | RedisCommand::ConfigSet(_, _)
      | RedisCommand::Time
      | RedisCommand::ClientId
      | RedisCommand::ClientGetName
      | RedisCommand::ClientSetName(_)
      | RedisCommand::ClientList
      | RedisCommand::ClientInfo
      | RedisCommand::ClientKill(_)
      | RedisCommand::ClientPause(_)
      | RedisCommand::ClientUnpause
      | RedisCommand::ClientUnblock { .. }
      | RedisCommand::ClientTracking { .. }
      | RedisCommand::ClientTrackingInfo
      | RedisCommand::ClientGetRedir
      | RedisCommand::ClientSetInfo(_, _)
      | RedisCommand::ClientNoTouch(_)
      | RedisCommand::ClientNoEvict(_)
      | RedisCommand::ClientReply(_)
      | RedisCommand::ClientHelp
      | RedisCommand::Info(_)
      | RedisCommand::Role
      | RedisCommand::Slowlog
      | RedisCommand::KProfile
      | RedisCommand::PerfLog
      | RedisCommand::Stats
      | RedisCommand::Latency(_)
      | RedisCommand::MemoryUsage(_)
      | RedisCommand::Monitor
      | RedisCommand::Shutdown
      | RedisCommand::Reset
      | RedisCommand::Debug(_)
      | RedisCommand::Disk(_)
      | RedisCommand::Rdb(_)
      | RedisCommand::Sst(_)
      | RedisCommand::Compact
      | RedisCommand::FlushMemTable
      | RedisCommand::FlushBlockCache
      | RedisCommand::Bgsave
      | RedisCommand::FlushBackup
      | RedisCommand::Lastsave
      | RedisCommand::SlaveOf(_, _)
      | RedisCommand::ReplicaOf(_, _)
      | RedisCommand::ApplyBatch(_)
      | RedisCommand::PollUpdates
      | RedisCommand::Dump(_)
      | RedisCommand::Restore { .. } => Box::pin(handle_conn(node, ctx, cmd)).await,

      // 2. 字符串与位图
      RedisCommand::Get(_)
      | RedisCommand::Set { .. }
      | RedisCommand::SetNx(_, _)
      | RedisCommand::SetEx(_, _, _)
      | RedisCommand::PSetEx(_, _, _)
      | RedisCommand::GetSet(_, _)
      | RedisCommand::GetDel(_)
      | RedisCommand::GetEx { .. }
      | RedisCommand::MGet(_)
      | RedisCommand::MSet(_)
      | RedisCommand::MSetNx(_)
      | RedisCommand::MSetEx { .. }
      | RedisCommand::Incr(_)
      | RedisCommand::Decr(_)
      | RedisCommand::IncrBy(_, _)
      | RedisCommand::DecrBy(_, _)
      | RedisCommand::IncrByFloat(_, _)
      | RedisCommand::IncrEx { .. }
      | RedisCommand::StrLen(_)
      | RedisCommand::Append(_, _)
      | RedisCommand::GetRange(_, _, _)
      | RedisCommand::SetRange(_, _, _)
      | RedisCommand::Digest(_)
      | RedisCommand::DelEx { .. }
      | RedisCommand::Cas { .. }
      | RedisCommand::Cad { .. }
      | RedisCommand::Lcs { .. }
      | RedisCommand::GetBit(_, _)
      | RedisCommand::SetBit(_, _, _)
      | RedisCommand::BitCount { .. }
      | RedisCommand::BitPos { .. }
      | RedisCommand::BitOp { .. }
      | RedisCommand::BitField { .. }
      | RedisCommand::BitFieldRo { .. } => Box::pin(handle_string(node, ctx, cmd)).await,

      // 3. 键生命周期与扫描
      RedisCommand::Del(_)
      | RedisCommand::Unlink(_)
      | RedisCommand::Exists(_)
      | RedisCommand::Keys(_)
      | RedisCommand::Scan { .. }
      | RedisCommand::ScanPrefix { .. }
      | RedisCommand::DbSize
      | RedisCommand::FlushDb
      | RedisCommand::FlushAll
      | RedisCommand::Type(_)
      | RedisCommand::Ttl(_)
      | RedisCommand::Pttl(_)
      | RedisCommand::ExpireTime(_)
      | RedisCommand::PExpireTime(_)
      | RedisCommand::Expire(_, _)
      | RedisCommand::PExpire(_, _)
      | RedisCommand::ExpireAt(_, _)
      | RedisCommand::PExpireAt(_, _)
      | RedisCommand::Persist(_)
      | RedisCommand::Rename(_, _)
      | RedisCommand::RenameNx(_, _)
      | RedisCommand::Copy { .. }
      | RedisCommand::RandomKey
      | RedisCommand::Touch(_)
      | RedisCommand::Sort { .. }
      | RedisCommand::SortRo { .. }
      | RedisCommand::Object { .. }
      | RedisCommand::KMetaData(_) => Box::pin(handle_key(node, ctx, cmd)).await,

      // 4. 哈希表 (Hash)
      RedisCommand::HSet(_, _)
      | RedisCommand::HSetNx { .. }
      | RedisCommand::HGet(_, _)
      | RedisCommand::HGetDel { .. }
      | RedisCommand::HDel(_, _)
      | RedisCommand::HExists(_, _)
      | RedisCommand::HLen(_)
      | RedisCommand::HStrLen(_, _)
      | RedisCommand::HIncrBy(_, _, _)
      | RedisCommand::HIncrByFloat(_, _, _)
      | RedisCommand::HMGet(_, _)
      | RedisCommand::HMSet(_, _)
      | RedisCommand::HGetAll(_)
      | RedisCommand::HKeys(_)
      | RedisCommand::HVals(_)
      | RedisCommand::HRandField { .. }
      | RedisCommand::HScan { .. }
      | RedisCommand::HExpire { .. }
      | RedisCommand::HPExpire { .. }
      | RedisCommand::HExpireAt { .. }
      | RedisCommand::HPExpireAt { .. }
      | RedisCommand::HTtl { .. }
      | RedisCommand::HPTtl { .. }
      | RedisCommand::HExpireTime { .. }
      | RedisCommand::HPExpireTime { .. }
      | RedisCommand::HPersist { .. }
      | RedisCommand::HSetExpire { .. }
      | RedisCommand::HGetEx { .. }
      | RedisCommand::HRangeByLex { .. } => Box::pin(handle_hash(node, ctx, cmd)).await,

      // 5. 列表 (List)
      RedisCommand::LPush(_, _)
      | RedisCommand::RPush(_, _)
      | RedisCommand::LPushX(_, _)
      | RedisCommand::RPushX(_, _)
      | RedisCommand::LPop(_, _)
      | RedisCommand::RPop(_, _)
      | RedisCommand::LLen(_)
      | RedisCommand::LIndex(_, _)
      | RedisCommand::LSet { .. }
      | RedisCommand::LRange(_, _, _)
      | RedisCommand::LTrim(_, _, _)
      | RedisCommand::LRem(_, _, _)
      | RedisCommand::LInsert { .. }
      | RedisCommand::LMove { .. }
      | RedisCommand::LMoveM { .. }
      | RedisCommand::RPopLPush(_, _)
      | RedisCommand::LPos { .. }
      | RedisCommand::BLPop(_, _)
      | RedisCommand::BRPop(_, _)
      | RedisCommand::BLMove { .. }
      | RedisCommand::BLMoveM { .. }
      | RedisCommand::LMPop { .. }
      | RedisCommand::BLMPop { .. } => Box::pin(handle_list(node, ctx, cmd)).await,

      // 6. 集合 (Set)
      RedisCommand::SAdd(_, _)
      | RedisCommand::SRem(_, _)
      | RedisCommand::SIsMember(_, _)
      | RedisCommand::SMIsMember(_, _)
      | RedisCommand::SCard(_)
      | RedisCommand::SMembers(_)
      | RedisCommand::SPop(_, _)
      | RedisCommand::SRandMember(_, _)
      | RedisCommand::SMove { .. }
      | RedisCommand::SUnion(_)
      | RedisCommand::SInter(_)
      | RedisCommand::SDiff(_)
      | RedisCommand::SInterCard { .. }
      | RedisCommand::SDiffCard { .. }
      | RedisCommand::SUnionCard { .. }
      | RedisCommand::SDiffStore(_, _)
      | RedisCommand::SUnionStore(_, _)
      | RedisCommand::SInterStore(_, _)
      | RedisCommand::SScan { .. } => Box::pin(handle_set(node, ctx, cmd)).await,

      // 7. 有序集合 (Sorted Set)
      RedisCommand::ZAdd { .. }
      | RedisCommand::ZRem(_, _)
      | RedisCommand::ZScore(_, _)
      | RedisCommand::ZMScore(_, _)
      | RedisCommand::ZIncrBy(_, _, _)
      | RedisCommand::ZCard(_)
      | RedisCommand::ZCount(_, _, _)
      | RedisCommand::ZLexCount(_, _, _)
      | RedisCommand::ZRange { .. }
      | RedisCommand::ZRevRange(_, _, _, _)
      | RedisCommand::ZRangeByScore { .. }
      | RedisCommand::ZRangeByLex { .. }
      | RedisCommand::ZRank { .. }
      | RedisCommand::ZRevRank { .. }
      | RedisCommand::ZPopMin(_, _)
      | RedisCommand::ZPopMax(_, _)
      | RedisCommand::BZPopMin(_, _)
      | RedisCommand::BZPopMax(_, _)
      | RedisCommand::ZMPop { .. }
      | RedisCommand::BZMPop { .. }
      | RedisCommand::ZInter { .. }
      | RedisCommand::ZInterCard { .. }
      | RedisCommand::ZInterStore { .. }
      | RedisCommand::ZUnion { .. }
      | RedisCommand::ZUnionStore { .. }
      | RedisCommand::ZDiff { .. }
      | RedisCommand::ZDiffStore { .. }
      | RedisCommand::ZRemRangeByRank(_, _, _)
      | RedisCommand::ZRemRangeByScore(_, _, _)
      | RedisCommand::ZRemRangeByLex(_, _, _)
      | RedisCommand::ZRevRangeByScore { .. }
      | RedisCommand::ZRevRangeByLex { .. }
      | RedisCommand::ZRandMember { .. }
      | RedisCommand::ZRangeStore { .. }
      | RedisCommand::ZScan { .. } => Box::pin(handle_zset(node, ctx, cmd)).await,

      // 8. 基数统计 (HyperLogLog)
      RedisCommand::PfAdd(..)
      | RedisCommand::PfCount(..)
      | RedisCommand::PfMerge(..)
      | RedisCommand::PfSelfTest => Box::pin(handle_hll(node, ctx, cmd)).await,

      // 9. 布隆与布谷鸟过滤器 (Bloom / Cuckoo Filter)
      RedisCommand::BfReserve { .. }
      | RedisCommand::BfAdd(..)
      | RedisCommand::BfMAdd(..)
      | RedisCommand::BfInsert { .. }
      | RedisCommand::BfExists(..)
      | RedisCommand::BfMExists(..)
      | RedisCommand::BfInfo { .. }
      | RedisCommand::BfCard(..)
      | RedisCommand::CfReserve { .. }
      | RedisCommand::CfAdd(..)
      | RedisCommand::CfAddNx(..)
      | RedisCommand::CfInsert { .. }
      | RedisCommand::CfInsertNx { .. }
      | RedisCommand::CfExists(..)
      | RedisCommand::CfMExists(..)
      | RedisCommand::CfDel(..)
      | RedisCommand::CfCount(..)
      | RedisCommand::CfInfo(..) => Box::pin(handle_bloom(node, ctx, cmd)).await,

      // 10. 有序整型集合 (SortedInt)
      RedisCommand::SiAdd(..)
      | RedisCommand::SiRem(..)
      | RedisCommand::SiCard(..)
      | RedisCommand::SiExists(..)
      | RedisCommand::SiRange { .. }
      | RedisCommand::SiRevRange { .. }
      | RedisCommand::SiRangeByValue { .. }
      | RedisCommand::SiRevRangeByValue { .. } => Box::pin(handle_sortedint(node, ctx, cmd)).await,

      // 11. 地理位置 (Geo)
      RedisCommand::GeoAdd { .. }
      | RedisCommand::GeoDist { .. }
      | RedisCommand::GeoHash(..)
      | RedisCommand::GeoPos(..)
      | RedisCommand::GeoRadius { .. }
      | RedisCommand::GeoRadiusByMember { .. }
      | RedisCommand::GeoSearch { .. }
      | RedisCommand::GeoSearchStore { .. } => Box::pin(handle_geo(node, ctx, cmd)).await,

      // 12. 流数据 (Stream)
      RedisCommand::XAdd { .. }
      | RedisCommand::XLen(_)
      | RedisCommand::XRange { .. }
      | RedisCommand::XRevRange { .. }
      | RedisCommand::XDel(..)
      | RedisCommand::XDelEx { .. }
      | RedisCommand::XTrim { .. }
      | RedisCommand::XGroup(..)
      | RedisCommand::XRead { .. }
      | RedisCommand::XReadGroup { .. }
      | RedisCommand::XAck(..)
      | RedisCommand::XAckDel { .. }
      | RedisCommand::XNack { .. }
      | RedisCommand::XPending { .. }
      | RedisCommand::XClaim { .. }
      | RedisCommand::XAutoClaim { .. }
      | RedisCommand::XSetId { .. }
      | RedisCommand::XInfo(..)
      | RedisCommand::XInfoStream { .. } => Box::pin(handle_stream(node, ctx, cmd)).await,

      // 13. RedisJSON
      RedisCommand::JsonSet { .. }
      | RedisCommand::JsonGet { .. }
      | RedisCommand::JsonDel { .. }
      | RedisCommand::JsonType { .. }
      | RedisCommand::JsonNumIncrBy { .. }
      | RedisCommand::JsonNumMultBy { .. }
      | RedisCommand::JsonStrAppend { .. }
      | RedisCommand::JsonStrLen { .. }
      | RedisCommand::JsonArrAppend { .. }
      | RedisCommand::JsonArrPop { .. }
      | RedisCommand::JsonArrInsert { .. }
      | RedisCommand::JsonArrLen { .. }
      | RedisCommand::JsonArrTrim { .. }
      | RedisCommand::JsonArrIndex { .. }
      | RedisCommand::JsonObjKeys { .. }
      | RedisCommand::JsonObjLen { .. }
      | RedisCommand::JsonToggle { .. }
      | RedisCommand::JsonClear { .. }
      | RedisCommand::JsonMGet { .. }
      | RedisCommand::JsonMSet(..)
      | RedisCommand::JsonMerge { .. }
      | RedisCommand::JsonResp { .. }
      | RedisCommand::JsonInfo(..)
      | RedisCommand::JsonDebug(..) => Box::pin(handle_json(node, ctx, cmd)).await,

      // 14. 分位数统计 (TDigest)
      RedisCommand::TDigestCreate { .. }
      | RedisCommand::TDigestReset(..)
      | RedisCommand::TDigestAdd(..)
      | RedisCommand::TDigestMerge { .. }
      | RedisCommand::TDigestQuantile(..)
      | RedisCommand::TDigestCdf(..)
      | RedisCommand::TDigestRank(..)
      | RedisCommand::TDigestRevRank(..)
      | RedisCommand::TDigestByRank(..)
      | RedisCommand::TDigestByRevRank(..)
      | RedisCommand::TDigestMin(..)
      | RedisCommand::TDigestMax(..)
      | RedisCommand::TDigestTrimmedMean(..)
      | RedisCommand::TDigestInfo(..) => Box::pin(handle_tdigest(node, ctx, cmd)).await,

      // 15. 时间序列 (TimeSeries)
      RedisCommand::TsCreate { .. }
      | RedisCommand::TsAlter { .. }
      | RedisCommand::TsAdd { .. }
      | RedisCommand::TsMAdd(..)
      | RedisCommand::TsDecrBy { .. }
      | RedisCommand::TsIncrBy { .. }
      | RedisCommand::TsCreateRule(..)
      | RedisCommand::TsRange { .. }
      | RedisCommand::TsRevRange { .. }
      | RedisCommand::TsMRange { .. }
      | RedisCommand::TsMRevRange { .. }
      | RedisCommand::TsGet { .. }
      | RedisCommand::TsMGet { .. }
      | RedisCommand::TsInfo(..)
      | RedisCommand::TsQueryIndex(..)
      | RedisCommand::TsDel(..) => Box::pin(handle_timeseries(node, ctx, cmd)).await,

      // 16. 全文检索 (RediSearch)
      RedisCommand::FtCreate { .. }
      | RedisCommand::FtSearch { .. }
      | RedisCommand::FtSearchSql { .. }
      | RedisCommand::FtExplain { .. }
      | RedisCommand::FtExplainSql { .. }
      | RedisCommand::FtInfo(..)
      | RedisCommand::FtList
      | RedisCommand::FtDropIndex { .. }
      | RedisCommand::FtAliasAdd { .. }
      | RedisCommand::FtAliasDel { .. }
      | RedisCommand::FtTagVals(..) => Box::pin(handle_search(node, ctx, cmd)).await,

      // 17 & 18. 发布订阅 (PubSub)、分布式 Raft 与集群管理
      _ => Box::pin(handle_admin(node, ctx, cmd)).await,
    }
  }
}
