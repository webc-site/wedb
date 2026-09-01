pub mod bloom;
pub mod geo;
pub mod hll;
pub mod json;
pub mod pubsub;
pub mod search;
pub mod sortedint;
pub mod stream;
pub mod tdigest;
pub mod timeseries;

use std::sync::Arc;
use webc_cmd::{Cmd, ConnectionContext};
use wedb_embed::{Error, Result, WeDb};
use wedb_resp::RespValue;

pub use self::bloom::handle_bloom;
pub use self::geo::handle_geo;
pub use self::hll::handle_hll;
pub use self::json::handle_json;
pub use self::pubsub::handle_pubsub;
pub use self::search::handle_search;
pub use self::sortedint::handle_sortedint;
pub use self::stream::handle_stream;
pub use self::tdigest::handle_tdigest;
pub use self::timeseries::handle_timeseries;

/// 调度扩展数据结构与高级模块命令
pub async fn handle_ext(
  db: &Arc<WeDb>,
  ctx: &mut ConnectionContext,
  cmd: Cmd,
) -> Result<RespValue> {
  match cmd {
    // ================= 1. 空间地理位置 (Geo) =================
    Cmd::GeoAdd { .. }
    | Cmd::GeoDist { .. }
    | Cmd::GeoHash(_, _)
    | Cmd::GeoPos(_, _)
    | Cmd::GeoRadius { .. }
    | Cmd::GeoRadiusByMember { .. }
    | Cmd::GeoSearch { .. }
    | Cmd::GeoSearchStore { .. } => handle_geo(db, cmd).await,

    // ================= 2. 布隆与布谷鸟过滤器 (Bloom / Cuckoo) =================
    Cmd::BfReserve { .. }
    | Cmd::BfAdd(_, _)
    | Cmd::BfMAdd(_, _)
    | Cmd::BfInsert { .. }
    | Cmd::BfExists(_, _)
    | Cmd::BfMExists(_, _)
    | Cmd::BfInfo { .. }
    | Cmd::BfCard(_)
    | Cmd::CfReserve { .. }
    | Cmd::CfAdd(_, _)
    | Cmd::CfAddNx(_, _)
    | Cmd::CfInsert { .. }
    | Cmd::CfInsertNx { .. }
    | Cmd::CfExists(_, _)
    | Cmd::CfMExists(_, _)
    | Cmd::CfDel(_, _)
    | Cmd::CfCount(_, _)
    | Cmd::CfInfo(_) => handle_bloom(db, cmd).await,

    // ================= 3. 基数估计 (HyperLogLog) =================
    Cmd::PfAdd(_, _) | Cmd::PfCount(_) | Cmd::PfMerge(_, _) | Cmd::PfSelfTest => {
      handle_hll(db, cmd).await
    }

    // ================= 4. JSON 文档 (RedisJSON) =================
    Cmd::JsonSet { .. }
    | Cmd::JsonGet { .. }
    | Cmd::JsonDel { .. }
    | Cmd::JsonType { .. }
    | Cmd::JsonArrAppend { .. }
    | Cmd::JsonArrInsert { .. }
    | Cmd::JsonArrTrim { .. }
    | Cmd::JsonClear { .. }
    | Cmd::JsonToggle { .. }
    | Cmd::JsonArrLen { .. }
    | Cmd::JsonMerge { .. }
    | Cmd::JsonObjKeys { .. }
    | Cmd::JsonArrPop { .. }
    | Cmd::JsonArrIndex { .. }
    | Cmd::JsonNumIncrBy { .. }
    | Cmd::JsonNumMultBy { .. }
    | Cmd::JsonObjLen { .. }
    | Cmd::JsonStrAppend { .. }
    | Cmd::JsonStrLen { .. }
    | Cmd::JsonMGet { .. }
    | Cmd::JsonMSet(_)
    | Cmd::JsonDebug(_, _)
    | Cmd::JsonResp { .. }
    | Cmd::JsonInfo(_) => handle_json(db, cmd).await,

    // ================= 5. 有序整型集合 (SortedInt) =================
    Cmd::SiAdd(_, _)
    | Cmd::SiRem(_, _)
    | Cmd::SiCard(_)
    | Cmd::SiExists(_, _)
    | Cmd::SiRange { .. }
    | Cmd::SiRevRange { .. }
    | Cmd::SiRangeByValue { .. }
    | Cmd::SiRevRangeByValue { .. } => handle_sortedint(db, cmd).await,

    // ================= 6. 流数据 (Stream) =================
    Cmd::XAdd { .. }
    | Cmd::XLen(_)
    | Cmd::XRange { .. }
    | Cmd::XRevRange { .. }
    | Cmd::XDel(_, _)
    | Cmd::XDelEx { .. }
    | Cmd::XTrim { .. }
    | Cmd::XRead { .. }
    | Cmd::XInfo(_, _)
    | Cmd::XInfoStream { .. }
    | Cmd::XAck(_, _, _)
    | Cmd::XAckDel { .. }
    | Cmd::XClaim { .. }
    | Cmd::XAutoClaim { .. }
    | Cmd::XGroup(_)
    | Cmd::XPending { .. }
    | Cmd::XReadGroup { .. }
    | Cmd::XSetId { .. }
    | Cmd::XNack { .. } => handle_stream(db, cmd).await,

    // ================= 7. 分位数估计 (TDigest) =================
    Cmd::TDigestCreate { .. }
    | Cmd::TDigestAdd(_, _)
    | Cmd::TDigestQuantile(_, _)
    | Cmd::TDigestCdf(_, _)
    | Cmd::TDigestMin(_)
    | Cmd::TDigestMax(_)
    | Cmd::TDigestRank(_, _)
    | Cmd::TDigestRevRank(_, _)
    | Cmd::TDigestByRank(_, _)
    | Cmd::TDigestByRevRank(_, _)
    | Cmd::TDigestTrimmedMean(_, _, _)
    | Cmd::TDigestReset(_)
    | Cmd::TDigestMerge { .. }
    | Cmd::TDigestInfo(_) => handle_tdigest(db, cmd).await,

    // ================= 8. 时间序列 (TimeSeries) =================
    Cmd::TsCreate { .. }
    | Cmd::TsAlter { .. }
    | Cmd::TsAdd { .. }
    | Cmd::TsMAdd(_)
    | Cmd::TsRange { .. }
    | Cmd::TsRevRange { .. }
    | Cmd::TsInfo(_)
    | Cmd::TsGet { .. }
    | Cmd::TsCreateRule(_, _, _, _)
    | Cmd::TsMGet { .. }
    | Cmd::TsMRange { .. }
    | Cmd::TsMRevRange { .. }
    | Cmd::TsIncrBy { .. }
    | Cmd::TsDecrBy { .. }
    | Cmd::TsDel(_, _, _)
    | Cmd::TsQueryIndex(_) => handle_timeseries(db, cmd).await,

    // ================= 9. 全文检索 (RediSearch) =================
    Cmd::FtCreate { .. }
    | Cmd::FtSearch { .. }
    | Cmd::FtSearchSql { .. }
    | Cmd::FtExplain { .. }
    | Cmd::FtExplainSql { .. }
    | Cmd::FtInfo(_)
    | Cmd::FtList
    | Cmd::FtDropIndex { .. }
    | Cmd::FtAliasAdd { .. }
    | Cmd::FtAliasDel { .. }
    | Cmd::FtTagVals(_, _) => handle_search(db, cmd).await,

    // ================= 10. 发布订阅 (PubSub) =================
    Cmd::Publish(_, _)
    | Cmd::MPublish(_)
    | Cmd::Subscribe(_)
    | Cmd::Unsubscribe(_)
    | Cmd::PSubscribe(_)
    | Cmd::PUnsubscribe(_)
    | Cmd::SSubscribe(_)
    | Cmd::SUnsubscribe(_)
    | Cmd::PubSub(_) => handle_pubsub(db, ctx, cmd).await,

    _ => Err(Error::internal("unsupported extended command")),
  }
}
