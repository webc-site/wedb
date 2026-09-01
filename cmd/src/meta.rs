use crate::slot::Crc;
use crate::types::Cmd;

impl Cmd {
  /// 是否为写操作（需要存储修改或 Raft 共识日志复制）
  #[inline]
  pub fn is_write(&self) -> bool {
    matches!(
      self,
      Cmd::Set { .. }
        | Cmd::SetNx(_, _)
        | Cmd::SetEx(_, _, _)
        | Cmd::PSetEx(_, _, _)
        | Cmd::MSet(_)
        | Cmd::MSetNx(_)
        | Cmd::MSetEx { .. }
        | Cmd::Incr(_)
        | Cmd::Decr(_)
        | Cmd::IncrBy(_, _)
        | Cmd::DecrBy(_, _)
        | Cmd::IncrByFloat(_, _)
        | Cmd::IncrEx { .. }
        | Cmd::Append(_, _)
        | Cmd::SetRange(_, _, _)
        | Cmd::GetSet(_, _)
        | Cmd::GetDel(_)
        | Cmd::GetEx { .. }
        | Cmd::DelEx { .. }
        | Cmd::Cas { .. }
        | Cmd::Cad { .. }
        | Cmd::Del(_)
        | Cmd::Unlink(_)
        | Cmd::Expire(_, _)
        | Cmd::PExpire(_, _)
        | Cmd::ExpireAt(_, _)
        | Cmd::PExpireAt(_, _)
        | Cmd::Persist(_)
        | Cmd::Rename(_, _)
        | Cmd::RenameNx(_, _)
        | Cmd::Copy { .. }
        | Cmd::Touch(_)
        | Cmd::Restore { .. }
        | Cmd::HSet(_, _)
        | Cmd::HSetNx(_, _, _)
        | Cmd::HDel(_, _)
        | Cmd::HIncrBy(_, _, _)
        | Cmd::HIncrByFloat(_, _, _)
        | Cmd::HExpire { .. }
        | Cmd::HPExpire { .. }
        | Cmd::HExpireAt { .. }
        | Cmd::HPExpireAt { .. }
        | Cmd::HPersist { .. }
        | Cmd::HGetDel { .. }
        | Cmd::LPush(_, _)
        | Cmd::RPush(_, _)
        | Cmd::LPushX(_, _)
        | Cmd::RPushX(_, _)
        | Cmd::LPop(_, _)
        | Cmd::RPop(_, _)
        | Cmd::RPopLPush(_, _)
        | Cmd::LMove { .. }
        | Cmd::LMoveM { .. }
        | Cmd::BLMove { .. }
        | Cmd::BLMoveM { .. }
        | Cmd::LSet(_, _, _)
        | Cmd::LInsert { .. }
        | Cmd::LRem(_, _, _)
        | Cmd::LTrim(_, _, _)
        | Cmd::BLPop { .. }
        | Cmd::BRPop { .. }
        | Cmd::LMPop { .. }
        | Cmd::BLMPop { .. }
        | Cmd::Sort { .. }
        | Cmd::SAdd(_, _)
        | Cmd::SRem(_, _)
        | Cmd::SMove { .. }
        | Cmd::SPop(_, _)
        | Cmd::SDiffStore { .. }
        | Cmd::SUnionStore { .. }
        | Cmd::SInterStore { .. }
        | Cmd::ZAdd { .. }
        | Cmd::ZRem(_, _)
        | Cmd::ZIncrBy(_, _, _)
        | Cmd::ZPopMin(_, _)
        | Cmd::ZPopMax(_, _)
        | Cmd::BZPopMin { .. }
        | Cmd::BZPopMax { .. }
        | Cmd::ZRemRangeByRank(_, _, _)
        | Cmd::ZRemRangeByScore(_, _, _)
        | Cmd::ZRemRangeByLex(_, _, _)
        | Cmd::ZDiffStore { .. }
        | Cmd::ZUnionStore { .. }
        | Cmd::ZInterStore { .. }
        | Cmd::ZMPop { .. }
        | Cmd::BZMPop { .. }
        | Cmd::SetBit(_, _, _)
        | Cmd::BitOp { .. }
        | Cmd::BitField { .. }
        | Cmd::SiAdd(_, _)
        | Cmd::SiRem(_, _)
        | Cmd::XAdd { .. }
        | Cmd::XTrim { .. }
        | Cmd::XDel(_, _)
        | Cmd::XGroup(_)
        | Cmd::XAck(_, _, _)
        | Cmd::XAckDel { .. }
        | Cmd::XNack { .. }
        | Cmd::XDelEx { .. }
        | Cmd::XClaim { .. }
        | Cmd::XAutoClaim { .. }
        | Cmd::XSetId { .. }
        | Cmd::BfReserve { .. }
        | Cmd::BfAdd(_, _)
        | Cmd::BfMAdd(_, _)
        | Cmd::BfInsert { .. }
        | Cmd::CfReserve { .. }
        | Cmd::CfAdd(_, _)
        | Cmd::CfAddNx(_, _)
        | Cmd::CfInsert { .. }
        | Cmd::CfInsertNx { .. }
        | Cmd::CfDel(_, _)
        | Cmd::PfAdd(_, _)
        | Cmd::PfMerge(_, _)
        | Cmd::TDigestCreate { .. }
        | Cmd::TDigestReset(_)
        | Cmd::TDigestAdd(_, _)
        | Cmd::TDigestMerge { .. }
        | Cmd::TsCreate { .. }
        | Cmd::TsAlter { .. }
        | Cmd::TsAdd { .. }
        | Cmd::TsMAdd(_)
        | Cmd::TsIncrBy { .. }
        | Cmd::TsDecrBy { .. }
        | Cmd::TsDel(_, _, _)
        | Cmd::GeoAdd { .. }
        | Cmd::GeoSearchStore { .. }
        | Cmd::JsonSet { .. }
        | Cmd::JsonDel { .. }
        | Cmd::JsonNumIncrBy { .. }
        | Cmd::JsonNumMultBy { .. }
        | Cmd::JsonStrAppend { .. }
        | Cmd::JsonArrAppend { .. }
        | Cmd::JsonArrInsert { .. }
        | Cmd::JsonArrPop { .. }
        | Cmd::JsonArrTrim { .. }
        | Cmd::JsonToggle { .. }
        | Cmd::JsonClear { .. }
        | Cmd::JsonMerge { .. }
        | Cmd::Batch(_)
        | Cmd::Txn(_)
    )
  }

  /// 是否为只读查询操作
  #[inline]
  pub fn is_readonly(&self) -> bool {
    !self.is_write() && !self.is_admin()
  }

  /// 是否为管理类操作（配置、集群控制、Raft 管控等）
  #[inline]
  pub fn is_admin(&self) -> bool {
    matches!(
      self,
      Cmd::ConfigGet(_)
        | Cmd::ConfigSet(_, _)
        | Cmd::ClientKill(_)
        | Cmd::ClientPause(_)
        | Cmd::ClientUnpause
        | Cmd::ClientUnblock { .. }
        | Cmd::ClientTracking { .. }
        | Cmd::ClientSetInfo(_, _)
        | Cmd::ClientNoTouch(_)
        | Cmd::ClientNoEvict(_)
        | Cmd::ClientReply(_)
        | Cmd::ClientHelp
        | Cmd::NamespaceAdd(_, _)
        | Cmd::NamespaceSet(_, _)
        | Cmd::NamespaceDel(_)
        | Cmd::NamespaceGet(_)
        | Cmd::NamespaceCurrent
        | Cmd::NamespaceId(_)
        | Cmd::NamespaceRename(_, _)
        | Cmd::ClusterNodes
        | Cmd::ClusterInfo
        | Cmd::ClusterSlots
        | Cmd::ClusterShards
        | Cmd::ClusterMembers
        | Cmd::ClusterMyId
        | Cmd::ClusterKeySlot(_)
        | Cmd::ClusterRebalance
        | Cmd::ClusterFailover
        | Cmd::ClusterSaveConfig
        | Cmd::ClusterReset(_)
        | Cmd::ClusterCountKeysInSlot(_)
        | Cmd::ClusterGetKeysInSlot { .. }
        | Cmd::ClusterSetTags { .. }
        | Cmd::ClusterGetTags(_)
        | Cmd::ClusterMeet { .. }
        | Cmd::ClusterForget(_)
        | Cmd::RaftJoin { .. }
        | Cmd::RaftLeave(_)
        | Cmd::RaftMembers
        | Cmd::RaftSnapshot
        | Cmd::RaftSnapshotStatus
        | Cmd::RaftPurge(_)
        | Cmd::RaftHealth
        | Cmd::RaftMetrics
        | Cmd::RaftStatus
        | Cmd::Shutdown
        | Cmd::Compact
        | Cmd::Bgsave
        | Cmd::FlushMemTable
        | Cmd::FlushBlockCache
    )
  }

  /// 提取受影响的首个 Key（用于集群 Slot 计算与快速写路由，零堆分配）
  pub fn first_key(&self) -> Option<&str> {
    match self {
      Cmd::Get(k)
      | Cmd::StrLen(k)
      | Cmd::Incr(k)
      | Cmd::Decr(k)
      | Cmd::IncrBy(k, _)
      | Cmd::DecrBy(k, _)
      | Cmd::IncrByFloat(k, _)
      | Cmd::Append(k, _)
      | Cmd::GetRange(k, _, _)
      | Cmd::SetRange(k, _, _)
      | Cmd::GetSet(k, _)
      | Cmd::GetDel(k)
      | Cmd::SetNx(k, _)
      | Cmd::SetEx(k, _, _)
      | Cmd::PSetEx(k, _, _)
      | Cmd::Digest(k)
      | Cmd::HGetAll(k)
      | Cmd::HLen(k)
      | Cmd::HStrLen(k, _)
      | Cmd::HKeys(k)
      | Cmd::HVals(k)
      | Cmd::HGet(k, _)
      | Cmd::HDel(k, _)
      | Cmd::HExists(k, _)
      | Cmd::HIncrBy(k, _, _)
      | Cmd::HIncrByFloat(k, _, _)
      | Cmd::HSet(k, _)
      | Cmd::HSetNx(k, _, _)
      | Cmd::LLen(k)
      | Cmd::LRange(k, _, _)
      | Cmd::LIndex(k, _)
      | Cmd::LPush(k, _)
      | Cmd::RPush(k, _)
      | Cmd::LPushX(k, _)
      | Cmd::RPushX(k, _)
      | Cmd::LPop(k, _)
      | Cmd::RPop(k, _)
      | Cmd::LSet(k, _, _)
      | Cmd::LRem(k, _, _)
      | Cmd::LTrim(k, _, _)
      | Cmd::SCard(k)
      | Cmd::SMembers(k)
      | Cmd::SIsMember(k, _)
      | Cmd::SMIsMember(k, _)
      | Cmd::SRandMember(k, _)
      | Cmd::SAdd(k, _)
      | Cmd::SRem(k, _)
      | Cmd::SPop(k, _)
      | Cmd::ZCard(k)
      | Cmd::ZScore(k, _)
      | Cmd::ZMScore(k, _)
      | Cmd::ZCount(k, _, _)
      | Cmd::ZLexCount(k, _, _)
      | Cmd::ZRem(k, _)
      | Cmd::ZIncrBy(k, _, _)
      | Cmd::ZPopMin(k, _)
      | Cmd::ZPopMax(k, _)
      | Cmd::ZRemRangeByRank(k, _, _)
      | Cmd::ZRemRangeByScore(k, _, _)
      | Cmd::ZRemRangeByLex(k, _, _)
      | Cmd::GetBit(k, _)
      | Cmd::SetBit(k, _, _)
      | Cmd::SiCard(k)
      | Cmd::SiExists(k, _)
      | Cmd::SiAdd(k, _)
      | Cmd::SiRem(k, _)
      | Cmd::XLen(k)
      | Cmd::XDel(k, _)
      | Cmd::BfCard(k)
      | Cmd::BfAdd(k, _)
      | Cmd::BfMAdd(k, _)
      | Cmd::BfExists(k, _)
      | Cmd::BfMExists(k, _)
      | Cmd::CfCount(k, _)
      | Cmd::CfAdd(k, _)
      | Cmd::CfAddNx(k, _)
      | Cmd::CfExists(k, _)
      | Cmd::CfMExists(k, _)
      | Cmd::CfDel(k, _)
      | Cmd::PfAdd(k, _)
      | Cmd::TDigestInfo(k)
      | Cmd::TDigestMin(k)
      | Cmd::TDigestMax(k)
      | Cmd::TDigestReset(k)
      | Cmd::TsInfo(k)
      | Cmd::GeoHash(k, _)
      | Cmd::GeoPos(k, _)
      | Cmd::Type(k)
      | Cmd::Ttl(k)
      | Cmd::Pttl(k)
      | Cmd::ExpireTime(k)
      | Cmd::PExpireTime(k)
      | Cmd::Persist(k)
      | Cmd::Dump(k)
      | Cmd::Expire(k, _)
      | Cmd::PExpire(k, _)
      | Cmd::ExpireAt(k, _)
      | Cmd::PExpireAt(k, _)
      | Cmd::Move(k, _)
      | Cmd::TsDel(k, _, _)
      | Cmd::XAck(k, _, _)
      | Cmd::CfInfo(k) => Some(k.as_str()),

      Cmd::Set { key: k, .. }
      | Cmd::GetEx { key: k, .. }
      | Cmd::DelEx { key: k, .. }
      | Cmd::IncrEx { key: k, .. }
      | Cmd::Cas { key: k, .. }
      | Cmd::Cad { key: k, .. }
      | Cmd::BitCount { key: k, .. }
      | Cmd::BitPos { key: k, .. }
      | Cmd::BitField { key: k, .. }
      | Cmd::BitFieldRo { key: k, .. }
      | Cmd::HScan { key: k, .. }
      | Cmd::HRandField { key: k, .. }
      | Cmd::HExpire { key: k, .. }
      | Cmd::HPExpire { key: k, .. }
      | Cmd::HExpireAt { key: k, .. }
      | Cmd::HPExpireAt { key: k, .. }
      | Cmd::HExpireTime { key: k, .. }
      | Cmd::HPExpireTime { key: k, .. }
      | Cmd::HTtl { key: k, .. }
      | Cmd::HPTtl { key: k, .. }
      | Cmd::HPersist { key: k, .. }
      | Cmd::LPos { key: k, .. }
      | Cmd::LInsert { key: k, .. }
      | Cmd::SScan { key: k, .. }
      | Cmd::ZRangeByScore { key: k, .. }
      | Cmd::ZRevRangeByScore { key: k, .. }
      | Cmd::ZRangeByLex { key: k, .. }
      | Cmd::ZRevRangeByLex { key: k, .. }
      | Cmd::ZRandMember { key: k, .. }
      | Cmd::ZScan { key: k, .. }
      | Cmd::ZAdd { key: k, .. }
      | Cmd::ZRank { key: k, .. }
      | Cmd::ZRevRank { key: k, .. }
      | Cmd::ZRange { key: k, .. }
      | Cmd::SiRangeByValue { key: k, .. }
      | Cmd::SiRevRangeByValue { key: k, .. }
      | Cmd::XRange { key: k, .. }
      | Cmd::XRevRange { key: k, .. }
      | Cmd::XInfoStream { key: k, .. }
      | Cmd::XPending { key: k, .. }
      | Cmd::XAdd { key: k, .. }
      | Cmd::XTrim { key: k, .. }
      | Cmd::XClaim { key: k, .. }
      | Cmd::XAutoClaim { key: k, .. }
      | Cmd::XSetId { key: k, .. }
      | Cmd::BfInfo { key: k, .. }
      | Cmd::BfReserve { key: k, .. }
      | Cmd::BfInsert { key: k, .. }
      | Cmd::CfReserve { key: k, .. }
      | Cmd::CfInsert { key: k, .. }
      | Cmd::CfInsertNx { key: k, .. }
      | Cmd::TDigestCreate { key: k, .. }
      | Cmd::TsGet { key: k, .. }
      | Cmd::TsRange { key: k, .. }
      | Cmd::TsRevRange { key: k, .. }
      | Cmd::TsCreate { key: k, .. }
      | Cmd::TsAlter { key: k, .. }
      | Cmd::TsAdd { key: k, .. }
      | Cmd::TsIncrBy { key: k, .. }
      | Cmd::TsDecrBy { key: k, .. }
      | Cmd::GeoDist { key: k, .. }
      | Cmd::GeoRadius { key: k, .. }
      | Cmd::GeoRadiusByMember { key: k, .. }
      | Cmd::GeoSearch { key: k, .. }
      | Cmd::GeoAdd { key: k, .. }
      | Cmd::JsonGet { key: k, .. }
      | Cmd::JsonType { key: k, .. }
      | Cmd::JsonStrLen { key: k, .. }
      | Cmd::JsonArrLen { key: k, .. }
      | Cmd::JsonObjKeys { key: k, .. }
      | Cmd::JsonObjLen { key: k, .. }
      | Cmd::JsonResp { key: k, .. }
      | Cmd::JsonSet { key: k, .. }
      | Cmd::JsonDel { key: k, .. }
      | Cmd::JsonNumIncrBy { key: k, .. }
      | Cmd::JsonNumMultBy { key: k, .. }
      | Cmd::JsonStrAppend { key: k, .. }
      | Cmd::JsonArrAppend { key: k, .. }
      | Cmd::JsonArrIndex { key: k, .. }
      | Cmd::JsonArrInsert { key: k, .. }
      | Cmd::JsonArrPop { key: k, .. }
      | Cmd::JsonArrTrim { key: k, .. }
      | Cmd::JsonToggle { key: k, .. }
      | Cmd::JsonClear { key: k, .. }
      | Cmd::JsonMerge { key: k, .. }
      | Cmd::Restore { key: k, .. }
      | Cmd::Object { key: k, .. }
      | Cmd::HGetDel { key: k, .. }
      | Cmd::Sort { key: k, .. }
      | Cmd::SortRo { key: k, .. }
      | Cmd::XAckDel { key: k, .. }
      | Cmd::XNack { key: k, .. }
      | Cmd::XDelEx { key: k, .. } => Some(k.as_str()),

      Cmd::MGet(keys)
      | Cmd::Del(keys)
      | Cmd::Unlink(keys)
      | Cmd::Exists(keys)
      | Cmd::Touch(keys)
      | Cmd::SDiff(keys)
      | Cmd::SUnion(keys)
      | Cmd::SInter(keys)
      | Cmd::SInterCard { keys, .. }
      | Cmd::SDiffCard { keys, .. }
      | Cmd::SUnionCard { keys, .. }
      | Cmd::PfCount(keys)
      | Cmd::TsQueryIndex(keys)
      | Cmd::JsonMGet { keys, .. } => keys.first().map(|s| s.as_str()),

      Cmd::MSet(pairs) | Cmd::MSetNx(pairs) => pairs.first().map(|(k, _)| k.as_str()),

      Cmd::Rename(k1, _) | Cmd::RenameNx(k1, _) => Some(k1.as_str()),
      Cmd::LMoveM { src, .. } | Cmd::BLMoveM { src, .. } => Some(src.as_str()),

      _ => None,
    }
  }

  /// 根据命令的首个 Key 计算 CRC16 Slot（零堆分配极速解析）
  #[inline]
  pub fn key_slot(&self) -> u16 {
    self
      .first_key()
      .map(|k| Crc::key_slot(k.as_bytes()))
      .unwrap_or(0)
  }

  /// 提取受影响的 Key 列表（用于集群 Slot 计算与事务 Key 冲突检测）
  pub fn extract_keys(&self) -> Vec<&str> {
    match self {
      Cmd::Get(k)
      | Cmd::StrLen(k)
      | Cmd::Incr(k)
      | Cmd::Decr(k)
      | Cmd::IncrBy(k, _)
      | Cmd::DecrBy(k, _)
      | Cmd::IncrByFloat(k, _)
      | Cmd::Append(k, _)
      | Cmd::GetRange(k, _, _)
      | Cmd::SetRange(k, _, _)
      | Cmd::GetSet(k, _)
      | Cmd::GetDel(k)
      | Cmd::SetNx(k, _)
      | Cmd::SetEx(k, _, _)
      | Cmd::PSetEx(k, _, _)
      | Cmd::Digest(k)
      | Cmd::HGetAll(k)
      | Cmd::HLen(k)
      | Cmd::HStrLen(k, _)
      | Cmd::HKeys(k)
      | Cmd::HVals(k)
      | Cmd::HGet(k, _)
      | Cmd::HDel(k, _)
      | Cmd::HExists(k, _)
      | Cmd::HIncrBy(k, _, _)
      | Cmd::HIncrByFloat(k, _, _)
      | Cmd::HSet(k, _)
      | Cmd::HSetNx(k, _, _)
      | Cmd::LLen(k)
      | Cmd::LRange(k, _, _)
      | Cmd::LIndex(k, _)
      | Cmd::LPush(k, _)
      | Cmd::RPush(k, _)
      | Cmd::LPushX(k, _)
      | Cmd::RPushX(k, _)
      | Cmd::LPop(k, _)
      | Cmd::RPop(k, _)
      | Cmd::LSet(k, _, _)
      | Cmd::LRem(k, _, _)
      | Cmd::LTrim(k, _, _)
      | Cmd::SCard(k)
      | Cmd::SMembers(k)
      | Cmd::SIsMember(k, _)
      | Cmd::SMIsMember(k, _)
      | Cmd::SRandMember(k, _)
      | Cmd::SAdd(k, _)
      | Cmd::SRem(k, _)
      | Cmd::SPop(k, _)
      | Cmd::ZCard(k)
      | Cmd::ZScore(k, _)
      | Cmd::ZMScore(k, _)
      | Cmd::ZCount(k, _, _)
      | Cmd::ZLexCount(k, _, _)
      | Cmd::ZRem(k, _)
      | Cmd::ZIncrBy(k, _, _)
      | Cmd::ZPopMin(k, _)
      | Cmd::ZPopMax(k, _)
      | Cmd::ZRemRangeByRank(k, _, _)
      | Cmd::ZRemRangeByScore(k, _, _)
      | Cmd::ZRemRangeByLex(k, _, _)
      | Cmd::GetBit(k, _)
      | Cmd::SetBit(k, _, _)
      | Cmd::SiCard(k)
      | Cmd::SiExists(k, _)
      | Cmd::SiAdd(k, _)
      | Cmd::SiRem(k, _)
      | Cmd::XLen(k)
      | Cmd::XDel(k, _)
      | Cmd::BfCard(k)
      | Cmd::BfAdd(k, _)
      | Cmd::BfMAdd(k, _)
      | Cmd::BfExists(k, _)
      | Cmd::BfMExists(k, _)
      | Cmd::CfCount(k, _)
      | Cmd::CfAdd(k, _)
      | Cmd::CfAddNx(k, _)
      | Cmd::CfExists(k, _)
      | Cmd::CfMExists(k, _)
      | Cmd::CfDel(k, _)
      | Cmd::PfAdd(k, _)
      | Cmd::TDigestInfo(k)
      | Cmd::TDigestMin(k)
      | Cmd::TDigestMax(k)
      | Cmd::TDigestReset(k)
      | Cmd::TsInfo(k)
      | Cmd::GeoHash(k, _)
      | Cmd::GeoPos(k, _)
      | Cmd::Type(k)
      | Cmd::Ttl(k)
      | Cmd::Pttl(k)
      | Cmd::ExpireTime(k)
      | Cmd::PExpireTime(k)
      | Cmd::Persist(k)
      | Cmd::Dump(k)
      | Cmd::Expire(k, _)
      | Cmd::PExpire(k, _)
      | Cmd::ExpireAt(k, _)
      | Cmd::PExpireAt(k, _)
      | Cmd::Move(k, _)
      | Cmd::TsDel(k, _, _)
      | Cmd::XAck(k, _, _) => vec![k.as_str()],

      Cmd::Set { key: k, .. }
      | Cmd::GetEx { key: k, .. }
      | Cmd::DelEx { key: k, .. }
      | Cmd::IncrEx { key: k, .. }
      | Cmd::Cas { key: k, .. }
      | Cmd::Cad { key: k, .. }
      | Cmd::BitCount { key: k, .. }
      | Cmd::BitPos { key: k, .. }
      | Cmd::BitField { key: k, .. }
      | Cmd::BitFieldRo { key: k, .. }
      | Cmd::HScan { key: k, .. }
      | Cmd::HRandField { key: k, .. }
      | Cmd::HExpire { key: k, .. }
      | Cmd::HPExpire { key: k, .. }
      | Cmd::HExpireAt { key: k, .. }
      | Cmd::HPExpireAt { key: k, .. }
      | Cmd::HExpireTime { key: k, .. }
      | Cmd::HPExpireTime { key: k, .. }
      | Cmd::HTtl { key: k, .. }
      | Cmd::HPTtl { key: k, .. }
      | Cmd::HPersist { key: k, .. }
      | Cmd::LPos { key: k, .. }
      | Cmd::LInsert { key: k, .. }
      | Cmd::SScan { key: k, .. }
      | Cmd::ZRangeByScore { key: k, .. }
      | Cmd::ZRevRangeByScore { key: k, .. }
      | Cmd::ZRangeByLex { key: k, .. }
      | Cmd::ZRevRangeByLex { key: k, .. }
      | Cmd::ZRandMember { key: k, .. }
      | Cmd::ZScan { key: k, .. }
      | Cmd::ZAdd { key: k, .. }
      | Cmd::ZRank { key: k, .. }
      | Cmd::ZRevRank { key: k, .. }
      | Cmd::ZRange { key: k, .. }
      | Cmd::SiRangeByValue { key: k, .. }
      | Cmd::SiRevRangeByValue { key: k, .. }
      | Cmd::XRange { key: k, .. }
      | Cmd::XRevRange { key: k, .. }
      | Cmd::XInfoStream { key: k, .. }
      | Cmd::XPending { key: k, .. }
      | Cmd::XAdd { key: k, .. }
      | Cmd::XTrim { key: k, .. }
      | Cmd::XClaim { key: k, .. }
      | Cmd::XAutoClaim { key: k, .. }
      | Cmd::XSetId { key: k, .. }
      | Cmd::BfInfo { key: k, .. }
      | Cmd::BfReserve { key: k, .. }
      | Cmd::BfInsert { key: k, .. }
      | Cmd::CfInfo(k)
      | Cmd::CfReserve { key: k, .. }
      | Cmd::CfInsert { key: k, .. }
      | Cmd::CfInsertNx { key: k, .. }
      | Cmd::TDigestCreate { key: k, .. }
      | Cmd::TsGet { key: k, .. }
      | Cmd::TsRange { key: k, .. }
      | Cmd::TsRevRange { key: k, .. }
      | Cmd::TsCreate { key: k, .. }
      | Cmd::TsAlter { key: k, .. }
      | Cmd::TsAdd { key: k, .. }
      | Cmd::TsIncrBy { key: k, .. }
      | Cmd::TsDecrBy { key: k, .. }
      | Cmd::GeoDist { key: k, .. }
      | Cmd::GeoRadius { key: k, .. }
      | Cmd::GeoRadiusByMember { key: k, .. }
      | Cmd::GeoSearch { key: k, .. }
      | Cmd::GeoAdd { key: k, .. }
      | Cmd::JsonGet { key: k, .. }
      | Cmd::JsonType { key: k, .. }
      | Cmd::JsonStrLen { key: k, .. }
      | Cmd::JsonArrLen { key: k, .. }
      | Cmd::JsonObjKeys { key: k, .. }
      | Cmd::JsonObjLen { key: k, .. }
      | Cmd::JsonResp { key: k, .. }
      | Cmd::JsonSet { key: k, .. }
      | Cmd::JsonDel { key: k, .. }
      | Cmd::JsonNumIncrBy { key: k, .. }
      | Cmd::JsonNumMultBy { key: k, .. }
      | Cmd::JsonStrAppend { key: k, .. }
      | Cmd::JsonArrAppend { key: k, .. }
      | Cmd::JsonArrIndex { key: k, .. }
      | Cmd::JsonArrInsert { key: k, .. }
      | Cmd::JsonArrPop { key: k, .. }
      | Cmd::JsonArrTrim { key: k, .. }
      | Cmd::JsonToggle { key: k, .. }
      | Cmd::JsonClear { key: k, .. }
      | Cmd::JsonMerge { key: k, .. }
      | Cmd::Restore { key: k, .. }
      | Cmd::Object { key: k, .. }
      | Cmd::HGetDel { key: k, .. }
      | Cmd::Sort { key: k, .. }
      | Cmd::SortRo { key: k, .. }
      | Cmd::XAckDel { key: k, .. }
      | Cmd::XNack { key: k, .. }
      | Cmd::XDelEx { key: k, .. } => vec![k.as_str()],

      Cmd::MGet(keys)
      | Cmd::Del(keys)
      | Cmd::Unlink(keys)
      | Cmd::Exists(keys)
      | Cmd::Touch(keys)
      | Cmd::SDiff(keys)
      | Cmd::SUnion(keys)
      | Cmd::SInter(keys)
      | Cmd::SInterCard { keys, .. }
      | Cmd::SDiffCard { keys, .. }
      | Cmd::SUnionCard { keys, .. }
      | Cmd::PfCount(keys)
      | Cmd::TsQueryIndex(keys)
      | Cmd::JsonMGet { keys, .. } => keys.iter().map(|s| s.as_str()).collect(),

      Cmd::MSet(pairs) | Cmd::MSetNx(pairs) => pairs.iter().map(|(k, _)| k.as_str()).collect(),

      Cmd::Rename(k1, k2) | Cmd::RenameNx(k1, k2) => vec![k1.as_str(), k2.as_str()],
      Cmd::LMoveM { src, dst, .. } | Cmd::BLMoveM { src, dst, .. } => {
        vec![src.as_str(), dst.as_str()]
      }

      _ => Vec::new(),
    }
  }
}
