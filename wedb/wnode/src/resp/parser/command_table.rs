//! RESP 命令表与查找逻辑（对标 libs/server/Resp/Parser/RespCommandHashLookupData.cs
//! 与 libs/server/Resp/Parser/RespCommandHashLookup.cs）
//!
//! 查表为编译期构建的 FNV-1a 开放寻址索引（`PRIMARY_INDEX` / `SUB_INDEX`）：
//! 表数据仍是唯一事实源（static 常量区），索引由 `const fn` 在编译期从表派生，
//! 运行时一次哈希 + 期望 O(1) 次探测即达，无锁、无分配、无二分回退分支。
//! 键唯一性（主表内不重复、子表内 (父命令, 名) 不重复）由构建期 `assert!`
//! 锁定，表若引入重复键即编译失败。

use wresp::command::RespCommand;

/// 主命令名称表项
type PrimaryEntry = (&'static str, RespCommand, bool);

/// 主命令名称表（表数据按 ASCII 升序保持人读可核；检索走编译期派生的
/// `PRIMARY_INDEX` 哈希索引，不再依赖有序性）。`has_subcommands` 标记经子命令表分派；
/// 逐项对标 RespCommandHashLookupData.cs:PopulatePrimaryTable（含 SLAVEOF/
/// SECONDARYOF 同命令双名与 RI.* 点名命令）。RI.LEN 是本仓自定义计数命令
/// RI.COUNT 的同命令双名，与 SLAVEOF/SECONDARYOF 同型（解析期归一到同一
/// RespCommand::Ricount，杜绝第二套命令枚举与第二套计数实现）。
pub(crate) static PRIMARY_TABLE: &[PrimaryEntry] = &[
  ("ACL", RespCommand::Acl, true),
  ("APPEND", RespCommand::Append, false),
  ("ASKING", RespCommand::Asking, false),
  ("ASYNC", RespCommand::Async, false),
  ("AUTH", RespCommand::Auth, false),
  ("BGSAVE", RespCommand::Bgsave, false),
  ("BITCOUNT", RespCommand::Bitcount, false),
  ("BITFIELD", RespCommand::Bitfield, false),
  ("BITFIELD_RO", RespCommand::BitfieldRo, false),
  ("BITOP", RespCommand::Bitop, true),
  ("BITPOS", RespCommand::Bitpos, false),
  ("BLMOVE", RespCommand::Blmove, false),
  ("BLMPOP", RespCommand::Blmpop, false),
  ("BLPOP", RespCommand::Blpop, false),
  ("BRPOP", RespCommand::Brpop, false),
  ("BRPOPLPUSH", RespCommand::Brpoplpush, false),
  ("BZMPOP", RespCommand::Bzmpop, false),
  ("BZPOPMAX", RespCommand::Bzpopmax, false),
  ("BZPOPMIN", RespCommand::Bzpopmin, false),
  ("CLIENT", RespCommand::Client, true),
  ("CLUSTER", RespCommand::Cluster, true),
  ("COMMAND", RespCommand::Command, true),
  ("COMMITAOF", RespCommand::Commitaof, false),
  ("CONFIG", RespCommand::Config, true),
  ("CUSTOMOBJECTSCAN", RespCommand::Coscan, false),
  ("DBSIZE", RespCommand::Dbsize, false),
  ("DEBUG", RespCommand::Debug, false),
  ("DECR", RespCommand::Decr, false),
  ("DECRBY", RespCommand::Decrby, false),
  ("DEL", RespCommand::Del, false),
  ("DELIFGREATER", RespCommand::Delifgreater, false),
  ("DISCARD", RespCommand::Discard, false),
  ("DUMP", RespCommand::Dump, false),
  ("ECHO", RespCommand::Echo, false),
  ("EVAL", RespCommand::Eval, false),
  ("EVALSHA", RespCommand::Evalsha, false),
  ("EXEC", RespCommand::Exec, false),
  ("EXISTS", RespCommand::Exists, false),
  ("EXPDELSCAN", RespCommand::Expdelscan, false),
  ("EXPIRE", RespCommand::Expire, false),
  ("EXPIREAT", RespCommand::Expireat, false),
  ("EXPIRETIME", RespCommand::Expiretime, false),
  ("FAILOVER", RespCommand::Failover, false),
  ("FLUSHALL", RespCommand::Flushall, false),
  ("FLUSHDB", RespCommand::Flushdb, false),
  ("GEOADD", RespCommand::Geoadd, false),
  ("GEODIST", RespCommand::Geodist, false),
  ("GEOHASH", RespCommand::Geohash, false),
  ("GEOPOS", RespCommand::Geopos, false),
  ("GEORADIUS", RespCommand::Georadius, false),
  ("GEORADIUSBYMEMBER", RespCommand::Georadiusbymember, false),
  (
    "GEORADIUSBYMEMBER_RO",
    RespCommand::GeoradiusbymemberRo,
    false,
  ),
  ("GEORADIUS_RO", RespCommand::GeoradiusRo, false),
  ("GEOSEARCH", RespCommand::Geosearch, false),
  ("GEOSEARCHSTORE", RespCommand::Geosearchstore, false),
  ("GET", RespCommand::Get, false),
  ("GETBIT", RespCommand::Getbit, false),
  ("GETDEL", RespCommand::Getdel, false),
  ("GETEX", RespCommand::Getex, false),
  ("GETIFNOTMATCH", RespCommand::Getifnotmatch, false),
  ("GETRANGE", RespCommand::Getrange, false),
  ("GETSET", RespCommand::Getset, false),
  ("GETWITHETAG", RespCommand::Getwithetag, false),
  ("HCOLLECT", RespCommand::Hcollect, false),
  ("HDEL", RespCommand::Hdel, false),
  ("HELLO", RespCommand::Hello, false),
  ("HEXISTS", RespCommand::Hexists, false),
  ("HEXPIRE", RespCommand::Hexpire, false),
  ("HEXPIREAT", RespCommand::Hexpireat, false),
  ("HEXPIRETIME", RespCommand::Hexpiretime, false),
  ("HGET", RespCommand::Hget, false),
  ("HGETALL", RespCommand::Hgetall, false),
  ("HINCRBY", RespCommand::Hincrby, false),
  ("HINCRBYFLOAT", RespCommand::Hincrbyfloat, false),
  ("HKEYS", RespCommand::Hkeys, false),
  ("HLEN", RespCommand::Hlen, false),
  ("HMGET", RespCommand::Hmget, false),
  ("HMSET", RespCommand::Hmset, false),
  ("HPERSIST", RespCommand::Hpersist, false),
  ("HPEXPIRE", RespCommand::Hpexpire, false),
  ("HPEXPIREAT", RespCommand::Hpexpireat, false),
  ("HPEXPIRETIME", RespCommand::Hpexpiretime, false),
  ("HPTTL", RespCommand::Hpttl, false),
  ("HRANDFIELD", RespCommand::Hrandfield, false),
  ("HSCAN", RespCommand::Hscan, false),
  ("HSET", RespCommand::Hset, false),
  ("HSETNX", RespCommand::Hsetnx, false),
  ("HSTRLEN", RespCommand::Hstrlen, false),
  ("HTTL", RespCommand::Httl, false),
  ("HVALS", RespCommand::Hvals, false),
  ("INCR", RespCommand::Incr, false),
  ("INCRBY", RespCommand::Incrby, false),
  ("INCRBYFLOAT", RespCommand::Incrbyfloat, false),
  ("INFO", RespCommand::Info, false),
  ("KEYS", RespCommand::Keys, false),
  ("LASTSAVE", RespCommand::Lastsave, false),
  ("LATENCY", RespCommand::Latency, true),
  ("LCS", RespCommand::Lcs, false),
  ("LINDEX", RespCommand::Lindex, false),
  ("LINSERT", RespCommand::Linsert, false),
  ("LLEN", RespCommand::Llen, false),
  ("LMOVE", RespCommand::Lmove, false),
  ("LMPOP", RespCommand::Lmpop, false),
  ("LPOP", RespCommand::Lpop, false),
  ("LPOS", RespCommand::Lpos, false),
  ("LPUSH", RespCommand::Lpush, false),
  ("LPUSHX", RespCommand::Lpushx, false),
  ("LRANGE", RespCommand::Lrange, false),
  ("LREM", RespCommand::Lrem, false),
  ("LSET", RespCommand::Lset, false),
  ("LTRIM", RespCommand::Ltrim, false),
  ("MEMORY", RespCommand::Memory, true),
  ("MGET", RespCommand::Mget, false),
  ("MIGRATE", RespCommand::Migrate, false),
  ("MONITOR", RespCommand::Monitor, false),
  ("MSET", RespCommand::Mset, false),
  ("MSETNX", RespCommand::Msetnx, false),
  ("MULTI", RespCommand::Multi, false),
  ("OBJECT", RespCommand::Object, true),
  ("PERSIST", RespCommand::Persist, false),
  ("PEXPIRE", RespCommand::Pexpire, false),
  ("PEXPIREAT", RespCommand::Pexpireat, false),
  ("PEXPIRETIME", RespCommand::Pexpiretime, false),
  ("PFADD", RespCommand::Pfadd, false),
  ("PFCOUNT", RespCommand::Pfcount, false),
  ("PFMERGE", RespCommand::Pfmerge, false),
  ("PING", RespCommand::Ping, false),
  ("PSETEX", RespCommand::Psetex, false),
  ("PSUBSCRIBE", RespCommand::Psubscribe, false),
  ("PTTL", RespCommand::Pttl, false),
  ("PUBLISH", RespCommand::Publish, false),
  ("PUBSUB", RespCommand::Pubsub, true),
  ("PUNSUBSCRIBE", RespCommand::Punsubscribe, false),
  ("QUIT", RespCommand::Quit, false),
  ("READONLY", RespCommand::Readonly, false),
  ("READWRITE", RespCommand::Readwrite, false),
  ("RENAME", RespCommand::Rename, false),
  ("RENAMENX", RespCommand::Renamenx, false),
  ("REPLICAOF", RespCommand::Replicaof, false),
  ("RESTORE", RespCommand::Restore, false),
  ("RI.CONFIG", RespCommand::Riconfig, false),
  ("RI.COUNT", RespCommand::Ricount, false),
  ("RI.CREATE", RespCommand::Ricreate, false),
  ("RI.DEL", RespCommand::Ridel, false),
  ("RI.EXISTS", RespCommand::Riexists, false),
  ("RI.GET", RespCommand::Riget, false),
  ("RI.LEN", RespCommand::Ricount, false),
  ("RI.METRICS", RespCommand::Rimetrics, false),
  ("RI.RANGE", RespCommand::Rirange, false),
  ("RI.SCAN", RespCommand::Riscan, false),
  ("RI.SET", RespCommand::Riset, false),
  ("ROLE", RespCommand::Role, false),
  ("RPOP", RespCommand::Rpop, false),
  ("RPOPLPUSH", RespCommand::Rpoplpush, false),
  ("RPUSH", RespCommand::Rpush, false),
  ("RPUSHX", RespCommand::Rpushx, false),
  ("RUNTXP", RespCommand::Runtxp, false),
  ("SADD", RespCommand::Sadd, false),
  ("SAVE", RespCommand::Save, false),
  ("SCAN", RespCommand::Scan, false),
  ("SCARD", RespCommand::Scard, false),
  ("SCRIPT", RespCommand::Script, true),
  ("SDIFF", RespCommand::Sdiff, false),
  ("SDIFFSTORE", RespCommand::Sdiffstore, false),
  ("SECONDARYOF", RespCommand::Secondaryof, false),
  ("SELECT", RespCommand::Select, false),
  ("SET", RespCommand::Set, false),
  ("SETBIT", RespCommand::Setbit, false),
  ("SETEX", RespCommand::Setex, false),
  ("SETIFGREATER", RespCommand::Setifgreater, false),
  ("SETIFMATCH", RespCommand::Setifmatch, false),
  ("SETNX", RespCommand::Setnx, false),
  ("SETRANGE", RespCommand::Setrange, false),
  ("SETWITHETAG", RespCommand::Setwithetag, false),
  ("SINTER", RespCommand::Sinter, false),
  ("SINTERCARD", RespCommand::Sintercard, false),
  ("SINTERSTORE", RespCommand::Sinterstore, false),
  ("SISMEMBER", RespCommand::Sismember, false),
  ("SLAVEOF", RespCommand::Secondaryof, false),
  ("SLOWLOG", RespCommand::Slowlog, true),
  ("SMEMBERS", RespCommand::Smembers, false),
  ("SMISMEMBER", RespCommand::Smismember, false),
  ("SMOVE", RespCommand::Smove, false),
  ("SPOP", RespCommand::Spop, false),
  ("SPUBLISH", RespCommand::Spublish, false),
  ("SRANDMEMBER", RespCommand::Srandmember, false),
  ("SREM", RespCommand::Srem, false),
  ("SSCAN", RespCommand::Sscan, false),
  ("SSUBSCRIBE", RespCommand::Ssubscribe, false),
  ("STRLEN", RespCommand::Strlen, false),
  ("SUBSCRIBE", RespCommand::Subscribe, false),
  ("SUBSTR", RespCommand::Substr, false),
  ("SUNION", RespCommand::Sunion, false),
  ("SUNIONSTORE", RespCommand::Sunionstore, false),
  ("SUNSUBSCRIBE", RespCommand::Sunsubscribe, false),
  ("SWAPDB", RespCommand::Swapdb, false),
  ("TIME", RespCommand::Time, false),
  ("TTL", RespCommand::Ttl, false),
  ("TYPE", RespCommand::Type, false),
  ("UNLINK", RespCommand::Unlink, false),
  ("UNSUBSCRIBE", RespCommand::Unsubscribe, false),
  ("UNWATCH", RespCommand::Unwatch, false),
  ("VADD", RespCommand::Vadd, false),
  ("VCARD", RespCommand::Vcard, false),
  ("VDIM", RespCommand::Vdim, false),
  ("VEMB", RespCommand::Vemb, false),
  ("VGETATTR", RespCommand::Vgetattr, false),
  ("VINFO", RespCommand::Vinfo, false),
  ("VISMEMBER", RespCommand::Vismember, false),
  ("VLINKS", RespCommand::Vlinks, false),
  ("VRANDMEMBER", RespCommand::Vrandmember, false),
  ("VREM", RespCommand::Vrem, false),
  ("VSETATTR", RespCommand::Vsetattr, false),
  ("VSIM", RespCommand::Vsim, false),
  ("WATCH", RespCommand::Watch, false),
  ("WATCHMS", RespCommand::Watchms, false),
  ("WATCHOS", RespCommand::Watchos, false),
  ("ZADD", RespCommand::Zadd, false),
  ("ZCARD", RespCommand::Zcard, false),
  ("ZCOLLECT", RespCommand::Zcollect, false),
  ("ZCOUNT", RespCommand::Zcount, false),
  ("ZDIFF", RespCommand::Zdiff, false),
  ("ZDIFFSTORE", RespCommand::Zdiffstore, false),
  ("ZEXPIRE", RespCommand::Zexpire, false),
  ("ZEXPIREAT", RespCommand::Zexpireat, false),
  ("ZEXPIRETIME", RespCommand::Zexpiretime, false),
  ("ZINCRBY", RespCommand::Zincrby, false),
  ("ZINTER", RespCommand::Zinter, false),
  ("ZINTERCARD", RespCommand::Zintercard, false),
  ("ZINTERSTORE", RespCommand::Zinterstore, false),
  ("ZLEXCOUNT", RespCommand::Zlexcount, false),
  ("ZMPOP", RespCommand::Zmpop, false),
  ("ZMSCORE", RespCommand::Zmscore, false),
  ("ZPERSIST", RespCommand::Zpersist, false),
  ("ZPEXPIRE", RespCommand::Zpexpire, false),
  ("ZPEXPIREAT", RespCommand::Zpexpireat, false),
  ("ZPEXPIRETIME", RespCommand::Zpexpiretime, false),
  ("ZPOPMAX", RespCommand::Zpopmax, false),
  ("ZPOPMIN", RespCommand::Zpopmin, false),
  ("ZPTTL", RespCommand::Zpttl, false),
  ("ZRANDMEMBER", RespCommand::Zrandmember, false),
  ("ZRANGE", RespCommand::Zrange, false),
  ("ZRANGEBYLEX", RespCommand::Zrangebylex, false),
  ("ZRANGEBYSCORE", RespCommand::Zrangebyscore, false),
  ("ZRANGESTORE", RespCommand::Zrangestore, false),
  ("ZRANK", RespCommand::Zrank, false),
  ("ZREM", RespCommand::Zrem, false),
  ("ZREMRANGEBYLEX", RespCommand::Zremrangebylex, false),
  ("ZREMRANGEBYRANK", RespCommand::Zremrangebyrank, false),
  ("ZREMRANGEBYSCORE", RespCommand::Zremrangebyscore, false),
  ("ZREVRANGE", RespCommand::Zrevrange, false),
  ("ZREVRANGEBYLEX", RespCommand::Zrevrangebylex, false),
  ("ZREVRANGEBYSCORE", RespCommand::Zrevrangebyscore, false),
  ("ZREVRANK", RespCommand::Zrevrank, false),
  ("ZSCAN", RespCommand::Zscan, false),
  ("ZSCORE", RespCommand::Zscore, false),
  ("ZTTL", RespCommand::Zttl, false),
  ("ZUNION", RespCommand::Zunion, false),
  ("ZUNIONSTORE", RespCommand::Zunionstore, false),
];

/// 子命令表：父命令 → (子命令名, RespCommand)（按名 ASCII 升序保持人读可核；
/// 检索经编译期派生的 `SUB_INDEX` 联合哈希索引）
/// Client 子命令表
pub(crate) static CLIENT_SUBTABLE: &[(&str, RespCommand)] = &[
  ("GETNAME", RespCommand::ClientGetname),
  ("ID", RespCommand::ClientId),
  ("INFO", RespCommand::ClientInfo),
  ("KILL", RespCommand::ClientKill),
  ("LIST", RespCommand::ClientList),
  ("SETINFO", RespCommand::ClientSetinfo),
  ("SETNAME", RespCommand::ClientSetname),
  ("UNBLOCK", RespCommand::ClientUnblock),
];

/// Config 子命令表
pub(crate) static CONFIG_SUBTABLE: &[(&str, RespCommand)] = &[
  ("GET", RespCommand::ConfigGet),
  ("REWRITE", RespCommand::ConfigRewrite),
  ("SET", RespCommand::ConfigSet),
];

/// Command 子命令表
pub(crate) static COMMAND_SUBTABLE: &[(&str, RespCommand)] = &[
  ("COUNT", RespCommand::CommandCount),
  ("DOCS", RespCommand::CommandDocs),
  ("GETKEYS", RespCommand::CommandGetkeys),
  ("GETKEYSANDFLAGS", RespCommand::CommandGetkeysandflags),
  ("INFO", RespCommand::CommandInfo),
];

/// Acl 子命令表
pub(crate) static ACL_SUBTABLE: &[(&str, RespCommand)] = &[
  ("CAT", RespCommand::AclCat),
  ("DELUSER", RespCommand::AclDeluser),
  ("GENPASS", RespCommand::AclGenpass),
  ("GETUSER", RespCommand::AclGetuser),
  ("LIST", RespCommand::AclList),
  ("LOAD", RespCommand::AclLoad),
  ("SAVE", RespCommand::AclSave),
  ("SETUSER", RespCommand::AclSetuser),
  ("USERS", RespCommand::AclUsers),
  ("WHOAMI", RespCommand::AclWhoami),
];

/// Script 子命令表
pub(crate) static SCRIPT_SUBTABLE: &[(&str, RespCommand)] = &[
  ("EXISTS", RespCommand::ScriptExists),
  ("FLUSH", RespCommand::ScriptFlush),
  ("LOAD", RespCommand::ScriptLoad),
];

/// Pubsub 子命令表
pub(crate) static PUBSUB_SUBTABLE: &[(&str, RespCommand)] = &[
  ("CHANNELS", RespCommand::PubsubChannels),
  ("NUMPAT", RespCommand::PubsubNumpat),
  ("NUMSUB", RespCommand::PubsubNumsub),
];

/// Latency 子命令表
pub(crate) static LATENCY_SUBTABLE: &[(&str, RespCommand)] = &[
  ("HELP", RespCommand::LatencyHelp),
  ("HISTOGRAM", RespCommand::LatencyHistogram),
  ("RESET", RespCommand::LatencyReset),
];

/// Slowlog 子命令表
pub(crate) static SLOWLOG_SUBTABLE: &[(&str, RespCommand)] = &[
  ("GET", RespCommand::SlowlogGet),
  ("HELP", RespCommand::SlowlogHelp),
  ("LEN", RespCommand::SlowlogLen),
  ("RESET", RespCommand::SlowlogReset),
];

/// Memory 子命令表
pub(crate) static MEMORY_SUBTABLE: &[(&str, RespCommand)] = &[("USAGE", RespCommand::MemoryUsage)];

/// Object 子命令表
pub(crate) static OBJECT_SUBTABLE: &[(&str, RespCommand)] = &[
  ("ENCODING", RespCommand::ObjectEncoding),
  ("FREQ", RespCommand::ObjectFreq),
  ("HELP", RespCommand::ObjectHelp),
  ("IDLETIME", RespCommand::ObjectIdletime),
  ("REFCOUNT", RespCommand::ObjectRefcount),
];

/// CLUSTER 子命令表（含 SET-CONFIG-EPOCH 连字符名）
///
/// 检查点传输流命令（SEND_CKPT_METADATA / SEND_CKPT_FILE_SEGMENT /
/// SNAPSHOT_DATA / BEGIN_REPLICA_RECOVER，对标 C#
/// RespClusterReplicationCommands.cs 的 recvCheckpointHandler 接收链）；
/// ATTACH_SYNC（diskless 复制路径）与 SYNC 显式注册，完整实现于
/// cluster_session/replication.rs（network_cluster_attach_sync /
/// network_cluster_sync），非占位回写不支持错误。
pub(crate) static CLUSTER_SUBTABLE: &[(&str, RespCommand)] = &[
  ("ADDSLOTS", RespCommand::ClusterAddslots),
  ("ADDSLOTSRANGE", RespCommand::ClusterAddslotsrange),
  ("ADVANCE_TIME", RespCommand::ClusterAdvanceTime),
  ("APPENDLOG", RespCommand::ClusterAppendlog),
  ("ATTACH_SYNC", RespCommand::ClusterAttachSync),
  ("BANLIST", RespCommand::ClusterBanlist),
  (
    "BEGIN_REPLICA_RECOVER",
    RespCommand::ClusterBeginReplicaRecover,
  ),
  ("BUMPEPOCH", RespCommand::ClusterBumpepoch),
  ("COUNTKEYSINSLOT", RespCommand::ClusterCountkeysinslot),
  ("DELKEYSINSLOT", RespCommand::ClusterDelkeysinslot),
  ("DELKEYSINSLOTRANGE", RespCommand::ClusterDelkeysinslotrange),
  ("DELSLOTS", RespCommand::ClusterDelslots),
  ("DELSLOTSRANGE", RespCommand::ClusterDelslotsrange),
  ("ENDPOINT", RespCommand::ClusterEndpoint),
  ("FAILOVER", RespCommand::ClusterFailover),
  (
    "FAILREPLICATIONOFFSET",
    RespCommand::ClusterFailreplicationoffset,
  ),
  ("FAILSTOPWRITES", RespCommand::ClusterFailstopwrites),
  ("FLUSHALL", RespCommand::ClusterFlushall),
  ("FLUSHALL_NS", RespCommand::ClusterFlushallNs),
  ("FORGET", RespCommand::ClusterForget),
  ("GETKEYSINSLOT", RespCommand::ClusterGetkeysinslot),
  ("GOSSIP", RespCommand::ClusterGossip),
  ("HELP", RespCommand::ClusterHelp),
  ("INFO", RespCommand::ClusterInfo),
  (
    "INITIATE_REPLICA_SYNC",
    RespCommand::ClusterInitiateReplicaSync,
  ),
  ("KEYSLOT", RespCommand::ClusterKeyslot),
  ("MEET", RespCommand::ClusterMeet),
  ("MIGRATE", RespCommand::ClusterMigrate),
  ("MLOG_KEY_TIME", RespCommand::ClusterMlogKeyTime),
  ("MTASKS", RespCommand::ClusterMtasks),
  ("MYID", RespCommand::ClusterMyid),
  ("MYPARENTID", RespCommand::ClusterMyparentid),
  ("NODES", RespCommand::ClusterNodes),
  ("PUBLISH", RespCommand::ClusterPublish),
  ("REPLICAS", RespCommand::ClusterReplicas),
  ("REPLICATE", RespCommand::ClusterReplicate),
  ("RESERVE", RespCommand::ClusterReserve),
  ("RESET", RespCommand::ClusterReset),
  (
    "SEND_CKPT_FILE_SEGMENT",
    RespCommand::ClusterSendCkptFileSegment,
  ),
  ("SEND_CKPT_METADATA", RespCommand::ClusterSendCkptMetadata),
  ("SET-CONFIG-EPOCH", RespCommand::ClusterSetconfigepoch),
  ("SETSLOT", RespCommand::ClusterSetslot),
  ("SETSLOTSRANGE", RespCommand::ClusterSetslotsrange),
  ("SHARDS", RespCommand::ClusterShards),
  ("SLOTS", RespCommand::ClusterSlots),
  ("SLOTSTATE", RespCommand::ClusterSlotstate),
  ("SNAPSHOT_DATA", RespCommand::ClusterSnapshotData),
  ("SPUBLISH", RespCommand::ClusterSpublish),
  ("SYNC", RespCommand::ClusterSync),
];

/// BITOP 子命令表（AND/OR/XOR/NOT/DIFF）
pub(crate) static BITOP_SUBTABLE: &[(&str, RespCommand)] = &[
  ("AND", RespCommand::BitopAnd),
  ("DIFF", RespCommand::BitopDiff),
  ("NOT", RespCommand::BitopNot),
  ("OR", RespCommand::BitopOr),
  ("XOR", RespCommand::BitopXor),
];

/// 父命令 → 子命令表清单（子命令哈希索引的唯一构建入口；表本体仍逐项可核）
const SUB_SOURCES: &[(RespCommand, &[(&str, RespCommand)])] = &[
  (RespCommand::Client, CLIENT_SUBTABLE),
  (RespCommand::Config, CONFIG_SUBTABLE),
  (RespCommand::Command, COMMAND_SUBTABLE),
  (RespCommand::Acl, ACL_SUBTABLE),
  (RespCommand::Script, SCRIPT_SUBTABLE),
  (RespCommand::Pubsub, PUBSUB_SUBTABLE),
  (RespCommand::Latency, LATENCY_SUBTABLE),
  (RespCommand::Slowlog, SLOWLOG_SUBTABLE),
  (RespCommand::Memory, MEMORY_SUBTABLE),
  (RespCommand::Object, OBJECT_SUBTABLE),
  (RespCommand::Cluster, CLUSTER_SUBTABLE),
  (RespCommand::Bitop, BITOP_SUBTABLE),
];

// ---------- 编译期哈希索引（FNV-1a + murmur3 终混，开放寻址线性探测） ----------

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 64 位（编译期与运行时共用一体）
const fn fnv1a(name: &[u8]) -> u64 {
  let mut hash = FNV_OFFSET;
  let mut i = 0;
  while i < name.len() {
    hash ^= name[i] as u64;
    hash = hash.wrapping_mul(FNV_PRIME);
    i += 1;
  }
  hash
}

/// murmur3 fmix64 终混：把 FNV 的高位熵摊到低位（槽位取低位掩码）
const fn mix64(mut hash: u64) -> u64 {
  hash ^= hash >> 33;
  hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
  hash ^= hash >> 33;
  hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
  hash ^= hash >> 33;
  hash
}

/// const 上下文可用的字节全等（`str` 的 `==` 非 const）
const fn bytes_eq(a: &[u8], b: &[u8]) -> bool {
  if a.len() != b.len() {
    return false;
  }
  let mut i = 0;
  while i < a.len() {
    if a[i] != b[i] {
      return false;
    }
    i += 1;
  }
  true
}

/// 哈希槽位（2 的幂容量 + 掩码，装载因子由下方编译期断言封顶 ≤ 2/3）
const PRIMARY_SLOTS: usize = 512; // 主表 ~264 项
const PRIMARY_MASK: usize = PRIMARY_SLOTS - 1;
const SUB_SLOTS: usize = 256; // 子命令合计 ~99 项
const SUB_MASK: usize = SUB_SLOTS - 1;

/// 子命令联合哈希：名字 FNV 混入父命令判别值后终混
const fn sub_hash(parent: RespCommand, name: &[u8]) -> u64 {
  mix64(fnv1a(name) ^ (parent as u64).wrapping_mul(FNV_PRIME))
}

/// 从 `PRIMARY_TABLE` 编译期构建开放寻址索引；重复键即编译失败
const fn build_primary_index() -> [Option<PrimaryEntry>; PRIMARY_SLOTS] {
  let mut index: [Option<PrimaryEntry>; PRIMARY_SLOTS] = [None; PRIMARY_SLOTS];
  let mut i = 0;
  while i < PRIMARY_TABLE.len() {
    let entry @ (name, ..) = PRIMARY_TABLE[i];
    let mut slot = mix64(fnv1a(name.as_bytes())) as usize & PRIMARY_MASK;
    while let Some((dup, ..)) = index[slot] {
      assert!(
        !bytes_eq(dup.as_bytes(), name.as_bytes()),
        "PRIMARY_TABLE 存在重复命令名"
      );
      slot = (slot + 1) & PRIMARY_MASK;
    }
    index[slot] = Some(entry);
    i += 1;
  }
  index
}

/// 从 `SUB_SOURCES` 编译期构建 (父命令, 子命令名) 联合索引；重复键即编译失败
const fn build_sub_index() -> [Option<(RespCommand, &'static str, RespCommand)>; SUB_SLOTS] {
  let mut index: [Option<(RespCommand, &'static str, RespCommand)>; SUB_SLOTS] = [None; SUB_SLOTS];
  let mut s = 0;
  while s < SUB_SOURCES.len() {
    let (parent, entries) = SUB_SOURCES[s];
    let mut i = 0;
    while i < entries.len() {
      let (name, cmd) = entries[i];
      let mut slot = sub_hash(parent, name.as_bytes()) as usize & SUB_MASK;
      while let Some((dup_parent, dup_name, _)) = index[slot] {
        assert!(
          !(dup_parent as u16 == parent as u16 && bytes_eq(dup_name.as_bytes(), name.as_bytes())),
          "子命令表存在重复 (父命令, 名)"
        );
        slot = (slot + 1) & SUB_MASK;
      }
      index[slot] = Some((parent, name, cmd));
      i += 1;
    }
    s += 1;
  }
  index
}

/// 子命令表项总数（编译期求和，供装载因子断言）
const fn sub_entry_total() -> usize {
  let mut total = 0;
  let mut i = 0;
  while i < SUB_SOURCES.len() {
    total += SUB_SOURCES[i].1.len();
    i += 1;
  }
  total
}

// 装载因子不变式：探测链必在空槽处终止（未命中回退路径的终止性前提）
const _: () = {
  assert!(
    PRIMARY_TABLE.len() * 3 <= PRIMARY_SLOTS * 2,
    "主表装载因子超 2/3"
  );
  assert!(
    sub_entry_total() * 3 <= SUB_SLOTS * 2,
    "子命令装载因子超 2/3"
  );
};

/// 主命令编译期哈希索引（const 初始化 static，无 lazy、无锁）
static PRIMARY_INDEX: [Option<PrimaryEntry>; PRIMARY_SLOTS] = build_primary_index();

/// 子命令编译期联合哈希索引（一次哈希覆盖全部 12 张子表）
static SUB_INDEX: [Option<(RespCommand, &'static str, RespCommand)>; SUB_SLOTS] = build_sub_index();

/// 主命令查表：返回 (命令, 是否含子命令)。一次哈希 + 期望 O(1) 探测，
/// 命中判据与原二分检索同为「与表项字面量逐字节全等」（大小写敏感，
/// 未命中探测至空槽即止，回退路径不变）
#[inline]
pub(crate) fn lookup_primary(name: &[u8]) -> Option<(RespCommand, bool)> {
  let mut slot = mix64(fnv1a(name)) as usize & PRIMARY_MASK;
  loop {
    match PRIMARY_INDEX[slot] {
      None => return None,
      Some((entry, cmd, has_subcommands)) => {
        if entry.as_bytes() == name {
          return Some((cmd, has_subcommands));
        }
      }
    }
    slot = (slot + 1) & PRIMARY_MASK;
  }
}

/// 子命令查表（C# RespCommandHashLookup.LookupSubcommand）。父命令判别值
/// 折入哈希，免「父命令 match 选表 + 表内二分」两级派发，一次哈希直达
#[inline]
pub(crate) fn lookup_subcommand(parent: RespCommand, name: &[u8]) -> Option<RespCommand> {
  let mut slot = sub_hash(parent, name) as usize & SUB_MASK;
  loop {
    match SUB_INDEX[slot] {
      None => return None,
      Some((entry_parent, entry_name, cmd)) => {
        if entry_parent == parent && entry_name.as_bytes() == name {
          return Some(cmd);
        }
      }
    }
    slot = (slot + 1) & SUB_MASK;
  }
}
