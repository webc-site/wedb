//! RESP 命令表与查找逻辑（对标 libs/server/Resp/Parser/RespCommandHashLookupData.cs
//! 与 libs/server/Resp/Parser/RespCommandHashLookup.cs）

use wresp::command::RespCommand;

/// 主命令名称表项
type PrimaryEntry = (&'static str, RespCommand, bool);

/// 主命令名称表（有序，二分检索）。`has_subcommands` 标记经子命令表分派；
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

/// 子命令表：父命令 → (子命令名, RespCommand)（按名 ASCII 有序，承接二分检索）
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

/// 有序表二分检索（C# RespCommandHashLookup.Lookup 的表语义等价）
#[inline]
fn lookup_in_table(table: &[(&str, RespCommand)], name: &[u8]) -> Option<RespCommand> {
  table
    .binary_search_by(|(entry_name, _)| entry_name.as_bytes().cmp(name))
    .ok()
    .map(|idx| table[idx].1)
}

/// 主命令查表：返回 (命令, 是否含子命令)
#[inline]
pub(crate) fn lookup_primary(name: &[u8]) -> Option<(RespCommand, bool)> {
  let idx = PRIMARY_TABLE
    .binary_search_by(|(entry_name, ..)| entry_name.as_bytes().cmp(name))
    .ok()?;
  let (_, cmd, has_subcommands) = PRIMARY_TABLE[idx];
  Some((cmd, has_subcommands))
}

/// 子命令查表（C# RespCommandHashLookup.LookupSubcommand）
#[inline]
pub(crate) fn lookup_subcommand(parent: RespCommand, name: &[u8]) -> Option<RespCommand> {
  let table = match parent {
    RespCommand::Client => CLIENT_SUBTABLE,
    RespCommand::Config => CONFIG_SUBTABLE,
    RespCommand::Command => COMMAND_SUBTABLE,
    RespCommand::Acl => ACL_SUBTABLE,
    RespCommand::Script => SCRIPT_SUBTABLE,
    RespCommand::Pubsub => PUBSUB_SUBTABLE,
    RespCommand::Latency => LATENCY_SUBTABLE,
    RespCommand::Slowlog => SLOWLOG_SUBTABLE,
    RespCommand::Memory => MEMORY_SUBTABLE,
    RespCommand::Object => OBJECT_SUBTABLE,
    RespCommand::Cluster => CLUSTER_SUBTABLE,
    RespCommand::Bitop => BITOP_SUBTABLE,
    _ => &[],
  };
  lookup_in_table(table, name)
}
