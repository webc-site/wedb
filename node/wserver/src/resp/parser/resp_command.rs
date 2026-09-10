//! RESP 命令解析（对标 libs/server/Resp/Parser/RespCommand.cs）
//!
//! C# `RespCommand.cs` 为 `RespServerSession` 的 partial 分片（命令枚举 +
//! 快速解析 + 哈希查表 + 子命令分派），Rust 侧映射为本文件的
//! `impl RespServerSession` 扩展块；命令枚举本体在
//! [`RespCommand`](crate::types::RespCommand)（types 域，判别值逐项对齐）。
//!
//! 解析分级（与 C# 一致）：
//! 1. 内联命令（`PING\r\n` / `QUIT\r\n`）；
//! 2. 16 字节模式表（固定参数个数热命令，C# SIMD Vector128 路径的标量
//!    等价；模式字节逐项与 RespCommandSimdPatterns.cs 对齐）+ 会话 MRU
//!    双槽缓存；
//! 3. 单数字帧标量快路径（`*_\r\n$_\r\n` 掩码比较；二级表含变参热命令与
//!    SET/GETEX/EXPIRE/PEXPIRE 长名命令）；
//! 4. 慢路径：大写化 → 数组头 → 命令名查表（有序名称表二分承接
//!    RespCommandHashLookup 的 Lookup / LookupSubcommand 语义）→ 子命令分派。

use std::mem::{swap, take};

use super::{
  super::{cmd_strings as cs, resp_server_session::RespServerSession},
  session_parse_state,
};
use crate::types::RespCommand;

/// C# MaxRespArrayLength：单条 RESP 命令参数上限（防预认证内存耗尽）
pub const MAX_RESP_ARRAY_LENGTH: usize = 1 << 20;

/// 主命令名称表项
type PrimaryEntry = (&'static str, RespCommand, bool);

/// 主命令名称表（有序，二分检索）。`has_subcommands` 标记经子命令表分派；
/// 逐项对标 RespCommandHashLookupData.cs:PopulatePrimaryTable（含 SLAVEOF/
/// SECONDARYOF 同命令双名与 RI.* 点名命令）。
static PRIMARY_TABLE: &[PrimaryEntry] = &[
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
  ("MODULE", RespCommand::Module, true),
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
  ("REGISTERCS", RespCommand::Registercs, false),
  ("RENAME", RespCommand::Rename, false),
  ("RENAMENX", RespCommand::Renamenx, false),
  ("REPLICAOF", RespCommand::Replicaof, false),
  ("RESTORE", RespCommand::Restore, false),
  ("RI.CONFIG", RespCommand::Riconfig, false),
  ("RI.CREATE", RespCommand::Ricreate, false),
  ("RI.DEL", RespCommand::Ridel, false),
  ("RI.EXISTS", RespCommand::Riexists, false),
  ("RI.GET", RespCommand::Riget, false),
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
static CLIENT_SUBTABLE: &[(&str, RespCommand)] = &[
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
static CONFIG_SUBTABLE: &[(&str, RespCommand)] = &[
  ("GET", RespCommand::ConfigGet),
  ("REWRITE", RespCommand::ConfigRewrite),
  ("SET", RespCommand::ConfigSet),
];

/// Command 子命令表
static COMMAND_SUBTABLE: &[(&str, RespCommand)] = &[
  ("COUNT", RespCommand::CommandCount),
  ("DOCS", RespCommand::CommandDocs),
  ("GETKEYS", RespCommand::CommandGetkeys),
  ("GETKEYSANDFLAGS", RespCommand::CommandGetkeysandflags),
  ("INFO", RespCommand::CommandInfo),
];

/// Acl 子命令表
static ACL_SUBTABLE: &[(&str, RespCommand)] = &[
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
static SCRIPT_SUBTABLE: &[(&str, RespCommand)] = &[
  ("EXISTS", RespCommand::ScriptExists),
  ("FLUSH", RespCommand::ScriptFlush),
  ("LOAD", RespCommand::ScriptLoad),
];

/// Pubsub 子命令表
static PUBSUB_SUBTABLE: &[(&str, RespCommand)] = &[
  ("CHANNELS", RespCommand::PubsubChannels),
  ("NUMPAT", RespCommand::PubsubNumpat),
  ("NUMSUB", RespCommand::PubsubNumsub),
];

/// Latency 子命令表
static LATENCY_SUBTABLE: &[(&str, RespCommand)] = &[
  ("HELP", RespCommand::LatencyHelp),
  ("HISTOGRAM", RespCommand::LatencyHistogram),
  ("RESET", RespCommand::LatencyReset),
];

/// Slowlog 子命令表
static SLOWLOG_SUBTABLE: &[(&str, RespCommand)] = &[
  ("GET", RespCommand::SlowlogGet),
  ("HELP", RespCommand::SlowlogHelp),
  ("LEN", RespCommand::SlowlogLen),
  ("RESET", RespCommand::SlowlogReset),
];

/// MODULE 子命令表
static MODULE_SUBTABLE: &[(&str, RespCommand)] = &[("LOADCS", RespCommand::ModuleLoadcs)];

/// Memory 子命令表
static MEMORY_SUBTABLE: &[(&str, RespCommand)] = &[("USAGE", RespCommand::MemoryUsage)];

/// Object 子命令表
static OBJECT_SUBTABLE: &[(&str, RespCommand)] = &[
  ("ENCODING", RespCommand::ObjectEncoding),
  ("FREQ", RespCommand::ObjectFreq),
  ("HELP", RespCommand::ObjectHelp),
  ("IDLETIME", RespCommand::ObjectIdletime),
  ("REFCOUNT", RespCommand::ObjectRefcount),
];

/// CLUSTER 子命令表（含 SET-CONFIG-EPOCH 连字符名）
static CLUSTER_SUBTABLE: &[(&str, RespCommand)] = &[
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
static BITOP_SUBTABLE: &[(&str, RespCommand)] = &[
  ("AND", RespCommand::BitopAnd),
  ("DIFF", RespCommand::BitopDiff),
  ("NOT", RespCommand::BitopNot),
  ("OR", RespCommand::BitopOr),
  ("XOR", RespCommand::BitopXor),
];

/// 有序表二分检索（C# RespCommandHashLookup.Lookup 的表语义等价）
fn lookup_in_table(table: &[(&str, RespCommand)], name: &[u8]) -> Option<RespCommand> {
  table
    .binary_search_by(|(entry_name, _)| entry_name.as_bytes().cmp(name))
    .ok()
    .map(|idx| table[idx].1)
}

/// 主命令查表：返回 (命令, 是否含子命令)
fn lookup_primary(name: &[u8]) -> Option<(RespCommand, bool)> {
  let idx = PRIMARY_TABLE
    .binary_search_by(|(entry_name, ..)| entry_name.as_bytes().cmp(name))
    .ok()?;
  let (_, cmd, has_subcommands) = PRIMARY_TABLE[idx];
  Some((cmd, has_subcommands))
}

/// 子命令查表（C# RespCommandHashLookup.LookupSubcommand）
fn lookup_subcommand(parent: RespCommand, name: &[u8]) -> Option<RespCommand> {
  let table = match parent {
    RespCommand::Client => CLIENT_SUBTABLE,
    RespCommand::Config => CONFIG_SUBTABLE,
    RespCommand::Command => COMMAND_SUBTABLE,
    RespCommand::Acl => ACL_SUBTABLE,
    RespCommand::Script => SCRIPT_SUBTABLE,
    RespCommand::Pubsub => PUBSUB_SUBTABLE,
    RespCommand::Latency => LATENCY_SUBTABLE,
    RespCommand::Slowlog => SLOWLOG_SUBTABLE,
    RespCommand::Module => MODULE_SUBTABLE,
    RespCommand::Memory => MEMORY_SUBTABLE,
    RespCommand::Object => OBJECT_SUBTABLE,
    RespCommand::Cluster => CLUSTER_SUBTABLE,
    RespCommand::Bitop => BITOP_SUBTABLE,
    _ => &[],
  };
  lookup_in_table(table, name)
}

/// C# IsAofIndependent：AOF 无关命令集（读写不依赖日志直写）。
/// 逐项对标 libs/server/Resp/Parser/RespCommand.cs:AofIndependentCommands
///（47 项，含 CLIENT/COMMAND/MEMORY/CONFIG/LATENCY/SLOWLOG 全族与 MULTI）
static AOF_INDEPENDENT_COMMANDS: &[RespCommand] = &[
  RespCommand::Async,
  RespCommand::Ping,
  RespCommand::Select,
  RespCommand::Swapdb,
  RespCommand::Echo,
  RespCommand::Monitor,
  RespCommand::ModuleLoadcs,
  RespCommand::Registercs,
  RespCommand::Info,
  RespCommand::Time,
  RespCommand::Lastsave,
  // ACL 族
  RespCommand::AclCat,
  RespCommand::AclDeluser,
  RespCommand::AclGenpass,
  RespCommand::AclGetuser,
  RespCommand::AclList,
  RespCommand::AclLoad,
  RespCommand::AclSave,
  RespCommand::AclSetuser,
  RespCommand::AclUsers,
  RespCommand::AclWhoami,
  // Client 族
  RespCommand::ClientId,
  RespCommand::ClientInfo,
  RespCommand::ClientList,
  RespCommand::ClientKill,
  RespCommand::ClientGetname,
  RespCommand::ClientSetname,
  RespCommand::ClientSetinfo,
  RespCommand::ClientUnblock,
  // Command 族
  RespCommand::Command,
  RespCommand::CommandCount,
  RespCommand::CommandDocs,
  RespCommand::CommandInfo,
  RespCommand::CommandGetkeys,
  RespCommand::CommandGetkeysandflags,
  // Memory / Config 族
  RespCommand::MemoryUsage,
  RespCommand::ConfigGet,
  RespCommand::ConfigRewrite,
  RespCommand::ConfigSet,
  // Latency 族
  RespCommand::LatencyHelp,
  RespCommand::LatencyHistogram,
  RespCommand::LatencyReset,
  // Slowlog 族
  RespCommand::SlowlogHelp,
  RespCommand::SlowlogLen,
  RespCommand::SlowlogGet,
  RespCommand::SlowlogReset,
  // 事务
  RespCommand::Multi,
];

/// C# RespCommandExtensions.IsAofIndependent
#[inline]
pub fn is_aof_independent(cmd: RespCommand) -> bool {
  AOF_INDEPENDENT_COMMANDS.contains(&cmd)
}

/// 固定形状热命令模式表：`( RESP 帧前缀, 命令, 参数个数 )`
///
/// 帧字节逐项对齐 RespCommandSimdPatterns.cs 的 RespPattern(argCount, cmd)：
/// `*N` 的 N = 参数个数 + 1（数组元素总数，含命令名）；13..15 字节模式在
/// C# 以掩码忽略模式长度之后的字节，16 字节模式（6 字符命令）为全等比较。
static FAST_PATTERN_TABLE: &[(&[u8], RespCommand, u8)] = &[
  // 13 字节：3 字符命令
  (b"*2\r\n$3\r\nGET\r\n", RespCommand::Get, 1),
  (b"*3\r\n$3\r\nSET\r\n", RespCommand::Set, 2),
  (b"*2\r\n$3\r\nDEL\r\n", RespCommand::Del, 1),
  (b"*2\r\n$3\r\nTTL\r\n", RespCommand::Ttl, 1),
  // 14 字节：4 字符命令
  (b"*1\r\n$4\r\nPING\r\n", RespCommand::Ping, 0),
  (b"*2\r\n$4\r\nINCR\r\n", RespCommand::Incr, 1),
  (b"*2\r\n$4\r\nDECR\r\n", RespCommand::Decr, 1),
  (b"*1\r\n$4\r\nEXEC\r\n", RespCommand::Exec, 0),
  (b"*2\r\n$4\r\nPTTL\r\n", RespCommand::Pttl, 1),
  // 15 字节：5 字符命令
  (b"*1\r\n$5\r\nMULTI\r\n", RespCommand::Multi, 0),
  (b"*3\r\n$5\r\nSETNX\r\n", RespCommand::Setnx, 2),
  (b"*4\r\n$5\r\nSETEX\r\n", RespCommand::Setex, 3),
  // 16 字节：6 字符命令（无掩码，全等）
  (b"*2\r\n$6\r\nEXISTS\r\n", RespCommand::Exists, 1),
  (b"*2\r\n$6\r\nGETDEL\r\n", RespCommand::Getdel, 1),
  (b"*3\r\n$6\r\nAPPEND\r\n", RespCommand::Append, 2),
  (b"*3\r\n$6\r\nINCRBY\r\n", RespCommand::Incrby, 2),
  (b"*3\r\n$6\r\nDECRBY\r\n", RespCommand::Decrby, 2),
  (b"*4\r\n$6\r\nPSETEX\r\n", RespCommand::Psetex, 3),
];

/// 模式比较（C# Vector128 载入 + 掩码 + EqualsAll 的标量等价：
/// 仅比较模式长度内的字节，模式之后的输入字节不作约束）
#[inline]
fn pattern_matches(buffer: &[u8], start: usize, pattern: &[u8]) -> bool {
  buffer.len() >= start + pattern.len() && &buffer[start..start + pattern.len()] == pattern
}

/// 会话侧 MRU 命令缓存（C# _cachedCmd0/1 双槽；哈希表命中后填充，
/// 槽 1 命中时晋升换位 —— C# SimdFastParse 的 promote 语义）
#[derive(Default)]
pub(crate) struct MruCommandCache {
  slot0: Option<MruEntry>,
  slot1: Option<MruEntry>,
}

#[derive(Clone, Copy)]
struct MruEntry {
  pattern: [u8; 16],
  len: u8,
  cmd: RespCommand,
  count: u8,
}

impl MruCommandCache {
  /// C# UpdateCommandCache 尾段：新命中晋升槽 0，原槽 0 降级槽 1
  fn update(&mut self, frame: &[u8], consumed: usize, cmd: RespCommand, count: u8) {
    debug_assert!((13..=16).contains(&consumed) && frame.len() >= consumed);
    let mut entry = MruEntry {
      pattern: [0; 16],
      len: consumed as u8,
      cmd,
      count,
    };
    entry.pattern[..consumed].copy_from_slice(&frame[..consumed]);
    self.slot1 = self.slot0;
    self.slot0 = Some(entry);
  }

  /// 查找命中帧与命中槽号（槽 1 命中由调用方执行晋升换位）
  fn lookup(&self, buffer: &[u8], start: usize) -> Option<(MruEntry, usize)> {
    for (slot_idx, slot) in [self.slot0, self.slot1].into_iter().enumerate() {
      let Some(entry) = slot else { continue };
      let len = entry.len as usize;
      if pattern_matches(buffer, start, &entry.pattern[..len]) {
        return Some((entry, slot_idx));
      }
    }
    None
  }

  /// 槽 1 命中 → 两槽互换（C# SimdFastParse 的 swap 晋升）
  fn promote(&mut self, slot_idx: usize) {
    if slot_idx == 1 {
      swap(&mut self.slot0, &mut self.slot1);
    }
  }
}

impl RespServerSession {
  /// libs/server/Resp/Parser/RespCommand.cs:ParseCommand
  ///
  /// 解析缓冲内下一条命令：快路径 → 慢路径 → 装载解析态 → AOF 阻塞标记。
  /// 返回 None 表示命令未完整到达或协议错误（C# success = false / 抛
  /// RespParsingException，由上层断连）；未知命令返回 RespCommand::Invalid
  /// 并按 C# writeErrorOnFailure 写错误应答。
  pub fn parse_command(&mut self) -> Option<RespCommand> {
    self.parse_command_with(true)
  }

  /// ParseCommand 主体（`write_error_on_failure` 对齐 C# 同名参数；
  /// 独立缓冲校验入口传 false，不往输出缓冲写解析错误）
  fn parse_command_with(&mut self, write_error_on_failure: bool) -> Option<RespCommand> {
    let mut count: isize = -1;
    self.end_read_head = self.read_head;

    // 快速解析
    let mut cmd = self.fast_parse_command(&mut count);

    // 慢路径
    if cmd == RespCommand::None {
      let cmd_start_offset = self.read_head;
      cmd = self.array_parse_command(&mut count, write_error_on_failure)?;
      // MRU 缓存更新（哈希表命中的命令；排除运行时注册名的自定义命令）
      if cmd != RespCommand::Invalid
        && cmd != RespCommand::None
        && !matches!(
          cmd,
          RespCommand::Customtxn
            | RespCommand::Customprocedure
            | RespCommand::Customrawstringcmd
            | RespCommand::Customobjcmd
        )
      {
        self.update_command_cache(cmd_start_offset, cmd, count);
      }
    }

    if count > MAX_RESP_ARRAY_LENGTH as isize {
      // C# RespParsingException.ThrowExcessiveArgumentCount → 上层断连（不写应答）
      return None;
    }
    let count = count.max(0) as usize;

    // 装载解析态（C# parseState.Initialize(count) + parseState.Read(i, ...)）
    self.parse_state.initialize(count);
    let mut ptr = self.read_head;
    for i in 0..count {
      if !session_parse_state::read(
        &mut self.parse_state,
        i,
        &self.recv_buffer,
        &mut ptr,
        self.bytes_read,
      ) {
        return None;
      }
    }
    self.end_read_head = ptr;

    // C# EnableAOF + WaitForCommit 时按命令依赖性维护阻塞标记
    self.handle_aof_commit_mode(cmd);
    Some(cmd)
  }

  /// libs/server/Resp/Parser/RespCommand.cs:UpdateCommandCache
  ///
  /// 参数个数超 u8 / 可用字节不足 16 / 实际消费不在 13..=16 字节均不缓存
  fn update_command_cache(&mut self, cmd_start_offset: usize, cmd: RespCommand, arg_count: isize) {
    let arg_count = arg_count.max(0) as usize;
    if arg_count > usize::from(u8::MAX) {
      return;
    }
    // 16 字节窗口须完整可用（C# Vector128.LoadUnsafe 前置条件）
    if self.bytes_read - cmd_start_offset < 16 {
      return;
    }
    // 命令名（含子命令）的解析消费长度，仅单 Vector128 窗口内可缓存
    let consumed = self.read_head - cmd_start_offset;
    if !(13..=16).contains(&consumed) {
      return;
    }
    let frame = &self.recv_buffer[cmd_start_offset..cmd_start_offset + 16];
    self.mru_cache.update(frame, consumed, cmd, arg_count as u8);
  }

  /// libs/server/Resp/Parser/RespCommand.cs:FastParseCommand
  pub fn fast_parse_command(&mut self, count: &mut isize) -> RespCommand {
    let start = self.read_head;
    let remaining = self.bytes_read.saturating_sub(start);

    // 模式表 + MRU 快路径（C# SIMD 门控等价：>= 16 字节且数组帧；
    // 模式长度之后的字节不作约束，与 C# 尾部掩码语义一致）
    if remaining >= 16 && self.recv_buffer[start] == b'*' {
      for (pattern, cmd, arg_count) in FAST_PATTERN_TABLE {
        if pattern_matches(&self.recv_buffer, start, pattern) {
          self.read_head += pattern.len();
          *count = isize::from(*arg_count);
          return *cmd;
        }
      }
      if let Some((entry, slot_idx)) = self.mru_cache.lookup(&self.recv_buffer, start) {
        self.mru_cache.promote(slot_idx);
        self.read_head += entry.len as usize;
        *count = isize::from(entry.count);
        return entry.cmd;
      }
    }

    // 标量快路径：单数字数组帧 + 单数字串长（C# 0xFFFF00FFFFFF00FF 掩码技巧）
    if remaining >= 8
      && self.recv_buffer[start] == b'*'
      && self.recv_buffer[start + 2] == b'\r'
      && self.recv_buffer[start + 3] == b'\n'
      && self.recv_buffer[start + 4] == b'$'
      && self.recv_buffer[start + 6] == b'\r'
      && self.recv_buffer[start + 7] == b'\n'
    {
      // 数组元素总数 - 1（首 token 即命令名；i64 算术杜绝 u8 下溢 panic）
      *count = i64::from(self.recv_buffer[start + 1]) as isize - isize::from(b'1');
      let length = i64::from(self.recv_buffer[start + 5]) - i64::from(b'0');

      // 命令名 1..=9 字节且完整帧在缓冲内（10 = 帧头 8 + 名尾 \r\n 2）
      if (1..=9).contains(&length) && remaining >= length as usize + 10 {
        let frame_len = length as usize + 10;
        let frame_end = start + frame_len;
        self.read_head += frame_len;

        // (1) 固定参数个数热命令（C# 第一标量表：count/lastWord 判定，
        // 与整帧字节比对等价；缓冲 < 16 字节时的主路径）
        for (pattern, cmd, _) in FAST_PATTERN_TABLE {
          if pattern.len() == frame_len && pattern_matches(&self.recv_buffer, start, pattern) {
            return *cmd;
          }
        }

        // (2) 变参热命令 + 名称超 6 字符命令（C# 第二标量表）
        // lastWord = 帧末 8 字节；prefix = 名首 2 字节
        let buf = &self.recv_buffer;
        let last_word = &buf[start + length as usize + 2..frame_end];
        let prefix = &buf[start + 8..start + 10];
        return match (*count, length) {
          (2, 7) if last_word == b"UBLISH\r\n" && buf[start + 8] == b'P' => RespCommand::Publish,
          (2, 8) if last_word == b"UBLISH\r\n" && prefix == b"SP" => RespCommand::Spublish,
          (3, 8) if last_word == b"TRANGE\r\n" && prefix == b"SE" => RespCommand::Setrange,
          (3, 8) if last_word == b"TRANGE\r\n" && prefix == b"GE" => RespCommand::Getrange,
          // (3) 长名/变参（C# 嵌套 (length << 4) | count 表）
          (3..=7, 3) if last_word == b"3\r\nSET\r\n" => RespCommand::Setexnx,
          (1..=3, 5) if last_word == b"\nGETEX\r\n" => RespCommand::Getex,
          (2..=3, 6) if last_word == b"EXPIRE\r\n" => RespCommand::Expire,
          (2..=3, 7) if last_word == b"EXPIRE\r\n" && buf[start + 8] == b'P' => {
            RespCommand::Pexpire
          }
          _ => self.matched_none(start, count),
        };
      }
      *count = -1;
      return RespCommand::None;
    }

    // 内联命令
    self.fast_parse_inline_command(count)
  }

  /// libs/server/Resp/Parser/RespCommand.cs:FastParseInlineCommand
  pub fn fast_parse_inline_command(&mut self, count: &mut isize) -> RespCommand {
    let start = self.read_head;
    *count = 0;
    // 内联命令形如 "XXXX\r\n"，首猜 PING / QUIT（精确大小写匹配，对齐 C#）
    if self.bytes_read - start >= 6 {
      let word = &self.recv_buffer[start..start + 4];
      if &self.recv_buffer[start + 4..start + 6] == b"\r\n" {
        self.read_head += 6;
        if word == b"PING" {
          return RespCommand::Ping;
        }
        if word == b"QUIT" {
          return RespCommand::Quit;
        }
        // 未命中回退游标
        self.read_head -= 6;
      }
    }
    RespCommand::None
  }

  /// libs/server/Resp/Parser/RespCommand.cs:MatchedNone（局部函数）
  fn matched_none(&mut self, old_read_head: usize, count: &mut isize) -> RespCommand {
    self.read_head = old_read_head;
    *count = -1;
    RespCommand::None
  }

  /// libs/server/Resp/Parser/RespCommand.cs:TryParseCustomCommand
  ///
  /// 自定义命令注册表由 custom 域承载；注册/匹配接线前恒未命中（None 命令
  /// 不进入自定义分派），与 C# 注册表为空时的行为一致。
  pub fn try_parse_custom_command(&mut self, command: &[u8]) -> Option<RespCommand> {
    let _ = command;
    None
  }

  /// libs/server/Resp/Parser/RespCommand.cs:AttemptSkipLine
  ///
  /// 跳至行尾（畸形内联输入）；找到 "\r\n" 返回 true 并推进双游标
  pub fn attempt_skip_line(&mut self) -> bool {
    let mut string_end = self.read_head;
    while string_end + 1 < self.bytes_read {
      if self.recv_buffer[string_end] == b'\r' && self.recv_buffer[string_end + 1] == b'\n' {
        self.read_head = string_end + 2;
        self.end_read_head = self.read_head;
        return true;
      }
      string_end += 1;
    }
    false
  }

  /// libs/server/Resp/Parser/RespCommand.cs:ParseRespCommandBuffer
  ///
  /// 独立缓冲命令解析（校验用途；不写错误应答）
  pub fn parse_resp_command_buffer(&mut self, buffer: &[u8]) -> Option<RespCommand> {
    let saved = self.take_receive_state();
    self.recv_buffer.clear();
    self.recv_buffer.extend_from_slice(buffer);
    self.bytes_read = self.recv_buffer.len();
    self.read_head = 0;
    let parsed = self.parse_command_with(false);
    self.restore_receive_state(saved);
    parsed.filter(|cmd| *cmd != RespCommand::Invalid)
  }

  /// libs/server/Resp/Parser/RespCommand.cs:FuzzParseCommandBuffer
  ///
  /// 模糊测试入口：允许部分命令；返回 (是否完整解析, 命令)
  pub fn fuzz_parse_command_buffer(&mut self, buffer: &[u8]) -> (bool, RespCommand) {
    let saved = self.take_receive_state();
    self.recv_buffer.clear();
    self.recv_buffer.extend_from_slice(buffer);
    self.bytes_read = self.recv_buffer.len();
    self.read_head = 0;

    let cmd = if self.bytes_read >= 4 {
      self
        .parse_command_with(false)
        .unwrap_or(RespCommand::Invalid)
    } else {
      RespCommand::Invalid
    };
    self.restore_receive_state(saved);
    (cmd != RespCommand::Invalid, cmd)
  }

  /// libs/server/Resp/Parser/RespCommand.cs:HandleAofCommitMode
  ///
  /// 无未发送数据时重置阻塞标记；命令 AOF 相关则保持/置位
  pub fn handle_aof_commit_mode(&mut self, cmd: RespCommand) {
    if self.pending_output_len() == 0 {
      self.wait_for_aof_blocking = false;
    }
    // 事务跳过模式中的命令不执行，不置位
    if self.txn_state == TxnState::Started {
      return;
    }
    self.wait_for_aof_blocking = self.wait_for_aof_blocking || !is_aof_independent(cmd);
  }

  /// libs/server/Resp/Parser/RespCommand.cs:ArrayParseCommand
  pub fn array_parse_command(
    &mut self,
    count: &mut isize,
    write_error_on_failure: bool,
  ) -> Option<RespCommand> {
    self.end_read_head = self.read_head;
    let start = self.read_head;

    // 全大写化后重试快路径（C# MakeUpperCase → FastParseCommand）
    if start < self.bytes_read && self.make_upper_case(start, self.bytes_read - start) {
      let cmd = self.fast_parse_command(count);
      if cmd != RespCommand::None {
        return Some(cmd);
      }
    }

    // 须为数组帧
    if start >= self.bytes_read || self.recv_buffer[start] != b'*' {
      // 内联命令包：跳行（畸形输入；行尾未完整到达返回 None）
      if !self.attempt_skip_line() {
        return None;
      }
      return Some(RespCommand::Invalid);
    }

    // 读数组长度（多位数字）
    let mut ptr = start + 1;
    let mut array_len = 0usize;
    while ptr < self.bytes_read && self.recv_buffer[ptr].is_ascii_digit() {
      array_len = array_len
        .saturating_mul(10)
        .saturating_add(usize::from(self.recv_buffer[ptr] - b'0'));
      ptr += 1;
    }
    if ptr + 2 > self.bytes_read || &self.recv_buffer[ptr..ptr + 2] != b"\r\n" {
      return None;
    }
    ptr += 2;
    self.read_head = ptr;
    *count = array_len as isize;

    // 命令名查表（哈希查表 + 子命令分派）；None = 命令名未完整到达
    //（C# success = false，不写错误应答，由上层等待后续字节）
    let mut specific_error: Option<Vec<u8>> = None;
    let cmd = self.hash_lookup_command(count, &mut specific_error)?;

    // 未知命令：写错误应答（C# writeErrorOnFailure 门；
    // RespWriteUtils.TryWriteError 的 -msg\r\n 封套）
    if write_error_on_failure && cmd == RespCommand::Invalid {
      if let Some(error) = specific_error {
        self.output.push(b'-');
        self.output.extend_from_slice(&error);
        self.output.extend_from_slice(b"\r\n");
        self.command_error_written = true;
      } else {
        self.abort_error_message(cs::RESP_ERR_GENERIC_UNK_CMD);
      }
    }
    Some(cmd)
  }

  /// libs/server/Resp/Parser/RespCommand.cs:HashLookupCommand
  ///
  /// 返回 None 表示命令名未完整到达（C# success = false）；未知命令返回
  /// Invalid 并填充 `specific_error`
  pub fn hash_lookup_command(
    &mut self,
    count: &mut isize,
    specific_error: &mut Option<Vec<u8>>,
  ) -> Option<RespCommand> {
    let command = self.get_command()?;
    // 命令名自读游标移除
    *count -= 1;

    match lookup_primary(&command) {
      None => {
        // 非内建命令 → 自定义命令
        if let Some(custom_cmd) = self.try_parse_custom_command(&command) {
          return Some(custom_cmd);
        }
        Some(RespCommand::Invalid)
      }
      Some((cmd, has_subcommands)) => {
        if has_subcommands {
          self.handle_subcommand_lookup(cmd, count, specific_error)
        } else {
          Some(cmd)
        }
      }
    }
  }

  /// libs/server/Resp/Parser/RespCommand.cs:HandleSubcommandLookup
  pub fn handle_subcommand_lookup(
    &mut self,
    parent_cmd: RespCommand,
    count: &mut isize,
    specific_error: &mut Option<Vec<u8>>,
  ) -> Option<RespCommand> {
    // COMMAND 无参 → COMMAND（列出全部命令）
    if parent_cmd == RespCommand::Command && *count == 0 {
      return Some(RespCommand::Command);
    }
    // 多数父命令要求至少一个子命令（BITOP 为语法错误文案；父命令名
    // 对齐 C# 枚举成员的大写形式）
    if *count == 0 {
      let parent = format!("{parent_cmd:?}").to_uppercase();
      *specific_error = Some(if parent_cmd == RespCommand::Bitop {
        cs::RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes().to_vec()
      } else {
        cs::GENERIC_ERR_WRONG_NUM_ARGS
          .replace("{0}", &parent)
          .into_bytes()
      });
      return Some(RespCommand::Invalid);
    }

    let sub_command = self.get_upper_case_command()?;
    *count -= 1;

    if let Some(sub_cmd) = lookup_subcommand(parent_cmd, &sub_command) {
      return Some(sub_cmd);
    }

    // 未知子命令错误文案（BITOP → 语法错误；CLUSTER/LATENCY 带帮助提示，
    // 其余为无提示版 —— 逐字节对齐 C# CmdStrings）
    let sub_text = String::from_utf8_lossy(&sub_command);
    let parent = format!("{parent_cmd:?}").to_uppercase();
    *specific_error = Some(if parent_cmd == RespCommand::Bitop {
      cs::RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes().to_vec()
    } else if matches!(parent_cmd, RespCommand::Cluster | RespCommand::Latency) {
      cs::GENERIC_ERR_UNKNOWN_SUB_COMMAND
        .replace("{0}", &sub_text)
        .replace("{1}", &parent)
        .into_bytes()
    } else {
      cs::GENERIC_ERR_UNKNOWN_SUB_COMMAND_NO_HELP
        .replace("{0}", &sub_text)
        .into_bytes()
    });
    Some(RespCommand::Invalid)
  }

  /// 接收状态快照（ParseRespCommandBuffer / FuzzParseCommandBuffer 恢复用）
  fn take_receive_state(&mut self) -> (Vec<u8>, usize, usize, usize) {
    (
      take(&mut self.recv_buffer),
      self.bytes_read,
      self.read_head,
      self.end_read_head,
    )
  }

  fn restore_receive_state(&mut self, saved: (Vec<u8>, usize, usize, usize)) {
    let (buffer, bytes_read, read_head, end_read_head) = saved;
    self.recv_buffer = buffer;
    self.bytes_read = bytes_read;
    self.read_head = read_head;
    self.end_read_head = end_read_head;
  }
}

use crate::resp::resp_server_session::TxnState;

#[cfg(test)]
mod tests {
  use super::*;

  /// 测试用父命令-子表三元组(父命令名, 父命令, 子命令表)
  type ParentSubtable = (
    &'static str,
    RespCommand,
    &'static [(&'static str, RespCommand)],
  );

  fn parse_one(
    session: &mut RespServerSession,
    buffer: &[u8],
  ) -> (Option<RespCommand>, Vec<Vec<u8>>) {
    session.recv_buffer.clear();
    session.recv_buffer.extend_from_slice(buffer);
    session.bytes_read = session.recv_buffer.len();
    session.read_head = 0;
    let cmd = session.parse_command();
    let args = (0..session.parse_state.count)
      .map(|i| {
        session
          .parse_state
          .get_arg_slice_by_ref(i)
          .as_slice()
          .to_vec()
      })
      .collect();
    (cmd, args)
  }

  #[test]
  fn fast_paths_parse_hot_commands() {
    let mut s = RespServerSession::default();
    // 恰 16 字节的帧首（*2 GET + 键头）命中模式表
    let (cmd, args) = parse_one(&mut s, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    assert_eq!(cmd, Some(RespCommand::Get));
    assert_eq!(args, vec![b"foo".to_vec()]);

    let (cmd, args) = parse_one(&mut s, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv1\r\n");
    assert_eq!(cmd, Some(RespCommand::Set));
    assert_eq!(args, vec![b"k".to_vec(), b"v1".to_vec()]);

    // *1 PING = 无参（数组元素仅命令名）
    let (cmd, args) = parse_one(&mut s, b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(cmd, Some(RespCommand::Ping));
    assert!(args.is_empty());

    // *2 PING msg → 慢路径解析（模式表不命中）
    let (cmd, args) = parse_one(&mut s, b"*2\r\n$4\r\nPING\r\n$3\r\nhey\r\n");
    assert_eq!(cmd, Some(RespCommand::Ping));
    assert_eq!(args, vec![b"hey".to_vec()]);

    // 小写命令走慢路径大写化
    let (cmd, args) = parse_one(&mut s, b"*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n");
    assert_eq!(cmd, Some(RespCommand::Get));
    assert_eq!(args, vec![b"foo".to_vec()]);

    // 内联 PING
    let (cmd, _) = parse_one(&mut s, b"PING\r\n");
    assert_eq!(cmd, Some(RespCommand::Ping));

    // EXISTS 16 字节全等模式
    let (cmd, args) = parse_one(&mut s, b"*2\r\n$6\r\nEXISTS\r\n$1\r\nk\r\n");
    assert_eq!(cmd, Some(RespCommand::Exists));
    assert_eq!(args, vec![b"k".to_vec()]);
  }

  #[test]
  fn scalar_fast_path_varargs_table() {
    let mut s = RespServerSession::default();
    // PUBLISH（长度 7，count 2）：二级表
    let (cmd, args) = parse_one(&mut s, b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$1\r\nb\r\n");
    assert_eq!(cmd, Some(RespCommand::Publish));
    assert_eq!(args, vec![b"foo".to_vec(), b"b".to_vec()]);

    // SPUBLISH（长度 8，count 2，名首 SP）
    let (cmd, _) = parse_one(&mut s, b"*3\r\n$8\r\nSPUBLISH\r\n$1\r\na\r\n$1\r\nb\r\n");
    assert_eq!(cmd, Some(RespCommand::Spublish));

    // SETRANGE / GETRANGE（长度 8，count 3，名首 SE/GE）
    let (cmd, _) = parse_one(
      &mut s,
      b"*4\r\n$8\r\nSETRANGE\r\n$1\r\nk\r\n$1\r\n0\r\n$1\r\nv\r\n",
    );
    assert_eq!(cmd, Some(RespCommand::Setrange));
    let (cmd, _) = parse_one(
      &mut s,
      b"*4\r\n$8\r\nGETRANGE\r\n$1\r\nk\r\n$1\r\n0\r\n$1\r\n9\r\n",
    );
    assert_eq!(cmd, Some(RespCommand::Getrange));

    // 带选项的 SET（count 3..7，名 SET）→ SETEXNX（C# 嵌套标量表）
    let (cmd, args) = parse_one(
      &mut s,
      b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nNX\r\n",
    );
    assert_eq!(cmd, Some(RespCommand::Setexnx));
    assert_eq!(args, vec![b"k".to_vec(), b"v".to_vec(), b"NX".to_vec()]);

    // GETEX key PERSIST（长度 5，count 2）
    let (cmd, args) = parse_one(&mut s, b"*3\r\n$5\r\nGETEX\r\n$1\r\nk\r\n$7\r\nPERSIST\r\n");
    assert_eq!(cmd, Some(RespCommand::Getex));
    assert_eq!(args, vec![b"k".to_vec(), b"PERSIST".to_vec()]);

    // EXPIRE key 100（长度 6，count 2）/ PEXPIRE（长度 7，名首 P）
    let (cmd, _) = parse_one(&mut s, b"*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$3\r\n100\r\n");
    assert_eq!(cmd, Some(RespCommand::Expire));
    let (cmd, _) = parse_one(&mut s, b"*3\r\n$7\r\nPEXPIRE\r\n$1\r\nk\r\n$3\r\n100\r\n");
    assert_eq!(cmd, Some(RespCommand::Pexpire));
  }

  #[test]
  fn mru_cache_captures_hash_lookup_commands() {
    let mut s = RespServerSession::default();
    // LPUSH 不在固定模式表 → 首次走慢路径（哈希查表），帧 15 字节进 MRU
    let (cmd, _) = parse_one(&mut s, b"*2\r\n$5\r\nLPUSH\r\n$1\r\nk\r\n");
    assert_eq!(cmd, Some(RespCommand::Lpush));
    // 第二次同帧 → MRU 槽 0 命中
    let (cmd, args) = parse_one(&mut s, b"*2\r\n$5\r\nLPUSH\r\n$1\r\nz\r\n");
    assert_eq!(cmd, Some(RespCommand::Lpush));
    assert_eq!(args, vec![b"z".to_vec()]);
    // HSET 帧入槽 0，LPUSH 降级槽 1；再发 LPUSH → 槽 1 命中并晋升
    let (cmd, _) = parse_one(
      &mut s,
      b"*4\r\n$4\r\nHSET\r\n$1\r\nk\r\n$1\r\nf\r\n$1\r\nv\r\n",
    );
    assert_eq!(cmd, Some(RespCommand::Hset));
    let (cmd, _) = parse_one(&mut s, b"*2\r\n$5\r\nLPUSH\r\n$1\r\nq\r\n");
    assert_eq!(cmd, Some(RespCommand::Lpush));
  }

  #[test]
  fn subcommand_dispatch_and_unknown() {
    let mut s = RespServerSession::default();
    let (cmd, args) = parse_one(&mut s, b"*2\r\n$6\r\nclient\r\n$2\r\nid\r\n");
    assert_eq!(cmd, Some(RespCommand::ClientId));
    assert!(args.is_empty());

    let (cmd, _) = parse_one(&mut s, b"*2\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n");
    assert_eq!(cmd, Some(RespCommand::ConfigGet));

    // 未知子命令 → Invalid + 无提示版文案（C# 文案自带句号）
    let (cmd, _) = parse_one(&mut s, b"*2\r\n$6\r\nCLIENT\r\n$4\r\nNOPE\r\n");
    assert_eq!(cmd, Some(RespCommand::Invalid));
    let out = s.take_output();
    assert_eq!(out, b"-ERR unknown subcommand 'NOPE'.\r\n");

    // CLUSTER 未知子命令 → 带帮助提示文案
    let (cmd, _) = parse_one(&mut s, b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nNOPE\r\n");
    assert_eq!(cmd, Some(RespCommand::Invalid));
    let out = s.take_output();
    assert_eq!(out, b"-ERR unknown subcommand 'NOPE'. Try CLUSTER HELP\r\n");

    // 父命令无参（非 COMMAND）→ wrong number of arguments（父名大写对齐 C# 枚举名）
    let (cmd, _) = parse_one(&mut s, b"*1\r\n$6\r\nCLIENT\r\n");
    assert_eq!(cmd, Some(RespCommand::Invalid));
    let out = s.take_output();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'CLIENT' command\r\n"
    );

    // 完全未知命令 → Invalid + unknown command
    let (cmd, _) = parse_one(&mut s, b"*1\r\n$4\r\nnope\r\n");
    assert_eq!(cmd, Some(RespCommand::Invalid));
    let out = s.take_output();
    assert_eq!(out, b"-ERR unknown command\r\n");
  }

  /// 主表 + 子命令表全量端到端 round-trip:每一条表项都构造真实 RESP 帧
  /// 解析,命中期望命令(二分可达性 + 名称/枚举映射的全量回归,不止抽样)
  #[test]
  fn every_table_entry_round_trips_via_parse() {
    let mut s = RespServerSession::default();

    // 主表:非父命令 `*2 NAME k` → 命令本身(1 个参数)
    for (name, cmd, has_subcommands) in PRIMARY_TABLE {
      if *has_subcommands {
        continue;
      }
      let frame = format!("*2\r\n${}\r\n{name}\r\n$1\r\nk\r\n", name.len());
      let (parsed, _) = parse_one(&mut s, frame.as_bytes());
      assert_eq!(parsed, Some(*cmd), "主表 {name} 解析不可达");
    }

    // 父命令 + 子命令:`*2 PARENT SUB` → 子命令
    let subtables: [ParentSubtable; 13] = [
      ("CLIENT", RespCommand::Client, CLIENT_SUBTABLE),
      ("CONFIG", RespCommand::Config, CONFIG_SUBTABLE),
      ("COMMAND", RespCommand::Command, COMMAND_SUBTABLE),
      ("ACL", RespCommand::Acl, ACL_SUBTABLE),
      ("SCRIPT", RespCommand::Script, SCRIPT_SUBTABLE),
      ("PUBSUB", RespCommand::Pubsub, PUBSUB_SUBTABLE),
      ("LATENCY", RespCommand::Latency, LATENCY_SUBTABLE),
      ("SLOWLOG", RespCommand::Slowlog, SLOWLOG_SUBTABLE),
      ("MODULE", RespCommand::Module, MODULE_SUBTABLE),
      ("MEMORY", RespCommand::Memory, MEMORY_SUBTABLE),
      ("OBJECT", RespCommand::Object, OBJECT_SUBTABLE),
      ("CLUSTER", RespCommand::Cluster, CLUSTER_SUBTABLE),
      ("BITOP", RespCommand::Bitop, BITOP_SUBTABLE),
    ];
    for (parent_name, parent_cmd, table) in subtables {
      // 父命令项须在主表且带子命令标记
      let entry = lookup_primary(parent_name.as_bytes());
      assert_eq!(
        entry,
        Some((parent_cmd, true)),
        "主表缺父命令 {parent_name}"
      );
      for (sub_name, sub_cmd) in table {
        let frame = format!(
          "*2\r\n${}\r\n{parent_name}\r\n${}\r\n{sub_name}\r\n",
          parent_name.len(),
          sub_name.len()
        );
        let (parsed, _) = parse_one(&mut s, frame.as_bytes());
        assert_eq!(
          parsed,
          Some(*sub_cmd),
          "{parent_name} {sub_name} 解析不可达"
        );
      }
    }
  }

  /// 主表二分检索回归：HELLO/HDEL 曾乱序致 HDEL 漏查；父命令与子命令表
  /// 补齐 ACL/MODULE/BITOP/CLUSTER/MEMORY/OBJECT 后应全部分派成功

  #[test]
  fn primary_table_order_and_full_dispatch() {
    let mut s = RespServerSession::default();

    // 乱序回归：HDEL 位于 HELLO 之后曾不可达
    let (cmd, args) = parse_one(&mut s, b"*3\r\n$4\r\nHDEL\r\n$1\r\nk\r\n$1\r\nf\r\n");
    assert_eq!(cmd, Some(RespCommand::Hdel));
    assert_eq!(args, vec![b"k".to_vec(), b"f".to_vec()]);

    // 曾缺失的根命令（EXPIREAT / ZREVRANK / SUBSTR / LMOVE）
    for (frame, expect) in [
      (
        &b"*3\r\n$8\r\nEXPIREAT\r\n$1\r\nk\r\n$1\r\n1\r\n"[..],
        RespCommand::Expireat,
      ),
      (
        &b"*3\r\n$8\r\nZREVRANK\r\n$1\r\nk\r\n$1\r\nm\r\n"[..],
        RespCommand::Zrevrank,
      ),
      (
        &b"*2\r\n$6\r\nSUBSTR\r\n$1\r\nk\r\n"[..],
        RespCommand::Substr,
      ),
      (
        &b"*5\r\n$5\r\nLMOVE\r\n$1\r\na\r\n$1\r\nb\r\n$4\r\nLEFT\r\n$5\r\nRIGHT\r\n"[..],
        RespCommand::Lmove,
      ),
    ] {
      let (cmd, _) = parse_one(&mut s, frame);
      assert_eq!(cmd, Some(expect), "{frame:?}");
    }

    // 父命令分派补齐：ACL / MODULE / BITOP / OBJECT / MEMORY / CLUSTER
    for (frame, expect) in [
      (
        &b"*2\r\n$3\r\nACL\r\n$3\r\nCAT\r\n"[..],
        RespCommand::AclCat,
      ),
      (
        &b"*2\r\n$6\r\nMODULE\r\n$6\r\nLOADCS\r\n"[..],
        RespCommand::ModuleLoadcs,
      ),
      (
        &b"*2\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$1\r\nk\r\n"[..],
        RespCommand::MemoryUsage,
      ),
      (
        &b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$1\r\nk\r\n"[..],
        RespCommand::ObjectEncoding,
      ),
      (
        &b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n"[..],
        RespCommand::ClusterMyid,
      ),
      (
        &b"*3\r\n$7\r\nCLUSTER\r\n$16\r\nSET-CONFIG-EPOCH\r\n$1\r\n0\r\n"[..],
        RespCommand::ClusterSetconfigepoch,
      ),
    ] {
      let (cmd, _) = parse_one(&mut s, frame);
      assert_eq!(cmd, Some(expect), "{frame:?}");
    }

    // BITOP NOT（连字段子命令 + 负载参数）
    let (cmd, args) = parse_one(
      &mut s,
      b"*4\r\n$5\r\nBITOP\r\n$3\r\nNOT\r\n$3\r\ndst\r\n$3\r\nsrc\r\n",
    );
    assert_eq!(cmd, Some(RespCommand::BitopNot));
    assert_eq!(args, vec![b"dst".to_vec(), b"src".to_vec()]);
  }

  #[test]
  fn incomplete_command_returns_none() {
    let mut s = RespServerSession::default();
    // 半截命令
    let (cmd, _) = parse_one(&mut s, b"*2\r\n$3\r\nSET\r\n$1\r\nk");
    assert_eq!(cmd, None);

    // 数组头不完整
    let (cmd, _) = parse_one(&mut s, b"*2\r");
    assert_eq!(cmd, None);
  }

  #[test]
  fn parse_resp_command_buffer_restores_state() {
    let mut s = RespServerSession::default();
    let read_head_before = s.read_head;
    let cmd = s.parse_resp_command_buffer(b"*2\r\n$6\r\nclient\r\n$4\r\ninfo\r\n");
    assert_eq!(cmd, Some(RespCommand::ClientInfo));
    assert_eq!(s.read_head, read_head_before);
    // 模糊入口容忍半截
    let (ok, cmd) = s.fuzz_parse_command_buffer(b"*1\r\n$3\r\nGE");
    assert!(!ok);
    assert_eq!(cmd, RespCommand::Invalid);
    let (ok, cmd) = s.fuzz_parse_command_buffer(b"*1\r\n$3\r\nGET\r\n");
    assert!(ok);
    assert_eq!(cmd, RespCommand::Get);
  }

  #[test]
  fn aof_commit_mode_marks_dependent_commands() {
    let mut s = RespServerSession::default();
    s.handle_aof_commit_mode(RespCommand::Set);
    assert!(s.wait_for_aof_blocking);
    // C#：无待发数据时每条命令先重置标记，PING（AOF 无关）不重新置位
    s.handle_aof_commit_mode(RespCommand::Ping);
    assert!(!s.wait_for_aof_blocking);
    // 有待发数据（dcurr > head）不重置：SET 置位保持到 PING 之后
    s.handle_aof_commit_mode(RespCommand::Set);
    s.write_direct_large(b"+OK\r\n");
    s.handle_aof_commit_mode(RespCommand::Ping);
    assert!(s.wait_for_aof_blocking);
    // 缓冲清空 → 重置
    s.output.clear();
    s.handle_aof_commit_mode(RespCommand::Ping);
    assert!(!s.wait_for_aof_blocking);
    // 事务 Started 态不改标记
    s.txn_state = TxnState::Started;
    s.handle_aof_commit_mode(RespCommand::Set);
    assert!(!s.wait_for_aof_blocking);
  }

  #[test]
  fn inline_malformed_skips_line() {
    let mut s = RespServerSession::default();
    s.recv_buffer
      .extend_from_slice(b"garbage\r\n*1\r\n$4\r\nPING\r\n");
    s.bytes_read = s.recv_buffer.len();
    s.read_head = 0;
    let consumed = s.try_consume_messages(b"garbage\r\n*1\r\n$4\r\nPING\r\n");
    // 畸形行被跳过后仍解析到 PING
    assert!(consumed.is_some());
  }

  #[test]
  fn is_aof_independent_matches_csharp_set() {
    // 逐项对标 RespCommand.cs:AofIndependentCommands 的 47 项全集
    let csharp_set: &[RespCommand] = &[
      RespCommand::Async,
      RespCommand::Ping,
      RespCommand::Select,
      RespCommand::Swapdb,
      RespCommand::Echo,
      RespCommand::Monitor,
      RespCommand::ModuleLoadcs,
      RespCommand::Registercs,
      RespCommand::Info,
      RespCommand::Time,
      RespCommand::Lastsave,
      RespCommand::AclCat,
      RespCommand::AclDeluser,
      RespCommand::AclGenpass,
      RespCommand::AclGetuser,
      RespCommand::AclList,
      RespCommand::AclLoad,
      RespCommand::AclSave,
      RespCommand::AclSetuser,
      RespCommand::AclUsers,
      RespCommand::AclWhoami,
      RespCommand::ClientId,
      RespCommand::ClientInfo,
      RespCommand::ClientList,
      RespCommand::ClientKill,
      RespCommand::ClientGetname,
      RespCommand::ClientSetname,
      RespCommand::ClientSetinfo,
      RespCommand::ClientUnblock,
      RespCommand::Command,
      RespCommand::CommandCount,
      RespCommand::CommandDocs,
      RespCommand::CommandInfo,
      RespCommand::CommandGetkeys,
      RespCommand::CommandGetkeysandflags,
      RespCommand::MemoryUsage,
      RespCommand::ConfigGet,
      RespCommand::ConfigRewrite,
      RespCommand::ConfigSet,
      RespCommand::LatencyHelp,
      RespCommand::LatencyHistogram,
      RespCommand::LatencyReset,
      RespCommand::SlowlogHelp,
      RespCommand::SlowlogLen,
      RespCommand::SlowlogGet,
      RespCommand::SlowlogReset,
      RespCommand::Multi,
    ];
    // 集合相等（不多、不少、不重复）
    assert_eq!(AOF_INDEPENDENT_COMMANDS.len(), csharp_set.len());
    for cmd in csharp_set {
      assert!(is_aof_independent(*cmd), "{cmd:?} 应为 AOF 无关");
    }
    for cmd in AOF_INDEPENDENT_COMMANDS {
      assert!(csharp_set.contains(cmd), "{cmd:?} 不在 C# 集内");
    }
    // AOF 相关命令不置独立位
    assert!(!is_aof_independent(RespCommand::Set));
    assert!(!is_aof_independent(RespCommand::Get));
    assert!(!is_aof_independent(RespCommand::Invalid));
    assert!(!is_aof_independent(RespCommand::None));
    assert!(!is_aof_independent(RespCommand::Latency));
    assert!(!is_aof_independent(RespCommand::Slowlog));
  }

  /// 二分可达性回归:所有有序表必须严格 ASCII 升序
  ///(主表曾发生 HELLO/HDEL 乱序致 HDEL 漏查)
  #[test]
  fn ordered_tables_strictly_ascending() {
    for pair in PRIMARY_TABLE.windows(2) {
      assert!(
        pair[0].0.as_bytes() < pair[1].0.as_bytes(),
        "PRIMARY_TABLE 乱序: {} >= {}",
        pair[0].0,
        pair[1].0
      );
    }
    for table in [
      CLIENT_SUBTABLE,
      CONFIG_SUBTABLE,
      COMMAND_SUBTABLE,
      ACL_SUBTABLE,
      SCRIPT_SUBTABLE,
      PUBSUB_SUBTABLE,
      LATENCY_SUBTABLE,
      SLOWLOG_SUBTABLE,
      MODULE_SUBTABLE,
      MEMORY_SUBTABLE,
      OBJECT_SUBTABLE,
      CLUSTER_SUBTABLE,
      BITOP_SUBTABLE,
    ] {
      for pair in table.windows(2) {
        assert!(
          pair[0].0.as_bytes() < pair[1].0.as_bytes(),
          "子命令表乱序: {} >= {}",
          pair[0].0,
          pair[1].0
        );
      }
    }
  }

  /// 每个 has_subcommands 父命令都必须注册子表分派且子表非空;
  /// 带子表标记的父命令恰为 C# PopulatePrimaryTable 的 13 个
  #[test]
  fn parent_commands_have_subtable_dispatch() {
    let parents: Vec<(&str, RespCommand)> = PRIMARY_TABLE
      .iter()
      .filter(|(.., has_sub)| *has_sub)
      .map(|(name, cmd, _)| (*name, *cmd))
      .collect();

    // C# PopulatePrimaryTable 中 hasSub: true 的 13 个父命令
    let expected: [&str; 13] = [
      "ACL", "BITOP", "CLIENT", "CLUSTER", "COMMAND", "CONFIG", "LATENCY", "MEMORY", "MODULE",
      "OBJECT", "PUBSUB", "SCRIPT", "SLOWLOG",
    ];
    let mut names: Vec<&str> = parents.iter().map(|(name, _)| *name).collect();
    names.sort_unstable();
    assert_eq!(names, expected);

    for (name, cmd) in parents {
      let table: &[(&str, RespCommand)] = match cmd {
        RespCommand::Client => CLIENT_SUBTABLE,
        RespCommand::Config => CONFIG_SUBTABLE,
        RespCommand::Command => COMMAND_SUBTABLE,
        RespCommand::Acl => ACL_SUBTABLE,
        RespCommand::Script => SCRIPT_SUBTABLE,
        RespCommand::Pubsub => PUBSUB_SUBTABLE,
        RespCommand::Latency => LATENCY_SUBTABLE,
        RespCommand::Slowlog => SLOWLOG_SUBTABLE,
        RespCommand::Module => MODULE_SUBTABLE,
        RespCommand::Memory => MEMORY_SUBTABLE,
        RespCommand::Object => OBJECT_SUBTABLE,
        RespCommand::Cluster => CLUSTER_SUBTABLE,
        RespCommand::Bitop => BITOP_SUBTABLE,
        _ => &[],
      };
      assert!(!table.is_empty(), "父命令 {name} 缺子表分派");
      // 首个与末个子命令均可命中（二分端点可达）
      assert_eq!(
        lookup_subcommand(cmd, table[0].0.as_bytes()),
        Some(table[0].1)
      );
      let last = table[table.len() - 1];
      assert_eq!(lookup_subcommand(cmd, last.0.as_bytes()), Some(last.1));
    }
  }
}
