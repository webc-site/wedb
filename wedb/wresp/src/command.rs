//! libs/server/Resp/RespCommand.cs:RespCommand

#[repr(u16)]
#[derive(
  Debug,
  Clone,
  Copy,
  PartialEq,
  Eq,
  Hash,
  num_enum::TryFromPrimitive,
  num_enum::IntoPrimitive,
  strum::EnumString,
  strum::Display,
  strum::IntoStaticStr,
  strum::AsRefStr,
)]
#[strum(ascii_case_insensitive, serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum RespCommand {
  None = 0,
  Append = 1,
  Bitfield = 2,
  Bzmpop = 3,
  Bzpopmax = 4,
  Bzpopmin = 5,
  Decr = 6,
  Decrby = 7,
  Del = 8,
  Delifexpim = 9,
  Delifgreater = 10,
  Expire = 11,
  Expireat = 12,
  Flushall = 13,
  Flushdb = 14,
  Geoadd = 15,
  Georadius = 16,
  Georadiusbymember = 17,
  Geosearchstore = 18,
  Getdel = 19,
  Getex = 20,
  Getset = 21,
  Hcollect = 22,
  Hdel = 23,
  Hexpire = 24,
  Hpexpire = 25,
  Hexpireat = 26,
  Hpexpireat = 27,
  Hpersist = 28,
  Hincrby = 29,
  Hincrbyfloat = 30,
  Hmset = 31,
  Hset = 32,
  Hsetnx = 33,
  Incr = 34,
  Incrby = 35,
  Incrbyfloat = 36,
  Linsert = 37,
  Lmove = 38,
  Lmpop = 39,
  Lpop = 40,
  Lpush = 41,
  Lpushx = 42,
  Lrem = 43,
  Lset = 44,
  Ltrim = 45,
  Blpop = 46,
  Brpop = 47,
  Blmove = 48,
  Brpoplpush = 49,
  Blmpop = 50,
  Migrate = 51,
  Mset = 52,
  Msetnx = 53,
  Persist = 54,
  Pexpire = 55,
  Pexpireat = 56,
  Pfadd = 57,
  Pfmerge = 58,
  Psetex = 59,
  Rename = 60,
  Ricreate = 61,
  Ridel = 62,
  Ripromote = 63,
  Rirestore = 64,
  Riset = 65,
  Restore = 66,
  Renamenx = 67,
  Rpop = 68,
  Rpoplpush = 69,
  Rpush = 70,
  Rpushx = 71,
  Sadd = 72,
  Sdiffstore = 73,
  Set = 74,
  Setbit = 75,
  Setex = 76,
  Setexnx = 77,
  Setexxx = 78,
  Setnx = 79,
  Setifmatch = 80,
  Setifgreater = 81,
  Setwithetag = 82,
  Setkeepttl = 83,
  Setkeepttlxx = 84,
  Setrange = 85,
  Sinterstore = 86,
  Smove = 87,
  Spop = 88,
  Srem = 89,
  Sunionstore = 90,
  Swapdb = 91,
  Unlink = 92,
  Vadd = 93,
  Vrem = 94,
  Vsetattr = 95,
  Zadd = 96,
  Zcollect = 97,
  Zdiffstore = 98,
  Zexpire = 99,
  Zpexpire = 100,
  Zexpireat = 101,
  Zpexpireat = 102,
  Zpersist = 103,
  Zincrby = 104,
  Zmpop = 105,
  Zinterstore = 106,
  Zpopmax = 107,
  Zpopmin = 108,
  Zrangestore = 109,
  Zrem = 110,
  Zremrangebylex = 111,
  Zremrangebyrank = 112,
  Zremrangebyscore = 113,
  Zunionstore = 114,
  Bitop = 115,
  BitopAnd = 116,
  BitopOr = 117,
  BitopXor = 118,
  BitopNot = 119,
  BitopDiff = 120,
  Bitcount = 121,
  BitfieldRo = 122,
  Bitpos = 123,
  Coscan = 124,
  Dbsize = 125,
  Dump = 126,
  Exists = 127,
  Expiretime = 128,
  Geodist = 129,
  Geohash = 130,
  Geopos = 131,
  GeoradiusRo = 132,
  GeoradiusbymemberRo = 133,
  Geosearch = 134,
  Get = 135,
  Getbit = 136,
  Getifnotmatch = 137,
  Getrange = 138,
  Getwithetag = 139,
  Hexists = 140,
  Hget = 141,
  Hgetall = 142,
  Hkeys = 143,
  Hlen = 144,
  Hmget = 145,
  Hrandfield = 146,
  Hscan = 147,
  Hstrlen = 148,
  Hvals = 149,
  Keys = 150,
  Lcs = 151,
  Httl = 152,
  Hpttl = 153,
  Hexpiretime = 154,
  Hpexpiretime = 155,
  Lindex = 156,
  Llen = 157,
  Lpos = 158,
  Lrange = 159,
  MemoryUsage = 160,
  Mget = 161,
  ObjectEncoding = 162,
  ObjectFreq = 163,
  ObjectIdletime = 164,
  ObjectRefcount = 165,
  Pexpiretime = 166,
  Pfcount = 167,
  Pttl = 168,
  Scan = 169,
  Scard = 170,
  Sdiff = 171,
  Sinter = 172,
  Sintercard = 173,
  Sismember = 174,
  Smembers = 175,
  Smismember = 176,
  Spublish = 177,
  Srandmember = 178,
  Sscan = 179,
  Ssubscribe = 180,
  Strlen = 181,
  Substr = 182,
  Sunion = 183,
  Ttl = 184,
  Type = 185,
  Vcard = 186,
  Vdim = 187,
  Vemb = 188,
  Vgetattr = 189,
  Vinfo = 190,
  Vismember = 191,
  Vlinks = 192,
  Vrandmember = 193,
  Vsim = 194,
  Watch = 195,
  Watchms = 196,
  Watchos = 197,
  Zcard = 198,
  Zcount = 199,
  Zdiff = 200,
  Zinter = 201,
  Zintercard = 202,
  Zlexcount = 203,
  Zmscore = 204,
  Zrandmember = 205,
  Zrange = 206,
  Zrangebylex = 207,
  Zrangebyscore = 208,
  Zrank = 209,
  Zrevrange = 210,
  Zrevrangebylex = 211,
  Zrevrangebyscore = 212,
  Zrevrank = 213,
  Zttl = 214,
  Zpttl = 215,
  Zexpiretime = 216,
  Zpexpiretime = 217,
  Zscan = 218,
  Zscore = 219,
  Zunion = 220,
  Riconfig = 221,
  Riexists = 222,
  Riget = 223,
  Rimetrics = 224,
  Rirange = 225,
  Riscan = 226,
  Eval = 227,
  Evalsha = 228,
  Async = 229,
  Ping = 230,
  Pubsub = 231,
  PubsubChannels = 232,
  PubsubNumpat = 233,
  PubsubNumsub = 234,
  Publish = 235,
  Subscribe = 236,
  Psubscribe = 237,
  Unsubscribe = 238,
  Punsubscribe = 239,
  Asking = 240,
  Select = 241,
  Echo = 242,
  Client = 243,
  ClientId = 244,
  ClientInfo = 245,
  ClientList = 246,
  ClientKill = 247,
  ClientGetname = 248,
  ClientSetname = 249,
  ClientSetinfo = 250,
  ClientUnblock = 251,
  Monitor = 252,
  Multi = 256,
  Exec = 257,
  Discard = 258,
  Unwatch = 259,
  Runtxp = 260,
  Readonly = 261,
  Readwrite = 262,
  Replicaof = 263,
  Secondaryof = 264,
  Info = 265,
  Time = 266,
  Role = 267,
  Save = 268,
  Expdelscan = 269,
  Lastsave = 270,
  Bgsave = 271,
  Commitaof = 272,
  Failover = 273,
  Customtxn = 274,
  Customrawstringcmd = 275,
  Customobjcmd = 276,
  Customprocedure = 277,
  Script = 278,
  ScriptExists = 279,
  ScriptFlush = 280,
  ScriptLoad = 281,
  Acl = 282,
  AclCat = 283,
  AclDeluser = 284,
  AclGenpass = 285,
  AclGetuser = 286,
  AclList = 287,
  AclLoad = 288,
  AclSave = 289,
  AclSetuser = 290,
  AclUsers = 291,
  AclWhoami = 292,
  Command = 293,
  CommandCount = 294,
  CommandDocs = 295,
  CommandInfo = 296,
  CommandGetkeys = 297,
  CommandGetkeysandflags = 298,
  Memory = 299,
  Object = 300,
  ObjectHelp = 301,
  Config = 302,
  ConfigGet = 303,
  ConfigRewrite = 304,
  ConfigSet = 305,
  Debug = 306,
  Latency = 307,
  LatencyHelp = 308,
  LatencyHistogram = 309,
  LatencyReset = 310,
  Slowlog = 311,
  SlowlogHelp = 312,
  SlowlogLen = 313,
  SlowlogGet = 314,
  SlowlogReset = 315,
  Cluster = 316,
  ClusterAddslots = 317,
  ClusterAddslotsrange = 318,
  ClusterAdvanceTime = 319,
  ClusterAppendlog = 320,
  ClusterAttachSync = 321,
  ClusterBanlist = 322,
  ClusterBeginReplicaRecover = 323,
  ClusterBumpepoch = 324,
  ClusterCountkeysinslot = 325,
  ClusterDelkeysinslot = 326,
  ClusterDelkeysinslotrange = 327,
  ClusterDelslots = 328,
  ClusterDelslotsrange = 329,
  ClusterEndpoint = 330,
  ClusterFailover = 331,
  ClusterFailreplicationoffset = 332,
  ClusterFailstopwrites = 333,
  ClusterFlushall = 334,
  ClusterForget = 335,
  ClusterGetkeysinslot = 336,
  ClusterGossip = 337,
  ClusterHelp = 338,
  ClusterInfo = 339,
  ClusterInitiateReplicaSync = 340,
  ClusterKeyslot = 341,
  ClusterMeet = 342,
  ClusterMigrate = 343,
  ClusterMlogKeyTime = 344,
  ClusterMtasks = 345,
  ClusterMyid = 346,
  ClusterMyparentid = 347,
  ClusterNodes = 348,
  ClusterPublish = 349,
  ClusterSpublish = 350,
  ClusterReplicas = 351,
  ClusterReplicate = 352,
  ClusterReserve = 353,
  ClusterReset = 354,
  ClusterSendCkptFileSegment = 355,
  ClusterSendCkptMetadata = 356,
  ClusterSetconfigepoch = 357,
  ClusterSetslot = 358,
  ClusterSetslotsrange = 359,
  ClusterShards = 360,
  ClusterSlots = 361,
  ClusterSlotstate = 362,
  ClusterSnapshotData = 363,
  ClusterSync = 364,
  Auth = 365,
  Hello = 366,
  Quit = 367,
  Invalid = 65535,
}

/// 数据命令区间下界（C# FirstDataCommand = FirstWriteCommand = APPEND）
const FIRST_DATA_COMMAND: RespCommand = RespCommand::Append;
/// 数据命令区间上界（C# LastDataCommand = EVALSHA）
const LAST_DATA_COMMAND: RespCommand = RespCommand::Evalsha;
/// 读命令区间下界（C# FirstReadCommand = LastWriteCommand + 1）
const FIRST_READ_COMMAND: RespCommand = RespCommand::Bitcount;
/// 读命令区间上界（C# LastReadCommand = EVAL - 1）
const LAST_READ_COMMAND: RespCommand = RespCommand::Riscan;

/// 最后一个有效命令（除 INVALID 外枚举最大值 = QUIT = 367）
///
/// libs/server/Resp/Parser/RespCommand.cs:RespCommandExtensions.LastValidCommand
/// （`Enum.GetValues<RespCommand>().Where(cmd => cmd != INVALID).Max()`）
pub const LAST_VALID_COMMAND: RespCommand = RespCommand::Quit;

/// 判定命令是否为只读命令（读区间双侧判定，无符号下溢天然出界）
///
/// libs/server/Resp/Parser/RespCommand.cs:IsReadOnly
#[inline]
pub const fn is_read_only(cmd: RespCommand) -> bool {
  let v = (cmd as u16).wrapping_sub(FIRST_READ_COMMAND as u16);
  v <= (LAST_READ_COMMAND as u16 - FIRST_READ_COMMAND as u16)
}

/// 判定命令是否为数据命令（写区间 + 读区间连续覆盖；C# 排除表逐一对标）
///
/// libs/server/Resp/Parser/RespCommand.cs:IsDataCommand
#[inline]
pub const fn is_data_command(cmd: RespCommand) -> bool {
  match cmd {
    // C# 排除表：MIGRATE / DBSIZE / MEMORY-USAGE / FLUSHDB / FLUSHALL / KEYS / SCAN / SWAPDB
    RespCommand::Migrate
    | RespCommand::Dbsize
    | RespCommand::MemoryUsage
    | RespCommand::Flushall
    | RespCommand::Flushdb
    | RespCommand::Keys
    | RespCommand::Scan
    | RespCommand::Swapdb => false,
    _ => {
      let v = (cmd as u16).wrapping_sub(FIRST_DATA_COMMAND as u16);
      v <= (LAST_DATA_COMMAND as u16 - FIRST_DATA_COMMAND as u16)
    }
  }
}

/// 判定命令是否为 CLUSTER 子命令（CLUSTER_ADDSLOTS..=CLUSTER_SYNC 连续区间）
///
/// libs/server/Resp/Parser/RespCommand.cs:IsClusterSubCommand
#[inline]
pub const fn is_cluster_sub_command(cmd: RespCommand) -> bool {
  let v = (cmd as u16).wrapping_sub(RespCommand::ClusterAddslots as u16);
  v <= (RespCommand::ClusterSync as u16 - RespCommand::ClusterAddslots as u16)
}

/// 判定命令是否为纯写命令
///
/// libs/server/Resp/Parser/RespCommand.cs:IsWriteOnly
#[inline]
pub const fn is_write_only(cmd: RespCommand) -> bool {
  let v = (cmd as u16).wrapping_sub(FIRST_DATA_COMMAND as u16);
  v <= (RespCommand::BitopDiff as u16 - FIRST_DATA_COMMAND as u16)
}

/// 如果是写命令返回 1，否则返回 0
///
/// libs/server/Resp/Parser/RespCommand.cs:OneIfWrite
#[inline]
pub const fn one_if_write(cmd: RespCommand) -> u64 {
  if is_write_only(cmd) { 1 } else { 0 }
}

/// 如果是读命令返回 1，否则返回 0
///
/// libs/server/Resp/Parser/RespCommand.cs:OneIfRead
#[inline]
pub const fn one_if_read(cmd: RespCommand) -> u64 {
  if is_read_only(cmd) { 1 } else { 0 }
}

/// 判定命令是否为 RangeIndex 专用命令
///
/// libs/server/Resp/Parser/RespCommand.cs:IsRangeIndexCommand
#[inline]
pub const fn is_range_index_command(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    RespCommand::Ricreate
      | RespCommand::Riset
      | RespCommand::Riget
      | RespCommand::Ridel
      | RespCommand::Riscan
      | RespCommand::Rirange
      | RespCommand::Ripromote
      | RespCommand::Rirestore
      | RespCommand::Riexists
      | RespCommand::Riconfig
      | RespCommand::Rimetrics
  )
}

/// 判定命令是否可在 RangeIndex 键上合法操作
///
/// libs/server/Resp/Parser/RespCommand.cs:IsLegalOnRangeIndex
#[inline]
pub const fn is_legal_on_range_index(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    RespCommand::Del
      | RespCommand::Unlink
      | RespCommand::Type
      | RespCommand::Debug
      | RespCommand::Rename
      | RespCommand::Renamenx
  ) || is_range_index_command(cmd)
}

/// 判定命令是否为 VectorSet 专用命令
///
/// libs/server/Resp/Parser/RespCommand.cs:IsVectorSetCommand
#[inline]
pub const fn is_vector_set_command(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    RespCommand::Vadd
      | RespCommand::Vcard
      | RespCommand::Vdim
      | RespCommand::Vemb
      | RespCommand::Vgetattr
      | RespCommand::Vinfo
      | RespCommand::Vismember
      | RespCommand::Vlinks
      | RespCommand::Vrandmember
      | RespCommand::Vrem
      | RespCommand::Vsetattr
      | RespCommand::Vsim
  )
}

/// 判定命令是否可在 VectorSet 键上合法操作
///
/// libs/server/Resp/Parser/RespCommand.cs:IsLegalOnVectorSet
#[inline]
pub const fn is_legal_on_vector_set(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    RespCommand::Del
      | RespCommand::Unlink
      | RespCommand::Type
      | RespCommand::Debug
      | RespCommand::Rename
      | RespCommand::Renamenx
  ) || is_vector_set_command(cmd)
}

#[cfg(test)]
mod tests {
  use std::str::FromStr;

  use super::*;

  #[test]
  fn test_resp_command_strum_roundtrip() {
    assert_eq!(RespCommand::Append.as_ref(), "APPEND");
    assert_eq!(RespCommand::BitopAnd.as_ref(), "BITOP_AND");
    assert_eq!(RespCommand::AclCat.as_ref(), "ACL_CAT");
    assert_eq!(
      RespCommand::ClusterSendCkptFileSegment.as_ref(),
      "CLUSTER_SEND_CKPT_FILE_SEGMENT"
    );

    assert_eq!(RespCommand::from_str("APPEND"), Ok(RespCommand::Append));
    assert_eq!(RespCommand::from_str("append"), Ok(RespCommand::Append));
    assert_eq!(RespCommand::from_str("ApPeNd"), Ok(RespCommand::Append));
    assert_eq!(RespCommand::from_str("acl_cat"), Ok(RespCommand::AclCat));
    assert_eq!(
      RespCommand::from_str("bitop_and"),
      Ok(RespCommand::BitopAnd)
    );
    assert_eq!(
      RespCommand::from_str("cluster_send_ckpt_file_segment"),
      Ok(RespCommand::ClusterSendCkptFileSegment)
    );
    assert!(RespCommand::from_str("NON_EXISTENT_COMMAND").is_err());

    let static_str: &'static str = RespCommand::BitopAnd.into();
    assert_eq!(static_str, "BITOP_AND");
    assert_eq!(RespCommand::BitopAnd.to_string(), "BITOP_AND");
  }
}
