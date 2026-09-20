//! libs/server/Resp/Parser/RespCommand.cs:RespCommand

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
  /// TTL 过期物理清除的内部 RMW 条目（对标 C# 统一存储 RMW 分派里的 DELIFEXPIM，
  /// 见「libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs」的 ExpireAndStop 臂），
  /// 只由 AOF 记录与重放链路投递（wnode service 的 TTL 清除、aof_processor 的重放判定）。
  /// 对外协议不接线是与 C# 一致的正确设计（C# 侧 RESP 解析与会话层同样无该命令的分派臂），
  /// 勿补 parser 条目、勿当僵尸命令删除
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
  // 编号 63/64（C# RIPROMOTE/RIRESTORE）空缺：MainStore 内部 RMW 僵尸命令，
  // rust 不设枚举成员：存根提升/回写语义由 wkv 元记录 RMW 等价承接
  // （wkv/src/range_index.rs 的 acquire_tree_read 惰性恢复链内
  // promote_range_index_to_tail 对标 RIPROMOTE、restore_range_index_stub 对标 RIRESTORE）
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
  /// 仅与 C# 枚举 1:1 占位（BasicCommands.cs:NetworkSET_Conditional 派发目标），
  /// 实际语义由 wnode 的 SetCmd 枚举承接，全仓无分发消费
  Setexxx = 78,
  Setnx = 79,
  Setifmatch = 80,
  Setifgreater = 81,
  Setwithetag = 82,
  /// 仅与 C# 枚举 1:1 占位（BasicCommands.cs:NetworkSET_Conditional 派发目标），
  /// 实际语义由 wnode 的 SetCmd 枚举承接，全仓无分发消费
  Setkeepttl = 83,
  /// 仅与 C# 枚举 1:1 占位（BasicCommands.cs:NetworkSET_Conditional 派发目标），
  /// 实际语义由 wnode 的 SetCmd 枚举承接，全仓无分发消费
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
  Ricount = 222,
  Riexists = 223,
  Riget = 224,
  Rimetrics = 225,
  Rirange = 226,
  Riscan = 227,
  Eval = 228,
  Evalsha = 229,
  Async = 230,
  Ping = 231,
  Pubsub = 232,
  PubsubChannels = 233,
  PubsubNumpat = 234,
  PubsubNumsub = 235,
  Publish = 236,
  Subscribe = 237,
  Psubscribe = 238,
  Unsubscribe = 239,
  Punsubscribe = 240,
  Asking = 241,
  Select = 242,
  Echo = 243,
  Client = 244,
  ClientId = 245,
  ClientInfo = 246,
  ClientList = 247,
  ClientKill = 248,
  ClientGetname = 249,
  ClientSetname = 250,
  ClientSetinfo = 251,
  ClientUnblock = 252,
  Monitor = 253,
  Multi = 257,
  Exec = 258,
  Discard = 259,
  Unwatch = 260,
  Runtxp = 261,
  Readonly = 262,
  Readwrite = 263,
  Replicaof = 264,
  Secondaryof = 265,
  Info = 266,
  Time = 267,
  Role = 268,
  Save = 269,
  Expdelscan = 270,
  Lastsave = 271,
  Bgsave = 272,
  Commitaof = 273,
  Failover = 274,
  /// 自定义对象命令：扩展命令（JSON / Roaring）编译期静态清单命中后由解析器统一回填
  /// 的哨兵值，经 network_custom_obj_cmd 存储执行域承接。
  ///
  /// 编号 275/276/278（C# CustomTxn / CustomRawStringCmd / CustomProcedure）为动态注册
  /// 层（模块 / REGISTERCS）解析期按运行时 id 回填的内部分派哨兵，随注册管理层按转写
  /// 规范整删、留为登记洞位（见 resp_command_values.rs 已登记差分）：事务过程入口单点
  /// 收敛到 RUNTXP，其余命令无静态对应与真实用户路径，不设枚举成员、勿当僵尸补回
  Customobjcmd = 277,
  Script = 279,
  ScriptExists = 280,
  ScriptFlush = 281,
  ScriptLoad = 282,
  Acl = 283,
  AclCat = 284,
  AclDeluser = 285,
  AclGenpass = 286,
  AclGetuser = 287,
  AclList = 288,
  AclLoad = 289,
  AclSave = 290,
  AclSetuser = 291,
  AclUsers = 292,
  AclWhoami = 293,
  Command = 294,
  CommandCount = 295,
  CommandDocs = 296,
  CommandInfo = 297,
  CommandGetkeys = 298,
  CommandGetkeysandflags = 299,
  Memory = 300,
  Object = 301,
  ObjectHelp = 302,
  Config = 303,
  ConfigGet = 304,
  ConfigRewrite = 305,
  ConfigSet = 306,
  Debug = 307,
  Latency = 308,
  LatencyHelp = 309,
  LatencyHistogram = 310,
  LatencyReset = 311,
  Slowlog = 312,
  SlowlogHelp = 313,
  SlowlogLen = 314,
  SlowlogGet = 315,
  SlowlogReset = 316,
  Cluster = 317,
  ClusterAddslots = 318,
  ClusterAddslotsrange = 319,
  ClusterAdvanceTime = 320,
  ClusterAppendlog = 321,
  ClusterAttachSync = 322,
  ClusterBanlist = 323,
  ClusterBeginReplicaRecover = 324,
  ClusterBumpepoch = 325,
  ClusterCountkeysinslot = 326,
  ClusterDelkeysinslot = 327,
  ClusterDelkeysinslotrange = 328,
  ClusterDelslots = 329,
  ClusterDelslotsrange = 330,
  ClusterEndpoint = 331,
  ClusterFailover = 332,
  ClusterFailreplicationoffset = 333,
  ClusterFailstopwrites = 334,
  ClusterFlushall = 335,
  ClusterFlushallNs = 336,
  ClusterForget = 337,
  ClusterGetkeysinslot = 338,
  ClusterGossip = 339,
  ClusterHelp = 340,
  ClusterInfo = 341,
  ClusterInitiateReplicaSync = 342,
  ClusterKeyslot = 343,
  ClusterMeet = 344,
  ClusterMigrate = 345,
  ClusterMlogKeyTime = 346,
  ClusterMtasks = 347,
  ClusterMyid = 348,
  ClusterMyparentid = 349,
  ClusterNodes = 350,
  ClusterPublish = 351,
  ClusterSpublish = 352,
  ClusterReplicas = 353,
  ClusterReplicate = 354,
  ClusterReserve = 355,
  ClusterReset = 356,
  ClusterSendCkptFileSegment = 357,
  ClusterSendCkptMetadata = 358,
  ClusterSetconfigepoch = 359,
  ClusterSetslot = 360,
  ClusterSetslotsrange = 361,
  ClusterShards = 362,
  ClusterSlots = 363,
  ClusterSlotstate = 364,
  ClusterSnapshotData = 365,
  ClusterSync = 366,
  Auth = 367,
  Hello = 368,
  Quit = 369,
  Sunsubscribe = 370,
  Invalid = 65535,
}

/// 数据命令区间下界（C# FirstDataCommand = FirstWriteCommand = APPEND）
///
/// libs/server/Resp/Parser/RespCommand.cs:FirstDataCommand
pub const FIRST_DATA_COMMAND: RespCommand = RespCommand::Append;
/// 数据命令区间上界（C# LastDataCommand = EVALSHA）
///
/// libs/server/Resp/Parser/RespCommand.cs:LastDataCommand
pub const LAST_DATA_COMMAND: RespCommand = RespCommand::Evalsha;
/// 读命令区间下界（C# FirstReadCommand = LastWriteCommand + 1）
const FIRST_READ_COMMAND: RespCommand = RespCommand::Bitcount;
/// 读命令区间上界（C# LastReadCommand = EVAL - 1）
const LAST_READ_COMMAND: RespCommand = RespCommand::Riscan;

/// 最后一个有效命令（除 INVALID 外枚举最大值 = SUNSUBSCRIBE = 370）
///
/// C# 尾值为 QUIT = 369（libs/server/Resp/Parser/RespCommand.cs 枚举尾部三连
/// AUTH / HELLO / QUIT）；rust 在其后追加自增命令 SUNSUBSCRIBE（redis 7 分片
/// pubsub 的配套退订，C# 无对位，已按 RI.COUNT 惯例入命令目录），权限位图长度
/// 随之含位 370。
///
/// libs/server/Resp/Parser/RespCommand.cs:RespCommandExtensions.LastValidCommand
pub const LAST_VALID_COMMAND: RespCommand = RespCommand::Sunsubscribe;

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

/// 向量登记表值域门的「豁免命令」集（本档一处定义的适用集裁剪面）
///
/// 门判据仍唯一取自登记表命中（wnode 侧 read_stored_index 单点），本谓词只圈定
/// 「数据命令中哪些在登记表命中时不按值域 WRONGTYPE 处理」，与白名单
/// [`is_legal_on_vector_set`] 互补。C# 无此枚举（判据挂在记录 RecordType 上，
/// ReadMethods.cs:115 CheckRecordTypeMismatch / RMWMethods / UpsertMethods 三处同判），
/// 但 rust 向量索引驻留 VectorManager 进程内登记表、不落 wkv 值域，除命令层判据外无
/// 物理域判据可用（见 task/done/ri-predicate-gate.md 三节：向量侧不能照搬 RI 删位图结论），
/// 故适用集以命令枚举显式声明，穷举覆盖测试钉死，新增命令不落清单即测试红。
///
/// 豁免分类：
/// - SET 族覆写：C# NetworkSET 撞 WRONGTYPE 后 DELETE+SET 强制覆写
///   （BasicCommands.cs:405-415、ArrayCommands.cs:54-64），rust 由 set_vector_guard 预清退，
///   登记即销毁向量集、落 String 记录，非值域拒绝。
/// - NX/存在性：MSETNX 登记表命中即键存在回 :0 零写入（ArrayCommands.cs:76-90）、
///   RESTORE 命中即 BUSYKEY（C# KeyAdminCommands 的 NetworkRESTORE 逻辑），均非 WRONGTYPE。
/// - 记录存活/元数据读侧：EXISTS/TTL/EXPIRE 族/MEMORY USAGE/OBJECT 族/DUMP/MGET 对存活
///   向量记录按普通存活记录工作（UnifiedStore/ReadMethods.cs:31-47 reader switch），
///   由第四态探针承接（task/ing/vector-key-ttl-fourth-domain.md），本门不重复挂。
/// - RI 族与事务/pubsub/脚本：判据不同层（RI 物理域 / 无键值访问），不得顺手统一。
#[inline]
pub const fn is_vector_gate_exempt(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    // SET 族覆写清退（set_vector_guard 承接，登记表命中即销毁再覆写，非 WRONGTYPE）。
    // 不含 Getset：C# NetworkGETSET 转 NetworkSET GET 标志，走 NetworkSET_Conditional
    // getValue=true 臂（BasicCommands.cs:832-835）撞 WRONGTYPE 直接回错、无 DELETE
    // 重试，登记保留——与覆写族终态相反，归通用 args[0] 值域门拒。
    RespCommand::Set
      | RespCommand::Setex
      | RespCommand::Psetex
      | RespCommand::Setnx
      | RespCommand::Setexnx
      | RespCommand::Setexxx
      | RespCommand::Setkeepttl
      | RespCommand::Setkeepttlxx
      | RespCommand::Mset
      // BITOP 族：源键/目的键在位点内逐键裁决，派发层不整命令扫。C#
      // StringBitOperation 源键撞向量存根整体 WRONGTYPE 零写（对位现位点
      // 前置源探测），目的键走 DELETE+SET 重试臂（BitmapOps.cs，有源命中
      // 即 dest 销毁 + 写折叠值；全源缺失零写、登记保留）——一刀切拦目的
      // 键会令有源时分叉为错误拒绝，故豁免出派发门、由位点承接。
      | RespCommand::Bitop
      | RespCommand::BitopAnd
      | RespCommand::BitopOr
      | RespCommand::BitopXor
      | RespCommand::BitopNot
      | RespCommand::BitopDiff
      // NX/存在性特殊裁决
      | RespCommand::Msetnx
      | RespCommand::Restore
      // 记录存活/元数据读侧（第四态探针承接）
      | RespCommand::Exists
      | RespCommand::Expire
      | RespCommand::Pexpire
      | RespCommand::Expireat
      | RespCommand::Pexpireat
      | RespCommand::Persist
      | RespCommand::Ttl
      | RespCommand::Pttl
      | RespCommand::Expiretime
      | RespCommand::Pexpiretime
      | RespCommand::MemoryUsage
      | RespCommand::Dump
      | RespCommand::ObjectEncoding
      | RespCommand::ObjectFreq
      | RespCommand::ObjectIdletime
      | RespCommand::ObjectRefcount
      | RespCommand::Mget
      // RI 族 / 事务 / pubsub 控制 / 脚本：不吃向量值域判据
      | RespCommand::Ricreate
      | RespCommand::Riset
      | RespCommand::Riget
      | RespCommand::Ridel
      | RespCommand::Riscan
      | RespCommand::Rirange
      | RespCommand::Riexists
      | RespCommand::Riconfig
      | RespCommand::Ricount
      | RespCommand::Rimetrics
      | RespCommand::Watch
      | RespCommand::Watchms
      | RespCommand::Watchos
      | RespCommand::Spublish
      | RespCommand::Ssubscribe
      | RespCommand::Coscan
      | RespCommand::Eval
      | RespCommand::Evalsha
  )
}

/// 值域门须逐键裁决的多键命令（全部实参均为键，无成员/数值/方向 token 混入，
/// 直扫 args 无误判风险），对标 C# 对每个 touched key 的 RecordType 判定：
/// PFMERGE dest src…（args[0]=dest）、SDIFF/SINTER/SUNION key…、
/// SDIFFSTORE/SINTERSTORE/SUNIONSTORE dest key…。BITOP 族不在本清单：其目的键
/// 终态与覆写族相同（销毁再写）、与拦拒相反，由位点内源/目分流裁决
/// （见 [`is_vector_gate_exempt`] BITOP 注记）。其余多键命令（含 numkeys 变体、
/// 双键 src/dst、含成员的 ZADD/SMOVE 等）的 ghost 关键位（目的键 / 首键）恒在
/// args[0]，由通用 args[0] 门覆盖；非目的侧源键命中登记按集合读空处理，属只读边缘
/// （不产幽灵、不销毁向量），不在本门射程。
#[inline]
pub const fn vector_gate_scan_all_keys(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    RespCommand::Pfmerge
      | RespCommand::Sdiff
      | RespCommand::Sinter
      | RespCommand::Sunion
      | RespCommand::Sdiffstore
      | RespCommand::Sinterstore
      | RespCommand::Sunionstore
  )
}

impl RespCommand {
  /// 判定命令是否可在 VectorSet 键上合法操作
  #[inline]
  pub const fn is_legal_on_vector_set(self) -> bool {
    is_legal_on_vector_set(self)
  }

  /// 判定命令是否为 VectorSet 专用命令
  #[inline]
  pub const fn is_vector_set_command(self) -> bool {
    is_vector_set_command(self)
  }
}

#[cfg(test)]
mod tests {
  use std::str::FromStr;

  use super::*;

  /// 向量写门适用集穷举覆盖（task/ing/vector-registry-write-gate-coverage.md 六.1）：
  /// 遍历 RespCommand 全枚举，钉死「白名单」与「值域门豁免集」互斥，且新增命令不落
  /// 豁免清单即被 args[0] 值域门接管（数据命令默认拒），杜绝第二套事实漂移。
  #[test]
  fn vector_write_gate_applicability_is_exhaustive() {
    for raw in 0u16..=(LAST_VALID_COMMAND as u16) {
      let Ok(cmd) = RespCommand::try_from(raw) else {
        continue;
      };
      // 白名单（V*/DEL/TYPE/RENAME…）与豁免集不得交叠：同一命令既放行登记判据又走覆写
      // /存在性裁决 = 两套事实并存。
      assert!(
        !(is_legal_on_vector_set(cmd) && is_vector_gate_exempt(cmd)),
        "白名单与豁免集交叠: {cmd:?}"
      );
      // 多键逐键命令必须是会被值域门接管的数据命令（非白名单、非豁免），
      // 否则逐键扫描分支永不命中（悬空清单）。
      if vector_gate_scan_all_keys(cmd) {
        assert!(
          is_data_command(cmd) && !is_legal_on_vector_set(cmd) && !is_vector_gate_exempt(cmd),
          "多键清单命令未被值域门接管: {cmd:?}"
        );
      }
    }
    // SET 族覆写与 MGET/EXISTS/TTL 族登记感知读侧须豁免值域门（覆写/存活裁决另有承接）。
    for cmd in [
      RespCommand::Set,
      RespCommand::Mset,
      RespCommand::Msetnx,
      RespCommand::Restore,
      RespCommand::Exists,
      RespCommand::Mget,
      RespCommand::Ttl,
      RespCommand::MemoryUsage,
    ] {
      assert!(
        is_vector_gate_exempt(cmd),
        "{cmd:?} 应豁免值域 WRONGTYPE 门"
      );
    }
    // BITOP 族豁免派发门（源/目分流在位点内裁决，C# dest DELETE+SET 重试臂）。
    for cmd in [
      RespCommand::Bitop,
      RespCommand::BitopAnd,
      RespCommand::BitopOr,
      RespCommand::BitopXor,
      RespCommand::BitopNot,
      RespCommand::BitopDiff,
    ] {
      assert!(
        is_vector_gate_exempt(cmd) && !vector_gate_scan_all_keys(cmd),
        "{cmd:?} 应由位点内源/目分流裁决而非派发门"
      );
    }
    // 核心幽灵写入口（对象族 / bitmap / PF / GEO）不得被豁免，须落 args[0] 值域门。
    // Getset 同归本组：C# NetworkGETSET 走 SET_Conditional getValue 臂，
    // WRONGTYPE 直接回错无 DELETE 重试，登记保留。
    for cmd in [
      RespCommand::Hset,
      RespCommand::Lpush,
      RespCommand::Sadd,
      RespCommand::Zadd,
      RespCommand::Pfadd,
      RespCommand::Geoadd,
      RespCommand::Setbit,
      RespCommand::Bitfield,
      RespCommand::Get,
      RespCommand::Incr,
      RespCommand::Getset,
    ] {
      assert!(
        is_data_command(cmd) && !is_legal_on_vector_set(cmd) && !is_vector_gate_exempt(cmd),
        "{cmd:?} 应受 args[0] 值域 WRONGTYPE 门约束"
      );
    }
  }

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

  #[test]
  fn test_range_index_command_value_intervals() {
    // RI 族命令的判别值区间守卫（与 RI 门禁判据无关：门禁按记录物理域事实，
    // 见 task/ing/ri-predicate-gate.md §三，此处只钉枚举值排布）
    //
    // RI.COUNT 为本仓自定义扩展（C# RI 族无此命令），仍须落在 C# 的连续
    // 判别值区间内：读命令区间 [BITCOUNT, RISCAN] 与数据命令区间
    // [APPEND, EVALSHA] 双覆盖，否则 is_read_only / is_data_command 的
    // 区间判定会漏掉它（集群键槽校验与读写命令指标同时失真）
    let ricount = RespCommand::Ricount as u16;
    assert!((FIRST_READ_COMMAND as u16..=LAST_READ_COMMAND as u16).contains(&ricount));
    assert!((FIRST_DATA_COMMAND as u16..=LAST_DATA_COMMAND as u16).contains(&ricount));
    assert!(is_read_only(RespCommand::Ricount));
    assert!(is_data_command(RespCommand::Ricount));
    assert!(!is_write_only(RespCommand::Ricount));
  }
}
