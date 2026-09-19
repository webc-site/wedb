//! RESP 命令层共享错误文案与应答写出辅助（对标 libs/server/Resp/CmdStrings.cs）
//!
//! `write_resp_error`（见 [`crate::ext::RespVecExt`]）固定前置 `-ERR `，
//! 而 C# 大量错误常量自带 `ERR`/`WRONGTYPE` 等完整前缀（经 RespWriteUtils.
//! TryWriteError 以 `-<msg>\r\n` 原样写出），故此处提供不加工前缀的原样写出。

use crate::{
  ext::sanitize_error_str,
  resp_memory_writer::{Resp3, RespWriter},
};

/// libs/server/Resp/CmdStrings.cs:RESP_OK
pub const RESP_OK: &[u8] = b"+OK\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_PONG
pub const RESP_PONG: &[u8] = b"+PONG\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_EMPTYLIST
pub const RESP_EMPTYLIST: &[u8] = b"*0\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_EMPTY（INFO 空段等「无内容」应答，协议恒定空批量串）
pub const RESP_EMPTY: &[u8] = b"$0\r\n\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_RETURN_VAL_0
pub const RESP_RETURN_VAL_0: &[u8] = b":0\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_RETURN_VAL_1
pub const RESP_RETURN_VAL_1: &[u8] = b":1\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_RETURN_VAL_N1
pub const RESP_RETURN_VAL_N1: &[u8] = b":-1\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_RETURN_VAL_N2
pub const RESP_RETURN_VAL_N2: &[u8] = b":-2\r\n";
/// MULTI 入队应答帧（libs/server/Resp/CmdStrings.cs:RESP_QUEUED，C# 消费点
/// TxnRespCommands.cs:199 的 `TryWriteDirect(CmdStrings.RESP_QUEUED)`）
pub const RESP_QUEUED: &[u8] = b"+QUEUED\r\n";

// pub/sub 会话帧头单点：C# 侧这些字节由数组头/批量串/push 头三个写入原语逐段产出
// （`libs/server/Resp/PubSubCommands.cs` 各 Network* 分支与 Publish / PatternPublish），
// rust 侧编译期拼成整帧，免运行时格式化与堆分配。
// 归本表单点的原因：本表已是 RESP 定长帧的唯一出处（RESP_OK / RESP_EMPTYLIST /
// RESP_RETURN_VAL_*），pub/sub 帧同属线协议层，长在 wpubsub 业务 crate 里
// 只能被 wedb 集群会话反向 use 取用。
/// 订阅会话 RESP2 下的 PING 应答整帧（两元素数组 `["pong",""]`，非普通 `+PONG`）
/// libs/server/Resp/CmdStrings.cs:SUSCRIBE_PONG（BasicCommands.cs NetworkPING 订阅臂）
pub const SUSCRIBE_PONG: &[u8] = b"*2\r\n$4\r\npong\r\n$0\r\n\r\n";
/// SUBSCRIBE ack 头（`libs/server/Resp/PubSubCommands.cs` NetworkSUBSCRIBE 分支）
pub const PUBSUB_SUBSCRIBE_FRAME_PREFIX: &[u8] = b"*3\r\n$9\r\nsubscribe\r\n";
/// SSUBSCRIBE ack 头（`libs/server/Resp/PubSubCommands.cs` NetworkSUBSCRIBE 的 shard 分支）
pub const PUBSUB_SSUBSCRIBE_FRAME_PREFIX: &[u8] = b"*3\r\n$10\r\nssubscribe\r\n";
/// PSUBSCRIBE ack 头（`libs/server/Resp/PubSubCommands.cs` NetworkPSUBSCRIBE 分支）
pub const PUBSUB_PSUBSCRIBE_FRAME_PREFIX: &[u8] = b"*3\r\n$10\r\npsubscribe\r\n";
/// UNSUBSCRIBE ack 头（`libs/server/Resp/PubSubCommands.cs` NetworkUNSUBSCRIBE 分支）
pub const PUBSUB_UNSUBSCRIBE_FRAME_PREFIX: &[u8] = b"*3\r\n$11\r\nunsubscribe\r\n";
/// SUNSUBSCRIBE ack 头（`libs/server/Resp/PubSubCommands.cs` NetworkSUNSUBSCRIBE 分支）
pub const PUBSUB_SUNSUBSCRIBE_FRAME_PREFIX: &[u8] = b"*3\r\n$12\r\nsunsubscribe\r\n";
/// PUNSUBSCRIBE ack 头（`libs/server/Resp/PubSubCommands.cs` NetworkPUNSUBSCRIBE 分支）
pub const PUBSUB_PUNSUBSCRIBE_FRAME_PREFIX: &[u8] = b"*3\r\n$12\r\npunsubscribe\r\n";
/// 频道消息 push 头 RESP2（`libs/server/Resp/PubSubCommands.cs` Publish）
pub const PUBSUB_PUSH_MSG_PREFIX_RESP2: &[u8] = b"*3\r\n$7\r\nmessage\r\n";
/// 频道消息 push 头 RESP3（`libs/server/Resp/PubSubCommands.cs` Publish）
pub const PUBSUB_PUSH_MSG_PREFIX_RESP3: &[u8] = b">3\r\n$7\r\nmessage\r\n";
/// 模式消息 push 头 RESP2（`libs/server/Resp/PubSubCommands.cs` PatternPublish）
pub const PUBSUB_PUSH_PMSG_PREFIX_RESP2: &[u8] = b"*4\r\n$8\r\npmessage\r\n";
/// 模式消息 push 头 RESP3（`libs/server/Resp/PubSubCommands.cs` PatternPublish）
pub const PUBSUB_PUSH_PMSG_PREFIX_RESP3: &[u8] = b">4\r\n$8\r\npmessage\r\n";
/// 分片频道消息 push 头 RESP2（shard 订阅为 rust 相对 C# 的扩展，同 Publish 句式）
pub const PUBSUB_PUSH_SMSG_PREFIX_RESP2: &[u8] = b"*3\r\n$8\r\nsmessage\r\n";
/// 分片频道消息 push 头 RESP3（shard 订阅为 rust 相对 C# 的扩展，同 Publish 句式）
pub const PUBSUB_PUSH_SMSG_PREFIX_RESP3: &[u8] = b">3\r\n$8\r\nsmessage\r\n";

/// wnode 命令层通用兜底错误文案（经 write_resp_error 前置 `-ERR ` 后输出 `-ERR generic error\r\n`）
pub const RESP_ERR_GENERIC: &str = "generic error";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOAUTH
pub const RESP_ERR_NOAUTH: &str = "NOAUTH Authentication required.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOPERM
pub const RESP_ERR_NOPERM: &str = "NOPERM this user has no permissions to run the command";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_WRONG_TYPE
pub const RESP_ERR_WRONG_TYPE: &str =
  "WRONGTYPE Operation against a key holding the wrong kind of value.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_WRONG_TYPE_HLL
pub const RESP_ERR_WRONG_TYPE_HLL: &str = "WRONGTYPE Key is not a valid HyperLogLog string value.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_UNK_CMD
pub const RESP_ERR_GENERIC_UNK_CMD: &str = "ERR unknown command";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_CLUSTER_DISABLED
pub const RESP_ERR_GENERIC_CLUSTER_DISABLED: &str =
  "ERR This instance has cluster support disabled";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_NOSUCHKEY
pub const RESP_ERR_GENERIC_NOSUCHKEY: &str = "ERR no such key";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_INVALIDEXP_IN_SET
pub const RESP_ERR_GENERIC_INVALIDEXP_IN_SET: &str = "ERR invalid expire time in 'set' command";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_INVALIDEXP_IN_GETEX
pub const RESP_ERR_GENERIC_INVALIDEXP_IN_GETEX: &str = "ERR invalid expire time in 'getex' command";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_OVERFLOWEXP_IN_GETEX
pub const RESP_ERR_OVERFLOWEXP_IN_GETEX: &str = "ERR expire time overflows date in 'getex' command";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_SYNTAX_ERROR
/// （C# 另有同义字段 RESP_SYNTAX_ERROR，见 CmdStrings.cs:RESP_SYNTAX_ERROR，同一文案）
pub const RESP_ERR_GENERIC_SYNTAX_ERROR: &str = "ERR syntax error";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_ETAG
pub const RESP_ERR_INVALID_ETAG: &str = "ETAG must be a numerical value greater than or equal to 0";
/// libs/server/Resp/CmdStrings.cs:NOGET
pub const NOGET: &str = "NOGET";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_NAN_INFINITY
pub const RESP_ERR_GENERIC_NAN_INFINITY: &str = "ERR value is NaN or Infinity";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_NAN_INFINITY_INCR
pub const RESP_ERR_GENERIC_NAN_INFINITY_INCR: &str = "ERR increment would produce NaN or Infinity";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_OFFSETOUTOFRANGE
pub const RESP_ERR_GENERIC_OFFSETOUTOFRANGE: &str = "ERR offset is out of range";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER
pub const RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER: &str =
  "ERR value is not an integer or out of range.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_HASH_VALUE_IS_NOT_INTEGER
pub const RESP_ERR_HASH_VALUE_IS_NOT_INTEGER: &str = "ERR hash value is not an integer.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_HASH_VALUE_IS_NOT_FLOAT
pub const RESP_ERR_HASH_VALUE_IS_NOT_FLOAT: &str = "ERR hash value is not a float.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE
pub const RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE: &str =
  "ERR value is out of range, must be positive.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_INDEX_OUT_RANGE
pub const RESP_ERR_GENERIC_INDEX_OUT_RANGE: &str = "ERR index out of range";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER
pub const RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER: &str = "ERR bit is not an integer or out of range";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER
pub const RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER: &str =
  "ERR bit offset is not an integer or out of range";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER
pub const RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER: &str =
  "ERR Protocol version is not an integer or out of range.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION
pub const RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION: &str = "ERR Unsupported protocol version";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOT_VALID_FLOAT
pub const RESP_ERR_NOT_VALID_FLOAT: &str = "ERR value is not a valid float";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_XX_NX_NOT_COMPATIBLE
pub const RESP_ERR_XX_NX_NOT_COMPATIBLE: &str =
  "ERR XX and NX options at the same time are not compatible";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GT_LT_NX_NOT_COMPATIBLE
pub const RESP_ERR_GT_LT_NX_NOT_COMPATIBLE: &str =
  "ERR GT, LT, and/or NX options at the same time are not compatible";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR
pub const RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR: &str =
  "ERR INCR option supports a single increment-element pair";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_SCORE_NAN
pub const RESP_ERR_GENERIC_SCORE_NAN: &str = "ERR resulting score is not a number (NaN)";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_ZSET_MEMBER
pub const RESP_ERR_ZSET_MEMBER: &str = "ERR could not decode requested zset member";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_LIMIT_NOT_SUPPORTED
pub const RESP_ERR_LIMIT_NOT_SUPPORTED: &str =
  "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_MIN_MAX_NOT_VALID_FLOAT
pub const RESP_ERR_MIN_MAX_NOT_VALID_FLOAT: &str = "ERR min or max is not a float";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_MIN_MAX_NOT_VALID_STRING
pub const RESP_ERR_MIN_MAX_NOT_VALID_STRING: &str = "ERR min or max not valid string range item";
/// rust 专有：扩展命令读写门禁文案，garnet 无对应文案（消费方跨 wext_json / wext_roaring，
/// 故落 wresp 单点）
pub const RESP_ERR_COMMAND_READ_ONLY: &str = "ERR command is read-only";
/// rust 专有：同上，garnet 无对应文案
pub const RESP_ERR_COMMAND_WRITE_ONLY: &str = "ERR command is write-only";
/// rust 专有：向量集迁移域索引校验文案，garnet 无 "migrated vector" 文案
/// （消费方跨 wedb cluster_session 与 wnode vector，故落 wresp 单点）
pub const RESP_ERR_INVALID_MIGRATED_VECTOR_SET_INDEX: &str =
  "ERR Invalid migrated vector set index";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_TIMEOUT_NOT_VALID_FLOAT
pub const RESP_ERR_TIMEOUT_NOT_VALID_FLOAT: &str = "ERR timeout is not a float or out of range";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOT_VALID_RADIUS
pub const RESP_ERR_NOT_VALID_RADIUS: &str = "ERR need numeric radius";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_RADIUS_IS_NEGATIVE
pub const RESP_ERR_RADIUS_IS_NEGATIVE: &str = "ERR radius cannot be negative";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOT_VALID_WIDTH
pub const RESP_ERR_NOT_VALID_WIDTH: &str = "ERR need numeric width";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOT_VALID_HEIGHT
pub const RESP_ERR_NOT_VALID_HEIGHT: &str = "ERR need numeric height";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE
pub const RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE: &str = "ERR height or width cannot be negative";
/// libs/server/Resp/CmdStrings.cs:GenericErrLonLat（{0}/{1} 由 GEO 会话层以
/// F6 定点渲染回填，对齐 string.Format {0:F6} 的 Infinity 词形与半点远离零）
pub const GENERIC_ERR_LON_LAT: &str = "ERR invalid longitude,latitude pair {0},{1}";
/// libs/server/Resp/CmdStrings.cs:GenericErrStoreCommand（{0} 由 GEO 会话层按命令名回填，
/// 对标消费点 string.Format(模板, command.ToString())，即命令枚举名全大写）
pub const GENERIC_ERR_STORE_COMMAND: &str =
  "ERR STORE option in {0} is not compatible with WITHDIST, WITHHASH and WITHCOORD options";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT
pub const RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT: &str =
  "ERR unsupported unit provided. please use M, KM, FT, MI";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_COUNT_IS_NOT_POSITIVE
pub const RESP_ERR_COUNT_IS_NOT_POSITIVE: &str = "ERR COUNT must be > 0";
/// 慢路径异步扫描/清库 IO 失败的兜底文案（非 C# 文案：存储层错误统一降噪为
/// 此单行，杜绝内部错误细节泄漏给客户端；wnode 执行域与 wedb 集群域共用）
pub const RESP_ERR_SLOW_PATH_STORAGE: &str = "ERR slow path storage error";
/// libs/server/Resp/CmdStrings.cs:RESP_WRONGPASS_INVALID_PASSWORD
pub const RESP_WRONGPASS_INVALID_PASSWORD: &str = "WRONGPASS Invalid password";
/// libs/server/Resp/CmdStrings.cs:RESP_WRONGPASS_INVALID_USERNAME_PASSWORD
pub const RESP_WRONGPASS_INVALID_USERNAME_PASSWORD: &str =
  "WRONGPASS Invalid username/password combination";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_BUSSYKEY
pub const RESP_ERR_BUSSYKEY: &str = "BUSYKEY Target key name already exists.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_EXPIRE_TIME
pub const RESP_ERR_INVALID_EXPIRE_TIME: &str = "ERR invalid expire time, must be >= 0";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOSCRIPT
pub const RESP_ERR_NOSCRIPT: &str = "ERR This Redis command is not allowed from script";
/// EVALSHA 摘要未命中脚本缓存的应答（libs/server/Resp/CmdStrings.cs:RESP_ERR_NO_SCRIPT，
/// 与上一条 RESP_ERR_NOSCRIPT 是 C# 里的两条不同常量：本条 `NOSCRIPT` 前缀、
/// 走 TryWriteError 原样成帧，故以整帧 `&[u8]` 形态承接，供 write_error_bytes 直写）
pub const RESP_ERR_NO_SCRIPT: &[u8] = b"NOSCRIPT No matching script. Please use EVAL.";
/// SCRIPT FLUSH 非法选项文案（libs/server/Resp/CmdStrings.cs:RESP_ERR_SCRIPT_FLUSH_OPTIONS，
/// 同 TryWriteError 原样成帧的 `&[u8]` 形态）
pub const RESP_ERR_SCRIPT_FLUSH_OPTIONS: &[u8] = b"ERR SCRIPT FLUSH only support SYNC|ASYNC option";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_LUA_DISABLED
pub const RESP_ERR_LUA_DISABLED: &str = "ERR This instance has Lua scripting support disabled";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS
pub const RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS: &str = "ERR HCOLLECT scan already in progress";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_ZCOLLECT_ALREADY_IN_PROGRESS
pub const RESP_ERR_ZCOLLECT_ALREADY_IN_PROGRESS: &str = "ERR ZCOLLECT scan already in progress";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_OBJECT_FREQ_UNSUPPORTED
pub const RESP_ERR_OBJECT_FREQ_UNSUPPORTED: &str = "ERR OBJECT FREQ is not supported: Garnet does not track access frequency (no LFU maxmemory policy).";
/// libs/server/Resp/CmdStrings.cs:RESP_INVALID_COMMAND_SPECIFIED
pub const RESP_INVALID_COMMAND_SPECIFIED: &str = "Invalid command specified";
/// libs/server/Resp/CmdStrings.cs:RESP_COMMAND_HAS_NO_KEY_ARGS
pub const RESP_COMMAND_HAS_NO_KEY_ARGS: &str = "The command has no key arguments";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOT_SUPPORTED_RESP2
pub const RESP_ERR_NOT_SUPPORTED_RESP2: &str = "ERR command not supported in RESP2";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_CANNOT_LIST_CLIENTS
pub const RESP_ERR_CANNOT_LIST_CLIENTS: &str = "ERR Clients cannot be listed.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_UBLOCKING_CLINET
pub const RESP_ERR_UBLOCKING_CLINET: &str = "ERR Unable to unblock client because of error.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NO_SUCH_CLIENT
pub const RESP_ERR_NO_SUCH_CLIENT: &str = "ERR No such client";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_CLIENT_ID
pub const RESP_ERR_INVALID_CLIENT_ID: &str = "ERR Invalid client ID";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_CLIENT_NAME
pub const RESP_ERR_INVALID_CLIENT_NAME: &str =
  "ERR Client names cannot contain spaces, newlines or special characters.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_CLIENT_UNBLOCK_REASON
pub const RESP_ERR_INVALID_CLIENT_UNBLOCK_REASON: &str =
  "ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR";
/// libs/server/Resp/CmdStrings.cs:RESP_UNBLOCKED_CLIENT_VIA_CLIENT_UNBLOCK
///
/// 无 ERR 前缀（C# 经 TryWriteError 原样写出文案）
pub const RESP_UNBLOCKED_CLIENT_VIA_CLIENT_UNBLOCK: &str =
  "UNBLOCKED client unblocked via CLIENT UNBLOCK";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_EXPDELSCAN_INVALID
pub const RESP_ERR_EXPDELSCAN_INVALID: &str =
  "ERR Cannot execute EXPDELSCAN with background expired key deletion scan enabled";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS
pub const RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS: &str = "ERR checkpoint already in progress";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_DB_INDEX_OUT_OF_RANGE
pub const RESP_ERR_DB_INDEX_OUT_OF_RANGE: &str = "ERR DB index is out of range.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_FIRST_DB_INDEX
pub const RESP_ERR_INVALID_FIRST_DB_INDEX: &str = "ERR invalid first DB index.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_SECOND_DB_INDEX
pub const RESP_ERR_INVALID_SECOND_DB_INDEX: &str = "ERR invalid second DB index.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE
///
/// 文案按 doc/zh/db.md SWAPDB 条款改口径：集群模式不再一刀切拒绝，仅当两库
/// 槽位非全部由本地节点掌管时拦截，故偏离 C# 原文案（"not allowed in cluster
/// mode"）改为归属判定语义
pub const RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE: &str =
  "ERR SWAPDB databases are not served by this node";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_SWAPDB_UNSUPPORTED
pub const RESP_ERR_SWAPDB_UNSUPPORTED: &str =
  "ERR SWAPDB is currently unsupported when multiple clients are connected.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_FLUSHALL_READONLY_REPLICA
pub const RESP_ERR_FLUSHALL_READONLY_REPLICA: &str =
  "ERR You can't write against a read only replica.";
/// libs/server/Resp/CmdStrings.cs:GenericErrWrongNumArgs
pub const GENERIC_ERR_WRONG_NUM_ARGS: &str = "ERR wrong number of arguments for '{0}' command";

/// 参数数量错误文案编译期展开宏（[`GENERIC_ERR_WRONG_NUM_ARGS`] 的单点静态
/// 形态）：concat! 零堆分配，供 `&'static str` 场景（如 VectorReply 错误帧）
/// 引用；命令名非字面量的运行时路径走 [`abort_with_wrong_number_of_arguments`]
/// 或模板 replace，三者展开文本逐字节一致
#[macro_export]
macro_rules! wrong_num_args {
  ($cmd:literal) => {
    concat!("ERR wrong number of arguments for '", $cmd, "' command")
  };
}

/// libs/server/Resp/CmdStrings.cs:RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS
pub const RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS: &str = "ERR wrong number of arguments for command";
/// LMPOP/SMPOP/BZMPOP 等命令的 numkeys 校验文案（跨 list/set/sortedset 三域复用）
pub const RESP_ERR_GENERIC_NUMKEYS: &str = "ERR numkeys should be greater than 0";
/// LMPOP COUNT 校验文案（GenericErrShouldBeGreaterThanZero 固定替换 {0}="count"）
pub const RESP_ERR_COUNT_GREATER_THAN_ZERO: &str = "ERR count should be greater than 0";
/// SINTERCARD/ZINTERCARD 的 LIMIT 负值校验文案
///（GenericErrCantBeNegative 固定替换 {0}="LIMIT"）
pub const RESP_ERR_LIMIT_CANT_BE_NEGATIVE: &str = "ERR LIMIT can't be negative";
/// ZINTERCARD numkeys < 1 校验文案（GenericErrAtLeastOneKey 固定替换 {0}="ZINTERCARD"）
pub const RESP_ERR_ZINTERCARD_AT_LEAST_ONE_KEY: &str =
  "ERR at least 1 input key is needed for 'ZINTERCARD' command";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NO_TRANSACTION_PROCEDURE
pub const RESP_ERR_NO_TRANSACTION_PROCEDURE: &str = "ERR Could not get transaction procedure";
/// （rust 自有文案；C# CmdStrings 无对应异步要求错误常量）
pub const RESP_ERR_ASYNC_REQUIRED: &str = "ERR command requires asynchronous completion";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_INVALIDCURSOR
pub const RESP_ERR_GENERIC_INVALIDCURSOR: &str = "ERR invalid cursor";
/// libs/server/Objects/Types/GarnetObjectBase.cs 对象命令不支持的通用文案
/// （跨 hash/set/list/sortedset 四对象域复用）
pub const RESP_ERR_GENERIC_UNSUPPORTED_OPERATION: &str = "ERR unsupported operation";
/// libs/server/Resp/CmdStrings.cs:GenericErrUnknownSubCommand
pub const GENERIC_ERR_UNKNOWN_SUB_COMMAND: &str = "ERR unknown subcommand '{0}'. Try {1} HELP";
/// libs/server/Resp/CmdStrings.cs:GenericErrUnknownSubCommandNoHelp
pub const GENERIC_ERR_UNKNOWN_SUB_COMMAND_NO_HELP: &str = "ERR unknown subcommand '{0}'.";
/// libs/server/Resp/CmdStrings.cs:GenericUnknownClientType
pub const GENERIC_UNKNOWN_CLIENT_TYPE: &str = "ERR Unknown client type '{0}'";
/// libs/server/Resp/CmdStrings.cs:GenericErrDuplicateFilter
pub const GENERIC_ERR_DUPLICATE_FILTER: &str = "ERR Filter '{0}' defined multiple times";
/// libs/server/Resp/CmdStrings.cs:GenericPubSubCommandDisabled
pub const GENERIC_PUBSUB_COMMAND_DISABLED: &str =
  "ERR {0} is disabled, enable it with --pubsub option.";
/// libs/server/Resp/CmdStrings.cs:GenericPubSubCommandNotAllowed
pub const GENERIC_PUBSUB_COMMAND_NOT_ALLOWED: &str = "ERR Can't execute '{0}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context";
/// libs/server/Resp/CmdStrings.cs:GenericErrShouldBeGreaterThanZero
pub const GENERIC_ERR_SHOULD_BE_GREATER_THAN_ZERO: &str = "ERR {0} should be greater than 0";
/// libs/server/Resp/CmdStrings.cs:GenericErrNotAFloat（SortedSet WEIGHTS 固定替换 {0}="weight"）
pub const GENERIC_ERR_NOT_A_FLOAT_WEIGHT: &str = "ERR weight value is not a valid float";
/// libs/server/Resp/CmdStrings.cs:GenericParamShouldBeGreaterThanZero
pub const GENERIC_PARAM_SHOULD_BE_GREATER_THAN_ZERO: &str =
  "ERR Parameter `{0}` should be greater than 0";
/// libs/server/Resp/CmdStrings.cs:GenericErrUnknownOptionConfigSet
pub const GENERIC_ERR_UNKNOWN_OPTION_CONFIG_SET: &str =
  "ERR Unknown option or number of arguments for CONFIG SET - '{0}'";
/// libs/server/Resp/CmdStrings.cs:GenericErrCommandDisallowedWithOption (DEBUG 实例化)
pub const RESP_ERR_DEBUG_DISALLOWED: &str = "ERR DEBUG command not allowed. If the enable-debug-command option is set to \"local\", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.";

// ---- 自 wnode 私有 const 上收的 CmdStrings 归属单点（resp-frames-cmdstrings-homing：
// C# 全部定义于 libs/server/Resp/CmdStrings.cs，命名与 C# 同名映射；
// 帧类 &[u8]、文案类 &str，与本文件既有风格一致）----

/// libs/server/Resp/CmdStrings.cs:RESP_ERR_STRING_EXCEEDS_MAX_SIZE
pub const RESP_ERR_STRING_EXCEEDS_MAX_SIZE: &str =
  "ERR string exceeds maximum allowed size (proto-max-bulk-len)";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_BITOP_KEY_LIMIT
pub const RESP_ERR_BITOP_KEY_LIMIT: &str = "ERR Bitop source key limit (64) exceeded";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED
pub const RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED: &str =
  "ERR BITOP DIFF must be called with at least two source keys.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY
pub const RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY: &str =
  "ERR BITOP NOT must be called with a single source key.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_BITFIELD_TYPE
pub const RESP_ERR_INVALID_BITFIELD_TYPE: &str =
  "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_OVERFLOW_TYPE
pub const RESP_ERR_INVALID_OVERFLOW_TYPE: &str = "ERR Invalid OVERFLOW type specified";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_NESTED_MULTI
pub const RESP_ERR_GENERIC_NESTED_MULTI: &str = "ERR MULTI calls can not be nested";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_EXEC_ABORT
pub const RESP_ERR_EXEC_ABORT: &str = "EXECABORT Transaction discarded because of previous errors.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_EXEC_WO_MULTI
pub const RESP_ERR_GENERIC_EXEC_WO_MULTI: &str = "ERR EXEC without MULTI";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_DISCARD_WO_MULTI
pub const RESP_ERR_GENERIC_DISCARD_WO_MULTI: &str = "ERR DISCARD without MULTI";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_WATCH_IN_MULTI
pub const RESP_ERR_GENERIC_WATCH_IN_MULTI: &str = "ERR WATCH inside MULTI is not allowed";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_SELECT_IN_TXN_UNSUPPORTED
pub const RESP_ERR_SELECT_IN_TXN_UNSUPPORTED: &str =
  "ERR SELECT is currently unsupported inside a transaction.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_SWAPDB_IN_TXN_UNSUPPORTED
pub const RESP_ERR_SWAPDB_IN_TXN_UNSUPPORTED: &str =
  "ERR SWAPDB is currently unsupported inside a transaction.";
/// libs/server/Resp/CmdStrings.cs:GenericErrIncorrectSizeFormat
pub const GENERIC_ERR_INCORRECT_SIZE_FORMAT: &str = "ERR Incorrect size format in (option: '{0}')";
/// libs/server/Resp/CmdStrings.cs:GenericErrMainLogMemorySizeTrackerNotRunning
pub const GENERIC_ERR_MAIN_LOG_MEMORY_SIZE_TRACKER_NOT_RUNNING: &str = "ERR Cannot adjust main log memory size configuration when size tracker is not running (option: '{0}')";
/// libs/server/Resp/CmdStrings.cs:GenericErrReadCacheMemorySizeTrackerNotRunning
pub const GENERIC_ERR_READ_CACHE_MEMORY_SIZE_TRACKER_NOT_RUNNING: &str = "ERR Cannot adjust readcache memory size configuration when size tracker is not running (option: '{0}')";
/// libs/server/Resp/CmdStrings.cs:GenericErrIndexSizePowerOfTwo
pub const GENERIC_ERR_INDEX_SIZE_POWER_OF_TWO: &str =
  "ERR Index size must be a power of 2 (option: '{0}')";
/// libs/server/Resp/CmdStrings.cs:GenericErrIndexSizeSmallerThanCurrent
pub const GENERIC_ERR_INDEX_SIZE_SMALLER_THAN_CURRENT: &str =
  "ERR Cannot set dynamic index size smaller than current index size (option: '{0}')";
/// libs/server/Resp/CmdStrings.cs:GenericErrIndexSizeGrowFailed
pub const GENERIC_ERR_INDEX_SIZE_GROW_FAILED: &str =
  "ERR failed to grow index size beyond current size (option: '{0}')";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_TIMEOUT_IS_NEGATIVE
pub const RESP_ERR_TIMEOUT_IS_NEGATIVE: &str = "ERR timeout is negative";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_TIMEOUT_IS_OUT_OF_RANGE
pub const RESP_ERR_TIMEOUT_IS_OUT_OF_RANGE: &str = "ERR timeout is out of range";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_ACL_AUTH_DISABLED
pub const RESP_ERR_ACL_AUTH_DISABLED: &str = "ERR ACL Authenticator is disabled.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_ACL_AUTH_FILE_DISABLED
///
/// 本仓 ACL 用户以 `KeyTag::Acl` 直接落底层存储、SETUSER/DELUSER 同步写穿，
/// 无 `--acl-file` 配置面，故 C# 的「未配 ACL 文件」门恒不成立。
pub const RESP_ERR_ACL_AUTH_FILE_DISABLED: &str =
  "ERR This instance persists ACL users directly in storage, ACL SAVE/LOAD are not applicable.";
/// libs/server/Resp/CmdStrings.cs:MainLogMemory（CONFIG 参数键名）
pub const MAIN_LOG_MEMORY: &[u8] = b"memory";
/// libs/server/Resp/CmdStrings.cs:ReadCacheMemory
pub const READ_CACHE_MEMORY: &[u8] = b"readcache-memory";
/// libs/server/Resp/CmdStrings.cs:Index
pub const INDEX: &[u8] = b"index";
/// libs/server/Resp/CmdStrings.cs:CertFileName
pub const CERT_FILE_NAME: &[u8] = b"cert-file-name";
/// libs/server/Resp/CmdStrings.cs:CertPassword
pub const CERT_PASSWORD: &[u8] = b"cert-password";
/// libs/server/Resp/CmdStrings.cs:ClusterUsername
pub const CLUSTER_USERNAME: &[u8] = b"cluster-username";
/// libs/server/Resp/CmdStrings.cs:ClusterPassword
pub const CLUSTER_PASSWORD: &[u8] = b"cluster-password";

// ---- 命令解析期输入 token 单点（对标 libs/server/Resp/CmdStrings.cs 的
// `public static ReadOnlySpan<byte> COUNT => "COUNT"u8;` 一族）----
//
// C# 的 CmdStrings 同时是「输出帧」与「输入 token」两半边的单点；本表此前只承接
// 输出半边，COUNT / WITHSCORES / LIMIT 等在 wcol、wnode 两个 crate 的解析臂逐处
// 裸内联（同一 token 复写 30 余处，同步段与慢段各写一遍），拼写漂移无人拦。
// 归位于此而非各文件私有 const 的理由与 CONFIG 键名族（MAIN_LOG_MEMORY 等）相同：
// 这些 token 跨命令族共享，私有 const 只能被同 crate 反向取用。
//
// 大小写口径：C# 另有 count / type 这类小写双常量，是给 EqualsUpperCaseSpanIgnoringCase
// 之外的窄口径比较用的；rust 解析臂统一走 `eq_ignore_ascii_case`，故每个 token 只收
// 一份大写形态。唯一例外是 [`COUNT_LOWER`]：LPOS 选项解析按 C#
// ListObjectImpl.cs 的 `SequenceEqual(COUNT) || SequenceEqual(count)` 双写形态转写，
// 比较语义要求保留精确大小写两臂。

/// SCAN 族 MATCH pattern 选项（libs/server/Resp/CmdStrings.cs:MATCH）
pub const MATCH: &[u8] = b"MATCH";
/// SCAN 族 / LPOS / LMPOP 族 / GEOSEARCH / ZRANGEBYSCORE 的 COUNT 选项
/// （libs/server/Resp/CmdStrings.cs:COUNT）
pub const COUNT: &[u8] = b"COUNT";
/// COUNT 的小写形态，仅与 [`COUNT`] 配成窄口径双写比较（LPOS 选项解析对位
/// C# `SequenceEqual(COUNT) || SequenceEqual(count)`，见
/// /Users/z/git/db/wedb/wedb/wcol/src/list/list_object_impl.rs）
/// （libs/server/Resp/CmdStrings.cs:count）
pub const COUNT_LOWER: &[u8] = b"count";
/// HSCAN / SSCAN / ZSCAN 的 NOVALUES 选项（libs/server/Resp/CmdStrings.cs:NOVALUES）
pub const NOVALUES: &[u8] = b"NOVALUES";
/// SCAN 族的类型过滤选项与 CLIENT LIST/KILL 的 TYPE 过滤选项
/// （libs/server/Resp/CmdStrings.cs:TYPE）
pub const TYPE: &[u8] = b"TYPE";
/// GETEX 的 PERSIST 选项（libs/server/Resp/CmdStrings.cs:PERSIST）
pub const PERSIST: &[u8] = b"PERSIST";
/// ZDIFF / ZINTERCARD / SINTERCARD 与 BYSCORE/BYLEX 范围命令的分页 LIMIT 选项
/// （libs/server/Resp/CmdStrings.cs:LIMIT）
pub const LIMIT: &[u8] = b"LIMIT";
/// ZUNION / ZUNIONSTORE / ZINTER 族权重表选项（libs/server/Resp/CmdStrings.cs:WEIGHTS）
pub const WEIGHTS: &[u8] = b"WEIGHTS";
/// ZRANK / ZREVRANK 的 WITHSCORE 选项（libs/server/Resp/CmdStrings.cs:WITHSCORE）
pub const WITHSCORE: &[u8] = b"WITHSCORE";
/// 有序集合（ZRANGE / ZDIFF / ZRANDMEMBER 族）与向量集 VSIM / VLINKS 的
/// WITHSCORES 选项（libs/server/Resp/CmdStrings.cs:WITHSCORES）
pub const WITHSCORES: &[u8] = b"WITHSCORES";
/// HRANDFIELD 的 WITHVALUES 选项（libs/server/Resp/CmdStrings.cs:WITHVALUES）
pub const WITHVALUES: &[u8] = b"WITHVALUES";

/// 反射参数/命令名截断上限（防恶意超长子命令/选项名攻击）
pub const MAX_PARAM_NAME_LEN: usize = 128;

/// 原样写出错误行 `-<msg>\r\n`（msg 自带 `ERR`/`WRONGTYPE` 等完整前缀，
/// 对标 libs/common/RespWriteUtils.cs:TryWriteError）
#[inline]
pub fn write_error_raw(output: &mut Vec<u8>, msg: &str) {
  RespWriter::new_ref(output).write_error(msg);
}

/// 以 `GenericErrWrongNumArgs` 格式化命令名并写出错误应答（零堆分配直接写入）
#[inline]
pub fn abort_with_wrong_number_of_arguments(output: &mut Vec<u8>, cmd_name: &str) {
  let clean_name = sanitize_error_str(cmd_name, MAX_PARAM_NAME_LEN);
  output.extend_from_slice(b"-ERR wrong number of arguments for '");
  output.extend_from_slice(clean_name.as_bytes());
  output.extend_from_slice(b"' command\r\n");
}

/// 以 `GENERIC_PUBSUB_COMMAND_DISABLED` 模板回填命令名并写出错误应答
/// （对标 C# 侧 AbortWithErrorMessage(string.Format(模板, 命令名)) 的调用点写法；
/// --pubsub 关闭的错误分支才走这里，非热路径）
#[inline]
pub fn abort_with_pubsub_command_disabled(output: &mut Vec<u8>, cmd_name: &str) {
  let clean_name = sanitize_error_str(cmd_name, MAX_PARAM_NAME_LEN);
  let message = GENERIC_PUBSUB_COMMAND_DISABLED.replace("{0}", clean_name);
  write_error_raw(output, &message);
}

/// 写出不支持选项错误应答：`-ERR Unsupported option <option>\r\n`（零堆分配）
#[inline]
pub fn abort_with_unsupported_option(output: &mut Vec<u8>, option: &str) {
  let clean_opt = sanitize_error_str(option, MAX_PARAM_NAME_LEN);
  output.extend_from_slice(b"-ERR Unsupported option ");
  output.extend_from_slice(clean_opt.as_bytes());
  output.extend_from_slice(b"\r\n");
}

/// 以 `GenericSyntaxErrorOption`（CmdStrings.cs:334
/// `"ERR Syntax error in {0} option '{1}'"`）回填命令名与选项名并写出
/// 错误应答（零堆分配直接写入，参数名过 MAX_PARAM_NAME_LEN 清洗帽）
#[inline]
pub fn abort_with_syntax_error_option(output: &mut Vec<u8>, cmd_name: &str, option: &str) {
  let clean_cmd = sanitize_error_str(cmd_name, MAX_PARAM_NAME_LEN);
  let clean_opt = sanitize_error_str(option, MAX_PARAM_NAME_LEN);
  output.extend_from_slice(b"-ERR Syntax error in ");
  output.extend_from_slice(clean_cmd.as_bytes());
  output.extend_from_slice(b" option '");
  output.extend_from_slice(clean_opt.as_bytes());
  output.extend_from_slice(b"'\r\n");
}

/// 写出未知子命令错误应答：`-ERR unknown subcommand '<sub_command>'. Try <cmd_name> HELP\r\n`（零堆分配）
#[inline]
pub fn abort_with_unknown_subcommand(output: &mut Vec<u8>, sub_command: &str, cmd_name: &str) {
  let clean_sub = sanitize_error_str(sub_command, MAX_PARAM_NAME_LEN);
  let clean_cmd = sanitize_error_str(cmd_name, MAX_PARAM_NAME_LEN);
  output.extend_from_slice(b"-ERR unknown subcommand '");
  output.extend_from_slice(clean_sub.as_bytes());
  output.extend_from_slice(b"'. Try ");
  output.extend_from_slice(clean_cmd.as_bytes());
  output.extend_from_slice(b" HELP\r\n");
}

/// 写出未知子命令或错误参数数量应答（零堆分配）
#[inline]
pub fn abort_with_unknown_subcommand_or_wrong_num_args(
  output: &mut Vec<u8>,
  sub_command: &str,
  cmd_name: &str,
) {
  let clean_sub = sanitize_error_str(sub_command, MAX_PARAM_NAME_LEN);
  let clean_cmd = sanitize_error_str(cmd_name, MAX_PARAM_NAME_LEN);
  output.extend_from_slice(b"-ERR unknown subcommand or wrong number of arguments for '");
  output.extend_from_slice(clean_sub.as_bytes());
  output.extend_from_slice(b"'. Try ");
  output.extend_from_slice(clean_cmd.as_bytes());
  output.extend_from_slice(b" HELP\r\n");
}

/// 原样写出错误应答并终止命令处理
pub fn abort_with_error_message(output: &mut Vec<u8>, error_message: &str) {
  write_error_raw(output, error_message);
}

/// 以 `+PONG\r\n` 等已含类型前缀的原始字节帧写出应答
#[inline]
pub fn write_raw(output: &mut Vec<u8>, frame: &[u8]) {
  output.extend_from_slice(frame);
}

/// RESP2 口径写 map 头（RESP2 分支：map 退化为双倍长度数组）
#[inline]
pub fn write_map_len_resp2(output: &mut Vec<u8>, len: usize) {
  RespWriter::new_ref(output).write_map_length(len);
}

/// 运行时按会话协议版本写 map 头（对标 RespServerSessionOutput.cs:WriteMapLength：
/// RESP3 写 `%<len>\r\n`，RESP2 退化为双倍长度数组 `*<2len>\r\n`）
#[inline]
pub fn write_map_len(output: &mut Vec<u8>, len: usize, resp_protocol_version: u8) {
  if resp_protocol_version >= 3 {
    RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(output).write_map_length(len);
  } else {
    write_map_len_resp2(output, len);
  }
}

/// 运行时按会话协议版本写 set 头（对标 RespServerSessionOutput.cs:WriteSetLength：
/// RESP3 写 `~<len>\r\n`，RESP2 退化为数组 `*<len>\r\n`）
#[inline]
pub fn write_set_len(output: &mut Vec<u8>, len: usize, resp_protocol_version: u8) {
  if resp_protocol_version >= 3 {
    RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(output).write_set_length(len);
  } else {
    RespWriter::new_ref(output).write_set_length(len);
  }
}

/// 运行时按会话协议版本写浮点数值（对标 RespMemoryWriter.cs:WriteDoubleNumeric：
/// RESP3 写 `,val\r\n`，RESP2 降级为 bulk string）
#[inline]
pub fn write_double_numeric(output: &mut Vec<u8>, value: f64, resp_protocol_version: u8) {
  if resp_protocol_version >= 3 {
    RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(output).write_double_numeric(value);
  } else {
    RespWriter::new_ref(output).write_double_numeric(value);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::ext::RespVecExt;

  #[test]
  fn write_error_raw_frames_message() {
    let mut out = Vec::new();
    write_error_raw(&mut out, RESP_ERR_GENERIC_NOSUCHKEY);
    assert_eq!(out, b"-ERR no such key\r\n");
  }

  #[test]
  fn write_error_raw_noperm() {
    let mut out = Vec::new();
    write_error_raw(&mut out, RESP_ERR_NOPERM);
    assert_eq!(
      out,
      b"-NOPERM this user has no permissions to run the command\r\n"
    );
  }

  #[test]
  fn write_error_raw_sanitizes_crlf() {
    let mut out = Vec::new();
    write_error_raw(&mut out, "ERR error with \r\ninjection");
    assert_eq!(out, b"-ERR error with \r\n");
  }

  #[test]
  fn wrong_num_args_formats_command_name() {
    let mut out = Vec::new();
    abort_with_wrong_number_of_arguments(&mut out, "GETEX");
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'GETEX' command\r\n"
    );
  }

  #[test]
  fn map_len_resp2_doubles_array_length() {
    let mut out = Vec::new();
    write_map_len_resp2(&mut out, 2);
    assert_eq!(out, b"*4\r\n");
  }

  #[test]
  fn map_len_dispatches_by_protocol_version() {
    // RespServerSessionOutput.cs:WriteMapLength：RESP3 写 %，RESP2 双倍数组
    let mut out = Vec::new();
    write_map_len(&mut out, 3, 3);
    assert_eq!(out, b"%3\r\n");

    let mut out = Vec::new();
    write_map_len(&mut out, 3, 2);
    assert_eq!(out, b"*6\r\n");
  }

  #[test]
  fn raw_frames_passthrough() {
    let mut out = Vec::new();
    write_raw(&mut out, RESP_OK);
    write_raw(&mut out, RESP_RETURN_VAL_N2);
    assert_eq!(out, b"+OK\r\n:-2\r\n");
  }

  #[test]
  fn pubsub_command_disabled_fills_template() {
    let mut out = Vec::new();
    abort_with_pubsub_command_disabled(&mut out, "PUBLISH");
    assert_eq!(
      out,
      b"-ERR PUBLISH is disabled, enable it with --pubsub option.\r\n"
    );

    let mut out = Vec::new();
    abort_with_pubsub_command_disabled(&mut out, "PUBSUB NUMPAT");
    assert_eq!(
      out,
      b"-ERR PUBSUB NUMPAT is disabled, enable it with --pubsub option.\r\n"
    );

    // 命令名经 sanitize 截断，杜绝 CRLF 注入
    let mut out = Vec::new();
    abort_with_pubsub_command_disabled(&mut out, "SUBSCRIBE\r\nINJECT");
    assert_eq!(
      out,
      b"-ERR SUBSCRIBE is disabled, enable it with --pubsub option.\r\n"
    );
  }

  #[test]
  fn pubsub_frame_prefixes_match_runtime_writers() {
    // 编译期整帧与逐段写出必须同字节（帧头 = 数组/push 头 + 名字批量串）
    let ack = |arity: usize, name: &[u8]| {
      let mut out = Vec::new();
      let mut w = out.resp_writer2();
      w.write_array_length(arity);
      w.write_bulk_string(name);
      out
    };
    let push = |arity: usize, name: &[u8]| {
      let mut out = Vec::new();
      let mut w = out.resp_writer3();
      w.write_push_length(arity);
      w.write_bulk_string(name);
      out
    };

    assert_eq!(PUBSUB_SUBSCRIBE_FRAME_PREFIX, ack(3, b"subscribe"));
    assert_eq!(PUBSUB_SSUBSCRIBE_FRAME_PREFIX, ack(3, b"ssubscribe"));
    assert_eq!(PUBSUB_PSUBSCRIBE_FRAME_PREFIX, ack(3, b"psubscribe"));
    assert_eq!(PUBSUB_UNSUBSCRIBE_FRAME_PREFIX, ack(3, b"unsubscribe"));
    assert_eq!(PUBSUB_SUNSUBSCRIBE_FRAME_PREFIX, ack(3, b"sunsubscribe"));
    assert_eq!(PUBSUB_PUNSUBSCRIBE_FRAME_PREFIX, ack(3, b"punsubscribe"));

    assert_eq!(PUBSUB_PUSH_MSG_PREFIX_RESP2, ack(3, b"message"));
    assert_eq!(PUBSUB_PUSH_PMSG_PREFIX_RESP2, ack(4, b"pmessage"));
    assert_eq!(PUBSUB_PUSH_SMSG_PREFIX_RESP2, ack(3, b"smessage"));
    assert_eq!(PUBSUB_PUSH_MSG_PREFIX_RESP3, push(3, b"message"));
    assert_eq!(PUBSUB_PUSH_PMSG_PREFIX_RESP3, push(4, b"pmessage"));
    assert_eq!(PUBSUB_PUSH_SMSG_PREFIX_RESP3, push(3, b"smessage"));

    // RESP2 侧 push 头降级为数组头
    let mut out = Vec::new();
    out.resp_writer2().write_push_length(4);
    assert_eq!(out, b"*4\r\n");
  }
}
