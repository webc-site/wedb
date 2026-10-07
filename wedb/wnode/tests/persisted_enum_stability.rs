#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 持久化敏感枚举稳定锁表（集成）
//!
//! 对位 C# 守卫测试: test/standalone/Garnet.test/PersistedEnumStabilityTests.cs
//! （落盘数值域另锚: libs/server/Resp/Parser/RespCommand.cs、libs/server/Objects/*/*Object.cs、
//! libs/server/AOF/AofEntryType.cs、libs/server/AOF/AofHeader.cs）
//!
//! 守卫口径与 C# 一致：这些判别值落 AOF、检查点与副本流，改一个已存在成员的数值
//! 即静默毁掉旧版本写入的数据，故黄金快照只许追加、不许改号、不许填洞。若因持久化
//! 格式演进确需改号，须走 AOF 版本门（[`waof::AofHeader::AOF_FORMAT_VERSION`]）换代，
//! 而非修改本文件的期望值。
//!
//! 与 C# 的登记差分（刻意，非漂移）：
//! - [`HashOperation`] 无 HLEN（洞位 9）：rust HLEN 走信封计数快路径
//!   （wnode rmw_helpers / object_store_utils），子操作面无此成员，9 须保持为洞；
//! - [`GarnetObjectType`] 多 `RangeIndex = 5`（transpile「类型枚举」条款自定义项，
//!   C# 侧 RangeIndex 是独立存储形态不入对象类型枚举）；
//! - [`AofEntryType`] 无 C# 流式 checkpoint 四连 0x40..=0x43（本仓统一 checkpoint
//!   0x30/0x32 承接），多 `FlushNs = 0x62`、统一存储四连 0x70..=0x73 与
//!   `RangeIndexStreamChunk = 0x80`（rust 自有扩展）；
//! - C# `LegacyRespCommand` v3 重映射层与 `LegacyCustomObjectTypeIdFailsFastOnDeserialize`
//!   无 rust 对位：本仓 AOF 版本门按等值拒绝旧代际文件（版本字节高位置 1，与 C#
//!   1..=5 版本域永不重叠），不存在 v3 重映射层，故不设对应测试。
//!
//! 黄金值硬编于此、自身即事实源：不读 garnet 目录（该目录被 gitignore，CI 工作树里
//! 不存在）。与 wresp crate 内 golden_values 单元测试互为双保险——crate 内表防改号，
//! 本集成测试防「顺手把单元测试迁就新号」；两侧表必须逐条同值。

use waof::{AofEntryType, AofHeader, AofHeaderType};
use wcol::{HashOperation, ListOperation, SetOperation, SortedSetOperation};
use wresp::command::{LAST_VALID_COMMAND, RespCommand};
use wval::{
  CUSTOM_OBJECT_TYPE_BASE, CustomObjectType, GarnetObjectType, LAST_RESERVED_BUILTIN_TYPE,
};

/// 写块闭区间（C# FirstWriteCommand..=LastWriteCommand = APPEND..=BITOP_DIFF，
/// 唯一落盘的 RespCommand 连续块）
const WRITE_BLOCK: (u16, u16) = (1, 120);
/// 写块黄金表登记条数（块稠密无洞的计数锚）
const WRITE_BLOCK_ENTRY_COUNT: usize = 120;
/// 哨兵判别值
const NONE_VALUE: u16 = 0;
const INVALID_VALUE: u16 = 65535;
/// 内置命令空间与自定义命令段的隔离距离（C# `INVALID - 256` 判据）
const CUSTOM_RANGE_GUARD_GAP: u16 = 256;
/// [`HashOperation`] 登记洞位（C# HLEN = 9；rust 无此成员，洞须保持为洞）
const HASH_OP_HLEN_HOLE: u8 = 9;
/// AOF 格式版本字节（落盘头第 0 字节，版本门等值判定单值；与 C# 1..=5 版本域刻意异号）
const AOF_FORMAT_VERSION: u8 = 0x80;
/// [`AofEntryType`] 登记洞位探针：StoreDelete 与 ObjectStoreUpsert 间隙、
/// 块间隙、C# 流式 checkpoint 四连（rust 无成员）与尾部未占用字节
const AOF_ENTRY_TYPE_HOLES: &[u8] = &[0x03, 0x0f, 0x40, 0x43, 0xfe];
/// [`AofHeaderType`] 未占用探针：3 位类型段的高位两值
const AOF_HEADER_TYPE_HOLES: &[u8] = &[6, 7];

/// 锁表单点：逐条钉判别值，并经 `from_repr` 按数值回读还原
/// （改号、复用既有值、成员被顶替均在此红）
macro_rules! lock_table {
  ($table:expr, $enum:ty) => {
    for (op, expected) in $table {
      assert_eq!(
        *op as u8, *expected,
        "落盘判别值漂移:{op:?} 应为 {expected}"
      );
      assert_eq!(
        <$enum>::from_repr(*expected),
        Some(*op),
        "判别值 {expected} 位被其他成员顶替:{op:?}"
      );
    }
  };
}

/// 写块黄金表：C# ExpectedWriteCommandValues 逐值对位，全量 120 条（唯一落盘块）。
/// 63/64 洞位与 C# 同值回填为 1:1 占位成员 RIPROMOTE/RIRESTORE（ACL 全枚举成员
/// 判定单源所需），全仓无分发臂、日志恒不产该判别值。
const WRITE_BLOCK_GOLDEN: &[(RespCommand, u16)] = &[
  (RespCommand::Append, 1),
  (RespCommand::Bitfield, 2),
  (RespCommand::Bzmpop, 3),
  (RespCommand::Bzpopmax, 4),
  (RespCommand::Bzpopmin, 5),
  (RespCommand::Decr, 6),
  (RespCommand::Decrby, 7),
  (RespCommand::Del, 8),
  (RespCommand::Delifexpim, 9),
  (RespCommand::Delifgreater, 10),
  (RespCommand::Expire, 11),
  (RespCommand::Expireat, 12),
  (RespCommand::Flushall, 13),
  (RespCommand::Flushdb, 14),
  (RespCommand::Geoadd, 15),
  (RespCommand::Georadius, 16),
  (RespCommand::Georadiusbymember, 17),
  (RespCommand::Geosearchstore, 18),
  (RespCommand::Getdel, 19),
  (RespCommand::Getex, 20),
  (RespCommand::Getset, 21),
  (RespCommand::Hcollect, 22),
  (RespCommand::Hdel, 23),
  (RespCommand::Hexpire, 24),
  (RespCommand::Hpexpire, 25),
  (RespCommand::Hexpireat, 26),
  (RespCommand::Hpexpireat, 27),
  (RespCommand::Hpersist, 28),
  (RespCommand::Hincrby, 29),
  (RespCommand::Hincrbyfloat, 30),
  (RespCommand::Hmset, 31),
  (RespCommand::Hset, 32),
  (RespCommand::Hsetnx, 33),
  (RespCommand::Incr, 34),
  (RespCommand::Incrby, 35),
  (RespCommand::Incrbyfloat, 36),
  (RespCommand::Linsert, 37),
  (RespCommand::Lmove, 38),
  (RespCommand::Lmpop, 39),
  (RespCommand::Lpop, 40),
  (RespCommand::Lpush, 41),
  (RespCommand::Lpushx, 42),
  (RespCommand::Lrem, 43),
  (RespCommand::Lset, 44),
  (RespCommand::Ltrim, 45),
  (RespCommand::Blpop, 46),
  (RespCommand::Brpop, 47),
  (RespCommand::Blmove, 48),
  (RespCommand::Brpoplpush, 49),
  (RespCommand::Blmpop, 50),
  (RespCommand::Migrate, 51),
  (RespCommand::Mset, 52),
  (RespCommand::Msetnx, 53),
  (RespCommand::Persist, 54),
  (RespCommand::Pexpire, 55),
  (RespCommand::Pexpireat, 56),
  (RespCommand::Pfadd, 57),
  (RespCommand::Pfmerge, 58),
  (RespCommand::Psetex, 59),
  (RespCommand::Rename, 60),
  (RespCommand::Ricreate, 61),
  (RespCommand::Ridel, 62),
  (RespCommand::Ripromote, 63),
  (RespCommand::Rirestore, 64),
  (RespCommand::Riset, 65),
  (RespCommand::Restore, 66),
  (RespCommand::Renamenx, 67),
  (RespCommand::Rpop, 68),
  (RespCommand::Rpoplpush, 69),
  (RespCommand::Rpush, 70),
  (RespCommand::Rpushx, 71),
  (RespCommand::Sadd, 72),
  (RespCommand::Sdiffstore, 73),
  (RespCommand::Set, 74),
  (RespCommand::Setbit, 75),
  (RespCommand::Setex, 76),
  (RespCommand::Setexnx, 77),
  (RespCommand::Setexxx, 78),
  (RespCommand::Setnx, 79),
  (RespCommand::Setifmatch, 80),
  (RespCommand::Setifgreater, 81),
  (RespCommand::Setwithetag, 82),
  (RespCommand::Setkeepttl, 83),
  (RespCommand::Setkeepttlxx, 84),
  (RespCommand::Setrange, 85),
  (RespCommand::Sinterstore, 86),
  (RespCommand::Smove, 87),
  (RespCommand::Spop, 88),
  (RespCommand::Srem, 89),
  (RespCommand::Sunionstore, 90),
  (RespCommand::Swapdb, 91),
  (RespCommand::Unlink, 92),
  (RespCommand::Vadd, 93),
  (RespCommand::Vrem, 94),
  (RespCommand::Vsetattr, 95),
  (RespCommand::Zadd, 96),
  (RespCommand::Zcollect, 97),
  (RespCommand::Zdiffstore, 98),
  (RespCommand::Zexpire, 99),
  (RespCommand::Zpexpire, 100),
  (RespCommand::Zexpireat, 101),
  (RespCommand::Zpexpireat, 102),
  (RespCommand::Zpersist, 103),
  (RespCommand::Zincrby, 104),
  (RespCommand::Zmpop, 105),
  (RespCommand::Zinterstore, 106),
  (RespCommand::Zpopmax, 107),
  (RespCommand::Zpopmin, 108),
  (RespCommand::Zrangestore, 109),
  (RespCommand::Zrem, 110),
  (RespCommand::Zremrangebylex, 111),
  (RespCommand::Zremrangebyrank, 112),
  (RespCommand::Zremrangebyscore, 113),
  (RespCommand::Zunionstore, 114),
  (RespCommand::Bitop, 115),
  (RespCommand::BitopAnd, 116),
  (RespCommand::BitopOr, 117),
  (RespCommand::BitopXor, 118),
  (RespCommand::BitopNot, 119),
  (RespCommand::BitopDiff, 120),
];

/// [`HashOperation`] 黄金表（C# ExpectedHashOps 对位；9 为登记洞位，见文件头差分登记）
const HASH_OP_GOLDEN: &[(HashOperation, u8)] = &[
  (HashOperation::Hcollect, 0),
  (HashOperation::Hexpire, 1),
  (HashOperation::Httl, 2),
  (HashOperation::Hpersist, 3),
  (HashOperation::Hget, 4),
  (HashOperation::Hmget, 5),
  (HashOperation::Hset, 6),
  (HashOperation::Hmset, 7),
  (HashOperation::Hsetnx, 8),
  (HashOperation::Hdel, 10),
  (HashOperation::Hexists, 11),
  (HashOperation::Hgetall, 12),
  (HashOperation::Hkeys, 13),
  (HashOperation::Hvals, 14),
  (HashOperation::Hincrby, 15),
  (HashOperation::Hincrbyfloat, 16),
  (HashOperation::Hrandfield, 17),
  (HashOperation::Hscan, 18),
  (HashOperation::Hstrlen, 19),
];

/// [`SortedSetOperation`] 黄金表（C# ExpectedSortedSetOps 逐值对位，0..=27 与 C# 全同）
const SORTED_SET_OP_GOLDEN: &[(SortedSetOperation, u8)] = &[
  (SortedSetOperation::Zadd, 0),
  (SortedSetOperation::Zcard, 1),
  (SortedSetOperation::Zpopmax, 2),
  (SortedSetOperation::Zscore, 3),
  (SortedSetOperation::Zrem, 4),
  (SortedSetOperation::Zcount, 5),
  (SortedSetOperation::Zincrby, 6),
  (SortedSetOperation::Zrank, 7),
  (SortedSetOperation::Zrange, 8),
  (SortedSetOperation::Geoadd, 9),
  (SortedSetOperation::Geohash, 10),
  (SortedSetOperation::Geodist, 11),
  (SortedSetOperation::Geopos, 12),
  (SortedSetOperation::Geosearch, 13),
  (SortedSetOperation::Zrevrank, 14),
  (SortedSetOperation::Zremrangebylex, 15),
  (SortedSetOperation::Zremrangebyrank, 16),
  (SortedSetOperation::Zremrangebyscore, 17),
  (SortedSetOperation::Zlexcount, 18),
  (SortedSetOperation::Zpopmin, 19),
  (SortedSetOperation::Zrandmember, 20),
  (SortedSetOperation::Zdiff, 21),
  (SortedSetOperation::Zscan, 22),
  (SortedSetOperation::Zmscore, 23),
  (SortedSetOperation::Zexpire, 24),
  (SortedSetOperation::Zttl, 25),
  (SortedSetOperation::Zpersist, 26),
  (SortedSetOperation::Zcollect, 27),
];

/// [`ListOperation`] 黄金表（C# ExpectedListOps 逐值对位，0..=17 与 C# 全同）
const LIST_OP_GOLDEN: &[(ListOperation, u8)] = &[
  (ListOperation::Lpop, 0),
  (ListOperation::Lpush, 1),
  (ListOperation::Lpushx, 2),
  (ListOperation::Rpop, 3),
  (ListOperation::Rpush, 4),
  (ListOperation::Rpushx, 5),
  (ListOperation::Llen, 6),
  (ListOperation::Ltrim, 7),
  (ListOperation::Lrange, 8),
  (ListOperation::Lindex, 9),
  (ListOperation::Linsert, 10),
  (ListOperation::Lrem, 11),
  (ListOperation::Rpoplpush, 12),
  (ListOperation::Lmove, 13),
  (ListOperation::Lset, 14),
  (ListOperation::Brpop, 15),
  (ListOperation::Blpop, 16),
  (ListOperation::Lpos, 17),
];

/// [`SetOperation`] 黄金表（C# ExpectedSetOps 逐值对位，0..=15 与 C# 全同）
const SET_OP_GOLDEN: &[(SetOperation, u8)] = &[
  (SetOperation::Sadd, 0),
  (SetOperation::Srem, 1),
  (SetOperation::Spop, 2),
  (SetOperation::Smembers, 3),
  (SetOperation::Scard, 4),
  (SetOperation::Sscan, 5),
  (SetOperation::Smove, 6),
  (SetOperation::Srandmember, 7),
  (SetOperation::Sismember, 8),
  (SetOperation::Smismember, 9),
  (SetOperation::Sunion, 10),
  (SetOperation::Sunionstore, 11),
  (SetOperation::Sdiff, 12),
  (SetOperation::Sdiffstore, 13),
  (SetOperation::Sinter, 14),
  (SetOperation::Sinterstore, 15),
];

/// [`AofEntryType`] 黄金表（判别值落 AOF 条目头 op_type 字节，全量 20 变体；
/// C# 对位 libs/server/AOF/AofEntryType.cs，差分见文件头登记）
const AOF_ENTRY_TYPE_GOLDEN: &[(AofEntryType, u8)] = &[
  (AofEntryType::StoreUpsert, 0x00),
  (AofEntryType::StoreRMW, 0x01),
  (AofEntryType::StoreDelete, 0x02),
  (AofEntryType::ObjectStoreUpsert, 0x10),
  (AofEntryType::ObjectStoreRMW, 0x11),
  (AofEntryType::ObjectStoreDelete, 0x12),
  (AofEntryType::TxnStart, 0x20),
  (AofEntryType::TxnCommit, 0x21),
  (AofEntryType::TxnAbort, 0x22),
  (AofEntryType::CheckpointStartCommit, 0x30),
  (AofEntryType::CheckpointEndCommit, 0x32),
  (AofEntryType::StoredProcedure, 0x50),
  (AofEntryType::FlushAll, 0x60),
  (AofEntryType::FlushDb, 0x61),
  (AofEntryType::FlushNs, 0x62),
  (AofEntryType::UnifiedStoreStringUpsert, 0x70),
  (AofEntryType::UnifiedStoreObjectUpsert, 0x71),
  (AofEntryType::UnifiedStoreRMW, 0x72),
  (AofEntryType::UnifiedStoreDelete, 0x73),
  (AofEntryType::RangeIndexStreamChunk, 0x80),
];

/// [`AofHeaderType`] 黄金表（判别值落 AOF 头 flags 低 3 位，回放侧按它跳头；
/// C# 对位 libs/server/AOF/AofHeader.cs:AofHeaderType）
const AOF_HEADER_TYPE_GOLDEN: &[(AofHeaderType, u8)] = &[
  (AofHeaderType::BasicHeader, 0),
  (AofHeaderType::ShardedHeader, 1),
  (AofHeaderType::SingleLogTransactionHeader, 2),
  (AofHeaderType::ShardedLogTransactionHeader, 3),
  (AofHeaderType::BasicChunkHeader, 4),
  (AofHeaderType::ShardedChunkHeader, 5),
];

/// C# RespCommandWriteBlockValuesAreStable 对位：写块逐值锁定 + 首尾锚与哨兵
/// + 块稠密无洞。改号、插队、删员都必在此红。
#[test]
fn resp_command_write_block_values_are_stable() {
  for (cmd, expected) in WRITE_BLOCK_GOLDEN {
    assert_eq!(
      *cmd as u16, *expected,
      "落盘判别值漂移:{cmd:?} 应为 {expected}"
    );
    assert_eq!(
      RespCommand::from_repr(*expected),
      Some(*cmd),
      "写块位 {expected} 被非黄金成员占用"
    );
    assert!(
      (WRITE_BLOCK.0..=WRITE_BLOCK.1).contains(expected),
      "写块黄金表混入块外条目:{cmd:?} = {expected}"
    );
  }
  assert_eq!(
    WRITE_BLOCK_GOLDEN.len(),
    WRITE_BLOCK_ENTRY_COUNT,
    "写块黄金表条目数与登记计数不符"
  );

  // 哨兵与块界锚是持久契约，不随成员增减漂移（C# First/LastWriteCommand 锚同口径）
  assert_eq!(RespCommand::None as u16, NONE_VALUE, "None 哨兵必须是 0");
  assert_eq!(
    RespCommand::Invalid as u16,
    INVALID_VALUE,
    "Invalid 哨兵必须是 65535"
  );
  assert_eq!(
    RespCommand::Append as u16,
    WRITE_BLOCK.0,
    "写块首 APPEND 必须锚在 {}",
    WRITE_BLOCK.0
  );
  assert_eq!(
    RespCommand::BitopDiff as u16,
    WRITE_BLOCK.1,
    "写块尾 BITOP_DIFF 必须锚在 {}",
    WRITE_BLOCK.1
  );

  // 块稠密无洞：1..=120 每个落盘位都有成员占位（洞位 63/64 已按 C# 同值回填）
  for value in WRITE_BLOCK.0..=WRITE_BLOCK.1 {
    assert!(
      RespCommand::from_repr(value).is_some(),
      "写块位 {value} 是洞——落盘块不许出现空洞"
    );
  }
}

/// C# BuiltinRespCommandSpaceDoesNotReachCustomRange 对位：内置命令空间不得
/// 长进自定义命令段（INVALID - 256 之下封顶）。
#[test]
fn resp_command_builtin_space_clears_custom_range() {
  assert!(
    (LAST_VALID_COMMAND as u16) < INVALID_VALUE - CUSTOM_RANGE_GUARD_GAP,
    "内置命令空间已长进自定义命令段:LAST_VALID = {}",
    LAST_VALID_COMMAND as u16
  );
}

/// C# ObjectSubOpValuesAreStable + ObjectSubOpsFitInByte 对位：四对象子操作面
/// 逐值锁定。C# 的「subId 占协议头 1 字节，max <= 255」宽度断言在 rust 由
/// #[repr(u8)] 编译期保证，不设恒真宽度断言；运行时对应的强断言形态是
/// 「全变体按落盘字节回读还原」（lock_table 逐条校验）+ 登记洞位与上沿锁洞。
#[test]
fn object_sub_op_values_are_stable() {
  lock_table!(HASH_OP_GOLDEN, HashOperation);
  lock_table!(SORTED_SET_OP_GOLDEN, SortedSetOperation);
  lock_table!(LIST_OP_GOLDEN, ListOperation);
  lock_table!(SET_OP_GOLDEN, SetOperation);

  // 登记洞位（文件头差分登记）：C# HLEN = 9 在 rust 子操作面无成员，
  // 洞须保持为洞——填洞即顶替 AOF 已写记录的位段语义
  assert_eq!(
    HashOperation::from_repr(HASH_OP_HLEN_HOLE),
    None,
    "HashOperation 洞位 {} 被占用(C# HLEN 位,rust 刻意留洞)",
    HASH_OP_HLEN_HOLE
  );

  // 各自操作面最大值之上须是洞（锁上沿：追加成员只许占新位，不许复用块外旧值）
  assert_eq!(
    HashOperation::from_repr(HashOperation::Hstrlen as u8 + 1),
    None,
    "HashOperation 上沿之上须是洞"
  );
  assert_eq!(
    SortedSetOperation::from_repr(SortedSetOperation::Zcollect as u8 + 1),
    None,
    "SortedSetOperation 上沿之上须是洞"
  );
  assert_eq!(
    ListOperation::from_repr(ListOperation::Lpos as u8 + 1),
    None,
    "ListOperation 上沿之上须是洞"
  );
  assert_eq!(
    SetOperation::from_repr(SetOperation::Sinterstore as u8 + 1),
    None,
    "SetOperation 上沿之上须是洞"
  );
}

/// C# GarnetObjectTypeBuiltinValuesAreStable 对位：类型字节全量锁定
/// （信封 tag / WRONGTYPE / TYPE 应答共用该字节）。
#[test]
fn garnet_object_type_builtin_values_are_stable() {
  // C# 六成员逐条对位
  assert_eq!(GarnetObjectType::Null as u8, 0);
  assert_eq!(GarnetObjectType::SortedSet as u8, 1);
  assert_eq!(GarnetObjectType::List as u8, 2);
  assert_eq!(GarnetObjectType::Hash as u8, 3);
  assert_eq!(GarnetObjectType::Set as u8, 4);
  assert_eq!(GarnetObjectType::All as u8, 0xfb);
  // 登记差分（文件头）：transpile「类型枚举」条款自定义项
  assert_eq!(GarnetObjectType::RangeIndex as u8, 5);

  // 按数值回读还原（信封解码单点 from_u8 与声明双保险）
  for (t, v) in [
    (GarnetObjectType::Null, 0u8),
    (GarnetObjectType::SortedSet, 1),
    (GarnetObjectType::List, 2),
    (GarnetObjectType::Hash, 3),
    (GarnetObjectType::Set, 4),
    (GarnetObjectType::RangeIndex, 5),
    (GarnetObjectType::All, 0xfb),
  ] {
    assert_eq!(
      GarnetObjectType::from_u8(v),
      Some(t),
      "类型字节 {v} 回读失配"
    );
  }

  // 保留段与自定义段在类型字节空间须是洞：RangeIndex 之上、内置保留段顶、
  // 自定义段基址（由 CustomObjectType 承接）与 0xff 全部不得解析为内置类型
  for hole in [
    GarnetObjectType::RangeIndex as u8 + 1,
    LAST_RESERVED_BUILTIN_TYPE,
    CUSTOM_OBJECT_TYPE_BASE,
    0xff,
  ] {
    assert_eq!(
      GarnetObjectType::from_u8(hole),
      None,
      "类型字节 {hole:#x} 应为洞,不得被内置类型占用"
    );
  }

  // 自定义扩展对象标签分配单点（信封 tag 落盘值，C# CustomObjectTypeMinId 分配序对位）
  assert_eq!(CustomObjectType::Roaring as u8, CUSTOM_OBJECT_TYPE_BASE);
  assert_eq!(CustomObjectType::Json as u8, CUSTOM_OBJECT_TYPE_BASE + 1);
  assert_eq!(
    CustomObjectType::from_u8(CUSTOM_OBJECT_TYPE_BASE),
    Some(CustomObjectType::Roaring)
  );
  assert_eq!(
    CustomObjectType::from_u8(CUSTOM_OBJECT_TYPE_BASE + 1),
    Some(CustomObjectType::Json)
  );
  assert_eq!(
    CustomObjectType::from_u8(CUSTOM_OBJECT_TYPE_BASE + 2),
    None,
    "自定义扩展段已登记成员之上须是洞"
  );
}

/// AofEntryType 判别值全量锁定（任务点名面）：落 AOF 条目头 op_type 字节，
/// 回放按它分派负载形状，改号即旧日志静默误读。
#[test]
fn aof_entry_type_values_are_stable() {
  lock_table!(AOF_ENTRY_TYPE_GOLDEN, AofEntryType);

  // 回放链公开转换面（TryFrom<u8>）同步全量对拍
  for (t, v) in AOF_ENTRY_TYPE_GOLDEN {
    assert_eq!(
      AofEntryType::try_from(*v),
      Ok(*t),
      "AOF 条目类型 {v:#x} 转换失配"
    );
  }

  // 登记洞位：C# 流式 checkpoint 四连 0x40..=0x43 在 rust 无成员（统一 checkpoint
  // 0x30/0x32 承接），间隙与尾部未占用字节同锁——填洞即顶替旧日志位段语义
  for hole in AOF_ENTRY_TYPE_HOLES {
    assert_eq!(
      AofEntryType::try_from(*hole),
      Err(*hole),
      "AOF 条目类型洞位 {hole:#x} 被占用"
    );
  }
}

/// AofHeaderType 判别值与 AOF 版本字节锁定：头类型落 flags 低 3 位、版本落头
/// 第 0 字节，回放侧按等值版本门拒绝旧代际文件。
#[test]
fn aof_header_type_and_version_are_stable() {
  lock_table!(AOF_HEADER_TYPE_GOLDEN, AofHeaderType);

  // 3 位类型段高位两值须是洞（mask 之上不再有合法类型）
  for hole in AOF_HEADER_TYPE_HOLES {
    assert_eq!(
      AofHeaderType::from_repr(*hole),
      None,
      "AOF 头类型洞位 {hole} 被占用"
    );
  }

  // 版本字节（落盘头第 0 字节）：与 C# 1..=5 版本域刻意异号（高位置 1 永不
  // 重叠），跨仓文件与本仓旧代际文件靠它显式拒绝——改值即换格式代际，须
  // 连带重审版本门，不许顺手动
  assert_eq!(AofHeader::AOF_FORMAT_VERSION, AOF_FORMAT_VERSION);
}
