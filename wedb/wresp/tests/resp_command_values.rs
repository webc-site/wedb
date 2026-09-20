//! RespCommand 落盘判别值黄金快照。
//!
//! 判别值是持久值：`wnode/src/aof/replay_input.rs` 的 32 字节头以 `[cmd u16]` 原样落
//! AOF、落副本流、落 RangeIndex 复制三面，改一个数即改一条已写记录的语义；且读写属性、
//! 数据命令、免认证、集群子命令五处门禁按数值区间判定，重编号即静默错判。本文件把这些值钉成
//! 期望常量：判别值只允许追加，不允许改号，也不允许填洞。
//!
//! 对位 C# 守卫测试 test/standalone/Garnet.test/PersistedEnumStabilityTests.cs（黄金值硬编在
//! 本文件、自身即事实源：不读 garnet 目录——该目录被 gitignore，CI 工作树里不存在）。
//! 与 C# 的已知差分按名登记在 `resp_command_registered_diffs_are_as_documented`，
//! 即固化差分清单而非假装零差分。

use wresp::{
  catalog::is_no_auth,
  command::{
    FIRST_DATA_COMMAND, LAST_DATA_COMMAND, LAST_VALID_COMMAND, RespCommand, is_cluster_sub_command,
    is_data_command, is_read_only, is_write_only,
  },
};

/// 写块（唯一持久化块，C# FirstWriteCommand..=LastWriteCommand）。块界数值在此独立于枚举声明
/// 写死，改号必撞本文件锚点。
const WRITE_BLOCK: (u16, u16) = (1, 120);
/// 读块（C# FirstReadCommand..=LastReadCommand，对位 Bitcount..=Riscan）：块内允许重排，
/// 块序与块界不允许动。
const READ_BLOCK: (u16, u16) = (121, 227);
/// 脚本与异步三连（Eval/Evalsha/Async）：数据命令区间只到 Evalsha，Async 已越出其外。
const SCRIPT_BLOCK: (u16, u16) = (228, 230);
/// 管理与会话块（Ping..=Sunsubscribe）。
const ADMIN_BLOCK: (u16, u16) = (231, 370);
/// 数据命令上界数值（C# LastDataCommand = EVALSHA；SCRIPT_BLOCK 尾的 Async 已不属数据命令）。
const LAST_DATA_VALUE: u16 = 229;
/// CLUSTER 子命令连续区间（`is_cluster_sub_command` 的判据）。
const CLUSTER_SUB_BLOCK: (u16, u16) = (318, 366);
/// 免认证区间（C# IsNoAuth 的 AUTH..=QUIT；rust 追加的 SUNSUBSCRIBE 不在其内）。
const NO_AUTH_BLOCK: (u16, u16) = (367, 369);
/// 哨兵值。
const NONE_VALUE: u16 = 0;
const INVALID_VALUE: u16 = 65535;
/// 判别值成员总数：None + 写 118 + 读 107 + 脚本 3 + 管理 134 + Invalid。
const GOLDEN_MEMBER_COUNT: u16 = 364;

/// 写块黄金表：C# ExpectedWriteCommandValues 的逐值对位，减去本仓不设成员的洞位 63/64
/// （见 `resp_command_registered_diffs_are_as_documented`），共 118 条。
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

/// `is_data_command` 的排除表（C# 排除臂逐一对标）：区间判定之外的一类静默错判风险。
const DATA_COMMAND_EXCLUDED: &[RespCommand] = &[
  RespCommand::Migrate,
  RespCommand::Dbsize,
  RespCommand::MemoryUsage,
  RespCommand::Flushall,
  RespCommand::Flushdb,
  RespCommand::Keys,
  RespCommand::Scan,
  RespCommand::Swapdb,
];

fn in_band(value: u16, band: (u16, u16)) -> bool {
  band.0 <= value && value <= band.1
}

/// 判别值反查（洞位返回 `None`）。
fn command_at(value: u16) -> Option<RespCommand> {
  RespCommand::try_from(value).ok()
}

/// 写块逐值锁定 + 首尾锚与哨兵：改号、插队、删成员都必在此红。
#[test]
fn resp_command_write_block_values_are_stable() {
  for (cmd, expected) in WRITE_BLOCK_GOLDEN {
    assert_eq!(
      *cmd as u16, *expected,
      "落盘判别值漂移：{cmd:?} 应为 {expected}"
    );
    assert!(
      in_band(*expected, WRITE_BLOCK),
      "写块黄金表混入块外条目：{cmd:?} = {expected}"
    );
    assert_eq!(
      command_at(*expected),
      Some(*cmd),
      "写块位 {expected} 被非黄金成员占用"
    );
  }
  assert_eq!(
    WRITE_BLOCK_GOLDEN.len(),
    118,
    "写块黄金表条目数与登记计数不符"
  );

  // 写块边界是持久契约，不随成员增减漂移。
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
  assert_eq!(
    FIRST_DATA_COMMAND as u16, WRITE_BLOCK.0,
    "数据命令下界必须仍是写块首"
  );
  assert_eq!(
    LAST_DATA_COMMAND as u16, LAST_DATA_VALUE,
    "数据命令上界必须仍是 EVALSHA（Async 不属数据命令）"
  );
  assert_eq!(
    LAST_VALID_COMMAND as u16, ADMIN_BLOCK.1,
    "最后有效命令必须仍是管理块尾"
  );
}

/// 判别空间普查（C# WriteBlockContainsExactlyWriteCommands 的 rust 形态）：写块稠密且不含
/// 非写命令、各块成员数与黄金计数一致、内置空间在登记块尾封顶——新增命令只能追加到块尾之后，
/// 且必须同步本文件（块尾锚 `LAST_VALID_COMMAND` 与总数任一漂移即红）。
#[test]
fn resp_command_discriminant_space_matches_golden_census() {
  let mut band_counts = [0u16; 4];
  let mut total = 0u16;
  for value in u16::MIN..=u16::MAX {
    if command_at(value).is_none() {
      continue;
    }
    total += 1;
    for (i, band) in [WRITE_BLOCK, READ_BLOCK, SCRIPT_BLOCK, ADMIN_BLOCK]
      .iter()
      .enumerate()
    {
      if in_band(value, *band) {
        band_counts[i] += 1;
      }
    }
  }
  assert_eq!(
    band_counts,
    [118, 107, 3, 134],
    "各块黄金成员数漂移（写/读/脚本/管理）"
  );
  assert_eq!(
    total, GOLDEN_MEMBER_COUNT,
    "判别值成员总数漂移（洞位被填，或块界外长出成员）"
  );
}

/// 数值区间门禁与登记块界一致：五处门禁错判不报错，故按全空间逐值对拍。
#[test]
fn resp_command_range_predicates_match_registered_blocks() {
  for (value, cmd) in (0..=ADMIN_BLOCK.1).filter_map(|v| command_at(v).map(|c| (v, c))) {
    assert_eq!(
      is_write_only(cmd),
      in_band(value, WRITE_BLOCK),
      "is_write_only 与写块界不符：{cmd:?} = {value}"
    );
    assert_eq!(
      is_read_only(cmd),
      in_band(value, READ_BLOCK),
      "is_read_only 与读块界不符：{cmd:?} = {value}"
    );
    assert_eq!(
      is_data_command(cmd),
      in_band(value, (WRITE_BLOCK.0, LAST_DATA_VALUE)) && !DATA_COMMAND_EXCLUDED.contains(&cmd),
      "is_data_command 与数据区间/排除表不符：{cmd:?} = {value}"
    );
    assert_eq!(
      is_no_auth(cmd),
      in_band(value, NO_AUTH_BLOCK),
      "is_no_auth 与免认证区间不符：{cmd:?} = {value}"
    );
    assert_eq!(
      is_cluster_sub_command(cmd),
      in_band(value, CLUSTER_SUB_BLOCK),
      "is_cluster_sub_command 与 CLUSTER 子命令区间不符：{cmd:?} = {value}"
    );
  }
  for sentinel in [RespCommand::None, RespCommand::Invalid] {
    assert!(
      !(is_write_only(sentinel) || is_read_only(sentinel) || is_data_command(sentinel)),
      "哨兵命令不得带读写/数据属性：{sentinel:?}"
    );
  }
}

/// 本仓与 C# 的已知差分清单，按名断言其确实成立：洞位保持为洞（顺手把空洞填上即改已写
/// 记录语义），rust 独有成员钉在登记位上。
#[test]
fn resp_command_registered_diffs_are_as_documented() {
  // 63/64：C# RIPROMOTE/RIRESTORE 是 MainStore 内部 RMW 僵尸命令，rust 由 wkv 元记录 RMW
  // 等价承接（见 command.rs 的 Delifexpim 注），不设枚举成员。
  for value in 63..=64 {
    assert_eq!(
      command_at(value),
      None,
      "{value} 是登记洞位（C# RIPROMOTE/RIRESTORE）"
    );
  }
  // 254..=256：C# MODULE / MODULE_LOADCS / REGISTERCS 未转写，故 MONITOR 之后跳 MULTI。
  for value in 254..=256 {
    assert_eq!(
      command_at(value),
      None,
      "{value} 是登记洞位（C# MODULE 族）"
    );
  }
  assert_eq!(RespCommand::Monitor as u16, 253);
  assert_eq!(RespCommand::Multi as u16, 257);

  // 275/276/278：C# CustomTxn / CustomRawStringCmd / CustomProcedure 是动态注册层
  // （模块 / REGISTERCS）解析期按运行时 id 回填的内部分派哨兵，随注册管理层按转写规范
  // 整删、留为登记洞位；扩展命令统一回填编译期静态清单命中的 CustomObjCmd（277）。
  for value in [275u16, 276, 278] {
    assert_eq!(
      command_at(value),
      None,
      "{value} 是登记洞位（C# 自定义命令动态注册哨兵）"
    );
  }
  assert_eq!(
    command_at(277),
    Some(RespCommand::Customobjcmd),
    "CustomObjCmd 静态哨兵位漂移"
  );

  // RI.COUNT 是本仓自定义扩展（别名 RI.LEN 在解析期归一到同一成员），钉在 222。
  assert_eq!(
    command_at(222),
    Some(RespCommand::Ricount),
    "RI.COUNT 追加位漂移"
  );

  // 尾三连 AUTH/HELLO/QUIT 为 C# 对位；SUNSUBSCRIBE 是 rust 补全 SSUBSCRIBE 配套退订的
  // 追加位，也是 LAST_VALID_COMMAND，且不入免认证区间。
  assert_eq!(RespCommand::Auth as u16, NO_AUTH_BLOCK.0);
  assert_eq!(RespCommand::Hello as u16, 368);
  assert_eq!(RespCommand::Quit as u16, NO_AUTH_BLOCK.1);
  assert_eq!(
    command_at(ADMIN_BLOCK.1),
    Some(RespCommand::Sunsubscribe),
    "SUNSUBSCRIBE 追加位漂移"
  );
  assert!(
    !is_no_auth(RespCommand::Sunsubscribe),
    "SUNSUBSCRIBE 不得进免认证区间"
  );
}
