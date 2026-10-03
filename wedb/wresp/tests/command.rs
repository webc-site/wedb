use wresp::{
  catalog::is_no_auth,
  command::{
    FIRST_DATA_COMMAND, FIRST_READ_COMMAND, LAST_DATA_COMMAND, LAST_READ_COMMAND,
    LAST_VALID_COMMAND, RespCommand, is_cluster_sub_command, is_data_command,
    is_legal_on_vector_set, is_read_only, is_vector_gate_exempt, is_write_only,
    vector_gate_fixed_key_count, vector_gate_numkeys_form, vector_gate_scan_all_keys,
  },
};

/// 值域 WRONGTYPE 门接管判据单源：数据命令 ∧ 非白名单 ∧ 非豁免（本测试四处复用）
fn gated(cmd: RespCommand) -> bool {
  is_data_command(cmd) && !is_legal_on_vector_set(cmd) && !is_vector_gate_exempt(cmd)
}

/// 向量写门适用集穷举覆盖（task/ing/vector-registry-write-gate-coverage.md 六.1）：
/// 遍历 RespCommand 全枚举，钉死「白名单」与「值域门豁免集」互斥，且新增命令不落
/// 豁免清单即被 args[0] 值域门接管（数据命令默认拒），杜绝第二套事实漂移。
#[test]
fn vector_write_gate_applicability_is_exhaustive() {
  for raw in 0u16..=(LAST_VALID_COMMAND as u16) {
    let Some(cmd) = RespCommand::from_repr(raw) else {
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
      assert!(gated(cmd), "多键清单命令未被值域门接管: {cmd:?}");
      // 全扫清单与固定键位 / numkeys 形清单互斥：双列即两套事实。
      assert!(
        vector_gate_fixed_key_count(cmd).is_none() && !vector_gate_numkeys_form(cmd),
        "命令同时落全扫与另两键位清单: {cmd:?}"
      );
    }
    // 固定键位双键命令（LCS）同样须被值域门接管，否则键位探针分支悬空。
    if vector_gate_fixed_key_count(cmd).is_some() {
      assert!(gated(cmd), "固定键位命令未被值域门接管: {cmd:?}");
      assert!(
        !vector_gate_numkeys_form(cmd),
        "命令同时落固定键位与 numkeys 形清单: {cmd:?}"
      );
    }
    // numkeys 形命令（SINTERCARD/ZINTERCARD，票 zcode-r157c-sintercard）须被
    // 值域门接管且与另两清单互斥——键位在 args[1..]，落清单者走 numkeys 形臂，
    // 未落清单的新 numkeys 形命令由通用 args[0] 门接管即空探针，须在此显名钉死。
    if vector_gate_numkeys_form(cmd) {
      assert!(gated(cmd), "numkeys 形命令未被值域门接管: {cmd:?}");
      assert!(
        !vector_gate_scan_all_keys(cmd) && vector_gate_fixed_key_count(cmd).is_none(),
        "命令同时落 numkeys 形与另两键位清单: {cmd:?}"
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
    assert!(gated(cmd), "{cmd:?} 应受 args[0] 值域 WRONGTYPE 门约束");
  }
  // LCS：次键位向量登记须整命令拒（C# LCSInternal 双 GET 任一 WRONGTYPE，
  // VectorSetWrongTypeTests.cs 次键位全库唯一硬测），走固定键位双臂而非
  // args[0] 单键门，也不入全扫清单（选项 token 会被误检为键）。
  assert!(
    gated(RespCommand::Lcs)
      && vector_gate_fixed_key_count(RespCommand::Lcs) == Some(2)
      && !vector_gate_scan_all_keys(RespCommand::Lcs),
    "LCS 应受固定两键位值域 WRONGTYPE 门约束"
  );
  // SINTERCARD/ZINTERCARD：numkeys 形键段整体在 args[1..]，args[0] 恒数值 token
  // 非键位（C# Slice(1, nKeys) 泛型 WRONGTYPE，票 zcode-r157c-sintercard），
  // 通用 args[0] 门对其系空探针——须落 numkeys 形单源清单，与另两清单互斥。
  for cmd in [RespCommand::Sintercard, RespCommand::Zintercard] {
    assert!(
      gated(cmd)
        && vector_gate_numkeys_form(cmd)
        && !vector_gate_scan_all_keys(cmd)
        && vector_gate_fixed_key_count(cmd).is_none(),
      "{cmd:?} 应受 numkeys 形键段值域 WRONGTYPE 门约束"
    );
  }
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

mod golden_values {
  use super::*;

  /// 写块（唯一持久化块，C# FirstWriteCommand..=LastWriteCommand）。块界数值在此独立于枚举声明
  /// 写死，改号必撞本文件锚点。
  const WRITE_BLOCK: (u16, u16) = (1, 120);
  /// 读块（C# FirstReadCommand..=LastReadCommand，对位 Bitcount..=Riscan）：块内允许重排，
  /// 块序与块界不允许动。
  const READ_BLOCK: (u16, u16) = (121, 227);
  /// 脚本与异步三连（Eval/Evalsha/Async）：数据命令区间只到 Evalsha，Async 已越出其外。
  const SCRIPT_BLOCK: (u16, u16) = (228, 230);
  /// 管理与会话块（Ping..=PubsubShardnumsub）。
  const ADMIN_BLOCK: (u16, u16) = (231, 372);
  /// 数据命令上界数值（C# LastDataCommand = EVALSHA；SCRIPT_BLOCK 尾的 Async 已不属数据命令）。
  const LAST_DATA_VALUE: u16 = 229;
  /// CLUSTER 子命令连续区间（`is_cluster_sub_command` 的判据）。
  const CLUSTER_SUB_BLOCK: (u16, u16) = (318, 366);
  /// 免认证区间（C# IsNoAuth 的 AUTH..=QUIT；rust 追加的 SUNSUBSCRIBE 不在其内）。
  const NO_AUTH_BLOCK: (u16, u16) = (367, 369);
  /// 哨兵值。
  const NONE_VALUE: u16 = 0;
  const INVALID_VALUE: u16 = 65535;
  /// 判别值成员总数：None + 写 120 + 读 107 + 脚本 3 + 管理 136 + Invalid。
  const GOLDEN_MEMBER_COUNT: u16 = 368;

  /// 写块黄金表：C# ExpectedWriteCommandValues 逐值对位全量 120 条——63/64
  /// 洞位已按 C# 同值回填为 1:1 占位成员 RIPROMOTE/RIRESTORE（ACL 全枚举成员
  /// 判定单源所需，见 `resp_command_registered_diffs_are_as_documented`）。
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
    RespCommand::from_repr(value)
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
      120,
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
      [120, 107, 3, 136],
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

  /// 本仓与 C# 的已知差分清单，按名断言其确实成立：登记洞位保持为洞（顺手把
  /// 已写记录位段的洞误填即改语义；仅按 C# 同值回填并在此登记的除外），rust
  /// 独有成员钉在登记位上。
  #[test]
  fn resp_command_registered_diffs_are_as_documented() {
    // 63/64：C# RIPROMOTE/RIRESTORE 是 MainStore 内部 RMW 僵尸命令，运行语义由
    // wkv 元记录 RMW 等价承接（见 command.rs 的 Ripromote 注），成员已按 C#
    // 同值回填为 1:1 占位并在此登记：ACL 命令名解析须全枚举成员判定单源
    //（C# Enum.TryParse 全成员，目录零条目名在 User.AddCommand 查目录失配
    // 失败关闭，garnet/libs/server/ACL/User.cs:189/:318），与 277 CustomObjCmd
    // 回填形同理。全仓无分发臂，本仓日志恒不产 63/64 判别值。
    assert_eq!(
      command_at(63),
      Some(RespCommand::Ripromote),
      "RIPROMOTE 回填位漂移"
    );
    assert_eq!(
      command_at(64),
      Some(RespCommand::Rirestore),
      "RIRESTORE 回填位漂移"
    );
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

    // 尾三连 AUTH/HELLO/QUIT 为 C# 对位；SUNSUBSCRIBE 与分片域查询族
    // PUBSUB_SHARDCHANNELS/PUBSUB_SHARDNUMSUB 为 rust 追加位（C# 无对位），
    // 尾位即 LAST_VALID_COMMAND，且均不入免认证区间。
    assert_eq!(RespCommand::Auth as u16, NO_AUTH_BLOCK.0);
    assert_eq!(RespCommand::Hello as u16, 368);
    assert_eq!(RespCommand::Quit as u16, NO_AUTH_BLOCK.1);
    assert_eq!(
      RespCommand::Sunsubscribe as u16,
      ADMIN_BLOCK.1 - 2,
      "SUNSUBSCRIBE 追加位漂移"
    );
    assert_eq!(
      command_at(ADMIN_BLOCK.1),
      Some(RespCommand::PubsubShardnumsub),
      "PUBSUB_SHARDNUMSUB 追加位漂移"
    );
    assert!(
      !is_no_auth(RespCommand::Sunsubscribe)
        && !is_no_auth(RespCommand::PubsubShardchannels)
        && !is_no_auth(RespCommand::PubsubShardnumsub),
      "rust 追加位不得进免认证区间"
    );
  }
}
