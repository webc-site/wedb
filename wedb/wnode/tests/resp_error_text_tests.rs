//! RESP 错误文案与应答头字节级回归（对标 C# CmdStrings.cs 权威文案）
//!
//! 覆盖面（逐条对标 C# 源码）：
//! - SUBSTR / GETRANGE 参数个数错误各报实名（BasicCommands.cs:494 cmd.ToString()）
//! - PEXPIRETIME 参数个数错误恒报 EXPIRETIME（KeyAdminCommands.cs:537
//!   nameof(RespCommand.EXPIRETIME) quirk）
//! - BLMPOP numkeys/count 走 GenericParamShouldBeGreaterThanZero 模板
//!   （ListCommands.cs:871/899，Parameter 反引号版）
//! - ZPOPMIN count 非法 → RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE
//!   （SortedSetCommands.cs:366，"must be positive." 带句点）
//! - ZINTER WEIGHTS 非法浮点 → GenericErrNotAFloat 替换 {0}="weight"
//!   （SortedSetCommands.cs:1107）
//! - GEOADD 经纬度两态：非浮点 → RESP_ERR_NOT_VALID_FLOAT；越界 →
//!   GenericErrLonLat 回显坐标六位小数（SessionParseStateExtensions.cs:781）
//! - CONFIG GET 经 HELLO 3 升级后写 %N map 头（ServerConfig.cs:69
//!   WriteMapLength）
//! - RI.COUNT 与同命令双名 RI.LEN 的端到端应答与错误文案（本仓自定义
//!   扩展，C# 无对应处理器；取数语义见 tests/range_index_tests.rs）

use std::{mem::forget, sync::Arc};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wnode::service::StorageSessionProvider;
use wnode_test::{cmd, err_frame, read_reply, send_cmd, session_factory, start_node, start_server};
use wresp::cmd_strings::{RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_WRONG_TYPE};
use wtest_base::test_store_config;

/// 行式错误应答断言（-<line>\r\n）
/// 期望串由 wresp 单点常量派生（测试不裸写整帧文案）
fn err_text(msg: &str) -> String {
  String::from_utf8(err_frame(msg)).unwrap()
}

async fn assert_error(stream: &mut TcpStream, args: &[&[u8]], expected: &str) {
  send_cmd(stream, args).await.expect("send");
  let raw = read_reply(stream).await;
  assert_eq!(String::from_utf8_lossy(&raw), expected);
}

#[test]
fn error_text_surface_matches_csharp() {
  let rt = Runtime::new().expect("compio runtime");
  let (_server, addr) = start_node();
  let mut s = rt.block_on(async { TcpStream::connect(addr).await.expect("connect") });
  rt.block_on(async {
    // 种子键：字符串与有序集合
    cmd(&mut s, &[b"SET", b"k", b"helloworld"]).await;
    cmd(&mut s, &[b"ZADD", b"z", b"1", b"m"]).await;

    // SUBSTR / GETRANGE 参数个数错误各报实名（C# cmd.ToString()）
    assert_error(
      &mut s,
      &[b"SUBSTR", b"k"],
      "-ERR wrong number of arguments for 'SUBSTR' command\r\n",
    )
    .await;
    assert_error(
      &mut s,
      &[b"GETRANGE", b"k"],
      "-ERR wrong number of arguments for 'GETRANGE' command\r\n",
    )
    .await;

    // PEXPIRETIME 恒报 EXPIRETIME（C# nameof(RespCommand.EXPIRETIME) quirk）
    assert_error(
      &mut s,
      &[b"PEXPIRETIME"],
      "-ERR wrong number of arguments for 'EXPIRETIME' command\r\n",
    )
    .await;

    // HEXPIRE / ZEXPIRE 族参数错误提示命令名对标真实命令名（对标 C# command.ToString()）
    for cmd in ["HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT"] {
      assert_error(
        &mut s,
        &[cmd.as_bytes(), b"k"],
        &format!("-ERR wrong number of arguments for '{cmd}' command\r\n"),
      )
      .await;
    }
    for cmd in ["HTTL", "HPTTL", "HEXPIRETIME", "HPEXPIRETIME"] {
      assert_error(
        &mut s,
        &[cmd.as_bytes(), b"k"],
        &format!("-ERR wrong number of arguments for '{cmd}' command\r\n"),
      )
      .await;
    }
    for cmd in ["ZEXPIRE", "ZPEXPIRE", "ZEXPIREAT", "ZPEXPIREAT"] {
      assert_error(
        &mut s,
        &[cmd.as_bytes(), b"k"],
        &format!("-ERR wrong number of arguments for '{cmd}' command\r\n"),
      )
      .await;
    }
    for cmd in ["ZTTL", "ZPTTL", "ZEXPIRETIME", "ZPEXPIRETIME"] {
      assert_error(
        &mut s,
        &[cmd.as_bytes(), b"k"],
        &format!("-ERR wrong number of arguments for '{cmd}' command\r\n"),
      )
      .await;
    }

    // BLMPOP：numkeys 非法与 COUNT 非法走 Parameter 反引号模板
    assert_error(
      &mut s,
      &[b"BLMPOP", b"0.1", b"abc", b"k", b"LEFT"],
      "-ERR Parameter `numkeys` should be greater than 0\r\n",
    )
    .await;
    assert_error(
      &mut s,
      &[b"BLMPOP", b"0.1", b"1", b"k", b"LEFT", b"COUNT", b"0"],
      "-ERR Parameter `count` should be greater than 0\r\n",
    )
    .await;

    // ZPOPMIN count 非法 → "must be positive."（带句点）
    assert_error(
      &mut s,
      &[b"ZPOPMIN", b"z", b"abc"],
      "-ERR value is out of range, must be positive.\r\n",
    )
    .await;

    // WEIGHTS 非法浮点 → "ERR weight value is not a valid float"
    assert_error(
      &mut s,
      &[b"ZINTER", b"1", b"z", b"WEIGHTS", b"abc"],
      "-ERR weight value is not a valid float\r\n",
    )
    .await;

    // WEIGHTS 数量不足优先于值解析（C# 两段式：先判数量够再判浮点，
    // SortedSetCommands.cs:1097/:1289/:1388/:1512）
    assert_error(
      &mut s,
      &[b"ZUNIONSTORE", b"d", b"2", b"a", b"b", b"WEIGHTS", b"xx"],
      &err_text(RESP_ERR_GENERIC_SYNTAX_ERROR),
    )
    .await;
    // 数量足够时非浮点值才报 weight is not a valid float
    assert_error(
      &mut s,
      &[
        b"ZUNIONSTORE",
        b"d",
        b"2",
        b"a",
        b"b",
        b"WEIGHTS",
        b"1",
        b"xx",
      ],
      "-ERR weight value is not a valid float\r\n",
    )
    .await;

    // GEOADD 经纬度两态：非浮点 → not a valid float
    assert_error(
      &mut s,
      &[b"GEOADD", b"g", b"abc", b"12", b"m"],
      "-ERR value is not a valid float\r\n",
    )
    .await;
    // 越界 → 回显坐标（{lon:F6},{lat:F6}）
    assert_error(
      &mut s,
      &[b"GEOADD", b"g", b"181", b"12", b"m"],
      "-ERR invalid longitude,latitude pair 181.000000,12.000000\r\n",
    )
    .await;
    // inf 词形可解析但越界：.NET F6 渲染 Infinity（非 Rust {:.6} 的 inf）
    assert_error(
      &mut s,
      &[b"GEOADD", b"g", b"inf", b"12", b"m"],
      "-ERR invalid longitude,latitude pair Infinity,12.000000\r\n",
    )
    .await;
    // 半点远离零：1/128 词形第 7 位精确半点，{:.6} 半到偶会得 180.007812
    assert_error(
      &mut s,
      &[b"GEOADD", b"g", b"180.0078125", b"12", b"m"],
      "-ERR invalid longitude,latitude pair 180.007813,12.000000\r\n",
    )
    .await;
    // 选项词元吞掉全部实参：C# do-while 至少校验一组 → syntax error
    assert_error(
      &mut s,
      &[b"GEOADD", b"g", b"CH", b"CH", b"CH"],
      &err_text(RESP_ERR_GENERIC_SYNTAX_ERROR),
    )
    .await;
    // 合法坐标可入键（排除误拦回归）
    cmd(&mut s, &[b"GEOADD", b"g", b"13.36", b"38.11", b"m"]).await;
  });
}

#[test]
fn config_get_writes_resp3_map_header_after_hello() {
  let rt = Runtime::new().expect("compio runtime");
  let (_server, addr) = start_node();
  let mut s = rt.block_on(async { TcpStream::connect(addr).await.expect("connect") });
  rt.block_on(async {
    // HELLO 3 → 升级协议，应答 map 头 %8（8 对字段）
    send_cmd(&mut s, &[b"HELLO", b"3"]).await.expect("hello");
    let hello = read_reply(&mut s).await;
    assert!(hello.starts_with(b"%8\r\n"), "HELLO %8 expected: {hello:?}");

    // CONFIG GET 升级后写 map 头（C# ServerConfig.cs:69）
    send_cmd(&mut s, &[b"CONFIG", b"GET", b"cluster-node-timeout"])
      .await
      .expect("config get");
    let reply = read_reply(&mut s).await;
    assert!(
      reply.starts_with(b"%1\r\n"),
      "CONFIG GET %1 expected: {reply:?}"
    );
    assert!(
      reply.ends_with(b"$2\r\n60\r\n"),
      "值 60 expected: {reply:?}"
    );
  });
}

/// 解析口径四条回归（对标 C# TryGetInt int32 宽度 + SCAN 两态 +
/// RI.CREATE 协议错误文案）
#[test]
fn parse_width_and_error_text_quirks() {
  let rt = Runtime::new().expect("compio runtime");
  let (_server, addr) = start_node();
  let mut s = rt.block_on(async { TcpStream::connect(addr).await.expect("connect") });
  rt.block_on(async {
    // 种子 zset 键
    cmd(&mut s, &[b"ZADD", b"z", b"1", b"m"]).await;

    // ZMPOP：numkeys/count i32 溢出即非整数（SortedSetCommands.cs:423/:468）
    assert_error(
      &mut s,
      &[b"ZMPOP", b"3000000000", b"z", b"MIN"],
      "-ERR value is not an integer or out of range.\r\n",
    )
    .await;
    assert_error(
      &mut s,
      &[b"ZMPOP", b"1", b"z", b"MIN", b"COUNT", b"3000000000"],
      "-ERR value is not an integer or out of range.\r\n",
    )
    .await;

    // BZMPOP：numkeys/count 非整数（含溢出）或 <=0/<1 均报 Parameter 版
    // （SortedSetCommands.cs:1644-1647/:1687-1690）
    assert_error(
      &mut s,
      &[b"BZMPOP", b"0.1", b"3000000000", b"z", b"MIN"],
      "-ERR Parameter `numkeys` should be greater than 0\r\n",
    )
    .await;
    assert_error(
      &mut s,
      &[b"BZMPOP", b"0.1", b"0", b"z", b"MIN"],
      "-ERR Parameter `numkeys` should be greater than 0\r\n",
    )
    .await;
    assert_error(
      &mut s,
      &[
        b"BZMPOP",
        b"0.1",
        b"1",
        b"z",
        b"MIN",
        b"COUNT",
        b"3000000000",
      ],
      "-ERR Parameter `count` should be greater than 0\r\n",
    )
    .await;

    // ZINTERCARD：nKeys<1 报 AtLeastOneKey 实名版（:1191）
    assert_error(
      &mut s,
      &[b"ZINTERCARD", b"0", b"z"],
      "-ERR at least 1 input key is needed for 'ZINTERCARD' command\r\n",
    )
    .await;
    // LIMIT 负值报 GenericErrCantBeNegative "LIMIT"（:1218）
    assert_error(
      &mut s,
      &[b"ZINTERCARD", b"1", b"z", b"LIMIT", b"-1"],
      "-ERR LIMIT can't be negative\r\n",
    )
    .await;
    // LIMIT i32 溢出报 NOT_INTEGER（:1211）
    assert_error(
      &mut s,
      &[b"ZINTERCARD", b"1", b"z", b"LIMIT", b"3000000000"],
      "-ERR value is not an integer or out of range.\r\n",
    )
    .await;

    // ZRANDMEMBER count i32 溢出（:832 TryGetInt）
    assert_error(
      &mut s,
      &[b"ZRANDMEMBER", b"z", b"3000000000"],
      "-ERR value is not an integer or out of range.\r\n",
    )
    .await;

    // ZUNION / ZDIFF numkeys i32 溢出（:1356/:921）
    assert_error(
      &mut s,
      &[b"ZUNION", b"3000000000", b"z"],
      "-ERR value is not an integer or out of range.\r\n",
    )
    .await;
    assert_error(
      &mut s,
      &[b"ZDIFF", b"3000000000", b"z"],
      "-ERR value is not an integer or out of range.\r\n",
    )
    .await;

    // LMPOP：numkeys/count 溢出走 GenericErr 版（ListCommands.cs:198/:228）
    assert_error(
      &mut s,
      &[b"LMPOP", b"3000000000", b"z", b"LEFT"],
      "-ERR numkeys should be greater than 0\r\n",
    )
    .await;
    assert_error(
      &mut s,
      &[b"LMPOP", b"1", b"z", b"LEFT", b"COUNT", b"3000000000"],
      "-ERR count should be greater than 0\r\n",
    )
    .await;
  });
}

/// SCAN 参数两态回归（ArrayCommands.cs:275-313 + Tsavorite
/// AllocatorScan.cs:268 acceptedCount >= count）
#[test]
fn scan_parse_quirks_match_csharp() {
  let rt = Runtime::new().expect("compio runtime");
  let (_server, addr) = start_node();
  let mut s = rt.block_on(async { TcpStream::connect(addr).await.expect("connect") });
  rt.block_on(async {
    // 空库基线：`*2` + 游标 "0" + 空数组
    let empty: &[u8] = b"*2\r\n$1\r\n0\r\n*0\r\n";

    // 未知选项静默跳过（C# if/else-if 链无 else）
    send_cmd(&mut s, &[b"SCAN", b"0", b"FOO", b"BAR"])
      .await
      .expect("scan");
    assert_eq!(read_reply(&mut s).await, empty);

    // COUNT 0 / 负值合法（TryGetLong 仅校验整数性），扫描层钳 1 条语义
    send_cmd(&mut s, &[b"SCAN", b"0", b"COUNT", b"0"])
      .await
      .expect("scan");
    assert_eq!(read_reply(&mut s).await, empty);
    send_cmd(&mut s, &[b"SCAN", b"0", b"COUNT", b"-5"])
      .await
      .expect("scan");
    assert_eq!(read_reply(&mut s).await, empty);

    // 未知 TYPE 值（非五类双字面量）：空列表 + 游标 0（DbScan :82-84）
    send_cmd(&mut s, &[b"SCAN", b"0", b"TYPE", b"Zset"])
      .await
      .expect("scan");
    assert_eq!(read_reply(&mut s).await, empty);

    // COUNT 非整数 / cursor 负值仍报错（回归不变）
    assert_error(
      &mut s,
      &[b"SCAN", b"0", b"COUNT", b"abc"],
      "-ERR value is not an integer or out of range.\r\n",
    )
    .await;
    assert_error(&mut s, &[b"SCAN", b"-1"], "-ERR invalid cursor\r\n").await;

    // 种子：字符串键 + zset 键（物理扫描序按日志地址，不逐字节断言键序）
    cmd(&mut s, &[b"SET", b"foo", b"bar"]).await;
    cmd(&mut s, &[b"ZADD", b"z", b"1", b"m"]).await;

    // TYPE zset 精确匹配 → 命中 z 键
    send_cmd(&mut s, &[b"SCAN", b"0", b"TYPE", b"zset"])
      .await
      .expect("scan");
    let reply = read_reply(&mut s).await;
    assert!(reply.starts_with(b"*2\r\n"), "SCAN *2 expected: {reply:?}");
    assert!(
      window_contains(&reply, b"$1\r\nz\r\n"),
      "z 键命中 expected: {reply:?}"
    );

    // TYPE Zset（混合大小写）支持大小写不敏感匹配 → 同样命中 z 键
    send_cmd(&mut s, &[b"SCAN", b"0", b"TYPE", b"Zset"])
      .await
      .expect("scan");
    let reply = read_reply(&mut s).await;
    assert!(
      window_contains(&reply, b"$1\r\nz\r\n"),
      "Zset 混合大小写命中 expected: {reply:?}"
    );

    // 未知类型（如 unknown）在非空库仍空回
    send_cmd(&mut s, &[b"SCAN", b"0", b"TYPE", b"unknown"])
      .await
      .expect("scan");
    assert_eq!(read_reply(&mut s).await, empty);

    // COUNT 0 有键库：单页至多 1 条（首条匹配后 acceptedCount >= count 停）
    send_cmd(&mut s, &[b"SCAN", b"0", b"COUNT", b"0"])
      .await
      .expect("scan");
    let reply = read_reply(&mut s).await;
    assert!(reply.starts_with(b"*2\r\n"), "SCAN *2 expected: {reply:?}");
    // 第二元素数组长度 <= 1（键数组头 = 应答最后一个 '*' 聚合帧）
    let keys_at = reply.iter().rposition(|&b| b == b'*').unwrap_or(0);
    let keys_header = &reply[keys_at..];
    assert!(
      keys_header.starts_with(b"*0\r\n") || keys_header.starts_with(b"*1\r\n"),
      "COUNT 0 单页至多 1 条 expected: {reply:?}"
    );
  });
}

/// RI.CREATE 数值选项协议错误文案（C# RespParsingException →
/// RespServerSession.cs:522 catch `ERR Protocol Error: {msg}`）；
/// 断连差异验证：rust 文案对齐而连接保持
#[test]
fn ri_create_numeric_protocol_errors() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("ri_parse.db");
  let config = test_store_config().with_range_index_dir(dir.path().join("ri"));
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      config,
      &data_path,
      None,
      None,
      session_factory,
    )
    .expect("open with aof"),
  );
  forget(dir);
  let (_server, addr) = start_server(provider);
  let mut s = rt.block_on(async { TcpStream::connect(addr).await.expect("connect") });
  rt.block_on(async {
    // 非数字 → ThrowNotANumber 文案（回显原始参数）
    assert_error(
      &mut s,
      &[b"RI.CREATE", b"k", b"CACHESIZE", b"abc"],
      "-ERR Protocol Error: Unable to parse number: abc\r\n",
    )
    .await;
    // 尾随垃圾同态
    assert_error(
      &mut s,
      &[b"RI.CREATE", b"k", b"CACHESIZE", b"12x"],
      "-ERR Protocol Error: Unable to parse number: 12x\r\n",
    )
    .await;
    // u64 溢出 → ThrowNotANumber（C# TryReadUInt64 失败路径）
    assert_error(
      &mut s,
      &[b"RI.CREATE", b"k", b"CACHESIZE", b"99999999999999999999"],
      "-ERR Protocol Error: Unable to parse number: 99999999999999999999\r\n",
    )
    .await;
    // u64 内超 i64 → ThrowIntegerOverflow（数字串不含符号）
    assert_error(
      &mut s,
      &[b"RI.CREATE", b"k", b"CACHESIZE", b"9223372036854775808"],
      "-ERR Protocol Error: Unable to parse integer. The given number is larger than allowed: 9223372036854775808\r\n",
    )
    .await;
    assert_error(
      &mut s,
      &[b"RI.CREATE", b"k", b"MINRECORD", b"-9223372036854775809"],
      "-ERR Protocol Error: Unable to parse integer. The given number is larger than allowed: 9223372036854775809\r\n",
    )
    .await;

    // 断连差异：C# 协议错误后 DisposeNetworkSender；rust 连接保持（已知差异）
    cmd(&mut s, &[b"PING"]).await;

    // 前导零合法（C# GetLong allowLeadingZeros 默认 true），PAGESIZE 04096 = 4096 创建成功
    assert_error(&mut s, &[b"RI.CREATE", b"k", b"PAGESIZE", b"04096"], "+OK\r\n").await;

    // RI.COUNT 与同命令双名 RI.LEN（PRIMARY_TABLE 解析期归一到
    // RespCommand::Ricount）：上一步建的空索引直读 MetaValue.size 回 :0，
    // 两名字应答字节一致，取值语义由 tests/range_index_tests.rs 覆盖
    assert_error(&mut s, &[b"RI.COUNT", b"k"], ":0\r\n").await;
    assert_error(&mut s, &[b"RI.LEN", b"k"], ":0\r\n").await;
    // 参数个数守卫（check_arg_count! 1 → 非 RI 族的 unknown 文案不同）
    assert_error(
      &mut s,
      &[b"RI.COUNT"],
      "-ERR wrong number of arguments for 'RI.COUNT' command\r\n",
    )
    .await;
    // 不存在的索引：网络层硬编码文案（与 GET/CONFIG/METRICS 同口径）
    assert_error(
      &mut s,
      &[b"RI.LEN", b"nope"],
      "-ERR range index not found\r\n",
    )
    .await;
    // 别名不引入第二套命令名：参数守卫恒出 canonical 'RI.COUNT'（解析期归一）
    assert_error(
      &mut s,
      &[b"RI.LEN"],
      "-ERR wrong number of arguments for 'RI.COUNT' command\r\n",
    )
    .await;
    // 写入一条字段后两名应答同值（默认 MINRECORD 64：字段 5 + 值 60 = 65）
    let long_val = vec![b'v'; 60];
    cmd(&mut s, &[b"RI.SET", b"k", b"alpha", &long_val]).await;
    assert_error(&mut s, &[b"RI.COUNT", b"k"], ":1\r\n").await;
    assert_error(&mut s, &[b"RI.LEN", b"k"], ":1\r\n").await;
    // 普通字符串键：存储层 WrongType → 网络层 CmdStrings 同款文案
    cmd(&mut s, &[b"SET", b"plain_str", b"v"]).await;
    assert_error(
      &mut s,
      &[b"RI.COUNT", b"plain_str"],
      &err_text(RESP_ERR_WRONG_TYPE),
    )
    .await;
    // 未启用 RI 预览的门（ri = None）与取数语义见
    // tests/range_index_tests.rs:ri_count_basic_test
  });
}

/// 子串包含判定（&[u8] 无 contains(&[u8]) 稳定方法，memchr 语义手写）
fn window_contains(hay: &[u8], needle: &[u8]) -> bool {
  hay.windows(needle.len()).any(|w| w == needle)
}
