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

use std::sync::Arc;

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wedb_test::test_store_config;
use wnode::service::StorageSessionProvider;
use wnode_test::{cmd, read_reply, send_cmd, session_factory, start_server};

/// 起一台临时服务器并返回（服务器句柄，实际监听地址）；目录随进程存活
macro_rules! start_node {
  () => {{
    let dir = tempdir().expect("tempdir");
    let data_path = dir.path().join("node").join("resp_text.db");
    let provider = Arc::new(
      StorageSessionProvider::open_with_config_and_aof(
        test_store_config(),
        &data_path,
        None,
        None,
        session_factory,
      )
      .expect("open with aof"),
    );
    std::mem::forget(dir);
    start_server(provider)
  }};
}

/// 行式错误应答断言（-<line>\r\n）
async fn assert_error(stream: &mut TcpStream, args: &[&[u8]], expected: &str) {
  send_cmd(stream, args).await.expect("send");
  let raw = read_reply(stream).await;
  assert_eq!(String::from_utf8_lossy(&raw), expected);
}

#[test]
fn error_text_surface_matches_csharp() {
  let rt = Runtime::new().expect("compio runtime");
  let (_server, addr) = start_node!();
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
    // 合法坐标可入键（排除误拦回归）
    cmd(&mut s, &[b"GEOADD", b"g", b"13.36", b"38.11", b"m"]).await;
  });
}

#[test]
fn config_get_writes_resp3_map_header_after_hello() {
  let rt = Runtime::new().expect("compio runtime");
  let (_server, addr) = start_node!();
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
