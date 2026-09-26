//! SET 覆盖对象键全组合矩阵回归测试（对标 C# libs/server/Resp/BasicCommands.cs:NetworkSET_Conditional）
//!
//! C# 机制（libs/server/Resp/BasicCommands.cs:NetworkSET_Conditional:788-796）：
//! 非 GET 形态遇 WRONGTYPE（键驻留对象存储域）promote 事务（Main | Object 双域
//! 排他锁）DELETE 后重试 SET_Conditional，旧 TTL 随 DELETE 一并消失：
//! - SET / SET EX：重试无条件写成功 → OK + string + 新 TTL（或无 TTL）；
//! - NX（SETNX / KEEPTTL NX）：键已删 → NOTFOUND → ok 翻转 → 写入回 OK；
//! - XX（SETEXXX / SETKEEPTTLXX）：键已删 → NOTFOUND → 不写回 nil，对象键被删；
//! - GET 形态遇 WRONGTYPE 仅报错不删（:821-823），键与 TTL 原样。
//!
//! rust 无双 store，对象键以 KeyTag::ObjectEnvelope 信封驻留字符串域，
//! network_set_conditional 按域分流承接同一最终语义（应答、值类型、TTL
//! 三项断言；KEEPTTL 组合锁死幽灵 TTL 回填回归）。

use std::{net::SocketAddr, sync::Arc};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wconf::RuntimeServerOptions;
use wnode::service::StorageSessionProvider;
use wnode_test::{cmd, err_frame, session_factory, start_server};
use wresp::cmd_strings::RESP_ERR_WRONG_TYPE;
use wtest_base::test_store_config;

/// 期望 WRONGTYPE 行：由 wresp 单点常量派生
/// （常量定义见 libs/server/Resp/CmdStrings.cs 的 RESP_ERR_WRONG_TYPE，非函数不入映射表）
fn wrongtype_line() -> String {
  String::from_utf8(err_frame(RESP_ERR_WRONG_TYPE)).unwrap()
}

/// 行式应答（+OK / -ERR / :N / $-1）
fn line_reply(raw: &[u8]) -> String {
  String::from_utf8_lossy(raw).trim_end().to_string()
}

/// 单格断言：值类型、TTL、应答三项
async fn assert_string_state(s: &mut TcpStream, key: &[u8], expect_get: &[u8], expect_ttl: Ttl) {
  let mut type_args = vec![&b"TYPE"[..]];
  type_args.push(key);
  assert_eq!(
    line_reply(&cmd(s, &type_args).await),
    "+string",
    "覆盖后键须为 string"
  );

  let mut get_args = vec![&b"GET"[..]];
  get_args.push(key);
  let got = cmd(s, &get_args).await;
  assert_eq!(
    String::from_utf8_lossy(&got),
    String::from_utf8_lossy(expect_get),
    "覆盖后 GET 值不符"
  );

  let mut ttl_args = vec![&b"TTL"[..]];
  ttl_args.push(key);
  let ttl: i64 = line_reply(&cmd(s, &ttl_args).await)
    .trim_start_matches(':')
    .parse()
    .expect("ttl int");
  match expect_ttl {
    Ttl::None => assert_eq!(ttl, -1, "不得残留对象时代幽灵 TTL"),
    Ttl::Within(secs) => assert!((0..=secs).contains(&ttl), "TTL 须在 (0,{secs}]: {ttl}"),
  }
}

/// TTL 断言口径
enum Ttl {
  /// 无过期（-1）
  None,
  /// (0, secs] 区间
  Within(i64),
}

/// 对目标键 HSET 建 hash 对象键（带可选 TTL 前置）
async fn hset_object_key(s: &mut TcpStream, key: &[u8], with_ttl: bool) {
  let mut args = vec![&b"HSET"[..], key, b"f", b"v"];
  assert_eq!(line_reply(&cmd(s, &args).await), ":1", "HSET 建对象键");
  if with_ttl {
    args = vec![&b"EXPIRE"[..], key, b"100"];
    assert_eq!(line_reply(&cmd(s, &args).await), ":1", "EXPIRE 前置");
    let ttl_args = vec![&b"TTL"[..], key];
    let ttl: i64 = line_reply(&cmd(s, &ttl_args).await)
      .trim_start_matches(':')
      .parse()
      .expect("ttl int");
    assert!((0..=100).contains(&ttl), "前置对象键须带 TTL: {ttl}");
  }
}

/// 裸 SET / SET EX / SET NX / SET NX EX / SET XX / SET GET 六格：
/// 应答、值类型、TTL 与 C# DELETE+重试语义逐格对齐
#[test]
fn set_overwrite_object_matrix() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("set_matrix.db");
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, addr): (_, SocketAddr) = start_server(Arc::clone(&provider));
  rt.block_on(async move {
    let mut s = TcpStream::connect(addr).await.expect("connect");

    // ---- 格 1：裸 SET 覆盖对象键 → OK + string + 无 TTL
    hset_object_key(&mut s, b"m1", false).await;
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"m1", b"s1"]).await),
      "+OK"
    );
    assert_string_state(&mut s, b"m1", b"$2\r\ns1\r\n", Ttl::None).await;

    // ---- 格 2：SET EX 100 覆盖对象键 → OK + string + 新 TTL
    hset_object_key(&mut s, b"m2", false).await;
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"m2", b"s2", b"EX", b"100"]).await),
      "+OK"
    );
    assert_string_state(&mut s, b"m2", b"$2\r\ns2\r\n", Ttl::Within(100)).await;

    // ---- 格 3：SET NX 对对象键（C# DELETE 后 NX 成立）→ OK + string
    hset_object_key(&mut s, b"m3", false).await;
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"m3", b"s3", b"NX"]).await),
      "+OK",
      "C# DELETE 后重试 SETNX 写入成功，须回 OK"
    );
    assert_string_state(&mut s, b"m3", b"$2\r\ns3\r\n", Ttl::None).await;

    // ---- 格 4：SET XX 对对象键（C# DELETE 后重试 NOTFOUND）→ nil + 键被删
    hset_object_key(&mut s, b"m4", false).await;
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"m4", b"s4", b"XX"]).await),
      "$-1",
      "C# DELETE 后重试 SETXX 缺键回 nil"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"EXISTS", b"m4"]).await),
      ":0",
      "SET XX 对对象键须 DELETE 旧键（C# 事务语义）"
    );

    // ---- 格 5：SET NX EX 覆盖对象键 → OK + string + 新 TTL
    hset_object_key(&mut s, b"m5", false).await;
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"m5", b"s5", b"NX", b"EX", b"100"]).await),
      "+OK"
    );
    assert_string_state(&mut s, b"m5", b"$2\r\ns5\r\n", Ttl::Within(100)).await;

    // ---- 格 6：SET GET 对对象键 → WRONGTYPE，键与类型原样
    hset_object_key(&mut s, b"m6", false).await;
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"m6", b"s6", b"GET"]).await),
      wrongtype_line().trim_end(),
      "C# GET 形态遇 WRONGTYPE 仅报错不删"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"TYPE", b"m6"]).await),
      "+hash",
      "GET 形态不得动对象键"
    );
    assert_eq!(line_reply(&cmd(&mut s, &[b"EXISTS", b"m6"]).await), ":1");
  });
  server.stop();
}

/// KEEPTTL 组合矩阵：对象时代 TTL 不得回填到新 string 值（幽灵 TTL 回归），
/// XX 族仍按 C# DELETE+NOTFOUND 回 nil 并删键；string 键对照组保留 TTL 本义
#[test]
fn set_keepttl_overwrite_object_no_ghost_ttl() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("set_keepttl_matrix.db");
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, addr): (_, SocketAddr) = start_server(Arc::clone(&provider));
  rt.block_on(async move {
    let mut s = TcpStream::connect(addr).await.expect("connect");

    // ---- 格 1：SET KEEPTTL 覆盖带 TTL 对象键 → OK + string + 无 TTL
    //（C# DELETE 连带旧 TTL 消失；旧实现回填幽灵 TTL）
    hset_object_key(&mut s, b"k1", true).await;
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"k1", b"s1", b"KEEPTTL"]).await),
      "+OK"
    );
    assert_string_state(&mut s, b"k1", b"$2\r\ns1\r\n", Ttl::None).await;

    // ---- 格 2：SET KEEPTTL NX 对带 TTL 对象键 → OK + string + 无 TTL
    //（C# 派发 SETEXNX：DELETE 后 NX 成立，写入无 TTL）
    hset_object_key(&mut s, b"k2", true).await;
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"k2", b"s2", b"KEEPTTL", b"NX"]).await),
      "+OK",
      "KEEPTTL NX 对对象键 DELETE 后 NX 成立"
    );
    assert_string_state(&mut s, b"k2", b"$2\r\ns2\r\n", Ttl::None).await;

    // ---- 格 3：SET KEEPTTL XX 对带 TTL 对象键 → nil + 键被删
    //（C# DELETE 后重试 SETKEEPTTLXX 缺键 NOTFOUND）
    hset_object_key(&mut s, b"k3", true).await;
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"k3", b"s3", b"KEEPTTL", b"XX"]).await),
      "$-1",
      "KEEPTTL XX 对对象键 DELETE 后重试缺键回 nil"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"EXISTS", b"k3"]).await),
      ":0",
      "KEEPTTL XX 对对象键须 DELETE 旧键"
    );

    // ---- 对照组：SET KEEPTTL 对带 TTL string 键保留 TTL（KEEPTTL 本义不回退）
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"cs", b"old", b"EX", b"100"]).await),
      "+OK"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"cs", b"new", b"KEEPTTL"]).await),
      "+OK"
    );
    assert_string_state(&mut s, b"cs", b"$3\r\nnew\r\n", Ttl::Within(100)).await;
  });
  server.stop();
}
