//! 对象键跨型防线回归测试（next/data.md P2-2 + P2-6：ETag 族与位图族
//! 对集合对象键的静默数据破坏）
//!
//! 对象信封带外化后，集合对象整条记录挂 KeyTag::ObjectEnvelope 物理键。
//! 跨型防线缺口与修复口径（1:1 对标 C#）：
//! 1. 位图族（SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD）：C# 主存读写
//!    首探 DataHeader.ValueIsObject 置 WrongType（RMWMethods.cs:InPlaceUpdater
//!    / ReadMethods.cs:Reader），RESP 层回 WRONGTYPE 且键原样。旧实现把对象
//!    键当缺失按空串扩写，经信封覆写清退墓碑化集合——静默丢数据。BITOP
//!    目的键例外：C# WRONGTYPE 后 DELETE + SET 重写（BitmapOps.cs），Rust
//!    信封覆写清退同语义，目的键被覆写为位图串属预期。
//! 2. ETag 族 DELIFGREATER：C# RMW 判型先于 etag 比较
//!    （RMWMethods.Etags.cs:HandleEtagNeedCopyUpdate 对 RecordType != 0 置
//!    WrongType），DEL_Conditional 未过期即回 keysDeleted = 0。旧实现对象
//!    键 etag 缺省 0，given >= 1 即删集合键。
//! 3. ETag 族 SETIFMATCH / SETIFGREATER / SETWITHETAG：C#
//!    BasicEtagCommands.cs:ExecuteETagSetCommand 对 WRONGTYPE promote 事务
//!    → DELETE 对象键 → SET_Conditional 重做（键已删走 InitialUpdater
//!    无条件初写，条件与 NOGET 均不参与），应答 `[newEtag, nil]`
//!    （SETWITHETAG 回整数 NoETag + 1）。旧实现 SETIFMATCH/SETIFGREATER 回
//!    WRONGTYPE 键保留（与 C# 完全相反），SETWITHETAG 直接覆写 String 域
//!    墓碑化集合。
//! 4. 读侧 GETWITHETAG / GETIFNOTMATCH 对象键回 WRONGTYPE（C# 一致，锁存）。

use std::sync::Arc;

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wnode::service::StorageSessionProvider;
use wnode_test::{cmd, err_frame, session_factory, start_server};
use wresp::cmd_strings::RESP_ERR_WRONG_TYPE;
use wtest_base::test_store_config;

/// 期望 WRONGTYPE 帧：由 wresp 单点常量派生
/// （常量定义见 libs/server/Resp/CmdStrings.cs 的 RESP_ERR_WRONG_TYPE，非函数不入映射表）
fn wrongtype_frame() -> Vec<u8> {
  err_frame(RESP_ERR_WRONG_TYPE)
}

/// 计算缓冲区首条完整 RESP 应答的字节长度（不完整返回 None）
fn complete_len(data: &[u8]) -> Option<usize> {
  let kind = *data.first()?;
  let nl = data.iter().position(|&b| b == b'\n')?;
  let header: i64 = String::from_utf8_lossy(&data[1..nl])
    .trim_end()
    .parse()
    .ok()?;
  match kind {
    b'+' | b'-' | b':' => Some(nl + 1),
    b'$' => {
      if header < 0 {
        Some(nl + 1)
      } else {
        (data.len() >= nl + 1 + header as usize + 2).then(|| nl + 1 + header as usize + 2)
      }
    }
    b'*' => {
      if header <= 0 {
        return Some(nl + 1);
      }
      let mut rest = &data[nl + 1..];
      let mut total = nl + 1;
      for _ in 0..header {
        let used = complete_len(rest)?;
        rest = &rest[used..];
        total += used;
      }
      Some(total)
    }
    _ => None,
  }
}

/// 行式应答（+OK / -ERR / :N）
fn line_reply(raw: &[u8]) -> String {
  String::from_utf8_lossy(raw).trim_end().to_string()
}

/// bulk 应答载荷（nil 帧返回 None）
fn bulk_reply(raw: &[u8]) -> Option<Vec<u8>> {
  if raw.starts_with(b"$-1\r\n") {
    return None;
  }
  let nl = raw.iter().position(|&b| b == b'\n')?;
  let len: usize = String::from_utf8_lossy(&raw[1..nl])
    .trim_end()
    .parse()
    .ok()?;
  Some(raw[nl + 1..nl + 1 + len].to_vec())
}

/// 数组应答平铺出全部 bulk 元素（SMEMBERS）
fn array_reply(raw: &[u8]) -> Vec<Vec<u8>> {
  let mut items = Vec::new();
  let mut rest = raw;
  // 首帧为数组头
  let nl = rest.iter().position(|&b| b == b'\n').expect("array head");
  let n: i64 = String::from_utf8_lossy(&rest[1..nl])
    .trim_end()
    .parse()
    .expect("alen");
  rest = &rest[nl + 1..];
  for _ in 0..n {
    let used = complete_len(rest).expect("complete element");
    let el = &rest[..used];
    if el[0] == b'$' {
      items.push(bulk_reply(el).expect("bulk element"));
    }
    rest = &rest[used..];
  }
  items
}

/// 场景 1：集合键上的位图族全部 WRONGTYPE，集合数据完好（P2-2 主回归）
#[test]
fn bitmap_family_rejects_object_key_and_preserves_set() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("bitmap_cross.db");
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
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");

    // 建集合键 bt = {a, b, c}
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SADD", b"bt", b"a", b"b", b"c"]).await),
      ":3"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SADD", b"bt", b"b"]).await),
      ":0"
    );

    // SETBIT → WRONGTYPE，且 TYPE 仍为 set、SMEMBERS 原样
    let raw = cmd(&mut s, &[b"SETBIT", b"bt", b"0", b"1"]).await;
    assert_eq!(
      raw,
      wrongtype_frame(),
      "SETBIT 集合键须回 WRONGTYPE（旧实现静默墓碑化集合）"
    );
    assert_eq!(line_reply(&cmd(&mut s, &[b"TYPE", b"bt"]).await), "+set");
    let members = array_reply(&cmd(&mut s, &[b"SMEMBERS", b"bt"]).await);
    assert_eq!(members.len(), 3, "集合三成员须完好: {members:?}");
    for m in ["a", "b", "c"] {
      assert!(
        members.iter().any(|v| v == m.as_bytes()),
        "须含 {m}: {members:?}"
      );
    }

    // 读侧位图族同样 WRONGTYPE（C# Read ValueIsObject 口径），键不受扰
    for args in [
      vec![&b"GETBIT"[..], b"bt", b"0"],
      vec![&b"BITCOUNT"[..], b"bt"],
      vec![&b"BITPOS"[..], b"bt", b"1"],
      vec![&b"BITFIELD"[..], b"bt", b"GET", b"u8", b"0"],
      vec![&b"BITFIELD_RO"[..], b"bt", b"GET", b"u8", b"0"],
    ] {
      let raw = cmd(&mut s, &args).await;
      assert_eq!(
        raw,
        wrongtype_frame(),
        "{:?}",
        String::from_utf8_lossy(&raw)
      );
    }
    assert_eq!(line_reply(&cmd(&mut s, &[b"TYPE", b"bt"]).await), "+set");

    // BITOP 源键含对象键：整体 WRONGTYPE 且目的键零副作用
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"bs", b"A"]).await),
      "+OK"
    );
    let raw = cmd(&mut s, &[b"BITOP", b"OR", b"bdst", b"bt", b"bs"]).await;
    assert_eq!(raw, wrongtype_frame(), "BITOP 源键对象键须整体 WRONGTYPE");
    assert_eq!(line_reply(&cmd(&mut s, &[b"EXISTS", b"bdst"]).await), ":0");

    // BITOP 目的键为对象键：C# BitmapOps.cs WRONGTYPE → DELETE + SET 重写，
    // 目的键被覆写为位图串属预期语义（Rust 信封覆写清退同径）
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SADD", b"bd", b"x"]).await),
      ":1"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"BITOP", b"AND", b"bd", b"bs"]).await),
      ":1"
    );
    assert_eq!(line_reply(&cmd(&mut s, &[b"TYPE", b"bd"]).await), "+string");
    assert_eq!(
      bulk_reply(&cmd(&mut s, &[b"GET", b"bd"]).await),
      Some(b"A".to_vec())
    );

    // 集合键 bt 经全程仍完好
    assert_eq!(line_reply(&cmd(&mut s, &[b"TYPE", b"bt"]).await), "+set");
    assert_eq!(
      array_reply(&cmd(&mut s, &[b"SMEMBERS", b"bt"]).await).len(),
      3
    );
  });
  server.stop();
}

/// 场景 2：DELIFGREATER 对象键拦截回 :0，键存活（P2-6 主回归）
#[test]
fn delifgreater_rejects_object_key() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("etag_del.db");
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
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");

    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SADD", b"se", b"a", b"b"]).await),
      ":2"
    );

    // 对象键 etag 缺省 0：given=1 即触发旧实现删除；修复后判型先于 etag
    // 比较，一律 :0 键保留（C# HandleEtagNeedCopyUpdate WrongType 口径）
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"DELIFGREATER", b"se", b"1"]).await),
      ":0"
    );
    assert_eq!(line_reply(&cmd(&mut s, &[b"EXISTS", b"se"]).await), ":1");
    assert_eq!(line_reply(&cmd(&mut s, &[b"TYPE", b"se"]).await), "+set");
    let members = array_reply(&cmd(&mut s, &[b"SMEMBERS", b"se"]).await);
    assert_eq!(members.len(), 2, "集合成员须完好: {members:?}");

    // given=0（条件本就不命中）同样 :0
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"DELIFGREATER", b"se", b"0"]).await),
      ":0"
    );
    assert_eq!(line_reply(&cmd(&mut s, &[b"EXISTS", b"se"]).await), ":1");
  });
  server.stop();
}

/// 场景 3：SETIFMATCH / SETIFGREATER / SETWITHETAG 对象键对齐 C# promote
/// 删写语义（DELETE 对象键 → InitialUpdater 无条件初写）
#[test]
fn etag_set_family_promotes_over_object_key() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("etag_set.db");
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
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");

    // SETIFMATCH 对象键（given=0 命中缺省 etag）：C# promote 后初写，
    // newEtag = given + 1 = 1，应答 [1, nil]（NOGET 不参与初写路径）
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SADD", b"sm", b"a"]).await),
      ":1"
    );
    let raw = cmd(&mut s, &[b"SETIFMATCH", b"sm", b"v1", b"0"]).await;
    assert_eq!(
      raw, b"*2\r\n:1\r\n$-1\r\n",
      "SETIFMATCH 对象键须 promote 删写回 [1, nil]"
    );
    assert_eq!(line_reply(&cmd(&mut s, &[b"TYPE", b"sm"]).await), "+string");
    assert_eq!(
      bulk_reply(&cmd(&mut s, &[b"GET", b"sm"]).await),
      Some(b"v1".to_vec())
    );

    // SETIFGREATER 对象键（given=5 > 0）：初写无条件进行（条件不在
    // InitialUpdater 路径），newEtag = given = 5，应答 [5, nil]
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SADD", b"sg", b"a"]).await),
      ":1"
    );
    let raw = cmd(&mut s, &[b"SETIFGREATER", b"sg", b"v5", b"5"]).await;
    assert_eq!(
      raw, b"*2\r\n:5\r\n$-1\r\n",
      "SETIFGREATER 对象键须 promote 删写回 [5, nil]"
    );
    assert_eq!(line_reply(&cmd(&mut s, &[b"TYPE", b"sg"]).await), "+string");
    assert_eq!(
      bulk_reply(&cmd(&mut s, &[b"GET", b"sg"]).await),
      Some(b"v5".to_vec())
    );

    // SETIFMATCH 对象键（given=7 条件不命中）：promote 后 InitialUpdater
    // 无条件初写，newEtag = 8——C# 判型先行，条件判定只发生在已有字符串
    // 记录的 RMW 路径
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SADD", b"sm2", b"a"]).await),
      ":1"
    );
    let raw = cmd(&mut s, &[b"SETIFMATCH", b"sm2", b"v7", b"7", b"NOGET"]).await;
    assert_eq!(
      raw, b"*2\r\n:8\r\n$-1\r\n",
      "SETIFMATCH NOGET 对象键初写仍回 [8, nil]"
    );
    assert_eq!(
      bulk_reply(&cmd(&mut s, &[b"GET", b"sm2"]).await),
      Some(b"v7".to_vec())
    );

    // SETWITHETAG 对象键：promote 后按 NoETag + 1 初写，应答整数 :1
    // （旧实现直接覆写 String 域静默墓碑化集合）
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SADD", b"sw", b"a"]).await),
      ":1"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SETWITHETAG", b"sw", b"v1"]).await),
      ":1"
    );
    assert_eq!(line_reply(&cmd(&mut s, &[b"TYPE", b"sw"]).await), "+string");
    assert_eq!(
      bulk_reply(&cmd(&mut s, &[b"GET", b"sw"]).await),
      Some(b"v1".to_vec())
    );

    // promote 携带 EX：新字符串记录按新过期生效（InitialUpdater
    // TrySetExpiration(input.arg1) 口径）
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SADD", b"sx", b"a"]).await),
      ":1"
    );
    let raw = cmd(&mut s, &[b"SETIFMATCH", b"sx", b"vx", b"0", b"EX", b"100"]).await;
    assert_eq!(raw, b"*2\r\n:1\r\n$-1\r\n");
    let raw = cmd(&mut s, &[b"TTL", b"sx"]).await;
    let ttl: i64 = line_reply(&raw)
      .trim_start_matches(':')
      .parse()
      .expect("ttl int");
    assert!((0..=100).contains(&ttl), "TTL 须在 (0,100]: {ttl}");
  });
  server.stop();
}

/// 场景 4：读侧 ETag 与字符串键既有语义不回归
#[test]
fn string_key_etag_and_bitmap_semantics_preserved() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("string_nonregress.db");
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
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");

    // 读侧：GETWITHETAG / GETIFNOTMATCH 对象键回 WRONGTYPE（C# 一致，锁存）
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"HSET", b"h", b"f", b"v"]).await),
      ":1"
    );
    let raw = cmd(&mut s, &[b"GETWITHETAG", b"h"]).await;
    assert_eq!(raw, wrongtype_frame(), "GETWITHETAG 对象键须回 WRONGTYPE");
    let raw = cmd(&mut s, &[b"GETIFNOTMATCH", b"h", b"0"]).await;
    assert_eq!(raw, wrongtype_frame(), "GETIFNOTMATCH 对象键须回 WRONGTYPE");
    assert_eq!(line_reply(&cmd(&mut s, &[b"TYPE", b"h"]).await), "+hash");

    // 字符串键 DELIFGREATER 既有条件删除语义不回归
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"ek", b"v1"]).await),
      "+OK"
    );
    let raw = cmd(&mut s, &[b"GETWITHETAG", b"ek"]).await;
    assert_eq!(raw, b"*2\r\n:0\r\n$2\r\nv1\r\n");
    // 无 etag 记录缺省 0：1 > 0 → 删除
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"DELIFGREATER", b"ek", b"1"]).await),
      ":1"
    );
    assert_eq!(line_reply(&cmd(&mut s, &[b"EXISTS", b"ek"]).await), ":0");

    // SETIFMATCH 字符串键既有推进语义不回归（缺省 0 命中 → 1 → 2）
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"ek2", b"a"]).await),
      "+OK"
    );
    let raw = cmd(&mut s, &[b"SETIFMATCH", b"ek2", b"b", b"0"]).await;
    assert_eq!(raw, b"*2\r\n:1\r\n$-1\r\n");
    let raw = cmd(&mut s, &[b"SETIFMATCH", b"ek2", b"c", b"1"]).await;
    assert_eq!(raw, b"*2\r\n:2\r\n$-1\r\n");
    // 不命中：回 [existing, 旧值]
    let raw = cmd(&mut s, &[b"SETIFMATCH", b"ek2", b"d", b"9"]).await;
    assert_eq!(raw, b"*2\r\n:2\r\n$1\r\nc\r\n");
    // 2 > 9 为假 → 不删
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"DELIFGREATER", b"ek2", b"2"]).await),
      ":0"
    );

    // 字符串键位图族既有读写语义不回归
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SET", b"sb", b"\x00"]).await),
      "+OK"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"SETBIT", b"sb", b"7", b"1"]).await),
      ":0"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"GETBIT", b"sb", b"7"]).await),
      ":1"
    );
    assert_eq!(line_reply(&cmd(&mut s, &[b"BITCOUNT", b"sb"]).await), ":1");
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"BITPOS", b"sb", b"1"]).await),
      ":7"
    );
    assert_eq!(
      bulk_reply(&cmd(&mut s, &[b"GET", b"sb"]).await),
      Some(b"\x01".to_vec())
    );
    // 缺失键位图口径：GETBIT :0 / BITCOUNT :0 / BITPOS 找 1 回 -1
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"GETBIT", b"gone", b"0"]).await),
      ":0"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"BITCOUNT", b"gone"]).await),
      ":0"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"BITPOS", b"gone", b"1"]).await),
      ":-1"
    );
  });
  server.stop();
}
