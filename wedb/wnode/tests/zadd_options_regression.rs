//! ZADD 选项组合回归测试（next/qwen.data.md 条 7）
//!
//! 对标 libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:GetOptions:39-87
//! （XX&NX 互斥、GT&LT&NX 互斥、INCR 仅单对）与 SortedSetAdd:89-213
//! （INCR+GT 拒更低分值时 WriteNull 且分值不变）：
//! 1. ZADD key INCR GT <低于既有分值> member → nil 且既有分值不变；
//! 2. ZADD key XX NX / GT LT NX → 互斥错误文案。

use std::sync::Arc;

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wnode::service::StorageSessionProvider;
use wnode_test::{cmd, session_factory, start_server};
use wtest_base::test_store_config;

/// 行式应答
fn line_reply(raw: &[u8]) -> String {
  String::from_utf8_lossy(raw).trim_end().to_string()
}

/// libs/server/Resp/CmdStrings.cs:RESP_ERR_XX_NX_NOT_COMPATIBLE
const XX_NX_ERR: &str = "-ERR XX and NX options at the same time are not compatible\r\n";

/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GT_LT_NX_NOT_COMPATIBLE
const GT_LT_NX_ERR: &str = "-ERR GT, LT, and/or NX options at the same time are not compatible\r\n";

/// ZADD key INCR GT <叠加分低于既有> member → nil 且分值不变；INCR GT
/// 高分值 → 叠加结果。XX NX / GT LT NX 互斥错误文案。
#[test]
fn zadd_incr_gt_rejects_lower_score_with_nil() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("zadd_incr_gt.db");
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

    // 前置：ZADD z 10 a
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"ZADD", b"z", b"10", b"a"]).await),
      ":1"
    );

    // INCR GT：先叠加（10 + (-5) = 5）后比较，叠加分 5 < 既有 10 → GT 拒更
    //（SortedSetObjectImpl.rs：GT && score_stored > score → INCR WriteNil）
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"ZADD", b"z", b"INCR", b"GT", b"-5", b"a"]).await),
      "$-1",
      "INCR GT 拒更低分值须回 nil"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"ZSCORE", b"z", b"a"]).await),
      "$2\r\n10",
      "拒绝后既有分值须保持 10 不变"
    );

    // 对照：INCR GT 15（叠加后 25 > 既有 10）→ GT 允许 → 回叠加结果
    let incr = cmd(&mut s, &[b"ZADD", b"z", b"INCR", b"GT", b"15", b"a"]).await;
    assert!(
      String::from_utf8_lossy(&incr).contains("25"),
      "INCR GT 更高分值须回叠加结果 25: {}",
      String::from_utf8_lossy(&incr)
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"ZSCORE", b"z", b"a"]).await),
      "$2\r\n25"
    );
  });
  server.stop();
}

/// ZADD XX NX / GT LT NX 互斥错误文案（GetOptions 判定表镜像）
#[test]
fn zadd_mutex_option_error_messages() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("zadd_mutex.db");
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
      line_reply(&cmd(&mut s, &[b"ZADD", b"z", b"XX", b"NX", b"1", b"m"]).await),
      XX_NX_ERR.trim_end(),
      "XX NX 互斥文案须与 C# CmdStrings 逐字一致"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"ZADD", b"z", b"GT", b"LT", b"NX", b"1", b"m"]).await),
      GT_LT_NX_ERR.trim_end(),
      "GT LT NX 互斥文案须与 C# CmdStrings 逐字一致"
    );

    // 互斥报错不得建键（GetOptions 先于 SortedSetAdd 执行）
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"EXISTS", b"z"]).await),
      ":0",
      "互斥报错路径不得创建键"
    );

    // 尾段偶数检查与 vendored C# 1:1 同在（GetOptions:80-87，upstream #644 起；
    // agy r6-data 条 4 复核裁定保留）：选项后剩余段为空/奇数均回 syntax error。
    // 注意 RESP 层 arity 门（ZADD -4）先行：仅选项形态（ZADD z XX）在 C# 同被
    // arity 表拦为 wrong number of arguments，GetOptions 尾段在 4 参以上才可达
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"ZADD", b"z", b"XX"]).await),
      "-ERR wrong number of arguments for 'ZADD' command",
      "仅选项形态先被 arity 门拦（C# 同款 arity 表）"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"ZADD", b"z", b"XX", b"m1"]).await),
      "-ERR syntax error",
      "选项后奇数尾须回 syntax error（C# GetOptions 尾段同判）"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"EXISTS", b"z"]).await),
      ":0",
      "syntax error 路径同样不得创建键"
    );
  });
  server.stop();
}
