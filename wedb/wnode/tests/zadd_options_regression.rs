#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! ZADD 选项组合回归测试
//!
//! 对标 libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:GetOptions:39-87
//! （XX&NX 互斥、GT&LT&NX 互斥、INCR 仅单对）与 SortedSetAdd:89-213
//! （INCR+GT 拒更低分值时 WriteNull 且分值不变、XX+INCR 成员缺席时
//! WriteNull+return 短路整条命令 :136-146）：
//! 1. ZADD key INCR GT <低于既有分值> member → nil 且既有分值不变；
//! 2. ZADD key XX NX / GT LT NX → 互斥错误文案；
//! 3. ZADD key XX INCR <缺席成员> → nil（RESP2 $-1 / RESP3 _）且不新增
//!    成员；成员存活 → 正常 double 帧不变。

use std::sync::Arc;

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wnode_test::{cmd, open_aof_provider, start_server};

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
  let provider = open_aof_provider(&data_path);
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

/// ZADD XX+INCR 成员缺席 → nil（C# SortedSetObjectImpl.cs:136-146 缺席臂
/// WriteNull+return 短路整条命令，真 Redis 同为 nil；INCR 恒单对由 RESP 层
/// 校验拦截，故 RESP 面可观察锁形即单对缺席 null 帧）；成员存活 → 正常
/// double 帧。RESP2/RESP3 双协议对位（RESP3 null 为 `_`、分值为 `,num`）。
#[test]
fn zadd_xx_incr_absent_member_null_short_circuits_command() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("zadd_xx_incr.db");
  let provider = open_aof_provider(&data_path);
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");

    // 前置：ZADD z 10 a
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"ZADD", b"z", b"10", b"a"]).await),
      ":1"
    );

    // XX+INCR 成员缺席 → nil（非 "0"），且不新增成员。C# :136-146 为
    // WriteNull+return 短路整条命令；INCR 恒单对（RESP 层校验拦截多对），
    // 故 RESP 面可观察锁形即单对缺席 null 帧
    assert_eq!(
      cmd(&mut s, &[b"ZADD", b"z", b"XX", b"INCR", b"5", b"ghost"]).await,
      b"$-1\r\n",
      "XX INCR 缺席成员须回 nil（RESP2 $-1）"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"ZCARD", b"z"]).await),
      ":1",
      "XX INCR 缺席不得新增成员"
    );

    // 对照：XX+INCR 成员存活 → 正常叠加 double 帧（10 + 5 = 15）
    assert_eq!(
      cmd(&mut s, &[b"ZADD", b"z", b"XX", b"INCR", b"5", b"a"]).await,
      b"$2\r\n15\r\n",
      "XX INCR 存活成员须回叠加结果 double 帧"
    );

    // ---- RESP3 会话（HELLO 3 后 null 为 `_`、分值为 `,num`）----
    let _ = cmd(&mut s, &[b"HELLO", b"3"]).await;
    assert_eq!(
      cmd(&mut s, &[b"ZADD", b"z", b"XX", b"INCR", b"5", b"ghost2"]).await,
      b"_\r\n",
      "XX INCR 缺席成员须回 nil（RESP3 _）"
    );
    assert_eq!(
      line_reply(&cmd(&mut s, &[b"ZCARD", b"z"]).await),
      ":1",
      "RESP3 侧同样不得新增成员"
    );
    assert_eq!(
      cmd(&mut s, &[b"ZADD", b"z", b"XX", b"INCR", b"5", b"a"]).await,
      b",20\r\n",
      "RESP3 侧存活成员回 ,num double 帧"
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
  let provider = open_aof_provider(&data_path);
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
