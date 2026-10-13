#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 嵌套集合深度熔断闭环测试（应答方向字符串数组臂 `* ~ >` 嵌套元素自递归）
//!
//! 对端深嵌套帧（每层最小入帧 `*1\r\n` 仅 4 字节）若无深度上限即无界递归、
//! 栈溢出整进程 abort（不可捕获）；现码经 read_array_with 骨架连接级层数
//! 计数熔断（上限 128 层），超限判 Error::UnexpectedToken 走读泵既有断连
//! 收场。本文件经 GarnetClient 生产链路（ReplyTx::Array → parse_array →
//! 字符串数组读臂）+ 假端点回帧做端到端闭环：深嵌套拒收 Err、浅嵌套回归
//! 正常解析，全程无桩。

use std::future::pending;

use compio::{io::AsyncWriteExt, net::TcpListener, runtime::spawn};
use wconn::client::GarnetClient;

/// 构造 `*1\r\n` 自嵌套 `n` 层、叶元素为 bulk 的应答帧（顶层即第 1 层）
fn nested_frame(n: usize, leaf: &[u8]) -> Vec<u8> {
  let mut frame = Vec::with_capacity(n * 4 + leaf.len());
  for _ in 0..n {
    frame.extend_from_slice(b"*1\r\n");
  }
  frame.extend_from_slice(leaf);
  frame
}

/// 假端点：接受一条连接后原样写回应答帧并永久静默（不消费请求帧，小帧
/// 内核缓冲足够），返回监听地址
async fn serve_reply(reply: Vec<u8>) -> String {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  spawn(async move {
    let (mut sock, _) = listener.accept().await.unwrap();
    // _sock 随 BufResult 归还并驻留至任务挂起，连接保持存活至进程退出，
    // 杜绝 EOF 抢先收场干扰深度熔断判定
    let compio::BufResult(res, _sock) = sock.write_all(reply).await;
    res.unwrap();
    pending::<()>().await;
  })
  .detach();
  addr
}

/// 建连并执行一条数组应答命令
async fn fetch_array(addr: String) -> wconn::Result<Vec<String>> {
  let mut client = GarnetClient::new(addr, None, None, None, 16, 0).unwrap();
  client.connect_async().await.unwrap();
  client
    .execute_for_string_array_result_async(&["GET", "k"])
    .await
}

/// 数万层深嵌套帧（约 400KB 载荷）：修复前该帧使读泵无界自递归至 guard
/// page SIGSEGV，测试进程整体 abort；修复后骨架超 128 层即判协议错误、
/// 读泵沿既有 Err 收场断连，命令以 Err 结算且进程存活（走到断言即未 abort）
#[compio::test]
async fn deep_nested_frame_rejected_not_abort() {
  let reply = nested_frame(100_000, b"$1\r\nx\r\n");
  let addr = serve_reply(reply).await;
  let res = fetch_array(addr).await;
  assert!(res.is_err(), "超限深嵌套帧必须拒收断连，而非栈溢出 abort");
}

/// 边界回归——恰 128 层嵌套为上限内合法应答：必须完整解析而非误杀
#[compio::test]
async fn nest_at_limit_parses() {
  let reply = nested_frame(128, b"$1\r\nx\r\n");
  let addr = serve_reply(reply).await;
  let res = fetch_array(addr).await;
  assert_eq!(res.unwrap(), vec!["x".to_string()], "128 层嵌套应正常解析");
}

/// 边界回归——第 129 层起超限：立即拒收断连（与 128 层放行成对锁死上限口径）
#[compio::test]
async fn nest_over_limit_rejected() {
  let reply = nested_frame(129, b"$1\r\nx\r\n");
  let addr = serve_reply(reply).await;
  let res = fetch_array(addr).await;
  assert!(res.is_err(), "129 层嵌套应判协议错误断连");
}

/// 浅嵌套回归闭环：多层混合内容（嵌套数组聚簇、兄弟 bulk 与整数、null
/// 数组嵌套）解析语义不变（嵌套子数组按 ", " 聚簇为单元素字符串）
#[compio::test]
async fn shallow_nesting_regression() {
  // 顶层 3 元：嵌套 [*1 $a]、bulk $b、整数 :5 → ["a", "b", "5"]
  let reply = b"*3\r\n*1\r\n$1\r\na\r\n$1\r\nb\r\n:5\r\n".to_vec();
  let addr = serve_reply(reply).await;
  let res = fetch_array(addr).await;
  assert_eq!(
    res.unwrap(),
    vec!["a".to_string(), "b".to_string(), "5".to_string()]
  );

  // 嵌套 null 数组元素：[*1 *-1] → 子数组 None 折叠为空串
  let reply = b"*1\r\n*-1\r\n".to_vec();
  let addr = serve_reply(reply).await;
  let res = fetch_array(addr).await;
  assert_eq!(res.unwrap(), vec![String::new()]);

  // 三层 RESP3 混合 sigil 嵌套：*3 → ~1 (>:1 +push) → $leaf 逐层聚簇
  let reply = b"*3\r\n~1\r\n>1\r\n+push\r\n*-2\r\n#t\r\n".to_vec();
  let addr = serve_reply(reply).await;
  let res = fetch_array(addr).await;
  assert_eq!(
    res.unwrap(),
    vec!["push".to_string(), String::new(), "t".to_string()]
  );
}
