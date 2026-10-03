//! 客户端在途命令超时集成测试（GarnetClient timeout_millis → TimeoutChecker）
//!
//! 假 server 收帧后不回包不断链（假死连接），三断言：
//! - timeout_millis=0 时命令挂起不返回（旋钮关闭零行为变化）；
//! - 设小超时时在途命令按 Error::Timeout 返回；
//! - 超时后 is_connected 翻假且后续命令即刻失败。

use std::{future::pending, sync::Arc, time::Duration};

use compio::{
  buf::BufResult,
  io::AsyncRead,
  net::TcpListener,
  runtime::spawn,
  time::{sleep, timeout},
};
use wconn::{Error, client::GarnetClient, network::timeout_checker, types::PumpProgress};

/// 在途超时收场总时限：判成周期 + 读泵限时读粒度（250ms）+ 余量
const TIMEOUT_BUDGET: Duration = Duration::from_secs(5);

/// 假 server：accept 一条连接、读满首帧后永久静默（不回包不关闭）
async fn silent_server() -> String {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  spawn(async move {
    let (mut sock, _) = listener.accept().await.unwrap();
    let buf = vec![0u8; 4096];
    let BufResult(res, _) = sock.read(buf).await;
    assert!(res.unwrap() > 0, "命令帧未到达假 server");
    // 假死：连接保持打开，此后不读不回（TCP 不 RST、对端不收不回）
    pending::<()>().await;
  })
  .detach();
  addr
}

/// 旋钮关闭（timeout_millis=0）：假死连接下命令挂起不返回，外层限时窗
/// 到期证明未收场（判成机制未启用，无拆客户端行为）
#[compio::test]
async fn timeout_disabled_keeps_command_pending() {
  let mut client = GarnetClient::new(silent_server().await, None, None, None, 16, 0).unwrap();
  client.connect_async().await.unwrap();
  assert!(client.is_connected());

  let probe = timeout(
    Duration::from_millis(80),
    client.execute_for_string_result_async(&["GET", "k"]),
  )
  .await;
  assert!(probe.is_err(), "timeout_millis=0 时命令应保持挂起不返回");
}

/// 旋钮开启：在途命令按 Error::Timeout 结算，超时后连接态翻假、
/// 后续命令即刻失败（对标 C# 判成 Dispose 后在途 TCS 全部置错）
#[compio::test]
async fn in_flight_timeout_fails_commands_and_marks_disconnected() {
  let mut client = GarnetClient::new(silent_server().await, None, None, None, 16, 100).unwrap();
  client.connect_async().await.unwrap();
  assert!(client.is_connected());

  let res = timeout(
    TIMEOUT_BUDGET,
    client.execute_for_string_result_async(&["GET", "k"]),
  )
  .await
  .unwrap_or_else(|_| panic!("在途超时未在 {TIMEOUT_BUDGET:?} 内收场"))
  .unwrap_err();
  assert!(
    matches!(res, Error::Timeout),
    "在途命令应按 Error::Timeout 结算，实际 {res:?}"
  );

  // 有界轮询至写泵收场、断连对外可见（存活轮询粒度 250ms，5s 上界）
  for _ in 0..200 {
    if !client.is_connected() {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }
  assert!(!client.is_connected(), "超时后 is_connected 应翻假");

  let next = client.execute_for_string_result_async(&["GET", "k2"]).await;
  assert!(next.is_err(), "断连后后续命令应即刻失败");
}

/// 建连失败（对端不可达）不残留检查任务：旋钮开启 + 真实拒绝端口
/// 127.0.0.1:1，connect_async 即刻失败；同 client 随后重连假死 server，
/// 检查任务按新连接的周期正常判成收场——spawn 时序回归形态（检查任务先于
/// connect spawn）下，失败路径 progress 三计数恒 0、退休标志永不置位，
/// 检查任务空转泄漏且判成周期错乱
#[compio::test]
async fn connect_failure_spawns_no_orphan_checker() {
  let mut client = GarnetClient::new("127.0.0.1:1".into(), None, None, None, 16, 40).unwrap();
  assert!(
    client.connect_async().await.is_err(),
    "无监听端口建连应失败"
  );
  assert!(!client.is_connected(), "失败路径不应残留连接态");

  // 同 client 重连假死 server：新连接的检查任务在位，在途按超时结算
  client.end_point = silent_server().await;
  client
    .connect_async()
    .await
    .expect("重连假死 server 应成功");
  let res = timeout(
    TIMEOUT_BUDGET,
    client.execute_for_string_result_async(&["GET", "k"]),
  )
  .await
  .unwrap_or_else(|_| panic!("重连后在途超时未在 {TIMEOUT_BUDGET:?} 内收场"))
  .unwrap_err();
  assert!(
    matches!(res, Error::Timeout),
    "重连后在途命令应按 Error::Timeout 结算，实际 {res:?}"
  );
}

/// 退休退场机制：retire 置位后检查任务在界内结束（spawn 时序回归的机制级
/// 钉子——spawn 先于 connect 时，建连失败路径退休标志永不置位，任务空转泄漏）
#[compio::test]
async fn timeout_checker_exits_once_retired() {
  let progress = Arc::new(PumpProgress::new());
  let handle = spawn(timeout_checker(
    Arc::clone(&progress),
    Duration::from_millis(10),
  ));
  progress.retire();
  timeout(Duration::from_secs(2), handle)
    .await
    .expect("退休置位后检查任务应在界内退场")
    .expect("退场路径应正常返回");
}

/// 反向判别：未退休、无在途无进展（建连失败路径的 progress 形态）时任务
/// 跨多个判成周期持续存活不退场——保证上一用例的退场断言有判别力
#[compio::test]
async fn timeout_checker_keeps_spinning_when_not_retired() {
  let progress = Arc::new(PumpProgress::new());
  let mut handle = spawn(timeout_checker(
    Arc::clone(&progress),
    Duration::from_millis(10),
  ));
  let exited = timeout(Duration::from_millis(35), &mut handle)
    .await
    .is_ok();
  assert!(!exited, "未退休且在途恒空时检查任务应持续存活");
  progress.retire();
  timeout(Duration::from_secs(2), handle)
    .await
    .expect("随后退休置位即应界内退场")
    .expect("退场路径应正常返回");
}
