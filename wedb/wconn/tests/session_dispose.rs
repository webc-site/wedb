//! 会话层拆连面集成测试（对标 C# ClientSession GarnetClientSession 的
//! Dispose 全臂拆连契约：幂等守卫 + socket 无条件拆 fd + 池缓冲归还）
//!
//! 断言组（与 client 侧 dispose_reclaim.rs 同判据形制，会话面独立成册）：
//! 半开静默对端下 dispose 收场（is_connected 幂等位翻假、读泵收场、池借出
//! 归还、后续命令即刻失败、二次 dispose 无害）、最后持有者落下 Drop 兜底
//! 收场、在途命令随拆连以断连错误结算、正常请求-响应链 dispose 零回归。

use std::{future::pending, sync::Arc, time::Duration};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::spawn,
  time::{sleep, timeout},
};
use wbase::pool::{DEFAULT_MAX_ENTRIES_PER_LEVEL, LimitedFixedBufferPool};
use wconn::{Error, network::READ_CHUNK, session::GarnetClientSession};

/// 收场总时限：双向拆连为内核即刻唤醒（毫秒级），写泵存活轮询粒度 250ms，
/// 5s 覆盖重载机器调度抖动（dispose_reclaim.rs 同档先例）
const SETTLE_BUDGET: Duration = Duration::from_secs(5);

/// 有界轮询断言面：粒度 25ms、上界 [`SETTLE_BUDGET`]，到期谓词仍假即调用方断言失败
async fn poll_until(cond: impl Fn() -> bool) -> bool {
  for _ in 0..200 {
    if cond() {
      return true;
    }
    sleep(Duration::from_millis(25)).await;
  }
  cond()
}

/// 静默对端：accept 后持有 socket 永久静默（裸半开形态，不发不断）。
/// 必须持有 accept 出的 socket——其随任务 drop 会立即回 RST/EOF，半开前提即消失
async fn silent_server() -> String {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  spawn(async move {
    let (_sock, _peer) = listener.accept().await.unwrap();
    pending::<()>().await;
  })
  .detach();
  addr
}

/// 注入池 + 静默对端建连的会话，返回已连接会话与池（稳态读泵恒为单块池借出）
async fn half_open_session() -> (GarnetClientSession, Arc<LimitedFixedBufferPool>) {
  let pool = LimitedFixedBufferPool::new(READ_CHUNK, DEFAULT_MAX_ENTRIES_PER_LEVEL);
  let mut session = GarnetClientSession::new(
    silent_server().await,
    None,
    None,
    None,
    Some(Arc::clone(&pool)),
  );
  session.connect_async().await.unwrap();
  assert!(session.is_connected(), "半开建连应报告已连接");
  assert!(
    poll_until(|| pool.borrowed_count() == 1).await,
    "读泵未进入稳态挂读（池借出未就绪）"
  );
  (session, pool)
}

/// dispose 收场主断言：静默对端半开连接，dispose 后读泵任务即时收场——池借出
/// 缓冲 RAII 归池、连接态翻假、后续命令以断连失败不挂起、二次 dispose 幂等无害
#[compio::test]
async fn dispose_settles_session_read_pump_on_silent_peer() {
  let (session, pool) = half_open_session().await;

  session.dispose();
  assert!(
    !session.is_connected(),
    "dispose 后 is_connected 应即刻翻假（幂等位，对标 C# Disposed 守卫）"
  );

  assert!(
    poll_until(|| pool.borrowed_count() == 0).await,
    "读泵任务未在 {SETTLE_BUDGET:?} 内收场：池借出缓冲未归还（静默对端悬挂立案面）"
  );
  assert_eq!(pool.free_count(), 1, "接收缓冲必须归池复用");

  let next = timeout(SETTLE_BUDGET, session.execute_async(&["PING"])).await;
  assert!(
    matches!(next, Ok(Err(_))),
    "dispose 后后续命令应即刻失败，实际 {next:?}"
  );

  session.dispose();
}

/// 最后持有者落下兜底：Drop 仅转发同一拆连面（C# 收口经持有方显式 Dispose 链，
/// rust 以 Drop 承接最终收口）
#[compio::test]
async fn session_drop_settles_read_pump_on_silent_peer() {
  let (session, pool) = half_open_session().await;

  drop(session);
  assert!(
    poll_until(|| pool.borrowed_count() == 0).await,
    "Drop 兜底未在 {SETTLE_BUDGET:?} 内令读泵收场：池借出缓冲未归还"
  );
  assert_eq!(pool.free_count(), 1, "接收缓冲必须归池复用");
}

/// 在途命令拆连结算：dispose 令在途 oneshot 随在途通道销毁断连，roundtrip
/// 以既有断连错误 ResponseChannelClosed 结算，不挂起
#[compio::test]
async fn dispose_settles_in_flight_command_as_disconnect() {
  let (session, _pool) = half_open_session().await;
  let session = Arc::new(session);

  let late = Arc::clone(&session);
  let discharger = spawn(async move {
    sleep(Duration::from_millis(50)).await;
    late.dispose();
  });
  let res = timeout(SETTLE_BUDGET, session.execute_async(&["PING"]))
    .await
    .unwrap_or_else(|_| panic!("在途命令未在 {SETTLE_BUDGET:?} 内随拆连收场"));
  discharger.await.unwrap();

  let err = res.expect_err("在途命令应随拆连以断连错误结算，不应 Ok");
  assert!(
    matches!(err, Error::ResponseChannelClosed),
    "拆连结算应为断连错误形态 ResponseChannelClosed，实际 {err:?}"
  );
}

/// 正对照零回归：正常请求-响应链（假 RESP 端点逐帧回 +PONG）多轮往返后
/// dispose——应答全部正常结算、连接态翻假、池借出归还、拆连后命令即刻失败
#[compio::test]
async fn dispose_after_normal_roundtrip_zero_regression() {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let endpoint = listener.local_addr().unwrap().to_string();
  spawn(async move {
    let (mut sock, _peer) = listener.accept().await.unwrap();
    let mut buf = vec![0u8; 4096];
    loop {
      let BufResult(res, next) = sock.read(buf).await;
      buf = next;
      match res {
        Ok(n) if n > 0 => {}
        // 对端拆连收场（EOF/err）：端点退场
        _ => break,
      }
      let BufResult(res, _) = sock.write_all(b"+PONG\r\n".to_vec()).await;
      if res.is_err() {
        break;
      }
    }
  })
  .detach();

  let pool = LimitedFixedBufferPool::new(READ_CHUNK, DEFAULT_MAX_ENTRIES_PER_LEVEL);
  let mut session = GarnetClientSession::new(endpoint, None, None, None, Some(Arc::clone(&pool)));
  session.connect_async().await.unwrap();

  for i in 0..8 {
    let resp = session
      .execute_async(&["PING"])
      .await
      .unwrap_or_else(|e| panic!("第 {i} 轮正常往返不应回归: {e}"));
    assert_eq!(resp, "PONG", "第 {i} 轮应答文本应逐轮结算");
    assert_eq!(pool.borrowed_count(), 1, "稳态恒为连接级单次池借出");
  }

  session.dispose();
  assert!(!session.is_connected(), "dispose 后 is_connected 应翻假");
  assert!(
    poll_until(|| pool.borrowed_count() == 0).await,
    "正常连接 dispose 后读泵未收场：池借出缓冲未归还"
  );
  assert_eq!(pool.allocated_count(), 1, "零回归：稳态未产生第二块新分配");

  let next = session.execute_async(&["PING"]).await;
  assert!(next.is_err(), "拆连后命令应即刻失败");
}
