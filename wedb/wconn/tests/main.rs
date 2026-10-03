use std::time::Duration;

use aok::{OK, Void};
use compio::{
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::{Runtime, spawn},
  time::{sleep, timeout},
};
use log::info;
use wconn::{client::GarnetClient, session::GarnetClientSession};

/// GarnetClient / GarnetClientSession 执行口与存活管理命令面链路验证
///
/// r7c 裁决后 api.rs 只存留生产存活面（replica_of + info），原基础 RESP /
/// List / SortedSet 便捷封装已删——同覆盖改走底层执行口
/// （execute_for_string_result_async / execute_for_string_array_result_async）
#[test]
fn test_garnet_client_and_session_apis() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let endpoint = addr.to_string();

    spawn(async move {
      while let Ok((mut stream, _)) = listener.accept().await {
        spawn(async move {
          let mut buf = vec![0u8; 4096];
          loop {
            let compio::BufResult(res, b) = stream.read(buf).await;
            buf = b;
            let n = match res {
              Ok(n) if n > 0 => n,
              _ => break,
            };
            let req = &buf[..n];
            let resp: &[u8] = if req.windows(6).any(|w| w.eq_ignore_ascii_case(b"LRANGE")) {
              b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"
            } else if req.windows(4).any(|w| w.eq_ignore_ascii_case(b"MGET")) {
              b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
            } else if req.windows(9).any(|w| w.eq_ignore_ascii_case(b"REPLICAOF")) {
              b"+OK\r\n"
            } else if req.windows(4).any(|w| w.eq_ignore_ascii_case(b"PING")) {
              b"+PONG\r\n"
            } else if req.windows(4).any(|w| w.eq_ignore_ascii_case(b"INFO")) {
              b"$4\r\ninfo\r\n"
            } else if req.windows(3).any(|w| w.eq_ignore_ascii_case(b"GET")) {
              b"$5\r\nhello\r\n"
            } else {
              b"+OK\r\n"
            };
            let compio::BufResult(res, _) = stream.write_all(resp.to_vec()).await;
            if res.is_err() {
              break;
            }
          }
        })
        .detach();
      }
    })
    .detach();

    let mut client = GarnetClient::new(endpoint.clone(), None, None, None, 64, 0).unwrap();
    client.connect_async().await?;

    // 底层执行口三形：行应答 / bulk 应答 / 数组应答
    assert_eq!(
      client.execute_for_string_result_async(&["PING"]).await?,
      "PONG"
    );
    assert_eq!(
      client
        .execute_for_string_result_async(&["GET", "key"])
        .await?,
      "hello"
    );
    assert_eq!(
      client
        .execute_for_string_array_result_async(&["MGET", "k1", "k2"])
        .await?,
      vec!["hello".to_string(), "world".to_string()]
    );

    // 存活管理命令面（api.rs 生产存活面）；INFO 无便捷壳，走底层执行口
    assert_eq!(
      client.execute_for_string_result_async(&["INFO"]).await?,
      "info"
    );
    assert_eq!(client.replica_of("127.0.0.1", 6379).await?, "OK");

    // 会话透传命令
    let mut session = GarnetClientSession::new(endpoint, None, None, None, None);
    session.connect_async().await?;
    // 会话层存活透传口（CLUSTER ATTACH_SYNC 帧往返；行/bulk/数组三形已由
    // 上方 GarnetClient 底层执行口断言覆盖）
    assert_eq!(session.execute_cluster_attach_sync(b"meta").await?, "OK");

    info!("GarnetClient 与 GarnetClientSession 执行口链路验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 滞留错误应答断连（APPENDLOG fire-and-forget 帧拒收感知）：
/// 记录帧不注册应答等待（协议约定无应答），服务端违规回写 `-ERR` 错误行
/// 无人认领滞留 read_buf → 网络泵空闲探测发现 → 判流失效退出 → 会话
/// `is_connected` 转假（C# 连接异常 → AofSyncTask IsConnected → 重同步
/// 的等价感知面；防主端 shipped_watermark 静默推进）
#[test]
fn stray_error_reply_after_fire_and_forget_disconnects() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = listener.local_addr()?.to_string();

    // 假服务端：对任意到达批次回一条错误应答（模拟副本拒收记录帧）
    spawn(async move {
      while let Ok((mut stream, _)) = listener.accept().await {
        spawn(async move {
          let mut buf = vec![0u8; 4096];
          loop {
            let compio::BufResult(res, b) = stream.read(buf).await;
            buf = b;
            let n = match res {
              Ok(n) if n > 0 => n,
              _ => break,
            };
            let _ = &buf[..n];
            let compio::BufResult(res, _) = stream
              .write_all(b"-ERR divergent aof stream\r\n".to_vec())
              .await;
            if res.is_err() {
              break;
            }
          }
        })
        .detach();
      }
    })
    .detach();

    let mut session = GarnetClientSession::new(endpoint, None, None, None, None);
    session.connect_async().await?;

    // fire-and-forget 记录帧：发出即忘（无应答注册），错误应答滞留由泵感知
    session.execute_cluster_append_log("primary_1", 0, 64, 64, 128, b"record")?;

    // 泵空闲探测周期 250ms：限 5s 内判失效断连
    let poll = timeout(Duration::from_secs(5), async {
      while session.is_connected() {
        sleep(Duration::from_millis(20)).await;
      }
    })
    .await;
    assert!(poll.is_ok(), "滞留 -ERR 应答未判失效：连接仍被视为健康");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
