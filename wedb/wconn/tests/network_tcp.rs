//! TCP 网络泵与发送门集成测试（归位自 wconn::network）
//!
//! 对标 C# test/standalone/Garnet.test/NetworkTests.cs
//! 覆盖合包滞留应答就地认领、全双工写饥饿防护、对端 EOF 断连可见性与在途准入闸背压。

use std::{future::pending, sync::Arc, time::Duration};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpListener,
  runtime::spawn,
  time::{sleep, timeout},
};
use wconn::{client::GarnetClient, network::encode_command};

/// 字节序列子串匹配（帧内容无歧义，直接搜键名）
fn contains_bytes(hay: &[u8], needle: &[u8]) -> bool {
  hay.windows(needle.len()).any(|w| w == needle)
}

/// 合包滞留回归：静默假端点读首帧后一次性合包写回两条应答，此后不再读
/// socket、不发任何字节。第二条命令的应答此时已在 read_buf 缓冲，读泵必须
/// 就地认领滞留应答（5s 超时兜底转失败）
#[compio::test]
async fn coalesced_reply_drain() {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();

  // 首帧长度由编码器算出，累计读容忍 TCP 分段
  let mut probe = Vec::new();
  encode_command(&mut probe, &["GET", "k1"]);
  let frame_len = probe.len();

  // 假端点：读首帧 → 合包写回两条应答（+OK 认领首命令，$2\r\nv2 滞留
  // read_buf 等第二条命令）→ 永久静默（连接保持打开不关）
  spawn(async move {
    let (mut sock, _) = listener.accept().await.unwrap();
    let mut acc = Vec::new();
    let mut buf = vec![0u8; 1024];
    while acc.len() < frame_len {
      let BufResult(res, next) = sock.read(buf).await;
      buf = next;
      let n = res.unwrap();
      assert!(n > 0, "对端提前关闭");
      acc.extend_from_slice(&buf[..n]);
    }
    sock
      .write_all(b"+OK\r\n$2\r\nv2\r\n".to_vec())
      .await
      .0
      .unwrap();
    pending::<()>().await;
  })
  .detach();

  let mut client = GarnetClient::new(addr, None, None, None, 16, 0).unwrap();
  client.connect_async().await.unwrap();

  // 首命令：一次读事件带回 r1+r2，r1 认领后 r2 滞留 read_buf
  let r1 = client
    .execute_for_string_result_async(&["GET", "k1"])
    .await
    .unwrap();
  assert_eq!(r1, "OK");

  // 第二条命令：应答已在缓冲且端点静默——滞留应答必须被就地认领
  let r2 = timeout(
    Duration::from_secs(5),
    client.execute_for_string_result_async(&["GET", "k2"]),
  )
  .await
  .unwrap_or_else(|_| panic!("合包滞留应答未被消费，第二条命令超时"))
  .unwrap();
  assert_eq!(r2, "v2");
}

/// 写饥饿回归（全双工）：端点收到首命令后不回任何应答，两条并发命令帧都
/// 必须到达端点。旧行为在途应答未全部认领前不排空新命令，第二命令帧永不
/// 被写出，2s 超时兜底转失败
#[compio::test]
async fn pipeline_flush_while_reply_pending() {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();

  spawn(async move {
    let (mut sock, _) = listener.accept().await.unwrap();
    // 两条命令帧都应到达；期间不回任何应答（挂起全部在途）
    let mut acc = Vec::new();
    let mut buf = vec![0u8; 1024];
    let arrived = timeout(Duration::from_secs(2), async {
      while !(contains_bytes(&acc, b"k1") && contains_bytes(&acc, b"k2")) {
        let BufResult(res, next) = sock.read(buf).await;
        buf = next;
        let n = res.unwrap();
        assert!(n > 0, "对端提前关闭");
        acc.extend_from_slice(&buf[..n]);
      }
    })
    .await;
    assert!(
      arrived.is_ok(),
      "在途应答挂起期间第二命令未被写出（写饥饿回归）"
    );
    sock.write_all(b"+OK\r\n+OK\r\n".to_vec()).await.0.unwrap();
    pending::<()>().await;
  })
  .detach();

  let mut client = GarnetClient::new(addr, None, None, None, 16, 0).unwrap();
  client.connect_async().await.unwrap();
  let client = Arc::new(client);

  // 双任务并发投递两条命令，均挂起等应答
  let c1 = Arc::clone(&client);
  let h1 = spawn(async move { c1.execute_for_string_result_async(&["GET", "k1"]).await });
  let c2 = Arc::clone(&client);
  let h2 = spawn(async move { c2.execute_for_string_result_async(&["GET", "k2"]).await });
  let (r1, r2) = (h1.await.unwrap(), h2.await.unwrap());
  assert_eq!(r1.unwrap(), "OK");
  assert_eq!(r2.unwrap(), "OK");
}

/// 断连传播：对端断链（EOF）后写泵在收场粒度（250ms 超时窗）内退出并
/// 丢弃 rx，is_connected 翻假
#[compio::test]
async fn eof_marks_disconnected() {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();

  spawn(async move {
    let (mut sock, _) = listener.accept().await.unwrap();
    sock.shutdown().await.unwrap();
  })
  .detach();

  let mut client = GarnetClient::new(addr, None, None, None, 16, 0).unwrap();
  client.connect_async().await.unwrap();
  assert!(client.is_connected());

  // 有界轮询至断连对外可见（读泵 EOF 退出 → 写泵超时窗收场 → rx 丢弃，
  // 存活轮询粒度 250ms，5s 上界）
  for _ in 0..200 {
    if !client.is_connected() {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }
  assert!(!client.is_connected());
}

/// 在途准入闸回归（闸 2）：静默端点期间——前三帧到达（闸满挂起点先刷出，
/// 回收链保活），第四帧不被发出（在途未回收 2 条即闸满，第 3 条挂写入队、
/// 第 4 条停在命令通道，退避而非无限积压）；端点回满 3 应答后回收腾位，
/// 第 3/4 条依次放行，4 条全 Ok
#[compio::test]
async fn gate_backpressure() {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();

  spawn(async move {
    let (mut sock, _) = listener.accept().await.unwrap();
    let mut acc = Vec::new();
    let mut buf = vec![0u8; 1024];
    // 等前三帧到达（闸满挂起点刷出回归）；期间不回任何应答
    let arrived = timeout(Duration::from_secs(2), async {
      while !(contains_bytes(&acc, b"k1")
        && contains_bytes(&acc, b"k2")
        && contains_bytes(&acc, b"k3"))
      {
        let BufResult(res, next) = sock.read(buf).await;
        buf = next;
        let n = res.unwrap();
        assert!(n > 0, "对端提前关闭");
        acc.extend_from_slice(&buf[..n]);
      }
    })
    .await;
    assert!(
      arrived.is_ok(),
      "闸满挂起期间前三命令帧未被写出（挂起点刷出回归）"
    );
    assert!(!contains_bytes(&acc, b"k4"), "闸 2 不应放出第 4 帧");
    // 静默保持：客户端命令在退避等待
    sleep(Duration::from_millis(50)).await;
    sock
      .write_all(b"+OK\r\n+OK\r\n+OK\r\n".to_vec())
      .await
      .0
      .unwrap();
    // 回收腾位后第 3/4 条依次放行，第 4 帧随后到达（独立读缓冲，
    // 前一 async 块已 move 走旧缓冲）
    let mut buf = vec![0u8; 1024];
    let fourth = timeout(Duration::from_secs(2), async {
      while !contains_bytes(&acc, b"k4") {
        let BufResult(res, next) = sock.read(buf).await;
        buf = next;
        let n = res.unwrap();
        assert!(n > 0, "对端提前关闭");
        acc.extend_from_slice(&buf[..n]);
      }
    })
    .await;
    assert!(fourth.is_ok(), "回收腾位后第 4 命令未被放行");
    sock.write_all(b"+OK\r\n".to_vec()).await.0.unwrap();
    pending::<()>().await;
  })
  .detach();

  let mut client = GarnetClient::new(addr, None, None, None, 2, 0).unwrap();
  client.connect_async().await.unwrap();
  let client = Arc::new(client);

  // 四任务并发投递（无握手命令，全带应答）
  let handles: Vec<_> = ["k1", "k2", "k3", "k4"]
    .iter()
    .map(|k| {
      let c = Arc::clone(&client);
      let key = (*k).to_string();
      spawn(async move { c.execute_for_string_result_async(&["GET", &key]).await })
    })
    .collect();

  // 退避判定在端点侧：静默窗内第 4 帧不被发出（闸失效时写泵无阻拦，
  // k4 必与前三帧同批到达，端点的 !contains_bytes 判定即失败）

  // 回收放行：4 条全 Ok（兜底 5s；端点回满 4 应答后逐条结算）
  for (i, h) in handles.into_iter().enumerate() {
    let r = timeout(Duration::from_secs(5), h)
      .await
      .unwrap_or_else(|_| panic!("回收后第 {} 条命令未放行", i + 1))
      .unwrap();
    assert_eq!(r.unwrap(), "OK");
  }
}
