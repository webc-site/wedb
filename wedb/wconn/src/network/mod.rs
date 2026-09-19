//! 客户端网络全双工读写双泵：命令帧编码 + 发送/接收解耦循环
//!
//! 在 garnet 中的相对路径:
//! - `libs/client/GarnetClient.cs`
//! - `libs/client/ClientSession/GarnetClientSession.cs`
//! - `libs/client/ClientTcpNetworkSender.cs`
//! - `libs/client/GarnetClientProcessReplies.cs`
//!
//! [`GarnetClient`](crate::GarnetClient) 与
//! [`GarnetClientSession`](crate::GarnetClientSession) 共用同一实现。
//! 子模块按 garnet libs/client 分文件拓扑拆分：
//! - [`encode`]：命令帧编码（ClientTcpNetworkSender.cs:Send /
//!   GarnetClientSession.cs:FormatRESP）；
//! - [`replies`]：帧解析与队列派发（GarnetClientProcessReplies.cs）；
//! - [`pump`]：读写双泵循环（ClientTcpNetworkSender.cs 发送流 +
//!   GarnetClientProcessReplies.cs 读循环）。
//!
//! 对标 libs/client 发送流（GarnetClientTcpNetworkSender 刷 socket）与接收流
//! （libs/client/GarnetClientProcessReplies.cs:ProcessReplies 认领应答）的
//! 解耦模型：[`TcpStream::into_split`] 拆分读写两半，写泵与读泵并发运行
//! （compio 单线程运行时内双任务，io_uring 并发 SQE），发送持续可推进，
//! 不被在途应答的认领进度串行阻滞；读泵独立读应答并按帧序认领回传
//! oneshot（C# TaskCompletionSource 的零锁等价物）。
//!
//! 退出闭环：读泵 EOF/协议错误退出 → 复位存活标志 → 写泵在限时收命令的
//! 超时窗内感知并退出、丢弃 rx，发送端经 `is_connected` 观测断连；调用方
//! 全部退场 → 写泵命令通道断连、shutdown 写半 → 读泵 EOF 收场，fd 两半
//! 全 drop 关闭连接（此路径属计划内关闭，不上抛错误）。

mod encode;
mod pump;
mod replies;
pub(crate) mod stream;

use crossfire::oneshot;
pub(crate) use encode::encode_command;
pub(crate) use pump::{network_loop, resolve_network_pool};

use crate::{
  Result,
  types::{ChannelTx, CommandItem, ReplyTx, roundtrip},
};

/// 执行单条字符串命令往返
///
/// 在 garnet 中的相对路径: libs/client/GarnetClient.cs:ExecuteForStringResultAsync
async fn exec(tx: &ChannelTx, command: &[&str]) -> Result<String> {
  let (resp_tx, resp_rx) = oneshot::oneshot();
  let item = CommandItem::new(command, ReplyTx::Str(resp_tx));
  roundtrip(tx, item, resp_rx, None).await
}

/// 连接后握手序列：AUTH（用户名优先，缺省密码按空串补齐）+ CLIENT SETINFO/SETNAME
///
/// （C# ConnectAsync 内的 AUTH/CLIENT SETINFO 握手子步骤，客户端与会话共用同一口径，SETINFO/SETNAME 同以 clientName 非空为前提）
pub(super) async fn handshake(
  tx: &ChannelTx,
  lib_name: &str,
  auth_username: Option<&str>,
  auth_password: Option<&str>,
  client_name: Option<&str>,
) -> Result<()> {
  if let Some(username) = auth_username {
    let pwd = auth_password.unwrap_or("");
    exec(tx, &["AUTH", username, pwd]).await?;
  } else if let Some(pwd) = auth_password {
    exec(tx, &["AUTH", pwd]).await?;
  }
  if let Some(client_name) = client_name {
    exec(tx, &["CLIENT", "SETINFO", "LIB-NAME", lib_name]).await?;
    exec(tx, &["CLIENT", "SETNAME", client_name]).await?;
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use std::{future::pending, sync::Arc, time::Duration};

  use compio::{
    buf::BufResult,
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    runtime::{Runtime, spawn},
    time::{sleep, timeout},
  };

  use super::*;
  use crate::client::GarnetClient;

  #[test]
  fn encode_command_frame() {
    let mut out = Vec::new();
    encode_command(&mut out, &[b"GET".as_slice(), b"k1".as_slice()]);
    assert_eq!(&out, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n");

    let mut out_str = Vec::new();
    encode_command(&mut out_str, &["GET", "k1"]);
    assert_eq!(&out_str, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n");
  }

  /// 合包滞留回归：静默假端点读首帧后一次性合包写回两条应答，此后不再读
  /// socket、不发任何字节。第二条命令的应答此时已在 read_buf 缓冲，读泵必须
  /// 就地认领滞留应答（5s 超时兜底转失败）
  #[test]
  fn coalesced_reply_drain() {
    Runtime::new().unwrap().block_on(async {
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
    });
  }

  /// 字节序列子串匹配（帧内容无歧义，直接搜键名）
  fn contains_bytes(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
  }

  /// 写饥饿回归（全双工）：端点收到首命令后不回任何应答，两条并发命令帧都
  /// 必须到达端点。旧行为在途应答未全部认领前不排空新命令，第二命令帧永不
  /// 被写出，2s 超时兜底转失败
  #[test]
  fn pipeline_flush_while_reply_pending() {
    Runtime::new().unwrap().block_on(async {
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
    });
  }

  /// 断连传播：对端断链（EOF）后写泵在收场粒度（250ms 超时窗）内退出并
  /// 丢弃 rx，is_connected 翻假
  #[test]
  fn eof_marks_disconnected() {
    Runtime::new().unwrap().block_on(async {
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

      // 读泵 EOF 退出 → 写泵超时窗收场 → rx 丢弃，断连对外可见
      sleep(Duration::from_millis(600)).await;
      assert!(!client.is_connected());
    });
  }

  /// 在途准入闸回归（闸 2）：静默端点期间——前三帧到达（闸满挂起点先刷出，
  /// 回收链保活），第四帧不被发出（在途未回收 2 条即闸满，第 3 条挂写入队、
  /// 第 4 条停在命令通道，退避而非无限积压）；端点回满 3 应答后回收腾位，
  /// 第 3/4 条依次放行，4 条全 Ok
  #[test]
  fn gate_backpressure() {
    Runtime::new().unwrap().block_on(async {
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
        // 静默保持：客户端 4 条命令均须在退避等待（探测窗总时长 < 静默窗）
        sleep(Duration::from_millis(600)).await;
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
    });
  }
}
