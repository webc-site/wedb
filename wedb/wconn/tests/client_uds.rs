//! Unix 域套接字出站链路集成测试（GarnetClient / GarnetClientSession 的建连分派臂）
//!
//! 假服务端以 `compio::net::UnixListener` 绑在 tempdir 下的套接字文件上（与
//! wnode 入站 UDS 同一形态），断言三件：
//! - 端点串两形态（显式 `unix:` 前缀 / `.sock` 结尾裸路径）均判为 UDS 并成功建连，
//!   经 UDS 完成 AUTH 握手与命令回环应答（C# 侧 UDS 往返的最小对位面）；
//! - 会话层同型可达（C# 两个客户端类共用同一形态门）；
//! - UDS 形态端点绝不回落 TCP 臂：按端点判 UDS 的两臂互斥，`set_nodelay` 只在
//!   TCP 臂上施加，故建连失败的错误形态（ENOENT 而非地址解析错）即分派落点的
//!   可观测面，不为断言另开生产接口。

use std::{io::ErrorKind, path::PathBuf};

use aok::{OK, Void};
use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::UnixListener,
  runtime::{Runtime, spawn},
};
use tempfile::TempDir;
use wconn::{Error, client::GarnetClient, session::GarnetClientSession};

/// 假 UDS 服务端：绑一枚监听器到 tempdir 下的套接字文件，按到达帧回 RESP 简单串
///
/// 返回 tempdir 守卫与套接字路径：守卫须在断言期内存活（drop 即连目录一并删除）
async fn fake_uds_server() -> (TempDir, PathBuf) {
  let dir = TempDir::new().expect("创建临时目录");
  let path = dir.path().join("garnet_uds.sock");
  let listener = UnixListener::bind(&path).await.expect("绑定假 UDS 监听器");

  spawn(async move {
    while let Ok((mut stream, _)) = listener.accept().await {
      spawn(async move {
        let mut buf = vec![0u8; 4096];
        loop {
          let BufResult(res, b) = stream.read(buf).await;
          buf = b;
          let n = match res {
            Ok(n) if n > 0 => n,
            _ => break,
          };
          let req = &buf[..n];
          let resp: &[u8] = if req.windows(4).any(|w| w.eq_ignore_ascii_case(b"PING")) {
            b"+PONG\r\n"
          } else {
            b"+OK\r\n"
          };
          let BufResult(res, _) = stream.write_all(resp.to_vec()).await;
          if res.is_err() {
            break;
          }
        }
      })
      .detach();
    }
  })
  .detach();

  (dir, path)
}

/// 显式 `unix:` 前缀端点：客户端经 UDS 完成 AUTH 握手 + PING 回环
#[test]
fn client_handshake_and_roundtrip_over_unix_prefixed_endpoint() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, path) = fake_uds_server().await;
    let endpoint = format!("unix:{}", path.display());

    let mut client = GarnetClient::new(endpoint, None, Some("secret".into()), None, 16, 0)?;
    client.connect_async().await?;
    assert!(client.is_connected(), "UDS 建连后连接态应为真");
    assert_eq!(
      client.execute_for_string_result_async(&["PING"]).await?,
      "PONG"
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 裸 `.sock` 路径端点（无 `unix:` 前缀）：会话层同一条分派规则同型可达
#[test]
fn session_roundtrip_over_bare_sock_path_endpoint() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, path) = fake_uds_server().await;

    let mut session =
      GarnetClientSession::new(path.to_string_lossy().into_owned(), None, None, None);
    session.connect_async().await?;
    assert!(session.is_connected(), "UDS 建连后会话连接态应为真");
    assert_eq!(session.execute_async(&["PING"]).await?, "PONG");
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// UDS 形态端点必落 UDS 臂：缺失的套接字文件按 ENOENT 收场（TCP 臂只会报地址
/// 解析错），两臂互斥即 nodelay 只可能落在 TCP 臂
#[test]
fn uds_shaped_endpoint_never_falls_back_to_tcp() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = TempDir::new().expect("创建临时目录");
    let absent = dir.path().join("absent.sock");

    let mut client = GarnetClient::new(
      absent.to_string_lossy().into_owned(),
      None,
      None,
      None,
      16,
      0,
    )?;
    let err = client
      .connect_async()
      .await
      .expect_err("缺失套接字文件应建连失败");
    match err {
      Error::Io(e) => assert_eq!(
        e.kind(),
        ErrorKind::NotFound,
        "UDS 臂应报 ENOENT，实际错误形态失真: {e:?}"
      ),
      other => panic!("UDS 臂建连失败应为透明转发的 IO 错误，实际 {other:?}"),
    }
    assert!(!client.is_connected(), "建连失败后不得残留伪连接态");
    aok::Result::<()>::Ok(())
  })?;
  OK
}
