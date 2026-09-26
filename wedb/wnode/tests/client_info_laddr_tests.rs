//! CLIENT LIST / CLIENT INFO laddr 端到端确定性用例（明文 TCP/UDS 面）
//!
//! C# 对标 libs/common/Networking/TcpNetworkHandlerBase.cs 构造期捕获
//! socket.LocalEndPoint（localEndpointName）→ LIST/INFO 两命令共读
//! networkSender.LocalEndpointName，laddr 恒有值：TCP 为 `ip:port`、
//! Unix 为监听套接字路径。本用例以真实监听端点逐值断言（非前缀、非假
//! mock）。TLS 臂见 wtls/tests/client_laddr.rs。
//!
//! CLIENT LIST 走 `ConsumerRegistry::global()` 进程级单例（对齐 C#
//! `Server is GarnetServerBase` 单服务器语义），两臂共用同一 provider
//!（同一注册表）且收敛在单测试函数内，杜绝跨用例单例互扰。

use std::{io, num::NonZeroUsize, sync::Arc};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::{TcpStream, UnixStream},
  runtime::Runtime,
};
use tempfile::tempdir;
use wnode::{
  GarnetServer, RespSessionConsumer, resp::resp_server_session::RespServerSessionOptions,
  service::StorageSessionProvider,
};
use wnode_test::complete_len;
use wtest_base::test_store_config;

/// 写出命令载荷（RESP 数组帧；泛型形态以共用 TCP/UDS 两真实夹具）
///
/// 写后即 flush（明文 TCP/UDS 的 flush 为空操作，两形态同一路径）
async fn send_cmd<S: AsyncWriteExt>(stream: &mut S, args: &[&[u8]]) -> io::Result<()> {
  let BufResult(res, _) = stream.write_all(wtest_base::resp_frame(args)).await;
  res?;
  stream.flush().await
}

/// 读取一条完整 RESP 应答（帧完整性判定复用 wnode_test::complete_len）
async fn read_reply<S: AsyncRead>(stream: &mut S) -> Vec<u8> {
  let mut acc = Vec::new();
  let mut buf = vec![0u8; 4096];
  loop {
    let BufResult(res, returned) = stream.read(buf).await;
    buf = returned;
    match res {
      Ok(0) | Err(_) => return acc,
      Ok(n) => {
        acc.extend_from_slice(&buf[..n]);
        if complete_len(&acc).is_some() {
          return acc;
        }
      }
    }
  }
}

/// bulk 帧载荷文本（CLIENT INFO/LIST 均 bulk 应答）
fn bulk_body(frame: &[u8]) -> String {
  let nl = frame.iter().position(|&b| b == b'\n').expect("bulk header");
  let len: usize = String::from_utf8_lossy(&frame[1..nl - 1])
    .parse()
    .expect("bulk len");
  String::from_utf8(frame[nl + 1..nl + 1 + len].to_vec()).expect("utf8 body")
}

/// 行内字段提取（`key=<value>` 至下一空格；空值返回 None）
fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
  line
    .split(' ')
    .find_map(|f| f.strip_prefix(key))
    .filter(|v| !v.is_empty())
}

/// 行内字段原始提取（`key=<value>` 至下一空格；保留空值 Some("")）
fn field_raw<'a>(line: &'a str, key: &str) -> Option<&'a str> {
  line.split(' ').find_map(|f| f.strip_prefix(key))
}

#[test]
fn client_info_laddr_tcp_uds_match_listen_endpoint() -> aok::Result<()> {
  let dir = tempdir()?;
  let uds_path = dir.path().join("l.sock");
  let uds_str = uds_path.display().to_string();
  let provider = Arc::new(StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join("node.db"),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    },
  )?);
  // 同一台服务器双端点监听（TCP 端口 0 随机 + Unix 域套接字路径）：注册表为
  // 进程级单例，本函数是文件内唯一用例，函数内顺序断言不与他用例互扰
  let server = Arc::new(GarnetServer::new(
    &["127.0.0.1:0".to_string(), uds_str.clone()],
    1 << 16,
    8,
    Arc::clone(&provider),
  )?);
  server.start(NonZeroUsize::new(1))?;
  let tcp_addr = server.local_addr()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    // ── TCP 臂：CLIENT INFO 的 laddr 逐值等于监听地址 ──
    let mut tcp = TcpStream::connect(tcp_addr).await?;
    send_cmd(&mut tcp, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut tcp).await, b"+PONG\r\n");
    send_cmd(&mut tcp, &[b"CLIENT", b"INFO"]).await?;
    let info = bulk_body(&read_reply(&mut tcp).await);
    let tcp_laddr = field(&info, "laddr=").expect("TCP CLIENT INFO laddr 非空");
    assert_eq!(
      tcp_laddr,
      tcp_addr.to_string(),
      "TCP CLIENT INFO laddr 须等于监听端点: {info}"
    );

    // ── UDS 臂：CLIENT INFO 的 laddr 等于监听套接字路径，addr 为空串
    //（C# UnixDomainSocketEndPoint.ToString 对未命名对端为空串，仅本端为路径）──
    let mut uds = UnixStream::connect(&uds_path).await?;
    send_cmd(&mut uds, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut uds).await, b"+PONG\r\n");
    send_cmd(&mut uds, &[b"CLIENT", b"INFO"]).await?;
    let uds_info = bulk_body(&read_reply(&mut uds).await);
    let uds_laddr = field(&uds_info, "laddr=").expect("UDS CLIENT INFO laddr 非空");
    assert_eq!(
      uds_laddr, uds_str,
      "UDS CLIENT INFO laddr 须等于监听套接字路径: {uds_info}"
    );
    assert_eq!(
      field_raw(&uds_info, "addr="),
      Some(""),
      "UDS CLIENT INFO addr 须为空串: {uds_info}"
    );

    // ── CLIENT LIST：登记行与 INFO 同源实值（条目 local_endpoint 共一次
    // 取值，UDS 行 addr= 为空、laddr= 为监听路径）──
    send_cmd(&mut uds, &[b"CLIENT", b"LIST"]).await?;
    let list = bulk_body(&read_reply(&mut uds).await);
    let tcp_line = list
      .lines()
      .find(|l| field(l, "laddr=") == Some(tcp_addr.to_string().as_str()))
      .expect("CLIENT LIST 含 laddr=监听地址 的 TCP 行");
    assert!(
      field(tcp_line, "addr=").is_some_and(|a| a.starts_with("127.0.0.1:")),
      "TCP 行 addr 为客户端临时端点: {tcp_line}"
    );
    let uds_line = list
      .lines()
      .find(|l| field(l, "laddr=") == Some(uds_str.as_str()))
      .expect("CLIENT LIST 含 laddr=监听套接字路径 的 UDS 行");
    assert_eq!(
      field_raw(uds_line, "addr="),
      Some(""),
      "CLIENT LIST UDS 行 addr 须为空串: {uds_line}"
    );
    assert_eq!(
      field(uds_line, "laddr="),
      Some(uds_str.as_str()),
      "CLIENT LIST UDS 行 laddr 须等于监听套接字路径: {uds_line}"
    );

    // ── CLIENT KILL ADDR 过滤回归：以监听路径过滤对 UDS 会话零命中（对标 C# 空串不匹配）──
    send_cmd(&mut uds, &[b"CLIENT", b"KILL", b"ADDR", uds_str.as_bytes()]).await?;
    assert_eq!(
      read_reply(&mut uds).await,
      b":0\r\n",
      "CLIENT KILL ADDR <监听路径> 对 UDS 会话须零命中（addr 为空串）"
    );

    Ok::<(), io::Error>(())
  })?;
  server.dispose();
  Ok(())
}
