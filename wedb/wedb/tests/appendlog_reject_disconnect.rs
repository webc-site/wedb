//! APPENDLOG 拒收断流端到端测试（对标 C# ReplicationSession.cs 断流与拒收语义）
//!
//! 验证副本接收面对拒收与畸形帧的断流语义（对齐 C#
//! `GarnetException(clientResponse: false)` 上抛 → RespServerSession catch
//! → DisposeNetworkSender）：
//! 1. divergent 拒收：连接被断（EOF），而非回 -ERR 错误行保连；
//! 2. 畸形帧（`*-1` 前导）：协议错误行写出后断连（Err 透传，不按半包等待）；
//! 3. 主端 fire-and-forget 推流滞留 -ERR 应答 → 连接判失效断连（重同步链路入口）；
//! 4. 真副本 divergent 拒收静默断链 → 主端发送通道健康面转断连（重同步入口）。

use std::{net::SocketAddr, num::NonZeroUsize, str::from_utf8, sync::Arc, time::Duration};

use aok::{Result, Void};
use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::{TcpListener, TcpStream},
  runtime::{Runtime, spawn},
};
use tempfile::{TempDir, tempdir};
use waof::{WalConfig, WalLog};
use wbase::pool::{DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL, LimitedFixedBufferPool};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    cluster_replication_session::ClusterReplicationSession, replica_wire::TcpSessionWire,
  },
  worker::{LocalWorkerSpec, NodeRole},
};
use wnode::{GarnetServer, SessionProviderFace, WireFormat};
use wtest_base::wait_for;

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 头区占位记录负载（8B 头 + 56B 负载 = 64B，副本尾位基线）
const HEAD_PAD_PAYLOAD: usize = 56;

/// 装配副本角色 provider（REPLICA of primary_1）
fn replica_provider() -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  let cm = provider.cluster_manager().expect("cm ready");
  cm.try_initialize_local_worker(LocalWorkerSpec {
    node_id: REPLICA_ID,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(PRIMARY_ID),
    hostname: None,
  });
  provider
}

/// 装配副本接收会话（WAL 头区就位，尾位 = REPLICA_TAIL）
fn replica_session() -> Result<(TempDir, ClusterReplicationSession<SegmentedDevice>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("replica.wal"),
  )?);
  let wal = Arc::new(WalLog::new(device, WalConfig::default())?);
  wal.enqueue(&[0u8; HEAD_PAD_PAYLOAD])?;
  let session = ClusterReplicationSession::new(replica_provider(), wal, None);
  Ok((dir, session))
}

/// APPENDLOG 初始化帧（三方地址 -1/-1/-1；节点 id 为 32 字符小写 hex）
fn init_frame() -> Vec<u8> {
  concat!(
    "*7\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$32\r\n",
    "0de10000000000000000000000000001",
    "\r\n$1\r\n0\r\n$2\r\n-1\r\n$2\r\n-1\r\n$2\r\n-1\r\n"
  )
  .into()
}

/// APPENDLOG 记录帧（current 位点与副本尾位不符 → divergent 拒收）
fn divergent_frame() -> Vec<u8> {
  concat!(
    "*8\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$32\r\n",
    "0de10000000000000000000000000001",
    "\r\n$1\r\n0\r\n$2\r\n64\r\n$4\r\n9000\r\n$4\r\n9064\r\n$3\r\nxyz\r\n"
  )
  .into()
}

struct SessionProvider(ClusterReplicationSession<SegmentedDevice>);
impl SessionProviderFace for SessionProvider {
  type Consumer = ClusterReplicationSession<SegmentedDevice>;
  fn get_session(
    &self,
    _wire_format: WireFormat,
    _network_sender_id: u64,
  ) -> Option<ClusterReplicationSession<SegmentedDevice>> {
    Some(self.0.clone())
  }
}

/// divergent 拒收断流：init 握手成功后记录帧位点不符 → 连接被断（EOF），
/// 不回 -ERR 错误行保连（C# clientResponse:false 口径）
#[test]
fn divergent_appendlog_disconnects_instead_of_error_reply() -> Void {
  let (_dir, session) = replica_session()?;
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    65536,
    100,
    Arc::new(SessionProvider(session)),
  )?;
  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?.to_string();

  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>()?).await?;
    // init 握手：+OK
    stream.write_all(init_frame()).await.unwrap();
    let mut acc = Vec::new();
    read_exactly(&mut stream, &mut acc, 5).await;
    assert_eq!(&acc, b"+OK\r\n", "初始化帧应答 +OK");

    // divergent 记录帧：连接必须被断（EOF），不得回错误行保连
    stream.write_all(divergent_frame()).await.unwrap();
    let mut tail = Vec::new();
    loop {
      let BufResult(res, buf) = stream.read(vec![0u8; 1024]).await;
      let n = res?;
      if n == 0 {
        break;
      }
      tail.extend_from_slice(&buf[..n]);
    }
    assert!(
      tail.is_empty(),
      "拒收不得写应答行（clientResponse:false），实际收到 {tail:?}"
    );
    aok::Result::<()>::Ok(())
  })?;
  server.dispose();
  Ok(())
}

/// 畸形帧断流：`*-1\r\n` 负数组长度经 Err 通道透传（不按半包等待），
/// 协议错误行写出后连接即断
#[test]
fn malformed_negative_array_frame_disconnects() -> Void {
  let (_dir, session) = replica_session()?;
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    65536,
    100,
    Arc::new(SessionProvider(session)),
  )?;
  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?.to_string();

  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>()?).await?;
    stream.write_all(b"*-1\r\n".to_vec()).await.unwrap();
    let mut acc = Vec::new();
    loop {
      let BufResult(res, buf) = stream.read(vec![0u8; 1024]).await;
      let n = res?;
      if n == 0 {
        break;
      }
      acc.extend_from_slice(&buf[..n]);
    }
    assert!(
      acc.starts_with(b"-ERR Protocol Error"),
      "畸形帧应写协议错误行后断连，实际 {acc:?}"
    );
    aok::Result::<()>::Ok(())
  })?;
  server.dispose();
  Ok(())
}

/// 真副本拒收断连（主端通道健康面感知）：init 握手成功后 divergent 记录帧
/// → 副本会话静默断链（EOF，clientResponse:false）→ TcpSessionWire 健康面
/// 转断连（对标 C# 异常断链 → 主端剔除重同步）
#[test]
fn primary_wire_divergent_appendlog_disconnects() -> Void {
  let (_dir, session) = replica_session()?;
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    65536,
    100,
    Arc::new(SessionProvider(session)),
  )?;
  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?.to_string();

  Runtime::new().unwrap().block_on(async {
    let wire = TcpSessionWire::connect(
      &addr,
      PRIMARY_ID,
      0,
      None,
      None,
      LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL),
      #[cfg(feature = "tls")]
      None,
    )
    .await?;
    // init 帧握手成功（connect 内已确认 +OK），通道保持健康
    assert!(wire.is_connected());

    // divergent 记录帧（current 位点与副本尾不符）：fire-and-forget 发送，
    // 副本拒收静默断链 → 主端通道健康面转断连
    wire.append_log(PRIMARY_ID, 0, 64, 9000, 9064, b"xyz")?;
    assert!(
      wait_for(|| !wire.is_connected(), Duration::from_secs(5)).await,
      "副本拒收断链后主端通道必须转断连态"
    );
    aok::Result::<()>::Ok(())
  })?;
  server.dispose();
  Ok(())
}

/// 主端推流滞留 -ERR 断连：服务端对 fire-and-forget 记录帧回错误应答（旧
/// bug 行为的兼容感知），wconn 泵在应答队列空时发现滞留错误行 → 判流失效
/// 断连 → TcpSessionWire 健康面转断连（触发主端剔除副本转入重同步）
#[compio::test]
async fn primary_wire_disconnects_on_stray_error_reply() -> Void {
  // 握手（SETINFO/SETNAME）+ init 帧各回 +OK，其后对记录帧回 -ERR
  let addr = reject_after_handshake_node().await;
  let wire = TcpSessionWire::connect(
    &addr,
    PRIMARY_ID,
    0,
    None,
    None,
    LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL),
    #[cfg(feature = "tls")]
    None,
  )
  .await?;
  assert!(wire.is_connected(), "建连后发送通道健康");

  // fire-and-forget 记录帧：发送成功（通道不饱和），错误应答由泵感知
  wire.append_log(PRIMARY_ID, 0, 64, 64, 128, b"record")?;
  assert!(
    wait_for(|| !wire.is_connected(), Duration::from_secs(5)).await,
    "滞留 -ERR 应答必须判流失效断连（而非滞留污染连接）"
  );
  Ok(())
}

/// 假副本节点：逐帧解析 RESP2 数组，前 3 帧（CLIENT SETINFO、SETNAME、
/// APPENDLOG init）回 +OK，此后每帧回 `-ERR divergent`（模拟拒收应答）
async fn reject_after_handshake_node() -> String {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  spawn(async move {
    while let Ok((mut stream, _)) = listener.accept().await {
      spawn(async move {
        let mut acc: Vec<u8> = Vec::new();
        let mut ok_left = 3usize;
        let mut buf = vec![0u8; 4096];
        loop {
          let BufResult(res, next) = stream.read(buf).await;
          buf = next;
          let n = match res {
            Ok(n) if n > 0 => n,
            _ => break,
          };
          acc.extend_from_slice(&buf[..n]);
          while let Some(frame_len) = try_parse_frame(&acc) {
            acc.drain(..frame_len);
            let reply: &[u8] = if ok_left > 0 {
              ok_left -= 1;
              b"+OK\r\n"
            } else {
              b"-ERR divergent aof stream\r\n"
            };
            if stream.write_all(reply.to_vec()).await.is_err() {
              return;
            }
          }
        }
      })
      .detach();
    }
  })
  .detach();
  addr
}

/// 从缓冲解析一个完整 RESP2 数组帧（*N\r\n + N 个 $len\r\npayload\r\n），
/// 返回帧总字节数；不完整返回 None
fn try_parse_frame(buf: &[u8]) -> Option<usize> {
  if buf.first() != Some(&b'*') {
    return None;
  }
  let header_end = buf.iter().position(|b| *b == b'\n')? + 1;
  let argc: usize = from_utf8(&buf[1..header_end - 2]).ok()?.parse().ok()?;
  let mut pos = header_end;
  for _ in 0..argc {
    if buf.get(pos) != Some(&b'$') {
      return None;
    }
    let len_line_end = buf[pos + 1..].iter().position(|b| *b == b'\n')? + pos + 2;
    let len: usize = from_utf8(&buf[pos + 1..len_line_end - 2])
      .ok()?
      .parse()
      .ok()?;
    pos = len_line_end + len + 2;
    if pos > buf.len() {
      return None;
    }
  }
  Some(pos)
}

/// 期望字节数读完（累计，容忍 TCP 分段）
async fn read_exactly(stream: &mut TcpStream, acc: &mut Vec<u8>, expect: usize) {
  while acc.len() < expect {
    let BufResult(res, buf) = stream.read(vec![0u8; 1024]).await;
    let n = res.unwrap();
    assert!(n > 0, "对端提前关闭（已收 {} 字节）", acc.len());
    acc.extend_from_slice(&buf[..n]);
  }
}
