//! INFO BPSTATS 段 server_socket 行接线端到端（工单
//! wnode-bpstats-server-socket-lines-missing）
//!
//! C# 对标 GarnetInfoMetrics.cs:408-416 PopulateClusterBufferPoolStats：
//! 逐 TCP server 恒出 `server_socket_{i}` 行（值＝networkPool.GetStats()），
//! clusterProvider 非空再追加集群行。修复前 wnode 会话侧只投影集群行，
//! standalone 默认面（BpStats 在 DEFAULT_INFO 段集）BPSTATS 段仅剩段头。
//!
//! 测试一、二走真实节点装配全链：监听器泵经 NetworkHandler::set_session
//! 向会话注入宿主共享池（net/handler/mod.rs:112-115），TCP 会话 INFO 的
//! server_socket_0 行自该池真值出——杜绝假 mock。测试三以轻量会话挂真实
//! 池与哨兵集群切面，锁集群形态 socket 行序在集群行前的拼装契约。

use std::sync::Arc;

use compio::{net::TcpStream, runtime::Runtime};
use wbase::pool::LimitedFixedBufferPool;
use wnode::{
  ClusterProvider,
  resp::resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::{read_reply, send_cmd, start_node};
use wresp::metrics::MetricsItem;

/// bulk 帧载荷文本（INFO 应答）
fn bulk_body(frame: &[u8]) -> String {
  let nl = frame.iter().position(|&b| b == b'\n').expect("bulk header");
  let len: usize = String::from_utf8_lossy(&frame[1..nl - 1])
    .parse()
    .expect("bulk len");
  String::from_utf8(frame[nl + 1..nl + 1 + len].to_vec()).expect("utf8 body")
}

/// 截取 INFO 文本中指定段（段头起至下一段头前的 `\r\n` 止）
fn section<'a>(text: &'a str, header: &str) -> &'a str {
  let head = format!("# {header}\r\n");
  let from = text
    .find(&head)
    .unwrap_or_else(|| panic!("缺 {header} 段: {text}"))
    + head.len();
  let to = text[from..]
    .find("\r\n# ")
    .map_or(text.len(), |off| from + off + 2);
  &text[from..to]
}

/// 真实节点全链：BPSTATS 显式段请求出 server_socket_0 行与段头，行值取
/// 宿主共享池统计真值非空（C# GarnetServerTcp.GetBufferPoolStats 对位）
#[test]
fn info_bpstats_renders_server_socket_row_over_real_listener() -> aok::Result<()> {
  let rt = Runtime::new()?;
  let (_dir, _server, addr) = start_node();
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await?;
    // 预热：泵在建连后首个数据帧才创建并注册会话
    send_cmd(&mut s, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut s).await, b"+PONG\r\n");

    send_cmd(&mut s, &[b"INFO", b"BPSTATS"]).await?;
    let info = bulk_body(&read_reply(&mut s).await);
    assert!(
      info.starts_with("# BufferPoolStats\r\n"),
      "应有 BufferPoolStats 段头: {info}"
    );
    let row = info
      .lines()
      .find(|l| l.starts_with("server_socket_0:"))
      .unwrap_or_else(|| panic!("缺 server_socket_0 行: {info}"));
    assert!(
      row.len() > "server_socket_0:".len(),
      "统计文本应非空: {row}"
    );
    Ok(())
  })
}

/// standalone 默认 INFO（无参 DEFAULT_INFO 段集含 BPSTATS）：段非仅段头，
/// server_socket_0 行在场（修复前空表悬空只剩头的回潮锁）
#[test]
fn standalone_default_info_bpstats_section_beyond_bare_header() -> aok::Result<()> {
  let rt = Runtime::new()?;
  let (_dir, _server, addr) = start_node();
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await?;
    send_cmd(&mut s, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut s).await, b"+PONG\r\n");

    send_cmd(&mut s, &[b"INFO"]).await?;
    let info = bulk_body(&read_reply(&mut s).await);
    let seg = section(&info, "BufferPoolStats");
    assert!(
      seg.contains("server_socket_0:"),
      "默认 INFO BPSTATS 段应非仅段头: {seg}"
    );
    Ok(())
  })
}

/// 集群形态哨兵切面（ClusterProviderFace 其余方法全默认体，仅覆盖池统计投影）
struct SentinelClusterProvider;

impl ClusterProvider for SentinelClusterProvider {
  fn get_buffer_pool_stats(&self) -> Vec<MetricsItem> {
    vec![MetricsItem::new("sentinel_cluster_bp", "CLUSTER_BP")]
  }
}

/// 会话直填一批字节并取回应答文本（网络泵替身，对标
/// resp_server_session_tests 的 pump 形态；BPSTATS 为纯同步段无停车臂）
fn pump(s: &mut RespServerSession, bytes: &[u8]) -> String {
  s.recv_buffer.extend_from_slice(bytes);
  s.try_consume_messages();
  let mut out = Vec::new();
  s.take_output_into(&mut out);
  String::from_utf8(out).expect("INFO 应答应为 UTF-8 文本")
}

/// 集群形态拼装序：真实池出的 socket 行排在集群切面行前
/// （C# :411-415 先逐 server 填充再追加集群行的次序）
#[test]
fn cluster_form_socket_row_precedes_cluster_rows() {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.attach_buffer_pool(LimitedFixedBufferPool::new(4096, 4));
  s.attach_cluster_provider(Arc::new(SentinelClusterProvider));

  let text = pump(&mut s, b"*2\r\n$4\r\nINFO\r\n$7\r\nBPSTATS\r\n");
  let socket = text
    .find("server_socket_0:")
    .unwrap_or_else(|| panic!("缺 server_socket_0 行: {text}"));
  let cluster = text
    .find("sentinel_cluster_bp:")
    .unwrap_or_else(|| panic!("缺集群行: {text}"));
  assert!(
    socket < cluster,
    "socket 行须排在集群行前（C# 拼装序）: {text}"
  );
}
