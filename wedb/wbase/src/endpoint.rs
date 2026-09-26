//! 套接字端点判定单源：Unix 域套接字路径形态与 typed 回环判定的区分规则
//!
//! C# 侧端点形态由 `EndPoint` 子类型承载（`libs/client/GarnetClient.cs` 的
//! `EndPoint is not UnixDomainSocketEndPoint` 门即读该类型），字符串 → 形态的
//! 判定按本仓自有约定（显式 `unix:` 前缀 / 路径前缀 / `.sock` 后缀）在此单点
//! 实现，入站与出站两个方向共读同一条规则，不另立第二套：
//! - 入站：`wnode::endpoint::ServerEndpoint::parse`（监听端点解析）
//! - 出站：`wconn` 的 `GarnetClient` / `GarnetClientSession` 建连分派

use std::{
  net::{IpAddr, SocketAddr},
  path::Path,
};

/// 显式 Unix 域套接字前缀标记
const UNIX_PREFIX: &str = "unix:";
/// 按路径形态识别为 Unix 域套接字的后缀
const SOCK_SUFFIX: &str = ".sock";

/// 端点串形态判定：`Some` 为 Unix 域套接字路径，`None` 为 TCP 地址串
///
/// 判定规则（全仓唯一一处）：去空白后以 `unix:` 起头者剥前缀取路径，或以 `/`、
/// `./` 起头、以 `.sock` 结尾者整串取路径；其余交调用方按 TCP 地址解析。
/// 返回值借用入参，调用方需要持久形态时自行落 `PathBuf`
pub fn uds_path(s: &str) -> Option<&Path> {
  let trimmed = s.trim();
  if let Some(rest) = trimmed.strip_prefix(UNIX_PREFIX) {
    return Some(Path::new(rest));
  }
  if trimmed.starts_with('/') || trimmed.starts_with("./") || trimmed.ends_with(SOCK_SUFFIX) {
    return Some(Path::new(trimmed));
  }
  None
}

/// typed 回环判定：对位 C# `System.Net.IPAddress.IsLoopback`
///
/// C# 网络层本地判定锚（`libs/common/Networking/TcpNetworkHandlerBase.cs:42-44`
/// 的 `remoteEndpoint is IPEndPoint ip ? IPAddress.IsLoopback(ip.Address) : ...`
/// IP 臂）。口径：IPv4 为 127.0.0.0/8 全段；IPv6 为 ::1，且 v4-mapped IPv6
/// （`::ffff:a.b.c.d`）先解映射再按 IPv4 口径判（.NET 文档明载行为，
/// `Ipv6Addr::to_ipv4_mapped` 对位）。accept 侧自 typed `SocketAddr` 一次性
/// 折叠为布尔，判定端绝不回落字符串再解析
#[inline]
pub fn ip_is_loopback(addr: SocketAddr) -> bool {
  match addr.ip() {
    IpAddr::V4(v4) => v4.is_loopback(),
    IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
      Some(mapped) => mapped.is_loopback(),
      None => v6.is_loopback(),
    },
  }
}
