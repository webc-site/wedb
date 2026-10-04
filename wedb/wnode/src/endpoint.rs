//! 服务器监听端点抽象：支持 TCP 与 Unix Domain Socket (UDS)
//!
//! 1:1 对标微软 Garnet EndPoints 与 UnixDomainSocketEndPoint

use std::{
  fmt,
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
  path::PathBuf,
};

use wbase::endpoint::uds_path;

use crate::error::{Error, Result};

/// 服务器监听端点类型
#[derive(Debug, PartialEq, Eq)]
pub enum ServerEndpoint {
  /// TCP 网络端点（IPv4 / IPv6）
  Tcp(SocketAddr),
  /// Unix Domain Socket 本地路径
  Unix(PathBuf),
}

impl ServerEndpoint {
  /// 从字符串解析端点定义列表
  ///
  /// 对标 libs/common/Format.cs:TryCreateEndpoint：
  /// - Unix 域套接字按 [`wbase::endpoint::uds_path`] 产出单项 [`ServerEndpoint::Unix`]；
  /// - `localhost` 显式映射双回环 `[127.0.0.1:port, [::1]:port]`（Format.cs:99-100 defaultBindLoopBack）；
  /// - 其余按 TCP 经 `to_socket_addrs` 迭代全收，消除 `.next()` 截断（Format.cs:135）；
  /// - 支持 `:6379` 简写补齐 `0.0.0.0`，支持未包裹方括号的 IPv6 端点规范化为 `[addr]:port`。
  pub fn parse_many(s: &str) -> Result<Vec<Self>> {
    let trimmed = s.trim();
    if let Some(path) = uds_path(trimmed) {
      return Ok(vec![Self::Unix(path.to_path_buf())]);
    }

    // localhost 特判：对标 Format.cs:99-100，显式展开为 IPv4 + IPv6 双回环
    if let Some((host, port_str)) = trimmed.rsplit_once(':')
      && host.eq_ignore_ascii_case("localhost")
    {
      let port: u16 = port_str
        .parse()
        .map_err(|e| Error::AddrParse(format!("无法解析端点 '{trimmed}': {e}")))?;
      return Ok(vec![
        Self::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)),
        Self::Tcp(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port)),
      ]);
    }

    // 处理 `:port` 缺省 IP 前缀与未包裹方括号的 IPv6 端点规范化
    let normalized = if let Some(port_str) = trimmed.strip_prefix(':')
      && !port_str.starts_with(':')
      && port_str.parse::<u16>().is_ok()
    {
      format!("0.0.0.0:{port_str}")
    } else if !trimmed.starts_with('[')
      && let Some((host, port_str)) = trimmed.rsplit_once(':')
      && host.parse::<Ipv6Addr>().is_ok()
      && port_str.parse::<u16>().is_ok()
    {
      format!("[{host}]:{port_str}")
    } else {
      trimmed.to_string()
    };

    let addrs: Vec<SocketAddr> = normalized
      .to_socket_addrs()
      .map_err(|e| Error::AddrParse(format!("无法解析端点 '{trimmed}': {e}")))?
      .collect();

    if addrs.is_empty() {
      return Err(Error::AddrParse(format!(
        "无法解析端点 '{trimmed}' 为有效套接字地址"
      )));
    }

    let mut seen = Vec::with_capacity(addrs.len());
    for addr in addrs {
      if !seen.contains(&addr) {
        seen.push(addr);
      }
    }
    Ok(seen.into_iter().map(Self::Tcp).collect())
  }
}

impl fmt::Display for ServerEndpoint {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Tcp(addr) => write!(f, "{addr}"),
      Self::Unix(path) => write!(f, "unix:{}", path.display()),
    }
  }
}
