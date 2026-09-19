//! 服务器监听端点抽象：支持 TCP 与 Unix Domain Socket (UDS)
//!
//! 1:1 对标微软 Garnet EndPoints 与 UnixDomainSocketEndPoint

use std::{
  fmt,
  net::{SocketAddr, ToSocketAddrs},
  path::PathBuf,
};

use crate::error::{Error, Result};

/// 服务器监听端点类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerEndpoint {
  /// TCP 网络端点（IPv4 / IPv6）
  Tcp(SocketAddr),
  /// Unix Domain Socket 本地路径
  Unix(PathBuf),
}

impl ServerEndpoint {
  /// 从字符串解析端点定义
  ///
  /// - `unix:/path/to.sock` 或以 `/` 或 `./` 开头或以 `.sock` 结尾：识别为 Unix 域套接字
  /// - 其他：解析为 TCP SocketAddr（支持 `:6379` 简写补齐 `0.0.0.0`）
  pub fn parse(s: &str) -> Result<Self> {
    let trimmed = s.trim();
    if let Some(rest) = trimmed.strip_prefix("unix:") {
      return Ok(Self::Unix(PathBuf::from(rest)));
    }
    if trimmed.starts_with('/') || trimmed.starts_with("./") || trimmed.ends_with(".sock") {
      return Ok(Self::Unix(PathBuf::from(trimmed)));
    }

    // 处理 `:port` 缺省 IP 前缀
    let normalized = if let Some(port_str) = trimmed.strip_prefix(':') {
      format!("0.0.0.0:{port_str}")
    } else {
      trimmed.to_string()
    };

    normalized
      .to_socket_addrs()
      .map_err(|e| Error::AddrParse(format!("无法解析端点 '{trimmed}': {e}")))?
      .next()
      .map(Self::Tcp)
      .ok_or_else(|| Error::AddrParse(format!("无法解析端点 '{trimmed}' 为有效套接字地址")))
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_endpoints() {
    let ep_tcp = ServerEndpoint::parse("127.0.0.1:6379").unwrap();
    assert!(matches!(ep_tcp, ServerEndpoint::Tcp(_)));
    assert_eq!(ep_tcp.to_string(), "127.0.0.1:6379");

    let ep_uds = ServerEndpoint::parse("/tmp/wedb.sock").unwrap();
    assert!(matches!(ep_uds, ServerEndpoint::Unix(_)));
    assert_eq!(ep_uds.to_string(), "unix:/tmp/wedb.sock");

    let ep_prefix = ServerEndpoint::parse("unix:/var/run/test.sock").unwrap();
    assert_eq!(ep_prefix.to_string(), "unix:/var/run/test.sock");
  }
}
