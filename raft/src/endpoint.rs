use std::fmt::{Display, Formatter, Result as FmtResult};
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;

use crate::error::Error;

/// 网络端点，包含主机地址和端口
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, bitcode::Encode, bitcode::Decode)]
pub struct Endpoint {
  pub host: String,
  pub port: u16,
}

impl Endpoint {
  pub fn new(host: impl Into<String>, port: u16) -> Self {
    Self {
      host: host.into(),
      port,
    }
  }

  pub fn host(&self) -> &str {
    &self.host
  }

  pub fn port(&self) -> u16 {
    self.port
  }

  pub fn parse(s: &str) -> Result<Self, Error> {
    s.parse()
  }

  pub fn to_socket_addr(&self) -> Result<SocketAddr, Error> {
    let host = &self.host;
    let port = self.port;
    (host.as_str(), port)
      .to_socket_addrs()
      .map_err(|e| Error::conf(format!("Invalid endpoint {host}:{port}: {e}")))?
      .next()
      .ok_or_else(|| {
        Error::conf(format!(
          "Cannot resolve endpoint to socket address: {host}:{port}"
        ))
      })
  }

  /// 转换为 Zenoh Unsecure QUIC 协议端点格式
  pub fn to_zenoh_endpoint(&self) -> String {
    Self::zenoh_endpoint(self)
  }

  /// 格式化任意地址（IP:PORT / HOST:PORT / Endpoint）为 Zenoh Unsecure QUIC 协议端点（显式启用多流复用）
  pub fn zenoh_endpoint(addr: impl Display) -> String {
    format!("udp/{addr}?rel=1;multistream=1")
  }
}

impl Display for Endpoint {
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    let host = &self.host;
    let port = self.port;
    write!(f, "{host}:{port}")
  }
}

impl FromStr for Endpoint {
  type Err = Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    if let Some((host, port_str)) = s.rsplit_once(':') {
      if host.contains('=') {
        return Err(Error::conf(format!(
          "Invalid endpoint '{s}': host '{host}' contains invalid '=' character"
        )));
      }
      let host = host.trim_start_matches('[').trim_end_matches(']');
      let port = port_str
        .parse::<u16>()
        .map_err(|e| Error::conf(format!("Invalid port '{port_str}' in endpoint '{s}': {e}")))?;
      return Ok(Self {
        host: host.to_string(),
        port,
      });
    }

    Err(Error::conf(format!(
      "Invalid endpoint format '{s}': expected IP:PORT or HOST:PORT"
    )))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_endpoint_parse_valid() {
    let ep: Endpoint = "127.0.0.1:4910".parse().unwrap();
    assert_eq!(ep.host, "127.0.0.1");
    assert_eq!(ep.port, 4910);
    assert_eq!(
      ep.to_zenoh_endpoint(),
      "udp/127.0.0.1:4910?rel=1;multistream=1"
    );
    assert_eq!(
      Endpoint::zenoh_endpoint("192.168.1.100:5000"),
      "udp/192.168.1.100:5000?rel=1;multistream=1"
    );

    let ep2: Endpoint = "[::1]:4910".parse().unwrap();
    assert_eq!(ep2.host, "::1");
    assert_eq!(ep2.port, 4910);
  }

  #[test]
  fn test_endpoint_parse_invalid() {
    assert!("1=127.0.0.1:4910".parse::<Endpoint>().is_err());
    assert!("invalid_address".parse::<Endpoint>().is_err());
    assert!("127.0.0.1:99999".parse::<Endpoint>().is_err());
  }
}
