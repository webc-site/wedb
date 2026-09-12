/// libs/cluster/Server/Gossip/ConnectionInfo.cs:ConnectionInfo
#[derive(Debug, Clone, Default)]
pub struct ConnectionInfo {
  pub connected: bool,
  pub ping: i64,
  pub pong: i64,
  pub last_io: i64,
}
