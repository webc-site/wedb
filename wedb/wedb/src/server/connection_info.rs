/// libs/cluster/Server/Gossip/ConnectionInfo.cs:ConnectionInfo
#[derive(Default)]
pub struct ConnectionInfo {
  pub connected: bool,
  pub ping: i64,
  pub pong: i64,
  pub last_io: i64,
}
