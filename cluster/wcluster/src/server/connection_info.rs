/// 在 garnet 中的相对路径:Server:Gossip:ConnectionInfo
#[derive(Debug, Clone, Default)]
pub struct ConnectionInfo {
  pub connected: bool,
  pub ping: i64,
  pub pong: i64,
}
