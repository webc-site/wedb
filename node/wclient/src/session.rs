use compio::net::TcpStream;
use crossfire::{mpsc, oneshot};

use crate::{
  Error,
  Result,
  network,
  types::{CHANNEL_CAP, ChannelTx, CommandItem, ReplyTx, roundtrip},
};

/// libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession
pub struct GarnetClientSession {
  pub end_point: String,
  auth_username: Option<String>,
  auth_password: Option<String>,
  client_name: Option<String>,

  tx: Option<ChannelTx>,
}

impl GarnetClientSession {
  /// libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession
  pub fn new(
    endpoint: String,
    auth_username: Option<String>,
    auth_password: Option<String>,
    client_name: Option<String>,
  ) -> Self {
    Self {
      end_point: endpoint,
      auth_username,
      auth_password,
      client_name,
      tx: None,
    }
  }

  /// 请求通道引用（未连接即报错）
  fn channel(&self) -> Result<&ChannelTx> {
    self.tx.as_ref().ok_or_else(|| Error::Other("Not connected".into()))
  }

  /// libs/client/ClientSession/GarnetClientSession.cs:ConnectAsync
  pub async fn connect_async(&mut self) -> Result<()> {
    let stream = TcpStream::connect(&self.end_point).await?;
    let (tx, rx) = mpsc::bounded_async(CHANNEL_CAP);
    self.tx = Some(tx);

    compio::runtime::spawn(async move {
      if let Err(e) = network::network_loop(stream, rx).await {
        log::error!("GarnetClientSession 网络循环退出: {e}");
      }
    })
    .detach();

    network::handshake(
      async |args| self.execute_async(args).await,
      "GarnetClientSession",
      self.auth_username.as_deref(),
      self.auth_password.as_deref(),
      self.client_name.as_deref(),
    )
    .await
  }

  /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteAsync
  /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteAsyncBatch
  ///
  /// C# ExecuteAsyncBatch(params string[]) 为单命令变长参数入队 + TCS 等待，
  /// 与 ExecuteAsync 同构 (rust 无 TCS 队列，由 network_loop 按需泵出)，
  /// 故两者共用此实现
  pub async fn execute_async(&self, command: &[&str]) -> Result<String> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem {
      cmd: command.iter().map(|s| s.to_string()).collect(),
      resp_tx: ReplyTx::Str(resp_tx),
    };
    roundtrip(self.channel()?, item, resp_rx).await
  }

  /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteForArrayAsync
  pub async fn execute_for_array_async(&self, command: &[&str]) -> Result<Vec<String>> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem {
      cmd: command.iter().map(|s| s.to_string()).collect(),
      resp_tx: ReplyTx::Array(resp_tx),
    };
    roundtrip(self.channel()?, item, resp_rx).await
  }
}
