use compio::net::TcpStream;
use crossfire::{mpsc, oneshot};

use crate::{
  Error, Result, network,
  types::{CommandItem, ReplyTx, roundtrip},
};

/// libs/client/GarnetClient.cs:GarnetClient
pub struct GarnetClient {
  pub end_point: String,
  auth_username: Option<String>,
  auth_password: Option<String>,
  client_name: Option<String>,

  /// 在途命令上限（即网络泵的命令通道容量下限）
  max_outstanding_tasks: usize,

  tx: Option<crate::types::ChannelTx>,
}

impl GarnetClient {
  /// libs/client/GarnetClient.cs:GarnetClient
  ///
  /// C# 构造函数还带 sendPageSize/bufferSize/timeout 等传输层旋钮，本实现
  /// 未引入对应机制，参数一并裁剪（需要时随机制一起加回）
  pub fn new(
    endpoint: String,
    auth_username: Option<String>,
    auth_password: Option<String>,
    client_name: Option<String>,
    max_outstanding_tasks: usize,
  ) -> Self {
    Self {
      end_point: endpoint,
      auth_username,
      auth_password,
      client_name,
      max_outstanding_tasks,
      tx: None,
    }
  }

  /// 请求通道引用（未连接即报错）
  fn channel(&self) -> Result<&crate::types::ChannelTx> {
    self
      .tx
      .as_ref()
      .ok_or_else(|| Error::Other("Not connected".into()))
  }

  /// libs/client/GarnetClient.cs:ConnectAsync
  pub async fn connect_async(&mut self) -> Result<()> {
    let stream = TcpStream::connect(&self.end_point).await?;
    let (tx, rx) = mpsc::bounded_async(self.max_outstanding_tasks.max(crate::types::CHANNEL_CAP));
    self.tx = Some(tx);

    compio::runtime::spawn(async move {
      if let Err(e) = network::network_loop(stream, rx).await {
        log::error!("GarnetClient 网络循环退出: {e}");
      }
    })
    .detach();

    network::handshake(
      async |args| self.execute_for_string_result_async(args).await,
      "GarnetClient",
      self.auth_username.as_deref(),
      self.auth_password.as_deref(),
      self.client_name.as_deref(),
    )
    .await
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteForStringResultAsync
  pub async fn execute_for_string_result_async(&self, command: &[&str]) -> Result<String> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem {
      cmd: command.iter().map(|s| s.to_string()).collect(),
      resp_tx: ReplyTx::Str(resp_tx),
    };
    roundtrip(self.channel()?, item, resp_rx).await
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteForStringArrayResultAsync
  pub async fn execute_for_string_array_result_async(
    &self,
    command: &[&str],
  ) -> Result<Vec<String>> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem {
      cmd: command.iter().map(|s| s.to_string()).collect(),
      resp_tx: ReplyTx::Array(resp_tx),
    };
    roundtrip(self.channel()?, item, resp_rx).await
  }
}
