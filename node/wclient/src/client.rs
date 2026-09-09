use compio::net::TcpStream;
use crossfire::{MAsyncTx, mpsc, oneshot};

use crate::{CommandItem, Error, Result, network};

/// libs/client/GarnetClient.cs:GarnetClient
pub struct GarnetClient {
  pub end_point: String,
  auth_username: Option<String>,
  auth_password: Option<String>,
  client_name: Option<String>,

  /// 在途命令上限（即网络泵的命令通道容量下限）
  max_outstanding_tasks: usize,

  tx: Option<MAsyncTx<mpsc::Array<CommandItem>>>,
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

  /// libs/client/GarnetClient.cs:ConnectAsync
  pub async fn connect_async(&mut self) -> Result<()> {
    let stream = TcpStream::connect(&self.end_point).await?;
    let (tx, rx) = mpsc::bounded_async(self.max_outstanding_tasks.max(1024));
    self.tx = Some(tx);

    compio::runtime::spawn(async move {
      if let Err(e) = network::network_loop(stream, rx).await {
        log::error!("GarnetClient 网络循环退出: {e}");
      }
    })
    .detach();

    // AUTH（对标 ConnectAsync：用户名优先，缺省密码按空串补齐）
    if let Some(ref username) = self.auth_username {
      let pwd = self.auth_password.as_deref().unwrap_or("");
      self
        .execute_for_string_result_async(&["AUTH", username, pwd])
        .await?;
    } else if let Some(ref pwd) = self.auth_password {
      self.execute_for_string_result_async(&["AUTH", pwd]).await?;
    }

    // CLIENT SETNAME / SETINFO（对齐 C#：二者同以 clientName 非空为前提）
    if let Some(ref client_name) = self.client_name {
      self
        .execute_for_string_result_async(&["CLIENT", "SETINFO", "LIB-NAME", "GarnetClient"])
        .await?;
      self
        .execute_for_string_result_async(&["CLIENT", "SETNAME", client_name])
        .await?;
    }

    Ok(())
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteForStringResultAsync
  pub async fn execute_for_string_result_async(&self, command: &[&str]) -> Result<String> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let cmd = command.iter().map(|s| s.to_string()).collect();
    let tx = self
      .tx
      .as_ref()
      .ok_or_else(|| Error::Other("Not connected".into()))?;
    tx.send(CommandItem::Command { cmd, resp_tx })
      .await
      .map_err(|_| Error::Other("Network loop died".into()))?;
    resp_rx
      .await
      .map_err(|_| Error::Other("Response channel closed".into()))?
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteForStringArrayResultAsync
  pub async fn execute_for_string_array_result_async(
    &self,
    command: &[&str],
  ) -> Result<Vec<String>> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let cmd = command.iter().map(|s| s.to_string()).collect();
    let tx = self
      .tx
      .as_ref()
      .ok_or_else(|| Error::Other("Not connected".into()))?;
    tx.send(CommandItem::CommandForArray { cmd, resp_tx })
      .await
      .map_err(|_| Error::Other("Network loop died".into()))?;
    resp_rx
      .await
      .map_err(|_| Error::Other("Response channel closed".into()))?
  }
}
