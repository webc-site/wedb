use compio::net::TcpStream;
use crossfire::{MAsyncTx, mpsc, oneshot};

use crate::{CommandItem, Error, Result, network};

/// libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession
pub struct GarnetClientSession {
  pub end_point: String,
  auth_username: Option<String>,
  auth_password: Option<String>,
  client_name: Option<String>,

  tx: Option<MAsyncTx<mpsc::Array<CommandItem>>>,
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

  /// libs/client/ClientSession/GarnetClientSession.cs:ConnectAsync
  pub async fn connect_async(&mut self) -> Result<()> {
    let stream = TcpStream::connect(&self.end_point).await?;
    let (tx, rx) = mpsc::bounded_async(1024);
    self.tx = Some(tx);

    compio::runtime::spawn(async move {
      if let Err(e) = network::network_loop(stream, rx).await {
        log::error!("GarnetClientSession 网络循环退出: {e}");
      }
    })
    .detach();

    // AUTH（与 GarnetClient 同一口径：用户名优先，缺省密码按空串补齐）
    if let Some(ref username) = self.auth_username {
      let pwd = self.auth_password.as_deref().unwrap_or("");
      self.execute_async(&["AUTH", username, pwd]).await?;
    } else if let Some(ref pwd) = self.auth_password {
      self.execute_async(&["AUTH", pwd]).await?;
    }

    // CLIENT SETNAME / SETINFO（与 GarnetClient 同一口径）
    if let Some(ref client_name) = self.client_name {
      self
        .execute_async(&["CLIENT", "SETINFO", "LIB-NAME", "GarnetClientSession"])
        .await?;
      self
        .execute_async(&["CLIENT", "SETNAME", client_name])
        .await?;
    }

    Ok(())
  }

  /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteAsync
  pub async fn execute_async(&self, command: &[&str]) -> Result<String> {
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

  /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteForArrayAsync
  pub async fn execute_for_array_async(&self, command: &[&str]) -> Result<Vec<String>> {
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
