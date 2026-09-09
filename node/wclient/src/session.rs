use std::{
  net::{SocketAddr, ToSocketAddrs},
  sync::{
    Arc,
    atomic::{AtomicI32, Ordering},
  },
};

use compio::{
  buf::{IntoInner, IoBuf, IoBufMut},
  net::TcpStream,
};
use crossfire::mpsc;
use parking_lot::Mutex;

use crate::Result;

/// garnet/libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession
pub struct GarnetClientSession {
  pub end_point: String,
  pub raw_result: bool,
  auth_username: Option<String>,
  auth_password: Option<String>,
  client_name: Option<String>,

  disposed: AtomicI32,
  // In C# this holds a socket and network handlers.
  // For now, we stub this out for the API.
  // network_sender: ...
}

impl GarnetClientSession {
  /// garnet/libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession
  pub fn new(
    endpoint: String,
    auth_username: Option<String>,
    auth_password: Option<String>,
    client_name: Option<String>,
    network_send_throttle_max: usize,
    raw_result: bool,
  ) -> Self {
    Self {
      end_point: endpoint,
      raw_result,
      auth_username,
      auth_password,
      client_name,
      disposed: AtomicI32::new(0),
    }
  }

  /// garnet/libs/client/ClientSession/GarnetClientSession.cs:ConnectAsync
  pub async fn connect_async(&self, timeout_ms: u32) -> Result<()> {
    // TCP connection
    // let stream = TcpStream::connect(&self.end_point).await?;

    // AUTH
    if let Some(ref username) = self.auth_username {
      let pwd = self.auth_password.as_deref().unwrap_or("");
      self.execute_async(&["AUTH", username, pwd]).await?;
    } else if let Some(ref pwd) = self.auth_password {
      self.execute_async(&["AUTH", pwd]).await?;
    }

    // CLIENT SETNAME
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

  /// garnet/libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteAsync
  pub async fn execute_async(&self, command: &[&str]) -> Result<String> {
    // Enqueue TCS, execute, flush, return
    // Stub
    Ok(String::new())
  }

  /// garnet/libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteAsyncBatch
  pub async fn execute_async_batch(&self, command: &[&str]) -> Result<String> {
    // Enqueue TCS, execute, return
    Ok(String::new())
  }

  /// garnet/libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteForArrayAsync
  pub async fn execute_for_array_async(&self, command: &[&str]) -> Result<Vec<String>> {
    Ok(Vec::new())
  }
}
