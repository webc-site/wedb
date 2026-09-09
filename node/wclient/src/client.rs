use std::sync::atomic::{AtomicI32, Ordering};

use crate::Result;

/// garnet/libs/client/GarnetClient.cs:GarnetClient
pub struct GarnetClient {
  pub end_point: String,
  auth_username: Option<String>,
  auth_password: Option<String>,
  client_name: Option<String>,

  send_page_size: usize,
  buffer_size: usize,
  max_outstanding_tasks: usize,
  timeout_milliseconds: u32,
  network_send_throttle_max: usize,

  disposed: AtomicI32,
}

impl GarnetClient {
  /// garnet/libs/client/GarnetClient.cs:GarnetClient
  pub fn new(
    endpoint: String,
    auth_username: Option<String>,
    auth_password: Option<String>,
    client_name: Option<String>,
    send_page_size: usize,
    buffer_size: usize,
    max_outstanding_tasks: usize,
    timeout_milliseconds: u32,
    network_send_throttle_max: usize,
  ) -> Self {
    Self {
      end_point: endpoint,
      auth_username,
      auth_password,
      client_name,
      send_page_size,
      buffer_size,
      max_outstanding_tasks,
      timeout_milliseconds,
      network_send_throttle_max,
      disposed: AtomicI32::new(0),
    }
  }

  /// garnet/libs/client/GarnetClient.cs:ConnectAsync
  pub async fn connect_async(&self) -> Result<()> {
    // TCP Connection and network handler initialization

    // AUTH
    if let Some(ref username) = self.auth_username {
      let pwd = self.auth_password.as_deref().unwrap_or("");
      // self.execute_for_string_result_async(&["AUTH", username, pwd]).await?;
    } else if let Some(ref pwd) = self.auth_password {
      // self.execute_for_string_result_async(&["AUTH", pwd]).await?;
    }

    // CLIENT SETNAME
    if let Some(ref client_name) = self.client_name {
      // self.execute_for_string_result_async(&["CLIENT", "SETINFO", "LIB-NAME", "GarnetClient"]).await?;
      // self.execute_for_string_result_async(&["CLIENT", "SETNAME", client_name]).await?;
    }

    Ok(())
  }
}
