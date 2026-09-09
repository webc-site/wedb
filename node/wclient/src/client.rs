use std::sync::atomic::AtomicI32;
use crate::Result;

/// garnet/libs/client/GarnetClient.cs:GarnetClient
pub struct GarnetClient {
    pub end_point: String,
    auth_username: Option<String>,
    auth_password: Option<String>,
    client_name: Option<String>,
    
    _send_page_size: usize,
    _buffer_size: usize,
    _max_outstanding_tasks: usize,
    _timeout_milliseconds: u32,
    _network_send_throttle_max: usize,

    _disposed: AtomicI32,
}

impl GarnetClient {
    /// garnet/libs/client/GarnetClient.cs:GarnetClient
    #[allow(clippy::too_many_arguments)]
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
            _send_page_size: send_page_size,
            _buffer_size: buffer_size,
            _max_outstanding_tasks: max_outstanding_tasks,
            _timeout_milliseconds: timeout_milliseconds,
            _network_send_throttle_max: network_send_throttle_max,
            _disposed: AtomicI32::new(0),
        }
    }

    /// garnet/libs/client/GarnetClient.cs:ConnectAsync
    pub async fn connect_async(&self) -> Result<()> {
        // TCP Connection and network handler initialization
        
        // AUTH
        if let Some(ref _username) = self.auth_username {
            let _pwd = self.auth_password.as_deref().unwrap_or("");
            // self.execute_for_string_result_async(&["AUTH", username, pwd]).await?;
        } else if let Some(ref _pwd) = self.auth_password {
            // self.execute_for_string_result_async(&["AUTH", pwd]).await?;
        }
        
        // CLIENT SETNAME
        if let Some(ref _client_name) = self.client_name {
            // self.execute_for_string_result_async(&["CLIENT", "SETINFO", "LIB-NAME", "GarnetClient"]).await?;
            // self.execute_for_string_result_async(&["CLIENT", "SETNAME", client_name]).await?;
        }
        
        Ok(())
    }
}
