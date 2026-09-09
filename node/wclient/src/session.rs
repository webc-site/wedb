use std::sync::atomic::AtomicI32;
use std::collections::VecDeque;
use compio::net::TcpStream;
use compio::io::{AsyncRead, AsyncWriteExt};
use crossfire::mpsc;
use crossfire::{MAsyncTx, AsyncRx};
use crossfire::oneshot;
use crate::{Result, Error, CommandItem, RespReadResponseUtils};

/// libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession
pub struct GarnetClientSession {
    pub end_point: String,
    pub raw_result: bool,
    auth_username: Option<String>,
    auth_password: Option<String>,
    client_name: Option<String>,
    
    _disposed: AtomicI32,
    
    tx: Option<MAsyncTx<mpsc::Array<CommandItem>>>,
}

impl GarnetClientSession {
    /// libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession
    pub fn new(
        endpoint: String,
        auth_username: Option<String>,
        auth_password: Option<String>,
        client_name: Option<String>,
        _network_send_throttle_max: usize,
        raw_result: bool,
    ) -> Self {
        Self {
            end_point: endpoint,
            raw_result,
            auth_username,
            auth_password,
            client_name,
            _disposed: AtomicI32::new(0),
            tx: None,
        }
    }

    /// libs/client/ClientSession/GarnetClientSession.cs:ConnectAsync
    pub async fn connect_async(&mut self, _timeout_ms: u32) -> Result<()> {
        let stream = TcpStream::connect(&self.end_point).await?;
        let (tx, rx) = mpsc::bounded_async(1024);
        self.tx = Some(tx);
        
        compio::runtime::spawn(async move {
            if let Err(e) = Self::network_loop(stream, rx).await {
                let _ = e;
            }
        }).detach();
        
        // AUTH
        if let Some(ref username) = self.auth_username {
            let pwd = self.auth_password.as_deref().unwrap_or("");
            self.execute_async(&["AUTH", username, pwd]).await?;
        } else if let Some(ref pwd) = self.auth_password {
            self.execute_async(&["AUTH", pwd]).await?;
        }
        
        // CLIENT SETNAME
        if let Some(ref client_name) = self.client_name {
            self.execute_async(&["CLIENT", "SETINFO", "LIB-NAME", "GarnetClientSession"]).await?;
            self.execute_async(&["CLIENT", "SETNAME", client_name]).await?;
        }
        
        Ok(())
    }

    /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteAsync
    pub async fn execute_async(&self, command: &[&str]) -> Result<String> {
        let (resp_tx, resp_rx) = oneshot::oneshot();
        let cmd = command.iter().map(|s| s.to_string()).collect();
        let tx = self.tx.as_ref().ok_or_else(|| Error::Other("Not connected".into()))?;
        tx.send(CommandItem::Command { cmd, resp_tx }).await.map_err(|_| Error::Other("Network loop died".into()))?;
        resp_rx.await.map_err(|_| Error::Other("Response channel closed".into()))?
    }

    /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteAsyncBatch
    pub async fn execute_async_batch(&self, command: &[&str]) -> Result<String> {
        self.execute_async(command).await
    }

    /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteForArrayAsync
    pub async fn execute_for_array_async(&self, command: &[&str]) -> Result<Vec<String>> {
        let (resp_tx, resp_rx) = oneshot::oneshot();
        let cmd = command.iter().map(|s| s.to_string()).collect();
        let tx = self.tx.as_ref().ok_or_else(|| Error::Other("Not connected".into()))?;
        tx.send(CommandItem::CommandForArray { cmd, resp_tx }).await.map_err(|_| Error::Other("Network loop died".into()))?;
        resp_rx.await.map_err(|_| Error::Other("Response channel closed".into()))?
    }
    
    async fn network_loop(mut stream: TcpStream, rx: AsyncRx<mpsc::Array<CommandItem>>) -> Result<()> {
        let mut tcs_queue: VecDeque<CommandItem> = VecDeque::new();
        let mut read_buf = Vec::with_capacity(8192);
        
        loop {
            if tcs_queue.is_empty() {
                let item = match rx.recv().await {
                    Ok(item) => item,
                    Err(_) => break, // caller disconnected
                };
                tcs_queue.push_back(item);
            }
            
            let mut out_buf = Vec::new();
            while let Ok(item) = rx.try_recv() {
                tcs_queue.push_back(item);
            }
            
            for item in &tcs_queue {
                let cmd = match item {
                    CommandItem::Command { cmd, .. } => cmd,
                    CommandItem::CommandForArray { cmd, .. } => cmd,
                };
                out_buf.extend_from_slice(format!("*{}\r\n", cmd.len()).as_bytes());
                for arg in cmd {
                    out_buf.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
                }
            }
            
            if !out_buf.is_empty() {
                let res = stream.write_all(out_buf).await.0;
                res?;
            }
            
            while !tcs_queue.is_empty() {
                let chunk = vec![0u8; 4096];
                let compio::buf::BufResult(res, chunk) = stream.read(chunk).await;
                let n = res?;
                if n == 0 {
                    return Err(Error::Other("EOF".into()));
                }
                read_buf.extend_from_slice(&chunk[..n]);
                
                let mut data_slice = read_buf.as_slice();
                let mut total_consumed = 0;
                while !data_slice.is_empty() && !tcs_queue.is_empty() {
                    let old_len = data_slice.len();
                    let front = tcs_queue.front().unwrap();
                    let mut consumed_bytes = 0;
                    
                    match front {
                        CommandItem::Command { .. } => {
                            let parsed = if data_slice[0] == b'+' {
                                RespReadResponseUtils::try_read_simple_string(&mut data_slice)?.map(Ok)
                            } else if data_slice[0] == b'-' {
                                RespReadResponseUtils::try_read_error_as_string(&mut data_slice)?.map(|e| Err(Error::Other(e)))
                            } else if data_slice[0] == b':' {
                                RespReadResponseUtils::try_read_integer_as_string(&mut data_slice)?.map(Ok)
                            } else if data_slice[0] == b'$' {
                                RespReadResponseUtils::try_read_string_with_length_header(&mut data_slice)?.map(|s| Ok(s.unwrap_or_default()))
                            } else {
                                return Err(Error::Other(format!("Unexpected token {}", data_slice[0] as char)));
                            };
                            
                            if let Some(res) = parsed {
                                consumed_bytes = old_len - data_slice.len();
                                if let CommandItem::Command { resp_tx, .. } = tcs_queue.pop_front().unwrap() {
                                    resp_tx.send(res);
                                }
                            }
                        }
                        CommandItem::CommandForArray { .. } => {
                            let parsed = if data_slice[0] == b'*' {
                                RespReadResponseUtils::try_read_string_array_with_length_header(&mut data_slice)?.map(|arr| Ok(arr.unwrap_or_default()))
                            } else if data_slice[0] == b'-' {
                                RespReadResponseUtils::try_read_error_as_string(&mut data_slice)?.map(|e| Err(Error::Other(e)))
                            } else {
                                return Err(Error::Other(format!("Unexpected token {}", data_slice[0] as char)));
                            };
                            
                            if let Some(res) = parsed {
                                consumed_bytes = old_len - data_slice.len();
                                if let CommandItem::CommandForArray { resp_tx, .. } = tcs_queue.pop_front().unwrap() {
                                    resp_tx.send(res);
                                }
                            }
                        }
                    }
                    
                    if consumed_bytes > 0 {
                        total_consumed += consumed_bytes;
                    } else {
                        break;
                    }
                }
                read_buf.drain(..total_consumed);
            }
        }
        Ok(())
    }
}
