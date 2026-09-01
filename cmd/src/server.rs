use socket2::{Domain, Protocol, Socket, Type};
use std::future::Future;
use std::mem::take;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::{Arc, Mutex};
use std::thread::{Builder, JoinHandle, available_parallelism};

use async_broadcast::{Receiver as BroadcastReceiver, Sender as BroadcastSender, broadcast};
use bytes::BytesMut;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::{TcpListener, TcpStream};
use compio::runtime::Runtime;
use compio::runtime::spawn;
use crossfire::oneshot::oneshot;
use futures_util::FutureExt;
use log::{debug, error, info};
use wedb_embed::{Error, Result};
use wedb_resp::{RespValue, parse_resp};

use crate::context::ConnectionContext;
use crate::types::Cmd;

/// 通用 Redis 命令调度接口
pub trait CmdHandler: Send + Sync + 'static {
  /// 执行已解析的 Redis 命令并返回 RESP 回复
  fn handle(&self, ctx: &mut ConnectionContext, cmd: Cmd)
  -> impl Future<Output = RespValue> + Send;
}

/// 高性能无锁多线程异步 Redis TCP 服务器引擎（对标 Kvrocks Worker 线程池）
pub struct RedisServer {
  shutdown_tx: BroadcastSender<()>,
  server_threads: Vec<JoinHandle<()>>,
  addr: String,
}

fn create_listener(addr_str: &str) -> Result<(TcpListener, String)> {
  let socket_addr: SocketAddr = addr_str
    .parse()
    .map_err(|e| Error::internal(format!("Invalid socket address {addr_str}: {e}")))?;
  let domain = if socket_addr.is_ipv6() {
    Domain::IPV6
  } else {
    Domain::IPV4
  };
  let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
    .map_err(|e| Error::internal_with_source("Failed to create socket", e))?;
  socket
    .set_reuse_address(true)
    .map_err(|e| Error::internal_with_source("Failed to set reuse_address", e))?;
  #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
  let _ = socket.set_reuse_port(true);
  socket
    .set_nonblocking(true)
    .map_err(|e| Error::internal_with_source("Failed to set nonblocking", e))?;
  socket
    .bind(&socket_addr.into())
    .map_err(|e| Error::internal_with_source(format!("Failed to bind socket on {addr_str}"), e))?;
  socket
    .listen(1024)
    .map_err(|e| Error::internal_with_source("Failed to listen on socket", e))?;
  let std_listener: StdTcpListener = socket.into();
  let local_addr = std_listener
    .local_addr()
    .map_err(|e| Error::internal_with_source("Failed to get local address", e))?
    .to_string();
  let listener = TcpListener::from_std(std_listener)
    .map_err(|e| Error::internal_with_source("Failed to convert to compio TcpListener", e))?;
  Ok((listener, local_addr))
}

impl RedisServer {
  #[inline]
  pub fn addr(&self) -> &str {
    &self.addr
  }

  /// 启动多工作线程监听服务（根据 CPU 核心数自动调度多事件循环，对标 Kvrocks worker threads）
  pub async fn start<H: CmdHandler>(handler: Arc<H>, addr: String) -> Result<Self> {
    let num_workers = available_parallelism().map(|n| n.get()).unwrap_or(4);
    Self::start_with_workers(handler, addr, num_workers).await
  }

  /// 启动指定工作线程数的监听服务
  pub async fn start_with_workers<H: CmdHandler>(
    handler: Arc<H>,
    addr: String,
    num_workers: usize,
  ) -> Result<Self> {
    let workers = num_workers.max(1);
    let (mut shutdown_tx, shutdown_rx) = broadcast::<()>(1);
    shutdown_tx.set_overflow(true);

    let mut server_threads = Vec::with_capacity(workers);
    let (ready_tx, ready_rx) = oneshot::<Result<String>>();
    let ready_tx_holder = Arc::new(Mutex::new(Some(ready_tx)));

    for worker_id in 0..workers {
      let handler_clone = handler.clone();
      let addr_clone = addr.clone();
      let ready_tx_clone = ready_tx_holder.clone();
      let mut shutdown_rx_clone = shutdown_rx.clone();

      let handle = Builder::new()
        .name(format!("redis-worker-{worker_id}"))
        .spawn(move || {
          let rt = match Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
              if let Ok(mut lock) = ready_tx_clone.lock()
                && let Some(tx) = lock.take()
              {
                tx.send(Err(Error::internal(format!(
                  "Failed to create compio runtime on worker {worker_id}: {e}"
                ))));
              }
              return;
            }
          };

          rt.block_on(async move {
            let (listener, local_addr) = match create_listener(&addr_clone) {
              Ok(pair) => pair,
              Err(e) => {
                if let Ok(mut lock) = ready_tx_clone.lock()
                  && let Some(tx) = lock.take()
                {
                  tx.send(Err(e));
                }
                return;
              }
            };

            info!("Redis worker {worker_id} listening on {local_addr}");
            if let Ok(mut lock) = ready_tx_clone.lock()
              && let Some(tx) = lock.take()
            {
              tx.send(Ok(local_addr.clone()));
            }

            loop {
              futures_util::select! {
                  _ = shutdown_rx_clone.recv().fuse() => {
                      info!("Redis worker {worker_id} shutting down...");
                      break;
                  }
                  accept_res = listener.accept().fuse() => {
                      match accept_res {
                          Ok((socket, peer_addr)) => {
                              debug!("Worker {worker_id} accepted client from {peer_addr}");
                              let _ = socket.set_nodelay(true);
                              let client_handler = handler_clone.clone();
                              let client_shutdown = shutdown_rx_clone.clone();
                              spawn(async move {
                                  if let Err(e) = handle_connection(
                                      socket,
                                      client_handler,
                                      client_shutdown,
                                  ).await {
                                      debug!("Client {peer_addr} disconnected: {e}");
                                  }
                              }).detach();
                          }
                          Err(e) => {
                              error!("Error accepting connection on worker {worker_id}: {e}");
                          }
                      }
                  }
              }
            }
          });
        })
        .map_err(|e| Error::internal(format!("Failed to spawn redis worker thread: {e}")))?;

      server_threads.push(handle);
    }

    let local_addr = ready_rx
      .recv_async()
      .await
      .map_err(|_| Error::internal("All worker threads terminated before startup"))??;

    Ok(Self {
      shutdown_tx,
      server_threads,
      addr: local_addr,
    })
  }

  /// 优雅停止服务
  pub async fn shutdown(mut self) -> Result<()> {
    let _ = self.shutdown_tx.broadcast(()).await;
    for thread in self.server_threads.drain(..) {
      let _ = thread.join();
    }
    Ok(())
  }
}

async fn handle_connection<H: CmdHandler>(
  mut socket: TcpStream,
  handler: Arc<H>,
  mut shutdown_rx: BroadcastReceiver<()>,
) -> Result<()> {
  let mut buf = Vec::with_capacity(4096);
  let mut parse_buf = BytesMut::with_capacity(4096);
  let mut write_buf = Vec::with_capacity(512);
  let mut ctx = ConnectionContext::default();

  loop {
    futures_util::select! {
        _ = shutdown_rx.recv().fuse() => {
            break;
        }
        res = socket.read(buf).fuse() => {
            let compio::BufResult(read_res, returned_buf) = res;
            buf = returned_buf;
            let n = match read_res {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => return Err(Error::Io(e)),
            };
            parse_buf.extend_from_slice(&buf[..n]);
            buf.clear();

            write_buf.clear();
            let mut should_quit = false;

            while !parse_buf.is_empty() {
                match parse_resp(&mut parse_buf) {
                    Ok(Some(resp_value)) => match Cmd::from_resp(&resp_value) {
                        Ok(Cmd::Quit) => {
                            RespValue::ok().serialize(&mut write_buf);
                            should_quit = true;
                            break;
                        }
                        Ok(cmd) => {
                            let reply = handler.handle(&mut ctx, cmd).await;
                            reply.serialize(&mut write_buf);
                        }
                        Err(err) => {
                            RespValue::error(format!("ERR {err}")).serialize(&mut write_buf);
                        }
                    },
                    Ok(None) => break,
                    Err(e) => {
                        RespValue::error(format!("ERR Protocol error: {e}")).serialize(&mut write_buf);
                        parse_buf.clear();
                        break;
                    }
                }
            }

            if !write_buf.is_empty() {
                let compio::BufResult(write_res, returned_buf) =
                    socket.write_all(take(&mut write_buf)).await;
                write_buf = returned_buf;
                write_res?;
            }

            if should_quit {
                return Ok(());
            }
        }
    }
  }

  Ok(())
}
