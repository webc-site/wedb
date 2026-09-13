//! 网络抽象层：统一流抽象、多核端口复用、Unix 域套接字治理与连接切片泵

pub mod handler;
pub mod socket_opt;
pub mod stream;
pub mod uds;

pub use handler::NetworkHandler;
pub use socket_opt::{TCP_LISTEN_BACKLOG, bind_reuseport};
pub use stream::{ConnectionStream, DirectWriter, SessionReader};
#[cfg(unix)]
pub use uds::UdsGuard;
