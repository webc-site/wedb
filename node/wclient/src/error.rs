use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

/// garnet/libs/client/ExceptionTypes.cs
#[derive(Debug, Error)]
pub enum Error {
  /// GarnetClient disposed exception
  /// garnet/libs/client/ExceptionTypes.cs:GarnetClientDisposedException
  #[error("GarnetClient is disposed")]
  Disposed,

  /// GarnetClient timeout exception
  /// garnet/libs/client/ExceptionTypes.cs:GarnetClientTimeoutException
  #[error("GarnetClient timeout")]
  Timeout,

  /// GarnetClient socket disposed exception
  /// garnet/libs/client/ExceptionTypes.cs:GarnetClientSocketDisposedException
  #[error("GarnetClient socket is disposed")]
  SocketDisposed,

  #[error(transparent)]
  Anyhow(#[from] anyhow::Error),

  #[error(transparent)]
  Io(#[from] std::io::Error),
}
