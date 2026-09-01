use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind, Result as IoResult};

pub fn encode<T: bitcode::Encode>(val: &T) -> IoResult<Vec<u8>> {
  Ok(bitcode::encode(val))
}

pub fn decode<T: bitcode::DecodeOwned>(buf: &[u8]) -> IoResult<T> {
  bitcode::decode(buf).map_err(|e| IoError::new(ErrorKind::InvalidData, e))
}

pub fn read_logs_err<E: StdError + Send + Sync + 'static>(e: E) -> IoError {
  IoError::other(e)
}
