#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
pub use error::{Error, Result};
pub mod argslice;
pub mod cmd_strings;
pub mod command;
pub mod ext;
pub mod frame;
pub mod length;
pub mod read;

pub use argslice::{ArgSlice, ArgSliceVector, DEFAULT_MAX_ITEM_NUM};
pub use command::RespCommand;
pub use ext::{
  MAX_ERROR_MSG_LEN, RespSliceExt, RespVecExt, sanitize_error_str, strict_i32, strict_i64,
};
pub use frame::parse_resp_frame;
pub use read::MAX_ARGUMENT_LENGTH_BYTES;
