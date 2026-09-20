#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod argslice;
pub mod argument;
pub mod catalog;
pub mod check_args;
pub mod cmd_strings;
pub mod command;
mod error;
pub mod ext;
pub mod frame;
pub mod key_spec;
pub mod length;
pub mod metrics;
pub mod options;
pub mod read;
pub mod resp_memory_writer;
pub mod session_parse_state;

pub use catalog::{RESP_COMMANDS_DOCS_JSON, RESP_COMMANDS_INFO_JSON};
pub use error::{Error, Result};
