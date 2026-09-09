#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
pub use error::{Error, Result};
pub mod commands;
pub mod length;
pub mod memory_writer;
pub mod read;
pub mod write;
