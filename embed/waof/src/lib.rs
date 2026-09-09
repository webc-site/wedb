#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
pub use error::{Error, Result};
pub mod aof_processor;
pub mod append_only_file;
pub mod types;
