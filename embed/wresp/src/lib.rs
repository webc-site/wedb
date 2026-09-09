#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
pub use error::{Error, Result};
pub mod length;
pub mod read;
pub mod commands;
