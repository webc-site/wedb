#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod api;
pub mod client;
mod error;
pub mod network;
pub mod parser;
pub mod record;
pub mod session;
pub mod types;

pub use error::{Error, Result};
