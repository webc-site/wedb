#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod commands;
mod error;
pub mod runner;

pub use error::{Error, Result};
