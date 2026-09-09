#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
pub use error::{Error, Result};

pub mod session;
pub use session::*;

pub mod client;
pub use client::*;
