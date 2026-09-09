#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
pub use error::{Error, Result};
pub mod acl;
pub mod auth;
