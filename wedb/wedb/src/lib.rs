#![feature(gethostname)]

//! WeDB 分布式集群门面层

pub mod args;
pub mod client;
pub mod error;
pub mod server;

pub use args::ClusterArgs;
pub use error::{Error, Result};
pub use server::{
  boot::run_cluster_server, cluster::IClusterProvider, cluster_provider::ClusterProvider,
};
