#![recursion_limit = "256"]
// boot.rs 启动序列巨型 async block 的 layout 查询在 x86 stable rustc
// 超默认 128 深度限制（CI R27 轮实证）；编译期属性，运行时无关

//! WeDB 分布式集群门面层

pub mod args;
pub mod client;
pub mod error;
pub mod server;

pub use args::ClusterArgs;
pub use error::Error;
pub use server::{
  boot::run_cluster_server, cluster::IClusterProvider, cluster_provider::ClusterProvider,
};
