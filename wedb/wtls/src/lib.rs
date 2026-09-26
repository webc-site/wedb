//! TLS 安全传输层（纯 rustls 栈：证书装载、出入站装配器、流读写内核）
//!
//! 在 garnet 中的相对路径:
//! - libs/server/TLS/GarnetTlsOptions.cs（服务端/客户端配置装配）
//! - libs/server/TLS/ServerCertificateSelector.cs（动态证书解析与定时刷新）
//! - libs/server/TLS/CertificateUtils.cs（PEM 装载）
//! - libs/common/Networking/NetworkHandler.cs（TLS 流读写面）

pub(crate) mod cert;
pub mod client;
pub mod server;
pub mod stream;

mod validity;

pub use client::ClientTlsConfig;
pub use server::ServerTlsConfig;
