//! 存储会话与分层裁决抽象
//!
//! # 存储三层架构定位
//! - 底层：`wkv::StoreSession` / `wkv::BatchStoreSession`
//!   负责物理键读写、纪元保护门（Epoch）、底层混合日志与对象存物理 I/O。
//! - 中间层：`wnode::storage::session::common` (`ttl_sync` / `etag_sync` / `user_read`)
//!   为过期裁决（TTL）、元存活判活（Meta is_live）、双域判型（KeyTag）的唯一权威裁决层。
//! - 门面层：`wnode::storage::session::storage_session::StorageSession`
//!   为 RESP 命令面操作存储的唯一统一入口，严格禁止 RESP 命令处理层绕过裁决层直调 wkv 原语。

pub mod session;

pub use session::storage_session::StorageSession;
