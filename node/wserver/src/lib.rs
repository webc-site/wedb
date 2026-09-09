//! Garnet 服务端库的 Rust 转写（对标 garnet/libs/server）
//!
//! 转写命名约定：C# partial 类按文件拆分后保留 `类型_分片` 形式
//! （如 `LuaRunner_Functions` 对应 LuaRunner.Functions.cs 分片），
//! 故 crate 级放宽 non_camel_case_types/non_snake_case；其余 lint 不放宽。

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

pub mod arg_slice;
pub mod input_header;
pub mod inputs;
pub mod session_parse_state;
pub mod session_parse_state_extensions;
pub mod types;

pub mod acl;
pub mod aof;
pub mod api;
pub mod argslice;
pub mod auth;
pub mod cluster;
pub mod config;
pub mod custom;
pub mod databases;
pub mod error;
pub mod lua;
pub mod metrics;
pub mod module;
pub mod objects;
pub mod providers;
pub mod pubsub;
pub mod resp;
pub mod server_config;
pub mod servers;
pub mod sessions;
pub mod storage;
pub mod taskmanager;
pub mod tls;
pub mod transaction;
