//! RangeIndex 分块序列化与反序列化测试套件

mod boundary;
#[path = "../guard/mod.rs"]
mod guard;

mod common;
mod error;
mod reader;
mod round_trip;
mod streaming;
