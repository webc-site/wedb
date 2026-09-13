//! RangeIndex 分块序列化与反序列化测试套件

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

mod boundary;
mod common;
mod error;
mod reader;
mod round_trip;
mod streaming;
