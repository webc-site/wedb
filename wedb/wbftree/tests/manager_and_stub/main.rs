//! RangeIndexManager 与 RangeIndexStub 集成测试套件

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

mod common;
mod locks;
mod manager;
mod service;
mod stub;
