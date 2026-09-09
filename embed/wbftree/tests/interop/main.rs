//! BfTree 与 Garnet 互操作接口集成测试套件

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

mod common;
mod lifecycle;
mod point_ops;
mod scan;
mod snapshot;
