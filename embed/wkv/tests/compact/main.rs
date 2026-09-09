//! Garnet Tsavorite 紧缩兼容集成测试入口（对标 Garnet test.hlog 套件）

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

mod basic;
mod concurrency_and_collision;
mod lazy_compaction;
mod more_log_compaction;
mod spanbyte_compaction;
mod support;
