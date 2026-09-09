//! Garnet Tsavorite Epoch 集成测试套件（对标 Garnet test.epoch）

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

mod concurrency;
mod drain;
mod protection;
mod support;
mod user_word;
