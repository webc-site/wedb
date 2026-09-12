//! Checkpoint 集成测试入口（对标 Garnet Tsavorite cs/test/test.recovery 套件）。

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

mod checkpoint_manager;
mod edge;
mod fault_defense;
mod index_checkpoint;
mod recovery;
