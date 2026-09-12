//! 复活池（Revivification FreeRecordPool / FreeRecordBin）对标测试套件入口

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

mod concurrency_stress;
mod free_bin_allocation;
mod purge_and_limits;
pub mod support;
