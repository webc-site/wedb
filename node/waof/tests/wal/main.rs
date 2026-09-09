#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

pub mod support;

mod enqueue_and_commit;
mod recovery_and_corruption;
mod scan_and_read;
mod truncate_and_evict;
