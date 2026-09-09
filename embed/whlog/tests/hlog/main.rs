#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

mod append_scan;
mod concurrent_shift;
mod disk_read_cache;
mod flush_and_shift;
mod flush_fault;
mod inplace_lifecycle;
mod recovery;
mod support;
