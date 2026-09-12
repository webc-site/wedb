mod boundary_defense;
mod header_and_bits;
mod lifecycle_and_chains;
pub mod support;
mod zero_copy_and_codec;

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}
