use aok::{OK, Void};
use log::info;

#[test]
fn test() -> Void {
  info!("> test {}", 123456);
  OK
}
