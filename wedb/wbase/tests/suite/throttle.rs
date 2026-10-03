//! 自研依据: 节流闸（本仓组件，C# 无对应）
use std::{sync::Arc, thread, time::Duration};

use wbase::{
  future::block_on,
  throttle::{NetworkSenderThrottle, ThrottleClosed},
};

#[test]
fn test_throttle_basic() {
  let throttle = NetworkSenderThrottle::new(2);
  assert_eq!(throttle.in_flight(), 0);

  assert!(block_on(throttle.enter_send()).is_ok());
  assert_eq!(throttle.in_flight(), 1);

  assert!(block_on(throttle.enter_send()).is_ok());
  assert_eq!(throttle.in_flight(), 2);

  throttle.exit_send();
  assert_eq!(throttle.in_flight(), 1);

  throttle.exit_send();
  assert_eq!(throttle.in_flight(), 0);
}

#[test]
fn test_throttle_close() {
  let throttle = Arc::new(NetworkSenderThrottle::new(1));
  assert!(block_on(throttle.enter_send()).is_ok());

  let t = Arc::clone(&throttle);
  thread::spawn(move || {
    thread::sleep(Duration::from_millis(20));
    t.close();
  });

  let result = block_on(throttle.enter_send());
  assert_eq!(result, Err(ThrottleClosed));
  assert!(throttle.is_closed());
}
