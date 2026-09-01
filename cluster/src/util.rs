use std::task::Poll;
use std::time::Duration;

use futures_timer::Delay;
use futures_util::future::poll_fn;

pub use wedb_raft::util::now_millis;

#[inline]
pub async fn sleep(d: Duration) {
  Delay::new(d).await;
}

#[inline]
pub async fn yield_now() {
  let mut yielded = false;
  poll_fn(move |cx| {
    if yielded {
      Poll::Ready(())
    } else {
      yielded = true;
      cx.waker().wake_by_ref();
      Poll::Pending
    }
  })
  .await;
}
