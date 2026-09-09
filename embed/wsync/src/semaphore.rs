use event_listener::{Event, Listener};
use parking_lot::Mutex;

pub struct Semaphore {
  permits: Mutex<usize>,
  event: Event,
}

impl Semaphore {
  pub fn new(permits: usize) -> Self {
    Self {
      permits: Mutex::new(permits),
      event: Event::new(),
    }
  }

  pub fn release(&self, count: usize) {
    let mut permits = self.permits.lock();
    *permits += count;
    self.event.notify(count);
  }

  pub fn wait(&self) {
    loop {
      let listener = self.event.listen();
      {
        let mut permits = self.permits.lock();
        if *permits > 0 {
          *permits -= 1;
          return;
        }
      }
      listener.wait();
    }
  }

  pub async fn wait_async(&self) {
    loop {
      let listener = self.event.listen();
      {
        let mut permits = self.permits.lock();
        if *permits > 0 {
          *permits -= 1;
          return;
        }
      }
      listener.await;
    }
  }
}
