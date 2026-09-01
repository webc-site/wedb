use async_broadcast::{Receiver as BroadcastReceiver, Sender as BroadcastSender, broadcast};
use hipstr::HipStr;
use rapidhash::RapidHashMap as HashMap;
use std::sync::{Arc, LazyLock, Mutex};

/// 全局单例 PubSub 消息管理器
pub static GLOBAL_PUBSUB: LazyLock<PubSubManager> = LazyLock::new(PubSubManager::new);

/// 全局 PubSub 消息管理器（采用 HipStr 优化通道名克隆开销）
#[derive(Clone, Default)]
pub struct PubSubManager {
  channels: Arc<Mutex<HashMap<HipStr<'static>, BroadcastSender<Vec<u8>>>>>,
}

impl PubSubManager {
  pub fn new() -> Self {
    Self {
      channels: Arc::new(Mutex::new(HashMap::default())),
    }
  }

  pub fn publish(&self, channel: &str, message: &[u8]) -> usize {
    let map = self.channels.lock().unwrap();
    if let Some(tx) = map.get(channel) {
      let count = tx.receiver_count();
      let _ = tx.try_broadcast(message.to_vec());
      count
    } else {
      0
    }
  }

  pub fn subscribe(&self, channel: &str) -> BroadcastReceiver<Vec<u8>> {
    let mut map = self.channels.lock().unwrap();
    let key = HipStr::from(channel);
    map
      .entry(key)
      .or_insert_with(|| {
        let (mut tx, _rx) = broadcast(128);
        tx.set_overflow(true);
        tx
      })
      .new_receiver()
  }

  pub fn channel_count(&self) -> usize {
    self.channels.lock().unwrap().len()
  }

  pub fn list_channels(&self) -> Vec<HipStr<'static>> {
    self.channels.lock().unwrap().keys().cloned().collect()
  }
}
