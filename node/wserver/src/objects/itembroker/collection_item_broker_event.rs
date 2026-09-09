//! 集合项经纪事件（对标 libs/server/Objects/ItemBroker/CollectionItemBrokerEvent.cs）
//!
//! C# 以 FieldOffset 显式布局复用 17 字节栈空间；Rust 侧为普通枚举承载
//! （键 / 键组 / 观察者 互斥，枚举天然等价且免于联合体不安全）。

/// 事件类型
///
/// libs/server/Objects/ItemBroker/CollectionItemBrokerEvent.cs:CollectionItemBrokerEventType
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CollectionItemBrokerEventType {
  NotSet = 0,
  /// 新观察者登记
  NewObserver = 1,
  /// 集合更新（可能有待取项）
  CollectionUpdated = 2,
}

/// 经纪事件
///
/// libs/server/Objects/ItemBroker/CollectionItemBrokerEvent.cs:CollectionItemBrokerEvent
#[derive(Debug, Clone)]
pub struct CollectionItemBrokerEvent {
  pub event_type: CollectionItemBrokerEventType,
  /// 更新集合的键（CollectionUpdated）
  pub key: Option<Vec<u8>>,
  /// 观察者请求订阅的键组（NewObserver）
  pub keys: Option<Vec<Vec<u8>>>,
  /// 新观察者（NewObserver）
  pub observer: Option<
    std::sync::Arc<crate::objects::itembroker::collection_item_observer::CollectionItemObserver>,
  >,
}

impl CollectionItemBrokerEvent {
  /// 构造 CollectionUpdated 事件
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBrokerEvent.cs:CreateCollectionUpdatedEvent
  #[inline]
  pub fn create_collection_updated_event(key: Vec<u8>) -> Self {
    Self {
      event_type: CollectionItemBrokerEventType::CollectionUpdated,
      key: Some(key),
      keys: None,
      observer: None,
    }
  }

  /// 构造 NewObserver 事件
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBrokerEvent.cs:CreateNewObserverEvent
  #[inline]
  pub fn create_new_observer_event(
    observer: std::sync::Arc<
      crate::objects::itembroker::collection_item_observer::CollectionItemObserver,
    >,
    keys: Vec<Vec<u8>>,
  ) -> Self {
    Self {
      event_type: CollectionItemBrokerEventType::NewObserver,
      key: None,
      keys: Some(keys),
      observer: Some(observer),
    }
  }

  /// 是否为缺省事件
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBrokerEvent.cs:IsDefault
  #[inline]
  pub fn is_default(&self) -> bool {
    self.event_type == CollectionItemBrokerEventType::NotSet
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::*;
  use crate::{
    objects::itembroker::collection_item_observer::CollectionItemObserver, types::RespCommand,
  };

  #[test]
  fn event_constructors() {
    let updated = CollectionItemBrokerEvent::create_collection_updated_event(b"k".to_vec());
    assert_eq!(
      updated.event_type,
      CollectionItemBrokerEventType::CollectionUpdated
    );
    assert_eq!(updated.key.as_deref(), Some(b"k".as_slice()));
    assert!(!updated.is_default());

    let observer = Arc::new(CollectionItemObserver::new(1, RespCommand::Blpop, vec![]));
    let new_observer =
      CollectionItemBrokerEvent::create_new_observer_event(observer, vec![b"k1".to_vec()]);
    assert_eq!(
      new_observer.event_type,
      CollectionItemBrokerEventType::NewObserver
    );
    assert_eq!(new_observer.keys, Some(vec![b"k1".to_vec()]));
    assert!(new_observer.observer.is_some());

    let default_ev = CollectionItemBrokerEvent {
      event_type: CollectionItemBrokerEventType::NotSet,
      key: None,
      keys: None,
      observer: None,
    };
    assert!(default_ev.is_default());
  }
}
