//! 发布订阅域（对标 libs/server/PubSub/*）
//!
//! 纯内存、零网络耦合的独立发布订阅核心组件，单机与集群均可高内聚低耦合复用。

pub mod pattern_subscription_entry;
pub mod session_commands;
pub mod subscribe_broker;
pub mod subscriber;

pub use pattern_subscription_entry::{PatternSubscriberSet, PatternSubscriptionEntry};
pub use session_commands::*;
pub use subscribe_broker::{MessageBroker, SubscribeBroker};
pub use subscriber::{PubSubMailbox, PubSubMessage, PubSubMessageKind, PubSubSink};
