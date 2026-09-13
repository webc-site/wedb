//! 发布订阅域（对标 libs/server/PubSub/*）
//!
//! 纯内存、零网络耦合的独立发布订阅核心组件，单机与集群均可高内聚低耦合复用。

mod pattern_subscription_entry;
mod session_commands;
mod subscribe_broker;
mod subscriber;

pub use pattern_subscription_entry::{PatternSubscriberSet, PatternSubscriptionEntry};
pub use session_commands::{
  DEFAULT_MAILBOX_CAPACITY, ERR_PUBLISH_DISABLED, ERR_PUBSUB_CHANNELS_DISABLED,
  ERR_PUBSUB_NUMPAT_DISABLED, ERR_PUBSUB_NUMSUB_DISABLED, ERR_PUNSUBSCRIBE_DISABLED,
  ERR_SUBSCRIBE_DISABLED, ERR_UNSUBSCRIBE_DISABLED, PubSubSession, PubSubSessionCommands,
};
pub use subscribe_broker::SubscribeBroker;
pub use subscriber::{PubSubMailbox, PubSubMessage, PubSubMessageKind, PubSubSink};
