//! 发布订阅域（对标 libs/server/PubSub/*）
//!
//! C# 侧 SubscribeBroker 以 TsavoriteLog 专日志为分发介质，后台任务消费并
//! 回放进会话输出缓冲；Rust 侧以有界待发队列 + 同步收敛点承接同等三面
//! （发布入队 / 消费广播 / 即时直投），订阅者经 [`PubSubSink`] 投递面解耦。

pub mod pattern_subscription_entry;
pub mod subscribe_broker;
pub mod subscriber;

pub use pattern_subscription_entry::PatternSubscriptionEntry;
pub use subscribe_broker::SubscribeBroker;
pub use subscriber::{PubSubMailbox, PubSubMessage, PubSubMessageKind, PubSubSink};
