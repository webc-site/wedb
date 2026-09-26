//! storeWrapper 提交标记写入通道（对标 libs/server/StoreWrapper.cs:EnqueueCommit）

use std::sync::Arc;

use waof::AofEntryType;

/// storeWrapper 提交标记写入回调（对标 C# ReplicationManager 直调
/// storeWrapper.EnqueueCommit 具体方法：C# 对位即委托字段，单一闭包形态，
/// 不设 trait/枚举间接层；集群装配期一次注入，绑定 GarnetLog 的
/// enqueue_database_commit 落盘面）
pub type StoreCommitFn = Arc<dyn Fn(AofEntryType, i64) + Send + Sync>;
