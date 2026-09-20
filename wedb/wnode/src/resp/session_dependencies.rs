//! 会话依赖注入聚合组件（对标 C# StoreWrapper 共享依赖组）

use std::sync::Arc;

use parking_lot::Mutex;
use wacl::GarnetAclAuthenticator;
use wconf::RuntimeServerConfig;
use wmetric::SlowLogContainer;
use wpubsub::subscribe_broker::SubscribeBroker;
use wtxn::{TxnLockTable, WatchVersionMap};

use super::ItemBroker;
use crate::{aof::garnet_append_only_file::GarnetAppendOnlyFile, primary_tasks::PrimaryTasks};

/// 会话共享依赖组件集合（对标 C# StoreWrapper 共享依赖组：统一单次注入会话）
///
/// 故意不派生 `Clone`：避免会话级有状态组件（如持有当前用户句柄的 `acl_authenticator`）
/// 被跨会话意外复用造成状态污染与锁争用，通过移动语义强制单次消费。
pub struct SessionDependencies {
  /// WATCH 版本表（事务管理器依赖）
  pub watch_version_map: Arc<WatchVersionMap>,
  /// 所属引擎实例锁表句柄（C# SessionFunctionsWrapper.cs:30
  /// `LockTable => _clientSession.store.LockTable`：会话不自建锁表，
  /// 一律从所属实例取句柄）
  pub lock_table: TxnLockTable,
  /// 集合项经纪（阻塞命令唤醒）
  pub item_broker: Arc<ItemBroker>,
  /// 服务器运行时配置
  pub runtime_config: Arc<RuntimeServerConfig>,
  /// 慢日志容器
  pub slow_log_container: Arc<SlowLogContainer>,
  /// ACL 认证器（可选；None = 免认证形态。requirepass 在装配期即裹成带口令
  /// 的 ACL 实例，认证源只有这一档）
  pub acl_authenticator: Option<Arc<Mutex<GarnetAclAuthenticator>>>,
  /// 发布订阅中枢（可选）
  pub pubsub: Option<Arc<SubscribeBroker>>,
  /// Primary 类后台任务生命周期域（C# RuntimeServerConfig.owner（StoreWrapper）
  /// 的任务域投影：CONFIG SET 产出的调停消息经此触达周期任务启停；
  /// None = 纯协议层 mock 形态，调停无目标仅落槽位）
  pub primary_tasks: Option<Arc<PrimaryTasks>>,
  /// AOF 追加日志门面（C# storeWrapper.appendOnlyFile：CONFIG SET
  /// aof-sync-max-lag-bytes 背压预算调停的推送位；None = 无 AOF 形态）
  pub aof: Option<Arc<GarnetAppendOnlyFile>>,
}
