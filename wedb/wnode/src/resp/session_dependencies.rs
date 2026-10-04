//! 会话依赖注入聚合组件（对标 C# StoreWrapper 共享依赖组）

use std::sync::Arc;

use wacl::GarnetAclAuthenticator;
use wconf::RuntimeServerConfig;
use wlua::StoreScriptCache;
use wmetric::SlowLogContainer;
use wpubsub::subscribe_broker::SubscribeBroker;
#[cfg(feature = "tls")]
use wtls::ServerTlsConfig;
use wtxn::{TxnLockTable, WatchVersionMap};

use super::ItemBroker;
use crate::{aof::garnet_append_only_file::GarnetAppendOnlyFile, primary_tasks::PrimaryTasks};

/// 会话共享依赖组件集合（对标 C# StoreWrapper 共享依赖组：统一单次注入会话）
///
/// 故意不派生 `Clone`：依赖组为一次性装配包，移动语义强制单次消费，防止
/// 漏装某项依赖的半装配会话被二次复用。
pub struct SessionDependencies {
  /// WATCH 版本表（事务管理器依赖）
  pub watch_version_map: Arc<WatchVersionMap>,
  /// 所属引擎实例锁表句柄（C# SessionFunctionsWrapper.cs:30
  /// `LockTable => _clientSession.store.LockTable`：会话不自建锁表，
  /// 一律从所属实例取句柄）
  pub lock_table: TxnLockTable,
  /// 全局脚本缓存（C# StoreWrapper.cs:120 `storeScriptCache` 实例级
  /// ConcurrentDictionary：生命周期绑定服务器存储实例而非会话，SCRIPT LOAD /
  /// EVAL / SCRIPT FLUSH 经会话共享同一字典，跨连接脚本共享与刷新契约的唯一
  /// 真值源）
  pub store_script_cache: Arc<StoreScriptCache>,
  /// 集合项经纪（阻塞命令唤醒）
  pub item_broker: Arc<ItemBroker>,
  /// 服务器运行时配置
  pub runtime_config: Arc<RuntimeServerConfig>,
  /// 慢日志容器
  pub slow_log_container: Arc<SlowLogContainer>,
  /// ACL 认证器（可选；None = 免认证形态。requirepass 在装配期即裹成带口令
  /// 的 ACL 实例，认证源只有这一档。无状态纯判定组件，只读共享免锁）
  pub acl_authenticator: Option<Arc<GarnetAclAuthenticator>>,
  /// 发布订阅中枢（可选）
  pub pubsub: Option<Arc<SubscribeBroker>>,
  /// Primary 类后台任务生命周期域（C# RuntimeServerConfig.owner（StoreWrapper）
  /// 的任务域投影：CONFIG SET 产出的调停消息经此触达周期任务启停；
  /// None = 纯协议层 mock 形态，调停无目标仅落槽位）
  pub primary_tasks: Option<Arc<PrimaryTasks>>,
  /// AOF 追加日志门面（C# storeWrapper.appendOnlyFile：CONFIG SET
  /// aof-sync-max-lag-bytes 背压预算调停的推送位；None = 无 AOF 形态）
  pub aof: Option<Arc<GarnetAppendOnlyFile>>,
  /// TLS 证书热加载共享句柄（C# storeWrapper.serverOptions.TlsOptions 的
  /// 会话侧可达面：CONFIG SET cert-file-name / cert-password 经此在线重载；
  /// None = 未装配 TLS，证书对按 "ERR TLS is disabled." 拒绝）
  #[cfg(feature = "tls")]
  pub tls_config: Option<Arc<ServerTlsConfig>>,
}
