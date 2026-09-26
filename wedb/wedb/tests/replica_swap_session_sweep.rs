//! 在线引擎置换断开存量会话回归（task/done/zcode-r20-wtxn 发现三 + r37-lockfix 发现 A）
//!
//! C# 锚点：副本恢复为原位恢复（libs/server/Databases/SingleDatabaseManager.cs:
//! RecoverCheckpointAsync 同一 store 对象重建），全体会话经 StoreWrapper.cs:41
//! 单计算属性恒见同引擎，「锁面==写面」不变量天然成立。rust 实例置换形态下
//! 存量会话执行域在 get_session 构造期钉死旧引擎，而事务锁表/屏障闭包现取
//! 当前引擎——置换后存量连接 MULTI/EXEC 将屏障注册与桶闩落新引擎、读写落
//! 旧引擎，互斥双侧落空。[`ClusterProvider::swap_online_store`] 的收口动作：
//! 投槽前重挂宿主写面钩子束、投槽并联动管理面后清扫 [`ConsumerRegistry`]
//! 中持旧引擎执行域的客户端会话（CLIENT KILL 同一机制面，客户端重连即装配
//! 于新引擎）；集群互连会话（client_type Master/Replica/Slave，CLIENT LIST
//! 同一语义面）与慢路径挂起条目豁免。
//!
//! 立据纠偏（r39-txnfix2 第二节，登记级）：发现 A 原文以「全量同步→清扫→
//! 重连→再全量同步构成自激循环」为 P1 危害叙事，该循环已被现码三点证伪
//!（swap 后收敛段零 await、慢路径应答竞速必胜、APPENDLOG wire 建连晚于
//! 清扫），修复正确但立据错误；真实残留为 P2 清扫过宽（握手期互连误杀、
//! 未知节点 gossip 互连落 Normal，均一轮收敛）与跨实例爆炸半径（本文件
//! 第二用例的实例级射程收口）。

use std::sync::Arc;

use parking_lot::Mutex;
use wedb::server::cluster_provider::ClusterProvider;
use wnode::{
  servers::{ClientView, ConsumerRegistry, ConsumerType},
  session_parse_state_extensions::ClientType,
};
use wtest_base::open_test_store;

static TEST_LOCK: Mutex<()> = Mutex::new(());

/// 置换清扫：客户端会话必须断开，复制与集群互连会话必须豁免
///
/// 断言取 [`wnode::servers::ConsumerEntry::kill_session`] 的首杀语义反证：
/// 被清扫条目的首杀位已置位，其后再杀必返回 false——修复前置换不动注册表，
/// 客户端条目此断言为 true（首杀位未被置位），即存量会话带旧引擎执行域
/// 存活、锁面/写面分叉；无差别清扫形态下集群条目此断言为 false（被误杀），
/// 即自断集群总线与复制拓扑（P2 清扫过宽复发）
#[test]
fn swap_online_store_sweeps_clients_and_spares_cluster_links() -> aok::Result<()> {
  let _guard = TEST_LOCK.lock();
  let registry = Arc::new(ConsumerRegistry::new());
  registry.install_global();
  // 客户端会话（view 缺省 Normal = 持旧引擎执行域的键写面，清扫射程内）
  let stale_client = registry.register(4101, "remote:swap-sweep".into(), "local:swap-sweep".into());
  // 集群互连会话（CLIENT LIST 语义的 Master 型节点间链路，豁免射程外）
  let cluster_link = registry.register(4102, "remote:peer".into(), "local:peer".into());
  cluster_link.update_view(ClientView {
    client_type: ClientType::Master,
    ..ClientView::default()
  });

  // 复制会话消费者（ConsumerType::Replication，握手期 view 仍为 Normal 缺省，豁免射程外）
  let replica_conn = registry.register_with_type(
    4103,
    "remote:replica".into(),
    "local:replica".into(),
    ConsumerType::Replication,
  );

  // 集群总线会话消费者（ConsumerType::Cluster，Gossip 互连握手期，豁免射程外）
  let cluster_bus_conn = registry.register_with_type(
    4104,
    "remote:clusterbus".into(),
    "local:clusterbus".into(),
    ConsumerType::Cluster,
  );

  // 慢路径挂起中的客户端会话（避免丢应答，豁免射程外）
  let slow_wait_client = registry.register(
    4105,
    "remote:slow-client".into(),
    "local:slow-client".into(),
  );
  slow_wait_client.set_in_slow_wait(true);

  let provider = ClusterProvider::new();
  let (_dir, new_store) = open_test_store("swap_session_sweep_new")?;

  // 换入引擎投槽（宿主钩子束与实例注册表未注入为退化形态，清扫回退
  // 进程级 global 兜底，无害空转）
  provider.swap_online_store(Arc::clone(&new_store));

  assert!(
    !stale_client.kill_session(),
    "置换必须断开存量客户端会话：清扫后首杀位应已置位（kill_session 再杀返回 false）"
  );
  assert!(
    cluster_link.kill_session(),
    "集群互连会话必须豁免清扫：首杀位未被置位（杀断拓扑即 P2 清扫过宽复发）"
  );
  assert!(
    replica_conn.kill_session(),
    "复制消费者必须豁免清扫：杜绝副本恢复自断主从连接"
  );
  assert!(
    cluster_bus_conn.kill_session(),
    "集群总线消费者必须豁免清扫：杜绝副本恢复自断集群连接"
  );
  assert!(
    slow_wait_client.kill_session(),
    "慢路径挂起会话必须豁免清扫：防止杀掉挂起条目丢应答"
  );
  ConsumerRegistry::reset_global();
  aok::OK
}

/// 置换清扫射程实例级收口（票 zcode-r37-lockfix 发现 A 残留，txnfix2 第二节
/// 6 条跨实例爆炸半径）：注入实例注册表后，清扫只断本实例的客户端条目；
/// 进程级 global 中他实例条目（多实例同进程形态的 Normal 连接）不越槽被杀。
/// 修复前清扫恒经 ConsumerRegistry::global 进程级单槽，嵌入双实例形态下
/// 一个实例换引擎会杀死同进程其他实例的全部 Normal 连接
#[test]
fn swap_online_store_sweep_is_instance_scoped() -> aok::Result<()> {
  let _guard = TEST_LOCK.lock();
  let own = Arc::new(ConsumerRegistry::new());
  let other = Arc::new(ConsumerRegistry::new());
  // 他实例注册表挂进程级槽（复现 global 单槽可见面）
  other.install_global();

  // 本实例客户端条目（清扫射程内）
  let own_client = own.register(4201, "remote:own".into(), "local:own".into());
  // 他实例客户端条目（global 可见，但实例槽射程外）
  let other_client = other.register(4202, "remote:other".into(), "local:other".into());

  let provider = ClusterProvider::new();
  provider.set_consumer_registry(Arc::clone(&own));
  let (_dir, new_store) = open_test_store("swap_session_sweep_scoped")?;
  provider.swap_online_store(Arc::clone(&new_store));

  assert!(
    !own_client.kill_session(),
    "本实例客户端会话必须被清扫：首杀位应已置位"
  );
  assert!(
    other_client.kill_session(),
    "他实例连接不得越槽误杀：global 单槽中的他实例 Normal 条目首杀位不应置位"
  );
  ConsumerRegistry::reset_global();
  aok::OK
}
