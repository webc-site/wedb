cluster_provider.rs 1705 行单 impl 920 行：按 C# 职责分界切六件（纯移动）

来源：qcode10.design 条 10（MED，「单 impl 920 行，承载四 trait 与多域编排」）。按主仓 HEAD 复核后判定
成立待做，射程与原报同（分件名与归属按其「四 trait 与多域编排」实测结论重排）。
载体说明：本件为 task/reject/qcode10.design.md 台账所列的本题载体；next/cluster-provider-file-split.md
是同题早先壳件（正文更短、沿原报未改写的分件名），派单前并档取一，禁两票各搬一半。

结论
ClusterProvider 在 C# 是 393 行、30 个 public 成员的薄提供方（编排职责落在 ReplicationManager /
ClusterManager / DatabaseManager 等独立类），在 rust 长成 1705 行、主 impl 独占 920 行、78 个 pub 方法，
且四段异质职责同文件堆叠：装配期注入槽、可 Set 的运行期旋钮、复制与故障转移编排判定链、检查点与快照操作面，
外加三个 trait 实现。本轮在该文件里抓出的两处配置断链（on-demand-checkpoint 恒真、replica-sync-delay
只进不出）正是这种堆叠的可读性代价——分件的收益是让「字段—写口—读者」三点能同屏核对。

现状（主仓 HEAD 实测行号）
- 文件：/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_provider.rs，1705 行。
- 类型与 impl 块：PrimaryReplicationAssets :69；struct ClusterProvider :79（字段块到 :182）；
  impl Default :183；主 impl ClusterProvider :225-:1145（920 行）；
  impl IClusterProvider :1147；impl CheckpointCallbackFace :1233；impl WnodeClusterProvider :1289。
- 主 impl 内部职责段（按方法地标，实测）：
  角色与句柄 :228-:270（is_primary、is_replica、new、self_arc、provider_handle）；
  管理器初始化 :271 initialize_replication_manager、:296 initialize_cluster_config、:342 get_connection_info；
  子管理器取口 :351-:369（cluster/replication/failover/migration_manager）；
  运行期旋钮 get/set :377-:471（max_send_buffer_content_size、set_replication_reestablishment_timeout、
  fast_aof_truncate 对、replica_diskless_sync_delay 对、cluster_node_timeout_ms 对、gossip_delay_ms 对、
  gossip_sample_percent 对、preferred_endpoint_type 对）；
  复制编排判定链 :481-:686（ensure_replication :521 的七步健康检查链——本层注释自陈该链在 C# 属
  ReplicationManager.cs:EnsureReplication，因依赖方向反转上收至 provider；start_replication_attach :641；
  gossip_manager :686）；
  装配期注入槽 :694-:992（set_commit_channel、set_store/try_store、set_primary_tasks 族、
  set_version_map、set_vector_manager、set_pubsub、set_aof、set_runtime_config :918/:923、set_wal、
  set_replica_replication_session、set_primary_replication、set_checkpoint_dir、set_store_swap_slot、
  set_database_manager）；
  epoch 机制 :712-:791（current_epoch、bump_current_epoch、bump_and_wait_for_epoch_transition 同步/异步两版）；
  布尔标志对 :992-:1046（on_demand_checkpoint 与 :997 set、allow_data_loss 与 :1012 set、
  replica_diskless_sync 与 set、recover 与 set）；
  检查点与恢复面 :1047-:1131（take_on_demand_checkpoint、swap_online_store、checkpoint_import_ctx、
  reset_sequence_number_generator、set_aof_replay_max_lag_bytes 对、dispose）。
- 规模对照实测：本文件 pub 方法计数 78（计数口径 `^  pub fn`/`^  pub async fn`）。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/cluster/Server/ClusterProvider.cs 全文 393 行、public 成员 30 个；
  构造器注入依赖（:47 `public ClusterProvider(StoreWrapper storeWrapper, RangeIndexManager rangeIndexManager)`），
  故 C# 无 rust 这一整段 set_*/try_* 注入槽；检查点方法族确在本件内（:156 AddNewCheckpointEntry、
  :175 SafeTruncateAOF、:192 OnCheckpointInitiated、:335 PurgeBufferPool），编排类不在此文件
  （ClusterManager.cs、Replication/、Failover/ 各自成件，见 /Users/z/git/db/wedb/garnet/libs/cluster/Server/ 目录）。
- 全仓 grep `class ClusterProvider` 仅此一处命中，C# 没有第二 partial 分件——即 C# 靠「一个薄 provider +
  多个独立管理器」控制体量，而不是靠文件切分。

修法
按上述实测职责段切目录，纯移动不改语义：
- wedb/src/server/cluster_provider/mod.rs —— struct ClusterProvider 字段块、PrimaryReplicationAssets、
  impl Default（角色/句柄/子管理器取口可留本件，对齐 C# 本体规模）。
- cluster_provider/assets.rs —— :694-:992 的注入槽族（除 epoch 面外），即「两阶段构造」的自有形态集中处。
  注：这些槽在 C# 由构造器承担，本件是本仓与 C# 结构性偏离最大的位置，集中后可单独评审是否收拢
  （收拢属另一票，本票不裁）。
- cluster_provider/flags.rs —— :377-:471 运行期旋钮 + :992-:1046 布尔标志对，
  每个旋钮在此件内即可完成「字段—写口—读者」三点核对。
- cluster_provider/replication.rs —— :481-:686 复制编排链（含 ensure_replication 七步链）与 :271/:296 初始化。
- cluster_provider/checkpoint.rs —— :1047-:1131 检查点/恢复/置换面 + :712-:791 epoch 机制
  （epoch 属副本读写一致性栅栏，与检查点/置换同生命周期，故并件）。
- cluster_provider/traits.rs —— impl IClusterProvider :1147、impl CheckpointCallbackFace :1233、
  impl WnodeClusterProvider :1289。
同目录 mod.rs 保持 `crate::server::cluster_provider::ClusterProvider` 引用形态不变；
被拆出的块若需读私有字段，按最小提级 `pub(crate)`，禁 `pub`，禁新增对外 API；
搬位禁夹带改名/改逻辑/改注释措辞（原报「同时清理本文件内的零接线注入槽」一句不属本票——
槽位死活归 on-demand-checkpoint-flag-unwired 与各配置断链票，纯移动票夹带删口会与之互踩），
文档注释里的 C# 锚点随函数同迁。

优先级
打磨（结构与可审性，零功能变更），排最后；但它是本轮两张配置断链票的复发抑制面，
建议在同文件在途行为票清账后紧接着开工。

协调
- 同文件在途票密集，必须串行：on-demand-checkpoint-flag-unwired（:171 字段与 :997 setter）、
  runtime-config-hot-reload-consumers（:421 set_cluster_node_timeout_ms 与 :918/:923 runtime_config 可达面）、
  boot-assembly-projection-single-source（本文件的装配注入调用方 boot.rs）、
  wnode-static-vtable-erase-collapse（本文件实现的 WnodeClusterProvider trait）、
  provider-store-single-accessor、info-store-snapshot-channel、cluster-suspend-await-lock、
  subscribe-broker-shutdown-dispose、aof-tail-witness-freq-config-wiring、
  client-type-remote-node-id-gate、flushall-broadcast-parallel-fanout。
  上述任一票先落地都会移动本票引用的行号——开工前按符号重取地标，不按本档行号。
- 与 task/ing/zero-consumer-dead-surfaces-batch-five.md 无文件交集（那单在 wlua/wbase/wnode/wconn 域）。

验收
- 拆分后 mod.rs 规模与 C# 本体同量级（数百行内），主 impl 不再存在 900 行级单块。
- 全仓 `cluster_provider::` 与 `ClusterProvider::` 外部引用零改动；78 个 pub 方法数量不减不增
  （本票不裁任何死口，死口归上述在册票）。
- ./js/check.js 无新增缺失/虚构锚点。
- cargo check --workspace --all-targets（私有 target 目录）零 error 零 warning；无新增 allow。
  test.sh/clippy 由中央整合轮执行。
