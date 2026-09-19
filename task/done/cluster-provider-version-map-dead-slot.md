优先级：低
分拣注记（qw.design 第 11 轮条 4 拆出；浅核 2026-09-19：set_version_map cluster_provider.rs:861、try_version_map :866、boot.rs:111 在场（并行会话致行号小漂移），try_version_map 全仓仅定义行；与 ing/cluster-provider-file-split.md 不同题——那票管文件体量拆分，本票管死槽删除，若该票先落地须按符号而非行号定位）

ClusterProvider 版本表注入槽读侧硬死：写侧活、读口全仓零引用（含测试），与 wnode
provider.watch_version_map 字段真源并存成第二份槽
问题：wedb/src/server/cluster_provider.rs:138 version_map: RwLock<Option<Arc<WatchVersionMap>>>
由 :851 set_version_map 在装配期写入（wedb/src/server/boot.rs:107 真调用），取口 :856 try_version_map
全仓出现次数 = 1（只有定义行本身，生产与 tests 皆无），即该槽注入后无人读取；实际 WATCH 版本表沿
wnode/src/service.rs:867 pub watch_version_map 字段 → resp/session_dependencies.rs:23 → 会话事务链
（resp_server_session.rs:581 形参、:585 装 TransactionManager、:624 注入会话）单线传递。
同文件同形态的 set_vector_manager :862 / try_vector_manager :867 有 5 处生产读者
（migration/frame_import.rs:132、migrate_driver/slots.rs:134、keys.rs:628、:710、
replication/diskless_replication/replication_snapshot_iterator.rs:233），唯此槽半途。
修法：删字段 + set_version_map + try_version_map 并同步 boot.rs:107 装配段（集群侧确需版本表时改为
经 provider 真源反查），禁止「写侧接线、读侧为零」的注入槽留存。
c#：garnet/libs/cluster/Server/ClusterProvider.cs（集群侧只经 storeWrapper 反查同一 VersionMap 实例，
无「再挂一份可写槽」一职）
