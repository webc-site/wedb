主库检查点完成回调的集群分支整体缺失：SAVE/BGSAVE/AOF 超限链把 AOF 无条件物理截到尾，副本钉线旁路、检查点历史不登记

来源：next/glm.db.md 条 5 立项（该文件本波剪空删除）。取证基线：主仓 /Users/z/git/db/wedb
分支 dev，行号按当下 HEAD 的符号重取。判定：成立且待做。

结论一句话
C# 拍完检查点后按形态二分：集群形态交给复制域（OnCheckpointInitiated 取 covered、完成后
AddNewCheckpointEntry 登记历史并带钉线安全截断），单机形态才 TruncateUntil + Commit。rust 只落了
单机分支，且不分形态无条件执行，集群主库每次 SAVE/BGSAVE/AOF 超限打点都会把 AOF 物理截到尾地址，
正在消费的慢副本的段被直接删掉，检查点历史也不登记，副本 attach 时无 entry 可查。更硬的一条
证据是内核自己的文档注释已经写了两分形态，代码没有实现。

现状（主仓 HEAD 实测）
1. 无集群分支的检查点内核：/Users/z/git/db/wedb/wedb/wnode/src/database/database_manager_base.rs:186-250
   take_database_checkpoint_async。covered 恒取 aof.tail_address()（:206-219，无任何角色
   判定），拍后 :233-245 无条件 aof.truncate_until_async(&covered) + aof.commit_flush_async()。
   同函数头注 :182-186 自述「随后 AddNewCheckpointEntry（集群 && AOF）登记检查点条目并安全
   截断；单机形态 TruncateUntil + Commit」——文档承诺与实现不符，属注释先行的未接线形态。
2. 三条生产链全走此路径：SAVE/BGSAVE
   /Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:1015-1031（:1027、:1038 →
   single_database_manager.rs:204 take_checkpoint → :205 内核）；AOF 体积超限周期任务
   /Users/z/git/db/wedb/wedb/wnode/src/service.rs:518-537 spawn_aof_size_limit_task →
   /Users/z/git/db/wedb/wedb/wnode/src/database/single_database_manager.rs:216-230
   checkpoint_within_pause_gate（:221 副本轮空、主库直落内核）。
3. 集群等价物在场却零接线：/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_provider.rs:1243-1256
   on_checkpoint_initiated（含 update_commit_safe_aof_address）全仓生产零调用，唯一引用是
   wedb/wedb/tests/{cluster_provider.rs:42,67、checkpoint_wiring.rs:50}；:1262-1285
   add_new_checkpoint_entry（登记 CheckpointMetadata 历史 + safe_truncate_aof）唯一生产调用挂在
   :1047-1071 take_on_demand_checkpoint，而该入口仅副本 attach 链消费
   （/Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_sync_session.rs:303）。
4. 类型面也接不通：wnode 侧的集群句柄是静态虚表
   /Users/z/git/db/wedb/wedb/wnode/src/cluster_provider.rs:278-302 ClusterProviderVtable，
   检查点相关只暴露 checkpoint_version_shift_start/end（:294-295，trait 默认空实现 :149/:157），
   database_manager_base 的 D/S 类型上没有任何承接 on_checkpoint_initiated /
   add_new_checkpoint_entry 的口，故当前形态下 wnode 不可能调到集群回调。
5. 后果取证：钉线截断内核
   /Users/z/git/db/wedb/wedb/wedb/src/server/replication/aof_sync_driver.rs:433-466
   safe_truncate_aof 会先按 min_aof_address_from_active_sync_tasks 回拉再删段——本路径完全旁路它，
   直接调 GarnetLog::truncate_until_async。检查点历史缺失还会连带使复制域 CheckpointStore 的
   淘汰链（其 add_checkpoint_entry → delete_outdated_checkpoints，
   /Users/z/git/db/wedb/wedb/wedb/src/server/replication/checkpoint_store.rs:132-141、:175-200）
   在集群主库的常规打点上永不触发，快照目录随之堆积（该面单独立单，见
   task/ing/checkpoint-dir-retention-wiring.md）。

C# 参考
/Users/z/git/db/wedb/garnet/libs/server/Databases/DatabaseManagerBase.cs:501-535
InitiateCheckpointAsync（:507-512 EnableCluster 时 covered 由
StoreWrapper.clusterProvider.OnCheckpointInitiated 取，否则取 AOF TailAddress 并
SetCurrentSafeAofAddress；:528-533 注释 "If cluster is enabled the replication manager is
responsible for truncating AOF" → AddNewCheckpointEntry，:535-538 else 分支
TruncateUntil + Commit）；接口声明
/Users/z/git/db/wedb/garnet/libs/server/Cluster/IClusterProvider.cs（OnCheckpointInitiated /
AddNewCheckpointEntry）；/Users/z/git/db/wedb/garnet/libs/cluster/Server/ClusterProvider.cs:170-186
SafeTruncateAOF（PRIMARY 经 AofSyncDriverStore 按全副本最小已发位点回拉）。

修法
第一步补口：在 wnode 的 ClusterProvider trait 与静态虚表上加两口
（on_checkpoint_initiated(&mut covered) 与 add_new_checkpoint_entry(full, covered, token, token)，
异步口按现有 SlowFuture 形态），wedb 侧 ClusterProvider 把已在位的
CheckpointCallbackFace 实现转发进去，去掉 wnode 侧默认空实现能瞒过缺失的可能（要么实现要么
不提供默认）。第二步改内核：take_database_checkpoint_async 按句柄在位与否二分——
集群形态 covered 经 on_checkpoint_initiated 取、拍后调 add_new_checkpoint_entry；单机形态保持
现有 truncate_until + commit_flush 不变（C# 的 TruncateUntil/Commit 两步在 rust 已由
truncate_until_async 单点物理承接，勿造第二套）。第三步把 covered 随检查点元数据发布的现有
publish_checkpoint_aof_address 调用与集群分支的位点口径对齐（副本形态 covered 是起始标记位点，
不是尾地址）。配套需要一条集群双节点集成测试：主库 SAVE 期间副本 attach 慢消费，断言慢副本正在
读的段未被删、CheckpointMetadata 历史有新增条目。

边界
与 task/ing/aof-driver-register-pre-transfer.md（副本同步流的驱动注册时机）不重叠：那条管注册
在前，本条管截断链选了错误的口。
与 task/ing/replica-replay-checkpoint-end-arm.md（同波新立，副本重放臂拍检查点）共用这一对回调
面，建议同批实施、一处收口，先主后副。
与 task/ing/checkpoint-dir-retention-wiring.md 分工：本条管回调接线，那条管快照保留策略。
本条还含一处「文档承诺集群分支、代码未实现」的注释失真，实施时以代码事实为准订正头注。

优先级
功能缺口且带线上破坏性（集群主库每次打点即删慢副本在用的段），高档，仅次于
task/ing/flush-safe-read-only-bound.md。
