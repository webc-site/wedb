检查点目录无保留/清理接线：wcpr purge_outdated 全仓零生产调用，主库快照文件集随每次检查点无限堆积

来源：next/glm.db.md 条 7 立项（该文件本波剪空删除）。取证基线：主仓 /Users/z/git/db/wedb
分支 dev，行号按当下 HEAD 的符号重取。判定：成立且待做。

结论一句话
C# 的检查点保留是引擎级自保：状态机每次完成后即调 CleanupIndexCheckpoint /
CleanupLogCheckpoint，检查点管理器用环形 tokenHistory 拍新删旧（removeOutdated 单机默认 true、
集群交给复制域），恢复完成后再清掉全部未用快照。rust 把这套能力的内核写好了（wcpr 的
purge_outdated/purge_all/purge_checkpoint）却一次都没接：主库每次 SAVE/BGSAVE/AOF 超限检查点
都在目录里新增一组 index_<token>.ckpt 与 <token>/ 子目录，永不回收。复制域的 CheckpointStore
淘汰链只在集群路径且只在 add_new_checkpoint_entry 被调用时才触发，而该回调在集群主库的常规
打点链上也没接线（同波另一单），于是集群形态同样堆积。长期运行必现磁盘耗尽。

现状（主仓 HEAD 实测）
1. 内核在位、零生产调用：/Users/z/git/db/wedb/wedb/wcpr/src/manager/mod.rs:521 purge_outdated
   （文档自述「每次成功恢复或周期性快照后调用，即可将检查点磁盘占用约束在 keep 个版本之内」）、
   :496 purge_all、:442 purge_checkpoint，导出面
   /Users/z/git/db/wedb/wedb/wcpr/src/lib.rs:58-60；全仓 grep 的命中只有定义、导出与测试
   （/Users/z/git/db/wedb/wedb/wcpr/tests/cpr/token_layout.rs:79、
   /Users/z/git/db/wedb/wedb/wkv/tests/checkpoint/*.rs）。
2. 生产堆积点：/Users/z/git/db/wedb/wedb/wnode/src/database/database_manager_base.rs:186-250
   take_database_checkpoint_async（拍后只 publish_checkpoint_aof_address + 截断 +
   update_last_save，无任何旧代回收）；三条触发链为 SAVE/BGSAVE
   （/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:1027、:1038）与 AOF 体积超限
   周期任务（/Users/z/git/db/wedb/wedb/wnode/src/service.rs:518-537，按 frequency_secs 周期，
   这是最稳定的堆积源）。
3. 唯一存在的清理形态覆盖面不足：
   /Users/z/git/db/wedb/wedb/wedb/src/server/replication/checkpoint_store.rs:175-200
   delete_outdated_checkpoints（读者闸门通过后 purge_checkpoint 物理删旧 token）只在
   add_checkpoint_entry（:132-141）时触发，而其生产调用点是
   /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_provider.rs:1280（在
   add_new_checkpoint_entry 内，该回调当前唯一生产入口是副本 attach 的
   take_on_demand_checkpoint，:1047-1071）与副本侧
   /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_diskbased_sync.rs:168、
   replica_sync_session.rs:468/:494/:551；单机形态（无 cluster provider）根本不进这条链。
   另有副本导入侧一次性清理 /Users/z/git/db/wedb/wedb/wedb/src/server/replication/
   checkpoint_store.rs:18 purge_checkpoint_files_except（:118 与 replica_diskbased_sync.rs:195
   消费），同样不覆盖主库。
4. 恢复后无清理：/Users/z/git/db/wedb/wedb/wnode/src/database/database_manager_base.rs:120-140
   recover_database_checkpoint_async 选定 token 恢复后直接返回，未用快照原样留盘。
5. 配置面无保留旋钮：/Users/z/git/db/wedb/wedb/wconf/src/runtime_server_options.rs 全域无
   keep/retention/history 类字段（grep 零命中），即想接线也没有承载位。

C# 参考
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexCheckpointSMTask.cs:52
与 /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/HybridLogCheckpointSMTask.cs:67
（状态机完成即 CleanupIndexCheckpoint / CleanupLogCheckpoint）；
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/DeviceLogCommitCheckpointManager.cs:219
PerformAutomaticCleanup => true、:222-235 CleanupIndexCheckpoint 与 :272-290 CleanupLogCheckpoint
（环形 tokenHistory，拍新删前一代）、:337-365 OnRecovery（恢复后清掉全部未被使用的快照）；
分工口径见 /Users/z/git/db/wedb/garnet/libs/host/GarnetServer.cs:396
（removeOutdated = !EnableCluster）与
/Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/GarnetClusterCheckpointManager.cs:34
（集群侧 removeOutdated: false，交给 CheckpointStore 的读者闸门淘汰）。

修法
按 C# 的分工补齐两轨，不要造第三套：
一，单机轨（引擎自保）：在 take_database_checkpoint_async 成功返回后（或 wcpr 检查点状态机
完成点）接 purge_outdated(dir, keep)，keep 取 2 起步并与 C# 的「保留最新一代 + 当前在用一代」
口径对齐；恢复成功后（recover_database_checkpoint_async 及 wnode open_recovered 族）接一次
「清未用」，即 OnRecovery 语义。keep 需要承载位：wconf 补一个检查点保留数槽位（对标
removeOutdated 的语义位，名字按本仓 nested_text 风格定），并同步
RuntimeServerOptions 投影与 CONFIG GET 只读展示。
二，集群轨（复制域淘汰）：沿用 CheckpointStore 的读者闸门，前提是
add_new_checkpoint_entry 在常规打点链上被接线，见
task/ing/primary-checkpoint-cluster-callback.md（同波新立，两单同一批实施即可让集群主库不再
堆积）；本单不重复实现集群淘汰逻辑。
三，接线后 wcpr 的 purge_all 只留给显式 reset/测试形态，勿当常规回收用（它会连孤儿 token 一起
扫）。验证：一条集成用例连续拍三次检查点，断言目录内 token 数不超过 keep；另一条恢复用例断言
恢复后未被选中的旧 token 已消失。

边界
与 task/ing/primary-checkpoint-cluster-callback.md 互补：那条负责把集群回调接上（顺带让集群轨
淘汰可达），本条负责保留策略与单机轨接线。
与 task/ing/replica-replay-checkpoint-end-arm.md（副本重放臂拍检查点）不同面：那条解决副本
不拍也不截的问题，本条解决主库快照文件不清理的问题。
与 task/ing/diskless-full-sync-flush-all.md、task/ing/checkpoint-refuse-while-growing.md 无交叠
（本文件所有跨条引用按文件名认领，实际位置可能在 next/ 或 task/ing/，以队列现状为准）。

优先级
功能缺口（磁盘单调增长，AOF 超限周期任务下必然耗尽），高档。

实际落地事实（分支 sv-retain，载荷 416dbb16，merge 5c3351a6）

一、单机轨接线（票面 修法 一 前半，采纳一棒判断）

1. wedb/wnode/src/database/database_manager_base.rs:329 在
   `take_database_checkpoint_async` 尾段（第 6 步 update_last_save 之后、Ok(true)
   之前）接 `wcpr::purge_outdated(&db.checkpoint_dir,
   CHECKPOINT_RETAIN_GENERATIONS)`，失败只 warn 不回滚已发布快照。该内核是全仓
   唯一检查点落盘面（`create_checkpoint_with_token` 在 wedb/*/src 仅此前后两处
   命中，SAVE/BGSAVE 的 slow.rs、AOF 超限的 service.rs 周期任务、
   take_on_demand_checkpoint 三条链全部汇入本口），故接线点即生命周期单点，
   无第二处需要补。
2. 回收形态位取基座 `cluster` 句柄在位与否
   （`if self.cluster.get().is_none()`），与检查点版本切换标记同一判据，对标 C#
   `libs/host/GarnetServer.cs:396` 的 `var removeOutdated =
   !serverOptions.EnableCluster`：集群形态整步让位复制域 CheckpointStore 读者
   闸门（`delete_outdated_checkpoints` 逐 token `purge_checkpoint`，感知在途读
   者），两轨互斥、不并行按条数纯 unlink。生产装配对位实据：集群宿主
   wedb/wedb/src/server/boot.rs:105 `attach_flush_gate`（该口内部转
   `base.attach_cluster_provider`，见 single_database_manager.rs:65）→ 句柄在位
   → 本步不触发；单机宿主 wedb/wedb_standalone/src/main.rs 全程无 ClusterProvider
   装配 → 句柄缺位 → 本步执行。
3. keep 承载位改判（唯一推翻票面字面要求处）：不补 wconf 槽位，落地为
   `pub const CHECKPOINT_RETAIN_GENERATIONS: usize = 2`。取证：C# 该值是编译期
   常数 `DeviceLogCommitCheckpointManager.cs:19 const byte indexTokenCount = 2`
   （logTokenCount = 1 是索引/日志异 Token 时代的配对余量，rust 统一检查点模型
   一 Token 承载 index/hlog/RangeIndex 一套文件，取两环上界 2 同形），garnet
   全域 `libs/host/Configuration/Options.cs` 无 checkpoint-keep / retention 类
   配置项（grep 仅命中 slowlog-max-len 与 device-throttle-limit 两条无关项），
   配置面唯一的相关位就是决定「是否」自动清理的 removeOutdated 形态位——rust
   已由 cluster 句柄承载。另立数值旋钮属给 C# 无对位之面造配置面，且 CONFIG GET
   只读展示一位常数无信息量，故不落。票面 现状 5「想接线也没有承载位」以本常量
   + 句柄形态位双承载解除。

二、恢复轨接线（票面 修法 一 后半与 现状 4，一棒整段缺席，二棒补完）

1. `recover_database_checkpoint_async` 恢复成功后新增尾段
   `purge_unrecovered_checkpoints`（database_manager_base.rs:187），对标 C#
   `DeviceLogCommitCheckpointManager.cs:337-365` 的 `OnRecovery`：首行
   `if (!removeOutdated) return;`，随后逐个 Delete 非本次恢复 Token 的快照
   （原文 "Purge all log/index checkpoints that were not used for recovery"）。
2. 按**身份**保留（留被选中那一版、删其余全部），不复用 `purge_outdated` 的按
   条数口径——`keep=1` 在显式恢复历史 Token 时会反向误删在用那一版。物理删除复
   用既有内核原语 `wcpr::purge_checkpoint`，未新建磁盘访问面。
3. 启动期形态位的结构差已在函数文档据实登记：rust 启动恢复先于 boot.rs:105 的
   句柄注入（管理器由 `open_from_args` 自建），故集群宿主启动亦走本段；删集与
   集群轨自身的启动清理段重合——replication_manager.rs:1130
   `initialize_checkpoint_store` 的 seed 同取 `find_latest_checkpoint` 那一
   Token，经 `CheckpointStore::initialize` →
   `purge_all_checkpoints_except_entry` 留同一版删其余，两段不构成并行第二套回
   收（运行期恢复入口形态位已就位，集群形态按位让位）。

三、测试（票面 验证 两条均落，未改任何既有断言口径）

1. wedb/wnode/tests/database_manager.rs::test_standalone_checkpoint_retention_bounds_directory
   ——连续拍 3 轮，逐轮断言目录内 token 数被钳制在保留数、保留集恰为最新两代、
   最旧一代的 meta / index / base32 子目录三处物理 unlink（不是只移出清单），
   幸存快照可恢复且内容完整。
2. 新增 ::test_recovery_purges_unrecovered_checkpoints ——两代快照并存时显式恢复
   较旧一代，断言恢复后目录内只余被选中那一版、未选中一代的 meta / 索引 / 快照
   子目录均物理消失，且恢复视图恰为所选那一版（其后一代的写入读不回来）。
3. wedb/wedb/tests/checkpoint_wiring.rs::disk_retention_follows_reader_gate 补
   `dm.attach_flush_gate(provider.clone())` 一行：这不是放宽断言，而是补生产装配
   链缺位——该用例证的是集群轨读者闸门，其宿主在 boot.rs:105 必持 cluster 句柄，
   缺位即被基座判为单机形态、与读者闸门并行出第二轨（恰是本票要杜绝的形态）。
   断言集合一字未改。

四、grep 判据（dev = 5c3351a6 实测）

- `wcpr::purge_outdated` 生产调用点：1（wnode/src/database/database_manager_base.rs:329），
  票面「全仓零生产调用」的黑洞已闭合；其余命中为 wcpr 定义与导出面
  （manager/mod.rs、lib.rs）、内核文档交叉引用（create.rs、recover.rs）、以及
  wcpr/tests/cpr/token_layout.rs 与 wnode/tests/database_manager.rs 的用例。
- `purge_checkpoint(` 生产调用点：wedb 集群轨 checkpoint_store.rs:23/:200/:205
  （读者闸门与副本导入清理，本票不动）+ wnode 基座恢复尾段
  database_manager_base.rs:198（新）。
- `purge_all` 生产调用点：0，消费面只余 wkv/wcpr 测试（票面 修法 三「只留给显式
  reset/测试形态」维持）。
- 单机形态下检查点目录的 token 数上界 = `CHECKPOINT_RETAIN_GENERATIONS`，由第
  一条用例逐轮断言，不再随打点次数单调增长。

五、门禁

- `cargo check --workspace --all-targets`：exit 0，warning 0（含 touch 源文件后的
  强制重检，同样 0）。
- `bun js/check.js`：exit 0，报告与合入前基线逐字节相同（本次接线未新增/消除任何
  C# 符号映射；js/check/ignore/common.yml 与 storage.yml 的自动改写与本票无关，
  已 `git checkout --` 还原；server.yml 仅 `RunPostCheckpointCleanup` 条目的理由
  文案随两轨分工改判同步）。
- 定向用例全绿：wnode/database_manager 4、wedb/checkpoint_wiring 4、
  wnode/recover_test 4、wnode/aof_size_limit_task 5（含与 SAVE 竞争的
  `tokens.len() > 1` 断言，两代保留下仍成立）、wnode/resp_admin 26、
  wnode/storage_api 11、wnode/aof_domain 7、wcpr/cpr 19、
  wedb/{checkpoint_import 5, replication_assembly_e2e 3, cluster_replication_session 3,
  replication_pipeline 6, replication_manager 16, cluster_provider 4,
  diskless_sync_anchor_window 1, diskless_sync_ri_vector 1,
  diskless_loop_convergence 1, large_value_e2e 1}。

六、边界与遗留（不改判，登记给后续单）

1. 集群轨按票面 边界 归 task/ing/primary-checkpoint-cluster-callback.md（落地时
   仍在 ing）：`add_new_checkpoint_entry` 未接常规打点链前，集群主库仍按该单
   收口，本票不重复实现集群淘汰逻辑。
2. `IDatabaseManager::recover_checkpoint_async`（i_database_manager.rs:52、
   single_database_manager.rs:400）全仓零调用者（grep 仅定义与实现两处），恢复
   轨唯一生产入口是 wnode/src/service.rs:782 `recover_checkpoint_store`；本票在
   基座内核接线已覆盖该生产链，该死臂属零消费面普查议题，未随本票删。
3. 配置面 `RuntimeServerOptions.enable_cluster`（wconf/src/runtime_server_options.rs:65）
   在 NodeArgs 投影里无写源（全仓 grep 只有默认 false 与 INFO 读取
   wnode/src/resp/info_provider.rs:59），即 C# `EnableCluster` 的对应位在 rust
   配置面上恒为 false，集群/单机的真实形态判据只活在基座 cluster 句柄上——本票
   据此选句柄位在位性作形态位；该配置面自身的名实问题另立单处理。
