# gossip 域三问题：增量判定、MEET 清理、参数注入与首轮 MEET

来源：next/ds.net.md 条 4、12 与 next/net.md 条 8、next/glm.md 条 2。三条同在 gossip_manager.rs，逐条独立核实后全部成立，一并处理。

## 一、[P1] gossip 增量判定以 epoch 替代配置版本（成立）

对标：garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs:162 GetMostRecentConfig

C# 语义：每连接持有 lastConfig（上次发送的配置对象），`conf != lastConfig`（对象身份变化）才发全量并序列化，否则发空包 ping。CurrentConfig 每次配置演化（merge、epoch、role、takeover 等）都替换为新对象，故"配置内容变化"必然触发全量。

wedb 偏差：gossip_manager.rs:238 以 `last_sent_epoch != epoch`（本地 config_epoch）判定。配置内容变化而本地 epoch 未变时（如 merge 仅带来远端节点信息）漏发全量，收敛依赖 has_sent_full 首轮兜底。

甄别修正：意见原文称"判定键改配置版本号（与 try_peek_version 同源）"——try_peek_version 读的是线格式协议版本（CLUSTER_CONFIG_VERSION 恒 2），不能做增量判定键。按 C# 语义实现配置演化计数器。

改动点：

1. cluster_manager.rs 新增 config_version: AtomicI64 计数器，flush_config（全部配置演化路径的统一出口：try_merge、try_set_local_config_epoch、try_bump_cluster_epoch、try_set_local_node_role、try_reset_replica、try_stop_writes、try_take_over_for_primary）内递增；提供 config_version() 读取。等价对标"配置对象被替换"；lazy_update_local_replication_offset 不走 flush_config，恰与 C# 引用不变不触发全量一致
2. node_connection.rs last_sent_epoch 改名 last_sent_config_version（对标 C# lastConfig 字段语义）
3. gossip_manager.rs gossip_to_peer_async：判定键改 config_version，成功应答后记账当前版本

## 二、[P2] MEET 响应未验配置版本即反序列化，失败残留临时连接（成立）

对标：garnet/libs/cluster/Server/Gossip/Gossip.cs:161 TryMeetAsync

C# 语义：

1. 先 GetWorkerNodeIdFromAddress 查已知 nodeId 复用现有连接；查不到才新建（created = true）
2. 响应先 TryPeekVersion 校验（:196-202），不兼容 → warn + failed 统计 + created 连接 dispose
3. 反序列化失败走 catch（:223-228）→ created 连接 dispose + failed 统计
4. 成功 merge 后 created 连接以正式 nodeId 入库（AddConnectionAsync），失败则 dispose；此后连接归 gossip 主循环所有

wedb 偏差：try_meet_async 恒以 "address:port" 临时 key get_or_add（不入库语义偏差：临时连接直接进 store）；:98 直接 from_byte_array 无版本校验（gossip 路径 :263 已有 try_peek_version 防护可复用）；空应答/失败/超时分支均不移除临时连接，残留连接被 broadcast_gossip_async 继续遍历。

改动点：try_meet_async 重构对齐 C#：

1. 先 get_worker_node_id_from_address 查已知 id 复用（store 命中则 created = false）；无已知 id 才以 temp key 新建（created = true）
2. 响应先 try_peek_version 校验再反序列化，不兼容 → warn + failed + created 移除
3. 反序列化失败、发送错误、超时、空应答各分支：created 连接统一 try_remove（对标 dispose created）
4. 成功：merge + succeed 统计 + created 时移除 temp key 并以 target_id 重建入库；空应答不再误记 succeed（C# 空应答不计成败）

## 三、[P1] gossip 参数写死且缺首轮 MEET（成立）

对标：

1. garnet/libs/server/Servers/GarnetServerOptions.cs:241 GossipSamplePercent（默认 100）、:246 GossipDelay（默认 5，秒）
2. garnet/libs/cluster/Server/ClusterProvider.cs:60 校验 GossipSamplePercent 越界 [0,100] 抛异常
3. garnet/libs/cluster/Server/Gossip/Gossip.cs:100 TryStartGossipTasks：启动先对 worker 2..=NumWorkers 各 RunMeetTask，再起 gossip 主循环
4. garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs:227 TryMeetAsync 超时用 clusterTimeout（CLUSTER_NODE_TIMEOUT），非 gossipDelay

wedb 偏差：gossip_manager.rs:43-44 gossip_delay 恒 100ms、sample_percent 恒 100 写死；start 无首轮 MEET；:95 meet_timeout 取 gossip_delay.max(3s)。

改动点：

1. wedb/src/args.rs ClusterArgs 新增 gossip_delay_secs（默认 5）、gossip_sample_percent（默认 100），与 cluster_node_timeout_ms 并列（对标 GarnetServerOptions 字段面；C# 中此二参数不经 RuntimeServerConfig 槽位，ClusterManager 构造直读 serverOptions）
2. cluster_provider.rs 新增 gossip_delay_ms: AtomicU64、gossip_sample_percent: AtomicI32 槽位与 setter/getter（默认对齐 C#：5000ms / 100）；装配校验 sample_percent 越界 [0,100] 报错（对标 ClusterProvider.cs:60）
3. main.rs 装配点注入（与 set_cluster_node_timeout_ms 同点，几行最小改动）
4. GossipManager 删除写死字段，gossip_delay()/gossip_sample_percent() 每次 live 读 provider 槽位；meet 超时改用 cluster_node_timeout_ms（对标 clusterTimeout）
5. start 内对已恢复配置的全部已知 worker（2..=num_workers）先 spawn try_meet_async 一轮（对标 RunMeetTask = Task.Run(TryMeetAsync)），再起 gossip 主循环

## 范围与风险

- 主要改动：wedb/wedb/src/server/gossip/gossip_manager.rs、node_connection.rs、cluster_manager.rs（config_version 数行）、cluster_provider.rs（槽位数行）、wedb/src/args.rs、main.rs（注入数行）
- 并发代理在改 cluster_provider.rs 的 epoch 静止逻辑与 migration/、wconn/、replication/：本任务在 cluster_provider.rs 仅新增两个原子槽位与存取方法（几行），merge 前按冲突逐行解
- 回归测试：gossip 收敛（epoch 不变而配置演化时补发全量）与 MEET 失败清理临时连接，参照 wedb/tests/ 既有集群测试基建

## 验收口径

1. ./clippy.sh 零警告（禁 allow）
2. ./test.sh 全量通过
3. bun ./js/check.js 无新增缺失（last_sent_epoch 改名非删除，若 check 报缺失则按规范登记 ignore）

## 验证结果

- 分支 w2-gossip（fork 后 4 commit，含 merge dev；已合并回 dev 并删除 worktree 与分支）
- ./clippy.sh 口径（cargo +nightly clippy --tests --all-targets --all-features -D warnings）零警告，无新增 allow
- ./test.sh 全量通过：fork 内 2001 项、合并后主目录最新 dev HEAD 2002 项，0 失败
- bun ./js/check.js 无输出（0 缺失 0 重复）；last_sent_epoch 改名未触发缺失，无需新增 ignore
- 测试注释去映射格式规避 check.js 误报重复：GossipManager::start 的 TryStartGossipTasks 映射归位 ClusterManager::try_start_gossip_tasks
- 甄别修正一处：意见原文「判定键与 try_peek_version 同源」不成立——try_peek_version 读的是线格式协议版本（恒 2），改按 C# 语义以 flush_config 统一出口递增的 config_version 计数器对标 GetMostRecentConfig 的对象引用比较
- 并发域说明：cluster_provider.rs 仅新增 gossip_delay_ms / gossip_sample_percent 两槽位与存取方法，main.rs 装配点数行注入；未触碰 migration/、wconn/、replication/ 与 cluster_provider 既有逻辑
