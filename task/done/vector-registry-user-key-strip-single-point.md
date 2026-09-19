优先级：重复惯用语 + 反向兜底成泄漏面

4 单问题：登记表复合键「剥域取用户键」这一动作有两处各写一遍，且解码失败时的兜底方向是错的——
失败就把**带域前缀的整键**当用户键发出去，防御臂反成泄漏面。

取证现状（2026-09-19 由向量登记键修红票代理上报，主代理未复核，须自行验证）
- 两处同形写法：split_registry_key(rk).map_or(rk, |(_, k)| k)
  1 wedb/wedb/src/server/replication/snapshot_iter/replication_snapshot_iterator.rs:240
  2 wedb/wedb/src/server/cluster_session/migrate_session/migrate_session_vector_set.rs:75
  （路径若已位移，按符号 split_registry_key 重定位）
- 登记表写入侧单点是 registry_key（复合键 = [NsVarint][DbVarint] + 用户键，对位 C#
  ./garnet libs/server/Resp/Vector/VectorManager.cs:171 / :173 的 dbId 首参形态），
  帧面按 C# 剥域发用户键。

修法（一个机制只留一处）
把剥域动作收成一个公开单点（建议命名 registry_user_key，放登记表所在模块，与 registry_key 配对），
两处调用点改为直调；失败兜底不得返回带域前缀的键——登记表键必由本模块自身构造，解析失败即
不可达态，按本仓既有口径显式失败（panic!/unreachable 一类既有风格，取该模块现用的那种），
不许静默降级。禁止新增第二层可选返回或兼容分支，禁止保留旧的两处内联写法。
补一条确定性用例：对 registry_key 构造出的复合键，registry_user_key 还原结果逐字节等于用户键；
并按当前编码构造一个畸形输入证明其显式失败（断言不得恒真、不得用 catch 兜住后放过）。

改动域：向量登记表模块 + 上述两个调用点及其测试。
避让：wkv/src/vdb.rs、wnode/src/storage/**、waof、wresp 各有修红代理在跑；本票不碰登记键的
编码格式本身（格式改动须另票）。

结案注记（载荷提交 9a40d05，合并 sha 2d5b5cc，载体分支 fix-vector-registry-two）
取证修正：票面「两处同形写法」实测为三处 map_or 兜底臂 + 两处静默丢弃，且第一处路径已位移
（按符号 split_registry_key 重定位）：
- 兜底臂（失败即把带域前缀整键外发，泄漏面）：
  wedb/wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs:240
  （票面写 snapshot_iter/，实为 diskless_replication/）、
  wedb/wedb/src/server/migration/migrate_session_vector_set.rs:75、
  wedb/wedb/src/server/sync_transport.rs:195（sketch 键门收录，票面未列）。
- 静默丢弃：vector_manager.rs for_each_domain_user_key（Option 解不开即整条键不投影）、
  vector_manager_quantization.rs:113（非法键直接 return true 当终态吞掉）。

落点（一个机制只留一处）：剥域动作全数收进 wnode/src/resp/vector/vector_manager_locking.rs
- split_registry_key 改为公开拆解单点，直返 (RegistryDomain, &[u8])；
- registry_user_key 为其用户键投影（与 registry_key 配对的帧面单点）；
- Option 解码降为模块私有 decode_registry_key，调用侧无从再写兜底臂；不可达态按本仓既有
  口径 unreachable! 显式失败，无新增可选返回层、无兼容分支，旧内联写法零残留
  （dev 上 `git grep "map_or(rk"` 断零命中）。
六处消费端（三帧面 + 枚举单点 + 域回收 + 量化）改为直调。

用例（wnode/tests/vector_key_domain_ops.rs）：registry_user_key_strips_to_exact_user_key 对
{空键、短键、长键、栈外键} × {0/0、7/3、1<<40/1<<33} 逐字节全切片相等，并先断复合键必带
域前缀段以防恒真；registry_user_key_fails_loud_on_malformed_composite 以畸形 varint [0x80]
should_panic(expected = "不变量破坏") 证显式失败，未用 catch 兜。

门禁（私有 target /tmp/target-fix-vector-two）
- cargo check --tests -p wnode -p wbase -p wkv：exit 0；cargo check --workspace --all-targets：
  exit 0，warning 计数 0。
- cargo nextest run -p wnode --test vector_key_domain_ops --test vector_set_rename
  --test vector_set_cleanup_vs_reset_race：15/15 passed（含两条新例）；
  cargo nextest run -p wedb --test cluster_migration vector_set_discovery_for_slots：passed。
- bun js/check.js（仅 worktree 内，与合并进的第一父 dev 59511ed 前后对跑）：报告条目集合逐项
  一致，唯一差异是同文件锚点行号漂移 vector_manager.rs:536 → :537（本票在 registry_key 区之上
  净增行），无新增/减少 C# 映射、无新增缺失项、ignore 语料零回写；本票载荷 .cs: 锚点增删为零
  （git show 9a40d05 取证）。
- cargo fmt --check：本票 9 个文件零 diff（树上唯一 diff 在 wedb/wnode/src/resp/txn_resp_commands.rs:456，
  系 dev 侧 wtxn-static-str 带入，非本票射程，未动）。

一棒越界现场处置（本票外，已回退）
工作树另有未提交的 requestDrop 整链删除：wnode vector_manager 的 requested_drops /
request_drop_task_channel / request_drop_in_memory_index、vector_manager_cleanup 的
RequestDrop 协程族与 drop_requested / wait_for_disk_ann_index_drop、vector_manager_locking
的锁侧等待臂、wbase/src/pool/work_set.rs（EventWorkSet，127 行）与其测试件、service.rs 注释、
及 js/check/ignore/{server.yml, libs/server/Resp/Vector/VectorManager.yml} 的配套登记改写
（共 12 文件 +61/-359）。该族改动与本票剥域单点无编译或语义连带（9a40d05 未触及其一），
属 task/ing/vector-request-drop-no-producer.md 判 2 的射程，已整批 git restore 回退。
载荷已导出留档供那一棒棒取：/tmp/fork/vector-request-drop-no-producer-payload.patch（707 行，
含两处暂存态删除）。EventWorkSet 全仓唯一产线消费者确为 requested_drops（HEAD 上 git grep
取证：除自身与自身测试外仅 vector_manager.rs:208 一处），故 work_set 之删属该票、不属本票。
