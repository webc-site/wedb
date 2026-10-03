甄别结论：通过（甄别席 J5，2026-09-27，定级 P3——磁盘卫生长尾，集群轨同型先例已修，一行枚举口替换性价比成立）。亲验：database_manager_base.rs:189 purge_unrecovered_checkpoints 确用 wcpr::list_checkpoints（meta 过滤 :565-586 仅认 META_PREFIX 完整 .meta），孤儿不入候选；wcpr/src/manager/mod.rs:625 list_all_checkpoint_tokens 物理口与 :610-624 分工文档（明写「含未提交/损坏的孤儿快照」「勿混用」）在位；集群轨 checkpoint_store.rs:31 purge_checkpoint_files_except 已用物理口且注释自陈同型危害先例；create.rs meta rename 发布在 index 快照之后（:377），两步间被杀即沉淀。C# 对照亲验：DeviceLogCommitCheckpointManager.cs:337 OnRecovery（removeOutdated 门）、GetIndexCheckpointTokens/GetLogCheckpointTokens（:235/:285 ListContents 物理列举）。方案枚举口单点替换、恢复选点轨 meta 过滤不动，debug_assert 兼容性论证成立。查重：deviations 与五池无此面，在途复制票系删段时序异轴。派沙箱席 c01f。

审核结论：通过（P3——磁盘卫生与长尾可用性面。理由：孤儿不进恢复视图、零一致性危害，需「检查点写出中途进程被杀」反复发生才逐份累积；但 GB 量级 index 快照多次沉淀可 ENOSPC 令检查点 fail-closed，级联 AOF 无法截断加速占满，且集群轨同型先例已修（wedb/tests/ckpt_purge_orphan_residue.rs 对位修复注记）、修复成本一行枚举口替换，性价比成立。若目标部署形态为频繁 kill -9 / OOM 循环的单机长周期实例，升 P2）

审核席亲验记录（2026-09-27，逐锚现码与 C# 现原型复跑，六项判定）：
1 真实性：属实。purge_unrecovered_checkpoints 枚举口 wcpr::list_checkpoints（database_manager_base.rs:189）确为 meta 过滤（wcpr/src/manager/mod.rs:565-586 仅认 META_PREFIX 完整 .meta），孤儿（index_<b32>.ckpt 与 <b32>/ 子目录、无 meta）不入候选，purge_checkpoint 对其永不调用；purge_outdated 轨（database_manager_base.rs:428-435 → mod.rs:758-767）同型。孤儿来源属实：index 快照 rename 发布（wcpr/src/index_ckpt/mod.rs:136）先于 meta rename（wcpr/src/manager/create.rs:377），两步间被杀即沉淀；write_index_checkpoint 的失败清场只覆盖 .tmp（:57-69），rename 后被杀无清场。实现与自身文档（:157-165 自陈对标 C# OnRecovery 物理枚举）构成契约分叉，非既定改良。
2 回收通道穷尽排查（反证）：全仓生产 purge 口仅四处——database_manager_base.rs:203（meta 过滤枚举）、:429（同）、checkpoint_store.rs:31 与 :218/:223（集群轨，:31 已物理枚举即已修）；wcpr::purge_all（mod.rs:728，内含 sweep_checkpoint_residue 孤儿清扫 :698-721）仅测试消费（wkv recovery/checkpoint_manager、wcpr token_layout 三测试），lib.rs:76 导出零生产调用。无 SIGHUP（signal.rs 仅 SIGINT/SIGTERM 停机竞速）、boot.rs 与 wkv/recover.rs 均无检查点目录清扫。单机轨孤儿确无其它回收通道，票不翻案。
3 C# 对照：属实。Recovery.cs:537 DoPostRecovery 尾段调 checkpointManager.OnRecovery（:337-365 首行 removeOutdated 门），GetLogCheckpointTokens/GetIndexCheckpointTokens（:235/:285）均 deviceFactory.ListContents 物理列举，删除一切非本次恢复 Token（含半截快照）。边界同型确证：C# OnRecovery 仅在检查点恢复成功路径触达（无检查点起库不经 DoPostRecovery），rust has_checkpoint 早退（database_manager_base.rs:129-135）与之同形，票面边界注记成立。
4 方案评审：最小改动形态成立——枚举口单点替换，token != recovered 守卫、删除臂不动；debug_assert（:199-201）兼容性补验通过：物理枚举新增候选只有「新于恢复 Token 的孤儿」（stale > recovered 走首析取臂）与「旧代孤儿」（签发闸门保证版本严格更低），断言两臂均不触发。恢复选点轨（find_latest/recover_latest，mod.rs:646-649 与 recover.rs:534）保持 meta 过滤不动，与 wcpr 分工文档（:612-624）一致。测试点可测：对齐集群轨孤儿锁测（checkpoint_store.rs:387-427）与 wedb/tests/ckpt_purge_orphan_residue.rs 既有夹具形态。
5 查重：deviations.md §27/§29/§40/§74/§78/§81/§95/§122 逐条核对均不圈此面（§74 系 wdev 设备段杂散文件/删段吞错/段尺寸三面，非 wcpr 检查点目录）；task 五池（issue/todo/ing/done/fix.md）与在途票 wedb-repl-snapshot-live-hlog-segment-truncated-under-inflight-reader 不重叠——彼票复制在传读者删段时序（release_history_until/delete_floor/SegmentNotFound），本票单机检查点孤儿枚举面，关键符号零交叠，票面自证属实。
6 格式纯粹度：纯文本无加粗、无表格、无横线分隔符，rust 与 C# 路径双向齐全，行号锚点逐一核对在位。

单机检查点恢复清未用与按代回收两轨均用 meta 过滤枚举，孤儿快照（有 index 文件无已提交 meta）永续泄漏累积至 ENOSPC 令检查点写出 fail-closed——C# OnRecovery 物理枚举语义被绕开，wcpr 物理口与 purge_all 生产零消费

问题分析：
1 Garnet 契约对齐：C# 单机启动恢复尾段 OnRecovery（garnet/libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/DeviceLogCommitCheckpointManager.cs:337-365，首行 removeOutdated 门）经 GetLogCheckpointTokens/GetIndexCheckpointTokens（:235/:285，deviceFactory.ListContents 物理列举、不以元数据完整性过滤）删除一切未被本次恢复选中的快照——进程在检查点写出中途被杀留下的「index 文件在、已提交 metadata 缺」的半截快照同样被物理回收，不留永久垃圾。
2 工程现状确证：rust 的 C# OnRecovery 直接对位是 wedb/wnode/src/database/database_manager_base.rs:185 purge_unrecovered_checkpoints，其文档（:160-164）自陈对标该物理枚举语义，但 :189 实现为 wcpr::list_checkpoints(&db.checkpoint_dir)——而 wedb/wcpr/src/manager/mod.rs:610-624 明文分工：list_checkpoints 仅认含完整 .meta 的有效已提交快照（保护恢复选点），物理全量枚举是 list_all_checkpoint_tokens（:625，文档明写「含未提交/损坏的孤儿快照……勿混用」）。按代回收轨 purge_outdated（database_manager_base.rs:429）同走 meta 过滤枚举。物理口生产唯一消费方在集群轨（wedb/wedb/src/server/replication/checkpoint_store.rs:31 purge_checkpoint_files_except，且带 test_purge_all_except_entry_cleans_orphan_files 锁测 :387），单机轨两处全部漏接；wcpr::purge_all（mod.rs:728）自陈「全量含残留清扫」但生产调用点为零（仅 lib.rs:76 导出与 create.rs:440 文档提及）。孤儿来源真实：create_checkpoint_inner（wcpr/src/manager/create.rs）第 2 步 index ckpt rename 发布、第 9 步 meta 才 rename，:366-378 发布序本身正确，第 2 步后任一点崩溃/kill 即留下无 meta 的 index_<b32>.ckpt 与 <b32>/ RangeIndex 子目录。
3 逻辑危害确证：孤儿文件永不进恢复视图（recover_latest/find_latest 均经 meta 过滤，无一致性危害）；但每次检查点中途崩溃沉淀一份 index 快照（num_buckets×64B 量级，大索引可达 GB）与 RI 树快照子目录，单机长周期反复崩溃累积直至磁盘满，下一轮检查点写出 fail-closed 拒发（SAVE/周期快照/AOF 限长守护连续失败），纯磁盘卫生与可用性面，命中板块 3.2 资源闭环与 2.2 快照协同（「严禁提前删除」之反向：残留不收）。与在途票 wedb-repl-snapshot-live-hlog-segment-truncated-under-inflight-reader（复制在传读者删段时序面）不重叠。

涉及代码：
rust 文件与函数：
wedb/wnode/src/database/database_manager_base.rs:purge_unrecovered_checkpoints（:189 枚举口错用）、take_database_checkpoint_async 按代回收步（:429）
wedb/wcpr/src/manager/mod.rs:list_checkpoints 分工（:610-624）、list_all_checkpoint_tokens 物理口（:625）、purge_all（:728 生产零消费）
wedb/wcpr/src/manager/create.rs:create_checkpoint_inner 发布序（:366-378）、:440 purge_all 兜底自陈
wedb/wedb/src/server/replication/checkpoint_store.rs:purge_checkpoint_files_except 物理口径先例（:31，锁测 :387-419）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/DeviceLogCommitCheckpointManager.cs:OnRecovery（:337-365）、GetLogCheckpointTokens（:235/:285 ListContents 物理列举）

精炼执行方案：
1 purge_unrecovered_checkpoints 枚举口改 wcpr::list_all_checkpoint_tokens（token != recovered 守卫与删除臂不变），与集群轨 purge_checkpoint_files_except 同一物理口径，精确对位 C# OnRecovery；恢复选点轨（find_latest/recover_latest）保持 meta 过滤不动（wcpr mod.rs 分工不可互换）
2 purge_outdated 维持按条数环语义不动物理口径（C# CleanupLogCheckpoint 亦按 tokenHistory 环删同型），孤儿由每轮启动恢复的方案 1 兜底；边界注记：目录完全无有效检查点时 has_checkpoint 早退不执行，首个检查点中途崩溃的孤儿留待下一次有检查点的启动恢复收口（C# 同型可接受）
3 测试验证点：预置无 meta 的 index_<b32>.ckpt 与 <b32>/ 子目录加一份有效检查点，启动恢复后断言孤儿被删、有效版保留、recover_latest 选中不回退（对齐集群轨孤儿锁测形态）

审核裁定执行方案（供 task/fix.md 直接消费）
1 wedb/wnode/src/database/database_manager_base.rs purge_unrecovered_checkpoints：:189 wcpr::list_checkpoints 改 wcpr::list_all_checkpoint_tokens，token != recovered 守卫、debug_assert 与 wcpr::purge_checkpoint 删除臂全不动；同步订正 :157-165 文档枚举面描述（物理全量、与恢复选点轨 meta 过滤分工不可互换，对齐 checkpoint_store.rs:21-29 既有口径注记），集群宿主启动恢复同走本段的删集重合注记（:175-181）维持不变
2 purge_outdated 维持按条数环语义不动物理口径（C# CleanupLogCheckpoint 亦按 tokenHistory 环删同型），孤儿由每轮启动恢复的方案 1 兜底；边界注记保留：目录无有效检查点时 has_checkpoint 早退不执行，首个检查点中途崩溃的孤儿留待下一轮有检查点的启动恢复收口（C# OnRecovery 同型可接受）
3 并发契约注记：本口仅启动恢复与显式 Token 恢复触达，与检查点并发窗的互斥沿 C#「恢复期间禁止取检查点」既有约定（Recovery.cs:543 同款警告），不新增闸、不造第二套机制；wcpr::purge_all 维持零生产消费现状（C# PurgeAll 对位导出面，测试在用），本票不接线不清退
4 测试验证点（单机启动恢复形态，对齐集群轨孤儿锁测 checkpoint_store.rs:387-427 夹具）：预置无 meta 的 index_<b32>.ckpt 与 <b32>/ 子目录加一份有效检查点，启动恢复后断言孤儿被删、有效版保留、recover_latest 选中不回退；补一例「存在新于恢复 Token 的孤儿」断言删除生效且 debug_assert 臂不触发

收口记录（收票席 R3 批次，2026-09-28）：合入 213e04cd（验货 db3960e9）。收口形态=purge_unrecovered_checkpoints 枚举口 list_checkpoints→list_all_checkpoint_tokens（物理全量对标 C# ListContents，孤儿快照穿透回收），守卫/删除臂/debug_assert 不动；按代回收轨（purge_outdated 与并发写出同刻）持 meta 口径勿改并注边界，恢复期禁取检查点契约承并发。锁测 tests/ckpt_recovery_purge_orphan_leak.rs 双臂（旧代/新于基线孤儿+无关文件不波及），回装 meta 口径双红实测。偏差登 §176。
