AOF 回放把条目物理虚拟域当逻辑域灌 set_context：从库与本机崩溃恢复都做「本地二次映射」

来源：next/glm.my.md 第 10 轮条。取证基线：主仓 /Users/z/git/db/wedb 分支 dev，
行号按符号在当下代码复核（初检 HEAD a7402c4，全部位点在后续 HEAD 复测未漂移）。
原报的四个位点全部命中，仅一处边界陈述需更正（见末段）。

结论

入账侧 keyed 条目的键一律是引擎物理键 [vns][vdb][KeyTag][用户键]，回放端 KeyContextGuard 解出
(vns, vdb) 之后却调用 batch.set_context——这是逻辑域入口：它会 is_virtual 置否、把 (vns, vdb)
当作 (logic_ns, logic_db) 去 get_or_create_ns / get_or_create_db 盲分配虚拟号，并把伪映射
经 try_persist_dbmeta_sync 落盘。正确入口 set_virtual_context（直设物理域、零解析零分配）
在同仓已是物理域处理惯例。FLUSH 族回放臂同病：条目载荷是 (vns, 换号前旧 vdb)，回放闭包
把它直灌 store.flush_database(ns, db_id)，而 flush_database 的入参口径是逻辑域。
净效果与 doc/zh/db.md:203「从库完全继承主库的映射体系，不进行本地二次映射」直接相反，
rust 实现正在做本地二次映射。

现状

1. 回放端逻辑域入口：/Users/z/git/db/wedb/wedb/wnode/src/aof/aof_processor.rs:1194 KeyContextGuard、
   :1204 enter（:1212 batch.set_context(ns, db)，ns/db 由 :1208
   NamespaceDbCodec::decode_tagged_key 从物理键解出），drop 侧 :1222 同用 set_context 复原；
   调用点 :827 replay_op，分块回放同型 /Users/z/git/db/wedb/wedb/wnode/src/aof/aof_processor_chunk_replay.rs:100。
2. FLUSH 族回放臂：aof_processor.rs:578 FlushDb 取 parse_flush_domain 载荷后 :592
   store.flush_database(ns, db)、:598 FlushNs → :605 store.flush_namespace(ns)。
   而 flush_database 的入参是逻辑域：/Users/z/git/db/wedb/wedb/wkv/src/store/keyspace.rs:98
   先 resolve_context(ns, db_id)（冷装载点查磁盘，映射权威在磁盘）再 vdb.flush_db(ns, db_id)
   （/Users/z/git/db/wedb/wedb/wkv/src/vdb.rs:961），返回的 (vns, old_vdb) 才是条目载荷值
   （keyspace.rs:134-138 注释自述），即载荷与入参不同域。
3. 逻辑域入口会盲分配并落盘伪映射：/Users/z/git/db/wedb/wedb/wkv/src/session/mod.rs:312 set_context
   （:318 get_or_create_ns、:324 get_or_create_db、:338-352 DbMetaRecord::NsMap/DbMap/NextId 同步批落盘）；
   分配原语是纯内存盲分配、绝不点查磁盘（vdb.rs:741 get_or_create_ns、vdb.rs:767 get_or_create_db）。
   物理域直设入口在 mod.rs:366 set_virtual_context（is_virtual 置真，只写 active_vns/active_vdb）。
4. 物理域处理惯例已在同仓他处成形：keyspace.rs:56 过期键逐键删除、
   /Users/z/git/db/wedb/wedb/wkv/src/gc.rs:473 与 :683 死域清扫一律 set_virtual_context。
5. 入账侧键确为物理域：/Users/z/git/db/wedb/wedb/wnode/src/service.rs:97 enqueue_raw 原样透传
   StoreEvent 携带的键，:131 physical_key 注释自述「ns/db 取事件携带的会话真值」，
   而会话前缀恒为虚拟号（wkv/src/session/mod.rs:265 session_prefix 由 active_vns/active_vdb
   构造，vdb.rs:22 ROOT_VIRTUAL_ID 与 vdb.rs:644-650 根域种子使 (0,0)→(0,0) 恒等）。
   这解释了为何现有测试全绿：根域两态数值相同，伪映射与真映射重合。
6. 固化错误模型的测试：/Users/z/git/db/wedb/wedb/wnode/tests/aof_flush_replay.rs:151
   test_flush_db_replica_mapping_follow，主库段用 (3,7)/(3,8) 作条目物理前缀，
   从库探针 probe.set_context(3, 7)/:181 set_context(3, 8) 也用同一组数值，
   断言注释「从库映射跟随主库换号」——探针与被测实现同错，故自洽。
7. 槽位分叉：在线面按逻辑域现算（/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:809
   slot_of(self.namespace, self.active_db_id)、wnode/src/resp/garnet_api/mod.rs:487），
   回放面 aof_processor.rs:931 slot_of(session.batch.namespace(), session.batch.active_db())
   经 KeyContextGuard 之后即 slot_of(vns, vdb)——RI 与向量登记表的槽位键主从两端算出不同值，
   SKILL.md:35「同一个 DB 对应同一个槽位」的确定性映射在回放面断裂。

C# 参考

garnet/libs/server/AOF/AofProcessor.cs:147/:164 SwitchActiveDatabaseContext（多库 AOF 按子日志切库，
域值是 db.Id 逻辑库号）、:483-545 ReplayOp keyed 分支。C# 每库独立 store、条目键无域前缀，
本就没有域换算面；rust 单日志加物理前缀承载域是本仓自定义优化（SKILL.md:32 前缀刚性隔离、
SKILL.md:36 从库回放换号条目），因此回放端必须以虚拟域直设，不能重新解析。
规范源：doc/zh/db.md:201-207（主从物理镜像、从库不进行本地二次映射、
从库收到换号条目投递本地 GC 异步队列）。

修法

一、KeyContextGuard::enter 改 set_virtual_context(vns, vdb)，drop 侧对称复原虚拟域，
守卫内部不再触碰 namespace/active_db 两个逻辑槽。is_virtual 为真时 session_prefix 直取
active_vns/active_vdb，写入即落回条目原域，伪映射与伪 DbMeta 落盘一并消失。
二、RI 与向量族回放的槽位口径随之订正：aof_processor.rs:931 需要逻辑域槽位，
故改从守卫携带的域值反查或经 store.vdb 的 vns→logic_ns 逆向表取真值
（vdb.rs:646 active_vns 逆映射已在册），绝不再用 set_context 的副作用凑。
三、FLUSH 族回放臂改物理域退役语义：从库继承映射体系下 FlushDb(vns, old_vdb) 的含义是
「vns 租户路由表中指向 old_vdb 的库格换号并记 gc_dead」，须新增物理域入口
（对标 keyspace.rs:98 的换号事务体，入参换成 (vns, old_vdb) 且不做 resolve_context 逻辑解析），
不得把载荷当逻辑域灌 flush_database；FlushNs 同理按物理 vns 退役整域。
四、aof_flush_replay.rs 的探针改经真实映射装载后按逻辑域验证：主库段先以 set_context 建真映射、
取实际 (vns, vdb) 作条目载荷，从库段断言条目落回继承域且 vdb 管理面无新增逻辑库；
另补一例「checkpoint 基线加 AOF 尾部」形态下非零 vns 的增量回放可见性用例（当前测试面缺该形态）。
五、原报「条目 (vns,vdb) 在从库被物化为 logic_db=new_vdb 的幽灵库」与「伪 DbMap 覆盖污染根域
DbMeta」两后果由一、三自动消解，不需额外补丁。

优先级

功能缺口里的正确性面，且是本文件各条里后果最重的一条：跨租户串数据、增量落伪域后 failover
不可见、回放端写脏 DbMeta。排在其它本文件票之前开工；改动跨 wnode 与 wkv 两层，
单棒处理，勿与向量登记表域隔离（另一票）并档。

边界与原报更正

原报第 7 行称 MIGRATE/CLUSTER SYNC 帧链的 (ns,db) 域断裂「已被在册票承接
（task/done/migration-frame-import-core.md:121-124 在途）」——经核该承接不成立：
该档不在 task/done/，/tmp/fork 下无对应 worktree、git branch 亦无对应分支，按未开始重判。
同段原报另引的 task/done/aof-replay-flush-barrier-single-predicate（多回放任务栅栏竞态）
与 task/reject/swapdb-replica-replay-catchup（拒造 SWAPDB 传播管线）两份档案在当下仓库
同样无同名档、无在途 worktree、无分支，三处引用一律按「未承接」处理，本单不以其存在
为前提设边界；其中「物理 (vns,vdb) 与逻辑 (ns,db) 口径未收口」这一原报归给该拒档的
前置问题，正是本单要结的题。帧导入侧属 migration/net 域，另票处理，但域值口径与本单同源，
两单落地顺序须由编排方排：本单先定「物理域直设、逻辑域解析」的唯一口径，帧侧照抄。
next/vector-registry-nsdb-isolation.md 管向量登记表键 (0,0) 硬编码（不同根，
本单修的是条目域解析入口，两单互不覆盖）。

并发双花登记

同一题面另有一份单问题派发档 next/aof-replay-physical-domain-context.md（拆分自同一
来源档，含本单全部面并多一条「从库换号后条目域物化为幽灵库」的后果展开），
两档取一实施：本单为细化方案（含四步修法与验收），那份为派发摘要（含 rust/C# 行号清单）。
若派发方按 next/ 档直派，须注意其边界段沿用上文三处已失效的档案引用，其余面与本单一致。

验收

1. 从库/恢复后 vdb 映射表条目数与主库一致（不因回放新增逻辑库或租户），
   根域 DbMeta 无 (vns 作 logic_ns) 的伪记录。
2. 非零 vns、非零 vdb 形态下的全量回放与 checkpoint 加尾部增量回放两条链均有端到端用例。
3. 回放面与在线面对同一 (ns, db) 算出的 slot_of 逐值一致。
4. cargo check --workspace --all-targets 零告警；wkv 与 wnode 现有 aof/flush 用例全绿。

落地补记（fixloop sv-aofdomain 棒／第二棒，接一棒 d-aofdomain 残树 66f16f57，2026-09-19）

一、票面四步修法逐条对账（分支 d324241f..HEAD 共 12 文件 +1194/-151）

1. 修法一 已落：wnode/src/aof/aof_processor.rs:1210 `KeyContextGuard::enter` 改
   `batch.set_virtual_context(vns, vdb)`，:1222 drop 侧对称复原，守卫内部不再触碰
   namespace/active_db 两个逻辑槽（is_virtual 恒真，session_prefix 直取
   active_vns/active_vdb），伪映射与 try_persist_dbmeta_sync 落盘一并消失。一棒另在
   wkv/src/session/mod.rs:359 收出新单一读点 `StoreSession::virtual_domain()`
   （session_prefix 退为其字节投影，register/unregister_bftree_key 同路改道）。
2. 修法二 已落：aof_processor.rs:943 回放面槽位改
   `virtual_domain() → vdb.logic_domain_of(vns, vdb) → slot_of(slot_ns, slot_db)`；
   新逆表读端 wkv/src/vdb.rs:835 `logic_ns_of`、:851 `logic_domain_of`
   （全程只读、零分配、零落盘、绝不改动映射），set_context 副作用彻底退出槽位面。
3. 修法三 已落（定性见下文审计二）：wkv 新增 `WedbStore::{flush_virtual_database,
   flush_virtual_namespace}`（keyspace.rs:194 / :294）与
   `VirtualDbManager::{flush_db_virtual, flush_ns_virtual}`（vdb.rs），回放臂
   aof_processor.rs:597 / :617 由 flush_database/flush_namespace 改指物理域放射：
   不做 resolve_context 冷装载点查，故从库不再物化 `logic_ns == 旧 vns` 的幽灵租户。
4. 修法四 已落（一棒漏做，本棒补齐）：
   - wnode/tests/aof_flush_replay.rs 五处探针全改按条目物理域判读（票面第 6 条
     「探针与被测实现同错」即此），载荷正确性判据直读账本
     `vdb.gc_dead.get(&旧 vdb).vns == Some(载荷全宽值)`（截断即落 8 可甄别），
     并逐例加 `mapping_face`（ns_map/db_routing/next_virtual_id 三元组）回放前后不变；
     `test_flush_db_replica_mapping_follow` 更名
     `test_flush_db_replica_replays_entry_domains_without_local_remap`
     （其旧断言模型是逻辑域锁步，与本票口径相反，逻辑域面转由新档承担）。
   - 新增 wnode/tests/aof_replay_domain.rs 四例端到端：checkpoint 基线 + AOF 尾部的
     非零域增量回放、全量回放落回条目域、FlushNs 放射退役继承域、
     回放面槽位与在线面逐值一致。域值取千位量级
     (logic_ns, logic_db) = (1003, 1007) / (2005, 2001)，使物理号与逻辑号永不相等。
5. 修法五 复核成立：「条目域物化为幽灵库」与「伪 DbMap 覆盖污染根域 DbMeta」两后果
   随一、三自动消解，未加额外补丁；验收 1 的判据以 mapping_face 不变 +
   `!is_dead_domain` 反证共同固化（伪记录必经映射面新增才可能存在，映射面零新增即
   根域无伪 DbMeta/伪 NsMap）。

二、一棒结论逐条审计（未采信其自述，全部回代码与 C# 复核）

1. 审计一「按物理域直设即 C# 口径」——采信。
   `grep -rn "VirtualId\|virtualDb\|VirtualDb" garnet/libs/server` 0 命中，
   `garnet/libs/server/Objects` 亦 0 命中：C# 全域无逻辑↔虚拟域换算面，
   AofProcessor.cs:483-545 ReplayOp 把条目域直接绑到既有库实例、FLUSH 回放臂
   （:130-190 SetLogicalContext 仅切 db.Id 逻辑号，本仓单日志物理前缀系自定义承载）
   不做二次解析。故去掉从库二次映射是忠实移植，不是「把错域灌进主存」。
2. 审计二「FLUSH 族物理域退役放射是否第二套清理机制」——判定为票面修法三要求的
   收口，但编排重复已收敛。回放射复用主库同一个 flush_db/flush_ns 事务体与同一步
   取号，两套 vdb 原语的差异属刻意（flush_db 走 get_or_create_ns/routing_for 建快照，
   flush_db_virtual 绝不建空快照且必落退役墓碑——新副本首回放时内存账本与磁盘都可能
   无该项，同键同载荷覆写幂等），非并行清理机制。一棒把「原子批安全顺序
   [新映射?, 旧域退役墓碑?, 0x05 水位?]」「水位只在真正取号时抬升」「取样须在
   lock_dbmeta 串行锁内同刻」三条不变式在四个入口各抄一遍，本棒收为
   `SwapRecords` 三形态（Swapped/FirstMap/RetireOnly）+ `swap_stamp` + `commit_swap` +
   `next_id_rec`（keyspace.rs:28 / :333 / :344 / :355），落盘形态与前后行为逐条等价；
   一棒私造的 `commit_dbmeta_batch` 无其他调用方，删除。
3. 审计三「aof_replay_domain.rs 是否断生产行为」——采信并修夹具三处缺陷，
   未放宽任何断言。判据有效性以 A/B 定：把本票 9 个源文件退回 fork base d324241f
   （cp 还原，未用 stash）单跑，四例全红。修正内容：
   - 他租户「不受波及」探针走冷路由表的 set_context，被引擎口径盲分配成新域
     （实测 (3,6) 而非 (3,4)），生产无错、是夹具：先 `resolve_context` 装载继承映射，
     再按物理域探针，并加 `!is_dead_domain(vns_b, vdb_b)` 反证；
   - 映射不变式判读原在探针之后，被探针自身「按引擎口径物化清后新空间库格」污染
     （水位 5 vs 7）：判读前移至 replay_all 之后，并注记原因；
   - 槽位一例曾提前 `drop(_dir)/drop(_device)/drop(_cp_dir)`：TempDir drop 即删目录，
     后续段创建 ENOENT，删除提前 drop。
4. 一棒的两处「越界」改动判定正确并保留（本票口径的必然推论，非扩面）：
   wkv/src/ttl.rs 与 wkv/src/store/event.rs 把 TtlPurge / RangeIndex 的 StoreEvent 域
   从 `namespace()/active_db()` 改上报 `virtual_domain()`，因 wnode/src/service.rs 的
   条目键由事件携带的域编码（`physical_key(ns, db, KeyTag::Meta, key)`），事件域若为
   逻辑域即与被清记录真实物理前缀不同源，副本按条目域直设即清错库。据此本棒把
   wkv/tests/ttl_purge.rs 的 `TtlPurge { ns: 5, db: 2 }` 逻辑域期望改为映射权威表
   `vdb.get_virtual_ids(5, 2)` 读出的物理域（与被测实现不同源，杜绝自证），并加
   非恒等门 `assert_ne!((vns, vdb), (5, 2))`——A/B 回退 ttl.rs 即红。

三、grep 判据（合入前 sv-aofdomain HEAD 实测）

- `grep -rn "set_context" wedb/wnode/src/aof/` → 0 命中（回放面已无逻辑域入口）
- `grep -rn "set_virtual_context" wedb/wnode/src/aof/aof_processor.rs` → 1182（注）
  / 1210（enter）/ 1222（drop）三处，别无第二写口
- `grep -rn "flush_virtual_database\|flush_virtual_namespace" wedb | grep -v tests/`
  → 定义 keyspace.rs:194 / :294，调用 aof_processor.rs:597 / :617——回放臂各一处
- `grep -rn "commit_dbmeta_batch" wedb` → 0（一棒第二收尾单点已删）；
  `grep -rn "commit_swap\|swap_stamp" wedb/wkv/src/store/keyspace.rs` → 13 命中
  （定义各 1 + 四处入口各取样 1 落盘 1 + 注记），不变式只此一份
- `grep -rn "logic_domain_of" wedb/wnode/src` → 唯一读点 aof_processor.rs:943（另 1 注）
- `grep -c "assert_ne!" wedb/wkv/tests/ttl_purge.rs` → 1（非恒等门在册）

四、门禁

- `cargo check --offline --workspace --all-targets`：合并前（基线 d324241f）exit 0 /
  0 warning；`git merge dev`（dev = 488dcc99，无冲突）后复跑 exit 0 / 0 warning。
  本机 crates.io 索引不可达，全部 cargo 命令加 `--offline`
  （偏离票面字面命令，环境所迫，覆盖面不变）。
- `bun js/check.js`（worktree 内，exit 0）：基线取同 commit 的 dev 导出
  （`git archive <基线> | tar -x -C <dir>` + `ROOT_DIR=<dir> bun js/check.js`），
  合并前对 d324241f、合并后对 488dcc99 各比一次，86 行报告均只差 3 处行号漂移
  （合并后：wnode/src/service.rs:606→607、wkv/src/range_index/stub.rs:344→347、
  wnode/src/service.rs:1420→1421，全为本票在上文加行所致），
  miss/ignore 条目零增删；js/check/ignore 无回写，跑后 git status 干净。
- 定向用例（合并后复跑）：aof_replay_domain 4 passed、aof_replay 10 passed、
  aof_flush_replay 8 passed（`--test-threads=1` 亦 8 passed）、`-p wkv` 全绿
  （13 个 target 共 220 passed / 0 failed）。
- 越界红登记（A/B 证 dev 既有，非本票引入）：`cargo test -p wnode --no-fail-fast`
  114 个 test target，合并前 7 红（acl_tests 4、lua_script_tests 2、
  resp_server_session_tests 1 = acl_limited_user_filters_commands、
  pending_latency_timing 1、resp_vector_set 3、tiered_background_demote 与
  vector_set_production_switch 各 SIGABRT）；把本票 9 个源文件退回 d324241f 单跑
  同一批，名单与计数逐条相同，故全部为派发基线既有红。合并 dev（488dcc99 的 wacl
  修法）后 acl 族三红自解，余 4 红：pending_latency_timing::
  pending_read_records_pending_latency、resp_vector_set::{vsim_options_and_output,
  result_writers_honor_bitmap_and_count, disabled_and_reply_encoding}、
  tiered_background_demote 与 vector_set_production_switch（SIGABRT）——肇因同批
  会话指标 / 向量 / 分层 zset 在途改动，转各票处理，本票不越界修。
- 残留（票面修法二口径内，未越权扩面）：`logic_domain_of` 的 `vdb → logic_db` 腿只读
  在册路由表快照，冷租户未装载即回落物理号（`vns → logic_ns` 腿走重建全量装载的
  active_vns 逆表，恒在册，不受影响）。较修复前「两腿皆取条目域」严格变好但未全闭；
  闭法须在该腿加磁盘 DbMeta 点查，属 vdb 装载面，另票处理。

五、三棒收口（合入棒，2026-09-19，分支 sv3-aofdomain）

1. 搬入：二棒撞 150 轮上限死于 06bc9032，实质工作已完成。三棒自 dev 另开
   sv3-aofdomain，`cherry-pick` 搬入 dev..sv-aofdomain 全部 6 个非 merge 提交
   （3666b3c4 回放面物理域直设 + FLUSH 族放射、99f2d7ad 四条端到端用例、
   880a7957 探针订正、b65568dd TtlPurge 判据、625bea07 换号事务单点、
   082aa560 归档补记），全部无冲突自动合并；二棒死树里那份未提交的门禁段订正经
   `git diff -- task/` 取文本判为有效补记（改的是合并后复跑口径与越界红自解名单，
   非残留垃圾），手工并入本票，独立成一条 docs 提交（301c11d7）。
   搬入完整性以树对树自证：`git diff --name-only sv-aofdomain HEAD -- wedb/` 命中的
   12 个文件全是 dev 侧后续提交自带（wbase/src/time.rs 停表、cluster_migration.rs、
   resp_server_session_vectors.rs、分层 tiered_* 族），本票 13 文件在
   sv-aofdomain 与合并后 HEAD 之间逐字节相同，无遗漏亦无多带。
2. 复核（未重写任何一行实现）：本票改动 13 文件全部落在 wkv 会话/换号/RI/TTL 事件域
   与 wnode 回放面 + 三个 test 档，`git diff --name-only dev...HEAD` 无
   wresp / wacl / resp_server_session / 成帧路径任何命中——越界面零改动。
   二棒自称的单点在合并后 HEAD 复测成立：
   - `grep -rn "set_context" wedb/wnode/src/aof/` → 0 命中（回放面无逻辑域入口，
     不再从条目读「逻辑域」二次映射），`set_virtual_context` 仅
     aof_processor.rs:1207（enter）/ :1219（drop）两写口（另 1 注 :1179）；
     需要逻辑域的槽位面唯一经 :940 `logic_domain_of` 反查（另 1 注 :1187）
   - `fn swap_stamp` / `fn commit_swap` 定义各 1（keyspace.rs:333 / :344），
     四处换号入口各取样 1 + 落盘 1，keyspace.rs 内 `commit_swap|swap_stamp` 共 13 命中
   - FLUSH 族物理域放射定义各 1（keyspace.rs:194 / :294）、回放臂调用各 1
     （aof_processor.rs:594 / :614）；一棒私造 `commit_dbmeta_batch` → 0 命中
3. 门禁（合并链：91eadacd 派发基线 → 6 提交 → merge dev 至 d54ce713，全部无冲突）：
   - `cargo check --offline --workspace --all-targets`（CARGO_TARGET_DIR 指 worktree）
     合并前、合并 dev 后各一次：exit 0 / 0 warning（262 crate 全 target）。
   - `bun js/check.js` 在 worktree 内跑：exit 0，86 行报告与同 commit dev 导出基线
     只差本票加行造成的 3 处行号漂移（wnode/src/service.rs:606→607、
     wkv/src/range_index/stub.rs:344→347、wnode/src/service.rs:1420→1421），
     miss/ignore 条目零增删、js/check 语料零回写，跑后 `git status` 干净。
   - 定向用例（合并 dev 后）：`-p wnode --test aof_replay_domain` 4 passed
     （tail_flushdb_replay_swaps_inherited_domain /
     full_replay_nonzero_domain_lands_in_entry_domain /
     tail_flushns_replay_retires_inherited_namespace /
     replay_face_slot_matches_online_face_slot）、`--test aof_flush_replay` 8 passed、
     `--test aof_replay` 10 passed、`-p wkv --test ttl_purge` 3 passed（含
     purge_with_event_sink_suppresses_writes_and_emits_purge 的物理域判据 +
     非恒等门）——共 25 passed / 0 failed，三棒未改任何用例、未放宽任何断言。
   - 二棒登记的 4 余红（pending_latency_timing 1、resp_vector_set 3、
     tiered_background_demote 与 vector_set_production_switch SIGABRT）属派发基线
     既有面，其修法已随合并链入 dev（91eadacd 停表基底 / 向量 RESP3 写入器、
     d54ce713 分层墓碑），三棒按门禁范围未复跑 `-p wnode` 全量，不为其闭合背书。
4. 事故登记（与本票无关，供编排方核）：三棒跑完首轮门禁后 /tmp/fork 整目录被外部
   进程删除（含二棒死树 sv-aofdomain 与一棒死树 d-aofdomain 的工作树，约 20:31），
   worktree 重建 + `git worktree prune` 后自分支续跑；分支与提交未损，门禁在重建后的
   worktree 复跑通过。死树留档面就此失效，勿再指望从 /tmp/fork 取二棒未提交内容。
