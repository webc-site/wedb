锁定注记（2026-10-01 r9 波主控，基线 `d057182`；只读裁定席四支可达性穷举 + 主控现码亲验闸不对称与夹具证据，锚以本注记为准，台账禁钉行号）
- 病灶：`wedb/wkv/src/store/keyspace.rs::retire_dead_domain` / `::retire_dead_namespace` 的**兜底本地换指臂**——
  在册路由表仍有逻辑库格指向旧物理域时，`alloc_next_virtual_id()` + `routing.table.swap_out(logic_db, new_vdb)`
  （Ns 腿 `insert_ns_mapping(logic_ns, new_vns)`）+ `bump_generation()`，**取号后零落盘**（不经
  `commit_swap`/`try_persist_dbmeta_sync`，不写 `KeyTag::DbMeta`）。
- 唯一生产调用点：`wedb/wnode/src/aof/aof_processor.rs::AofProcessor::process_aof_record_internal` 的
  `AofEntryType::FlushDb` / `AofEntryType::FlushNs` 两臂（`flush_under_barrier` 闭包内
  `store.retire_dead_*(...).await`），另 `wedb/wkv/tests/store/flush_database.rs` 一处测试调用。
- **闸不对称（主控复核坐实，非席面臆断）**：位点/版本闸 `record_gate::should_skip_record` 只在
  `aof_processor.rs::AofProcessor::replay_op_dispatch`（`process_aof_record_internal` 的 `_ =>` 分支）与
  `aof_processor_chunk_replay.rs::replay_chunk` 生效；DbMeta 镜像腿（`aof_processor_store_ops.rs::replay_dbmeta`，
  经 StoreUpsert/StoreDelete 走 `_ =>`）受闸，而 FlushDb/FlushNs 两臂在闸**之前**即就地分派返回。
  单一下界 `aof_floor_of` 切在 DbMeta 条目与同子日志其后 FlushDb 条目之间时，前者被跳、后者照跑 →
  内存格仍指旧域 → 违背臂命中。此即 `doc/zh/db.md`「早于恢复基线的 DbMeta 镜像条目又被 AOF 版本闸挡在应用面之外，
  重启 / 检查点基线后回放即处于『映射权威在磁盘、内存快照为空』形态」所述窗。
- **夹具实证（现码违背臂今日仍在静默行使）**：`wedb/wnode/tests/aof_flush_replay.rs`
  `test_unsafe_truncate_log_flush_db_triggers_store_truncate` / `test_unsafe_truncate_log_flush_ns_triggers_store_truncate`
  两测以裸 `enqueue_safe_flush_aof(FlushDb|FlushNs, .., 0, 0)` 打根域且条目前无 DbMeta 换指批，
  回放即走取号换格臂（根域 (0,0) 启动期恒装载，`db_routing.get(0)` 必 `Some`，`find(vdb == old_vdb)` 必命中），
  现码回 `Ok` 且分配水位 +1 而测试全绿。
- 不可达支（席面判定，主控采纳）：(a) 单节点冷租户重启——非根域 `db_routing` 启动只灌根域，
  `store/mod.rs::rebuild_apply_record` 的 DbMap/DbSwap 臂仅根域执行，`get(&vns)` 回 `None` 即跳过；
  `store/vdb_load.rs::load_routes_of_vns` 逐格点查的是盘上 0x02 **末态**（非换号前态），根域格亦被末态覆写；
  生产时序 `single_database_manager.rs::flush_database` 的 `commit_swap` 先于 `safe_flush_aof`，
  条目按序到达即映射已就位。(c) 副本增量续传——游标由复制位点承担
  （`wedb/wedb/src/server/replication/replica_replay_task.rs` 置 `aof_floor: vec![]`），
  位点只能落在已 ACK 之后，被跳的 DbMeta 腿必已应用并由 `apply_dbmeta_record` 落本节点盘。
- 可达支：(b) 基线/位点切在两条目之间（闸不对称，见上）；(d) 主库 DbMeta 原子批丢失——
  即 deviations `doc/zh/deviations.md §195` 已登记 tolerated 旧域泄漏面（批 safe-order `[map, dead, nextid]`
  的 hlog 落地与 waof 镜像条目属两个独立刷盘面，崩溃可留 AOF 尾段而失 `map` 记录），重启盘上 0x02 仍指旧域 → 臂命中。
- C# 对位：`garnet/libs/server/Databases/DatabaseManagerBase.cs::FlushDatabase` 仅
  `Log.ShiftBeginAddress(TailAddress)` + `AOF.TruncateUntil`，`garnet/libs/server/AOF/AofProcessor.cs`
  的 `case AofEntryType.FlushDb` 重放即重跑同段截断——C# 无虚拟号概念，无可二次分配之「映射」，重放天然幂等。
  故 rust 两臂的**栅栏对齐 + 判死 + 树回收**是多租户扩展的必要承接（`doc/zh/db.md`「主库换号条目即屏障」），
  **取号换格属无对位自创**，且其害已由前案 `task/done/wkv-flush-firstmap-sentinel-replay-retires-live-domain.md`
  坐实（「主从路由永久分叉、紧缩静默丢数据」，见 `aof_flush_replay.rs::test_firstmap_race_produces_no_flush_sentinel_broadcast` 注）。
- 危害链（逐环核消费者）：(i) 真——主库/单机命中后新写按内存格落**未落盘**号（副本面写经 `KeyContextGuard::enter`
  直设条目物理域，故副本侧表现为读空而非写丢）；(ii) `VirtualDbManager::gc_dead` → `pop_reclaimable` →
  纪元到期物理回收的对象是旧域，与镜像腿幂等，非额外危害；(iii) 最重且真——重启按盘回加载，本地号成无主幽灵域，
  它**未入 `gc_dead`**，`is_virtual_id_dead_and_expired` 判不死 → 紧缩永不回收（永久泄漏）且其后写入静默丢失，
  `reclaim_dead_domain_bftrees` 只销毁已判死域，挡不住；(iv) 真——与主库号发散，`bump_watermark` 只挡本地重号，
  挡不住与主库未来取号撞号；自愈臂被短路：根域恒 `authoritative`、`resolve_ns`/`swap_ns` 亦置权威，
  `load_routes_of_vns` 权威即零 I/O 直返，中毒格**永不回建**（仅 `apply_dbmeta_record` 新建的非权威快照可被回建救回）。
- 选型裁定（三案穷举后定 (B)，主控独立复核采纳）：
  (A) 只删取号不上抛——留静默洞（格仍指已判死旧域，写落死域、读空，仍不可自愈）且违背「禁静默冒充」先例口径，**否**；
  (C) 补落盘——直接与 `doc/zh/db.md`「从库完全继承主库的映射体系，不进行本地二次映射」冲突，
  且把副本发明的号固化进盘（重启按盘加载即永久分叉），其立论前提（副本可达支 (c)）本就不成立，**否**；
  **(B) fail-closed**：先按盘上权威点查回建（零取号零落盘），回建后仍指旧域即显式错误上抛留痕——
  与 `doc/zh/db.md` 反查面先例（「显式失败上抛留痕，禁静默以物理号冒充逻辑号定槽——错槽一经登记项盖章即不可自愈」）同形，**采**。
- 前案边界（查重已核）：五池无「回放臂本地二次映射 / retire_dead 兜底取号」同题票；
  `task/done/wkv-vdb-generation-swap-slow-path-blind-alloc-unpersisted-watermark-reuse.md`（§195）裁的是
  **首访慢路径盲分配**的落盘缺失（对位本票可达支 (d)，两票成前后链），本票不重判该臂，亦
  **不在回放臂复刻其落盘形态**；`task/done/r9-waof-replay-screening-20261001.md` 淘汰的是回放侧并行臂折错，
  与本票身份维度异面。
- 禁触域（同侪在途）：`wedb/wedb/src/server/replication/**`（仅作可达证据，零改动）、
  `wedb/wnode/src/resp/objects/hash_commands/read.rs`、`wedb/wnode/src/resp/objects/sorted_set_commands/write.rs`、
  `wedb/wtxn/src/txn_key_entry.rs`、`wedb/wkv/src/vdb/manager.rs`（本票只读其 API，不改其实现）；
  本票只动 `wedb/wkv/src/store/keyspace.rs`（两臂与其签名）、`wedb/wnode/src/aof/aof_processor.rs`
  （两臂错误接线）与 `wedb/wnode/tests/aof_flush_replay.rs`、`wedb/wkv/tests/store/flush_database.rs` 夹具。

审核结论：通过（2026-10-01 主控亲验立案；P2。常规时序下 (a)(c) 两支不可达，但 (b) 闸不对称是真缺口、
(d) 与 §195 登记面直接相连；命中后果是不可逆的永久泄漏 + 静默丢写 + 主从分叉，且现码夹具证明该臂今日仍被静默行使。
低频 × 不可逆 × 明文违背 `doc/zh/db.md`，不达 P1（需崩溃窗或位点恰切两条目之间），定 P2）

AOF FlushDb/FlushNs 回放臂兜底本地换指取号零落盘：违背「从库不作本地二次映射」，命中即幽灵域永久泄漏并静默丢写

问题分析：
1. 契约违背（判据为设计文档而非 C#，本缝属 rust 多租户扩展）：`doc/zh/db.md` 三处明文——
   「映射权威在磁盘，绝不换号」「从库完全继承主库的映射体系，不进行本地二次映射」
   「回建后仍无格可查即本节点对该域确无逻辑入口……显式失败上抛留痕，禁静默以物理号冒充逻辑号定槽」。
   现臂三处全违：不查盘即本地 `alloc_next_virtual_id`（无权威优先）、取号即改内存格（副本面即二次映射）、
   且全程 `Ok` 静默（零留痕）。
2. 闸不对称是真缺口：DbMeta 腿与 FlushDb 腿同属一个日志流、同受单一 `aof_floor_of` 下界，
   但只有 DbMeta 腿过 `should_skip_record`。下界一旦落在两条目之间，回放面即见到「条目顺序正确而映射未就位」的
   组合，兜底臂正是为此而写——它把「未就位」处理成「本地发明一个新号」，把一个可诊断的回放缺口
   转成不可逆的映射分叉（危害 (iii) 的幽灵域不入 `gc_dead`，任何回收臂都不认它）。
3. 落盘形态不可取：本票与 §195 前案的区别在于**面**——§195 收口的是主库首访慢路径（该节点即映射权威，
   补落盘正确）；回放臂的节点是继承方，把本地发明的号写盘等于把分叉固化，重启后按盘加载即永无收敛可能。
   故收口只能取 fail-closed：回建优先、仍不足则显式失败，把不可自愈的静默危害变成可诊断的启动/回放错误。

涉及代码：
rust 文件与函数：
wedb/wkv/src/store/keyspace.rs::retire_dead_domain、::retire_dead_namespace（违背臂本体；签名改 `Result`）
wedb/wkv/src/store/vdb_load.rs::probe_db_mapping、::probe_ns_mapping、::load_routes_of_vns（盘上权威点查回建腿，只读复用）
wedb/wkv/src/vdb/manager.rs::insert_db_mapping、::insert_ns_mapping（装载写入口，单点复用，禁另造）
wedb/wnode/src/aof/aof_processor.rs::AofProcessor::process_aof_record_internal（FlushDb/FlushNs 两臂错误接线）
对应 c# 文件与函数：
libs/server/Databases/DatabaseManagerBase.cs::FlushDatabase（无虚拟号面，重放幂等，本臂属 rust 扩展）
libs/server/AOF/AofProcessor.cs::ProcessAofRecordInternal（case FlushDb）

精炼执行方案：
1. **违背臂改两段式 fail-closed**：`retire_dead_domain(vns, old_vdb)` / `retire_dead_namespace(old_vns)` 删去
   `alloc_next_virtual_id` + `swap_out`/`insert_ns_mapping` 的**取号**语义，改为
   (i) 先按盘上权威回建：库腿经 `probe_db_mapping`（Ns 腿 `probe_ns_mapping`）点查，命中值**非**旧域即以
   `insert_db_mapping`/`insert_ns_mapping` 单点装载（零取号、零落盘）；
   (ii) 盘上仍指旧域（或无格可查而本节点对该域确无逻辑入口）即返回 `Err`，文案携 `vns`/`logic_db`/`old_vdb`
   三值留痕（对标反查面先例口径）；两臂签名改 `Result<(), ...>`，错误进本域既有错误面（禁字符串直曝新类型）。
2. **屏障腿顺序**：判死（`gc_dead.insert`）与 `take_bftree_domain` + `reclaim_bftree_keys` 两腿
   **无条件保留并先于** `Err` 执行，杜绝上抛丢屏障回收（`swap_stamp` 取用点不变，回收期限与尾地址同刻同锁内取）；
   树回收卸载形态（离核、不触条带停车档）一字不动。
3. **消费点接线**：`aof_processor.rs` FlushDb/FlushNs 两臂闭包内以 `?` 上抛；须**实测并申报**：
   错误经 `aof_replay_coordinator.rs::process_synchronized_operation_async` 的 `BarrierJoin` Drop 让栏时
   其余参与者不悬挂（禁把新 `Err` 折成 `let _ =` / `Ok(false)` / 仅 `log::warn`——本项目「禁静默冒充」硬口径）。
   两臂注释「存活命中臂只兜换指未达的并发换号窗」改 fail-closed 口径。
4. 禁做项：禁任何 `alloc_next_virtual_id`/`bump_watermark` 出现在回放臂；禁在回放面（尤其 `as_replica`）
   调 `try_persist_dbmeta_sync`/`commit_swap` 或写 `KeyTag::DbMeta`；禁改 `record_gate.rs` 闸语义
   （闸不对称的**根治**属另一缝，本票只收口违背臂，勿顺手改门）；禁动
   `single_database_manager.rs` 的 FirstMap 灭绝门（前案已收）；禁触 `wedb/wedb/src/server/replication/**`；
   禁 `#[allow]`/`#[expect]`；禁占位实现。
5. 锁测：
   (a) 新册/新臂（wnode）——手工只投 `FlushDb(vns, old_vdb)` 条目且盘上 0x02 仍指旧域：断言回放回 `Err`、
       `next_virtual_id` 零推进、内存格未被覆写、`is_dead_domain(vns, old_vdb)` 为真（判死腿已先行）；
   (b) 正形回归——`wedb/wnode/tests/aof_flush_replay.rs` 的 `test_flush_entry_payload_u64_domain` /
       `test_flush_db_replica_replays_entry_domains_without_local_remap` / `test_flush_ns_replica_replay` /
       `test_firstmap_*` 族保持绿（其夹具已具 DbMeta 批，走装载/空转腿）；
   (c) **夹具修正**（本票自带的现码违背证据）——`test_unsafe_truncate_log_flush_db_triggers_store_truncate` /
       `test_unsafe_truncate_log_flush_ns_triggers_store_truncate` 两测须在条目前补 DbMeta 换指批（或以盘上权威
       预置使回建腿命中），不得靠改断言迁就；
   (d) `wedb/wkv/tests/store/flush_database.rs`（`tests/main.rs` 的 `mod store` leaf 内册，
       跑靶为 `--test main`）的 `retire_dead_domain` 调用点配 `?`（该处格已换新，走空转腿，不触发上抛）。
   revert-proof：撤第 1 步（恢复取号换格且回 `Ok`）后 (a) 的 `Err` 与「水位零推进」断言必红；
   若退化为只删取号不上抛（案 (A) 形态），(a) 的 `Err` 断言仍红——反向钉死 fail-closed 形态，杜绝半收口。
6. 验证面：`cargo check -q -p wkv -p wnode --all-targets` 与 `cargo nextest run -p wnode --test aof_flush_replay`
   （+ `--test aof_replay` 回归）与 `cargo nextest run -p wkv --test main --test keyspace`
   （`flush_database`/`retire_dead_domain` 族在 `--test main` 的 `store` leaf 内，禁照抄 `--test store`）；
   禁在主树或沙箱跑 `./test.sh`/`./sh/clippy.sh`（波次门禁由主控统一跑）。

终态注记（2026-10-01 闭环）：
1. 方案落地：`wedb/wkv/src/store/keyspace.rs` 中 `retire_dead_domain`/`retire_dead_namespace` 删去 `alloc_next_virtual_id` 取号与本地换指，改为盘上权威点查回建（`probe_db_mapping`/`probe_ns_mapping`）+ 单点装载；盘上仍指旧域时判死与树回收无条件先行，显式上抛带三值留痕的 `Err`；
2. 消费接线：`aof_processor.rs` 中 FlushDb/FlushNs 闭包内以 `?` 上抛；BarrierJoin 在 Drop 契约下安全解栏，实测其余参与者零挂起；
3. 锁测覆盖：修复既有两项夹具测试（补换指镜像），新增违背形回放中止与让栏锁测，revert-proof 验证通过。

触面订正（2026-10-01 复用违规补收，dev 合并 2f7230d）：
1. 收口时 Ns 回建腿在 `keyspace.rs` 内留了第二份 `0x01` 键布局实现（自造 `dbmeta_ns_map_key` 助手），
   与 `vdb_load.rs::probe_ns_mapping` 同义重复，违背「一处定义」；
2. 补收形态：`vdb_load.rs::probe_ns_mapping` Visibility 提到 `pub(super)`，`keyspace.rs` Ns 回建腿
   改为 `probe_ns_mapping` 点查 + 仅在新旧 vns 不等时 `insert_ns_mapping` 与 `bump_generation`，
   删除本地第二键布局助手与随之失效的 `session::StoreSession`/`ROOT_DBMETA_PREFIX` 导入；
3. 门禁面：`cargo nextest run -p wkv --test main --test keyspace` 198/198、17/17 全绿。
