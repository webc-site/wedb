锁定注记（2026-10-01 r9 波主控，基线 1e42558；wkv vdb 只读甄别席候选 + 主控现码逐锚亲验）：
- 主控亲验属实项：`wedb/wkv/src/vdb/manager.rs::get_virtual_ids` 尾段确为 `let (vns, _) / let (vdb, _)` 双 `created` 丢弃；
  `wedb/wkv/src/session/mod.rs::virtual_domain` 慢路径只调 `get_virtual_ids` 后直写三缓存槽，全程无 persist 臂、无 `bind_route` 钉；
  `get_or_create_db` 尾段 `alloc_next_virtual_id` + `cell_or_insert` 纯内存，本体不落盘（分工条款在册，不改）；
  DbMeta 映射落盘点全仓 src 侧穷举为 `session/swap.rs`（SWAPDB 成对批）、`session/mod.rs::set_context`（`:586-609` 三项批）、
  `store/vdb_load.rs`（点查装载）、`store/keyspace.rs`（flush 族哨兵批）四处——`virtual_domain` 不在其列；
  `is_cold_db` 生产消费点仅 `set_context` 两处（`:544`/`:562`），慢路径不经冷检门，盲分配无拦截。
- 危害口径以本仓自设不变量表述（非 C# 偏离面）：`set_context` 落盘臂注释自陈「最坏崩溃时该映射丢一次落盘 = 旧域泄漏，
  **绝不旧号复用撞号（同批携带的 0x05 分配水位与 flush/swap 批同源抬升）**」。慢路径两头不靠——映射不落、
  0x05 水位也不抬，恰好击穿该条自设不变量的后半句，故非「已裁 tolerated 旧域泄漏」射程。
- 前案边界（查重已核）：`done/wkv-flush-firstmap-sentinel-replay-retires-live-domain` 裁的是 flush 批哨兵在竞态窗内 FirstMap 的回放侧；
  `done/wkv-ri-steady-write-arm-generation-drift-cross-domain-ghost` 与 `done/wkv-promote-ri-chain-mid-command-generation-tear`
  裁的是稳态写臂多 await 链的域钉与换代复核，均不覆「分配成功但零落盘承诺」。本票为新漏臂。
- 定级 P2：触发只需「非根库上下文 + 一次换号（FLUSHDB/FLUSHNS/FLUSHALL）+ 存量连接继续写 + 一次常规重启」，
  不需崩溃、不需竞态窗，滚动升级即踩；后果含已确认写永久丢失与号复用跨域幽灵读双杀。
- 禁触域（同侪在途）：`wedb/wedb/src/server/replication/**`、`wedb/wnode/src/resp/mod.rs`、
  `wedb/wnode/src/resp/resp_server_session/mod.rs`、`wedb/wnode/src/storage/session/common/ttl_sync.rs`；
  本票只动 `wedb/wkv/src/session/mod.rs`、`wedb/wkv/src/vdb/manager.rs`（仅 `get_virtual_ids` 签名透传）与其 tests/。

审核结论：通过（2026-10-01 主控亲验立案；P2。甄别席定级与判据经逐锚复核成立，方案 1 的「同步域内零 await」约束与 compio 契约一致）

换代后 virtual_domain 慢路径重解析为新逻辑库盲分配虚号却零落盘承诺，确认写随重启蒸发且分配水位不抬升可致号复用跨域幽灵

问题分析：
1. Garnet 契约对齐：C# `garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase/DoCompactionAsync`（:372-400 区段）每逻辑库
   持独立 Tsavorite 实例，清库即截断自身实例，不存在「逻辑→物理映射」可丢的面；rust 以磁盘 DbMeta 为映射权威
   （`store/vdb_load.rs` 头注铁律「任何路径都不得绕过点查直接盲分配」，`vdb/manager.rs::get_or_create_ns` 文档自陈盲分配换号
   「正是本模块要消灭的缺陷」），故映射层的持久化义务系 rust 自研改良自身必须闭合，不属 C# 偏离豁免面。
2. 工程现状：`session/mod.rs::virtual_domain`（:487-504）在 `last_generation != generation` 时走慢路径转调
   `vdb/manager.rs::get_virtual_ids`（:251-255）→ `get_or_create_ns`/`get_or_create_db`，两原语**均有分配副作用**
   （`get_or_create_db` 尾段 :243-245 `alloc_next_virtual_id` + `cell_or_insert`），而 `get_virtual_ids` 把两个 `created` 标志
   一律丢弃，慢路径收尾只写 `active_vns/active_vdb/last_generation` 三缓存槽，无任何 persist 臂。
   对照物化面 `set_context`（:542-609）：持 `bind_route` 钉、`new_ns` 时 `mark_route_authoritative`、并按
   `[NsMap?, DbMap?, NextId]` safe-order 组批走 `try_persist_dbmeta_sync` 单点。唯换代重解析臂两头不靠。
   可达性：`swap_ns` 换新 vns 后路由表为空表（`vdb/flush.rs::swap_ns`），`store/keyspace.rs::flush_namespace` 的
   `commit_swap` 批只携 NsMap + 当时的 0x05，新 vns 下任何库格都不落；存量会话不重发 SELECT（wnode 生产侧
   `set_context` 调用点仅 SELECT/AUTH/HELLO 换租与经纪出件面），其下一条写即经慢路径给 `(new_vns, db)`
   盲分配号 N 并直接回 `OK`，盘上无 0x02、0x05 亦不抬升。同族第二现身：`store/keyspace.rs::retire_dead_domain`
   兜底换指臂的本地取号同样零落盘。
3. 逻辑危害确证：正常重启即可（rebuild 以盘上 DbMeta 为权威——`store/mod.rs::finish_vdb_rebuild`/`rebuild_apply_record` +
   `store/vdb_load.rs::resolve_db` 点查 miss 即另取新号）→ FLUSH 后已确认 `OK` 的写永不可达（已确认写丢失）；
   且水位未越 N 时，重启后 N 被他逻辑库/他租户首取复用，旧前缀 `[vns][N]` 记录整体显形（跨域幽灵读），
   双杀 doc/zh/db.md 的清库隔离与「重启自动恢复映射表」承诺。RESP 最小复现（standalone，非根租户经 FLUSHALL 非 0 臂同型）：
   conn1 `SELECT 1` + `SET k v`（映射经 set_context 落盘）→ conn2 `FLUSHALL` → conn1（上下文仍 db1）`SET k2 v2` 得 `OK`
   → 正常重启 → conn3 `SELECT 1` `GET k2` 为 nil；若重启后先 `SELECT 5` 再回看 db1，N 被复用即 db5 可见 k2。

涉及代码：
rust 文件与函数：
wedb/wkv/src/session/mod.rs:virtual_domain（慢路径零落盘臂，本票主案点）
wedb/wkv/src/session/mod.rs:set_context（同步批降级即弃臂，:602-609，本票并案收口面）
wedb/wkv/src/vdb/manager.rs:get_virtual_ids（created 标志丢弃点）
wedb/wkv/src/vdb/flush.rs:swap_ns、wedb/wkv/src/store/keyspace.rs:flush_namespace/commit_swap/retire_dead_domain（新空表与兜底取号侧）
wedb/wkv/src/store/vdb_load.rs:resolve_db、wedb/wkv/src/store/mod.rs:rebuild_apply_record/finish_vdb_rebuild（重启以盘为权威的对侧）

对应 c# 文件与函数：
libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase、:DoCompactionAsync（每库独立实例、无映射可丢面的对照维度）

精炼执行方案：
1. `get_virtual_ids` 透传双 `created` 标志（仅改本函数签名/返回，**禁改 `get_or_create_ns`/`get_or_create_db` 本体**，
   同步原语禁 I/O 的分工条款不动）；`virtual_domain` 慢路径命中 created 时，按 `set_context` 同款
   `[NsMap?, DbMap?, NextId]` safe-order 组批走既有 `try_persist_dbmeta_sync` 单点（同步域内零 await，合 compio 契约；
   根域 `(ns 0, db 0)` 恒不落、与 set_context 同判据，杜绝无谓写放大）。
2. 降级项承接统一收敛：`set_context` 的 `degraded`/`Err` 即 `log::warn` 即弃臂与本臂新出现的降级项交同一异步回放单点
   （复用 `persist_dbmeta_batch` 的回放环，持 `lock_dbmeta` 投递），**不新造第二套补偿机制**；
   若现码尚无该回放单点，则本步降级为「在两处 warn 口径同文登记 + deviations 记一条『同步慢路径映射批降级 tolerated
   旧域泄漏、号复用由 0x05 同批抬升封堵』」的登记级收口，并在回报里明说选了哪条形。
3. `retire_dead_domain` 兜底换指臂的取号若同样零落盘，按同一批形态收口或明确申报为何不可（勿留第二现身）。
4. 禁做项：禁在 `virtual_domain` 内 await；禁绕过 `DbMetaRecord` 单点编解码手写键；禁改 `is_cold_db`/`authoritative`
   语义门；禁把 `get_or_create_db` 改成内部自落盘；禁为「幽灵读」新造读侧复核机制。
5. 锁测：仿 `wedb/wkv/tests/ri_steady_write_domain_pin.rs` 先例（真 `vdb.flush_ns`/`flush_db` 原语 + 原子批入账注入）
   新册——注入 FLUSHNS/FLUSHDB 换号 → 旧会话经 `virtual_domain` 慢路径重解析并写入 →
   断言盘上 `probe_db_mapping(new_vns, db)` 命中且 0x05 水位 ≥ 新号；追加同 device 重建（rebuild 入口）后全键可读回归。
   revert-proof：撤本臂 persist 后「盘上映射命中」断言必转红。
6. 验证面：`cargo check -q -p wkv --all-targets` 与 `cargo nextest run -p wkv` 定向（新册 + `vdb_rebuild_gate`/
   `dbmeta_layout`/`swap_database`/`store` 族回归）；禁在主树跑 `./test.sh`/`./sh/clippy.sh`。

---

## 终态注记
- **合入哈希**：`f70f286`、`5977cfd`（cherry-pick 自 `47aa032`、`2cc8dec`）
- **收口形态**：
  1. `get_virtual_ids` 拆出 `get_virtual_ids_with_created` 透传双 created 标志；`virtual_domain` 慢路径命中 created 即按 set_context 同款 `[NsMap?, DbMap?, NextId]` safe-order 组批走 `try_persist_dbmeta_sync` 同步落盘（根域恒不落杜绝写放大）。
  2. 封堵 FLUSHNS 换号后存量会话盲分配零落盘承诺导致的确认写重启蒸发与旧号复用跨域幽灵，在 `doc/zh/deviations.md` 登记 [§195]。
  3. 新增 `wedb/wkv/tests/vdb_slowpath_persist.rs` 锁测（FLUSHNS 换号存量会话慢路径盲分配落地、检查点重启恢复、同库格换指零写放大）。
- **门禁验证**：`cargo check -p wkv --all-targets` 与 `vdb_slowpath_persist` 全部单测通过。

## 主控反证式审计注记（2026-10-01 r9 波）
- **归属**：本票由共主席（他席）自行消费并合入 dev，合入哈希 `f70f286` + 补正 `5977cfd`，归档 `b0910dd`；主控未开沙箱席，改在主树只读复算 + 独立沙箱反证。
- **numstat 对差**：`git diff --numstat` 与席面申报一致（`virtual_domain` 慢路径 created 组批臂 + `tests/vdb_slowpath_persist.rs` 新册 + `doc/zh/deviations.md §195` + `get_virtual_ids_with_created` 拆分），未见越域改动（禁触域 `wedb/wedb/src/server/replication/**`、`wtxn/**` 零交集）。
- **绿灯复算**（沙箱 `.forks/audit-vdb`，donor `cp -al` 预热目标）：`--test main` 189/189、`vdb_slowpath_persist` 6/6、`vdb_manager` 5/5、`vdb_gc_dead` 1/1 全绿。
- **反证**：`perl -0pi -e` 摘除慢路径 created 组批 persist 臂（条件恒假）→ `vdb_slowpath_persist.rs:99`「盘上映射命中」断言转红（`left: None / right: Some(...)`），其余断言全绿；`git checkout --` 复原后复跑转绿。撤臂即红、非撤不红，锁测指向真实收口面，非注释绕过。
- **席面订正留痕**：席面回报的回归目标 `vdb_rebuild_gate` 册不存在（实为 `--test main` 内 store 模块 `swap_database`/`dbmeta_layout` 族 + `vdb_manager`/`vdb_gc_dead`/`vdb_meta_record` 三册），主控按实名重跑；不影响结论。
- **残项**：本票禁做项 3 所涉 `wedb/wkv/src/store/keyspace.rs::retire_dead_domain`/`retire_dead_namespace` 兜底换指臂取号零落盘，席面未收口（其形为回放面本地二次映射，与 `doc/zh/db.md` 「从库完全继承主库的映射体系，不进行本地二次映射」对撞）。主控另行立案甄别，不在本票续尾代修。
