甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P0
核验记录（现码复跑，非票面背书）：
1 灭失排查关键复跑：并发合入 wcpr-hlog-meta-late-sampling-race（done，甄别席 r16-hlogmeta P1）已将元数据采样上移至第 3 步同步段——现读 create.rs:269 `let cp_begin = store.begin_address()` 位于 :242-245 `write_index_checkpoint(...).await?` 之后，上移点仍在索引快照固化之后，本票「快照条目地址 < 落盘 begin」形态未因此灭失，危害链原样可达；两票互补裁定维持。
2 rust 窗口面复跑：第 2 步快照 await 与 :269 采样之间无紧缩互斥——wkv/src/gc/compact.rs 现 grep ResizePhase/Checkpoint 零命中、reclaim.rs:29 RELEASE_POLL_MS=200 常驻轮在位、wcompact compact_with_filter 收尾 shift_begin_address 与 whlog shift.rs begin fetch_max 推进链按审核席 r20 亲读成立。
3 C# 锚亲验：HybridLogCheckpointSMTask.cs:38-39 PREPARE 段 `info.beginAddress = store.hlogBase.BeginAddress` 先于一切相位原文现读坐实——对齐方案（入口增采 cp_begin、第 9 步改用）为唯一正解，零新锁零新机制不违 thread-per-core；release_history_until(begin) 钳制使修复侧删段更保守，无副作用。
4 查重：deviations.md 无「快照条目悬挂」在册；四池唯一近轴为已归档的 late-sampling 撕裂票（形态正交：彼为 begin/head>tail 具名拒启，本为 meta 合法下静默悬挂，彼票修复不救济本形态，审核席 r20 第 4 点裁定经本席现码复跑成立）；无 reject 前案。
5 格式与可执行度：纯文本、双侧路径齐全、测试夹具 freeze_cross_round/roundtrip/meta_tamper/version_floor 在位、新增窗口注入测闭环。定级 P0：检查点正常发布无告警下存活键索引静默永久丢失。

审核结论：通过（审核席 zcode-r20-review-snapshothang，2026-09-26）

双侧源码亲验记录（逐锚现码复跑，非票面背书）：
1 C# 契约锚属实。HybridLogCheckpointSMTask.cs:36-41 PREPARE 段捕获 startLogicalAddress（:38）与 beginAddress（:39 store._hybridLogCheckpoint.info.beginAddress = store.hlogBase.BeginAddress），先于 FullCheckpointSM 相位序 IN_PROGRESS → WAIT_INDEX_CHECKPOINT（索引快照刷写，FullCheckpointSM.cs:28-29）→ WAIT_FLUSH（:51-59 才捕获 tail/head）；PERSISTENCE_CALLBACK 的 WriteHybridLogMetaInfo（Recovery/Checkpoint.cs:48-52）只序列化已捕获 info 不重读地址；BeginAddress 推进推迟到 REST 段 CleanupLogCheckpoint（Checkpoint.cs:54-59 Log.ShiftBeginAddress(beginAddress, truncateLog: true) 且用捕获的小 begin 截断）。构造性次序「落盘 begin <= 快照扫描时 BeginAddress <= 快照条目地址」成立。注：在案票甄别已订正 C# 紧缩不经驱动注册可检查点期并发 ShiftBeginAddress——C# 防失恰靠 PREPARE 捕获次序而非互斥，rust 对齐捕获次序即唯一正解，无互斥补课必要。
2 rust 现状锚全部成立。create.rs 第 2 步 :233-241 索引快照固化扫描时点槽位地址（index_ckpt/batch.rs:138 sanitize_data_slot(resolve_slot, None) 写侧不按地址截断；恢复装载侧 :314 才按 tail 截断、不防低于 begin 的地址），第 9 步 :286-291 HlogMeta 现场采样 begin/head，中间横跨 :241 快照 await、:267 排空 await、:271 flush_all await、:283 目录树 fsync await 四窗口（大索引秒级）。try_compact（wkv/src/gc/compact.rs）全文无检查点相位互斥，常驻 spawn_bftree_reclaimer 以 RELEASE_POLL_MS=200ms 节奏驱动（reclaim.rs:29）；compact_with_filter 收尾 store.shift_begin_address(run_res.actual_until)（wcompact/src/compactor/mod.rs:219 亲读）；shift.rs:149 shift_head_address(new_begin) 连带推 head、:160 begin fetch_max 全量推进，:117-124 tail 校验对活跃 tail 而 actual_until <= safe_ro <= tail 恒过——窗口内紧缩推 begin 越过快照已固化地址 a 真实可达。
3 危害链四路亲验闭合。子形态一（a < begin <= index_start）：落盘 meta 全程合法（begin <= tail、head >= begin、head <= tail、flushed >= head），recover.rs:172-217 校验族全过；装载窗 [head, tail)（:281 head = meta.hlog_meta.head_address、:301 from_bounds(begin, head, ...)）不含 a；冷读 whlog/src/hlog/io.rs:148-155 addr < begin 直接 Err(AddressOutOfRange)；run_recovery_kernel 单趟扫描 [begin, tail) 且重插仅 addr >= index_start，a 双重不含；compact.rs:85 紧缩搬迁旁路写监听（AOF 只记原始写效果）且 K 原始写位点低于 AOF floor——四路皆无补救，静默悬挂。子形态二（紧缩 until > index_start）：触发 recover.rs:232-239 index_start < begin 具名拒启，落入彼票回退丢失链；彼票修复（采样上移第 3 步）对两子形态均无救济（上移点仍在快照固化后），本票方案一同时消除两子形态（cp_begin <= 入口 tail = index_start 恒成立）。
4 正交性裁定成立。彼票形态为 begin/head > tail 采样撕裂 → 具名拒启回退叠加 AOF 已截断丢失；本票形态为 meta 全程合法下快照条目地址低于落盘 begin 的静默悬挂。彼票修复后本形态依然可达（第 3 步采样仍在第 2 步快照之后），两票危害形态、代码锚、修复面三者均不同，互补缺一不可。彼票现由 fix.md 接管居 task/ing/（非 todo），不影响正交裁定。
5 查重净。deviations.md §27（恢复 flushed 前缀短读）、§29（AOF 超限守护续跑）、§81（checkpoint-dir 旋钮）、§95、§122（装载失败恒拒启策略）均不同轴，全册无「快照条目悬挂/begin 越过快照固化地址」在册条目；r15-dura 只核发布顺序与删段地板，r17-wcpr 三票不同轴，r20-compaction 立案本票之判净线索 2 论证（快照条目地址 < 其后捕获 tail）恰好不覆盖本形态（begin 无上限序约束），分界清晰。
6 方案最小性确认。单点增采 cp_begin（快照前）+ 第 9 步改用，无新锁无新机制无双机制，控制面单点不渗透数据面；与彼票修复自洽（cp_begin <= cp_head 恒成立，begin <= head 不变式）；物理删段由第 10 步 release_history_until(落盘 begin) 钳制，修复后落盘 begin 变小、删段更保守、无副作用；恢复后快照条目 a ∈ [begin, head) 磁盘区冷读可及且物理段不删（delete_floor = cp_begin <= a），闭环成立。引用测试夹具 freeze_cross_round / roundtrip / meta_tamper / version_floor 亲验存在于 wedb/wcpr/tests/cpr/。格式纯文本合规。

索引快照条目地址可被并发紧缩推进的落盘 begin 越过而悬挂致静默丢键

问题分析：
1. Garnet 契约对齐。C# HybridLogCheckpointSMTask.GlobalBeforeEnteringState 在 PREPARE 段先于全部检查点相位捕获 beginAddress（HybridLogCheckpointSMTask.cs:36-41，其中 :39 store._hybridLogCheckpoint.info.beginAddress = store.hlogBase.BeginAddress），索引快照（FullCheckpointSM 的 WAIT_INDEX_CHECKPOINT）与 WAIT_FLUSH 入口的 head/tail 捕获（同文件 :51-59）均在其后，PERSISTENCE_CALLBACK 只序列化已捕获快照（Recovery/Checkpoint.cs:48-52）。该次序的构造性保证：落盘 meta.begin <= 索引快照扫描时点的 BeginAddress <= 快照全部条目地址。即使检查点窗口内紧缩并发 ShiftBeginAddress 越过快照已固化条目的地址，恢复后该条目地址仍 >= meta.begin，主日志冷读可及（慢路径兜底），键不丢；物理段删除另由检查点发布点钳制，不受 begin 捕获次序影响。
2. 工程现状确证。rust 侧 create_checkpoint_inner 的索引快照逐桶扫描固化当前槽位地址（wedb/wcpr/src/manager/create.rs:233-241 write_index_checkpoint，写侧 sanitize_data_slot 传 None 不按地址截断），而 HlogMeta.begin_address 现场采样于第 9 步（create.rs:287），即使按在案票 task/ing/wcpr-hlog-meta-late-sampling-race 的方案上移到第 3 步同步段（:250-251 附近），采样点仍晚于索引快照固化的时点。紧缩链无检查点相位互斥（wedb/wkv/src/gc/compact.rs try_compact 全文无 ResizePhase::Checkpoint 门，在案票甄别记录已确证；常驻回收轮 200ms 节奏驱动）：compact_with_filter 对存活键 CAS 迁移索引槽位后调 shift_begin_address(actual_until)（wedb/wcompact/src/compactor/mod.rs:219），shift 链逻辑 begin fetch_max 全量推进（wedb/whlog/src/hlog/shift.rs:160）且连带 shift_head_address（:149，补刷后 head 可推越过紧缩区间）。若紧缩在「索引快照已固化某桶键 K→旧地址 a」之后、begin/head 采样之前完成迁移与移位（大索引快照扫描秒级，多核下该窗口真实可达），落盘 HlogMeta.begin/head 即越过 a，且 begin <= tail、head <= tail 全部通过恢复侧地址不变式校验（与在案票的撕裂形态不同，本形态 meta 完全合法）。
3. 逻辑危害确证。恢复侧装载窗为 [head, tail)（wedb/wcpr/src/manager/recover.rs:281 head = meta.hlog_meta.head_address、:301 AddressSnapshot::from_bounds(begin, head, ...)）：a < head 不装载；a < begin 冷读判定失效；恢复重放窗 [index_start, tail) 不含老地址（a << index_start）；紧缩迁移帧刻意不写 AOF（wedb/wkv/src/compact.rs CompactSession::allocate_record「紧缩搬迁旁路写监听」，AOF 只记原始写效果）且 K 的原始写位点低于本检查点 AOF floor 不会重放。索引指向 a 的存活键四路皆无补救，静默永久丢失——检查点正常发布、恢复正常、无拒启、无回退、无告警。与在案 wcpr-hlog-meta-late-sampling-race 票危害形态正交：彼票为采样撕裂（begin/head > tail）导致恢复具名校验拒启后回退旧检查点叠加 AOF 已截断丢失；本票为 meta 全程合法（begin <= tail）下快照条目地址低于落盘 begin 的静默悬挂，彼票修复（采样上移到第 3 步）后本形态依然可达，因为上移点仍在索引快照固化之后。

涉及代码：
rust 文件与函数：
wedb/wcpr/src/manager/create.rs:create_checkpoint_inner（:233-241 索引快照固化、:287 begin_address 采样晚于快照固化）
wedb/wcompact/src/compactor/mod.rs:LogCompactor::compact_with_filter（:219 移位推进 begin）
wedb/whlog/src/hlog/shift.rs:HybridLog::shift_begin_address（:149 连带推 head、:160 begin fetch_max 全量推进）
wedb/wkv/src/gc/compact.rs:GcManager::try_compact（无检查点相位互斥的紧缩调度入口）
wedb/wcpr/src/manager/recover.rs:recover_checkpoint_components（:281/:301 恢复装载窗下界 head 与 begin 边界）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/HybridLogCheckpointSMTask.cs:GlobalBeforeEnteringState（PREPARE 段 :36-41 捕获 beginAddress 先于快照与 WAIT_FLUSH；:51-59 仅捕获 head/tail）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Recovery/Checkpoint.cs:WriteHybridLogMetaInfo（只序列化相位入口已捕获快照）

精炼执行方案（审核席整理版，供 task/fix.md 直接消费）：
1 create_checkpoint_inner 入口同步段（第 1 步纪元排空屏障之前，对标 C# PREPARE 段相位最先捕获，:199 附近）增采局部变量 let cp_begin = store.begin_address()；第 9 步 HlogMeta 构造（:287）begin_address 改用 cp_begin。构造性保证：cp_begin <= 快照扫描时 BeginAddress <= 快照全部条目地址（begin 单调推进），恢复后快照条目恒 >= begin，冷读 [begin, head) 磁盘区可及且物理段受第 10 步 release_history_until(cp_begin) 钳制不删，键不丢；附带消除 begin > index_start 的拒启子形态（cp_begin <= 入口 tail = index_start 恒成立，recover.rs:232-239 校验恒过）。快照内低于 cp_begin 的陈旧同键旧槽无害（链上必有更新版本槽可达）。
2 head_address 维持在案票 wcpr-hlog-meta-late-sampling-race 的上移点（第 3 步同步段）：head 晚采样仅扩大恢复装载窗、无正确性危害，与 C# 分工同型（begin 先行保正确性、head 后行保窗口最小）；两票合施时次序自洽：cp_begin（快照前）<= cp_head（第 3 步）恒成立（begin <= head 不变式）。
3 注释锚写明与 C# PREPARE 段（HybridLogCheckpointSMTask.cs:38-39）先于一切相位捕获 beginAddress 的构造性次序对齐，并引用在案票甄别订正：C# 防失靠捕获次序而非紧缩互斥（紧缩链不经驱动注册可并发直调 ShiftBeginAddress），rust 同样不新增加锁；标注与 wcpr-hlog-meta-late-sampling-race 票边界：彼票修 meta 撕裂拒启（begin/head > tail），本票修快照条目悬挂（begin 合法但 > 快照条目地址），两修复互补、缺一不可。
4 测试验证点：wcpr tests/ 新增回归——小夹具索引 + 紧缩夹具注入「索引快照固化后、begin 采样前」窗口内完成迁移与 shift_begin_address 越过已固化条目地址，断言落盘 meta.begin <= 索引快照文件全部条目地址且 recover_latest 恢复后该键冷读可达不再悬挂（可另断言 index_start >= begin 恒过校验族）；既有 freeze_cross_round / roundtrip / meta_tamper / version_floor 套件回归；跑 ./sh/clippy.sh 零警告。

合入哈希：9652ddd 收口形态：cp_begin 采样上移至检查点入口同步段先于索引快照固化（1:1 对标 C# HybridLogCheckpointSMTask.cs:38-39 PREPARE 段 beginAddress 先于一切相位捕获），第 9 步 HlogMeta 改用冻结值、head 维持前票上移的第 3 步采样（cp_begin <= cp_head 不变式自洽、不回退前票语义），零新锁收口「快照条目地址 < 落盘 begin」静默悬挂并附带消除 begin > index_start 具名拒启子形态，物理删段受 release_history_until(落盘 begin) 钳制更保守；恢复侧 recover.rs 校验族保持拒启语义零改动；新增 index_snapshot_hang.rs 两形态竞态会合回归（真实 shift_begin_address 独立 Runtime 紧缩线程：a<begin<=index_start 静默悬挂与 begin>index_start 拒启，落盘 begin 冻结且恢复冷读不丢键），wcpr 全套 38 用例绿，追平 dev 后 cargo check --workspace --all-targets 零警告零错误。
