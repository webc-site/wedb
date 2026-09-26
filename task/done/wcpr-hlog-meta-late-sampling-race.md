甄别结论：通过（甄别席 zc-fix-r16-hlogmeta，2026-09-26）定级 P1
核验记录（逐锚现码复跑，非票面背书）：
1 C# 相位入口捕获锚成立。HybridLogCheckpointSMTask.cs:38-39 PREPARE 段捕获
startLogicalAddress 与 beginAddress；:54/:57 WAIT_FLUSH 入口同段相邻语句取
finalLogicalAddress = GetTailAddress() 与 headAddress = HeadAddress；
Checkpoint.cs:48-52 WriteHybridLogMetaInfo 只序列化已捕获快照；:54-59
CleanupLogCheckpoint 至 REST 段才 Log.ShiftBeginAddress。亲验属实。
2 矛盾订正（不颠覆结论）。票称「C# 紧缩与检查点共用 StateMachineDriver 单槽互斥、
检查点运行期无并发 ShiftBeginAddress」系对上游误读：现树紧缩链不经驱动注册，
TsavoriteCompaction.cs:65/:141 与 DatabaseManagerBase.cs:447（Shift 档）、
LogAccessor.cs:148（Truncate）均直调 ShiftBeginAddress；驱动单槽互斥仅覆盖
检查点三态与索引扩容（Tsavorite.cs:301/338/387 Register、:857 RunAsync，
StateMachineDriver.cs 实际行号 Register :164-173、RunAsync :187-196，票面
:183-193 微偏）。该订正恰强化票方案：C# 防失靠相位入口快照本身而非紧缩互斥，
rust 亦应采样点上移单点收口、不新增加锁——执行方案未依赖此论断，予以通过并
要求开发时注释锚不得引用「紧缩共用单槽」错误表述。
3 rust 现状锚全部成立。create.rs:251 tail 捕获与 :286-291 HlogMeta begin/head/
flushed 现场采样分离，中间跨 :264 wait_epoch_drain、:271 flush_all、:283
sync_dir_tree 三个 await；flush.rs:87 flush_all 取调用时刻活跃 tail 为目标；
append.rs:25-29 ensure_page_ready flushed 达标自动推 head；shift.rs:117-124
tail 校验对调用时刻活跃 tail、:160 逻辑 begin fetch_max 全量推进、:162 物理
删段受 delete_floor 钳制；compact.rs try_compact（:92-156）全文无检查点相位
互斥，cpr_host.rs:411-428 enter_checkpoint 单槽仅拒扩容不拦紧缩；缺陷现码仍在，
未自证收口。
4 危害链成立。recover.rs:172-217 不变式具名拒启族、:288 flushed.min(tail)
钳制、:554-580 recover_latest 逐代回退、database_manager_base.rs:423-424
发布后即 truncate_until_async(&covered) 物理截 AOF，回退旧代重放段已删，
数据丢失链闭合。
5 查重净。deviations.md §27/§29/§81/§95 与 §37/§53/§96（TTL 采样域）均不同轴；
todo 另三张 wcpr 票（dblreset/ckptgates/rcwait）不同轴，task/ing、task/done
为空；r15-dura 归档只核发布顺序与删段地板；stale_window_sampling 测试系写路径
版本位采样，已收口且非同轴。
6 架构合规与可执行度。方案系单点采样上移对齐 C# 相位入口快照，单向分层、
单套机制、无新锁无假桩无过度设计；改动点具体到行，测试引用 freeze_cross_round/
roundtrip/meta_tamper/version_floor 夹具亲验真实存在于 wcpr/tests/cpr/，验证
闭环；纯文本格式合规。定级 P1：低概率时序窗，触发即拒启回退加 AOF 已截断的
静默永久丢失，长跑写载可达。

合入哈希：7d8ca38 收口形态：create.rs 第 3 步 WAIT_FLUSH 同步段与 tail 同一无 await 区段
增采 cp_begin/cp_head、第 9 步 HlogMeta 改用该快照（flushed 维持后采样、恢复侧
flushed.min(tail) 钳制在位），恢复侧零改动；tests/cpr/meta_sampling_race 补驱逐推 head
与紧缩推 begin 两形态真实原语竞态回归（落盘 meta 满足 begin<=head<=min(flushed,tail)<=tail
且 recover_latest 不再拒启）。

审核结论：通过（审核席 zcode-r17-review-metasample，2026-09-26）

双侧源码亲验记录：
1 C# 契约锚全部属实。HybridLogCheckpointSMTask.cs PREPARE 段（:35-40）同步段捕获
startLogicalAddress 与 beginAddress；WAIT_FLUSH 入口（:49-57）finalLogicalAddress =
GetTailAddress() 与 headAddress = HeadAddress 相邻同步语句同段捕获，此刻运行时不变式
head <= tail 保证捕获恒自洽；PERSISTENCE_CALLBACK 只序列化已捕获快照（Recovery/
Checkpoint.cs:48-52 WriteHybridLogMetaInfo），BeginAddress 推进推迟到 REST 段
CleanupLogCheckpoint（Checkpoint.cs:54-59 Log.ShiftBeginAddress）；紧缩与检查点共用
StateMachineDriver 单槽互斥（StateMachineDriver.cs:164-175 Register 与 :183-193 RunAsync
的 CompareExchange 空槽抢占），检查点运行期无并发 ShiftBeginAddress。
2 rust 现状属实。create.rs:251 tail 与 :286-291 HlogMeta 采样分离，中间跨第 5 步
wait_epoch_drain、第 6 步 flush_all、第 8 步 sync_dir_tree 三个 await；flush_all
（wkv/src/store/flush.rs:87）取调用时刻活跃 tail 为刷盘目标，flushed 推进远超捕获
截断点；append.rs:25-29 ensure_page_ready 在 flushed 达标时自动推 head，窗口内写入
回绕环形页池即可推 head 越过捕获截断点；shift.rs:113-168 shift_begin_address 的
tail 校验（:117-124）对的是调用时刻活跃 tail（窗口内持续增长），逻辑 begin
（:160 fetch_max）不受捕获截断点约束，物理删段（:162）才受 delete_floor 钳制；
紧缩入口 wkv/src/gc/compact.rs try_compact 全文无任何检查点相位互斥，随常驻 GC
回收轮与检查点完全并发，until 单源 safe_ro（flush_all 经 seal 链已把 safe_ro 推过
捕获截断点），窗口内 begin 越过捕获截断点真实可达。
3 危害链成立。recover.rs:172-217 地址不变式校验族（begin > tail / head > tail /
head < begin / flushed < begin / flushed < head 具名拒启）与 recover_latest 逐代回退
（recover.rs:554-580 跳过无效检查点回退更早版本）亲验属实；database_manager_base.rs
检查点发布成功后即 truncate_until_async(&covered) 截断 AOF，回退旧代所需重放段
[prev_covered, covered) 已被物理删除，数据丢失链闭合。integrity_crc32 封签只覆盖
采样结果本身，拦不住时序撕裂，定性正确。
4 查重净。deviations.md §27（恢复侧 flushed 前缀短读拒启）、§29（AOF 超限守护
续跑）、§81（checkpoint-dir 旋钮）、§95（无盘同步键门）均不同轴；全册采样类条目
（§37/§53/§96）均系 TTL 时钟采样域；r15-dura 档（task/review_history/
zcode-r15-dura.md）维度 5 只核 create.rs 发布顺序（meta 先行、release 后置）与
截断地板闭合，未覆盖采样点时序，无在册冲突。
5 方案最小性确认。采样点上移到第 3 步同步段后，捕获时刻运行时不变式
begin <= head <= flushed <= tail 保证落盘 meta 恒过恢复校验族；flushed 维持第 9 步
后采样成立（恢复侧 recover.rs:288 flushed.min(tail) 钳制在位；head 先采样、
flushed 后采样次序自洽：head_c <= 当场 flushed <= 第 9 步 flushed）。head 提前采样
使恢复装载 [head, tail) 驻留窗口变宽，与 C# WAIT_FLUSH 入口捕获行为一致，无内存
正确性影响。单点改动、无新锁无新机制。

问题分析：
1. Garnet 契约对齐。C# HybridLogCheckpointSMTask 在状态机相位入口的同步段一次性捕获元数据，持久化阶段只序列化已捕获快照：PREPARE 段捕获 startLogicalAddress 与 beginAddress（HybridLogCheckpointSMTask.cs:35-40）；WAIT_FLUSH 入口同段先取 finalLogicalAddress = store.hlogBase.GetTailAddress() 再取 headAddress = store.hlogBase.HeadAddress（同文件 :47-59，相邻同步语句，此刻运行时不变式 head <= tail 使捕获值恒自洽）；PERSISTENCE_CALLBACK 的 WriteHybridLogMetaInfo（Recovery/Checkpoint.cs:48-52）只序列化 _hybridLogCheckpoint.info，不重读任何地址。本轮检查点生命周期内的 BeginAddress 推进被推迟到 REST 段 CleanupLogCheckpoint（Checkpoint.cs:54-59 的 Log.ShiftBeginAddress）；且 C# 紧缩与检查点共用 StateMachineDriver 单槽互斥（StateMachineDriver.cs:164-196 的 CompareExchange 空槽抢占），检查点运行期不存在并发 ShiftBeginAddress。
2. 工程现状确证。rust 侧 create_checkpoint_inner 在第 3 步同步段捕获一致性截断点（wedb/wcpr/src/manager/create.rs:251 let tail = store.tail_address()，对标 C# WAIT_FLUSH 入口），但 HlogMeta 的 begin_address 与 head_address 推迟到第 9 步元数据落盘前才现场采样（create.rs:286-291），中间横跨第 6 步 flush_all 全量刷盘、第 7 步屏障放行、第 8 步 sync_dir_tree 目录树 fsync 等多个 await 窗口（大库可达秒级）。窗口内两类推进不受检查点控制：其一，前台写入的环形页池回绕驱逐自动推进 head（wedb/whlog/src/hlog/append.rs:19-45 ensure_page_ready 在 flushed 达标且 head 落后时自动 shift_head_address，:25-29），flush_all 把 flushed 推进到活跃尾（远超捕获 tail），窗口内持续写入驱动 head 可越过捕获 tail；其二，紧缩链 shift_begin_address 对逻辑 begin 全量 fetch_max 推进（wedb/whlog/src/hlog/shift.rs:113 起，物理删段受 delete_floor 钳制但逻辑 begin 照常推进），紧缩入口无 ResizePhase::Checkpoint 互斥（wedb/wkv/src/gc/compact.rs 全文无 phase 检查），窗口内 begin 可越过捕获 tail。
3. 逻辑危害确证。产出 head_address > tail_address 或 begin_address > tail_address 的检查点元数据，integrity_crc32 封签覆盖的是采样结果本身，拦不住时序撕裂。崩溃恢复时该违例被 recover_checkpoint_components 的具名校验拒启（wedb/wcpr/src/manager/recover.rs:172-198，BeginAddress 超出 TailAddress / HeadAddress 超出 TailAddress），recover_latest 回退更早检查点；而本代检查点发布成功后上层已执行 AOF 截断至本代 covered（wedb/wnode/src/database/database_manager_base.rs:422-425 truncate_until），回退旧检查点所需重放段 [prev_covered, covered) 已被物理删除，旧检查点之后的写入永久丢失。另注意 begin 越界形态下即便侥幸通过校验，恢复重放窗 [begin, tail) 为负区间同样构成非法视图。触发条件为检查点刷盘窗口内驱逐或紧缩恰越过截断点，长跑写入负载下可达。

涉及代码：
rust 文件与函数：
wedb/wcpr/src/manager/create.rs:create_checkpoint_inner（:251 tail 捕获与 :286-291 HlogMeta 现场采样）
wedb/whlog/src/hlog/append.rs:HybridLog::ensure_page_ready（写入路径自动推进 head）
wedb/whlog/src/hlog/shift.rs:HybridLog::shift_begin_address（逻辑 begin 全量推进）
wedb/wcpr/src/manager/recover.rs:recover_checkpoint_components（:163-240 地址不变式校验族）
wedb/wnode/src/database/database_manager_base.rs:take_database_checkpoint_async（:422-425 AOF 截断）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/HybridLogCheckpointSMTask.cs:GlobalBeforeEnteringState（PREPARE 捕获 begin，WAIT_FLUSH 同段捕获 tail 与 head）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Recovery/Checkpoint.cs:WriteHybridLogMetaInfo（只序列化已捕获快照）与 CleanupLogCheckpoint（REST 段才 ShiftBeginAddress）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs:Register 与 RunStateMachine（紧缩与检查点单槽互斥、单次清槽）

精炼执行方案（审核席整理版，供 task/fix.md 直接消费）：
1 wedb/wcpr/src/manager/create.rs 的 create_checkpoint_inner 第 3 步同步段（take_range_index_checkpoints 与 store.tail_address() 同一无 await 区段，:250-251 附近）增采两个局部变量：let cp_begin = store.begin_address(); let cp_head = store.head_address();（与 tail 同点，捕获时刻运行时不变式 begin <= head <= flushed <= tail 恒自洽）。
2 第 9 步 HlogMeta 构造（:286-291）改用局部变量：begin_address: cp_begin, head_address: cp_head；flushed_until_address 维持 store.hlog().flushed_until_address() 第 9 步后采样不变（次序自洽：cp_head <= 当场 flushed <= 第 9 步 flushed，恢复侧 recover.rs flushed.min(tail) 钳制在位）；tail_address: tail 维持捕获值。
3 补注释锚：create.rs 采样点注明对齐 C# HybridLogCheckpointSMTask.cs WAIT_FLUSH 入口 GlobalBeforeEnteringState 同段捕获（finalLogicalAddress 与 headAddress 相邻同步语句），并注明 begin 推迟到 REST 段 CleanupLogCheckpoint 的 C# 时序对照（rust 逻辑 begin 由紧缩链推进、物理删段由第 10 步 release_history_until 收口，均不在采样点内重读）。恢复侧 recover.rs 不变式校验族保持现状零改动（拒启语义正确，错在创建侧采样点）。
4 测试验证点（wcpr tests/ 下新增竞态回归，两条形态同断言：落盘 meta 满足 begin <= head <= min(flushed, tail) <= tail 且 recover_latest 恢复成功不再拒启）：
a 驱逐形态：小容量页池夹具（num_pages 压小），检查点 flush_all 窗口注入并发写入驱动环形页池回绕驱逐，ensure_page_ready 自动推 head 越过捕获截断点；
b 紧缩形态：窗口内并发触发 shift_begin_address 推 begin 越过捕获截断点（可直接调 shift_begin_address 或以 GC 夹具触发）；
c 既有 freeze_cross_round / roundtrip / meta_tamper / version_floor 套件回归，另跑 ./sh/clippy.sh 零警告。
