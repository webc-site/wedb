甄别结论：通过（甄别席 zc-fix-r16-exitreset，2026-09-26）定级 P1
核验记录（现码复跑，逐锚）：
1 rust 锚成立：create.rs:344 显式 store.exit_checkpoint() 与 create.rs:127-133 CkptPhaseGuard::drop 二次调用，cpr_host.rs:435-440 无条件 store(ResizePhase::Rest, Release)；两调用间仅 info!（create.rs:346-349）与函数返回，无 await，跨线程窗口属实。resize.rs 锚逐条吻合：grow CAS :426-441、try_acquire_txn :116-126（SeqCst load 判 PrepareGrow）、barrier_enter :163-177、is_growing :99-104（Rest/Checkpoint 均 false）、split_status/old_index 覆盖面 :484-489、先切表后发相位 :491-500。
2 C# 锚成立：StateMachineDriver.cs:164-173 Register 以 Interlocked.CompareExchange(ref stateMachine, sm, null) 空槽抢占；:345-362 finally 内 :349 Interlocked.Exchange(ref stateMachine, null) 每实例恰清一次自身槽位；:93-117 AcquireTransactionVersion 的 PREPARE_GROW 全屏障亲验。rust 二次无条件复位为移植失真成立。
3 危害链复核成立：显式 exit 置 Rest 后 grow CAS 成功进入，Drop 无条件 store 打回 Rest；phase=Rest 期间 try_acquire_txn/barrier_enter 拦截判据失效、事务钉旧表跨越 :496 切表即丢写；二次 grow 并发覆盖扩容状态；下一轮 enter_checkpoint CAS Rest→Checkpoint 成功致快照与切表并发。ensure_not_growing 判据链路现码亲验。
4 非重复非灭失：deviations.md 无 exit_checkpoint/CkptPhaseGuard/双重退出条目；task/todo、ing、reject、issue 内仅本票命中该关键词；近邻票 wcpr-checkpoint-gate-global-registry（闸门键粒度轴）与 wkv-flushall-index-clear-latch-straddle（清表闩毒化轴）均非同案；现码缺陷仍在（cpr_host.rs:439 仍为无条件 store）。
5 架构合规与可执行度：仅改 exit_checkpoint 单函数为 CAS Checkpoint→Rest 条件复位，单套机制无新增承重件、无假桩；方案采「优化后执行方案」SeqCst 口径（与 resize.rs:454-457 状态机槽全序自洽及 try_acquire_txn Dekker 配对一致），精炼段 AcqRel 旧口径被其覆盖不再采；既有 exit 直调测试（wkv/tests/store/resize.rs:535/:596、wcpr checkpoint_slot.rs:41）相位恒为 Checkpoint，CAS 语义下不破坏；wcpr concurrent_ckpt/growing_gate/multi_instance_gate 套件现码存在，验证闭环。

检查点临界区成功路径双重退出，Drop 无条件复位把显式退出后并发进入的 grow PrepareGrow 打回 Rest，事务屏障与切表序失效

审核结论：通过（审查席 zcode-r17-review-dblreset，2026-09-26）

审核亲验记录：
1 双退出形态属实：create.rs:344 成功路径显式 store.exit_checkpoint() 与 create.rs:127-133 CkptPhaseGuard Drop 二次调用均落 cpr_host.rs:435-440 的无条件 store(ResizePhase::Rest)；两次调用间为 info! 宏与两层函数返回的同步代码（无 await），本任务不重入调度，但跨线程（另一 reactor 或后台任务上的 grow_index）窗口真实可达。
2 竞态时序真实可达：显式 exit 置 Rest 后，他线程 grow_index（resize.rs:426-441）CAS Rest→PrepareGrow 成功，Drop 的无条件 store 把 PrepareGrow 打回 Rest 而扩容状态机继续运行。
3 危害链闭合：其一，try_acquire_txn（resize.rs:116-126）与 barrier_enter（resize.rs:163-177）拦截判据均为 phase==PrepareGrow，phase=Rest 时新事务 fetch_add 放行并钉旧表，其注册可晚于 grow 的 1a 计数排空检查、其纪元保护可晚于 1b 的 bump_and_wait 目标纪元，写入落旧表后在 resize.rs:496 切表即丢失（Dekker 配对两臂判据同时失效，屏障保证被破坏）；其二，第二个 grow_index 可再 CAS Rest→PrepareGrow 与第一个并发，resize.rs:484-489 的 split_status/old_index/num_pending_chunks 互相覆盖；其三，下一轮检查点 enter_checkpoint（cpr_host.rs:411-428）CAS Rest→Checkpoint 可成功，且 ensure_not_growing 的 is_growing 判 phase（resize.rs:99-104）在 Rest/Checkpoint 下为 false，索引快照与残留扩容切表并发撕裂 index_meta.size 与 store_meta.index_size。
4 C# 契约对标属实：StateMachineDriver.cs:164-173 Register 以 CompareExchange(ref stateMachine, sm, null) 空槽抢占；:345-362 RunStateMachine finally 内 Interlocked.Exchange(ref stateMachine, null) 单次清槽，每个状态机实例恰清一次自身槽位，清槽者恒为当前持有者，无「完成后再次清槽踢出后来者」形态。rust 的无条件二次 store 正是该契约的移植失真。
5 查重通过：doc/zh/deviations.md 无 exit_checkpoint/CkptPhaseGuard/双重退出相关在册条目；r16-wkv 第二节 2 裁定口径为「guard 存续期（phase=Checkpoint）无竞争面」，本案覆盖其外的「显式退出后、Drop 前」窗口，边界互补非重复。
6 方案最小性成立：仅改 cpr_host.rs exit_checkpoint 单函数为条件复位，失败路径 Drop 兜底（phase 保持 Checkpoint，CAS 成功复位）与成功路径显式退出（CAS 成功）均不受破坏，grow 已进入 PrepareGrow/InProgressGrow 时 no-op 保留扩容，正常退出路径无损。

优化后执行方案（供 task/fix.md 直接消费）：
1 cpr_host.rs exit_checkpoint 改条件复位：compare_exchange(ResizePhase::Checkpoint as u8, ResizePhase::Rest as u8, SeqCst, SeqCst)，仅当槽位仍为本检查点所置 Checkpoint 相位时复位，对 Rest/PrepareGrow/InProgressGrow 一律 no-op，语义对齐 C#「清槽者只清自身持有的槽」。内存序统一 SeqCst：对齐 resize.rs:455-457「本函数内相位字读写一律 SeqCst 全序」的码内自洽口径，消除与 grow_index 相位 CAS 的推理分叉。显式退出与 Drop 兜底双调在该形态下均安全幂等，create.rs 成功路径的显式退出与 Drop 分工保留不变。
2 同步订正两处注释：cpr_host.rs exit_checkpoint 的「复位点不可能与他方转移竞争」补注「显式退出后的二次 Drop 复位靠条件 CAS 防覆盖并发 grow」；create.rs CkptPhaseGuard 注释补「grow_index 不走检查点闸门，无条件 store 会把显式退出后进入的 PrepareGrow 打回 Rest」。
3 测试验证点：wkv 增加竞态用例（enter_checkpoint 成功后显式 exit 复位 Rest，随即手工 CAS 进入 PrepareGrow 模拟并发 grow 抢占，再触发 Drop 路径 exit，断言 phase 保持 PrepareGrow 未被打回；以及 Drop 路径 phase 仍为 Checkpoint 时正常复位两臂）；wcpr 既有 concurrent_ckpt / growing_gate / multi_instance_gate 套件回归；按 rust_review 规范跑 ./sh/clippy.sh 清警告后 ./test.sh。

问题分析：
1. Garnet 契约对齐。C# 状态机槽位由 StateMachineDriver 唯一持有，RunStateMachine 收尾 finally 块单次清槽（StateMachineDriver.cs:345-362，Interlocked.Exchange(ref stateMachine, null)），每个状态机实例在自身生命周期内恰清一次自身槽位；Register（同文件 :164-173）以 CompareExchange(ref stateMachine, sm, null) 从空槽抢占，槽非空即失败。清槽与抢占共享同一原子字：清槽者永远是槽的当前持有者，后来者只会在槽清空后进入，不存在「状态机完成后再次清槽把后来者踢出」的形态。
2. 工程现状确证。rust 侧检查点临界区退出存在两次无条件复位：成功路径在 create_checkpoint_inner 尾段显式调用 store.exit_checkpoint()（wedb/wcpr/src/manager/create.rs:344），随后函数返回时 CkptPhaseGuard::drop（create.rs:127-133）再次执行 exit_checkpoint；宿主实现为无条件 store(ResizePhase::Rest)（wedb/wkv/src/store/cpr_host.rs:435-440）。两次退出之间的同步窗口（create.rs:346-349 的 info! 日志与两层函数返回）内，并发 grow_index 的 Rest→PrepareGrow CAS（wedb/wkv/src/store/resize.rs:426-441）可以成功进入扩容状态机，随后的 Drop 复位把 PrepareGrow 无条件打回 Rest。码内注释（cpr_host.rs:430-433「复位点不可能与他方转移竞争」、create.rs:121-126「检查点闸门串行化下无并发检查点，复位互不踩踏」）只论证了并发检查点被进程级闸门串行化与临界期内 PrepareGrow CAS 恒失败（r16-wkv 第二节 2 同口径裁定亦仅覆盖 guard 存续期），均未覆盖「显式退出后、Drop 前的二次复位窗口」——扩容不走检查点闸门。
3. 逻辑危害确证。phase 被打回 Rest 后扩容状态机仍在运行，三重失效面：其一，事务屏障失守，try_acquire_txn（resize.rs:116-126）与 barrier_enter（resize.rs:163-177）均以 phase == PrepareGrow 为拦截判据，phase=Rest 期间新事务与新会话放行注册并钉住旧表，跨越 grow_index 切表边界（resize.rs:496 的 index.store）后其写入落入旧表丢失，正是「先切表后发相位」两写序列（resize.rs:491-500 注释自陈的屏障保证）被破坏的形态；其二，第二个 grow_index 可再次 CAS Rest→PrepareGrow 成功，与第一个扩容并发，split_status 与 old_index 互相覆盖；其三，下一轮检查点 enter_checkpoint 可 CAS Rest→Checkpoint 成功，索引快照与残留扩容的切表并发，index_meta.size 与 store_meta.index_size 撕裂。窗口为 info! 日志执行的微秒级，后台扩容任务（GrowIndexIfNeededAsync 对位）与周期检查点长跑并存时可达，后果为静默写丢失或索引撕裂。

涉及代码：
rust 文件与函数：
wedb/wcpr/src/manager/create.rs:CkptPhaseGuard（Drop 复位）与 create_checkpoint_inner（:344 显式退出）与 create_gated（守卫作用域）
wedb/wkv/src/store/cpr_host.rs:WedbStore::enter_checkpoint（:411-428 CAS Rest→Checkpoint）与 WedbStore::exit_checkpoint（:435-440 无条件 store Rest）
wedb/wkv/src/store/resize.rs:WedbStore::grow_index（:426-441 CAS 入口）与 IndexResizeState::try_acquire_txn（:116-126 复查判据）与 WedbStore::barrier_enter（:163-177 拦截判据）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs:RunStateMachine（:345-362 单次清槽）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs:Register（:164-173 空槽 CAS 抢占）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs:AcquireTransactionVersion（:93-117 屏障判据与槽位状态一致性由单次清槽保证）

精炼执行方案：
1 exit_checkpoint 改条件复位：compare_exchange(ResizePhase::Checkpoint as u8, ResizePhase::Rest as u8, AcqRel, Acquire)，仅当槽位仍为本检查点所置 Checkpoint 相位时复位，对 Rest / PrepareGrow / InProgressGrow 一律 no-op，语义对齐 C#「清槽者只清自身持有的槽」；显式退出与 Drop 兜底双调在该形态下均安全幂等，create.rs 成功路径的显式退出与 Drop 分工保留不变。
2 同步订正两处注释：cpr_host.rs exit_checkpoint 的「复位点不可能与他方转移竞争」补注「显式退出后的二次 Drop 复位靠条件 CAS 防覆盖并发 grow」；create.rs CkptPhaseGuard 注释补「grow_index 不走检查点闸门，无条件 store 会把显式退出后进入的 PrepareGrow 打回 Rest」。
3 测试验证点：wkv 增加竞态用例（enter_checkpoint 成功后显式 exit 复位 Rest，随即手工 CAS 进入 PrepareGrow 模拟并发 grow 抢占，再触发 Drop 路径 exit，断言 phase 保持 PrepareGrow 未被打回；以及 Drop 路径 phase 仍为 Checkpoint 时正常复位两臂）；wcpr 既有 concurrent_ckpt / growing_gate / multi_instance_gate 套件回归。
合入哈希：874f8ee 收口形态：exit_checkpoint 改仅清自身所置 Checkpoint 相位的条件 CAS（SeqCst，对标 C# StateMachineDriver 单次清槽契约），显式退出与 Drop 兜底双调幂等，并发 grow 抢占 PrepareGrow/InProgressGrow 不再被打回 Rest，wkv resize 补双重退出竞态两臂回归
