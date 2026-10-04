终态：已合入 dev（2026-09-27）。6472191 help_drain(prior_epoch) 定向刷新:TLS轨无条件∨Participant轨仅新鲜钉,自旋臂与收尾同罩;participant_pinned_below 原语+方向B五处断言;双新测试

甄别结论：通过 | 定级 P2 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：条件化谓词须同罩 epoch.rs:531 自旋臂与 :535（c01d 后行号下移约40行，按内容锚寻）

问题分析：
1 Garnet 契约对齐：C# 受保护线程就地 Bump 不刷新本线程保护条目（garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/LightEpoch.cs，Acquire :513-529 尾部仅 Drain、ProtectAndDrain :296-316 显式发起才刷新并文档明示 Drops protection for the old epoch），刷新与获取路径分界明确。
2 工程现状确证：wepoch claim_entry 改 drain_if_pending（票 wepoch-claim-entry-help-drain-lifts-participant-guard，合入 604eb5e）后，获取路径已统一「只 drain 不可 refresh」；但 bump_current_epoch_action 收尾（wepoch/src/epoch.rs:535）保留的 help_drain 为无条件全量刷新本线程条目——rust_review 审查席（c01d 票审查）上报既有残留面：whlog shift 链（shift_read_only_address / shift_head_address / wait_safe_read_only_drained）与 wbftree 管理面（dispose_bf_tree_deferred / release_detached）的调用线程若持跨 await Participant 守卫触达，守卫即被抬，同 c01d 票受害机理（resize.rs:581-592 barrier_enter 守卫横跨 async run_recovery_kernel 同型）。现可达性排查：resize 受害临界区内不触达 shift 链与 bump（审查席亲验）；wkv read_cache pump_close_barrier 调用点已按契约显式适配（append.rs:210-215）；故现产无活跃受害路径，属防御性收窄面。
3 逻辑危害确证：未来任何「持跨 await Participant 守卫的调用栈触达 shift 链/bump」新接线即复现 c01d 同款抬守卫丢键面，且无编译期护栏；契约审计仅散见调用点注释，缺单点机制防线。

涉及代码：
rust 文件与函数：
wepoch/src/epoch.rs:bump_current_epoch_action（:531/:535 收尾 help_drain）、help_drain（:487-497）、refresh_thread_protected_entries（:470-476）
whlog/src/hlog/shift.rs（shift 链三面调用线程契约）、wbftree（dispose_bfree_deferred / release_detached 调用面）、wkv/src/store/append.rs（pump_close_barrier 既有适配先例 :210-215）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/LightEpoch.cs:Acquire（:513-529）/ ProtectAndDrain（:296-316）分界

精炼执行方案：
1 方向 A（机制收窄）：bump_current_epoch_action 收尾 help_drain 改条件化——仅当本线程名下存在钉住 prior_epoch 的条目才刷新（守护纪元与待排水纪元比对，单点判据），对齐 C# 「就地 Bump 不刷新」形；既有 bump 消费面（注册收尾/列表满自旋）语义复核不回退。
2 方向 B（契约审计钉子）：whlog shift 链与 wbftree 管理面调用点补「本线程不得持跨 await Participant 守卫触达」契约注释与 debug 断言（wepoch 开查询原语），机制面不动。
3 两方向可并取（A 治本 B 留痕）；严禁反向放宽获取路径口径。
4 测试验证点：同线程 Participant 守卫钉旧纪元下触达 bump_current_epoch_action，断言守卫公布纪元不被抬（旧形即红）；bump 排水活性回归（pending 动作照常收割）；c01d 票两案回归不回退。
