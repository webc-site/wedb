# runtxp-slot-verify-replay-hang 拒绝存档

来源：/Users/z/git/db/wedb/next/runtxp-slot-verify-replay-hang.md（AI 生成单问题票，本棒已删）
取证：主仓 dev HEAD e75716e，工作区干净，CARGO_TARGET_DIR=/tmp/target-fix-runtxp-slot-verify-hang
裁决：不成立（当前代码无待做项）。票面主张的挂死为真，但真因已在 HEAD 落地的
0f7ec6a 中按票面要求的方式（删掉回放期多出的那条「不等 Commit」分支，而非给测件加超时）
修掉了。本棒不建分支、不改码，只删票归档。

## 逐条裁决

1 主张「wtxn::runtxp_slot_verify replay_path_skips_verification 确定性挂死、180s 不返回」
当前代码不再成立。实跑票面验收 1 的命令：
`cargo nextest run -p wtxn --test runtxp_slot_verify --no-fail-fast`
=> 6 passed，其中 replay_path_skips_verification PASS [0.009s]，已是票面要求的毫秒级。
另用单测二进制以 --test-threads=1 与默认并发两种跑法各跑一遍，均 6/6 通过，零挂起。

2 主张的取证基线不可核：票面写「基线 commit 0b08d80b 与 d54ce713」，两对象在主仓均不存在
（`git cat-file -t` 均 Not a valid object name；`git log --all` 全量仅 10 个提交，
历史被反复压缩重置）。故本票的落地判定不靠 sha，只按当前代码事实 grep 与实跑。

3 真因判读成立且已修（对应票面排查方向 2）：
旧码 run_transaction_proc 主段后是
`if is_replaying { true } else { log_proc(..).is_ok() && commit(false).is_ok() }`
（0f7ec6a 之前的 /Users/z/git/db/wedb/wedb/wtxn/src/transaction_manager.rs），
回放期把 Commit 整体跳过；而 ran 为 true 使尾部 `if !ran { self.reset(); }` 也不触发，
于是本笔事务钉定的桶独占闩永不释放（windex 桶内嵌闩第 63 位，
/Users/z/git/db/wedb/wedb/windex/src/bucket.rs:113 `try_lock_exclusive` 先判
`curr & EXCLUSIVE_LATCH_MASK == 0`，闩不可重入）。
测件在同一个 manager 上连跑两次
（/Users/z/git/db/wedb/wedb/wtxn/tests/runtxp_slot_verify.rs:273 与 :277），
第二次 prepare 再登记同键 b"a" 后走 `run` -> `lock_all_keys`
（/Users/z/git/db/wedb/wedb/wtxn/src/txn_key_entry.rs:203），
其 `while !self.acquire_plan(&plan) { thread::yield_now(); }` 无超时、无通知可等，
本线程自持的闩恒取不到 => 永不返回。这与票面「两轮 180.005s / 180.006s 毫秒级一致、
与机器负载无关」的现象完全吻合。
现码（transaction_manager.rs:680-685）把回放短路收窄为「仅跳过 AOF 落盘」，
Commit 必跑：commit -> reset -> key_entries.unlock_all_keys
（transaction_manager.rs:418-432、:336-347；txn_key_entry.rs:250-260 逆序放闩），
对标 C# /Users/z/git/db/wedb/garnet/libs/server/Transaction/TransactionManager.cs:341
`Commit()` 无条件执行。即票面要求的「删掉本仓多出的那条机制」已经这样做掉了，无第二处可删。
生产侧后果亦已核实为真（非仅测件问题）：AOF 回放为新事务管理器共用引擎实例锁表
（/Users/z/git/db/wedb/wedb/wnode/src/aof/replaycoordinator/stored_proc_replay.rs:94
`TransactionManager::new(self.lock_table.clone(), ..)`，锁源与 wkv TTL 读改写同份内存），
漏放的桶闩会卡住后续任意落同桶的在线事务，确属生产级死等。

4 主张「若结论是测件夹具写错」（排查方向 3）不成立：RecordingVerifier 的三个切面方法
只在自身 Mutex 内做计数与向量 push，回调里不再取第二把锁、无 Drop 依赖；
夹具形态与生产实现同构（/Users/z/git/db/wedb/wedb/wnode/src/resp/txn_resp_commands.rs:53
`impl TxnSlotVerifyFace for IterativeSlotVerifyAdapter`，纯同步委托 ClusterSession，
无等待边），生产回放侧则按「调用侧 None 闸」传 None
（stored_proc_replay.rs:111 `run_transaction_proc(.., true, None, ..)`），
与 C# /Users/z/git/db/wedb/garnet/libs/server/Transaction/TxnKeyManager.cs:48
`if (!clusterEnabled || IsReplaying) return;` 的早退口径一致。夹具无需改动。

5 「无守卫自旋是否属本仓多出的等待边」——不删：C# `TxnKeyEntries.LockAllKeys`
（/Users/z/git/db/wedb/garnet/libs/server/Transaction/TxnKeyEntry.cs:106）走
`TransactionalContext.Lock` 阻塞取锁、无超时，rust 的无限重试与之 1:1。
票面所指等待边的正解是「回放期必须释放锁」（已落地）；若改而为自旋加超时或给测件加
timeout，反倒引入 C# 没有的复杂度，按 transpile 需求不予采纳。
回归护栏即该测件本身（同一 manager 连跑两次），跳过 Commit 的分支若复辟必再次挂死，
无需新增用例。

6 相邻红四条（wnode::tls_test test_garnet_server_tls_mtls_* 与
wnode::vector_set_production_switch vector_set_preview_*）：越界不改。本票改动域
wedb/wtxn/**，票面另禁碰 wnode/src/service.rs、wconf、wnode/src/resp/objects；
next/ 下已另有 tls-mtls-client-cert-required-not-enforced.md、
vector-registry-user-key-strip-single-point.md 等票覆盖该域，留对应棒处理。

7 验收复核（虽无代码改动，仍按票面验收跑）：
验收 1 见第 1 条，全绿且毫秒级。
验收 2 `cargo check --all-targets -p wtxn -p wnode` 退出 0、零 warning、零 error，
未新增 `#[allow(`。
验收 3 无改动，故不涉及「删边后由何机制守不变量」；补充说明：该不变量（事务持有的桶闩
必随 Commit/reset 释放）现由 commit -> reset -> unlock_all_keys 单链承接，
外加 `Drop for TransactionManager`（transaction_manager.rs:251-255）与
`Drop for TxnKeyEntries`（txn_key_entry.rs:286-291）兜异常路径，与 C#
`Reset(true)` + using/finally 对偶，既有断言面无删改。

## 处置

- 删除 next/runtxp-slot-verify-replay-hang.md（原文见下）。
- 未建 worktree/分支，未改任何代码与测试，主仓仅提交本档与删票。

## 票面原文（逐字存档）

```text
优先级：功能缺口之首（死等——回放路径在纯内存单测里 180s 不返回，即生产代码级死锁）

单问题：`wtxn::runtxp_slot_verify replay_path_skips_verification` 确定性挂死（不是慢，是永不返回）。

取证现状（2026-09-19 主代理两轮干净快照基线实跑，日志已随并发清理丢失，结论如下）
- 两轮全量 nextest（基线 commit 0b08d80b 与 d54ce713）该用例均以
  `TIMEOUT [180.005s]` / `TIMEOUT [180.006s]` 结束，两次耗时到毫秒级一致 ⇒ 确定性挂死，
  与机器负载无关。它是全仓当前唯一非零退出项之一（另见下「相邻红」）。
- 测件位置：/Users/z/git/db/wedb/wedb/wtxn/tests/runtxp_slot_verify.rs:261
  `fn replay_path_skips_verification()`。两次调用同一入口
  `run_transaction_proc`（:273 带 `Some(&handle)` 切面、:277 带 `None`），
  夹具全为同步内存件：`manager()`、`RecordingVerifier`（内部 `std::sync::Mutex`，
  经 `.lock()` 无超时读取）、`KeyRegisteringProc`（注册 `("a", LockType::Exclusive)`）、
  `NoopView`。挂死点在这三者与生产路径的交互上。
- 测件自述意图（:269-271 注释）：回放要走「调用侧 None 闸 + IsReplaying 闸」双闸，
  带切面回放时 `reset` 缓存照常一次、`add_key` 内 `IsReplaying` 短路，denied 键不触校验仍放行。
  即该路径设计上不应阻塞。

排查方向（按证据判定，不要凭猜改）
1 先取挂死现场：`CARGO_TARGET_DIR=<私有> cargo test -p wtxn --test runtxp_slot_verify
  replay_path_skips_verification --no-fail-fast` 跑起来后用 lldb `attach` 或
  `sample`/`stackshot` 抓挂死线程栈，给出「哪两把锁互等 / 哪个 condvar 永等」的帧证据；
  亦可临时插桩打印到 stderr 定位最后进入的函数（临时件收尾删净、不提交）。
2 高度可疑但须自证：同键既被 `SlotVerifyHandle` 校验、又被 `KeyRegisteringProc` 以
  `LockType::Exclusive` 注册，若回放分支在持锁状态下再取同一锁（或不死锁但等待一个
  永不到来的通知），即成本测试挂死。判据要与 C# 对齐：
  /Users/z/git/db/wedb/garnet/libs/server/Transaction/TransactionManager.cs 的
  `RunTransaction`（及 `IsReplaying` 早退臂）在同形态下如何解锁——C# 侧不存在
  「回放期等待校验回调」这条边，若本仓多出一条等待边，即为转写多出的机制，按
  「不兼容保留、直接删」处理该等待边本身，而不是给测件加超时。
3 若结论是测件夹具写错（例如夹具自身在 `verify` 回调里再取锁、或 `Drop` 未释放），
  要指出该夹具形态在生产里有无对应调用者（grep 生产侧 SlotVerifyHandle 实现），
  只有确认生产无此形态才允许改测件，且改后仍须验证生产回放路径行为不变。

相邻红（同一波要一并核，别单开代理）
`wnode::tls_test test_garnet_server_tls_mtls_{permissive_without_issuer,client_cert_required}`
与 `wnode::vector_set_production_switch vector_set_preview_{enabled_end_to_end,disabled_by_default}`
四条为 SIGABRT（0.07-0.72s 即中止，非断言失败），证据在下一棒 brief 附的实测输出。
与本票改动域不同（wresp/wacl/wvector 面），若你判定越界请勿顺手改。

改动域：wedb/wtxn/**（必要时 wnode 侧的 SlotVerifyHandle 生产实现）。
禁止碰 wedb/wnode/src/service.rs、wedb/wconf/**、wedb/wnode/src/resp/objects/**（并发会话在跑）。

验收
1 `cargo nextest run -p wtxn --test runtxp_slot_verify --no-fail-fast` 全绿，且该用例耗时回到毫秒级。
2 `cargo check --all-targets -p wtxn -p wnode` 零错零警告，禁 `#[allow(`。
3 若动的是生产等待边：给出该边原本要守的不变量、删/改后由什么机制继续守住（对齐 C# 同处），
  并在回报里点名受影响的既有断言。
```
