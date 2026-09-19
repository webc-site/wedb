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
