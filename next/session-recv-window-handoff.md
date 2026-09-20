会话 recv 分域签名拆分（挂起续做，非新审查票，勿按票面甄别流程处理）

原票 next/fix-session-arg-borrow-split.md 已被认领删除，实现工作留在分支
fix-session-arg-borrow-split（worktree /tmp/fork/fix-session-arg-borrow-split）。
该票连续六棒折损（服务中断一次、预算耗尽两次），第六棒退出时正在修最后两个测试
文件的签名随动，本主代理已把它的未提交改动固化为提交 0abbf44，现场完整可续。

规模：分支相对 dev 六十九个文件，净 +1048/−676；其中 src 侧二十文件净
+448/−374（核心是 parser/resp_command.rs、resp_server_session/core.rs、
parse.rs、garnet_api/mod.rs 与 recv 窗口参数化），测试随动四十八文件
+429/−302。

当前真实状态（主代理实测，2026-09-21，CARGO_TARGET_DIR 私有）：
cargo check -p wnode --all-targets 剩 12 条 error 行，全部集中在
wedb/wnode/tests/resp_server_session_tests.rs（约九处同类，均为 recv 窗口参数
签名随动未改完），src 侧与其余测试文件已编译通过。也就是说剩余工作是单文件收尾，
不是重构本身。

为什么挂起而不是立刻续做：这二十个 src 文件与当时在途的三票改动面直接相交，
先合会互相制造冲突。续做前必须确认以下三票已落地并同步过 dev：
lua-txn-mode-drop-placeholder（resp_server_session/{attach,core,lua,txn}.rs）、
r4client-registry-live-view（resp/client_commands.rs、servers/consumer_registry.rs）、
wtxn-drop-dead-txn-proc-chain（resp/garnet_api/mod.rs、resp/txn_resp_commands.rs、
storage/session/txn_proc_view.rs）。三票任一未落地就续做，等于把冲突前移。

续做棒的硬性要求：第一动作是 git merge dev 后跑
CARGO_TARGET_DIR 私有 cargo check -p wnode --all-targets，以报错清单为准逐个收口，
每个可编译检查点立即分段提交，禁止攒改动（本票已两次因此丢过现场）。收口完成后
必须回头判定这票的收益是否成立：recv 分域是否真的消除了 C# 里存在的坏味道，
还是 rust 为绕开借用检查自造了一层 RecvWindow 抽象（分支含 72c9d6e 骨架提交）。
若判定为自造复杂度，整票拒绝并回退分支，宁可不并也不把比 C# 更绕的形态合进主目录。
验收口径：cargo check --workspace 与 -p wnode -p wtxn -p wtxn_test --all-targets
零错误；受影响定向 nextest（不带 --all-features）通过；按 .agents/skills/rust_review
自审，只允许一套 recv 参数传递机制，旧签名调用点不得以兼容壳保留。
