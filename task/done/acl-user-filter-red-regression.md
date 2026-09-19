dev 基线红：wacl user.rs 重写后 acl_limited_user_filters_commands 取到 Get 被放行

来源：d-sessmetrics 棒的越界发现（该票已落地 435bec6c/3ceb75bb）。它跑
/Users/z/git/db/wedb/wedb/wnode/tests/resp_server_session_tests.rs:782
`acl_limited_user_filters_commands` 时用 dev 原文件 A/B 单跑仍红，肇因指向并发提交
52f03c98 对 /Users/z/git/db/wedb/wedb/wacl/src/user.rs 的整段重写（约 526 行）。
取证基线：主仓 /Users/z/git/db/wedb，分支 dev，HEAD 6e016b4f。

结论

受限 ACL 用户的命令过滤面在 user.rs 重写后语义漂移：用例断言
`check_acl_permissions(Get)` 在非授权用户上应为拒（假），当下返回真，即权限判定
把未授权的 GET 当放行。这是产线权限面的实际缺口，不是用例过时，属最高优先级
（任何带 ACL 的路径都会少过滤）。

要求

1. 先跑该用例复现，再用 git bisect 或直接对 52f03c98^ 与 dev 做 A/B，确认漂移点。
2. 对照 C# 的 ACL 权限判定链（/Users/z/git/db/wedb/garnet/libs/server/access_control 系，
   按 user.rs 注释锚点里的 .cs 文件与函数名定位），把 rust 侧判定改回单点口径。
3. 若 user.rs 重写同时留了新旧两套判定入口，删旧套，不许并存；不做向下兼容。
4. 只放这 1 条红（同族还有 next/wacl-user-local-concurrent-primitives.md 的并发原语面，
   不在本票范围），每收一条红立刻 pathspec commit。

门禁：子代理只跑 `cargo check --workspace --all-targets`（在 worktree 的 wedb 那一层，
exit 0 且 0 warning）与 worktree 内 `bun js/check.js`（报告前后逐字节相同），
外加 `cargo test -p wnode --test resp_server_session_tests acl_limited_user_filters_commands`
定向转绿。禁 ./test.sh、禁 ./sh/clippy.sh、禁 git stash（本仓 refs/stash 跨 worktree 共享）。
