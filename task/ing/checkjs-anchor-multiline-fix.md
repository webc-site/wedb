# checkjs 锚点单行化与路径修正方案

## 任务背景
在 check.js 门禁校验中，因文档注释断行以及 C# 局部类与分部文件的路径偏差，存在 AST 校验匹配缺失与伪缺失报出。需要对标 C# Garnet 源码与 check.js 需求进行修正。

## 甄别分析
1. wedb/wedb/src/server/replication/replica_sync_session.rs:114-115 的 SendCheckpointAsync 锚点注释断成两行。check.js 的正则与行级扫描无法将其识别为有效锚点，导致该方法被作为未文档化方法计入 miss 台账，并被降级为仅词元提及。将其合并为单行后即可恢复正确匹配。
2. wedb/wnode/src/resp/resp_server_session/lua.rs:162 的 CheckScriptPermissions 注释写成了 libs/server/Resp/RespServerSession.cs:CheckScriptPermissions。经核对 C# Garnet 代码，CheckScriptPermissions 方法真实定义在 libs/server/Resp/AdminCommands.cs:95，RespServerSession.cs:653 仅为调用点。将其修正为 libs/server/Resp/AdminCommands.cs:CheckScriptPermissions 可正确核销 AdminCommands.cs 的缺失记录。
3. 对照 libs/server/Resp/AdminCommands.cs 的缺失项，ProcessAdminCommands 在 wedb/wnode/src/resp/admin_commands.rs 中由 process_admin_session_commands 直接承接分臂分派，补充对应锚点；CommitAofAsync 为 NetworkCOMMITAOF 的内部异步提交辅助函数，rust 统一走 route_slow_command 转挂慢路径 garnet_api.commit_aof 闭环，需在 js/check/ignore/server.yml 中补充合理差异 ignore 规则，确保 check.js 保持 0 误报。

## 执行步骤
1. 运行 ./fork.sh checkjs-anchor-multiline 创建工作区 /tmp/fork/checkjs-anchor-multiline 并切换分支。
2. 在工作区中合并 replica_sync_session.rs 的 SendCheckpointAsync 锚点注释为单行。
3. 修正 lua.rs 中 CheckScriptPermissions 锚点为 libs/server/Resp/AdminCommands.cs:CheckScriptPermissions。
4. 在 admin_commands.rs 中为 process_admin_session_commands 补充 libs/server/Resp/AdminCommands.cs:ProcessAdminCommands 锚点。
5. 在 js/check/ignore/server.yml 补充 CommitAofAsync 的合理差异 ignore 规则。
6. 运行 bun ./js/check.js 验证 miss 台账与门禁校验，运行 cargo check 确保编译通过。
7. 提交分支修改，变基合并 dev 最新提交，按照 rust_review 规范核查代码。
8. 合并分支回 dev，清理 /tmp/fork/checkjs-anchor-multiline 及分支，将方案移动至 task/done/。
