任务名称: checkjs-anchor-multiline-fix

问题描述
1. wedb/wedb/src/server/replication/replica_sync_session.rs:114-115 中 SendCheckpointAsync 锚点注释断行，导致 AST 校验匹配缺失。
2. 修正 CheckScriptPermissions 锚点路径。
3. 为合理差异项补充 ignore 配置，确保 check.js 保持 0 误报。

实现规划
1. 合并 SendCheckpointAsync 注释为单行。
2. 校验 bun ./js/check.js 退出码为 0。
3. 审查优化代码。
