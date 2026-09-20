# HELLO 附带 AUTH 认证成功后冷上下文装载挂起检查

来源：next/zcode.my.md 问题四

## 问题

客户端通过 HELLO 附带 AUTH 认证冷租户或冷库时，apply_authenticated_handle 将冷上下文记入 self.cold_ctx。
process_hello_command_state 未检查并处理该挂起上下文，
可能导致 cold_ctx 悬挂或会话未切入正确物理域。

## 涉及路径

- wedb/wnode/src/resp/resp_server_session/auth.rs
- wedb/wnode/src/resp/basic_commands/mod.rs

## 解决建议

1. 在 HELLO 认证返回成功前，检查 self.cold_ctx 是否存在挂起上下文。
2. 若存在挂起冷上下文，触发装载与切换流程，确保会话上下文与认证用户一致。
3. 补充 HELLO 认证冷租户用例测试。
