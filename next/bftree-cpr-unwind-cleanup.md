# wbftree CPR 快照移除 catch_unwind 强化前置校验

来源：next/zcode.db.md 问题 11

## 问题

BfTreeService::cpr_snapshot 使用 panic::catch_unwind 尝试捕获底层快照异常。
但在 panic = "abort" 构建配置下 catch_unwind 失效，底层 panic 仍会直接中止进程。

## 涉及路径

- wedb/wbftree/src/service/snapshot.rs

## 解决建议

1. 移除无效的 catch_unwind 封装，改为严格的前置状态与参数校验。
2. 确保底层调用不会发生无保护 panic，返回明确的 Result 错误。
