# wconn 客户端行为面测试补充（空数组应答与并发 PING）

来源：next/zcode-r2-test.md 缺口 5。

## 问题
C# 客户端测试 GarnetClientTests.cs 覆盖了客户端行为特性：
1. `ShouldNotThrowExceptionForEmptyArrayResponseAsync`：收到空数组 RESP 应答时不抛异常、正确解析为 Empty Vec；
2. `MultipleSocketPing`：多并发 socket 连续 PING 应答稳定。

## 目标
在 `wedb/wconn/tests/` 补充测试用例：
1. 验证 `GarnetClient` 对空数组 RESP (`*0\r\n`) 能够无异常正常解析并返回空结果；
2. 验证多连接并发 PING 的鲁棒性。

## 验收
1. cargo check -p wconn --tests 0 error 0 warning。
2. wedb/wconn/tests/ 相关单测通过。
