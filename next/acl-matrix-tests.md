任务名称: acl-matrix-tests

问题描述
当前针对逐命令 ACL 授权矩阵缺少系统性断言测试。
C# Garnet 具有 RespCommandTests.cs 全量命令矩阵测试。
Rust 侧需补充代表性命令测试与全目录展开等价性断言，覆盖自增命令 SUNSUBSCRIBE、RI.COUNT 与 CLUSTER FLUSHALL_NS。

实现规划
1. 在 wedb/wnode/tests/acl_tests.rs 中补充 ACL 类别展开与命令授权断言。
2. 断言全目录命令均可被 +@all 放行、-@all 拒绝。
3. 验证三条自增命令的类别与权限行为。
4. 运行 cargo check 确保编译通过。
5. 审查优化代码。
