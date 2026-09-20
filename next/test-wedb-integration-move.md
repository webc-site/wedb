# wedb src 内真实 socket / AOF 集成测试平移至 tests/

来源：next/zcode-r2-test.md 划分 6。

## 问题
`wedb/wedb/src/server/replication/replica_wire.rs` 内的 `tcp_wire_*` 测试起真实 TcpListener + GarnetClientSession 连接；`aof_sync_task.rs` 内的 8 个测试构造真实 AOF 目录，属于跨组件集成测试。
代码规范与 SKILL 要求集成测试放置于 crate 的 tests/ 目录，保证 src 纯净并加速单元测试编译。

## 目标
1. 将 `replica_wire.rs` 中的真实网络 socket 集成测试移动至 `wedb/tests/`；
2. 将 `aof_sync_task.rs` 中的磁盘 AOF 目录集成用例平移至 `wedb/tests/`；
3. 保留原 src 内的纯内存单元测试。

## 验收
1. cargo check -p wedb --tests 0 error 0 warning。
2. 平移后的测试正常运行通过。
