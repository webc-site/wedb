拒绝原因：混编集群不支持是已确定的项目边界

SKILL 明文规定：编码用 bitcode、不与 C# 格式兼容、无向下兼容与旧版迁移负担。
代码已多处刻意差异（集群配置 v2、MIGRATE 头改形、库级定槽废除键哈希、nodeid u128、FLUSHALL_NS 自增命令）。

该文件的 3 条文档声明建议（在 doc/zh/db.md 加集中声明、补 epoch 碰撞注释、补 SyncMetadata 注释）属于文档改善，
不涉及代码逻辑修改，且混编不可达，优先级极低。如需后续补文档，可单独处理。

原文件：next/zcode-r12-hetero.md
