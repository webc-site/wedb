裁决：主体不成立（三级结构与 C# 同构、投影单点已注明；「wmetric 统一定义全部快照结构」制造跨层耦合）
来源：next/agy.design.md 条 16 + next/muse.design.md 条 9（两轮同题）。核销 2026-09-19。
残留一条真实增量（锚点复挂）已拆出由 next/design-anchor-remount-batch.md 第 5 组承接。

一句话结论：wkv StoreSnapshot（存储原始域 u64）→ wnode 投影 → wmetric DbSnapshot（INFO 展示域
i64+String）与 C#「StoreWrapper.GetDatabasesSnapshot 出快照、GarnetInfoMetrics 消费」同构；
project_db_snapshot / project_aof_snapshot 是注释自明的全仓唯一组装点，字段同名是快照逐级
投影的必然，不是三套并行实现。

逐条核销
1. C# 对标原貌：garnet/libs/server/StoreWrapper.cs:GetDatabasesSnapshot 产出快照结构，
   garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetDatabaseStoreStats /
   GetDatabasePersistenceStats 消费渲染——C# 即「快照结构 + 统计函数」两层；
   rust 三级多出的 wnode 投影是因为 wmetric 不依赖 wkv（C# 单库无此问题），投影即解耦代价，
   且已单点化（mod.rs:203 doc 自述「wmetric DbSnapshot 的全仓唯一组装点」）。
2. 结构体域差异真实存在：StoreSnapshot（wkv/src/store/stats.rs:45，u64 地址域，16 字段）
   vs DbSnapshot（wmetric 定义，i64 + String system_state + 展示口径派生字段如
   index_memory_size_bytes / log_heap_size_bytes）——后者是 INFO 输出列的直接映射，合并两结构
   要么 wmetric 依赖 wkv 内部件、要么 wkv 定义展示字段，两向都破坏分层。
3. 修法评估：「快照结构由 wmetric 统一定义」若含 StoreSnapshot 上移，wkv 将反向依赖 wmetric
   的展示口径，层级倒置；「字段大面积同名」是投影保名的正常形态（C# DatabaseConfigStoreState
   与 INFO 列同名同理），非冗余镜像。
4. 真实残留（唯一成立点）：project_db_snapshot 与 get_database_store_stats 同挂
   GarnetInfoMetrics.cs:GetDatabaseStoreStats、project_aof_snapshot 与
   get_database_persistence_stats 同挂 GetDatabasePersistenceStats，构成 check.js 复挂——
   已并入 next/design-anchor-remount-batch.md 第 5 组（只改注释），本票不重复立项。
