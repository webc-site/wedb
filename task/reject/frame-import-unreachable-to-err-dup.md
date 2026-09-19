重复：task/reject/glm.design.md 第 1 条（该面已被分拣拒绝）（关键符号 frame_import.rs:174-176/unreachable/兜底拒绝面 命中）
优先级：高

迁移帧导入收尾臂 unreachable!() 与注释「兜底拒绝面」声明错位，panic 直达网络泵线程无隔离
  frame_import.rs:174-176 帧类型分派收尾臂对 RangeIndexStream/VectorSetIndex/VectorSetElement 三变体写 unreachable!()，当前由上游 :95-106 与 :128-156 两段短路保证暂不可达；但 :179-181 紧邻注释自称「此为帧内兜底拒绝面，绝不静默当 string 写入……C# SYNC 面意外 kind 本就抛」——「兜底拒绝」语义是返回 Err，unreachable!() 是 panic，实现与自身声明错位。该函数执行链为 cluster_session/migrate.rs:253-264 SlowWait::new 挂 cluster_migrate_slow，由网络泵 worker take_slow_wait 后直接 await（slow_path.rs 模块头「网络泵 take_slow_wait 后 await」），全仓无 catch_unwind 包裹该链（仅 vector_manager_cleanup.rs:169、wbftree service/ops.rs 两处局部使用）：上游门控（本文件 :128-156 的 matches! 宏）一旦在重构中漏一个变体，对端一帧即 panic 撕整个 compio worker 线程上的全部连接；C# 对位同位置是 try 块内可捕获 throw，失败面收敛到本迁移会话断链。修法：三臂改 return Err（文案对齐 C# Unexpected kind 形态），与注释「兜底拒绝面」声明及 C# 行为一致。
  rust：wedb/wedb/src/server/migration/frame_import.rs:174-176（unreachable! 臂）、:95-106/:128-156（上游短路）、:179-181（「兜底拒绝面」注释）；执行链 wedb/wedb/src/server/cluster_session/migrate.rs:253-264 + wedb/wnode/src/resp/slow_path.rs（网络泵 await，无 panic 隔离）
  c#：garnet/libs/cluster/Session/RespClusterMigrateCommands.cs:135、:303 throw new InvalidOperationException($"Unexpected MigrationRecordSpanType: {kind}")（:120 try 块内可捕获）
