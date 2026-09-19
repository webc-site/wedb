优先级：低

问题
gate-anchor-drift-reclean 票的唯一残留收尾：C# PrimarySync 族的 js/check/ignore
登记仍缺位，门禁（check.js）是否复绿未证。其余该票判据均已落地（虚构锚点
HashObjectImpl.cs:Set 已订正为 HashSet，19 族甄别大部已登记），本票只收尾。

取证（dev e75716e，按当下代码）
- js/check/ignore/ 全域 grep PrimarySync 零命中；
  js/check/ignore/garnet/libs/cluster/Server/Replication/ 下仅 ReplicationNetworkBufferSettings.yml
  与 PrimaryOps 目录，无 PrimarySync 子目录；
- 对应 C# 族在 garnet/libs/cluster/Server/Replication/PrimarySync/PrimaryOps/
  （整族文件），rust 侧同步面实现在 wedb/wedb/src/server/replication/
  （replica_diskbased_sync.rs / replica_diskless_sync.rs 等，符号级对位需逐文件甄别）。

C# 对标（garnet 相对路径:符号）
garnet/libs/cluster/Server/Replication/PrimarySync/PrimaryOps/ 整族（逐文件判
已实现 / 无需实现并登记 ignore / 补文档注释锚点）。

修法建议
逐文件甄别 PrimarySync 族：已实现的在 rust 函数文档注释挂 C# 相对路径锚点；
无需实现的在 js/check/ignore/garnet/libs/cluster/Server/Replication/PrimarySync/
下建 yml 登记理由；完成后实跑 bun js/check.js 取退出码，复绿即闭环（此前
gate-anchor 票因门禁未证复绿而保留，本票是它的全部剩余工作量）。
