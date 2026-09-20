# README 与 doc/zh/db.md 架构拓扑与接口描述修正

来源：next/zcode-r6-doc.md 问题 1 至问题 5

## 问题

1. 外层 README.md 与 readme/{zh,en}.md 中将 wnode 描述为零存储零 AOF 的纯网络底座，实际 wnode 承载了 Garnet.server 服务运行时，存储执行域与 AOF 重放均在 wnode 编排。
2. 外层 README 拓扑图中 wedb_standalone 指向 wkv/waof/wresp 等五条直接依赖线，实际仅依赖 wnode，其余全在 dev-dependencies 中。
3. 对标矩阵中误将 BfTree 范围索引算子层标注在 wcol，实际位于 wbftree 与 wkv/wnode。
4. doc/zh/db.md §4.5 虚构了 ClusterMsgFlushAll 控制帧结构体，实际实现为基于 gossip 连接的 CLUSTER FLUSHALL_NS 命令帧。
5. doc/zh/db.md 中存在三处过时路径（garnet_api.rs 改为目录、vdb/gc.rs 改为 gc_dead.rs、wkv/src/gc.rs 改为目录）。

## 涉及路径

- README.md
- readme/zh.md
- readme/en.md
- doc/zh/db.md

## 解决建议

1. 修正 README.md 及 readme/*.md 中对 wnode 的定位说明与依赖拓扑图。
2. 修正对标矩阵中 BfTree 范围索引归属。
3. 修正 doc/zh/db.md 中的 FLUSHALL 协议命令描述与三处文件路径锚。
