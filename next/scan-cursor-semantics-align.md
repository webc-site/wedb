# SCAN 族迭代器语义边界与刻意差异对齐

来源：next/zcode-r6-scan.md 问题 2、问题 3、问题 4

## 问题

1. 向量键仅在 cursor==0 首页追加，若客户端期望每页严格不超过 COUNT 或多页均匀分布时存在分叉。
2. COUNT<=0 规整为 1 与 TYPE 空串处理在代码中声明为同语义，实际上偏离 C# 原样行为（C# count<=0 直传且未匹配时硬写游标 0）。
3. 集合在跨升阶（信封到树）或懒降阶（树到信封）过程中，游标序域在哈希序与字典序之间切换，游标不保证不重不漏，缺少文档声明。

## 涉及路径

- wedb/wnode/src/resp/garnet_api/slow.rs
- wedb/wnode/src/resp/array_commands.rs
- wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs
- wedb/wcol/src/hash/hash_object.rs
- doc/zh/collection.md

## 解决建议

1. 向量键首页聚合与可能超出 COUNT 的行为登记为架构差异，或在流式迭代中按配额分页并入。
2. 修正 parse_scan_filter 注释，将 COUNT<=0 规整为 1 及 TYPE 空串作为刻意差异登记。
3. 在 exec_tiered_scan 与 wcol scan 的文档注释中补充升降阶游标序域切换说明。
