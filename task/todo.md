# 任务待办区

## 9. 向量搜索对标 C# DiskANN 与 wvector 拆分(用户指示 2026-09-13)

已完成(2026-09-13,本轮):
- wvector crate 建立:官方 `diskann` / `diskann-providers` / `diskann-quantization` /
  `diskann-utils` / `diskann-vector` 0.59.0 全系接入;`WedbProvider` 实现
  `DataProvider`/`SetElement`/`Delete`/`NeighborAccessor(Mut)`/glue 策略
  (DynamicQuantization 全精度↔全量化双轨、Rerank 重排、内联过滤),
  对标微软官方 diskann-garnet 的 provider.rs;全部数据经存储回调持久化。
- `DiskANNService` 语义层(context → 索引实例注册表、IndexState 起点状态机、
  SearchResults 输出缓冲、Knn/InlineFilterSearch 检索、量化建表/回填生命周期),
  对标 diskann-garnet 的 lib.rs + dyn_index.rs。
- wnode `hnsw.rs` 自研纯内存图(947 行)删除,仅保留 HnswConfig 配置结构;
  wnode 向量模块经 mod.rs 别名桥接改调 wvector 路径;重复拷贝
  (vector_types/attribute_extractor/expr_compiler/expr_runner/
  vector_filter_expression/vector_manager_element_data)下沉 wvector 后删除。
- 验证:clippy 全工作区 0 警告 0 错误;test.sh 1943/1943 通过 + regress 2/2。

待办缺口:
1. `wvector::service::SearchParams` 缺 `count`(K)参数:检索输出按 L 定容
   (最坏 EF=1_000_000 时约 12MB 输出缓冲);wnode 侧以距离序截断到 count 承载。
   建议在 SearchParams 增加 count 并以 K 定容输出缓冲。
2. wvector 存储回调为内存过渡(`VectorStoreBridge` / `MemoryStore`):
   `(context|项类型, 键)` 物理键的 wkv/whlog 真实落盘与恢复重建待接入
   (对标 C# Tsavorite 回调注入 + VectorSessionFunctions)。
3. `DiskANNService::continue_search` C# 侧即未实现(NotImplementedException),
   wnode 对齐为 Err;分页续检待 diskann-garnet 上游补齐后再接入。
