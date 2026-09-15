# wbftree crate 死面清理

来源 next/glm.md 条目 29 / 64 / 65，主代理拍板执行。

## 核实结论

### 来源 1：by_ptr unsafe 家族与读缓冲选路（全属实）

ops.rs 七个 by_ptr（noop_by_ptr / insert_by_ptr / read_by_ptr_into / read_by_ptr /
delete_by_ptr / scan_with_count_by_ptr_callback / scan_with_end_key_by_ptr_callback）+
snapshot.rs cpr_snapshot_by_ptr，全仓引用仅 tests/manager_and_stub/service.rs 两个测试。

读缓冲栈/暂存选路在 read_callback、read_into_fallback、scan_callback 三处重复，
删 by_ptr 后 read 侧两份收敛 with_read_buffer 单点（行为等价）。

### 来源 2：双枚举与零消费状态码

RangeIndexResult：仅 tests/main.rs test_range_index_result_mapping 引用，
wkv/wnode 只是注释文字提及 C# 侧同名概念，可删。

StorageBackend 与 StorageBackendType 合一：已落地（主代理解除并发约束后补做）。
保留 Garnet 1:1 语义的 StorageBackendType（Disk/Memory），删除 Std/Memory 版
StorageBackend 与全部互转 From impl；lifecycle/stub/wkv(range_index、store/event)/
wnode(service、aof_processor、resp/rangeindex 两文件) 全链统一签名，
wnode service.rs 的 storage_backend_type 转换函数与
range_index_manager_replication.rs 的 storage_backend_from_u8 随之消亡
（后者改 StorageBackendType::from_u8）。

### 来源 3：pub 面收缩（逐项核实）

可删（零外部生产引用，仅 wbftree 自测或 wkv 注释提及）：
- scan_all / scan_all_callback（waof 同名属别 crate；C# ScanAll 无 rust 外部用户）
- unregister_index（测试改用 dispose_tree_under_lock(key, false) 等价路径）
- recover_all_trees_from_checkpoint（wkv 实际走 recover_all_trees_from_dir）
- wait_for_global_checkpoint（wrapper；within 版保留）
- snapshot_all_trees_for_checkpoint（wkv 注释提及，实际调 to_dir；注释同步修正）
- hash_prefix_of（standalone 集成测试 3 处改用 base32_prefix_of，零堆形态）
- CacheAlignedLock（纯 re-export 别名，零内部使用）
- RangeIndexLocks（类型别名；locks() 保留，返回类型改为具体类型）
- NUM_LOCK_STRIPES（pub 转私有）
- open_disk / open_memory / BfTreeConfig / BfTreeService::new / preset_config
  （外部构造走 RangeIndexManager::create_bftree 或 recover_from_cpr_snapshot，
  对标 C# BfTreeService 构造由 RangeIndexManager 托管的事实）

保留（对标 C# 复制面预留）：on_flush / on_flush_address / on_truncate /
enumerateate_files_for_replication / get_replication_file_names /
wait_for_global_checkpoint_within。

## 改动清单

### wbftree/src

service/ops.rs
- 删七个 by_ptr 函数与两段注释块
- read_callback / read_into_fallback 选路收敛 with_read_buffer
- 删 scan_all / scan_all_callback，SCAN_ALL_START_KEY 常量随删

service/snapshot.rs
- 删 cpr_snapshot_by_ptr
- 新增 crate 内单元测试承接 use_snapshot=false 快照 Err 路径
  （原集成测试因 new_with_backend 为 pub(crate) 无法在外部构造）

service/mod.rs
- 删 new / preset_config / open_disk / open_memory
- mem_service() 改走 new_with_backend + bf_tree::Config::default().cache_only(true)

types.rs
- 删 BfTreeConfig（含 Deref/DerefMut/From/impl 块）
- 删 RangeIndexResult（含三个 From impl）

lib.rs
- 导出收缩：BfTreeConfig、RangeIndexResult、CacheAlignedLock、
  RangeIndexLocks、NUM_LOCK_STRIPES、SCAN_ALL_START_KEY 移除
- 文档注释同步（RangeIndexLocks 提述改为具体类型描述）

manager/mod.rs
- 删 CacheAlignedLock / RangeIndexLocks 别名，locks 字段与 locks()
  改用 StripedRwLock<(), NUM_LOCK_STRIPES>
- NUM_LOCK_STRIPES 转 pub(crate)
- 删 hash_prefix_of，base32_prefix_of 注释补正式 C# 映射

manager/lifecycle.rs
- 删 unregister_index
- instantiate_tree 去 BfTreeConfig，直接构造 bf_tree::Config

manager/replication.rs
- 删 recover_all_trees_from_checkpoint

manager/checkpoint.rs
- 删 wait_for_global_checkpoint 与 snapshot_all_trees_for_checkpoint
- snapshot_all_trees_to_dir 注释补 SnapshotAllTreesForCheckpoint 映射

### wbftree/tests

- tests/main.rs：删 test_range_index_result_mapping；其余三个测试改
  RangeIndexManager::create_bftree 构造
- tests/manager_and_stub/service.rs：删两个 by_ptr 测试；open_memory/
  BfTreeService::new 改 create_bftree；scan_all 断言改 scan_with_count
- tests/manager_and_stub/manager.rs：hash_prefix_of 改 base32_prefix_of；
  snapshot_all_trees_for_checkpoint 改 to_dir；recover_all_trees_from_checkpoint
  改 from_dir；wait_for_global_checkpoint 改 within(Duration)；unregister_index
  改 dispose_tree_under_lock(key, false)
- tests/manager_and_stub/locks.rs：经 manager.locks() 测读写语义；
  对齐断言归 wbase 职责删除
- tests/interop/*：open_disk/open_memory/BfTreeService::new/BfTreeConfig
  改 RangeIndexManager 构造；scan_all 测试删；自定义配置测试走 TreeTuning

### 仓内配套

- wedb/wkv/src/checkpoint.rs:42 注释修正（snapshot_all_trees_for_checkpoint
  改 snapshot_all_trees_to_dir）
- wedb/wedb_standalone/tests/range_index_tests.rs 三处
  Engine::hash_prefix_of 改 Engine::base32_prefix_of

### js/check/ignore 登记

- native.yml：BfTreeService.cs 的 InsertByPtr / ReadByPtr / ReadByPtrInto /
  DeleteByPtr / ScanWithCountByPtrCallback / ScanWithEndKeyByPtrCallback /
  CprSnapshotByPtr / ScanAll
- server.yml：RangeIndexManager.cs 的 UnregisterIndex
- test.yml：BfTreeInteropTests.cs 的 ScanAll_ReturnsAllEntries /
  ScanAll_EmptyTree / ScanAll_KeyOnly

## 验证

干净 worktree（dev 合并后基线）复核：bun ./js/check.js 退出 0（check/miss 空）、
./clippy.sh 退出 0、./test.sh 退出 0（2008 passed + 1 skipped + regress 2 passed；
较清理前减少的 17 个为本次删除的死面自测）。

## 主代理 code review 结论与收尾

实现代理在分支提交时误将 target/ 构建产物（约 12.5 万文件）一并入库，
主代理重做提交剔除（42 文件 +763/-1338），并在根 .gitignore 补 /target/
规则防复发（提交 17215e3c）。

review 逐文件对照规划核实：
- by_ptr 七函数 + cpr_snapshot_by_ptr 删除，无残留调用；ignore 登记齐全
- 读缓冲三份选路收敛 with_read_buffer 单点，STACK_BUF_SIZE 统一 8192
  （原 read 4096 / scan 8192 双阈值合一，对标 C# stackalloc byte[8192]；
  SAFETY 契约保持：栈路径仅暴露引擎完全覆盖写入的前缀切片）
- RangeIndexResult 及三个 From impl 删除
- StorageBackend/StorageBackendType 合一如上，转换层自然消亡
- pub 面收缩与规划一致；on_flush/on_truncate/enumerate_files_for_replication
  预留面保留；wait/snapshot/recover 仅删便捷壳，底层 _within/_to_dir/_from_dir 保留
- 测试与 bench/regress 适配统一经 RangeIndexManager 托管构造
  （managed_env 助手），bench 引擎持有 manager 保 Drop 顺序
- 附带：bench 原经 open_disk 直建；删除后经 create_bftree，gossip 场景
  未来落地时按 C# Gossip.cs 构造形态走 RangeIndexManager 或恢复便捷构造

最终验证在干净 worktree（dev 合并后基线）复核，结果见下。
