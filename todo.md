# 未完成工作清单（本轮收口后遗留）

> 生成于 opt-r3 合并收口时。已完成部分见 git log（可观测性闭环、ACL 会话门控、日志/存储过程回放、
> 向量 block_on 死锁修复、队列选型矩阵、check.js 缺失重复双清零、dev e630cfc 语义融合）。

## 1. 大集合 Flattened/wbftree 引擎恢复（最高优先，已获批准方向）

transpile/SKILL.md 的数据布局契约当前对 Hash/ZSet/Set/List 未兑现：四类集合命令全走
`run_sync_rmw` 整信封 RMW（10 万成员 ZSET 每次 ZADD 全量反序列化+重序列化）。
全仓唯一写 `StorageEncoding::FlattenedTree` 的只剩 RangeIndex。

- 实现源：`origin/opt-r1` 分支 `wedb/wkv/src/session/collection_bftree/{hash,list,set,zset}.rs`
  + `collection_flattened.rs`（已接线、含迟滞防震荡、hash_flattened_test 全过）
- 移植要点（按 SKILL.md 原契约）：
  - 升级门限 `HASH_UPGRADE_ITEM_THRESHOLD = 32768` 或 1MB；降级 16384 且 512KB；50% 迟滞
  - 17B 定长帧（wval `SUBKEY_HEADER_SIZE = 17` 已有）+ 单树多前缀（0x01..0x06）
  - O(1) 计数规约：HLEN/SCARD/ZCARD/LLEN 直读 MetaValue.size，LLEN 用 ListStub 算术差
  - 全区间 ZCOUNT/RI.COUNT 短路直读；删空生命周期 + O(1) 版本号栅栏
- 打通：升级/降级钩子接进当前 wkv session 与 wnode 信封写路径（wcol 合并后架构，需语义适配）
- 清理老代码：被取代的信封路径残留、死分支
- 恢复测试：hash_flattened_test（迟滞矩阵）等
- 完成后：若最终采纳单信封架构（不做本项），则反向修订 SKILL.md 并配 ignore——二选一，不能保持矛盾

## 2. waof/wkv Group-Commit 双实现抽公共件

`waof/src/log.rs:167-171` 与 `wkv/src/store.rs` FlushPipeline 是同一 Leader/Follower
Group-Commit 模式的两份实现。抽公共 `GroupCommitPipeline` 入 wbase（waiters 注册/Leader
级联/批量唤醒一处定义），两 crate 参数化。建议随 wkv 重构窗口一并做。

## 3. 手写自旋阶梯收敛 wbase::backoff

`wkv/src/read_cache.rs:425`、`wrecord/src/header.rs:360`、`wnode/src/aof/*`、`waof/src/log.rs:563`
各自手写 "spin<32→yield" 阶梯（行为正确、常量重复）。先解 backoff 的 feature 门控与
pool/map feature 的耦合。

## 4. RespClusterIterativeSlotVerify（集群迭代式逐键槽位校验）

对标 `TxnKeyManager.cs:51` 迭代逐键入口；rust 现有 `network_multi_key_slot_verify` 未覆盖
迭代形态。已在 js/check/ignore 声明为能力缺口，属集群收尾项。

## 5. 小项

- `wnode/src/resp/vector/vector_manager_{replication,quantization}.rs` 头注释仍指向失效路径，
  改指 `wbase::EventWorkQueue` + `vector_manager_cleanup.rs`（并行代理禁区未落，纯注释）
- `StoreWrapper.Reset`（C# Pause+Reset+Resume 语义）：落地后补
  vector_set_cleanup_vs_reset_race 测试的三段锤击
- `net/handler.rs` 直读/回退两读取形态的镜像累加 net_in 口径统一（本轮融合后遗留的观察项，
  监视器测试当前全绿）
- flaky `wkv store::flush_evict::test_adversarial_heavy_concurrency_with_eviction` 根治
  （高并发调度级偶发，已有 nextest.toml retries 兜底）
