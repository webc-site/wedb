# wkv 内部重复实现清理批（wkv/wcol 域八条）

来源：next/glm.md wkv 域八条（主代理已预清理移出）。逐条甄别后执行六条、拒绝一条、两项标注已处理。

对标
- garnet/libs/storage/tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:105-131（读路径单一分类枚举 + IsClosedOrTombstoned 单点）
- garnet/libs/server/GarnetCheckpointManager.cs（单类无双面，purge 继承基类单实现）
- garnet/libs/server/Storage/SessionFunctionsUtils.cs（过期判定单点）
- garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs 与 Storage/ReadCache.cs
- garnet/libs/server/Objects/Types/GarnetObjectBase.cs:ReadScanInput（SCAN 参数解析单点）

验收口径
- ./clippy.sh 零警告（禁 allow）
- ./test.sh 全过（读路径、过期、紧缩、checkpoint 回归全绿）
- bun ./js/check.js 无新增缺失/重复
- 删除有 C# 映射的符号在 js/check/ignore 登记
- CARGO_TARGET_DIR=/tmp/fork/w5-wkv-target

## 逐条甄别与改法

一 [P2] CheckpointManager 剩余转发面 —— 执行
- 甄别：CheckpointManager 全部方法为 wcpr 自由函数逐字转发；生产（wnode/wedb/wedb_standalone src）仅 wdatabase database_manager_base.rs:334 用 purge_outdated，其余消费全在测试。purge_checkpoint（单 token）/purge_all（清目录）/purge_outdated（保留 N）三语义不同非互删对象，「双面」指 wkv 面与 wcpr 面重复。
- 改法：
  1. 删 CheckpointManager 纯转发静态面：purge_checkpoint / purge_all / purge_outdated / list_checkpoints / find_latest_checkpoint / recover_store（零引用）/ recover_latest_store（零引用）
  2. create_checkpoint_with_token 私有化（仅 create_checkpoint 内部消费）
  3. 保留 new / create_checkpoint（token 预知逻辑非转发）/ recover / recover_latest（类型锚定恢复入口，测试与生产在用）
  4. 调用方直用 wcpr：wkv tests（checkpoint/checkpoint_manager.rs、recovery.rs）、wedb_standalone tests（resp_flush_swap_collect.rs、storage_api.rs）改 wcpr:: 自由函数；wkv cargo add --dev wcpr、wedb_standalone cargo add --dev wcpr；wdatabase 生产 purge_outdated 改 wcpr::purge_outdated（已依赖 wcpr，零新增）

二 [P2] 读路径双探针枚举合一 —— 执行
- 甄别：raw/mod.rs ReadProbeResult（Miss/Tombstone/Retry/Found）与 TraceBackResult（TraceBack/Tombstone/Retry/Found）四态同构仅命名与顺序异；raw/read.rs 四份同构探针闭包体（主链 immutable/memory + fallback immutable/memory，逐字重复 matches_key → is_closed → is_tombstone → Found/Miss 分类）。
- 改法：
  1. 删 TraceBackResult，统一 ReadProbeResult（Miss(u64) 承接原 TraceBack(u64)）
  2. 抽单一探针函数 probe_hlog_record（RecordRef 入参，键比对/密封/墓碑/命中分类单点，对应 InternalRead.cs:118 IsClosedOrTombstoned），四处调用点各一行化
  3. whlog with_immutable_record 与 with_memory_record 回调签名同为 FnOnce(RecordRef) -> Result<R>，抽取消融安全

三 [P2] TTL 读取双实现 —— 执行（与四联动）
- 甄别：ttl.rs ttl_of（read_raw_with + TtlCodec::decode 3 行）vs compact.rs read_ttl_expiry（26 行手写内存探针 + 磁盘 fallback + 手写 8B 大端解码）。read_ttl_expiry 是 wcompact CompactSession trait 实现方法不可删，但实现体与 ttl_of 语义等价（内存优先 + 磁盘回退正是 read_raw_with 内核）。
- 改法：ttl.rs 抽 ttl_record_of(ttl_k: &[u8])（接受已编码 TTL 物理键，read_raw_with + TtlCodec 单点）；ttl_of 转 ttl_record_of；read_ttl_expiry 实现体改一行转发 ttl_record_of，删手写解码（TtlCodec::decode 单点回归）

四 [P2] 惰性过期裁决内联复制收敛 —— 执行
- 甄别：has_ttl_tag && check_expired 形态 6 处（range_index.rs:287 load_range_index_stub、collection.rs:96 contains_key、collection.rs:214 load_meta、modify.rs:199 rmw（空 if 体，死条件）、read.rs:758 read_tag_with、read.rs:776 read_tag_with_size）。keyspace.rs:60 与 gc.rs:366 为 GC 候选双检路径（已知有 TTL，完整裁决正确），不纳入。
- 改法：ttl.rs 定义 probe_alive(user_key) -> Result<bool>（true=存活/无 TTL；false=已过期已物理清除，内含 has_ttl_tag 单探针快门控 + check_expired 完整裁决，注释对标 SessionFunctionsUtils 过期判定单点）；6 处调用点改写；modify.rs rmw 空 if 体顺势清理为副作用调用

五 [P2] ReadCache 薄包装与生产开关 —— 执行（包装删除）+ 登记（开关不接 NodeArgs）
- 甄别：read_cache.rs is_read_cache_addr/absolute_address/tag_read_cache_addr 三包装逐字转发 wbase::addr::{is_read_cache,to_absolute,with_read_cache}；tag_read_cache_addr 仅 crate 内 3 处。生产装配 store_config() 硬编码（wnode/service.rs:505），NodeArgs 无存储引擎开关通道；ReadCache 本体带未闭环 r1 观察项（read_cache.rs 撕裂写窗口注释「维持观察，待 on_flush 管线接线时一并评估」）；C# GarnetServerOptions.cs:582 EnableReadCache 默认 false。接活命令行 = 激活未定型引擎面，拒绝。
- 改法：
  1. 删三包装，read_cache.rs 及全 crate 消费点（checkpoint.rs、compact.rs、range_index.rs、store/addr.rs、raw/read.rs、raw/write.rs、raw/modify.rs）直用 wbase::addr 原语
  2. lib.rs 删 is_read_cache_addr 出口；wkv tests/store/collision_chain.rs 改 wbase::addr::is_read_cache（wkv cargo add --dev wbase）
  3. 开关显式登记：config.rs enable_read_cache/with_read_cache 文档写明接线通道 = StoreConfig（open_node_with_config 嵌入式注入）+ 检查点 StoreMeta 恢复面自动复原，生产命令行不暴露（r1 观察项未闭环 + C# 默认 false）；read_cache.rs 模块注释同步；check.js 若出 EnableReadCache miss 则 ignore 落条

六 [P2] 零调用 pub 面收敛批 —— 部分执行（逐项甄别）
- RunGuard：仅 gc.rs:275 内部使用 + lib.rs re-export → 私有化（删 pub），lib.rs 出口收缩
- GcStatsSnapshot：store/gc.rs gc_stats() 返回值 + wkv/tests/gc.rs 消费 → 保留
- ListTree：全仓不存在，上一轮清理已删 → 标注已处理
- compact_lazy：wkv/tests/compact/lazy_compaction.rs 在用 → 保留
- compact_with_filter：wkv/tests/compact/spanbyte_compaction.rs 在用 + 对标 C# Tsavorite Compact 带 ICompactionFunctions → 保留
- tag_read_cache_addr：条五覆盖（删）
- wcol 双 ScanInput：resp/input.rs:168（usize 版，零消费）删除，resp/mod.rs、types/mod.rs、lib.rs re-export 链同步收缩；garnet_object_base.rs ScanInput + read_scan_input 单点化——生产在用的解析实际内联于 hash_object.rs scan_operate_shared 与 zset sorted_set_object_impl.rs scan_operate（三份同构 MATCH/COUNT/NOVALUES/cursor 解析），收敛：read_scan_input 从 trait 缺省方法提为自由函数单点，ScanInput 改 ScanInput<'a>（pattern: &'a [u8] 零拷贝），钳制改无条件（对齐 C# countInInput > limitCountInOutput 与生产内联版，修正原「limit > 0 才钳」偏差），hash/set/zset 三消费方改调单点
- ObjectOutputFlags：sorted_set_object.rs 在用（WRONG_TYPE/REMOVE_KEY）→ 保留
- ExpirationQueue：hash_object.rs、sorted_set_object.rs 在用 → 保留
- CUSTOM_TYPE_ID_START：全仓不存在 → 标注已处理

七 [P2] 单实现 trait 收敛评估 —— 拒绝降级（保留 trait），清理过时注释
- 甄别：wbftree 无任何 trait（「TreeOps 族」不存在，标注）；RiTreeOps 仅 impl BfTreeService。
- 评估证据（拒绝降固有方法块）：
  1. 孤儿规则：BfTreeService 定义于 wbftree，wkv 不能写固有 impl 块
  2. 依赖方向：方法挪 wbftree 需 wbftree 依赖 wkv 的 CollectionError，反向成环（wkv 依赖 wbftree）
  3. 换 wbftree::Error 会改变 wkv CollectionResult 对外语义面并波及 range_index.rs 与 wnode 消费方
- 结论：单实现 extension trait 是当前依赖拓扑下为外部类型扩展方法的正解；wcol::CollectionItemStore 依赖倒置点保留不动（任务已声明）。仅清理 ri.rs 模块头过时注释（ri_exists 不在接口清单）。不碰 design.md 条 3（wnode 越层 wbftree 门控，仍在 next）

八 [P2] wnode 穿透引擎字段 —— 执行（最小改动）
- 甄别：range_index_manager_migration.rs:117 `&session.store.range_index` 跨层直取；同型穿透 wnode 生产面共 3 处（另 service.rs:414、garnet_api.rs:687）；字段 pub（WedbStore 字段面整体 pub 形态）。
- 改法：WedbStore 加显式访问器 range_index() -> &RangeIndexManager（wkv/src/store/mod.rs）；migration.rs:117 一行改访问器；service.rs、garnet_api.rs 同型两处顺手改（生产面字段直取清零）；wedb_standalone 测试 2 处直取保留（字段暂保 pub，design.md 条 3 门控时统一收敛，本批不加闸）

## 回归保障
- wkv/tests 全量（checkpoint/*、compact/*、gc.rs、store/*、ttl 面）覆盖读路径、过期、紧缩、checkpoint 四域
- wedb_standalone/tests（storage_api.rs、resp_flush_swap_collect.rs、range_index_tests.rs、service.rs）适配后全过
- wbftree/wcompact 测试不受动（条七不改行为）

## 分批作业
wkv（条一三五内联 + 二三四读路径/过期 + 六 RunGuard + 七注释 + 八访问器）→ wcol（六 ScanInput 合一）→ wdatabase/wnode/wedb_standalone 适配 → ignore 登记 → 全量验收。
