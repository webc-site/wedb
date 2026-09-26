拒绝结论：判净（核对 C# HashObjectImpl.cs 与 HashCommands.cs，Hash 族在纯内存 HashObject 与分层存储 TieredHash 下 RMW 锁窗独占、物化写回删空、4KB 升阶门限及字段到期清退双态逐字节全等，无缺陷无分叉）

Hash 族字段级写入与 TTL 边界对标审查报告

一、审查视角与背景说明
审查视角：Hash 族字段级写入与 TTL 边界 (HSET/HDEL/HEXISTS/HINCRBY/HINCRBYFLOAT 在分层存储 TieredHash vs 内存 HashObject 下的 RMW 锁窗、单页钳制与字段到期清退一致性)
核查目标与范围：
1. 核查 wedb 中 Hash 族字段级写入与读取命令（HSET/HMSET/HSETNX/HDEL/HEXISTS/HINCRBY/HINCRBYFLOAT）。
2. 核查分层存储（TieredHash / BfTree）与纯内存态（HashObject）在 RMW 锁窗持有、原位更新、降级物化（HDEL 物化穿透与零墓碑）上的正确性与竞态防御。
3. 核查单页容量硬上限拦截（单页钳制）在内存信封态超限升阶与 BfTree 分层态写入前校验（validate_bftree_record、record_fits_page）中的防御完备性。
4. 核查键级 TTL 与字段级 TTL 在 HSET/HSETNX/HDEL/HEXISTS/HINCRBY/HINCRBYFLOAT 下的到期判定、清退、删空自愈以及双态行为一致性。
5. 对标 garnet/libs/server/Objects/Hash/ 与 garnet/libs/server/Resp/Objects/HashCommands.cs 等 C# 原型。
6. 核验 doc/zh/deviations.md 与 doc/zh/collection.md 等在册条款，严禁将既定架构改良报为缺陷。

二、原型行为与对标核查（C# Garnet 事实确证）
1. C# 官方契约与源码现状
对应 c# 文件与函数：
garnet/libs/server/Objects/Hash/HashObjectImpl.cs:HashObject.Operate
garnet/libs/server/Objects/Hash/HashObjectImpl.cs:HashObject.InPlaceUpdate
garnet/libs/server/Objects/Hash/HashObjectImpl.cs:HashObject.NeedInPlaceUpdate
garnet/libs/server/Objects/Hash/HashObject.cs:HashObject
garnet/libs/server/Resp/Objects/HashCommands.cs:RespServerSession.HSet
garnet/libs/server/Resp/Objects/HashCommands.cs:RespServerSession.HDel
garnet/libs/server/Resp/Objects/HashCommands.cs:RespServerSession.HExists
garnet/libs/server/Resp/Objects/HashCommands.cs:RespServerSession.HIncrBy
garnet/libs/server/Resp/Objects/HashCommands.cs:RespServerSession.HIncrByFloat
garnet/libs/server/Resp/Objects/HashCommands.cs:RespServerSession.HashCommands
garnet/libs/server/Storage/Session/ObjectStore/HashOps.cs:StorageSession.HashInPlaceUpdate
garnet/libs/server/Storage/Session/ObjectStore/HashOps.cs:StorageSession.HashRMW

核查确证事实：
1) 原型对象模型与内存布局：
C# Garnet 纯内存 HashObject 内部采用 Dictionary<byte[], byte[]> 或 HashObject 类，在 Tsavorite 引擎上以对象信封形式落盘/缓存。Garnet 原生未设计针对单一 Hash 容器的分层 B 树分片存储机制，大对象超出内存阈值时由主存换出至 Tsavorite 磁盘存储层。
2) 原位更新与锁持有：
Garnet 在 Tsavorite 记录内通过 NeedInPlaceUpdate 与 InPlaceUpdate 进行原位更新，当值长度不增且字段存在时原位覆写；当长度变更或新字段插入时通过 RMW 创建新版本对象。
3) TTL 边界行为：
C# 原型中 Hash 仅支持键级 TTL（由 Tsavorite 记录头部的 Expiration 控制），不原生支持 Redis 7.4+ 的 Hash 字段级 TTL（HTTL/HEXPIRE/HPEXPIRE 等字段级到期时间戳）。对于 HINCRBY/HINCRBYFLOAT/HSET，仅在键级生命周期内对 Dictionary 进行字段操作，无字段到期时间戳校验。
4) 单页钳制：
C# Garnet 依托 Tsavorite Object Store，对象序列化为整块 byte[]，无 B 树叶页 4KB 钳制，但当大对象持续增长时会承受整包序列化与 I/O 放大。

三、工程现状确证（Rust wedb 实现核查）
1. 模块结构与实现路径
对应 rust 文件与函数：
wedb/wnode/src/resp/objects/hash_commands/write.rs:RespServerSession::hset
wedb/wnode/src/resp/objects/hash_commands/write.rs:RespServerSession::hsetnx
wedb/wnode/src/resp/objects/hash_commands/write.rs:RespServerSession::hincrby
wedb/wnode/src/resp/objects/hash_commands/write.rs:RespServerSession::hincrbyfloat
wedb/wnode/src/resp/objects/hash_commands/write.rs:run_sync_rmw_with_mode
wedb/wnode/src/resp/objects/hash_commands/read.rs:RespServerSession::hexists
wedb/wnode/src/resp/objects/hash_commands/slow.rs:RespServerSession::hash_slow_path
wedb/wnode/src/resp/objects/tiered_collection_ops/hash.rs:tiered_hash_arm
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:tiered_precheck
wedb/wnode/src/resp/objects/rmw_helpers.rs:run_sync_rmw
wedb/wnode/src/resp/objects/rmw_helpers.rs:run_async_rmw
wedb/wnode/src/resp/objects/rmw_helpers.rs:apply_rmw_post_operate
wedb/wnode/src/resp/objects/rmw_helpers.rs:promote_to_bftree
wedb/wnode/src/resp/objects/rmw_helpers.rs:envelope_overflow
wedb/wcol/src/hash/hash_object.rs:HashObject::operate
wedb/wcol/src/hash/hash_object_impl.rs:HashObject::replace_value_slice
wedb/wcol/src/hash/hash_object_impl.rs:HashObject::in_place_update
wedb/wkv/src/range_index/ops.rs:StorageContext::tree_put_batch
wedb/wkv/src/range_index/ops.rs:StorageContext::validate_bftree_record
wedb/wkv/src/range_index/promote.rs:StorageContext::materialize_raw_records

核查确证事实：
1) RMW 锁窗机制（Lock Window）：
纯内存 HashObject：
快路径（run_sync_rmw）通过 try_rmw_window 获取锁窗，覆盖装载、operate 变异、should_write、终态复验到写回全过程；若发生争用或冷键，平滑降级至慢路径。
慢路径（run_async_rmw）通过 rmw_window.await 获取全异步 RMW 独占锁，跨越 I/O 与让核点全程持有，确保并发更新无锁窗撕裂。
分层存储 TieredHash：
写变体（HSET/HMSET/HSETNX/HINCRBY/HINCRBYFLOAT）在 tiered_hash_arm 中经 tiered_guard(..., write=true) 获取条带独占写锁 acquire_tree_write，并在写锁内执行刷新元记录、树内变异（tree_put_batch / tree_put_ok）、元数据落盘与 AOF 镜像。
读命令（HEXISTS）经 tiered_guard(..., write=false) 获取条带共享读锁 acquire_tree_read。
HDEL 物化降级机制：
HDEL 在 slow.rs 中不入 op_opt 分层快速通道，统一穿透至 run_async_rmw 物化降级通道，通过 tiered_materialize_blob_sealed 获取 swap_in_window 独占封窗，将树上全部字段物化为标准内存 HashObject 执行删除，再经 apply_rmw_post_operate 决策写回新树或删空回收。严格遵循 doc/zh/collection.md 第 8.4 节「物化臂唯一、树内零墓碑」不变量，杜绝了树内墓碑垃圾积累与双域状态撕裂。

2) 原位更新与内存复用（In-place Update）：
纯内存 HashObject：
HashObject::replace_value_slice 判定新旧值等长时，直接通过 copy_from_slice 复用原堆内存缓冲区；若异长则精确更新堆内存记账并重新分配，防止内存虚增。
分层存储 TieredHash：
tree_put_batch 与 tree_put_ok 在 BfTree 叶页内执行原位 upsert，直接覆写页内键值记录，避免分裂与页碎片。

3) 单页容量硬上限拦截（单页钳制）：
内存信封态超限升阶：
当内存 HashObject 字段持续累积导致信封体积超过单页（4KB / record_fits_page）时，run_sync_rmw 与 apply_rmw_post_operate 经 envelope_overflow 闸口平滑触发 promote_to_bftree，将内存 HashObject 自动升阶拆解为分层存储 BfTree，防止单个大对象导致底层页溢出。
若单个字段极端过大超过 BfTree 单记录上限且信封超页，系统通过 fail-closed 干净拦截，返回存储错误帧并保持旧状态完全无损。
分层树态单页前置校验：
在 tiered_hash_arm 中，写入前由 tiered_precheck 统一调用 validate_bftree_record，强校验 stub.max_record_size 与 stub.max_key_len，若单条记录超限立即返回 InvalidKV 错误帧早退，硬性防御叶页溢出崩溃。

4) 键级 TTL 与字段级 TTL 清退一致性：
键级 TTL：
统一由 wkv 域内 KeyTag::Ttl 与 TtlGate 裁决；字段更新（HSET/HINCRBY 等）保持既有键级 TTL 不变；
当字段被 HDEL 全部删空或全部到期出账时，触发删空自愈（bftree_drain 或 delete_string），联动销毁键级 TTL，彻底杜绝孤儿键。
字段级 TTL（内存 HashObject vs 分层 TieredHash 双态全等）：
HEXISTS：两态均调用到期判定，到期字段视同不存在，一致返回 0。
HSET/HMSET：覆写存活字段清除字段 TTL（重置为无 TTL）；覆写已到期字段在双态下均视同插入新字段（清理旧到期元数据，计入新增数，返回 1）。
HSETNX：已到期字段视同不存在，允许插入新值并返回 1；存活字段命中则拒写并返回 0。
HINCRBY/HINCRBYFLOAT：存活字段累加保留原有字段 TTL（old_expiry）；已到期字段视同缺席，从 0 开始累加且新字段无 TTL。TieredHash 写入时通过 !expired_hit 守卫防止 meta.size 虚增，双态行为逐字节一致。
HDEL：删除已到期字段两态均视同不存在，返回 0，不虚增删除计数。

四、核查视角规约确证与偏离对齐
1. doc/zh/deviations.md 既有在册条款核验：
第 19 条：集合删空自愈不残留空对象，与 Redis 规约对齐（不对齐 C# 存留空对象缺陷）。
第 66a 条：自适应分层存储（TieredCollection）与 BfTree 降级机制在册合法。
第 80 条 / 第 1 条：浮点格式化标准。
第 120 条 / 第 129 条 / 第 136 条 / 第 142 条：RMW 锁窗、条带锁与并发一致性保证在册合规。
2. doc/zh/collection.md 既有在册条款核验：
第 8.4 节：明确「物化臂唯一，树内零墓碑」，HDEL 穿透物化为设计基准，严禁树内引入删除墓碑。
双门限迟滞规约与 O(1) 计数规约在 TieredHash 与 HashObject 中严格兑现。

五、结论总结
本席对 C# Garnet 原型与 Rust wedb 仓内 Hash 族字段级写入与 TTL 边界（HSET/HMSET/HSETNX/HDEL/HEXISTS/HINCRBY/HINCRBYFLOAT）进行了跨内存态与分层态的全链路深度审查。确证：
1. RMW 锁窗在同步快路径与异步慢路径下全程互斥，无锁窗提前释放或撕裂，分层态条带写锁与封窗屏障完整。
2. HDEL 严格遵守 collection.md 第 8.4 节物化降级规约，树内零墓碑，删空自愈与键级 TTL 清退无缝联动。
3. 单页容量硬上限拦截完备，内存态超页自动升阶为 BfTree，分层态写入前经 validate_bftree_record 硬性钳制，无叶页溢出风险。
4. 字段级与键级 TTL 在 HSET/HSETNX/HDEL/HEXISTS/HINCRBY/HINCRBYFLOAT 下判定、清退与状态转移双态逐字节全等，到期覆写出账正确。
5. 仓内实现闭环严密，无新缺陷，无未在册偏离。

视角结论:已穷尽
