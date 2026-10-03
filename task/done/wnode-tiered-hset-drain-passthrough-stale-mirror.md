审核结论：通过（2026-09-30 审核席）。完整行为链逐环亲验坐实：删空臂置脏 common.rs:563 后成功路径 :632 return Swept(Vec::new()) 不复位 dirty（仅 :578/:606/:624 失败臂复位）；RangeIndexDrop 于 drain.rs:138-148 在 drain_and_delete_index 末位入账，先于臂尾 emit（删空 await 先于 :728 完成）；hash.rs:199-214 删空后 Swept 分支 :203 load_collection_stub 探空（键已消亡）落 :205 break 'arm Ok(false) 穿透而非判已处理；mirror 变量 :130 一次性生成（Hset/Hmset 在 tiered_hash_writes 写集 :53-62 内恒 Some），穿透路径无置 None 环节；臂尾 :728 emit 不分支 result，判据 Some+dirty（common.rs:822-825）全真即照发。重放端 RangeIndexDrop 重放后 stub 消亡，object_replay.rs:185-191 探空必落 missing_tiered_stub_skip 的 log::error!（:150），beyond_tiered_arm（:131）为同类 error 留痕出口。反向甄别：emit 前无 stub 存活门控、穿透无早退跳过臂尾 emit，反证链不成立。非重复：五池 grep 无 emit_tiered_mirror 同面票；dead-key-read-arms（读臂触树应答面）、deferred-sweep-undercount（推迟窗计数应答面）、swapin-size-overcount（size 记账面）判据面互斥。架构合规：dirty 为 WATCH（finish_tiered_arm common.rs:749）与镜像（:823）共用单判据，穿透点复位即单机制收敛；同构穿透面仅 hash 族（set/list/zset 无臂内 Swept 穿透形态，scan.rs 为 mirror=None 计数面不受累）。

执行方案优化（审核席补强）：
1. 复位点必须落在两处探空确认之后、break 之前（hash.rs:204-205 与 :209-210），严禁上提到 Swept 分支头——:203 的 map_err(|_| ())? IO 失败臂树面已变更须保持 dirty 真实推进（Err+dirty 兜底口径不变）。
2. 严禁改 common.rs 删空成功路径（:632 前）统一复位：scan.rs 读臂调用方 mirror=None、无穿透物化段承接，dirty 是其删空变更唯一 WATCH 推进通道，统一复位丢 WATCH 推进致并发 WATCH 事务误成功。
3. 既有口径不动：finish_tiered_arm、emit_tiered_mirror、save_tiered_meta 零改动；WATCH 语义由穿透后 run_async_rmw 物化段对象层写承接（hash.rs:716-722 既有漏斗）。
4. 测试验证点补两项：WATCH 回归（分层键全员到期后 HSET 穿透路径，并发 WATCH 事务经物化段正常夭折/推进，无漏推无假推进）；四族写臂镜像面回归（Hset/Hmset/Hsetnx/Hincrby 正常稳态写镜像照发，仅删空穿透臂受抑）。

原票面：
HSET 全员到期删空穿透臂仍发 TieredCollectionWrite 镜像，重放端按发散残留 error! 误报

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   自研分层面，C# 无对应。判据出处：doc/zh/collection.md 第 2 节稳态写入账为 TieredCollectionWrite，且重放臂自带生产前提（aof_processor_object_replay.rs:167 文注：镜像条目只产自树内稳态写臂生效的命令（dirty 判据），违反即 AOF 流损坏或主从发散残留）。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   tiered_collection_ops/common.rs:expire_sweep_or_rebuild 删空臂 ctx.dirty = true（:563）后 handle_bftree_drain_and_delete 成功返回 Swept(Vec::new())，dirty 保持真（仅失败臂复位）；hash.rs:tiered_hash_arm Hset/Hmset 折叠臂探空后 break 'arm Ok(false) 穿透物化与信封通道，但臂尾 emit_tiered_mirror（hash.rs:728 附近）不分支 handled 结果——dirty 真 + mirror Some 即照发 TieredCollectionWrite。同命令随后经信封通道重放执行 EnvelopeUpsert 再入账，AOF 序列为 RangeIndexDrop → TieredCollectionWrite(HSET) → ObjectStoreUpsert；重放端 tiered_replay_arm 见 stub 已随 RangeIndexDrop 消亡，落 missing_tiered_stub_skip 或 beyond_tiered_arm 的 log::error 发散残留留痕。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   每次 AOF 恢复或副本重放含该序列的日志段必触发 error! 级发散误报，真主从发散的告警信噪比被系统性污染，运维误判；镜像前提与重放臂文注直接矛盾（账外 dangling 条目）。重放终态正确（skip 后信封全值收敛），无数据损坏。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:expire_sweep_or_rebuild
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:emit_tiered_mirror
wedb/wnode/src/resp/objects/tiered_collection_ops/hash.rs:tiered_hash_arm
wedb/wnode/src/aof/aof_processor_object_replay.rs:tiered_replay_arm
wedb/wnode/src/aof/aof_processor_object_replay.rs:missing_tiered_stub_skip
wedb/wnode/src/aof/aof_processor_object_replay.rs:beyond_tiered_arm

对应 c# 文件与函数：
无 C# 对应（自研分层面，判据出处 doc/zh/collection.md 第 2 节与重放臂文注前提）

精炼执行方案：
1. 删空穿透臂（break 'arm Ok(false) 前）复位 ctx.dirty = false（键已消亡、无树内稳态变更，镜像前提恢复自洽），复用既有 dirty 判据单机制，严禁另立第二镜像抑制通道。
2. 测试验证点：分层态 hash 全员到期后 HSET 触发删空穿透，恢复重放该日志段断言零 error! 留痕且副本终态与主端一致。

终态注记：
- 合入收口形态：在 tiered_hash_arm 的 SweepOutcome::Swept 分支中，当 session.load_collection_stub(key) 探空返回 None 以及 tiered_guard(session, key, ctx, true) 返回 None 时，在 break 'arm Ok(false); 之前设置 ctx.dirty = false;，抑制删空穿透后陈旧的 TieredCollectionWrite 镜像发射。
- 合入哈希：3baa3db
- 状态：已收口归档。

