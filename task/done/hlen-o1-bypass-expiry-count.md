HLEN 旁路过期计数落地 O(1) 与升阶判定计数解耦（wcol 侧）

来源 next/hlen-o1-bypass-expiry-count.md。仅做 wcol 内存态半边；wnode obj_save_or_gc 判定下沉单点（next/obj-save-or-gc-promote-gate.md）另票，本单不碰其入口，避免两处各改 should_promote。

现状（主仓 dev HEAD 事实）
wcol/src/hash/hash_object.rs count() 在 expiration_times 在册时走 times.keys().filter(is_expired).count()，即 O(带 TTL 字段数)；全字段 HEXPIRE 的 65536 条目 hash 每次 HLEN O(64K)。该 count() 同时是升阶判定输入：wcol/src/types/garnet_object.rs should_promote/should_demote 取 self.count()，被 wnode object_store_utils run_sync_rmw / apply_rmw_post_operate 每个写操作调用，把带 TTL 大 hash 的读路径 O(K) 成本搬进写热路径。doc/zh/collection.md 声明「HLEN 惰性过滤下保持 O(1)」「全部计数命令严格 O(1)」，实现无旁路计数。C# HashObject.Count 本身即 O(K) 忠实转写，故本项是自研规范声明未兑现，非转写缺口。

改动方向
1. 旁路过期计数落地：count() 的在册分支改为 O(1) 精度——维护 expired_pending 标量（set_expiration 把过期时刻落在过去刻度时递增、堆序 delete_expired_items purge 时清零），HLEN 直读 len - expired_pending；或先走 delete_expired_items（PQ 只弹到期前缀，摊还 O(弹过量)、稳态 peek 短路）后直读 len。二者择一，禁两套存活计数口径。
2. 升阶判定输入与 O(K) 计数解耦：should_promote/should_demote 的 hash 计数输入改用 O(1) 字段量（len 未剔过期作启发式容忍，或 purge 后 len），禁每写 O(K) 扫描。
3. 若裁决维持 C# O(K) 口径不实施方向 1，则必须同步修正 doc/zh/collection.md 声明，且方向 2 仍须执行。

约束
- 中文注释；禁 #[allow]；只在 100% 安全处 unwrap/get_unchecked；不新增依赖（必须 cargo add，禁改 Cargo.toml）。
- 不改 wnode 侧 obj_save_or_gc 判定入口（另票）；只换 wcol Hash 计数输入源。
- 与已归档 task/done/tiered-field-ttl-accounting.md（分层态 MetaValue 抵扣标量）同口径，禁并存两套「存活计数」。

验收
- 复杂度探针或断言：全字段 HEXPIRE 的 65536 条 hash 连续 HSET/HLEN 无 O(K) 增长。
- HLEN 精度与 C# 对照逐值一致。
- cargo check --workspace --all-targets 绿。

---
甄别与落地方案（2026-09-19，已核实主仓 dev HEAD 与 garnet C#）

事实核实
1. 方向 1 成立：HashObject::count()（wcol/src/hash/hash_object.rs:563）在册分支
   times.keys().filter(is_expired).count() 为 O(带 TTL 字段数)；调用面封闭于
   hash_object_impl.rs 5 处（107 HGETALL / 140 HLEN / 182、227 HRANDFIELD /
   315 HKEYS·HVALS），全在 &mut self 方法内。C# HashObjectImpl.cs:95
   HashLength → HashObject.cs:510 Count() 同为 O(K) 只读，系忠实转写，
   属 doc/zh/collection.md §6「计数命令严格 O(1)」与 transpile SKILL
   「HLEN 直读内存对象计数 O(1)」自研规约未兑现。
2. 方向 2 失实（无需改动）：should_promote/should_demote（wcol/src/types/
   garnet_object.rs:65-73）取的是 trait IGarnetObject::count，对 Hash 实现
   （garnet_object.rs:118-120）即 self.hash.len() O(1)；wnode 调用点
   （resp/objects/rmw_helpers.rs:377、597，object_store_utils.rs:687）均为
   泛型/trait 上下文，写热路径不存在 O(K) 扫描。票据「现状」一节对此描述有误。
3. 方案 a（expired_pending 标量）不可行：set_expiration 对过去刻度走
   remove + KeyAlreadyExpired 分支（hash_object.rs），在册已过期态只能由
   时间流逝形成，set 时点无法预判，无扫描标量不可维护，弃。

落地方案（方案 b，单一存活计数口径）
1. hash_object.rs count(&self) → count(&mut self)：先 delete_expired_items()
   （堆序 purge：摊还 O(弹过量)，稳态堆顶 peek 短路 O(1)），后直读 hash.len()。
   应答值与 C# Count() 逐值一致（两侧均剔已过期项；C# XX/GT 幻影项缺陷 rust
   已有意不复刻，见 set_expiration 注释）；物理剔除经既有 mutated_by_ttl
   写回升格闭环，防已剔除字段重装载复活（与 C# HTTL 读路径 DeleteExpiredItems
   经 checkpoint 落盘同理）。文档注释写明刻意差异声明。
2. HGETALL / HKEYS / HVALS / HRANDFIELD 的 count() 调用点零改动：purge 后
   len 即存活数，随后的写时过滤循环恒放行，语义与 C# 逐值一致。
3. doc/zh/collection.md §6 第 3 点措辞由「旁路过期计数抵扣」（未落地的标量
   设想）修正为实际机制「堆序惰性剔除 + 直读 len」。
4. 测试：wcol/tests/object_serialize_expiration_tests.rs 追加 count 断言
   （复用 insert_expiration 造态单点）：精度（存活+过期混合 count 值与物理
   剔除后 len 一致）与短路（全字段未来 TTL 时 count 不剔除、结构无损）。

验收：cargo check --workspace --all-targets 绿（本流程约束仅跑 cargo check）。
