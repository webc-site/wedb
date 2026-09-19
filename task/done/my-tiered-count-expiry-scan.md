优先级：中

问题
分层集合带字段级 TTL 且当前时间越过元记录 next_expiry 水位时，HLEN/ZCARD 由承诺的 O(1) 骤降为 O(N) 全树扫描。obj_length_sync/obj_length_async 水位命中返回 Degrade，慢路径 Hlen/Zcard 臂调 collect_expired_members：自树头无界 scan_with_count_callback(&[0u8], usize::MAX, ...) 全树扫，所有到期键压入无界 Vec 再批量删。千万级集合一条瞬时计数命令即全树扫盘 + 批量删除，同时该扫描面在墓碑连跑上有栈溢出残余风险（与 next/tiered-ttl-tombstone-residual-source.md 同漏斗）。模块头注:30-35 已把「是否将 TTL 面收敛到重灌」列为待裁决项，但 O(1) 计数契约的违背本身需要独立定案：重灌收敛会把水位命中那次计数显式 O(N) 化，契约缺口仍在。

取证（dev 当下代码重取）
wedb/wnode/src/resp/objects/object_store_utils.rs:413-416 obj_length_sync（now_ticks() >= meta.next_expiry 即 ObjLoad::Degrade）与 :476-479 obj_length_async 同型。wedb/wnode/src/resp/objects/tiered_collection_ops.rs:794-803 Hlen 臂、:1569-1578 Zcard 臂调 collect_expired_members；:423-451 collect_expired_members 无界全扫 + expired_keys Vec 无界收集。计数执行体 :2068-2093 exec_tiered_collect 同内核（HCOLLECT/周期收集共用）。

C# 对标
garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:606-618 Count()（内存字典 Count 减过期计数，只读零 I/O）；garnet/libs/server/Objects/Hash/HashObject.cs:Count 同口径。SKILL「O(1) 复杂度计数规约：HLEN/SCARD/ZCARD/LLEN 直读计数」与 doc/zh/collection.md 计数规约第 3 条。

修法建议
三选一须显式裁决：(a) 到期辅助索引（树内到期刻度有序第二索引或 MetaValue 摘要）使水位命中后有界出账；(b) collect_expired_members 单批限截断（每轮固定批量，next_expiry 推进承接剩余），把瞬时 O(N) 化为分摊有界；(c) 维持现状但在 collection.md 计数规约改口径为「水位命中首个计数 O(N)」。与 next/tiered-ttl-tombstone-residual-source.md 共享调用面（member_expire 三核 + collect_expired_members），两票须同一次裁决定写形，禁各改一次。
