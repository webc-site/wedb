优先级：低

问题
wcol 内存对象 count 同名双口径：内建 count(&mut self) 先 delete_expired_items 自毁式清过期再 len，trait IGarnetObject::count(&self) 为原始 len 只读。两口径同名不同义：&mut 语境下方法解析优先命中内建版，调用方以为只读实则改对象（C# Count() 是纯读扣除，不物理删）；升阶链路用 raw len、命令链路触发 purge 版，误用面随调用点增长。

取证（dev 当下代码重取）
wedb/wcol/src/hash/hash_object.rs:571-574 与 wedb/wcol/src/zset/sorted_set_object.rs:571-574 内建 `pub fn count(&mut self)`（delete_expired_items 后 len）。wedb/wcol/src/types/garnet_object.rs:118/:190/:251/:317 trait `fn count(&self)` 原始 len。&mut 消费面示例：wedb/wnode/src/resp/objects/sorted_set_commands/slow.rs:865-868 ZPOPMIN 判空与 min 钳制（&mut obj 命中内建版）。

C# 对标
garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:606-618 Count()（HasExpirableItems 短路 raw Count，否则遍历过期字典只读扣除，零物理删除）；garnet/libs/server/Objects/Hash/HashObject.cs:Count 同口径。

修法建议
内建口径二选一：对齐 C# 改只读扣除（count = len - 过期数，零删改），或更名 purge_expired_len 显式自毁语义；trait count 保持只读。命令面计数已走 O(1) 快道（count_of_blob / MetaValue.size），本票只收口径与命名，不动快道。来源 next/muse.my.md 条 4。

裁决（2026-09-19 实现）
采纳方案二：purge 版保留语义、改名消歧为 purge_expired_len；不采纳「对齐 C# 只读扣除」。

依据
1. doc/zh/collection.md §6 已立规约：带字段级 TTL 集合计数前先走堆序惰性剔除（摊还 O(弹过量)，稳态 peek 短路）再直读标量，并点名 wcol 单点。C# Count() 逐键过滤 expirationTimes 为 O(带 TTL 项数)，违反本仓 O(1) 计数规约（SKILL 最高准绳），代码注释已作刻意差异声明，不应回退对齐。
2. rust 信封无常驻对象层、无 C# DeleteExpiredItemsWorker 后台线程；物理剔除唯一落地路径即读路径 purge + mutated_by_ttl 升格写回（wnode hash_commands.rs should_write_back / sorted_set_commands/mod.rs 同范式）。若改只读扣除，剔除永不落盘（重装载虽仍过期但永占内存），空集墓碑与树文件释放（严格删空自愈）亦无从触发——purge 版是 SKILL 严格删空设计的必要组成。
3. 重取取证（dev 523b234）：全仓 21 处调用点全部需要「扣除过期」口径（raw len 会返回错值），无一处需要对外 raw len；trait IGarnetObject::count 仅 should_promote/should_demote 升阶判定使用。同名双口径的风险面（&mut 语境方法解析优先命中 purge 版）以显式命名根除。

落地
- wedb/wcol/src/hash/hash_object.rs、zset/sorted_set_object.rs：内建 count(&mut self) → purge_expired_len，保留 C# Count 映射注释与消歧说明
- trait count(&self) 保持只读，garnet_object.rs 文档注明两口径分工
- 调用点 21 处随改（wcol impl 9、wcol 测试 3、wnode 9）
- doc/zh/collection.md §6 同步新名
- 无 C# 映射函数删除，无需配置 js/check/ignore
- 验证：worktree 与主仓 cargo check --workspace 通过（按票流程未跑 test.sh/clippy.sh，主代理负责）
