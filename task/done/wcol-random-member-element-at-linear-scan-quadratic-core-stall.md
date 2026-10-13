终态：合入 cc38072，hash/zset/set 随机成员族带 count 各臂改 n 长借用视图 sink 内 O(1) 直取、SPOP count 臂互异下标一次产出统一剔除，O(k·n) 线性放大收口，zset element_at 零消费删除

审核结论：通过

审核核实：
1. 真实性成立：wedb/wcol/src/hash/hash_object.rs:564 element_at、wedb/wcol/src/zset/sorted_set_object.rs:715 element_at 均为 HashMap iter().nth；wedb/wcol/src/set/set_object_impl.rs set_pop count 臂（每弹一次 nth）与 set_random_member 正负两臂（sink 内 nth）逐下标线性定位属实，hash_random_field / sorted_set_random_member sink 内逐下标调用 element_at 属实。容器为 std HashMap/HashSet（wbase::map 别名，GxBuildHasher），nth 无随机访问特化。
2. 信封态 n 上限：doc/zh/collection.md 升阶门为条目 65536 或体积 4MB 任一触发（OR），6 万成员需单成员均摊约 66 字节以内，短成员（如 8-16 字节 field/member）完全可达，6 万假设成立，危害不因分层门限消解。
3. k 上限：wedb/wnode/src/resp/objects/object_store_utils.rs:MAX_RANDOM_MEMBER_COUNT = i32::MAX >> 2（2^29-1），HRANDFIELD/ZRANDMEMBER/SRANDMEMBER 快慢路径同口放行，负 count 臂 k 与 n 脱钩属实。票内引用的 task/done/wcol-random-family-negative-count-reply-unbounded-process-oom.md 现仓库已无此文件，以代码常量与 wedb/wcol/src/types/random_utils.rs:pick_k_random_indexes 注释「禁止任何按 k 的预分配」为准，结论不变。本票只治「每元素 O(n) 定位」计算放大，与应答体积面不重叠，非重复立案。
4. C# 对照：garnet HashObject.cs:661 ElementAt（无 TTL 时 hash.ElementAt）、SortedSetObject.cs:793 同形、SetObjectImpl.cs:138/153/194/206/233 Set.ElementAt，Dictionary/HashSet 均不实现 IList，Enumerable.ElementAt 为 O(index)，C# 同形。C# 失败面为单请求线程，rust compio thread-per-core 下放大为整核停摆（同核全部连接、复制、阻塞族唤醒），属运行时模型差异引发的工程缺陷，予以立项。
5. 已裁决核查：task/done、task/reject、task/todo 无同题票；doc/zh 无 ElementAt/element_at 偏差登记。
6. 方案评审：n 长借用视图合理（n 受升阶门约束、与对象本体同阶，不随 k 增长；构建代价 O(n) 等于原单次 element_at，任何 k 下不劣于现状）。否决两个替代：排序下标单趟扫描会改变应答序且负 count 臂 k 可达 5 亿、排序下标 O(k log k) 且需按 k 分配，违反禁按 k 预分配约束；蓄水池单趟只适用 distinct 正 count，负 count 放回臂无法覆盖，引入双机制。视图元素改为条目引用 (&K, &V) 而非胖切片元组，单元素 16 字节（set 为 8 字节），65535 条上限约 1MB。SPOP 不用 retain/extract_if 按下标标记删除（依赖 std 迭代序与 retain 序一致，非文档契约）。

优化后执行方案（供 task/fix.md 直接消费，替代下方原「精炼执行方案」）：
1. wedb/wcol/src/hash/hash_object_impl.rs:hash_random_field included_count 臂：purge_expired_len 之后、pick_k_random_indexes 之前 let view: Vec<_> = self.hash.iter().collect()（Vec<(&Arc<[u8]>, &Vec<u8>)>），sink 内 view.get(index) 取条目，写帧不变；无 count 单枚臂保留 element_at。
2. wedb/wcol/src/zset/sorted_set_object_impl.rs:sorted_set_random_member：同法 self.sorted_set_dict.iter().collect() 视图（Vec<(&Arc<[u8]>, &f64)>），sink 内 view.get(idx) 直取；改后 SortedSetObject::element_at 零消费，按零死代码纪律删除 wedb/wcol/src/zset/sorted_set_object.rs:element_at 及其文档引用。
3. wedb/wcol/src/set/set_object_impl.rs:set_random_member 正负两臂：在 pick_k_random_indexes 前 self.set.iter().collect::<Vec<_>>() 视图，sink 内 view.get(index) 直取；NO_COUNT 臂维持 nth 原样。
4. wedb/wcol/src/set/set_object_impl.rs:set_pop count 臂：view 同上，pick_k_random_indexes(n, count_parameter, fastrand::i32(..), true, sink) 一次产出互异下标；sink 内写 bulk 帧并 to_vec 收集待删成员（k 已钳 min(count, n)，分配受 n 约束，与现状逐枚 cloned 同量）；释放视图后统一 self.set.remove + self.update_size(item, false)；result1 口径（恒为 count）不变。NO_COUNT 臂维持原样。
5. 测试闭环（wedb/wcol/tests/random_member_sampling.rs、wedb/wcol/tests/set_pop.rs 追加，不另起文件）：
   a. 6 万短成员 Hash/Set/ZSet，HRANDFIELD/SRANDMEMBER/ZRANDMEMBER count=-200000：条数恒等 200000、全部成员属集合；耗时宽松上界断言（debug 下 5 秒，旧实现约 6x10^9 迭代步必超时）。
   b. 同集合正 count=60000：条数等于 n、互异、全属集合（覆盖洗牌臂）；正 count=100（覆盖拒绝采样臂）互异全属。
   c. SPOP count=30000：弹出数 30000、互异、弹出成员不再存在、剩余基数 30000、heap_memory_size 与逐枚 update_size 基线一致；SPOP count 大于 n 时清空且 heap_memory_size 回到容器基线。
   d. 既有 random_member_sampling.rs、set_pop.rs、random_utils_tests.rs 全绿；./sh/clippy.sh 零告警，./test.sh 通过。

随机成员族（HRANDFIELD / SRANDMEMBER / ZRANDMEMBER / SPOP count）信封臂逐下标 iter().nth 线性定位，单命令 O(k·n) 计算放大独占 thread-per-core 工作核

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 三族同为「先取下标、再逐下标 ElementAt」：HashObject.cs:661-679 ElementAt 对 Dictionary 走 Enumerable.ElementAt（Dictionary 未实现 IList，O(index) 枚举），SortedSetObject.cs:793-809 同形，SetObjectImpl.cs:SetPop/SetRandomMember 对 HashSet 调 Set.ElementAt 同为 O(index)。应答契约只约定成员集合与条数（distinct 正 count 互异、负 count 可重复、SPOP 弹出即删），不约定定位算法；单次取样复杂度不属协议面。C# 失败面为单连接线程被拖住，同进程其他连接由线程池继续服务。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
信封态对象层 1:1 照搬了「逐下标线性定位」：
wedb/wcol/src/hash/hash_object.rs:564-570 element_at 为 self.hash.iter().nth(index)，hashbrown 迭代器 nth 无特化，逐槽 next，O(index)。
wedb/wcol/src/hash/hash_object_impl.rs:235-245 hash_random_field 的 pick_k_random_indexes sink 内每个下标调一次 element_at。
wedb/wcol/src/zset/sorted_set_object.rs:715-721 element_at 为 sorted_set_dict.iter().nth(index)，sorted_set_object_impl.rs:909-927 sorted_set_random_member sink 内逐下标调用。
wedb/wcol/src/set/set_object_impl.rs:199-205、229-235 set_random_member 两臂 sink 内 self.set.iter().nth(index)；:126-139 set_pop count 臂每弹一枚 self.set.iter().nth(index) 再 remove。
k 个下标各付 O(n/2) 平均扫描，总代价 O(k·n)。量级（信封态 n 上限受升阶门 TIERED_PROMOTE_THRESHOLD=65536 约束，n 取 65535）：
HRANDFIELD k 65535 / SRANDMEMBER k 65535（正 count 互异，k 钳至 n）约 2.1x10^9 次迭代步；SPOP k 65535 约 1.1x10^9 步。
负 count 放回臂 k 与基数脱钩，上限经 task/done/wcol-random-family-negative-count-reply-unbounded-process-oom.md 裁定为 |count| 至 2^29-1 合法放行：HRANDFIELD k -1000000 即约 3.3x10^10 步，取上限 -536870912 约 1.7x10^13 步，单命令 CPU 时长从秒级到小时级。
该放大与应答体积无关：上述 OOM 票只收口了应答字节面（体积 O(k) 属协议必需），未触及「每条应答元素额外付 O(n) 定位」这一与协议无关的纯计算放大，非重复立案。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
本仓运行时为 compio thread-per-core：信封 RMW/读臂在会话所在核上同步执行，单命令执行期间该核调度器不迭代，钉在同核的全部连接（含复制、心跳、阻塞族唤醒）一并停摆。单客户端对一个 6 万成员的普通 Hash/Set/ZSet 发一条合法 HRANDFIELD/SRANDMEMBER 负 count 命令即可把一个工作核拖住数十秒以上，属纯参数放大的核级可用性 DoS；失败面由 C# 的单连接级放大为 rust 的整核级。违背板块 3.1「单次遍历收敛」、rust_review「时间复杂度尽量最优、能提前终止避免全遍历」与板块 3.2「消除无界开销」。

涉及代码：
rust 文件与函数：
wedb/wcol/src/hash/hash_object.rs:HashObject::element_at
wedb/wcol/src/hash/hash_object_impl.rs:HashObject::hash_random_field
wedb/wcol/src/zset/sorted_set_object.rs:SortedSetObject::element_at
wedb/wcol/src/zset/sorted_set_object_impl.rs:SortedSetObject::sorted_set_random_member
wedb/wcol/src/set/set_object_impl.rs:SetObject::set_random_member
wedb/wcol/src/set/set_object_impl.rs:SetObject::set_pop
wedb/wcol/src/types/random_utils.rs:pick_k_random_indexes（下标产出单源，不改）

对应 c# 文件与函数：
garnet/libs/server/Objects/Hash/HashObject.cs:ElementAt
garnet/libs/server/Objects/Hash/HashObjectImpl.cs:HashRandomField
garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:ElementAt
garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRandomMember
garnet/libs/server/Objects/Set/SetObjectImpl.cs:SetRandomMember
garnet/libs/server/Objects/Set/SetObjectImpl.cs:SetPop

精炼执行方案：
1. 带 count 的多下标臂（hash_random_field included_count 臂、sorted_set_random_member、set_random_member 正负两臂）在调用 pick_k_random_indexes 前一次性收集存活条目借用视图 Vec（hash 为 (&[u8], &[u8])、zset 为 (&[u8], f64)、set 为 &[u8]，与容器迭代序同序，故下标语义与原 element_at/nth 逐位等价），sink 内改为 O(1) 下标直取；视图长度为集合基数 n（信封态受升阶门约束、与对象本体同阶），不随客户端 k 增长，不破坏 OOM 票「禁止按 k 预分配」约束。无 count 单枚臂维持 element_at / nth 原样（单次 O(n) 不放大，零额外分配）。
2. set_pop count 臂改为：同样先取借用视图，经 pick_k_random_indexes(n, k, seed, true) 一次产出 k 个互异下标并收集待弹成员（k 已钳 min(count, n)），再统一 remove + update_size + 写帧，消除逐弹一次 nth 的 O(n^2)；fastrand 非种子化语义保持（seed 取 fastrand::i32(..) 现取）。
3. element_at 若改后仅剩单枚臂消费则保留；若零消费按零死代码纪律删除并清理文档引用。
4. 测试验证点：wcol/tests 下新增 6 万成员 Hash/Set/ZSet 上 HRANDFIELD/SRANDMEMBER/ZRANDMEMBER 正负 count 与 SPOP count 的耗时界（如 |count|=10^6 在 debug 下亚秒级完成）与语义断言（正 count 互异且全属集合、负 count 条数恒等 |count|、SPOP 弹出数与剩余基数守恒、heap_memory_size 回归基线）；既有 random_member_sampling.rs、set_pop.rs 回归全绿。
