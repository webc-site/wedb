# dbsize-keys-single-pass-scan

来源 next/zcode-r3-perf.md 问题 2（认领棒：zcode-r3-perf / zcode-r3-txn 甄别票）。

问题一句话
DBSIZE / KEYS / 槽位删除三命令共用 string_keys_snapshot 做全库键名物化（逐记录 to_vec + HashMap 去重 + 二次过滤 + sort_unstable），而 C# 三者都是单趟扫描内判定的零分配迭代器。

rust 现状
- wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:426 string_keys_snapshot：全日志扫描，每条命中记录 user_key.to_vec() 塞 GxHashMap 去重（每键至少一次堆分配），再 into_iter 过滤 probe_ttl collect，再 sort_unstable，O(n) 分配 + O(n log n) 排序。
- 同文件 :407 db_size：只需要一个计数，却先做全量物化再取 len()；入口 wedb/wnode/src/resp/garnet_api/slow.rs:311 C::Dbsize。
- 同文件 :381 db_keys：先物化全部键名、后才做 glob 过滤，未命中键也各付一次 to_vec。
- 同文件 :255 delete_slot_keys：先全量快照再逐键 await delete_string（消费面 wedb/wedb/src/server/cluster_session/slot_mgmt.rs:177）。
- 另有口径分叉一层：string_keys_snapshot 的去重是「扫描序最后写胜」map 折叠，而 SCAN / COUNTKEYSINSLOT / GETKEYSINSLOT 走 live_key_at（:70）的「链首 + 对侧域新者胜」判定，同一片键空间在命令族之间并存两套存活判定。

C# 证据（逐字核对 /Users/z/git/db/wedb/garnet）
- libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DbSize：UnifiedStoreGetDBSize.Reader 在扫描回调内 ++info.count，零分配零排序。
- 同文件 DBKeys：pattern 指针注入 UnifiedStoreGetDBKeys.Initialize，glob 匹配在扫描回调内完成，只收命中键。
- 同文件 DeleteSlotKeys 与内嵌类 DeleteSlotKeysScan.Reader：扫描回调内直接 storageSession.DELETE，无任何快照中转；注意其口径明言「不过滤过期」（expired-but-not-yet-tombstoned 也删）。
- 同文件 KeyspaceStats：C# 注释明言其键计数与 DbSize 同一非过期判据；rust 已有同族引擎侧单趟内核 wedb/wkv/src/store/keyspace.rs:474 WedbStore::keyspace_stats（INFO KEYSPACE 单一消费点 slow.rs:450），活键判定与 EXISTS 口径同源。

修法
1. db_size 改单趟计数扫描：复用 keyspace_stats 内核（取其当前会话 (ns, db) 行）或以 live_key_at 判定单点重写扫描主体，二者取一，键空间活键判定自此只许一处定义；slow.rs:C::Dbsize 的 vector 登记表域内合并计数臂原样保留。
2. db_keys：glob 判定与收集下沉扫描回调内（一致读逐键预检 ctx.with_consistent_read 原样保留在回调内），未命中键零分配。
3. delete_slot_keys：仿 C# 扫描内删除，热路径经同步删除臂就地删，禁预物化全库键名；顺带对齐 C#「过期未清也删」口径（现快照反而跳过 Due 键，留下槽位退役后的孤儿域）。
4. 三臂统一判定单点后 string_keys_snapshot 应零消费者，整体删除，不留第二套遍历。
5. 排序一并去掉：C# 三命令均无排序口径（Redis 不承诺 KEYS/SCAN 输出序）；若有测试依赖字节序，改测试为集合比较，不得保留 O(n log n) 排序迁就测试。

验收（修复前必须能红的判据）
- 新增集成测试（wnode crate tests/，用计数 global allocator）：预置一万键键空间，DBSIZE 全程累计堆分配为常数级上限（修复前为 O(n) 必红）；KEYS 用仅命中 1 键的 pattern，分配与命中数同阶而非全库（修复前线性必红）。
- 同键空间下断言口径合一：DBSIZE == COUNTKEYSINSLOT(当前库槽) == 全量 SCAN 去重计数 == 修复前响应集合（逐字节回归防退化）。
- CLUSTER 槽删除用例：过期未清键随槽退役一并消失。
- ./test.sh 与 ./clippy.sh 全绿。

为什么这不是自造优化
本条不触碰信封整值重编码等已裁决架构面，方向是向 C# 逐函数形状（DbSize 扫描内计数、DBKeys 扫描内过滤、DeleteSlotKeysScan 扫描内删除）回归，消除的是对 C# 的复杂度阶偏离（换一个 len() 却付 O(n) 分配 + O(n log n) 排序）与仓内并存的三套键空间遍历判定（string_keys_snapshot / live_key_at / keyspace_stats）。属规程最高优先级的重复逻辑与多套架构并存收口，且 SKILL 读路径零拷贝准则（「避免分配」「能不 collect，就避免 collect」）明文要求。
