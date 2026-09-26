# 与 C# Garnet 的有意偏差登记

本文档汇总系统在迁移及重构过程中，与 C# 基座（Garnet）产生的刻意行为偏差。
仅登记业务逻辑边界或可观测行为上的刻意分歧，以避免重复审视。

## 1. 浮点应答与落盘文本
未 1:1 对齐 C# `WriteDouble` 的 15 位逐位截断（`NumUtils.cs:486-490/:122-166`），亦未对齐 `TryFormat G` 路径的大写 E 科学计数窗口与 `-0.0` 表现（`RespWriteUtils.cs:616`）。
保留理由：C# 的截断破坏了 double 的最短往返特性（如 `0.1+0.2` 被存成 `"0.300000000000000"`，后续累加会偏离 IEEE），与 real Redis 亦不一致，强行对齐无兼容性收益。
连带后果：`INCRBYFLOAT` 的落盘文本与应答同串，该偏差会导致双侧同一累加序列自首个不可精确表示值起，数值路径发散。
特值输出词形回指注（工单 zcode-r151c-hincrby 案一；§80 同族第二消费位互引，不重立 §80 已载事实）：±inf 输出词形分叉（rust `format_double` "inf"/"-inf" 对 C# TryFormat G "Infinity"）与严禁回改声明已在 §80 在册（第一消费位 ZSCAN 分值），本节不重复立「词形分叉」事实；本注只钉第二消费位与唯一真新料方向——HINCRBYFLOAT 求和溢出（`HSET k f 1e308; HINCRBYFLOAT k f 1e308` 和逾 DBL_MAX，增量词形恒被输入无穷门拒于外，故此为本族唯一可达的 inf 值写入路径）：**双侧均不设求和结果门**——C# `HashIncrementFloat` 求和（`HashObjectImpl.cs:408`）后直进 TryFormat（:410-412）零复检、:429 落帧，rust 信封态 `hash_increment_float`（`wcol/src/hash/hash_object_impl.rs:451-456`）与分层浮点臂（`wnode/src/resp/objects/tiered_collection_ops/hash.rs` Hincrbyfloat 臂 `new_val = cur_val + incr` 处）求和后均不复检，文本恒走 `format_double` 单源（`wresp/src/resp_memory_writer.rs:24-33`）；真 Redis 的「结果为 ±inf → 回 would-produce-NaN-or-Infinity 错帧且不改值」拒改形双侧皆不复刻——该裁决方向全册零命中在册（本席 grep "would produce"/"结果门" 俱无，勿误判为已在册；错帧文案常量本身系存量无穷门在册文本，与本注的结果门方向两不相干）。裁决方向：rust 不回改 "Infinity" 词形（承 §80），亦不增设求和结果门；首轮应答与落盘恒锁 3 字节 "inf"（HSTRLEN 同 3；记账面 `round_up_ptr(3)=round_up_ptr(8)=8` 同槽不外显，wbase/src/heap.rs:48-49，非实害），次轮起存量 "inf" 过 §2 词形放行落双侧同款的存量无穷门帧收敛。锁面：`resp_hash.rs::hincrbyfloat_sum_overflow_inf_wordform_envelope` 与 `tiered_field_ttl.rs::tiered_hincrbyfloat_sum_overflow_inf_wordform`（双臂逐字节全等）。

## 2. 特值词形文法（NaN/Infinity）
C# dotnet `Utf8Parser` 大小写不敏感地接受 `"nan"` 与 `"infinity"` 全拼。
Rust 侧恒拒 `nan`，对 `infinity` 仅认 `inf`/`+inf`/`-inf`。
后果：C# 会将 `ZADD k nan m` 视为有效输入并把 NaN 落进集合，而 Rust 直接报错 `not a valid float` 拒收，防止数据污染。
在产生 NaN 结果时（如 0 加上 +inf 与 -inf），两侧一致报 `SCORE_NAN`，文案逐字节相同：`ERR resulting score is not a number (NaN)`（C# `CmdStrings.cs:227` `RESP_ERR_GENERIC_SCORE_NAN` 与 rust `wresp/src/cmd_strings.rs:143` 同串常量）。

## 3. LPOS 的 RANK 0 与 COUNT -1
C# `ListObjectImpl.cs:ListPosition` 无零值门防线（`rank==0` 时会落入负向遍历臂甚至静默回 `null`），且无负数防线（`COUNT -1` 会直写数组头导致输出畸形帧）。
Rust 侧修复了该缺陷，明确拦截并返回标准的 `not-an-integer` 错误。
收口注记：本偏差已不存在，系反向失真收口，后续对账勿再据旧文判 rust 为修复型偏离。C# 参考树 `garnet/libs/server/Objects/List/ListObjectImpl.cs:ListPosition` 现具 `rank==0` / `count<0` / `maxlen<0` 三门（:355-371 附近，各回 `RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER`），rust 侧 `wcol/src/list/list_object_impl.rs` 拒绝臂（约 :384-388）与 C# 同帧收敛，双侧无可观测分歧，偏差消除。

## 4. EXPIRE/EXPIREAT 大值钳制（两处同源）

### a) EXPIRE/PEXPIRE 相对大秒数
C# 中 `DateTimeOffset.UtcNow.AddSeconds` 计算绝对过期时间，当越界时（阈值约 2.5e11 秒）会抛出 `ArgumentOutOfRangeException`，走会话异常通道掐断连接。
Rust 侧通过 `saturating_add` 钳制到 `i64::MAX` ticks，并正常返回 `:1`，远端大值不再引发连接断开。（与 b) EXPIREAT/PEXPIREAT 钳制面同源，`wbase/src/convert.rs::expire_after_to_ticks` 单点）
补登（工单 zcode-r143c-hexmatrix 席案一）：字段族相对域秒/毫秒双域 HEXPIRE/HPEXPIRE（`garnet/libs/server/Resp/Objects/HashCommands.cs:627-628`）与 zset 域 ZEXPIRE/ZPEXPIRE（`garnet/libs/server/Resp/Objects/SortedSetCommands.cs:1796-1797`）同款走 `DateTimeOffset.UtcNow.AddSeconds/AddMilliseconds`，超阈（秒域约 2.5e11、毫秒域约 2.5e14）抛 `ArgumentOutOfRangeException` 掐连——同款分叉并归本条裁决：rust 同换算单点饱和钳（`wbase/src/convert.rs:158-169` `expire_after_to_ticks`/`expire_after_ms_to_ticks`）钳后正常应答 `*N :1` 且连接存活；对账席遇「C# 掐连对 rust 回 :1」直引本条判有意偏差，严禁回改饱和算式（禁以「对齐 C#」为名复刻掐连）。zset 位仅随本条登记句指涉，不另立案。

### b) EXPIREAT/PEXPIREAT 绝对 Unix 时间戳回绕
工单 doc-deviations-wording-and-attribution-five 补登（r121-triage-devbatch 席三订正点二）。
C# 一手形态：`garnet/libs/common/ConvertUtils.cs:58-61/:68-71` `UnixTimestampInSecondsToTicks` / `UnixTimestampInMillisecondsToTicks` 为 unchecked 乘加（`unixTimestamp * TicksPerSecond + _unixEpochTicks`，全仓无 CheckForOverflowUnderflow），超大 Unix 时间戳静默回绕成负 ticks 或过去时刻——无异常、无错误帧，回绕垃圾值直接写进 TTL 账本按已过期处置。调用面 `KeyAdminCommands.cs:425-426`（EXPIREAT/PEXPIREAT）与 `UnifiedStoreOps.cs:196-197`、`HashCommands.cs:629-630`、`SortedSetCommands.cs:1798-1799`（HEXPIREAT/ZEXPIREAT）同式。
Rust 侧裁决：`wedb/wbase/src/convert.rs` 的 `expire_at_seconds_to_ticks`（:177-179）/`expire_at_milliseconds_to_ticks`（:187-191）以 `clamp(0, MAX_UNIX_TIMESTAMP_SECONDS/MILLISECONDS)` 确定性钳制——负值夹 0（Unix 纪元），越界钳至最大可表示 ticks，永不回绕。属崩溃/错值防御型分叉，与 a) 同族：a) 防越界掐连，b) 防回绕错值。
后果与严禁回改：C# 面 `EXPIREAT k 9223372036854775807` 落回绕 TTL，rust 面落确定性极大 ticks；严禁以「对齐 C#」为名撤钳制恢复 unchecked 回绕。锁面：`wedb/wbase/src/convert.rs` 单测 `expire_at_clamps` / `cap_constants_match_clamp`（约 :296-345）钉死负值夹 0 与 cap 不动点。

## 5. ZCOUNT/ZRANGEBYSCORE 空串边界
当参数值为空串时，C# 侧因无长度校验硬读首字节导致 `IndexOutOfRangeException` 进而掐断连接。
Rust 侧因安全访问首字节会自然回落解析失败，最终回发 `min or max is not a float` 错误帧。属于崩溃防御机制。

## 6. PUBSUB 分片域独立路由隔离

C# `SubscribeBroker` 仅两张表（`subscriptions` + `patternSubscriptions`），`SSUBSCRIBE` 复用普通频道表，`SPUBLISH` 走 `Broadcast` 同时命中普通与模式订阅者。
Rust 侧三表分离（普通频道 / 模式 / 分片），域间互不投递，是向 Redis 标准语义的有意偏差。
后果：`SPUBLISH` 仅投递给分片订阅者，不触发普通或模式订阅回调。

## 7. SUNSUBSCRIBE 命令补齐

C# Garnet 未实现 `SUNSUBSCRIBE` 命令。
Rust 侧补齐了 Redis 7.0+ 标准的 `SUNSUBSCRIBE` 命令支持。

## 8. PUBSUB 命名空间前缀隔离键折叠

C# `SubscribeBroker` 无命名空间概念，通道名裸存。
Rust 侧在 `PUBLISH` / `SUBSCRIBE` 等操作前以 `ChannelNsPrefix` 折叠命名空间隔离键，租户分区随键贯通。
后果：多租户场景通道天然隔离，但通道名在 broker 内含前缀，`PUBSUB CHANNELS` 等命令返回时需剥回裸名。
附注：换域残留安全丢弃兜底验收见 `wedb/wpubsub/tests/namespace_isolation.rs`（源票 `wpubsub-auth-namespace-leak` 已随 2026-09-22 历史重置灭失，验收即该测试）。

## 9. 哈希分裂期按 begin 过滤死条目（防膨胀）

C# `SplitIndex.cs:SplitChunk` 的地址门只认 HeadAddress：低于 HeadAddress（含低于 BeginAddress 的物理截断死条目）一律走「Insert in both new locations」双写分支，分裂期不滤死条目，事后靠查找路径（`FindTagOrFreeInternal`，rust 同款已落地于 `windex/src/find.rs` 的 `classify_slot` 截断 CAS 置零清退）惰性清退兜底。`TraceBackForOtherChainStart` 同样只门 HeadAddress，低于头即 break 并把该低于头的锚地址返回插入另一子桶。
Rust 侧在分裂迁移（`windex/src/split.rs` 的 `split_single_bucket` / `split_chunk` 与 `wkv/src/store/resize.rs` 的 `trace_back_for_other_chain_start`）额外按 begin_address 过滤：地址低于 begin 的死条目与死锚点直接跳过，不再写入新表两侧子桶。
保留理由：死条目按冷记录双写会在扩容后数量翻倍（1 变 2），挤占新表 7 个数据槽位并无谓诱发溢出桶分配（桶膨胀）。安全性：日志地址单调、prev 链恒指向更旧地址，凡仅经低于 begin 锚点可达的记录其地址必同样低于 begin（早已物理回收）；查找侧对 < begin 条目一律清退、绝不解引用，故分裂期过滤不改变任何活键可达性，仅省新表槽位与溢出桶分配。
后果：本偏差系刻意留存，勿在代码注释中改写为「对标 C# SplitChunk 门控」；扩容后残留于旧表的死条目由查找路径清退与全量迁移重建照常收敛。

## 10. HSCAN/ZSCAN 游标尾判定 `==` 放宽为 `>=`（防原游标死锁）

C# `HashObject.cs:Scan`（尾段 `if (cursor + expiredKeysCount == hash.Count) cursor = 0;`）与 `SortedSetObject.cs:Scan` 同型判定存在固有死锁死角：HSCAN/ZSCAN 为纯只读路径（`Storage/Session/ObjectStore/Common.cs:ReadObjectStoreOperation` 直入，绝不调用 `DeleteExpiredItems`），到期成员持续滞留并垫高 `Count`。当存活数 L < 起始游标 start <= 含到期总数 N 时，全部存活条目下标恒 < start 被跳过、无产出，`cursor` 停在 `start`，尾判定退化为 `start == L`（恒假），游标无法归零 → 向客户端返回原游标 `([], start)`，标准客户端持非零游标无限轮询挂死，耗尽连接与 CPU。本仓分层态 `exec_tiered_scan` 早已放宽为 `>=` 收敛，反使内存态与分层态对同一数据集同游标给出死锁 / 终止截然相反的应答，破坏多路径行为同构。
Rust 侧将该尾判定由 `==` 放宽为 `>=`，并抽出单点判定函数 `wcol::types::scan_input::scan_converge_cursor(cursor, expired_keys_count, total)`，内存态 hash/zset 与分层态三处复用，强制双态收敛同口径。此为上游继承缺陷的修复，非架构改良。
数学完备性：正常未截断遍历恒有 `cursor + expired == total`（两判等价，收敛一致）；分页 COUNT 截断有 `cursor + expired ≤ total`（严格小于时两判均不命中，维持续页游标；截断恰落集尾取等时两判等价命中、游标归零收敛）；唯 `L < start <= N` 死锁死角下 `>=` 补足归零。成员级到期数 E 恒 0 的 Set 域（`set_object.rs:Scan`，无成员 TTL，`cursor == len` 与 `cursor >= len` 数学等价）不受影响、不改动。
后果：仅消除死锁死角，正常与截断路径的应答逐值不变。

## 11. TCP 端口复用（SO_REUSEPORT）与多 Worker 架构偏差

C# `GarnetServerTcp.cs:111-114` 采用单 Listener 集中 Accept + 线程池分发模型：全局仅一个 `listenSocket`，绑定后统一 accept 再交线程池处理，天然不需要端口复用；并在 Unix 下显式 `SetRawSocketOption(SO_REUSEPORT, 0)` 关闭该选项（.NET 的 `ReuseAddress` 在 Unix 上会同时设置 SO_REUSEADDR 与 SO_REUSEPORT，保留地址复用利重启，但不允许两个活实例分抢同一端口），以此防止误启第二实例静默分流。
Rust 侧为 compio thread-per-core 架构，每个 CPU 核心一个 worker 线程，各 worker 独立 bind + listen 同一端口（`bind_reuseport`，`server.rs:start_tcp_workers`）。此模型下 SO_REUSEPORT 是内核按连接四元组哈希将新连接分流到各 worker 监听套接字的唯一机制，属刚性架构前提：若对标 C# 关闭该选项，除 worker 0 外其余 worker 启动即报 `EADDRINUSE` 崩溃。故此处与 C# 作出相反裁决——C# 关、Rust 必须开，非疏漏而是架构底层差异。
安全边界：所谓"任意第三方进程可静默抢流量"系对内核机制的误读。Linux 自 3.9 引入 SO_REUSEPORT 起即强制要求复用同端口套接字的进程具有相同 effective UID，异 UID 进程无法复用抢入，端口面天然隔离。
残留风险与定性：同机同 UID 误启第二个 wedb 实例会与本实例内核级分流，此为该架构选择下可观测的有意行为；防多实例并发双写的正规职责由存储目录排他锁（flock）承担（已知限制：flock 互斥仅在本地文件系统与 Linux NFS+NLM/lockd（>=2.6.37）在位形态成立，nolocks/SMB 及 darwin 网络卷静默退化为本地锁、跨客户端不设防），而非在网络层做双活探测——网络层伪探测存在 TOCTOU 竞态、超时延迟与假阳性，属过度设计与双机制，坚决不引入。代码裁决已固化于 `wedb/wnode/src/net/socket_opt.rs:bind_reuseport` 文档注释，严禁误改关闭。

## 12. SRANDMEMBER 正数 count 采全集抽样（修正原型前 k 退化）

C# 一手形态：`libs/server/Objects/Set/SetObjectImpl.cs:181-187` 的 SRANDMEMBER 正数 count 臂，局部 `count` 取自 `input.arg1`（客户端原始参数），钳制后的 `countParameter` 才是应抽个数 k；它却把 k 当抽样域 n 传入 `PickKRandomIndexes(countParameter, indexes, seed)`（indexes 数组按 k 长在 `:184-185` 经 stackalloc/new 三元配置）——变量命名与 Hash/ZSet 侧同源、实参取错，属原型笔误。
缺陷推演链：`libs/common/RandomUtils.cs:30-45` 先按 `(double)indexes.Length / n < KOverNThreshold(0.1)` 分派，n＝k 时比值恒 1.0 ⇒ 必走 `PickKRandomDistinctIndexesWithShuffle`（`RandomUtils.cs:73-86`），该函数体填 `shuffledIndexes[i] = i`（i ∈ 0..n-1）后整体洗牌、再取整个置换数组回写 ⇒ 返回下标恒为 0..k-1 的一个置换。后果：C# `SRANDMEMBER key <正数>` 无论 seed 为何，应答成员集合永远是 Set 的前 k 个成员（仅顺序随机），与 Redis「从全集不放回抽取 k 个不重复成员」的承诺相悖。
Rust 侧落点：`wcol/src/set/set_object_impl.rs:201` 以 `self.set.len()`（全集基数）作抽样域调 `pick_k_random_indexes`，即刻意采真全集抽样；此系对上游可观测缺陷的修复（同条目 10 的「上游缺陷修复型」先例），非疏漏。随机下标已单源于 `wcol/src/types/random_utils.rs`，Set/Hash/ZSet 三面共用。
连带对照：该缺陷唯 C# Set 面独有——Hash 侧 `HashObjectImpl.cs:135-143` 先 `if (countParameter > count) countParameter = count;` 再以集合基数 `count` 为 n 传入，ZSet 侧 `SortedSetObjectImpl.cs:687` 传 `sortedSetCount`，两面 C# 本就采全集；rust 与之对齐。故本偏差仅落 Set 面，HRANDFIELD/SRANDMEMBER/ZRANDMEMBER 三命令语义在 rust 侧由此归于一致，非新增分叉。
后果：同 seed 下同命令的应答成员集合可与 C# 不同（rust 能命中位置 ≥ k 的成员，C# Set 面永远命不中）。本仓对 collection 的全等承诺限于数据结构与命令语义层，不含对 C# 原型可观测缺陷的逐位复刻。语义锁用例见 `wcol/tests/random_member_sampling.rs`（跨 seed 应答并集严格超出前 k 个成员），严禁把抽样域回改成 C# 形态。

## 13. CLUSTER 命令族防御性纠偏与边界拦截

C# `garnet/libs/cluster/Session/RespClusterSlotManagementCommands.cs` 与 rust `wedb/wedb/src/server/cluster_session/slot_mgmt.rs` 逐点双侧对账后的刻意分歧登记，共六处防御性收紧、一处配置面缺席（g)）与一处顶层拒绝（h)），a 至 h 合计八臂。后续轮次对账直接引用本条，免重查双侧源码，严禁当缺陷回退防御逻辑。
锚形态裁决（r107-devaudit 台账收口）：本条 a) 至 f) 的六个 C# 函数改采纯符号锚（函数名即锚，不再携行号）。原登记行号（:441-487、:536-587、:646-666、:277-296、:359-385、:166-177）随 garnet 参考树版本更新系统性漂移，r107-devaudit 登记该失真、本票执行期现码复测为 :423、:518、:635、:272、:338、:133（漂移幅度按定义行实测 5 至 33 行，DELKEYSINSLOT -5、COUNTKEYSINSLOT -33），符号与裁决语义逐处仍真——行号锚属可再腐面、符号锚不可，故本条 C# 侧统一降级为符号锚；六处函数名逐一点名，杜绝原 e)/f) 只写行号不写名的形态。本条其余锚（rust `slot_mgmt.rs` 各行号、`ClusterKeyIterationFunctions.cs:61-69`、`FailoverCommand.cs:24`/`:35-65`、`ClusterSession.cs:84-130`）现码亲验在位，维持行号锚形态。

a) SETSLOT FAIL 臂：C# `NetworkClusterSetSlot` 状态门仅拒 INVALID/OFFLINE，解析成功的 FAIL 须按形态分说——传统二参形态 `SETSLOT <slot> FAIL`（不带 node-id）先被 node-id 元数校验门拒回 `RESP_SYNTAX_ERROR`，仅三参 `SETSLOT <slot> FAIL <node-id>` 才穿透到 switch default 抛 `InvalidOperationException` 掐断连接（该函数按本条锚形态裁决维持符号锚，两门现码位 RespClusterSlotManagementCommands.cs:458-460/:487 备查）；rust `network_cluster_set_slot`（slot_mgmt.rs:420-428）将 FAIL 一并拒绝，回 `ERR Slot state FAIL not supported.` 错误帧。保留理由：崩溃防御，协议面拒绝优于连接异常。
b) SETSLOTSRANGE OFFLINE/FAIL 臂：C# `NetworkClusterSetSlotsRange` 对解析成功的 OFFLINE/FAIL 落 switch default，回 `ERR Slot state {X} not supported.`（带输入词原样）；rust `network_cluster_set_slots_range`（slot_mgmt.rs:493-498）回 `ERR Invalid slot state`（ERR_GENERIC_SLOT_STATE）。文案级分叉，语义同向拒绝，不逐字对齐。
c) SLOTSTATE 越界：C# `NetworkClusterSlotState` 无 OutOfRange 检查，`(ushort)70000` 截断为 4464 查询并回错误槽位数据；rust `network_cluster_slot_state`（slot_mgmt.rs:710-713）报 `ERR Slot out of range`。保留理由：边界拦截，杜绝静默错值应答。
d) DELKEYSINSLOT 越界：C# `NetworkClusterDelKeysInSlot` 无越界检查，越界槽静默 `+OK`；rust `network_cluster_del_keys_in_slot`（slot_mgmt.rs:676-684）报错拒绝。保留理由：边界拦截，杜绝假成功。
e) GETKEYSINSLOT 负 count：C# `NetworkClusterGetKeysInSlot` 负 keyCount 经 `GetKeysInSlot`（ClusterKeyIterationFunctions.cs:61-69）扫一条记录即停（谓词 `keys.Count < maxKeyCount` 在 maxKeyCount 为负时首条记录后恒假，未必收键、可为 0 键），`Math.Min(keys.Count,-1)=-1`，`TryWriteArrayLength(-1)` 回 RESP null 数组 `*-1`；rust `network_cluster_get_keys_in_slot`（slot_mgmt.rs:642）`key_count.max(0)` 钳 0 回 `*0` 空数组。保留理由：负 count 无 Redis 标准语义，null 数组系上游实现事故，空数组应答更稳。
f) COUNTKEYSINSLOT 存储异常：C# `NetworkClusterCountKeysInSlot` catch 后回 `:-1`（哨兵值）；rust `count_keys_in_slot_slow`（slot_mgmt.rs:141-142）回 RESP_ERR_SLOW_PATH_STORAGE 错误帧。保留理由：`-1` 哨兵与真实计数域重叠，显式错误帧可被客户端区分。
g) COUNTKEYSINSLOT/GETKEYSINSLOT 副本读门：C# 无 `serverOptions.enableReplicaReads` 配置项（garnet 全仓零命中，原登记该配置项系虚构、默认 false 亦失实）；enableReplicaReads 仅为 `ClusterConfig.IsLocal` 的形参（ClusterConfig.cs:174，默认 true），`NetworkClusterCountKeysInSlot`/`NetworkClusterGetKeysInSlot` 两处调用点（RespClusterSlotManagementCommands.cs:160/:373）按默认参调用，即 C# 默认形态副本对其主节点持有的槽判真、直接本地计数/取键。rust `slot_mgmt.rs:588/:635` 显式 `is_local(slot, false)` 恒拒副本读（一律 MOVED 重定向），不提供配置面，并在 :578-584/:625-631 码内注释自陈裁决：C# 此处缺省 true 属笔误面（同参考树 IMPORTING 门与 MigrateCommand 槽门均显式传 false），rust 统一传 false。保留理由：副本读走慢路径存储面在 rust 架构下未开；本处分叉系上游笔误修复型更严侧，并非「行为等价 C# 默认部署形态」，对账时不得将副本管理读形态宣称为天然一致。严禁按虚构配置项或 C# 默认参 true 形态回改 rust 恒拒侧。
h) 顶层 FAILOVER TAKEOVER 拒绝：C# `FailoverCommand.cs`（:24）`TryGetFailoverOption` 把 TAKEOVER 解析为合法枚举逃过 syntax error，switch（:35-65）无 TAKEOVER 分支落 default `throw new Exception("Failover option TAKEOVER not supported")`，`ProcessClusterCommands`（ClusterSession.cs:84-130）无 catch，异常上抛掐断连接——顶层 TAKEOVER 在 C# 是不可用输入；rust `network_failover`（cluster_session/failover.rs）原接受并降格为 FORCE 发起真实停写让渡（Force 跳过同步等待的豁免语义仅存在于从端 CLUSTER FAILOVER 臂），属可用性敏感操作的无声扩权。修复后解析到 TAKEOVER 回 `ERR syntax error` 帧，不逐字复刻 C# 掐连接的异常通道形态。保留理由：对齐 C# 拒绝面（不可用输入），错误帧优于连接异常，与 a) 同向。

## 14. PubSub 订阅邮箱有界水位与满时拒收

C# `SubscribeBroker.Broadcast` 在发布线程同步直调订阅会话 `Publish/PatternPublish`
（`libs/server/PubSub/SubscribeBroker.cs:87/:108`），写入其网络发送器固定尺寸应答缓冲
（`libs/common/Networking/GarnetTcpNetworkSender.cs:120-134`），在途发送超门限
（ThrottleMax=8，同文件 `:47`，`--network-send-throttle` 可调）时 `throttle.Wait()`
阻塞发布线程传导背压、零丢弃（同文件 `:310-330`）——订阅侧待投积压在 C# 天然有界。
Rust 会话为单线程属主、发布线程可恰为对端会话属主线程，阻塞版背压在互订拓扑下成环
即死锁（且广播线程钉持 papaya 守卫），故 `PubSubMailbox` 收口为 crossfire
`flavor::Array` 有界队列（IS_BOUNDED=true）：水位 = `DEFAULT_MAILBOX_CAPACITY=1024`
（宿主经 `with_mailbox_capacity` 调整），发布端 0 锁 0 阻塞，满即拒收丢尾帧
（丢尾不丢头，`try_publish` 返回 false）。
后果：慢订阅者内存积压刚性封顶水位（C# 同封顶于 发送缓冲×在途门限）；差异仅在
满位窗口——C# 阻塞发布端无损，Rust 丢新帧保发布端进度，订阅者排空后即刻恢复收帧。

## 15. wext_json 对 C# JSON 模块四处原型缺陷的修复性偏离

工单 zcode-r24-wextjson 发现八登记。四组均为 rust 侧可观测行为与 C# 原型相反或更强，
保留理由：修复 C# 死门 / 崩溃面 / 非法 JSON 产出，语义对齐 RedisJSON 标准。后续对账
直接引用本条，严禁按 C# 缺陷形态回改。语义锁用例统一在
`wedb/wext_json/tests/json_deviation_locks_tests.rs`。

a) JSON.SET 的 NX/XX 生效面：C# `JsonCommands.cs:JsonSET.Updater` 条件门
`parseState.Count is 4`（:59）恒 false（合法参数 Count 最大 3），`TryGetExistOption`
永不调用，C# 的 NX/XX 从不生效、恒按无条件覆盖写执行。Rust
`wedb/wext_json/src/json_commands/set_get.rs` 与 `json_object.rs:set` 全量实现 NX/XX
（已存在路径 NX 回 `ConditionNotMet`、缺失路径 XX 回 `ConditionNotMet`）。保留理由：
条件写恒失效使 NX/XX 语义反转成无条件覆盖，属数据面静默丢写；RedisJSON 标准即
rust 形态。锁用例：`nx_rejects_existing_path`、`xx_rejects_missing_path`。

b) `$` 根路径的条件写：C# `GarnetJsonObject.cs:Set` 在检查 existOptions 之前对
`pathStr == "$"` 直接替换根返回 Success（NX 也覆盖已存在根，XX 也在空文档上落值）。
Rust `json_object.rs:set` 对 `$` 分支按 existOptions 分流：根已存在时 NX 回
`ConditionNotMet`，空文档上 XX 回 `ConditionNotMet`（空文档 GET $ 两侧同为 RESP nil，
C# `TryGetToWriter` root is null 即 `WriteNull`）。保留理由：同 RedisJSON。
锁用例：`root_nx_rejects_existing_document`、`root_xx_rejects_empty_document`。

c) 引号正则 operand 不崩：C# `QueryExpression.cs:RegexEquals` 对无斜杠包裹的引号串
pattern 取 `LastIndexOf('/')` 得 -1 后 `Substring(1, -2)` 抛
`ArgumentOutOfRangeException` 掐断连接。Rust `json_path/expression.rs:match_tokens`
对运行期字符串按需 `compile_regex`，正常匹配；pattern 非法视为不匹配，不 panic、
不报错帧。保留理由：崩溃防御。锁用例：
`quoted_regex_operand_matches_without_crash`、`invalid_quoted_regex_pattern_does_not_panic`。

d) 多路径 JSON.GET 的键名转义：C# `GarnetJsonObject.cs:TryGet` 多路径分支把 path
原文裸拼为 JSON 键，路径含引号/反斜杠时产出非法 JSON。Rust `json_object.rs:try_get`
以 `sonic_rs::to_vec(&path_str)` 正确转义键名，整帧恒为合法 JSON 且键可逐字符还原为
路径原文。保留理由：非法 JSON 应答击穿客户端解析；转义后键仍可精确对账路径原文。
锁用例：`multi_path_key_escapes_special_chars`。

e) JSON.SET 命中载荷反序列化失败的防御帧（工单 zcode-r28-errframe 增补）：C# 模块
对象（GarnetJSON）装载失败走异常通道掐断连接，无对等错误帧文案。Rust
`json_commands/set_get.rs` 对 `from_slice` 失败回 `-ERR JSON object decode failed`
单行错误帧、连接存活。保留理由：同 c) 崩溃防御。

f) JSON.GET 选项带值收尾无路径形态（工单 wext-json-get-trailing-options-no-path-fork
增补，登记级零代码改动）：C# `JsonCommands.cs:JsonGET.Reader` 选项循环（:131-161）三
选项匹配均附 `offset < parseState.Count` 门，选项与值成对消费后抵达参数尾时越界读回的
空 token 不匹配任何选项名，落入 `offset > parseState.Count` 判定（:151-153）回
`ERR wrong number of arguments for 'json.get' command`——故 `JSON.GET key INDENT "  "`
在 C# 侧回参数错误帧。Rust 侧 `json_commands/set_get.rs:json_get_reader` 选项循环
（:174-186）以 `while let [opt, val, ..]` 切片模式消费完 INDENT+值后自然退出，paths 取
空切片，`json_object.rs:try_get` 零路径臂（:234-243）回全量美化文档 bulk string，正常
成功应答。裁决：路径缺省合法系 RedisJSON 标准形态，本分叉属**修复 C# 参数收口缺陷**
的偏离（C# 形把合法调用拒于门外），登记台账而非留作未申报分叉；**严禁按 C# 缺陷形态
回改**（不得在选项循环尾补「选项消费后参数耗尽且无路径」wrong-num-args 错误臂）。登记
范围严格限定为「选项对完整消费后参数耗尽」形；悬空单选项带（`JSON.GET k INDENT`）rust
切片模式不匹配、break 后按路径处理，与 C# 同落路径错误臂，双侧本就同形，不入本款，
勿据此扩写文案。锁用例：`json_get_trailing_options_no_path_returns_full_pretty_document`
（`wext_json/tests/json_deviation_locks_tests.rs`，命令级 reader 漏斗——INDENT 单选项带、
INDENT+NEWLINE 双选项带两形态锁全量美化文档成功应答，另以 `JSON.GET k INDENT i $.a`
正常路径用例作参照防误伤路径主干）。

臂级澄记（非偏离，防反复疑报）：wext_json 过滤器两处已按 C# 代码原文（而非审查票
枚举文案）1:1 对齐——① `MatchTokens` else 容器臂（`QueryExpression.cs:242-247`）仅
`Exists`/`NotEquals` 回 true，`StrictNotEquals` 在容器侧恒 false（票面文案曾误列其回
true）；② `In` 的右值恒为数组、被容器门挡在比较族 switch 之外，C# `CheckIn` 实际不可
达，`@.a in [1,2]` 在 C# 侧恒空结果集，rust 逐臂同形（`in [1,2]`/`in[1,2]`/`IN` 三形态
同为空）。两处对拍锁见 `wedb/wext_json/tests/json_path_semantic_tests.rs` 的
`container_operands_bypass_comparison_family` 与 `in_operator_needs_no_trailing_space`。

## 16. PFCOUNT 多键尾键缺失回真实并集基数（修正原型恒 0 缺陷）

C# 一手形态：`libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogLength`（:118-174）多键循环中计数变量 `count` 仅在尾键判定 `i == input.parseState.Count - 1` 且该键 GET 命中的两处分支赋值（首键即尾键 :158-165、TryMerge 后尾键 :168-173）；尾键 NOTFOUND 时 :135-137 直接 `continue`，`count` 保持 out 参数初值 0。后果：`PFCOUNT a b`（a 存在、b 缺失）C# 回 `:0` 而非 a 的基数，与 Redis「缺失键按空集并入」语义相悖。C# 测试 `HyperLogLogTests.cs:225-263` 仅覆盖双键俱在，未锁定该缺陷行为。
Rust 侧落点：快路径 `hyper_log_log_length` 与慢路径 `slow_hll_count` 均为逐键装载、命中并入稠密累加器、Missing 跳过、末尾统一 `hll.count(acc)`，任一键（含尾键）缺失时返回全部命中键的真实并集基数，快慢两路内部一致且对齐 Redis 语义。此系对上游可观测缺陷的修复（同第 3/10/12 条「上游缺陷修复型」先例），非疏漏。
后果：`PFCOUNT a b`（b 缺失）C# 恒回 0、Rust 回 card(a)，双侧对账测试在该用例必然发散，勿判为转写缺陷或回改归 0（归 0 才是真回归）。若后续上游修复 C# 该臂，本条按登记撤销处理。语义锁用例见 `wedb/wnode/tests/hyperloglog.rs` 的 `pfcount_trailing_missing_key_returns_real_union_cardinality`。

## 17. PFADD/PFMERGE 对带 TTL 键恒保留 key 级 TTL（不对齐 InPlace 臂 RemoveExpiration）

C# 一手形态：`MainStore/RMWMethods.cs` 的 PFADD InPlace 臂（:657-693，RemoveExpiration 于 :666/:677）与 PFMERGE InPlace 臂（:695-728，:707/:718）在原位更新前显式 `logRecord.RemoveExpiration()` 清除随键 TTL；RemoveExpiration 位于 `IsValidHYLL` 前置门之内（PFADD :663/:674、PFMERGE :704/:715），仅载荷为合法 HLL 时才执行，非法载荷直接置无效 HLL 旗返回、清 TTL 臂根本不可达；而 CopyUpdater 的 PFADD 臂（:1215-1301）与 PFMERGE 臂（:1304-1351）经 `TryCopyOptionals(in srcLogRecord)` 保留 expiration。同一命令在 C# 内部随记录可变性（IsPinnedValue / 值大小）走不同臂即得到相反的 TTL 结局，属 C# 家族自相矛盾；Redis 语义为 PFADD/PFMERGE 保留 TTL。
Rust 侧裁决：PFADD/PFMERGE 快路径写回统一经 `wkv/src/session/raw/write/rmw.rs:try_rmw_sync`（TtlGate::Pass 不触碰 TTL 记录恒保留，仅过期键清退残留 TTL 后按无 TTL 重建，对标 CheckExpiry → ExpireAndResume），慢路径 `rmw_string` 同语义，恒取保留侧且双臂自洽。与 r15-stringbits 发现二（SETBIT/BITFIELD 写臂清 TTL 分叉）系同一 RemoveExpiration 家族的独立分支：SETBIT/BITFIELD 的清 TTL 有 inline 槽位腾挪注释动机，PFADD/PFMERGE 原位更新不变长无空间动机。
后果：真实分叉场景为 `PFADD k e1` 建合法 HLL 键、`EXPIRE k 100` 后再 `PFADD k e2` 或 `PFMERGE k …`——C# InPlace 路径 TTL 丢失键永生、Copy 路径 TTL 保留，Rust 恒保留并在 100 秒后删除（语义锁用例即此序列，见下）；勿按 C# InPlace 臂改回清 TTL。原登记示例 `SET k v EX 100` 后 `PFADD k e` 不可达该分叉：`"v"` 非法 HLL 载荷被 C# `IsValidHYLL` 前置门直接拒 WRONGTYPE（RemoveExpiration 不执行），rust 侧 `load_hll` 同拒非法 HLL 载荷（`hyperloglog/hyper_log_log_commands.rs:531/:548` 等 `RESP_ERR_WRONG_TYPE_HLL` 拒绝臂），双侧该序列同回错型无分叉，严禁再以该序列做本条对账。裁决声明已固化于 `try_rmw_sync` 头注，语义锁用例见 `wedb/wnode/tests/hyperloglog.rs` 的 `pfadd_pfmerge_preserve_key_ttl_until_expiry`。若后续上游修复 C# 该臂，本条按登记撤销处理。

## 18. SETBIT/BITFIELD 写臂增长场景恒保留 key 级 TTL（不对齐 InPlace 增长臂 RemoveExpiration）

C# 一手形态：`MainStore/RMWMethods.cs` 的 SETBIT InPlace 臂（:568-598，RemoveExpiration 于 :581/:595）与 BITFIELD InPlace 臂（:610-638，RemoveExpiration 于 :621/:635）在值长不足容纳目标偏移的增长臂显式 `logRecord.RemoveExpiration()` 清除随键 TTL，注释自认 "Remove Expiration first to free up the space for value growth"——为 inline 槽位腾挪空间的实现副作用；而同文件 SETRANGE/APPEND 的 InPlace 增长臂注释明言 "not changing the presence of ETag or Expiration"（:734-763、:799-834），CopyUpdater 的 SETBIT 臂（:1133-1186）经 `TryCopyOptionals` 保留 expiration 亦不清 TTL。同一命令在 C# 内部随记录可变性（inline 与否）与臂别（InPlace/Copy）得到相反的 TTL 结局且随值大小漂移，家族自相矛盾，属上游缺陷（同第 3/10/12/16/17 条先例）；Redis 语义为写子命令保留 TTL。
Rust 侧裁决：SETBIT/BITFIELD 快路径写回统一经 `wkv/src/session/raw/write/rmw.rs:try_rmw_sync`（`bitmap_commands.rs:network_string_set_bit` 与 `string_bit_field_action` 共用，TtlGate::Pass 不触碰 TTL 记录恒保留，仅过期键清退残留 TTL 后按无 TTL 重建，对标 CheckExpiry → ExpireAndResume），慢路径 `rmw_string` 同语义，恒取保留侧且双臂自洽——与第 17 条 PFADD/PFMERGE 系同一 RemoveExpiration 家族的姊妹分支，此处增长变长有槽位腾挪动机，但 Redis 语义与仓内全家族口径均为保留，故不随 C# 清除。
后果：`SET k v EX 100` 后 `SETBIT k 100 1` / `BITFIELD k SET u8 100 1`，C# InPlace 路径 TTL 丢失键永生、Copy 路径 TTL 保留，Rust 恒保留并在到期后删除；双侧对账测试在该用例必然发散，勿判为转写缺陷，更严禁按 C# InPlace 增长臂改回清 TTL（改回才是真回归）。裁决声明已固化于 `try_rmw_sync` 头注，语义锁用例见 `wedb/wnode/tests/ttl_rmw_semantics.rs` 的 `bitmap_grow_preserves_ttl_until_expiry`。若后续上游修复 C# 该臂，本条按登记撤销处理。

## 19. GEOADD XX 缺失键与 GEO STORE 族空结果不创建空 zset 键（不对齐 InitialUpdater 空对象残留）

C# 一手形态：`Objects/SortedSet/SortedSetObject.cs:Operate`（:452-455）每次操作尾部对空集置 `RemoveKey` 标志，但该标志仅被 `Storage/Functions/ObjectStore/RMWMethods.cs` 的 InPlaceUpdaterWorker（:122）与 PostCopyUpdater（:207）消费触发 ExpireAndStop 删键；InitialUpdater（:44-74）无 `HasRemoveKey` 检查，经 `TrySetValueObjectAndPrepareOptionals` 直接挂载空对象并推版本写 AOF，而 `Objects/Types/GarnetObject.cs:NeedToCreate`（:34-49）对 GEOADD/ZADD 恒返回 true。后果：`GEOADD key XX lon lat member`（键缺失，XX 挡住全部新增）与 GEOSEARCHSTORE / GEORADIUS STORE 族 0 命中（C# `SortedSetGeoOps.cs:GeoSearchStore` 先 `Delete(destination)` 再 RMW ZADD 空成员集）都会创建空 zset 键：EXISTS 为 1，AOF 重放副本同建。
Rust 侧裁决：GEOADD 回写门 `existed || !obj.sorted_set_dict.is_empty()`（`wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:geo_add`，缺失键上仍空不落库）；GEOSEARCHSTORE / GEORADIUS STORE 族空结果同步段 `zset_save_or_gc` 空集合整键回收、慢路径 `store_dest_cold` 删空回收臂，空键一律不创建。恒取与 Redis 一致侧（GEOADD XX 对缺失键回 :0 且键不存在），属对上游可观测缺陷的修复（同第 3/10/12/16 条先例）。
后果：双侧 EXISTS / ZCARD / DEL 应答分叉，依赖键存在性判定的客户端逻辑在两实现间发散；双侧对账测试在该用例必然发散，勿判为转写缺陷，更严禁按 C# InitialUpdater 改回创建空键（改回才是真回归）。语义锁用例见 `wedb/wnode/tests/geo_store_tiered_retire.rs` 的 `geoadd_xx_missing_key_leaves_no_key` 与 `geosearchstore_zero_hit_leaves_no_key`。若后续上游修复 C# 该臂，本条按登记撤销处理。

### 追加澄记：HDEL/HPERSIST/LSET 缺键臂空对象幻键拒挂裁决（工单 zcode-r40-delempty 发现二）
C# 一手形态：`NeedToCreate` 判定矩阵（`garnet/libs/server/Objects/Types/GarnetObject.cs:34-79`）对 HashOperation 的 HDEL/HPERSIST 落默认 `_ => true`（仅 HEXPIRE/HCOLLECT 列 false），对 ListOperation 的 LSET 落默认 true（仅 LPOP/RPOP/LRANGE/LINDEX/LTRIM/LREM/LINSERT/LPUSHX/RPUSHX 列 false）；InitialUpdater（`RMWMethods.cs:44-74`）无 `HasRemoveKey` 检查、经 `TrySetValueObjectAndPrepareOptionals` 直接挂载对象恒返 true（与本条已登 GEO 面完全同型）；且网络臂无缺键预检——`HashDelete`（`HashCommands.cs:392-408`）直入 `storageApi.HashDelete` RMW，HPERSIST（`HashOps.cs:573-576` RMWObjectStoreOperation）、LSET（`ListCommands.cs:813-829` ListSet RMW）同。故 C# 在缺键上执行 `HDEL k f`、`HPERSIST k FIELDS 1 f`、`LSET k 0 v` 会挂载空 Hash/List 对象建幻键：EXISTS 回 1、TYPE 回 hash/list、HLEN 回 0。
Rust 侧裁决：rust 四族 `should_write_back` 统一 `(!existed && empty) -> false` 拦截（`hash_commands/mod.rs:84`、`sorted_set_commands/mod.rs:117`、`set_commands/mod.rs:128`、`list_commands/mod.rs:195`），SMOVE 源缺失臂等装载型手写臂同口径，恒不建空键，EXISTS 恒回 0、TYPE 恒回 none，取 Redis 一致侧，属上游缺陷修复型家族（同第 3/10/12/16 条先例）。
后果：双侧对账测试在这些命令的缺键用例必然发散（C# EXISTS 1 vs rust EXISTS 0），勿判为转写缺陷，严禁按 C# InitialUpdater 形态回改挂空对象（回改即真回归，复活幻键）。语义锁用例见 `wedb/wnode/tests/delempty_parity_locks.rs` 的 `missing_key_hdel_hpersist_lset_leaves_no_key`。


## 20. SCAN 族主存储面三处修复性偏离（COUNT 负零钳制 / TYPE 空串视为空集 / 游标不对齐即终结；附对象族 COUNT 极值翻倍回绕登记 d)）

工单 zcode-r19-scan 发现三登记，同属主存储 SCAN 游标面的客户端可观测应答分歧，三小节合一；后续工单 zcode-r147c-hscanmt 案一追加 d)（对象族 COUNT 极值族，遵本条「后续 SCAN 域对账直接引用本条免重取证」纪律合一不另立条目）。
后续 SCAN 域对账直接引用本条免重取证，严禁当转写缺陷按 C# 缺陷形态回改（改回才是真回归）。

a) COUNT 负零钳 0 + 扫描层 max(1)：C# `NetworkSCAN`（libs/server/Resp/ArrayCommands.cs:291-304）对 COUNT 仅 TryGetLong 校验整数性，零与负值原样传入 DbScan 的 count；底层 ScanLookup（libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorScan.cs:268-271）以 `acceptedCount >= count` 判页满，count<=0 时第一条记录（无论匹配与否）处理后即停页，若该条不匹配则 keys 为空，NetworkSCAN 尾段（ArrayCommands.cs:319-331）对 keys.Count==0 硬编码回游标 0 终止遍历——后续全部匹配键漏掉。该缺陷路径仅在未带 TYPE 时可达：传 TYPE 态时 DbScan 实参经 `ArrayCommands.cs:315-316` 将 count 置 long.MaxValue，负零值不落入扫描层。Rust `wedb/wnode/src/resp/array_commands.rs:parse_scan_filter` 将负/零 COUNT 钳 0，扫描层 `wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:scan_cursor` 再以 `count.max(1)` 保底每页至少扫 1 条。保留理由：向 Redis 语义收敛（COUNT<1 属非法参数），防 COUNT 0 漏键提前终止全遍历。

b) TYPE 空串视为空集：C# DbScan（libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:57-58）以 `!typeObject.IsEmpty` 为门，空串 TYPE（参数存在但长度 0）不进入类型判定，matchType 保持 null，全类型透传，等价于未带 TYPE。Rust `parse_scan_filter` 将空串与未知值统一归 `type_unknown`，慢路径 C::Scan 臂（`wedb/wnode/src/resp/garnet_api/slow.rs`）直接回空列表 + 游标 0。保留理由：空串 TYPE 属客户端请求残缺，显式空集优于静默吞掉过滤条件；与未知类型共臂单径，免双态分叉。边界互引（票 zcode-r161c-scantype，见本册 §141）：多 TYPE 词元末值覆盖形下，空串以其**末值位**出现时（如 `TYPE stream TYPE ""`）仍按本条归空集，两条互指——本条裁「空串作末值时的裁决」，§140 裁「判型取哪个词元的值」。

c) 游标不对齐终结回 (0, 空)：C# 对未落在记录起始字节的游标经 SpanByteScanIterator.SnapCursorToLogicalAddress（libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteScanIterator.cs:72-90）内部 SnapToLogicalAddressBoundary（同文件 :154-176）自页首按 allocatedSize 步进将游标前推到下一记录边界后续扫，遍历不终止。Rust `wedb/whlog/src/scan.rs:HybridLog::validate_cursor` 复用 scan_iter 步进链做纯校验，不对齐一律判 false，由 `scan_cursor` 终结遍历回 (0, 空)。保留理由：Redis SCAN 本无快照保证、漏扫方向可容忍，免为回退对齐重写三区分派逻辑。

d) 对象族 HSCAN/ZSCAN COUNT=i32 下溢极值处 `count * 2` 回绕分叉（工单 zcode-r147c-hscanmt 案一登记，纯文档治理条，零行为改动）：分叉域严格为三条件合取——**HSCAN 或 ZSCAN（成对翻倍族，SSCAN 直比无翻倍不在内）且 COUNT 取 i32 下溢单值 -2147483648 且带 MATCH 且起始游标后首条记录未命中 pattern**。C# 编译面 unchecked（仓库根 Directory.Build.props 全文件无 CheckForOverflowUnderflow 属性，甄别票订正注记：票首「仅两项属性」不实、实为五项通用编译属性，unchecked 结论不变）下 `count * 2` int32 回绕恰为 0（`garnet/libs/server/Objects/Hash/HashObject.cs:380` 翻倍、:418 `items.Count == count` 判停且 break 检查在 match 分支外每记录 cursor++ 后，首条未命中即 items.Count 恒 0 命中回绕后 count；`SortedSetObject.cs:522 items.Count == (count * 2)` 同形态），截断判定退化为 `items.Count == 0`：该极值下 C# 应答为**非零游标 + 空列表**，客户端逐条爬行分页（首条即命中则 emitted 先于判停累加、恒不等于 0，归全量族，故须「首条未命中」合取）。其余负值 [-2147483647, -1073741825] 回绕为正大数、[-1073741824, -1] 回绕/本值为负，恒不命中 → 双侧同为全量遍历，不分叉。Rust 把 count 提升 i64 承载（钳制单点 `wcol/src/types/scan_input.rs:read_scan_input` 仅 `i64::from(c).min(limit)` 单向上钳，负值原样下传，与 C# TryGetInt 同点），翻倍永不回绕：内存态 `hash_object.rs`/`sorted_set_object.rs` 与分层态 `tiered_collection_ops/scan.rs` 对该极值一律**单页全量 + 游标 bulk "0"**（SSCAN 判净臂 SetObject.cs:225-226 直比无翻倍，双侧恒全量同形）。保留理由：其一系 review.md 板块 4.1「算术溢出保护」取向，杜绝按 C# int32 回绕复刻；其二 rust 内存态与分层态据此双态逐字节全等（collection.md 全等承诺），按 C# 形回改 i32 截断回绕将连带破坏双态全等，**回改才是真回归，严禁按 C# 缺陷形态回改**，亦严禁在 read_scan_input 增设负值下限门（负值语义已由本条 a) 在主存储面裁决，对象面负 COUNT 全量遍历为两侧同形现状）。三处「1:1 保留」失真注释（hash_object.rs 头注与截断点、sorted_set_object.rs 头注与截断点、tiered scan.rs 截断点）已随本票改写为「负 COUNT 恒不命中 → 全量遍历；C# int32 侧 COUNT=-2147483648 翻倍回绕为 0 致首条未命中即停，rust i64 加宽不复刻」形。锁测：`wedb/wnode/tests/scan_family_dualstate_frames.rs::count_i32_min_no_wraparound_lock`（七档 COUNT 双态逐字节锁，绝不出现空列表+非零游标的回绕形态）。

后果：三处同参数下应答与 C# 分叉（C# COUNT 0 漏键终止 vs Rust 续扫；C# 空串 TYPE 全类型透传 vs Rust 空集；C# 回退对齐续扫 vs Rust 终结），三处均比 C# 更接近真实 Redis 语义（Redis 对 COUNT<1 报错、TYPE 空串不透传）。本条为纯文档治理登记，无代码行为变更；下一轮 SCAN 域审查按本条跳过此三面，勿重复立项亦勿反向改写。d) 另登对象族 HSCAN/ZSCAN COUNT 极值翻倍回绕分叉（C# 空页爬行 vs rust 单页全量游标 0），同为零行为改动的纯登记，SCAN 域对账一并引用本条。

## 21. DUMP 载荷 crc64 从类型字节起算（修复上游 DUMP/RESTORE 往返破裂）

工单 zcode-r15-generic 发现二登记。C# `NetworkDUMP`
（`libs/server/Resp/KeyAdminCommands.cs:200` 的 `payloadToHash`）crc64 覆盖段跳过
载荷首字节 0x00 类型字节、从长度前缀起算，而其 `NetworkRESTORE` 校验
（`KeyAdminCommands.cs:74` 的 `calculatedCrc`）用 `Crc64.Hash(valueSpan.Slice(0,
valueSpan.Length - 8))` 从载荷首字节（含类型字节）起算——双侧口径互异，C# 自身
DUMP 产出的载荷经自家 RESTORE 必被 "ERR DUMP payload version or checksum are
wrong" 拒绝，DUMP→RESTORE 往返破裂是上游固有缺陷。
Rust 侧 `wedb/wnode/src/resp/key_admin_commands/types.rs` 的 `network_dump` 刻意
对齐 RESTORE 校验口径：crc64 从类型字节 0x00 起算（覆盖类型字节起至版本字节止），
保证 DUMP→RESTORE 往返成立。
保留理由：修复上游双侧口径互异的往返破裂，语义向「序列化—反序列化自洽」收敛；
后续对账直接引用本条，严禁按 C# DUMP 侧口径改回（改回即重新引入往返破裂）。
锁面：既有 DUMP→RESTORE 往返测试保持通过即为本条的语义锁。
订正注记（r107-devaudit 台账收口）：本条 C# 两处行号原引 `:190`/`:94`，系 garnet
参考树版本更新后的漂移，现码为 `:200` 的 `payloadToHash` 与 `:74` 的
`calculatedCrc`（两处定义行本票执行期亲验在位）；rust 码内注释
`wedb/wnode/src/resp/key_admin_commands/types.rs:137` 此前已引 `:200`，登记落后于
码，本次顺齐后双侧同锚。

## 22. 向量写命令冷态保守拒 WRONGTYPE（C# 真读裁决的 rust 快慢分臂取舍）

工单 zcode-r16-vector 发现一登记。C# 全部向量命令经 `VectorManager.Locking.cs` 的
`ReadVectorIndex`/`ReadOrCreateVectorIndex` 走 `Read_MainStore` 真读落盘裁决：读到
非向量记录才回 WRONGTYPE，键不存在（含冷态墓碑）回 NOTFOUND 族应答。rust 快路径
以同步探针承接同一判据，但同步段读不准磁盘候选待裁决 / 存储错误两态。
Rust 侧处置（`wedb/wnode/src/resp/garnet_api/mod.rs` exec 向量分支 +
`resp_server_session_vectors.rs` 的 `vector_key_guard`）：只读命令（VSIM/VEMB/
VCARD/VDIM/VGETATTR/VINFO/VISMEMBER/VLINKS/VRANDMEMBER）遇读不准态降级慢路径
异步真读裁决（`network_vector_read_slow`），对齐 C# 语义——存活非向量键回
WRONGTYPE，冷态缺失键放行 NOTFOUND 族应答；写命令（VADD/VREM/VSETATTR）保留
保守拒——读不准态一律 WRONGTYPE。
保留理由：写命令保守拒防双域键——向量索引登记表与 wkv 值域并行，冷态不确定键
若放行写入，双域键一经写即成幽灵（KEYS 迁移只迁 string，源端残留幽灵上下文）；
误拒可 DEL 后重试，无数据危害。后续对账直接引用本条，写命令读不准态回 WRONGTYPE
系刻意取舍，严禁按 C# 真读形态报缺陷。
锁面：`wedb/wnode/tests/vector_cold_key_read_parity.rs`。

## 23. VSIM ELE 路径 FILTER 编译失败统一回 "ERR Compiling filter failed"（修复上游臂间文案分叉）

工单 zcode-r16-vector 发现二登记。C# `VectorManager.ElementSimilarity`
（`VectorManager.cs:1023-1030`）在 `ExprCompiler.TryCompile` 失败时返回
BadParams 但未设置 errorMsg（对比同文件 `ValueSimilarity:849-858` 明确设置
"ERR Compiling filter failed"），会话层 `NetworkVSIM` 对 BadParams 且
customErrMsg 为空时回落 "ERR asked quantization mismatch with existing vector set"
（`RespServerSessionVectors.cs:930-937`）——C# 上 VSIM ELE + 非法过滤表达式实际
回量化 mismatch 误导文案，VSIM FP32/VALUES 路径同错却回 Compiling filter failed，
C# 两条路径自身即不一致。
Rust 侧 `element_similarity`（`vector_manager.rs`）与 `value_similarity` 双臂统一
回 "ERR Compiling filter failed"，修复了该臂间不一致。
保留理由：修复上游臂间文案分叉，同错同文案；量化 mismatch 文案与过滤编译失败
无语义关联，回落属上游缺陷。后续对账直接引用本条，严禁按 C# ElementSimilarity
缺 errorMsg 的回落形态报分叉。
锁面：`wedb/wnode/tests/vector_cold_key_read_parity.rs` 的
`vsim_filter_compile_failure_parity`。

## 24. 阻塞族命令经纪未注入域立即回空（不对齐恒阻塞）

C# 一手形态：阻塞族 ListBlockingPop（`ListCommands.cs:284`）、ListBlockingMove（`:372-376`）、ListBlockingPopMultiple（`:913`）无条件 `AsyncUtils.BlockingWait`，键态冷热与阻塞语义解耦，timeout=0 经 `CollectionItemBroker.cs:139-141` 映射 `FromMilliseconds(-1)` 无限等待；`StoreWrapper.cs:245-246` 在 `!serverOptions.DisableObjects` 下构造期建立 CollectionItemBroker（DisableObjects=true 时经纪为 null），但 objects 禁用面在 `ObjectStore/Common.cs:32/:53/:764-765`（`ThrowObjectStoreUninitializedException`，GarnetException "Object store is disabled"）先行整面拒绝，阻塞族不可达，故「生产可达域不存在无经纪阻塞面」结论不变，恒阻塞不可选。
Rust 侧裁决：阻塞等待面以经纪注入为前提——慢路径执行域经 `item_broker_wait()` 构造 `BlockWaitFace`（`garnet_api/slow.rs`，生产主链路 `service.rs` 恒注入），经纪注入域装载未取到时经 `BlockWaitFace::wait` 内联等待闭环（`list_commands/slow.rs`，与快路径 `park_broker_wait` 同一 `BlockedWait` 竞速单源与 `write_collection_item_result` 应答单源，timeout=0 无限等待同 C#）；经纪未注入的独立会话域（`item_broker_wait` 为 None，仅嵌入式/测试装配形态）无等待面，冷键/空键上阻塞族立即回空值（BLPOP/BRPOP 空数组、BLMOVE/BLMPOP 空值），不挂起不等待。
后果：仅无经纪装配下阻塞族语义与 C# 分叉（该域在 C# 不可达）；生产单机装配恒注入经纪，阻塞语义与 C# 恒阻塞逐点对齐。语义锁用例见 `wedb/wnode/tests/list_blocking_cold_wait.rs` 的 `blpop_broker_absent_stays_immediate`。

## 25. 零耗时 pending 样本丢弃不计（修正 C# 单参 Stop 记上界缺陷）

C# 一手形态：pending 延迟真实调用链为 `Storage/Session/Metrics.cs:29-32 StopPendingMetrics` → `GarnetLatencyMetricsSession.cs:78-83 Stop(cmd)` → `LatencyMetricsEntrySession.cs:40-50 RecordValue(int ver)` 单参重载：`elapsed = Stopwatch.GetTimestamp() - startTimestamp`，elapsed==0 时 `IsValidRange` 为假（LOWER_BOUND=1，0 非法），落 `HISTOGRAM_UPPER_BOUND`（100 秒上界巨值）；双参重载 `RecordValue(int ver, long elapsed)` 虽含零值短路，但真实 pending 链不经过它。
Rust 侧裁决：`PendingLatencyMeter::record`（`wmetric/src/latency/pending_latency_meter.rs`）对 elapsed==0 直接 return 丢弃不计——亚微秒异步闭环在 rust 计时域差值饱和为 0，记 100s 上界只会污染直方图尾桶；越界（超上界）仍收敛上界对齐 C#。
后果：同一 PENDING_LAT 类别下，零耗时样本 C# 计为上界巨值、rust 不计，双侧直方图 calls 计数与上界桶占比分叉；rust 为刻意修正，非疏漏。下一轮 wmetric 域审查按本条跳过，勿反向对齐 C# 缺陷。行为钉死测试：`pending_latency_meter.rs` 无 `mod tests`，钉死测试实为集成测试 `wedb/wmetric/tests/pending_latency_zero_sample.rs` 的 `record_zero_discards_sample_and_keeps_counts`（:41）之 record(0) 样本数不变断言。

## 26. AofAddress 逗号串解析负号逐段生效与段数上限门禁（修复上游解析双缺陷）

工单 zcode-r20-waof 发现二登记。C# 一手形态：`AofAddress.FromString`
（`libs/server/AOF/AofAddress.cs:150-188`）逗号分支只写 `value` 不施加
`negative` 也不复位标志，负号只对末段生效——输入 "1,-2,300" 产出
`[1, 2, -300]`（中段 -2 被静默解析为 +2）；段数超 `MaxSublogCount = 4` 时构造仅
`Debug.Assert` 门禁，release 下 `fixed long addresses[4]` 固定数组越界写。
Rust 侧裁决：`wedb/waof/src/aof/address.rs` 的 `from_string` 逐段施加负号并逐段
复位（"1,-2,300" → `[1, -2, 300]`），段数 > 4 返回 None。
后果：均为上游缺陷的修复性偏离——负号逐段语义向「串面书写即所得」收敛，段数
门禁杜绝越界写。消费面为副本 attach 位点串解析（双侧均为本仓实现，实际地址恒
非负，负号分支当前不可达），无跨实现对接危害；但逗号段负值在两侧解析出不同
地址向量，登记以防后续审查轮重复对账与跨实现静默误读。下一轮 AOF 域审查按本条
跳过，勿反向对齐 C# 缺陷。行为钉死测试：`address.rs` 单测
`string_roundtrip_and_rejects`（"1,-2,300" 往返 + 超长拒绝）。

## 27. 检查点恢复 flushed 前缀短读短缺拒启（C# 静默截短起库）

工单 zcode-r21-doczh 发现四登记。C# 一手形态：恢复读完成回调
`AsyncReadPagesForRecoveryCallback`（`libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:1456`，
由 `AllocatorBase.cs:2112` `AsyncReadPagesForRecovery` 传入）只判 `errorCode`、
不校验实读 `numBytes` 是否达到请求长度——文件末端短读按可得字节放行，
缺失区等效读为全零，恢复扫描遇零头即停止重建；检查点一致性承诺的
flushed_until 连续前缀缺失时 C# 静默起库、只截短数据。
Rust 侧裁决：`wcpr/src/manager/recover.rs:135-161` 两档分治——`flushed_until` 连续已落盘前缀做前缀末端覆盖性检查：现码仅探 flushed-1 所在末段的物理覆盖（:146-154），前缀末端未覆盖即数据文件与检查点不配套，具名拒启（前缀中段截断由后续页装载报错承接；flushed 仍处初始基准的全新库零长度设备豁免本档）；`[flushed, tail)`
崩溃残留段维持原容灾语义，刻意不校验、不升级为拒启。
保留理由：C# 静默截短起库后新写入接在残缺数据之后，历史前缀永久丢失
且无告警；拒启把「残缺状态被后续写入固化」显式化，交运维裁决。
后果：同一检查点 + 数据文件故障形态下 C# 可静默起库（数据截短）、
rust 拒绝启动；容灾演练与故障恢复预期双侧分叉。后续对账直接引用本条，
勿按 C# 形态放行短读。

## 28. EXPIRE 过去时间戳写内物理删（C# 惰性过期）

工单 zcode-r21-doczh 发现五登记。C# 一手形态：
`libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:194` EXPIRE 分支
`HandleExpireInPlaceUpdate` 仅把记录过期域设为过去值（惰性过期），由后续
读路径 CheckExpiry（`LogRecordUtils.cs:18`，`MainStore/ReadMethods.cs:37`
等读臂调用）清理；AOF 记
RMW-EXPIRE 条目。写路径 NX/XX/GT/LT 设置判定为另一函数
`SessionFunctionsUtils.cs:32 EvaluateExpire`（原登记括注将其误指为读路径
清理，已订正）。
Rust 侧裁决：`wnode/src/resp/key_admin_commands/keys.rs` 的
`expire_apply_sync`（偏差声明 :749-762、实现 :778-828）`is_expired_or_now`
判定即写命令内物理删键（先删 TTL 记录再删数据，镜像 purge_expired 顺序），
AOF 为 DEL 形态墓碑；判定单源 `wkv::is_expired_or_now`（含相等口径）。
保留理由：对齐 Redis 7.4「EXPIRE 过去时间戳立即删除」语义。应答（:1）与
最终可见状态（键消失）双侧一致；重放均幂等但机制各别——rust DEL 形态
墓碑重放遇缺键零写入无副作用；C# RMW-EXPIRE 条目重放走 AofProcessor
`UnifiedStoreRMW`（AofProcessor.cs:520-521/:742）原样反序列化重发 RMW，
`EvaluateExpire` 复设同一过去 ticks 与时刻无关故幂等；C# AOF 重放链无
DELIFEXPIM 转换（DELIFEXPIM 唯产自 rust GC 清退链 `wkv/src/ttl.rs:391`
TtlPurge → `wnode/src/service.rs:436-445` 与 C# 主动扫描清退
`ArrayKeyIterationFunctions.cs:219-221`，与本条 EXPIRE 命令 AOF 形态
无关，原登记「重放端 DELIFEXPIM 确定性」系论据错位，已订正）。
后果：AOF 字节流形态分叉（DEL vs RMW-EXPIRE），跨实现对账以可见状态为
准、勿逐条比对 AOF；WATCH 版本推进次数 rust 两写（TTL 墓碑 + 数据墓碑）
vs C# 单记录一次（只多不少、方向单调，不影响事务失效判定正确性）。
下一轮 keys/EXPIRE 域审查按本条跳过，勿重复提报。

## 29. AOF 体积超限自动检查点守护任务单次失败续跑（C# 杀任务）

工单 zcode-r21-doczh 发现六登记。C# 一手形态：
`libs/server/StoreWrapper.cs:AutoCheckpointBasedOnAofSizeLimitAsync` 的
try-catch 包在 while 外——执行体单次异常即退出循环杀任务，且 TaskManager
注册面 AofSizeLimitTask 无重拉通路，守护静默失效直至进程重启。
Rust 侧裁决：`wnode/src/service.rs` AOF 体积超限自动检查点后台任务
（头注自证）单次失败仅记 error 日志不退出循环，下一轮周期继续尝试。
保留理由：守护任务的存在意义是持续兜底，单次瞬时失败（如 IO 抖动）杀掉
守护使 AOF 体积限额彻底失守且无自愈；续跑保持守护活性，失败经日志可观测。
后果：异常风暴场景 C# 守护停止（体积限额不再受控）、rust 守护存活（每轮
重试），双侧任务生命周期可观测行为分叉。后续对账直接引用本条，勿按 C#
形态改回「单次失败退出」。

## 30. DEBUG 族 .NET 运行时能力无 rust 对位之降级（PANIC 刻意崩进程／FORCEGC·PURGEBP GC 执行面）

条目一（PANIC 崩溃面）：工单 zcode-r21-doczh 发现七登记。C# 一手形态：
`libs/server/Resp/AdminCommands.cs:743-744` DEBUG PANIC
`throw new GarnetException(LogLevel.Debug, panic: true)` 刻意崩溃进程
（注释自认 intentional and desirable）。
Rust 侧裁决：`wnode/src/resp/admin_commands.rs:407-411` 全库禁 panic 约束
下按存储失败惯例降级回 `RESP_ERR_GENERIC` 错误帧（`:409`），进程存活；
enable-debug-command 门禁两侧同形。
保留理由：compio 线程一崩即整个 worker 运行时终止，崩溃注入语义无法等价
移植；错误帧保留命令通路与可观测性。
后果：依赖 DEBUG PANIC 做崩溃注入的运维/测试脚本在 rust 侧收到错误帧
而非进程退出。后续对账直接引用本条，勿按 C# 形态恢复 panic。

条目二（GC 回收面，工单 zcode-r120-objmisc1 立案二发现、r121-triage-misc1 复跑执笔）。C# 一手形态：`AdminCommands.cs:807` DEBUG FORCEGC 臂 `GC.Collect(generation, GCCollectionMode.Forced, blocking: true)` 阻塞式强制回收后 :809 回 "GC completed"；`PurgeBPCommand.cs:76` PURGEBP 成功路径 `GC.Collect(GC.MaxGeneration, Forced, true)` 后经 `ManagerTypeExtensions.ToReadOnlySpan`（:37-39）回 "GC completed for <type>"——两臂应答文案语义均宣称「已执行一次阻塞式全代回收」。Rust 侧裁决：rust 无 GC.Collect 之分代回收对位（内存回收归所有权与 allocator 模型），FORCEGC 臂（`wnode/src/resp/admin_commands.rs:458-481`）文法值域门后无回收动作、:479 按文案原样应答，注释 :471-472/:478 自认在案；PURGEBP 臂（:483-516）精确二分——池清洗为实（ServerListener 臂 :499-505 直清监听器缓冲池 listener_buffer_pool 对位 C# :63-66，迁移/复制臂 :512 经 `cluster_provider.rs:300` 真接线清洗，standalone 恒由 :508-511 CLUSTER_DISABLED 先行拦截、trait 默认空臂 `cluster_provider.rs:131` 不可达），完成串中 GC 语义为应答文案原样保留（`session_parse_state_extensions.rs:125-131` gc_completed_text 对 C# :37-39 逐字对齐，令牌解析 `manager_type_from_token`（:110）对 TryGetManagerType）。裁决：不采番、不回改文案，PURGEBP 池清洗实效勿因本条被误读为整臂 no-op 而误删已真接线的清洗；严禁按 C# 形态补运行时级全局 GC 钩子（过度设计），若后续接入 allocator 级回收钩子按本条升格为实装注记即可。后果：依赖 FORCEGC/PURGEBP 做内存回收节流的压测与运维脚本在 rust 侧获得虚标成功应答而无回收副作用，对拍「已发生回收」维度直接按本条判净勿复勘；应答文案与非法代数错误帧（"ERR Invalid GC generation."，常量单点 `admin_commands.rs:41`，发射 :468/:474 与 C# :804 逐字同形）双侧逐字同形，代数文法前导零分叉另见 §32b 第 23 项。锁面对账订正：PURGEBP 既有力锁（`wedb/wnode/tests/resp_admin.rs` :482 注释、:515 文档注、:536 "+GC completed for ServerListener" 帧断言）在位不动；FORCEGC 臂现树无任何既有测试锁，凡后续票再称「FORCEGC 力锁不动」即假锚，据实登记防以讹传讹；FORCEGC 形态锁（"01" 拒、"1"/"+1"/"-0" 放行与 0..=2 值域门）另归对拍/测试席处理，不搭本登记票便车。

## 31. TYPE 扩展对象回注册名（C# 零字节 quirk）

工单 zcode-r21-doczh 发现八登记。C# 一手形态：
`libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:126` `HandleType`
对 ValueObject 的 switch 仅覆盖 SortedSet/List/Set/Hash 四内建类型且无
default 臂——custom object（模块扩展对象）落入后不写任何字节，TYPE 对
扩展对象键回零字节应答（quirk）。
Rust 侧裁决：`wnode/src/resp/array_commands.rs` 的
`envelope_object_type_name`（:212-217）内建段走 `GarnetObjectType` 小写名，
扩展段统一走 `resp/custom_objects.rs:custom_object_type_name` 编译期静态
清单回 C# modules 注册名，使 TYPE 与 EXISTS 对扩展对象键的存活口径一致；
未知标签仍 none（畸形信封防御臂）。
保留理由：零字节应答击穿客户端 RESP 解析；回注册名向「TYPE 可读出类型
名」标准语义收敛。
后果：扩展对象键上 TYPE 应答 C# 零字节 vs rust 注册名，双侧对账测试在
该用例必然发散；勿按 C# quirk 改回零字节（改回才是真回归）。下一轮
TYPE/generic 域审查按本条跳过，勿重复提报。

## 32. 整数文法统一严格口径（拒 '+前导零' 形态：参数 i32 死参面与 INCR 族旧值面）

族目纪律：参数面与旧值面系同一文法单点（`wbase/src/num.rs:scan_digits`）的两侧分叉，登记合并为一条族目，禁分立描述；`zcode-r31-intparse`（参数面 i32 死参）落地时并入本条 b) 扩充消费点清单与锚修正记录，不再另立新条。

a) INCR 族旧值面（工单 zcode-r24-wvalstring 发现一）：C# 旧值解析收敛于 `PrivateMethods.cs:672` 与 `VarLenInputMethods.cs:21/:42` 三处同型 `IsValidNumber`（InPlace :405/:444 与 Copy :574/:586 两臂共用），均转调 `NumUtils.cs:216-233 TryReadInt64`；该函数前导零检查为「剥 '-' 后判首字节 '0'」但不剥 '+'——"+007" 的 beg 指向 '+' 使检查天然滑过，随后 `NumUtils.cs:504 TryParse` → Utf8Parser D 格式（接受前导 '+'，符号后前导零不拒绝——跳过，"+007"→7 与 "000"→0 其本身均收；"007" 的拒绝系 `TryReadInt64` :224 自家前置自查段，非 Utf8Parser 行为）收下得 7。故 C# `SET k "+007"` 后 `INCR k` 回 `:8` 并写回 "8"。而 C# 参数侧走 `RespReadUtils.cs:158 TryReadInt64Safe`（TryReadSign 剥 '+' 后查首 '0'），"+007" 被拒——C# 自家旧值与参数双路径文法互异，旧值侧系 '+' 未剥的实现漏洞。Rust 裁决：旧值与参数全域统一 `strict_i64` 严格文法，"+007"/"+00" 与 "007" 同拒（含 '+' 前缀形态），与 C# 自家参数路径同口径；注释锚已固化于 `wnode` incr.rs / slow.rs（回指本条）。

b) 参数面（zcode-r31-intparse 并入）：C# `RespReadUtils.TryReadInt32Safe`（`RespReadUtils.cs:258-302`）声明了 `bool allowLeadingZeros` 参数但函数体未消费（无前导零检查段，直入 `TryReadUInt64`；而同文件 `TryReadInt64Safe:172-177` 实拒前导零），导致 `ParseUtils.TryReadInt`（`ParseUtils.cs:47-55`，显式传 `allowLeadingZeros: false`）传参失灵，`SessionParseState.TryGetInt`（`SessionParseState.cs:391-394`）在整个参数面上实际放行 "007"/"+007" 并解析为 7。C# 自家 i32 档与 i64 档文法互异，i32 档系上游实现疏漏而非设计。
分叉消费点清单涵盖 C# 走 `TryGetInt` 的 20 余处参数点（左侧 C# 均为 TryGetInt 放行 007，右侧 Rust 统一严格文法拒前导零）：
1. `SELECT` 库号：`ArrayCommands.cs:125` 对 `array_commands.rs` `network_select`→`parse_db_index` 调用点（原登 `:464` 漂入 MSETNX 快路径 `msetnx_resume` 区，订锚）
2. `SWAPDB` 双库号：`ArrayCommands.cs:175/180` 对 `array_commands.rs` `network_swapdb` 内两处 `parse_db_index` 调用点（原登 `:516/529` 漂入 MSETNX 快路径区，订锚）
3. `SETRANGE` 偏移：`BasicCommands.cs:450` 对 `set.rs` `parse_setrange_args`（`strict_i32`；原登 `:752` 系 APPEND 读块，订锚）
4. `GETRANGE` / `SUBSTR` start/end：`BasicCommands.cs:499` 对 `get.rs:227/231`、`slow.rs:583`
5. `SETEX` / `PSETEX` 秒/毫秒：`BasicCommands.cs:542` 对 `set.rs` `parse_setex_args`（原登 `:729-730` 系 APPEND 邻读块，订锚）
6. `SET EX` / `PX` 秒/毫秒：`BasicCommands.cs:653` 对 `set.rs:815`
7. `RESTORE` ttl：`KeyAdminCommands.cs:NetworkRESTORE` 对 `types.rs:197/210`
8. `LCS` `MINMATCHLEN`：`ArrayCommands.cs:450` 对 `array_commands.rs:185/188`
9. `GEOSEARCH` `COUNT`：`SortedSetGeoCommands.cs:262` 转 `TryGetGeoSearchOptions`，整数解析点在 `SessionParseStateExtensions.cs:477`；`ZRANGEBYSCORE` `LIMIT` offset/count 在物层 `SortedSetObjectImpl.cs:435/436`（会话层 `SortedSetCommands.cs:146` `SortedSetRange` 无 TryGetInt；原登 `SortedSetCommands.cs:468` 系 `SortedSetMPop` (:413) 区 ZMPOP COUNT，订锚） 对 `sorted_set_geo_commands.rs:331`
10. `SINTERCARD` / `ZINTERCARD` numkeys 与 `LIMIT`：`SetCommands.cs:168/195`、`SortedSetCommands.cs:1184/1211`（ZINTERCARD 臂系 `SortedSetIntersectLength` (:1175)；原登 `:423` 系 `SortedSetMPop` (:413) 区 ZMPOP numkeys，订锚；`:1211` LIMIT 原锚正确保留） 对 `object_store_utils.rs:403/423`
11. `SRANDMEMBER` / `HRANDFIELD` count：`SetCommands.cs:647`（SRANDMEMBER 臂，原登 `:531` 系 SPOP 臂，r118-triage-set1 订锚）与 `HashCommands.cs:228`（HRANDFIELD 臂）对 `set_commands/read.rs:162-172`/`slow.rs:237-247`（SRANDMEMBER）与 `object_store_utils.rs:298`（仅 H/ZRANDMEMBER）
12. `LTRIM` start/stop：`ListCommands.cs:464/465`（`ListTrim` (:454) 内 start/stop 连读两参；原登 `:512` 系 `ListRange` (:502) 即 LRANGE，订锚） 对 `list_commands/mod.rs:122`
13. `LINDEX` / `LSET` index：`ListCommands.cs:564`（仅 `ListIndex` (:554) 覆 LINDEX）与 `ListObjectImpl.cs:311`（LSET 会话层 `ListSet` (:813) 不解析整数、真实 index 解析点在物层，订锚） 对 `list_commands/slow.rs:234/389`
14. `LPOP` count：`ListCommands.cs:83` 对 `list_commands/write.rs:293`
15. `LMPOP` / `BLMPOP` numkeys 与 count：`ListCommands.cs:198/228/866/903` 对 `list_commands/mod.rs:60/95`、`slow.rs:477`
16. `LPOS` RANK / COUNT：`ListObjectImpl.cs:481/489/499`（RANK/COUNT/MAXLEN 整数解析均在物层；原登 `ListCommands.cs:158` 系 `ListPosition` (:131) NOTFOUND 分支的 COUNT 关键词扫描、无 TryGetInt，订锚） 对 `list_commands/slow.rs:322`
17. `HELLO` 协议版本：`BasicCommands.cs:1455` 对 `basic_commands/mod.rs` `parse_hello_args`（strict_i32 及回指本条注在 :914-915；原登 `:511` 系 HELLO 错误应答臂，改登函数名符号锚）（注：C# 放行 007 为 7 后落协议不支持门回 `unsupported protocol version`，Rust 严格文法直接报 `protocol version is not an integer` 错误帧）
18. `etag` 族过期秒数：`BasicEtagCommands.cs:167/239` 对 `basic_etag_commands.rs:368/489`
19. 向量族全部整型参数（`VADD` DIM/EFP/M、`VCARD`、`VGETATTR` count、`VSIM` count/ef/filtering effort）：`RespServerSessionVectors.cs:44/92/320/366/626/720/768/814` 对 `resp_server_session_vectors.rs:306/342/474/498/710/768/794/818/1133`
20. `RI.SCAN` count：`RespServerSessionRangeIndex.cs:361`（原登「RI.RANGE count」系虚设——RI.RANGE 语法 `key start end [FIELDS]` 无 COUNT 参数、`NetworkRIRANGE` (:420) 无 TryGetInt，删目） 对 `resp_server_session_range_index.rs:484`
21. `CLUSTER COUNTKEYSINSLOT` / `GETKEYSINSLOT` 槽号：`RespClusterSlotManagementCommands.cs:146/352`（:146 COUNTKEYSINSLOT 原锚正确；GETKEYSINSLOT 臂系 `NetworkClusterGetKeysInSlot` (:338) 内 `:352`，原登 `:283` 系 `NetworkClusterDelKeysInSlot` (:272) 即 DELKEYSINSLOT，订锚；C# 为 TryGetInt int32 档，Rust 用 strict_i64 严格收口，007 拒收落 invalid slot 错误帧；对位 `slot_mgmt.rs:568`）
22. `CLUSTER FORGET` / `RESET` 过期秒：`RespClusterBasicCommands.cs:71/:472`（C# TryGetInt int32 档、死参放行 007；对位 `basic.rs:539-547/:434-443` strict_i64——文法向更严（007 拒，本条族同形）叠加值域向更宽（>2^31 如 3000000000 C# 拒、rust 收，FORGET 侧入 ban 窗、RESET 侧双侧均死参/未用形参仅错误帧差），值域外溢系本条统一严格文法裁例方向之 redis 64 位整数同向面，非转写宽松缺陷，对账勿起案）
23. `DEBUG FORCEGC` 代数：`AdminCommands.cs:802` 对 `admin_commands.rs:467`（`strict_i32` 形态：C# TryGetInt 死参放行 "01"/"+002" 并为 1/2、折值落 0..=2 值域门内即成功回 "GC completed"，Rust `num.rs:106` scan_digits 单点拒前导零回 "ERR Invalid GC generation."；仅「前导零且折值在域」字面形态发散，非前导零合法代数双侧同成功，越界代数（含 "09" 类前导零越界形态，C# 折值后仍撞值域门）双侧同回 "ERR Invalid GC generation." 同文错误帧；该应答之 GC 执行面降级另见 §30 条目二，同族互引不重出。编号注记：登记票 doc-deviations-debug-gc-forcegc-purgebp-registry（源 zcode-r120-objmisc1 立案一、r121-triage-misc1 甄别）票称「第 22 项」，因 `CLUSTER FORGET`/`RESET` 条先入库占号，依台账让位条款顺编取第 23 项）
24. `MEMORY USAGE` 样本数：`BasicCommands.cs:1610` 对 `basic_commands/mod.rs:561`（`strict_i32` 形态：C# TryGetInt 死参放行 "007"/"+007" 折 7 过 `<0` 语法门后照常执行记账回整数或 nil，Rust `num.rs:106` scan_digits 单点拒前导零回 "ERR value is not an integer or out of range." 帧；"-007" 形态双侧同错异文（C# 折 -7 落 syntax error、rust 落 not-integer）；samples 值双侧均仅语法门不消费、无值语义差，该面判净勿复勘。编号注记：登记票 zcode-r129c-memusage（r130丙-tr-memusage1 甄别）立案时本清单现 23 项、第 23 项 `DEBUG FORCEGC` 条先入库占号，依台账活号让位条款顺编取第 24 项）
25. `SLOWLOG GET` count：`RespSlowlogCommands.cs:53`（`parseState.TryGetInt(0, out count)` 死参面放行 "007"/"+007" 并为 7，过 `count < -1` 门后按 7 条照常回）对 `wmetric/src/slowlog/resp_slowlog_commands.rs:75-81`（`parse_i32`→`strict_i32` 单点，本文件 :203-204 转 `wbase/src/num.rs` scan_digits，拒前导零落 `ERR count should be greater than or equal to -1.` 错误帧；文案词形与值域门双侧同形另见该票对位清单）——「前导零且折值在域」形态与 §32 族裁决全同向，rust 严格文法系在册裁决非疏漏，对拍轮遇 `SLOWLOG GET 007` 用例直引本目免复勘。编号注记：登记票 zcode-r145c-dbglat（甄别 r145c-tr-dbglat 席）立案丙纯补目零行为变更；立案时本清单现 24 项、第 24 项 `MEMORY USAGE` 条先入库占号，依台账让位条款顺编取第 25 项

Rust 裁决：维持全仓统一 `strict_i64`/`strict_i32`/`strict_u64`/`parse_db_index` 严格文法（`wbase/src/num.rs:scan_digits` 单点），统一拒绝前导零（含 `+`/`-` 符号后跟前导零），与 C# 自家 i64 档、INCRBY 参数路径、redis 标准文法完全同口径，按上游缺陷修复型裁决；全仓注释锚已修正（清理「TryGetInt 拒前导零」等失真表述，明确标注 C# 死参放行事实与回指本条）；严禁后续按 C# 死参形态回改放宽。

后果：`SET k "+007"` 后 `INCR k`，C# 回 `:8` 且键值变 "8"，rust 报 not-integer 且键值保持 "+007"；参数面形态（如 SELECT 007）C# 成功 rust 拒。凡含该形态的双侧对拍测试必然发散，勿判转写缺陷，更严禁按 C# 漏洞形态回改放宽（放宽才是文法倒退）。旧值面语义锁见 `wedb/wnode/tests/incr_grammar_and_ri_encoding_locks.rs`；参数面文法锁见 `wedb/wnode/tests/intparse_grammar_locks.rs` 与 `wedb/wedb/tests/cluster_resp_session.rs`（`cluster_countkeysinslot_grammar_lock`）。

库号双层口径边界尾注（SELECT/SWAPDB 线面 i32 vs 内部 u64 全宽）：SELECT/SWAPDB 线面库号经 `parse_db_index` 收口 i32 值域系对齐 C# int32 一档契约的裁决，内部库 ID 与槽位链路（`active_db_id`/`slot_of`/`set_active_db`）为 u64 全宽自由切库；两档各自单源，严禁按「64 位库 ID」宣称放宽线面、亦严禁按 C# 面收缩内部全宽；锚指 `wbase/src/num.rs:161-176` 头注。

## 33. 集合项经纪出件路径两处同源改良（观察者锁内原子出件 / retain 全队列清退）

工单 zcode-r23-broker 发现三登记，两处同源（均在经纪出件路径、均属上游缺陷
或局限的修复性偏离），一处两条。
C# 一手形态：其一，InitializeObserver 的 TryGetResult（`CollectionItemBroker.cs:269`）
在 ObserverStatusLock 之外执行——弹出元素经事务无条件提交后才在 `:275` 事后
补设 `HandleSetResult(result)`；观察者超时路径（`GetCollectionItemAsync` `:146`
WaitAsync 超时 → `:156-160` `HandleSetResult(Empty)`）恰落于弹出与补设之间时，
补设因状态已非 WaitingForResult 而 no-op，已物理弹出的元素随结果一起丢弃
（TryAssignItemFromKey 臂 `:326-358` 在锁内，仅 InitializeObserver 臂裸奔）。
其二，CleanKeysToObservers（`CollectionItemBroker.cs:724-733`）受
ConcurrentQueue 无锁单向队列接口所限只能队首 TryPeek/TryDequeue 窥探：队首
一旦是长等待的活跃观察者循环即中止，排在其后的失效观察者（超时/断连/多键
已被满足）被永久阻隔滞留，command_args 与结果载荷无人回收，队列无界膨胀。
Rust 侧裁决：其一，`CollectionItemObserver::try_assign_with`
（`wcol/src/itembroker/collection_item_observer.rs:132-156`）把状态校验、出件、
落结果并成单一临界区，与超时/销毁路径互斥——要么结果落袋、要么元素留集，
消灭丢件窗；三个专项测试钉死行为（`wcol/tests/collection_item_broker_tests.rs`
`timeout_landing_in_pop_window_keeps_popped_item` 等）。其二，
`clean_keys_to_observers`（`wcol/src/itembroker/collection_item_broker.rs`）
在 `Mutex<VecDeque>` 排他锁内 `retain` 保留 FIFO 相对顺序单次 O(N) 剔除队内
全部死节点，不照搬队首窥探。
后果：同一竞态输入下 rust 单侧保件、C# 丢件，属可观测行为分歧；队列清理
完备性 rust 优于 C#。均为刻意设计，下一轮经纪域审查按本条跳过，勿反向对齐
C# 缺陷。

## 34. 集合对象未知子操作码回错误帧（C# 抛 GarnetException 掐断连接）

工单 zcode-r28-errframe 条目二登记。C# 一手形态：四对象 Operate 的 switch
default 臂各抛 `GarnetException($"Unsupported operation {op} in {Type}Object.Operate")`——
`libs/server/Objects/Hash/HashObject.cs:298`、`List/ListObject.cs:185`、
`Set/SetObject.cs:167`、`SortedSet/SortedSetObject.cs:449`，异常通道掐断连接，
C# 无同名错误帧文案。
Rust 侧裁决：子操作码 `from_repr` 未命中即回 `-ERR unsupported operation`
单行错误帧、连接存活。常量单点
`wresp/src/cmd_strings.rs:RESP_ERR_GENERIC_UNSUPPORTED_OPERATION`，四对象
default 臂共用（`wcol` hash/set/list/zset）。
保留理由：未知子码属内部不一致防御面，优雅回错替代上游崩溃通道
（同条目 5/10 的崩溃防御型先例）。
后果：同输入 rust 连接存活并回错帧，C# 断连；对账测试在该用例发散系刻意。
语义锁用例见 `wedb/wcol/tests/unsupported_operation_tests.rs`。

## 35. 慢路径存储错误降噪单行帧与异步要求拒绝帧（C# 外泄异常消息且断连）

工单 zcode-r28-errframe 条目三登记。C# 一手形态：慢路径执行异常被
`libs/server/Resp/RespServerSession.cs:546` 的 catch GarnetException 臂转写为
`-ERR Garnet Exception: {ex.Message}`——内部异常消息原样外泄给客户端，且随
异常断连。
Rust 侧裁决：存储层错误统一降噪为稳定单行 `ERR slow path storage error`
（`wresp/src/cmd_strings.rs:RESP_ERR_SLOW_PATH_STORAGE`，wnode 执行域与 wedb
集群域共用），连接存活。
保留理由：内部错误细节外泄属信息面泄漏，降噪单行不损失可观测性（服务端
日志仍留全量异常细节），客户端也无从对易变异常文本做解析依赖。
同类 rust 自有帧：`RESP_ERR_ASYNC_REQUIRED`（"ERR command requires
asynchronous completion"，挂起模型下异步要求拒绝；C# CmdStrings 无对应
常量，rust 拓扑专有面）。
后果：同输入 rust 回稳定单行且不断连，C# 回异常消息且断连；后续审查勿把
降噪单行误判为「缺失 C# 异常细节」回改外泄形态。

## 36. TLS 面证书校验与装载五处分叉的裁决缺席登记

工单 devsync-tls 登记（zcode-r80-devplan 派生；出处工单 zcode-r27-wtls
发现一/二与 zcode-r48-certfile；抽验锚点已按现尖复核）。deviations.md 前
35 条零 TLS 条目，wtls 路径自仓库 init 仅 700ba7b 一笔 task 登记，以下五
分面均属安全契约面（分叉可直接观测：C# 拒 rust 过或方向相反），此前零裁
决登记，后续任何 TLS 轮次对账直接引用本条，勿重复取证疑报。

a) 出站校验目标主机空 fail-fast 门弃用：C#
`libs/server/TLS/GarnetTlsOptions.cs:GetSslClientAuthenticationOptions`
对 `ServerCertificateRequired && ClientTargetHost 为空` LogError + throw
GarnetException（:172-176，构造期 fail-fast）；rust `ClientTlsConfig::new`
（`wedb/wtls/src/client.rs:59`）对空 `target_host` 无拦截（:100 原样存
置），装配点 `wedb/wedb/src/server/boot.rs:199` 的 `unwrap_or("")` 透传空
串（票面 :191，现尖漂移至 :199），握手期 `server_name`（client.rs:138-141）
空串静默回落 endpoint host 段作 SNI。C# 启动拒、rust 静默回落，方向相
反。修复向缺口，待底层票收口。

**收口注记（zcode-r27-wtls）**：门已按 C# 收口——`ClientTlsConfig::new`
入口对 `server_cert_required && target_host 为空` 即报
`InvalidInput("tls-client-target-host should be provided when
tls-server-cert-required is enabled")`，经 boot.rs 装配链启动期炸出；
`required=false` 空目标保持回落可用（不校验远端证书时回落无危害），
`server_name` 回落逻辑原样保留。同票观察项一并裁决：`wconf has_tls()`
仍不含 `tls_server_cert_required` 与 `tls_cert_refresh_freq`，系刻意保留
——两旋钮单独配置无证书路径时属惰性配置（required 默认 true，若入
has_tls 则无任何 TLS 意图的裸节点将全部被守卫拒启）；出站是否加密由证书
路径驱动，对齐 C# `TlsClientOptions` 仅在证书在位时产出的形态。

b) 证书装载 PEM-only 收窄：C# 装载走 X509Certificate2 /
X509CertificateLoader 与 Windows 机器证书库（CertSubjectName 面仅
Windows），支持更宽格式族；rust `wedb/wtls/src/cert.rs` 的
`load_certs/load_private_key`（:20/:38）收窄 rustls_pemfile PEM-only
（PEM 内容识别不依赖扩展名，同文件测试锁形态）。格式族收窄向分叉，修复
向缺口待底层票收口。
收口注记：已由 §55 完整展开。

c) cert-subject-name 校验面缺席：C#
`GarnetTlsOptions.cs:ValidateCertificateName`（:218-227）取 DnsName、空
则回退 SimpleName，忽略大小写比对 targetHostName，主体名不符即拒；rust
出站仅 SNI 指名预校验（`server_name`），入站 AnyClientCert 无主体名/身份
校验臂，对位能力缺席。修复向缺口待底层票收口。
收口注记：已由 §56 完整展开。

d) 证书加载失败语义相反：rust 装载链 fail-fast——`load_certs /
load_private_key` 解析失败即 io::Error 上抛，CA 空证书集 `ca_roots`
（server.rs）快速失败禁静默空根，构造期失败启动拒启；C#
`libs/server/TLS/ServerCertificateSelector.cs:GetServerCertificate`
（:109-140）catch Exception 吞错仅 LogError，刷新定时器在场按
certificateRefreshRetryInterval 安排重试自愈，否则证书留 null 启动不
拒。方向相反；本分面收口方向明示取 rust fail-fast 向（安全面收紧，禁静
默降级带病起库），余同按修复向缺口待底层票收口。
收口注记：已由 §57 完整展开。

e) AnyClientCert 宽松臂现状：r79-devsync2 抽验锚点在码复核——
`wedb/wtls/src/server.rs:403` issuer 缺席臂仍挂 AnyClientCert；:442
`struct AnyClientCert`（证书在位即过，不校验颁发者链）；:463
`verify_client_cert` 仅 `_intermediates`（:466）下划线弃用，`end_entity`
（:465）与 `now`（:467）已具名消费——`Validity::from_der(end_entity)?.check(now)?`
（:469）后方回 `Ok(ClientCertVerified::assertion())`（:470）。对标 C#
`GetCertificateIssuer` 空路径臂 LogWarning「chain will not be validated
against issuer」+ 链策略 AllowUnknownCertificateAuthority 任意未知 CA 放
行（GarnetTlsOptions.cs:253-277、:286-327），双侧同为宽松臂非方向相反，
但 rust 现状未经登记裁决，一并入档，后续是否引入钉根强制随底层票收口。
订正注记（r107-devaudit 台账收口）：上段系 r79-devsync2 抽验时的原形态，其
「三参数（_end_entity/_intermediates/_now）全下划线弃用」与行号 :430/:448/:454
已被同条 zcode-r27-wtls 收口注记（见下）所述实校验形态取代——本段按现码顺齐为
:442/:463/:465/:466/:467/:469/:470，宽松臂裁决与 C# 对照结论不变，形态描述以收口
注记为准。

**收口注记（zcode-r27-wtls）**：宽松臂已补最小校验面——
`verify_client_cert` 经 `wtls::validity` 严格 DER 抽取 NotBefore/NotAfter
并对 `UnixTime::now()` 实校验（对标 X509Chain.Build 的 NotTimeValid 拒绝
面），窗口外报 `InvalidCertificate(ExpiredContext/NotValidYetContext)`，
DER 畸形报 BadEncoding，失败方向恒为拒绝；过期/未生效客户端证书放行的
身份门失效面已闭合。保留面：宽松臂仍不做完整链构建（签名链/锚信任豁免，
窗口内自签/未知 CA 证书放行）——此为本条尾段钉根分叉的宽松侧既定形态，
非新分叉。

尾段（刻意分叉防疑报）：钉根与 SAN 匹配两处收紧向分叉并入本条——rust
入站钉根臂 `client_verifier`（server.rs:394 起，本票顺齐原引 :383 的漂移）WebPkiClientVerifier 只
认 issuer 钉根链，无 C# AllowUnknownCertificateAuthority 兜底对等面；
rust 出站 rustls 仅认证书 SAN 匹配，无 C# DnsName 空 → SimpleName 回退
（GarnetTlsOptions.cs:218-227）对等面。两处 rust 均更严，系刻意分叉：
C# 过 rust 拒为预期，勿按 C# 宽松臂回改，后续对账不再另行取证。
同族延伸注记（§124c 双向回指）：webpki 中间证书深度上限 6 与 EKU
required_if_present 两处收紧系本段裁决同族，适用面与对拍口径见 §124c)，
本段不重复制文。
后果：收口前对账测试在 a/d 用例必然发散，勿判转写缺陷；底层票收口序见
工单 zcode-r27-wtls、zcode-r48-certfile、zcode-r30-defaults。

## 37. 成员级 TTL 三处登记缺口与 zset 采样窄窗口径偏离

工单 devsync-memberttl 登记（zcode-r80-devplan 派生；出处工单 zcode-r33-misctail、zcode-r34-memberttl2、zcode-r47-triage4246 及 r79-devsync2 抽验）。补齐成员级 TTL 散落注释与分叉台账：

a) purge_expired_len O(1) 矫正臂：仅在 doc/zh/collection.md 第 6 节第 3 条部分承接，本条补齐 deviations.md 偏差台账正位（对标 wedb/wcol/src/hash/hash_object_impl.rs）。定性为刻意分叉/已修未登。
收口注记：已由 §66b 完整展开。

b) mutated_by_ttl 信封升格写回闭环：此前仅在代码注释承载（hash_object_impl.rs），本条补齐台账登记。已修未登。
收口注记：已由 §66b 完整展开。

c) SetExpiration 拒绝臂幻影项不复刻：C# `SetExpiration`（`HashObject.cs:562-622`）幻影面系 XX/GT 拒绝臂 `GetValueRefOrAddDefault` 预插 0 值到期字典项（机理详见 §66b-2，两节同源），rust 只读探测现值、拒绝臂零副作用，在 `wedb/wcol/src/hash/hash_object.rs:508-512` 码内注释声明不复刻。定性为刻意分叉/已修未登。
（原登记「C# 拒载」措辞系 §45 的 `Dictionary.Add` 反序列化判重面混入，与本条无关，已删。）
收口注记：已由 §66b 完整展开。

d) zset 窄窗双态应答分叉：信封态与分层态对恰到期成员双态应答分叉（rust 信封态恒视同缺席 vs C# 窗内按存活，wedb/wcol/src/zset/sorted_set_object_impl.rs:337、:378-387、:560-576 对标 Garnet SortedSetObjectImpl.cs 的 DeleteExpiredItems 一次采样）。定性为修复向缺口，由底层票 zcode-r34-memberttl2 承载修复，登记不替代修复。
收口注记：已由 §66a 拍板收口。

## 38. SAVE/BGSAVE/COMMITAOF/LASTSAVE 失败语义分叉

工单 devsync-replpersist 登记（zcode-r80-devplan 派生；出处工单 zcode-r26-servercmd）。
rust 侧在 `wedb/wnode/src/resp/admin_commands.rs` 命令面与
`wedb/wnode/src/resp/garnet_api/slow.rs:checkpoint_command_slow`（:1340-1448）执行段，
SAVE、BGSAVE、COMMITAOF、LASTSAVE 失败时返回真实错误帧且不推进 last_save 时间戳
（原登记路径「wnode/src/resp/server」不存在，已订正为实位）。
C# Garnet 侧吞错真位：`DatabaseManagerBase.cs:185-205`
（TakeCheckpointAsync catch→return null）＋ `SingleDatabaseManager.cs:136-141`
（无条件推进 LastSaveTime）＋ `AdminCommands.cs:625-650`
（NetworkCOMMITAOF 弃结果恒回 ok），即 §52 已载版本；原登记文件归属
「CheckpointManager.cs」系错指——garnet/libs 无该文件（仅 Tsavorite
ICheckpointManager 族与 GarnetCheckpointManager.cs，均非该缺陷落点，全仓
rg 零命中亲验），已订正。
裁决：rust 行为取真错误帧与不推进 last_save 向，判改良向修复性偏离（安全与可观测性收紧，C# 假成功判缺陷）。
边界澄清：与 §29（自动检查点守护续跑）明确边界——§29 规范后台守护任务单次失败续跑，本条规范交互式命令对客户端返回真实错误帧与拒绝推进 last_save 时间戳的运维契约。
收口注记：已由 §52 完整展开。

## 39. 副本 attach 带损逃生门焊死

工单 devsync-replpersist 登记（zcode-r80-devplan 派生；出处工单 zcode-r25-synctrans）。
副本 attach 终点调用 try_add_replication_driver 恒传 false。C# Garnet 的 AllowDataLoss 带损放行能力（允许在缺失全量历史时强制同步）在 rust 侧被写死为 false，裁决缺席。
裁决：现状定性为刻意收紧，待底层票裁决是否恢复逃生门配置通道；在此之前不预判方向，留裁决槽。
收口注记：带损逃生门透传链已落地，本条按登记撤销处理，勿再判为未接线，裁决槽撤销。生产调用点均已透传配置派生值而非恒传 false——`wedb/wedb/src/server/replication/replica_sync_session.rs:107`（`allow_data_loss` 参数透传 `try_add_replication_driver`）、`:424`（`provider.allow_data_loss()`），无盘对位 `replication/diskless_replication/replica_sync_session.rs:415`、`replication/diskless_replication/replication_sync_manager.rs:410`；派生式 `wedb/wedb/src/server/cluster_provider/flags.rs:165-167` `fast_aof_truncate() && !on_demand_checkpoint()` 精确对标 C# `garnet/libs/server/Servers/GarnetServerOptions.cs:653-654`，装配接线 `wedb/wedb/src/server/boot.rs:172-181`（fast_aof_truncate / on_demand_checkpoint 双输入注入），锁测 `wedb/wedb/tests/synctrans_replication.rs`（另 `aof_sync_driver_store.rs` 锁语义面）。修复票 zcode-r25-synctrans。

## 40. 设备段目录三处防御分叉与 last_checkpointed_version 登记点

工单 devsync-replpersist 登记（zcode-r80-devplan 派生；出处工单 zcode-r41-wakeup、zcode-r48-aofopen、task/issue/zcode-r47-wkvstats）。仿 §27 检查点形态登记三处设备段目录防御分叉与一处统计指标登记点：

a) 杂散文件防御：设备段目录出现非段名格式杂散文件时，rust 忽略跳过，C# Garnet 报错拒启。

b) 段删除失败防御：设备段过旧被淘汰物理删除失败时，rust 上抛底层 I/O 错误，C# Garnet 捕获吞错假成功继续运行。

c) 段大小不匹配（SegmentSizeMismatch）：rust 统一拒启，保证数据完整性一致。

d) last_checkpointed_version 登记点：r47-wkvstats 发现一所述之版本号统计口径登记点。

边界澄清：注明与 §26（AofAddress 解析）、§27（检查点恢复短读拒启）、§29（AOF 体积超限守护续跑）的边界，避免重复立项；a/b/c 三小节已在 §74 依工单 zcode-r48-aofopen 完整展开收口。

## 41. AUTH 事务门多租户单向强隔离自有面

工单 devsync-proto 登记（zcode-r80-devplan 派生；出处工单 zcode-r26-txnarm、zcode-r47-triage4246；抽验见 r79-devsync2）。
rust 侧在事务排队期（MULTI 之后）若收到 AUTH 命令，直接拒绝并置 EXECABORT（wedb/wnode/src/resp/resp_server_session/auth.rs:35 RESP_ERR_AUTH_IN_MULTI，排队态由 NoMulti 元数据触发报错中止）；C# Garnet（RespServerSession.cs:668-675，RespCommandsInfo.json 中 AUTH 无 NoMulti）允许 AUTH 进事务队列白名单。
裁决：判定为本仓自有面刻意分叉（多租户单向强隔离，禁止在事务中动态变更身份破坏执行边界）。以 wedb/wnode/tests/transaction_tests.rs:964-999 回归测试与详细注释为裁决依据，登记即闭环。

## 42. 键规格 keyword 未命中有界化防御与 COMMAND GETKEYS 组合查询延伸

工单 devsync-proto 登记（zcode-r80-devplan 派生；出处工单 zcode-r31-commanddocs）。

a) 键规格 keyword 未命中有界化防御：rust 侧对解析过程中 keyword 越界/未命中进行边界检查有界化防御，对标 C# Garnet 潜在的越界读取缺陷。定性为改良向刻意分叉。
收口注记：已由 §72 完整展开。

b) COMMAND GETKEYS 父子命令组合查询延伸：rust 扩展支持父子命令复合查询场景，C# 原型未延伸支持。留裁决槽，待底层票 zcode-r31-commanddocs 收口。
收口注记：已由 §73 完整展开。

## 43. SETNAME 折叠、断连文案字符位差、MULTI 排队窗 EXECABORT 三面

工单 devsync-proto 登记（zcode-r80-devplan 派生；出处工单 zcode-r33-protocoldge）。单条多分面：

a) SETNAME 非 ASCII 字符折叠裁决：承接 dedupeapply D2，留裁决槽待底层票拍板。
收口注记：已由 §60 完整展开。

b) 断连文案字符位差（off-by-two）：rust 修复了 C# 断连报错信息中字符偏移计算差 2 字符的缺陷，定性为已修未登，登记即闭环。
收口注记：已由 §61 完整展开。

c) MULTI 排队窗 Invalid/ACL 统一 EXECABORT：rust 统一在 MULTI 排队窗内遇到语法错误或 ACL 鉴权失败时标记事务中止（EXECABORT），与 Redis 规范一致，定性为已修未登，登记即闭环。
收口注记：已由 §62 完整展开。

## 44. 向量重放 remove-before-insert 幂等收敛

工单 devsync-proto 登记（zcode-r80-devplan 派生；出处工单 zcode-r32-vecreg2）。
在向量写命令重放路径（replay_vector_set_add）中，rust 采用 remove-before-insert 语义实现重放幂等收敛；C# Garnet 的 TryAdd 遇重复项仅回 `VectorManagerResult.Duplicate` 结果码（VectorManager.cs:641），重放臂 `ApplyVectorSetAdd`（VectorManager.Replication.cs:471-475）对任何非 OK 结果抛泛化 `GarnetException`（文案无 Duplicate 字样）致重放任务中止——抛出点在 fire-and-forget 后台重放任务内，外层 catch `LogCritical` 后 rethrow（:393-410/:415-422），.NET 未观测任务异常不终止进程，实际后果为重放任务夭折/复制推流停摆，非进程崩溃（原登记「抛 Duplicate 异常直接崩溃中止」表述过强，已收敛为如实形态）。
裁决：改良向刻意分叉（保障重放安全性与崩溃恢复幂等性），对齐高可用要求，登记即闭环。

## 45. hash 信封反序列化重复 field 判重守卫缺失（zset 侧同位守卫双标准）

工单 devsync-storage 登记（zcode-r80-devplan 派生；出处工单 zcode-r27-wcolstruct 发现三、r79-devsync2 抽验）。
修复前形态：`wedb/wcol/src/hash/hash_object.rs` 反序列化 entries 逐条无条件 `update_size` + `insert`，缺失重复 field 判重守卫；而同位 `wedb/wcol/src/zset/sorted_set_object.rs` 已具判重守卫，双标准未收敛。C# 对位 HashObject.cs 使用 Dictionary.Add 遇重复键直接拒载抛出（旗依据仍真）。
裁决：定性为修复向缺口，具体修复归属底层票 zcode-r27-wcolstruct。
已修收口（zcode-r27-wcolstruct 发现三 + zcode-r46-wcolser 发现一并棒收口）：现码 hash 装载 entries 臂已具 `contains_key` 判重跳过保持首条（`hash_object.rs:165-167`），expirations 臂具 `ledger.get_time` 判重（:181）；zset 侧对位守卫见 `sorted_set_object.rs:233-235`（entries）与 :254（expirations）。两型均杜绝 `update_size` 重复累加与 ExpiryLedger 满额双入账虚标，回归测试 `wedb/wcol/tests/hash_deserialize_dup_field.rs` 在档。

## 46. BlockWaitFace 观察者 id usize::MAX 递减专用域

工单 devsync-storage 登记（zcode-r80-devplan 派生；出处工单 zcode-r38-colfix）。
rust 侧 BlockWaitFace 观察者 id 从 usize::MAX 递减分配专用域，导致慢路径等待体不响应 CLIENT UNBLOCK；§24 仅收口经纪未注入域，未覆盖此面。
裁决：注明与 §24 边界，裁决槽留给底层票 zcode-r38-colfix 拍板（修复对齐或判自有面）。
收口注记：已由 §63 拍板收口。

## 47. InitialUpdater 空对象幻键面增量与读臂删空自愈分叉

工单 devsync-storage 登记（zcode-r80-devplan 派生；出处工单 zcode-r40-delempty 发现二、发现三）。

### a) InitialUpdater 空对象幻键面增量（HDEL/HPERSIST/LSET 缺键臂）
与 §19 追加澄记同源同锚、纯指涉不另铺：C# `NeedToCreate` 判定矩阵（`garnet/libs/server/Objects/Types/GarnetObject.cs:34-79`）对 `HashOperation.HDEL` 与 `HashOperation.HPERSIST` 落默认 `_ => true`（仅 HEXPIRE/HCOLLECT 列 false），对 `ListOperation.LSET` 落默认 true；网络层无缺键预检直入 RMW；`InitialUpdater`（`RMWMethods.cs:44-74`）无 `HasRemoveKey` 检查直接挂载对象恒返 true。故 C# 在缺键上执行 `HDEL k f`、`HPERSIST k FIELDS 1 f`、`LSET k 0 v` 会挂载空 Hash/List 对象建幻键（EXISTS 回 1、TYPE 回 hash/list、HLEN 回 0）。
Rust 侧四族 `should_write_back` 统一 `(!existed && empty) -> false` 拦截，恒不落库、不建空键（EXISTS 回 0、TYPE 回 none），对齐 Redis 标准语义。
裁决：上游缺陷修复型偏离。证据链全量见 §19 追加澄记，严禁按 C# InitialUpdater 形态回改挂空对象。语义锁见 `wedb/wnode/tests/delempty_parity_locks.rs`。

### b) 读臂删空自愈分叉（HLEN/ZCARD 全员到期剔空即整键回收）
C# 一手形态：C# HLEN/ZCARD 走纯读通道 `ReadObjectStoreOperation`（`HashOps.cs:449-453` HashLength，ZCARD 对位通道 `ObjectStore/SortedSetOps.cs:872-874`），读臂对象内只做逐项过滤计数、不调 `DeleteExpiredItems`（`HashObjectImpl.cs:95-98` HashLength 直调 `Count()`、`SortedSetObjectImpl.cs:235-240` SortedSetLength 同型）；`Operate` 尾部（`HashObject.cs:301-302`）虽对空集置 `RemoveKey`，但读通道头 `ReadMethods.cs:27` 明言 Reads should not update the database、该标志在读臂无消费者、无删键动作——全字段到期的键在 C# 读后仍存活（EXISTS 1、TYPE hash、HLEN 0），直到下一次 RMW 写命令才经 `InPlaceUpdaterWorker` 删空臂消亡。
Rust 侧裁决：rust 读臂物理化矫正通路把「全员到期剔空」升格为整键回收——`sorted_set_commands/mod.rs:134-135` 的 Zcard 判定含 `REMOVE_KEY` 置写回，`run_sync_rmw` 空对象臂整键回收；异步侧 HLEN 经 `envelope_length_correct`（`rmw_helpers.rs`「全成员到期剔空落 apply_rmw_post_operate 空对象臂即删空自愈」）；阻塞族取件臂删空自愈同构判据见 `collection_item_source.rs:311-312`。rust 为 Redis 一致侧（集合成员全部到期键即消亡），刚性满足 `task/review.md` 3.2 与 `doc/zh/collection.md` 删空自愈红线（归零原子写墓碑、清理随键 TTL、原子销毁底层树与外部文件，杜绝幽灵空元记录与孤儿 TTL）。
裁决：上游缺陷修复型偏离。注明与历史既有裁决「HLEN/ZCARD 计数规约」的边界：该规约仅覆盖计数规约（O(1) 计数与水位校正），本条覆盖全员到期整键回收口径。双侧对账测试在全员到期后 HLEN/ZCARD 必然发散（C# EXISTS 1 vs rust EXISTS 0），严禁按 C# 读路径空对象键残留回改（回改即违反删空自愈红线）。语义锁用例见 `wedb/wnode/tests/delempty_parity_locks.rs` 的 `hexpire_all_expired_hlen_triggers_empty_deletion`。


## 48. DBSIZE 链首去重活键口径与 DELKEYSINSLOT 无过期过滤

工单 devsync-storage 登记（zcode-r80-devplan 派生；出处工单 zcode-r43-keyspace 与 zcode-r3-perf-dbsize-materialize）。

### a) DBSIZE 链首去重活键口径与流式计数
C# `UnifiedStoreGetDBSize.Reader`（libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:350-356）按原始日志逐记录计数（仅 `!IsInternalRecord && !CheckExpiry` 门槛，不查链首不去重），RCU 更新留下的旧版本记录与更新前版本一并计入；而 C# KEYS（DBKeys :160-163）经 `IterateLookupSnapshot` 按每唯一活键恰发一次。导致 C# 同一库态下 `DBSIZE` 与 `len(KEYS)` 在键发生覆盖更新后必然不等，属原型自身粗粒度缺陷（`count_keys_in_slot` 的仓内偏差声明已认定同族结论）。
Rust 侧 `db_size` 走链首地址校验去重 + 双域裁决 + 统一活键判定（与 `db_keys`/`string_keys_snapshot` 同源同判据），同一库态下 `DBSIZE == len(KEYS)` 恒成立，系对上游缺陷的修复（同 deviations 第 3/10/12/16/52 条「上游缺陷修复型」先例）。同时采用流式计数，逐键累加数量，避免全量物化 `Vec<Vec<u8>>` 键名列表的堆内存峰值与拷贝开销。
裁决：上游缺陷修复型偏离。登记「上游缺陷修复型」依据，警示切勿按 C# 原始形态回改成逐记录计数。

### b) DELKEYSINSLOT 槽位键删除无过期过滤
C# `DeleteSlotKeysScan`（libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:416-440）注释明示「every matched live key is deleted, including expired-but-not-yet-tombstoned records (no expiry filter)」，到期未墓碑化记录照常纳入删除与计数。
Rust 侧 `delete_slot_keys` 枚举臂改用不过滤到期键的用户面判定（仅前缀命中 + 链首去重 + 双域最新胜 + 非墓碑），到期未回收键（Due 与 Degrade 键）照常交由 `delete_string` 统一删除并计入删除数（`delete_string` 本身无过期门，连带清理随键 TTL 与 ETag 并推进 WATCH 版本），彻底对齐 C# 语义，杜绝槽位删除期因预过滤导致到期未回收键少报与漏删。
裁决：对标 C# 无过期过滤删除契约收敛，杜绝口径分叉。

## 49. RmwWindow 模块头裁决文两项事实缺陷勘误

工单 devsync-misc 登记（zcode-r80-devplan 派生；出处工单 zcode-r37-lockfix 发现 C 与第二节）。
wedb/wkv/src/session/rmw_window.rs:11-14 与 :41-43 模块头裸写的既有裁决注释存在两项事实缺陷：
a) 自陈契约自相矛盾：注释前半部声称的锁生命周期与后半部约束矛盾。
b) 持闩陈述为假：注释声称「仍持记录桶 ephemeral 闩」，而在默认 enable_revivification=false 配置下并不持闩（证据链：inplace.rs:191-192、session/mod.rs:551-553、config.rs:393）。
裁决：性质定性为裁决文勘误，纠正台账错误；模块头注释本体修改挂回底层票 zcode-r37-lockfix。
收口注记（r107-devaudit 台账收口，勘误对象已消失）：现码头 `wedb/wkv/src/session/
rmw_window.rs` 全文「仍持」零命中，b) 所劾的「仍持记录桶 ephemeral 闩」假陈述在码
不复存在；现码头 `:32-40` 已明述 `session/raw/write/inplace.rs` 的记录桶闩与本窗口
的 `user_key` 桶「是两个不同基」（`:33-34`）、两桶偶合时窗口持闩期内层取闩必失败并
按 `RETRY_LATER` 退避重试（`:35-36`，另见 `:115`）、取闩序恒为「user_key 桶 → 记录桶」
单向且无反序死锁面（`:38-39`），a) 的自陈矛盾亦由该段单一口径闭合。落笔归属说明：
本票源档记为「经 `57fe875` 整体重写」，现树核验该提交确对本文件作了大体量重写
（多键窗口 SmallVec 计划，`+239` 行），但其 diff 未触及 `//!` 码头段；码头现状段在现树
可溯至历史根 `fdb01f5`（init 压缩快照，码头全文自 init 即在位，「仍持」注释在现树
git 历史中从未出现于本文件），故勘误实落点早于可考历史，以现码头文本自证为准。
本体挂单 `zcode-r37-lockfix` 于 todo/ing/done/reject/issue 五池均不存在（疑随
2026-09-22 历史重置灭失），本条不再指向任何在途修改票；条目整条保留以存历史挂账
脉络，不作注销以免断链，后续复核按「已收口的裁决文勘误」处理，勿再据 b) 判定注释
缺陷仍在。

## 50. NetworkSenderThrottle 两不靠态

工单 devsync-misc 登记（zcode-r80-devplan 派生；出处工单 zcode-r41-wakeup 发现四）。
wedb/wbase/src/throttle.rs:44-100 的 NetworkSenderThrottle 发送端在途门限机制处于结构性失活状态，配置参数 network_send_throttle_max 任意取值均无任何可观测效果。
裁决：注明与 §14（订阅邮箱入端水位）不同面；两不靠态在案，留裁决槽给底层票 zcode-r41-wakeup 拍板（删除死机制或接入路径激活）。
收口注记：已由 §59 拍板收口。

## 51. 集群路径 PUBLISH 先发后订投递窗分叉

工单 devsync-misc 登记（zcode-r80-devplan 派生；出处工单 zcode-r40-pubsub2）。
在集群跨节点投递场景下，先 PUBLISH 后 SUBSCRIBE 竞争窗口中，C# Garnet 可能将消息送达，而 rust 恒定丢弃。
裁决：逐一注明 §6、§7、§8、§14 四条既有 PubSub 条目均不覆盖跨节点投递窗；分叉事实在案，修复挂底层票 zcode-r40-pubsub2，方向裁决留底层票。
收口注记：已由 §65 拍板收口。

## 52. 管理命令失败语义（SAVE/BGSAVE/COMMITAOF）

工单 zcode-r26-servercmd 登记（出处工单 zcode-r26-servercmd 发现三）。
回指注记：承接 §38 占位条目。
C# 原型实现中，DatabaseManagerBase.cs:TakeCheckpointAsync catch 全部异常仅打日志后返回 null，SingleDatabaseManager 仍无条件推进 LastSaveTime 为 UtcNow 并回 +OK；NetworkCOMMITAOF 忽略刷盘失败恒回 "AOF file committed"。
rust 侧在 slow-path storage 失败时返回错误帧，且因失败提前上抛，last_save_ms 不予推进；COMMITAOF 刷盘失败返回错误帧而非掐断连接。
裁决：上游缺陷修复型偏离。如实上抛存储失败，严禁吞错假成功与推进假记账；防回退，严禁按 C# 假成功形态回改。

## 53. 成员级 TTL 容器读臂两次时钟采样修复型偏离

工单 zcode-r27-wcolstruct 发现二（C# 同窗缺陷在案）。
HGETALL/HKEYS/HVALS/HRANDFIELD/ZRANDMEMBER 原实现 purge_expired_len（delete_expired_items 单次采样 t1 物理摘除）后按存活数写 RESP 头，迭代臂 is_expired 逐项重采样 t_i 过滤，expiry 落于 [t1, t_i) 的成员被计入头部声明却被跳过，声明数大于实际项数即客户端按声明读取时 RESP 流永久错位（C# WriteMapLength(Count()) + foreach IsExpired 双采样同窗，HashObject.cs:673 窗口内存活不足时 ElementAt 越界抛断流，同为损坏面）。
裁决：修复型偏离——purge 后主容器已无 expiry<t1 存留项，读臂（含两型 element_at）删除二次重采样过滤直接输出全部条目，声明数恒等写出数；窗口内「此刻恰到期」成员按 purge 时刻语义输出（比 C# 多写一项而非断流/错位）。行为锁 wcol/tests/member_ttl_reply_header.rs。
收口注记（工单 zcode-r143c-spopcnt 案一）：本条「声明数恒等实写数」不变量在 ZRANDMEMBER 的 n=0 退化点尚有缺口——成员级 TTL 全到期键（Present 存活零）上对象臂无早退守卫，负 count 头按 |count| 声明而 pick_k_random_indexes 空域零产出（头多宣断流）、正 count 钳 0 与无 count 形整帧零字节挂读；C# 同形态触 Random.Next(0) 抛断流系 §12 已登记原型危险面，不构成复刻豁免。修复与 HRANDFIELD/SRANDMEMBER 既有守卫同构（带 count → 版本感知空数组、无 count → null，result1 归零免 RMW 写回），一处收口快慢双臂与分层穿透臂；后续席面勿按 C# n=0 抛断流形态回改。行为锁 wcol/tests/member_ttl_reply_header.rs（zrandmember_all_expired_*）、wnode/tests/sorted_set_random_member_live_zero.rs。
收口注记（zcode-r159c-zpopcnt）：ZPOPMIN/ZPOPMAX 显式 count 形弹出臂同此不变量——环前一次剔除、环内免重采样，弹出臂环内到期成员视存活原样弹出并计入声明数，与 C# SortedSetPopMinOrMaxCount 逐字节全等。行为锁 wcol/tests/member_ttl_pop_reply_header.rs（沿 member_ttl_reply_header.rs 形制）。

## 54. UDS 绑定父目录自动创建（UdsGuard::bind）

工单 zcode-r44-uds 发现二登记。
C# 原型实现中，对 UDS 路径无父目录自动创建，父目录不存在时直接抛 ENOENT；
rust 侧 `UdsGuard::bind` 宽容保留 `create_dir_all(parent)` 自动创建父目录，若父目录已存在且为目录则成功，若创建失败或非目录则显式点名路径和真实 errno 上抛（严禁吞错静默）。
裁决：宽容性有意偏离，方便容器和自动化部署免预建目录树；失败时透传真实错误，杜绝静默生根排障困难。

## 55. TLS 证书装载面收窄为 PEM-only 与 cert-password 键路径化

工单 zcode-r48-certfile 发现三登记。
回指注记：承接 §36b 占位条目。
C# 原型实现中，`CertificateUtils.cs:GetMachineCertificateByFile` 按文件头字节嗅探格式（`IsPemFile` 认 `"-----BEGIN"` 走 PEM 臂，否则按 PKCS#12/PFX 装载并消费 `certPassword` 为解密密码），支持 PFX/PKCS#12 容器及带密码加密私钥。
rust 侧 `wtls::load_certs` 与 `load_private_key` 刻意收窄为 rustls_pemfile 纯 PEM 解析，不提供 PFX/PKCS#12 与 DER 格式嗅探及私钥解密；配置参数 `cert-password` 按 C# PEM 臂对位映射为独立私钥文件路径（与 boot/wconf 注释一致）。
裁决：平台纯净性刻意收窄。清理专有平台/冗余容器格式支持，全链路仅接受标准 PEM 格式；如需 PFX/DER 证书请在部署前经 OpenSSL 转为 PEM。

## 56. cert-subject-name 配置删员与互拒门不设（Windows 证书存储专有面）

工单 zcode-r48-certfile 发现五登记。
回指注记：承接 §36c 占位条目。
C# `GarnetTlsOptions` 声明 `CertSubjectName` 配置项，构造与 `UpdateCertFile` 强制 `cert-file-name` 与 `cert-subject-name` 互斥互拒，subject 臂经 `CertificateUtils.cs:GetMachineCertificateBySubjectName` 从 Windows 本机证书存储（`X509Store`）按主体名检索最新证书。
rust 侧按跨平台纯净性规范彻底删除 Windows 证书存储专有集成，`wconf` 不提供 `cert-subject-name` 配置字段，`wtls` 无第二证书来源，亦无对应互拒门。
裁决：平台依赖清理向有意偏离。契约面删员已固化，无行为危害。

## 57. TLS 证书加载失败双向 fail-fast 严格语义

工单 zcode-r48-certfile 发现六登记。
回指注记：承接 §36d 占位条目。
C# 原型实现中，启动期 `ServerCertificateSelector` 构造器证书加载失败仅记日志并置 `sslServerCertificate` 为 null，进程仍正常启动；后台定时器 5 秒重试自愈仅在配置 `cert-refresh-freq` > 0 时成立（`ServerCertificateSelector.cs:56-66/:78-86`，装载失败时首拍提前为 5 秒重试间隔），默认 0 不挂表、证书永留 null 无任何自愈（§36d 的「刷新定时器在场…否则…」形态为准，原登记漏该前提，已补）；运行时 `CONFIG SET cert-file-name` 遇到非法路径同样返回 true（假 +OK），新 selector 持 null 导致后续 TLS 握手全断。
rust 侧双向均采取严格 fail-fast 语义：启动期 `ServerTlsConfig::from_pem_files` 失败直接上抛拒启，杜绝无可用证书的半死实例暴露网络；运行时 `CONFIG SET cert-file-name` / `cert-password` 加载失败回传错误帧，并严格保留原活跃证书（禁半态换装），保障既有连接与后续握手不中断。
裁决：上游缺陷修复与安全收紧型偏离。如实上抛坏配置，严禁吞错假成功与半死状态。

## 58. 事务内禁 HELLO 认证停泊与换租（对齐 SELECT/AUTH 事务窗禁语）

工单 zcode-r41-scalar 发现一落地。
C# 原型无多租户隔离与冷租户概念，HELLO 命令未声明 NoMulti（allowed_in_txn 为 true），在事务排队期与重放期与普通命令同形无拦截。
rust 多租户架构下，HELLO 携带 AUTH 认证可触发冷租户异步装载停泊（park_cold_context_load），在 MULTI/EXEC 重放窗内停泊将重入物化并导致跨租户 txn.reset()，撕裂事务重放流并在 AOF 中残留无主 TxnStart。
裁决：事务窗禁停泊围栏与对称加固——
a) 排队期对齐 SELECT 异库中止先例（txn_resp_commands.rs），HELLO 携带 AUTH 选项时直接报错回 RESP_ERR_HELLO_IN_TXN_UNSUPPORTED 并置事务 Aborted，EXEC 时整体中止（回 -EXECABORT），AOF 无 TxnStart，会话上下文保持旧值；
b) process_hello_command 与 park_cold_context_load 设事务态门禁，事务在途时严禁登记 SlowWait，直接弃置 ColdContextPending；
c) process_messages 挂起 break 臂设事务窗断言（唯一合法停泊窗为普通命令窗口）；resolve_slow_wait_into 物化门收紧为仅在非事务窗（txn_state == None）消费 cold_ctx。
d) 重放窗一态收口（波次票 wnode-exec-replay-hello-acl-txn-gate-overflow 追记）。C# NetworkSKIP（TxnRespCommands.cs:105-204）无 HELLO/ACL 拒臂、HELLO 与 ACL 族 Flags 均无 NoMulti（libs/resources/RespCommandsInfo.json HELLO :2104-2108、ACL|LIST :41-45），重放窗 NetworkHELLO/ACL 族无事务门正常执行（RespServerSession.cs:1090、AdminCommands.cs:65-74）——两形本仓排队期照常 +QUEUED。旧 a) 的漏斗预筛（core.rs dispatch_via_garnet_api）在重放窗被扩大化：无 AUTH 合法形 HELLO 的 EXEC 元素亦回错误帧（与 a)「中止面恰等于携 AUTH 组形集」自相矛盾），ACL 族十子命令更共回 HELLO 错位文案。现裁决收口为一态——
  - 文法合法且不携 AUTH 的 HELLO 形零存储点查、零停泊（协议回显/mode/role/客户端名均会话元数据，process_hello_command_state 空 username 臂全仓无 await 点查），重放窗经无存储同步快臂直出应答 map（单点 auth.rs commit_hello_state_and_write_reply，与异步臂一处定义），执行窗会话协议版本/客户端名真实落位，与 C# 重放窗执行同形；语法错形与携 AUTH 形维持围栏拒绝帧（b) 停泊红线不动）；
  - ACL 族规则读写须存储点查串行停泊，事务窗严禁（§58 立条红线），维持重放窗拒但回专属帧 RESP_ERR_ACL_IN_TXN_UNSUPPORTED（"ERR ACL is currently unsupported inside a transaction."）——裁文案分支、不采排队期补中止：后者使 EXEC 一律 -EXECABORT、连坐已排队正常命令，对契约更毁；「ACL 重放窗回错误帧而非 C# 正常应答」系多租户 ACL 存储真源架构（doc/zh/db.md §3）的必然让位，与 C# 残差分叉以本条登记；
  - park_cold_context_load 事务窗围栏文案改由调用方传入：SELECT 臂回 RESP_ERR_SELECT_IN_TXN_UNSUPPORTED、AUTH 臂回 RESP_ERR_AUTH_IN_MULTI、HELLO 臂回 RESP_ERR_HELLO_IN_TXN_UNSUPPORTED，杜绝 b) 围栏三域共用 HELLO 文案错位（围栏机制不变）。
  锁面：`wnode/tests/exec_replay_hello_acl_txn_gate.rs` 三锁（无 AUTH HELLO 重放直出／ACL 专属拒帧／同库 SELECT 冷库窄窗专属帧）；`transaction_tests.rs` §58a 回归锁族执行窗断言随收口更新（值位 AUTH 形元素改钉正常 map，缺尾参文法错形维持围栏帧）。

## 59. NetworkSenderThrottle 单写者泵下架构性失活（network_send_throttle_max 旋钮无效化）

工单 zcode-r41-wakeup 发现四登记。
回指注记：承接 §50 占位条目。
C# GarnetTcpNetworkSender 的 Throttle 为 SemaphoreSlim(0) 信号通知面（GarnetTcpNetworkSender.cs:37），ThrottleMax=8 另为字段（:47，--network-send-throttle 可调）；背压靠计数门——Send 每派发一次异步写以 Interlocked.Increment 增 throttleCount（:312，SAEA 异步发送堆叠，慢客户端内核发送缓冲打满时在途可真实触顶），仅 cnt > ThrottleMax 才 throttle.Wait() 背压阻塞网络线程（:319-320），完成回调 Decrement 达阈 Release 放行一个通知（:336-337）。ThrottleMax 可观测可调（原登记「Throttle 为 SemaphoreSlim(ThrottleMax)（:47）」机理失真，结论不变、已订正为如实机理）。
rust 侧 NetworkSenderThrottle（wbase/src/throttle.rs，1:1 转写）生产消费点仅网络泵写出段命令臂与推送臂两处（drive.rs），enter_send → write_all 内联挂起 → exit_send 严格串行于同一泵任务（单写者，无第二并发写者），在途计数恒 ≤ 1，enter_send 背压分支与 close() 唤醒面结构不可达——不可达系单写者 enter/exit 串行时序（enter 时刻计数恒已归 0），非构造 .max(1) 保底推演（throttle_max==1 即配置 0/1 时在途 1 并不小于 1，原「恒小于 throttle_max」推演有缝，已订正）；实际背压由 write_all 内联挂起隐式承担。旋钮 network_send_throttle_max（server.rs 装配形对位 C# 同名旋钮）接线在但任意取值行为全同。
裁决：架构性失活登记——保留机制与旋钮（装配形兼容，防断嵌入式 builder 面），throttle.rs 模块头同文声明；单写者串行泵下语义脱钩无行为危害，运维按 C# 心智调参预期无效，防后续轮次反复疑报。

## 60. 客户端名校验严拒非 ASCII 字符（防乱码落名分叉，勘误 r26-clientarm 结论）

工单 zcode-r33-protocoldge 发现一登记（dedupeapply D2 单一承接）。
回指注记：承接 §43a 占位条目。
C# 原型实现中，`TryGetClientName`（`SessionParseStateExtensions.cs:103-127`）取 `parseState.GetString` → `ParseUtils.ReadString`（`ParseUtils.cs:196-199`），内部使用 `Encoding.ASCII.GetString`。.NET 的 ASCII 解码器对 `>=0x80` 的非 ASCII 字节会将其替换为问号 `?`（0x3F）而非 UTF-8 替换符 U+FFFD；0x3F 落在 33..=126 打印性允许区间内，导致打印性校验通过，非 ASCII 字符被 C# 接受并以每字节降级为一个 `?` 的形式落名（如 `CLIENT SETNAME "café"` 在 C# 中存为 `"caf??"` 并回 `+OK`）。
rust 侧 `try_get_client_name_bytes`（`wnode/src/session_parse_state_extensions.rs:68-77`）在 `from_utf8` 校验通过后，逐字节检查 `(33..=126)`；非 ASCII 字节（如 0xC3 0xA9）超出区间直接返回 `None` 并回 `RESP_ERR_INVALID_CLIENT_NAME` 错误帧。
裁决：保持 rust 严拒现状（UTF-8 硬校验 + ASCII 可打印区间严格判定），杜绝乱码（mojibake）降级落名。
勘误引述：历史票 r26-clientarm 覆盖面对账第 54 条曾以「C# 替换 U+FFFD 后必落 >126 失败臂」为前提误判双侧等价；因 .NET Encoding.ASCII 实际替换为 0x3F，该前提与事实相反，特此勘误，防后续按「已证等价」误判或回改。
锁面：`wedb/wnode/tests/resp_tests.rs` 的 `client_set_name_non_ascii_rejected` 与 `hello_setname_non_ascii_rejected`。

## 61. 帧头终止符不符 UnexpectedToken 回显首个不符字节（消除 C# 越界邻位读风险）

工单 zcode-r33-protocoldge 发现二登记。
回指注记：承接 §43b 占位条目。
C# 原型实现中，`TryReadSignedLengthHeader`（`RespReadUtils.cs:427`）在检查终止符是否为 `\r\n` 之前，先执行了 `ptr = readHead + 2`，在终止符比对不符时抛出 `ThrowUnexpectedToken(*ptr)`；回显的是终止符两字节之后的第 3 字节（如 `*3X\r\n` 回显 `\n`）；且当终止符恰收在接收缓冲区批尾（`readHead + 2 == end`）时，`*ptr` 发生越界读取接收缓冲 end 之后的邻位脏字节，依赖分配余量兜底不崩溃。
rust 侧 `try_read_signed_length_header`（`wresp/src/read.rs`）在 `read_head.len() >= 2` 判定下，对比 `b"\r\n"` 并恒在界内取首个不符字节（若 `read_head[0] != b'\r'` 取 `read_head[0]`，否则取 `read_head[1]`）回显（如 `*3\rX\r\n` 回显 `'X'`），消除了 C# 偏移 +2 的字符位差与批尾越界读邻位内存的安全隐患。
裁决：上游 off-by-two 缺陷修复与越界读消除（崩溃防御型先例，同第 3/5 条）。read.rs 补充对标注释，保持 rust 取位准确现状，严禁按 C# 越界偏移回改。
锁面：`wedb/wnode/tests/resp_server_session_tests.rs` 的 `protocol_error_bad_length_header_terminator_unexpected_token_lock` 及 `wresp/tests/parse_state_read.rs` 的 `try_read_signed_length_header_bad_terminator_returns_first_unexpected_token`。

## 62. MULTI 排队期未知命令与权限拒绝统一 EXECABORT 收敛（Redis 标准语义）

工单 zcode-r33-protocoldge 发现三登记。
回指注记：承接 §43c 占位条目。
C# 原型实现中，`RespServerSession.cs:ProcessMessages` 以 `if (cmd != RespCommand.INVALID)` 为分派总门，解析级 Invalid（未知命令/未知子命令/内联垃圾行）仅落 else 臂置 `containsSlowCommand = true`，不进入事务分派、不调 `TransactionManager.Abort`，排队态持续为 Started，后续 EXEC 照常回放队列中有效命令；ACL/脚本权限拒绝走 else 臂写 NOPERM/NOAUTH/NOSCRIPT 错误帧后同样不 Abort。C# 仅在 `NetworkSKIP` 校验臂（`AllowedInTxn`/arity 等）与 DISCARD 执行 Abort。
rust 侧在 `resp_server_session/core.rs:process_messages` 将未知命令/内联垃圾行（Invalid）、ACL 拒绝（NOPERM/NOAUTH）以及脚本拒绝（NOSCRIPT）在事务排队期统一置 `queue_failure = true`，并在循环尾单点调用 `abort_pending_transaction` 将事务管理器与会话镜像同置为 `TxnState::Aborted`，随后 EXEC 统一拒绝并回 `-EXECABORT Transaction discarded because of previous errors.\r\n`，严禁回放队列中的任何命令。
裁决：保持 rust 现状。符合 Redis 官方标准事务契约（All-or-Nothing 原子性保障），杜绝入队失败命令后写命令仍被执行的数据污染风险。裁决源自归档工单 `wnode-txn-queue-unknown-and-permission-error-bypass-abort-breaks-execabort`（r26-txnarm 边界）。
锁面：`wedb/wnode/tests/txn_queue_abort_execabort.rs`（全 5 项入队失败中止用例）。

## 63. 列表慢路径阻塞等待面观察者 ID 专用域与 CLIENT UNBLOCK 隔离

工单 zcode-r38-colfix（增量二）与 zcode-r60-list2 登记。
回指注记：承接 §46 占位条目。

C# 一手形态：
C# 阻塞命令族（BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP）经 Tsavorite pending 慢路径重放时，仍持原会话实例（`RespServerSession`）并以其原 `sessionId` 重入 `CollectionItemBroker.GetCollectionItemAsync`（`AsyncUtils.BlockingWait`，`CollectionItemBroker.cs:127-163`），慢路径等待体仍驻留在原会话的观察者槽位中；
`CLIENT UNBLOCK <id>`（`libs/server/Resp/ClientCommands.cs`）按 `ActiveConsumers` 查找目标会话，若该会话正处于慢路径阻塞等待，可被同会话 ID 正常解除；对于未分配或负数 ID，C# 在会话表中查无命中直接返回 `:0`。

Rust 侧裁决：
在 compio thread-per-core 架构下，慢路径异步执行域（`exec_slow`）脱离了网络会话上下文（无会话可达面）。
1. 观察者 ID 专用域：慢路径阻塞等待（`wedb/wnode/src/resp/objects/list_commands/slow.rs` 的 `BlockWaitFace::wait`）采用原子计数器自 `usize::MAX` 递减生成专用 `session_id`，登记入经纪的 `session_id_to_observer` 映射，确保多连接并发慢路径等待互不顶替；慢路径等待体的生命周期由超时竞速、写唤醒出件以及 `ObserverDropGuard`（网络泵终止/会话 dispose/脚本槽取消时摘除观察者）自治闭环，不依赖会话级取消。
2. CLIENT UNBLOCK 门禁隔离：`network_clientunblock`（`wedb/wnode/src/resp/client_commands.rs`）显式对 `client_id < 0` 设单点门禁直接返回 `:0`（对齐 C# 负数 ID 查无会话回 0），彻底杜绝客户端输入负数 ID 经 `as usize` 投影到高位 `[2^63, 2^64)` 别名命中并误伤 `BlockWaitFace` 慢路径观察者。

后果与保留理由：
正会话 ID（小正整数）与 `usize::MAX` 递减专用域严格隔离，因此客户端执行 `CLIENT UNBLOCK <id>` 无法寻址或解除其他会话处于慢路径冷键等待中的等待体（仅可解除快路径挂起的等待体；C# 慢路径仍持原会话 ID 可被解除）。这是 Rust 异步执行域解耦架构下的有意偏离。
负数 ID 输入恒回 `:0`，慢路径阻塞语义不被跨会话篡改。
锁面：`wedb/wnode/tests/list_blocking_cold_wait.rs` 的 `blpop_cold_tombstone_client_unblock_negative_id_noop_and_timeout_kept`。

## 64. 启动期主机名解析在 compio 运行时线程内同步执行（出站解析异步安全，纯启动相位差）

工单 zcode-r47-endpoint 发现四登记。
C# 原型实现中，bind 地址与主机名解析在 Options 构造校验段同步执行（`Options.cs:795 TryParseAddressList` → `Format.cs:108 Dns.GetHostAddresses`，非异步上下文，阻塞宿主启动线程属预期）；出站域名解析异步化且全地址回退（`GarnetClient.cs:357-372 Dns.GetHostEntryAsync` 逐地址 `TryConnectSocketAsync`）。
rust 侧 `ServerEndpoint::parse` / `parse_many` 与 `parse_announce_ip`（`announce.rs`）使用标准库 `to_socket_addrs`（同步 `getaddrinfo`），调用点位于 `ServerBootstrap::run_async` 的 `rt.block_on` 启动回调内——显式主机名 bind/宣告时 compio 启动线程被同步阻塞至解析完成，但此时 accept 循环尚未拉起、无任何数据面并发，纯属启动初始化相位差异。出站端点解析则经 `compio-net` 的 `spawn_blocking_at` 线程池解析并逐地址回退，与 C# 全地址异步回退同构。
裁决：登记备案——无运行期危害，无需将启动期短时解析复杂化引入运行时跳板；防后续轮次误报「compio 运行时内阻塞解析」。

## 65. 集群 PUBLISH 同步直投（对标 C# TsavoriteLog 延迟消费窗）

工单 zcode-r40-pubsub2 登记（r14 遗产处置落地）。
回指注记：承接 §51 占位条目。
C# 集群路径双向均入日志：发送侧 `NetworkPUBLISH` 聚集态走 `ClusterPublishAsync`（`PubSubCommands.cs:146`），收端 `NetworkClusterPublish` 调 `Publish` 追加 `TsavoriteLog`（`RespClusterBasicCommands.cs`），常驻 `StartAsync` 消费环（`SubscribeBroker.cs`，`ScanSingle` + `ConsumeAllAsync`）按消费时刻订阅图广播，「PUBLISH 先到、SUBSCRIBE 后到但消费在后」的并发窗口 C# 可能送达。单机 `NetworkPUBLISH` 无集群会话时同为 `PublishNow`（`PubSubCommands.cs:134`）。
rust 侧无磁盘日志介质亦无常驻消费任务，收端 `network_cluster_publish`（`wedb/wedb/src/server/cluster_session/basic.rs`）与单机路径统一收敛为 `publish_now` / `publish_shard_now` 同步直投（`wedb/wpubsub/src/subscribe_broker.rs`），按当前订阅图广播，无订阅者即在 `is_idle` 早退臂静默丢弃。
保留理由：系统无日志介质，单机与集群统一收敛至同步直投单机制，杜绝为复刻延迟窗口而引入伪日志或轮询任务。
后果：单机路径双侧一致；集群路径在「PUBLISH 先到、SUBSCRIBE 后到」的竞态窗口下，投递语义双侧相反（C# 延迟消费可能送达，rust 恒按当下订阅图直投丢弃）。

## 66. 集合成员级 TTL 写臂窄窗到期复查守卫与账本治理（ZADD NX/GT/LT 与 ZINCRBY 缺席语义收敛）

工单 zcode-r34-memberttl2 登记（出处工单 zcode-r34-memberttl2 发现二与 r10-memberttl 缺口合并）。
回指注记：承接 §37（§37a-d）占位条目。

### a) 信封态写臂窄窗到期复查守卫（ZADD NX/GT/LT 与 ZINCRBY / HSET / HINCRBY）
回指注记：承接 §37d 占位条目。
C# 原型实现中，`SortedSetAdd`（`SortedSetObjectImpl.cs:90`）与 `SortedSetIncrement`（`:321`）以及 `HashSet`（`:187`）入口仅调一次 `DeleteExpiredItems`（`:94/:325`）；在采样与成员判定之间恰到期的成员，C# 按存活处理：`SortedSetAdd` 的 NX 拒绝更新（`:171-181` continue 臂回 0，continue 在 :180）、GT/LT 判定受旧分值干扰，`SortedSetIncrement` 就地覆写分值但不动账本，残留已过去旧刻度致成员落盘（`serialize_wire`）时被整体滤除。
Rust 侧在 `sorted_set_add` 字典探测前与 `sorted_set_increment` 更新前加到期守卫：若 `self.is_expired(member)` 则先调 `self.rem(member)`（摘双索引 + 退账 + 清账本，与 `hash_set` / `hash_increment` 守卫同形）。恰到期成员落真缺席臂：NX 不拦新增、GT/LT 按缺席语义新增、ZINCRBY 按新增臂落 `incr_value` 且清账本，杜绝残留旧刻度写回滤除。与分层态三态判据（`tree_member_state` / `expired_hit`）行为同构收敛。
裁决：上游缺陷修复型偏离（同第 3/10/12/16 条「上游缺陷修复型偏离」先例）。
锁面：`wedb/wnode/tests/sorted_set_ttl_test.rs`、`wedb/wnode/tests/hash_ttl.rs` 与 `wedb/wnode/tests/tiered_field_ttl.rs`。
增量族存期／覆写清期方向回指注（工单 zcode-r151c-hincrby 案二，防双向回改，与本条同条并册不另立号）：本节只裁到期窄窗守卫，未载存续面；本注钉家族内刻意不对称在册——HSET/HMSET 覆写存活字段**清期**（C# `HashSet` 变更分支 `HashObjectImpl.cs:226-236` `expirationTimes.Remove`＋原文注释 "To persist the key, if it has an expiration"；rust `hash_set` 覆写臂 `hash_object_impl.rs:309-312` `ledger.remove_expiration`；分层 HSET 折叠臂「覆盖写清字段 TTL」注位），而 HINCRBY/HINCRBYFLOAT 对存活字段累加**存期**：C# `HashIncrement`(:285-351)/`HashIncrementFloat`(:353-432) 全臂零 expirationTimes 触碰，rust 信封增量臂存活分支零 ledger 触碰、分层臂以 `old_expiry` 原值回写（Hincrby 臂「存活成员增量保留既有 TTL」注位，浮点臂逐点对位），到期字段一律按缺席起算新字段无期（即本节窄窗处理，勿外推到存活臂）。双向禁回改：勿按「写命令覆写字段应一致清期」直觉在增量臂补 ledger 清理（HTTL 将由正剩余静默变 -1、字段到期隐没变永存，无帧面征兆），亦勿按真 Redis/本节到期形外推在累加路径挂清零。comparator 双侧同严格小于基准（`expiry_ledger.rs` is_expired_at 与 `member_ttl.rs` member_expired_at），无 <=/< 分叉。锁面：`hash_ttl.rs::incr_family_preserves_live_field_ttl_envelope` 与 `tiered_field_ttl.rs::tiered_hash_incr_preserves_live_field_ttl`。

### b) mutated_by_ttl 信封升格写回闭环与 SetExpiration 拒绝臂幻影项不复刻
回指注记：承接 §37a/b/c 占位条目。
1. 读路径惰性剔除升格写回：C# 读路径剔除仅常驻内存，若无后续显式写则不落盘；rust 设立 `mutated_by_ttl` 标志，在读命令触发剔除后升格写回存储层，配合严格删空自愈杜绝幽灵成员装载复活。
2. SetExpiration 拒绝臂幻影项消除：C# `SetExpiration`（`HashObject.cs:562-622`）的入队均位于条件通过之后（:598/:600/:614/:616），不存在「条件不满足前先入队」的孤儿堆项（原登记该机制失实，且所引 :499-503 落在 Remove 区，已订正）；真实幻影副作用系 :583/:585 `CollectionsMarshal.GetValueRefOrAddDefault` 在 exists=false 拒绝臂（XX/GT）先行插入 0 值到期字典项——是字典项预插非堆项入队、是 XX/GT 拒非 NX/GT/LT 拒，被拒字段自此在 HGET/HLEN/HEXPIRE 眼中失活且记账缺失。rust 只读探测现值、条件满足确认后才入账本，拒绝臂零副作用不产生幻影字典项（准确版见 `hash_object.rs:508-512` 码内注释，与 §37c 同源互指）。
3. purge_expired_len 摊还 O(1) 计数规约：直读集合计数前先走堆序清退，保持计数严格 O(1) 与删空自愈闭环。
裁决：收敛至成员级 TTL 统一规约体系，杜绝状态分叉。

## 67. 控制台日志面三点刻意分叉（输出流 / 时间戳制式 / 类别形态）

工单 zcode-r63-logmacro 发现三登记（r63-recheck62 抽验在码；doc-deviations-wording-and-attribution-five 顺锚订正）。
C# `GarnetServer.cs:118-122` 构造器 `builder.AddSimpleConsole` 仅设 `SingleLine` 与 `TimestampFormat`：默认写 `Console.Out`（stdout）、`TimestampFormat = "hh::mm::ss "`（上游 quirk：12 小时制 + 双冒号分隔）；.NET SimpleConsole 单行格式恒含「级别: Category[EventId]」类别前缀段且无开关可关——真实分叉在类别形态（C# `Category[EventId]` 前缀段 vs rust `<target>` 角括号段），不在有无。文件面 `FileLoggerProvider.cs:75-80` `FileLoggerOutput.Log` 单行模板为 `[eventId.D3.date] (level) <category> message`（含 eventId 三位零填充段）。
rust 侧 `wedb/wnode/src/logging.rs` 的 `ConsoleLogger`（:341-366）与 `format_record`（:62-68）：控制台行写 stderr（stdout 留给协议/数据重定向面，`systemd`/管道采集语义更稳）；时间戳 `"%H:%M:%S "`（24 小时制单冒号，弃上游 12 小时制+双冒号 quirk）；单行含 `<target>` 类别段（与文件面模板同构，`logging.rs` 现有单测已锁）；文件模板无 eventId 段（rust 无 EventId 概念，target 承担类别）。
裁决：三点（输出流 / 时间戳制式 / 类别形态）均为可观测但纯外观分叉，声明为有意选择，不复刻上游 quirk；代码行为零变更，纯文档治理登记。
## 68. RI 预览门恒开（EnableRangeIndexPreview 旋钮不设）

工单 zcode-r30-defaults 立项一登记。
C# `EnableRangeIndexPreview` 默认 false（`GarnetServerOptions.cs:673`、`defaults.conf:539`）：RangeIndexManager 仅在预览开启时装配，预览关闭时全部 RI.* 用户命令逐臂前置拦截回 "ERR Range Index (preview) commands are not enabled"（`RespServerSessionRangeIndex.cs` 各门），AOF 重放 RI 记录直接抛 "RangeIndexPreview disabled; Replay failed"（`AofProcessor.cs:636-639`）。
rust 侧 RI 引擎是自适应混合分层存储的承重组件（集合升阶 wbftree 的存根保序原语 RIPROMOTE/RIRESTORE、升阶树数据恢复/复制经既有 `RangeIndexStreamChunk` AOF 通道灌入与重放，见 `wnode/src/service.rs` AofSinkContext 与 `wkv/src/store/event.rs`），预览关闭即戳穿分层恢复链，C# 式「预览关 → 管理器 null → 命令与重放全拒」语义不可达；补半截旋钮（仅门命令面）又会造出「预览关闭却持续产生 RI 记录」的更怪分叉。
裁决：RI.* 命令面恒开为刻意偏差（分层内部机器恒开的自然推论），`enable-range-index-preview` 旋钮不设、CONFIG 面不登记；原 `ri: Option<R>` 门参数与 RI_DISABLED 死文案已随本条删除（单套机制）。
锁面：`wnode/tests/range_index_tests.rs`（命令族默认可用性回归）。

## 69. 读缓存页容量随主存页（ReadCachePageSize 旋钮不设）

工单 zcode-r30-defaults 立项五登记。
C# `ReadCachePageSize` 默认独立 "4m"（`defaults.conf:73`、`GarnetServerOptions.cs:592`），ReadCacheMemorySize "1g" 推导 256 页。
rust 侧读缓存页容量恒复用主存 hlog 页，页数常量 `DEFAULT_READ_CACHE_NUM_PAGES=64`（`wkv/src/config.rs`），满配 64 页 × 16m = 1g 总内存预算与 C# 相等，仅换页粒度与二机会窗口粒度 4 倍于 C#；`read-cache-page-size` 旋钮不设。注：页容量非恒 16m——随内存预算自适应规划 clamp [64KB, 16MB]（`wbase/src/cfg.rs:34` 系 16MB 上限、`wkv/src/config.rs:323-324` 规划式、`whlog/src/config.rs:9` 系 64KB 下限基线，`minimal()` 基线 64KB `wkv/src/config.rs:391`），16m 为上限；「64×16m=1g 与 C# 相等」仅在预算充裕机成立，预算受压机页容量随规划缩水、总预算同步缩小。
裁决：总预算对齐、粒度随主存页为刻意口径，读缓存默认关（两侧一致），影响面限显式开启者，不回改。
回锚注记（doc-deviations-memory-shape-five-groups-registry）：尺寸族其余旋钮收形（内联尺寸对／初始读尺寸／reviv 几何／缓冲池预算对／pagecount 折叠与 tree_cache_budget 反向形）统一登记见 §111，本条只钉读缓存页容量随主存页一面，两条勿重复裁决。

## 70. 初始索引容量与主存内存预算为自适应规划（IndexMemorySize/LogMemorySize 固定值不复刻）

工单 zcode-r30-defaults 立项六登记。
C# 固定 IndexMemorySize "128m"（2,097,152 桶）、LogMemorySize "16g"（`ServerOptions.cs:67/:41`、`defaults.conf:40/:25`）。
rust 侧嵌入基线 `DEFAULT_INDEX_SIZE=65536` 桶（4MB，`wkv/src/config.rs`），生产走 `StoreConfig::auto` 内存预算规划：预算 = 25% 物理内存钳 [256MB, 32GB]（`DEFAULT_MEMORY_PERCENT`），索引取预算 37.5% 向下取 2 的幂，日志页数取余量；规划器系既定改良（容量规划契约见 wconf 注释与 `wkv/src/config.rs`）。
裁决：自适应规划不取 C# 固定值，属既定架构改良的登记性声明，后续对拍轮勿复报。
回锚注记（doc-deviations-memory-shape-five-groups-registry）：LogMemorySize 的 pageCount 折叠形（--pagecount/--readcache-pagecount 双旋钮缺席）与其余四组尺寸几何旋钮收形见 §111，本条只钉初始索引容量与主存预算自适应规划一面。

## 71. 集群出站 TLS 期望名默认回落对端 host（ClusterTlsClientTargetHost 固定串不复刻）

工单 zcode-r30-defaults 立项七登记。
C# `ClusterTlsClientTargetHost` 默认固定 "GarnetTest"（`defaults.conf:232`），对真实证书必然失配；rust `tls_client_target_host` 默认 None，建连时回落对端地址 host 段（剥 IPv6 方括号），校验机制同向（出站均校验证书名），期望名来源更合理。
配套 fail-fast 门：出站校验开启（`tls_server_cert_required=true`，缺省即开）且目标名为空时构造期即拒启（`wtls/src/client.rs` empty_target_host_gate），杜绝静默恒真；显式配置目标名后正常装配。
裁决：回落对端 host 为刻意选择，不复刻 C# 测试串；双空（无目标名且端点空）拒启语义由 `wtls` 单测锁定。

## 72. SimpleRespKeySpec 负 step 回扫与 keyword 未命中防御性有界化（消除 C# 越界读 UB 与命令名误吞）

工单 zcode-r31-commanddocs 发现一登记。
回指注记：承接 §42a 占位条目。

C# 原型实现中，`TryGetKeySearchArgsFromSimpleKeySpec`（`libs/server/SessionParseStateExtensions.cs:930`）在处理 keyword 型 begin_search 且 `StartFrom < 0` 时，回扫循环 `for (i = beginSearchIdx; i < parseState.Count; i += -1)` 仅有上界而缺失下界防护；当 keyword 未命中时 `firstKeyIdx` 保持初值 -1，函数未作校验直落 `FindKeys` 段，range 型产出 `searchArgs=(-1,...)`。
在以下两个典型形态下触发严重缺陷：
1. `MIGRATE`（KEYS，StartFrom=-2）：未携带 `KEYS` 关键字时，负向回扫循环穿越 0 与 -1（GETKEYS 槽）、-2（COMMAND 槽）后继续越界下行，`GetArgSliceByRef`（`Parser/SessionParseState.cs:369`）将协议头字节重释为 `PinnedSpanByte`，读野指针造成越界读未定义行为（UB）直至访问违例崩溃；
2. `GEORADIUS`/`GEORADIUSBYMEMBER`（STORE/STOREDIST，StartFrom 6/5，LastKey=0）：当参数多于 StartFrom 且未带 `STORE` 关键字时 `lastKeyIdx=-1`，`TryAppendKeysFromSpec`（`SessionParseStateExtensions.cs:881`）单步 `i=-1` 命中参数切片前一条槽（即被审命令名 token，如 `GEORADIUS`），非空则作为下标 -1 的键收集；`ExtractCommandKeys` 按 Index 升序排列后将命令名自身作为首个键返回（双 keyword 规格均未命中时重复两份），`COMMAND GETKEYSANDFLAGS` 同样将其连带 flags 输出给客户端。
该辅助函数还被 `TxnKeyManager.cs:73`（集群事务键计划）与 `RespClusterSlotVerify` 槽校验共用，污染面超出 `COMMAND GETKEYS`。

Rust 侧裁决：
`try_get_key_search_args`（`wedb/wresp/src/catalog/simplified.rs:121-137`）将回扫循环设定为 `while i >= 0 && i < count` 双向有界，且在 keyword 未命中时对 `first_key_idx < 0` 显式 `return None` 整体跳过该键规格。
`COMMAND GETKEYS`/`GETKEYSANDFLAGS`（`wedb/wnode/src/resp/basic_commands/mod.rs`）以及集群槽校验、事务键计划共用该单点防御机制，`MIGRATE` 无 `KEYS` 返回 `*0` 空数组，`GEORADIUS` 无 `STORE` 返回 `*1` 仅主键，无内存越界与错键回显。

保留理由与警示：
此系修复上游越界读 UB 及命令名误吞键缺陷的防御性偏离（同第 3/5/13c/61 条崩溃防御型先例）。**勿按 C# 形态回改**（回改即引入越界读 UB 崩溃风险与误吞命令名）。
锁面：`wedb/wnode/tests/resp_tests.rs` 的 `command_getkeys_keyword_unmatched_bounds_and_no_wrong_key`。

## 73. COMMAND GETKEYS/GETKEYSANDFLAGS 父子命令组合查询补齐性延伸（对齐 Redis 标准行为）

工单 zcode-r31-commanddocs 发现四登记。
回指注记：承接 §42b 占位条目。

C# 原型实现中，`TryGetSimpleCommandInfo`（`libs/server/Resp/BasicCommands.cs:2041` 起）仅使用 `Enum.TryParse<RespCommand>(cmdName, true)` 按首参单名解析；对于 `COMMAND GETKEYS OBJECT ENCODING k`，仅解析出 `OBJECT` 父命令，因其根级无 `KeySpecifications`，直接返回 `-The command has no key arguments\r\n` 错误帧。

Rust 侧裁决：
`prepare_command_keys_context`（`wedb/wnode/src/resp/basic_commands/mod.rs:344-356`）对首参为 `is_parent` 且存在次参的输入组合 `"{parent}_{sub}"`（如 `OBJECT_ENCODING`）进一步检索子命令键规格，成功提取子命令参数键，对齐 Redis 官方标准行为；而对子命令规格同样缺席的命令（如 `CONFIG GET`），两侧一致返回 no-key-args 错误。

保留理由与警示：
本偏离系 Rust 对 Redis 标准协议行为的补齐性延伸，提升标准客户端兼容性。**勿按 C# 回退**（回退将破坏对标准子命令提取键的支持）。
锁面：`wedb/wnode/tests/resp_tests.rs` 的 `command_getkeys_parent_sub_lookup`。

## 74. 设备段目录与段文件处置的三处刻意分叉

工单 zcode-r48-aofopen 发现二登记（出处工单 zcode-r48-aofopen，对齐 §40 占位条目完整展开）。对位 Garnet 设备族（`LocalStorageDevice.cs` / `ManagedLocalStorageDevice.cs`）与 Rust 设备实现（`wedb/wdev/src/segmented_device/{recover,truncate}.rs`）的三处刻意防御分叉：

### a) 杂散/异常段名：C# int.Parse 拒启与负段号 startSegment 击穿 vs Rust 规范回验跳过
C# 原型实现中，`LocalStorageDevice.cs:RecoverFiles`（:180-213，`ManagedLocalStorageDevice.cs:97-131` 同构）以 `int.Parse(item.Name.Replace(bareName, "").Replace(".", ""))` 解析段号——目录内若存在任何非段名格式的杂散文件（如 `wal.log.bak`、编辑器临时文件等），抛出 `FormatException` 使设备构造失败拒启；且形如 `<基名>.-1` 的负号后缀被 `int.Parse` 接受为段号 `-1`，经空隙状态机（`segmentId != prevSegmentId + 1`）可把 `startSegment` 击穿置为 `-1`，原型解析面缺乏格式校验与防御。
Rust 侧 `wedb/wdev/src/segmented_device/recover.rs` 的 `parse_segment_suffix`（:25-33）对长度不符、非法字符、非规范小写变体、超 `u32` 域或非 UTF-8 后缀一律返回 `None`，`SegmentEntries::next`（:44-63）`continue` 静默忽略跳过杂散文件不拒启；且负号形态被规范性回验（`encode_u64(val) != s`）天然拒绝，杜绝负段号击穿风险。既有单测 `test_parse_segment_suffix` 已加锁。
裁决：防御增强型刻意分叉。杂散文件静默忽略、异常后缀规范回验跳过，严禁按 C# `int.Parse` 易碎形态回改。

### b) 删段失败：C# 尽力吞错 vs Rust 上抛 + purged_segment 幂等补删
C# 原型实现中，`LocalStorageDevice.cs:RemoveSegment`（:354-360）调用 `Native32.DeleteFileW` 后不检查返回值，物理删除失败尽力吞错不感知，对外假成功返回。
Rust 侧 `wedb/wdev/src/segmented_device/truncate.rs` 的 `truncate_until_segment_impl`（:176-240，Unix 删除失败上抛臂 :227-229）Unix 下删除失败显式上抛底层 I/O 错误（注释已自我声明对标差异）；同时采用逻辑栅栏 `start_segment` 与物理水位 `purged_segment` 解耦机制，仅在删段遍历全量成功后才推进 `purged_segment`，失败重试可幂等重新扫描清除残留段；Windows 平台读者持句柄删除失败（sharing violation）则记入 `pending_removes` 延迟删除队列并在后续调用中重试补删（注释 :135-142 已自证）。
裁决：防御增强与可靠性收紧型刻意分叉。如实上抛删段错误，幂等水位保障补删闭环，严禁按 C# 尽力吞错假成功形态回改。

### c) 段大小越限：Rust 全平台 SegmentSizeMismatch 拒启 vs C# 管理设备不校验
C# 管理设备族 `RecoverFiles` 无段文件大小校验，段尺寸越限校验仅在 C++ Native 层 `ValidateRecoveredSegments` 存在（`DeviceTests.cs:763` 注释口径），Windows/跨平台托管路径不设防，静默放行超限段文件。
Rust 侧在 `wedb/wdev/src/segmented_device/recover.rs:98-114` 全平台统一增加 `SegmentSizeMismatch` 校验（`file_size > seg_size` 显式拒启报错），杜绝已损坏或超限段文件被静默装载导致后续寻址错位。
裁决：数据完整性防御收紧型刻意分叉。全平台统一越限拒启，严禁按 C# 托管设备不设防形态回改。

后续审查按本条跳过上述三面，勿重复立项，亦严禁按 C# 缺陷形态回改。

## 75. 向量键 TTL/EXPIRE 族刻意收敛（读 -2 与写 :0 哨兵应答）

工单 zcode-r40-sentinel 发现一登记。

C# 原型实现中，向量索引键与普通键同属主存（`VADD` 经 `libs/server/Storage/Session/MainStore/VectorStoreOps.cs:190` 的 `new StringInput(RespCommand.VADD, ref parseState)` 落主存记录，`UnifiedSessionFunctions.Reader` / `HandleTtl` 经 `ConvertUtils.SecondsFromDiffUtcNowTicks` 读出 expiration=-1 出口恒 -1；`EXPIRE` 走 `HandleExpireInPlaceUpdate` + `SessionFunctionsUtils.cs:EvaluateExpire` 成功返回 `:1`）。

Rust 侧实现中，向量索引键不驻 wkv 值域，宿主为进程内 `ConcurrentMap`（`VectorManager::key_index_registry`），三域探针（String/ObjectEnvelope/Meta）对向量键恒判缺失：
1. 读面：快路径 `ttl_read_sync` 与慢路径 wkv `pttl_ms`/`expiretime_ms` 对向量键恒回 `-2`；
2. 写面：`expire_at`（`contains_key_ignore_ttl` 判假早退）与快路径 `expire_apply_sync`（`alive=false`）对向量键同回 `:0`。

裁决与保留理由：
刻意收敛声明。向量索引键不驻 wkv 值域且登记表无过期刻度，重放端 `wkv::expire_at` 拿不到登记表，主端若单独开闸回 -1 或设过期回 :1 会造成主从/副本两套终态与读写撕裂（如「读 -1 写 :0」合成缺口）。故 TTL 族读写两侧统一三域探针单源收敛（锚定 `ttl_sync.rs:probe_alive_with_registry`）。
对可观测面：`EXISTS` 快路径因接登记表第四态回 `1`，而 `TTL` 族回 `-2`，此为已知可观测代价。

对齐前提：
待登记表挂载过期刻度且 AOF/复制/迁移重放端配套放行后，方可按本条登记统一撤销对齐。杜绝单端开闸。

锁面：
`wedb/wnode/tests/vector_key_domain_ops.rs` 中的 `vector_key_exists_ttl_expire_semantic_lock` 与 `vector_set_ttl_family_converged_with_write_side`（断言向量键写入后 EXISTS 回 1、TTL 回 -2、EXPIRE 回 0 的确定性组合应答锁）。

## 76. 配置导出内容差异（Rust 全量字段 vs C# 仅非默认项）

工单 zcode-r59-cfgio 发现二登记。

C# 原型实现中，`GarnetConfigProvider.TryExportOptions`（`Configuration/ConfigProviders.cs:162-171`）调用全量序列化器，但其 `CompactOptionsJsonConverter`（`ConfigProviders.cs:268` 嵌套类，无自有 `Write`）经基类 `CompactObjectJsonConverter<T>.Write`（`garnet/libs/host/Configuration/CompactObjectJsonConverter.cs:71-95`）对每个属性均与 `DefaultOptions` 实例执行 `AreEqual` 判等（:87），相等属性一律跳过，导出文件恒仅含非默认项。

Rust 侧 `export_config`（`wedb/wconf/src/node_options.rs`）以 `toml_spanner::to_string(self)` 导出全部字段。

保留理由与裁决：
Rust 全量字段导出确保 TOML 单格式回导自洽、全量自包含，导出的配置文件可作为独立完整基线无损回导，属改良向偏差。后续审查勿按 C# 仅导出非默认项形态收窄导出。

## 77. 主存段尺寸用户旋钮缺席（DEFAULT_MAIN_LOG_SEGMENT_SIZE 硬编码 1GB）

工单 devsync-deviations 登记（zcode-r86-devsync3 对账派生，wdev-single-file 恒分段遗留）。

C# 原型实现中，主存段尺寸与对象日志段尺寸为用户可配项：`SegmentSize` 与 `ObjectLogSegmentSize` 默认 "1g"（`garnet/libs/server/Servers/ServerOptions.cs:57/:62`，`defaults.conf` 在册），允许用户按需调整段粒度。

Rust 侧生产装配硬编码 `DEFAULT_MAIN_LOG_SEGMENT_SIZE = 1 << 30`（`wedb/wnode/src/service.rs:34`），在 `open_node_with_config` 及节点恢复装配共三处（`service.rs:945/:1502/:1554`）直接传入该编译期常量；主存段尺寸无暴露用户旋钮（仅 WAL 侧存在 `--aof-segment-size` 配置）。双侧默认值一致对齐（1g = 1 << 30），分叉面仅表现为用户旋钮缺席。

裁决与保留理由：
本分叉与 §68/§69/§70/§71（r30-defaults）属于同族同形态，`single_file` 入口重定义为 1GB 段构造器系 Rust 内部 API 语义收编而非对位分叉，行为面偏差即本条旋钮缺席。
划界声明：本条仅负责登记分叉事实；若后续决定为主存段尺寸补充用户旋钮，属功能代码变更，归独立代码票承载，本条不预设裁决方向。
回锚注记（doc-deviations-memory-shape-five-groups-registry）：尺寸族其余五组旋钮收形（内联尺寸对／初始读尺寸／reviv 几何／缓冲池预算对／pagecount 折叠与 tree_cache_budget 反向形）登记见 §111，本条只钉 SegmentSize/ObjectLogSegmentSize 段尺寸一面，两条边界即此。

## 78. 新建段排他创建与 sync_dir 目录持久化屏障（新建段回滚删段面）

工单 devsync-deviations 登记（zcode-r86-devsync3 对账派生，barrier 修复轮遗留）。

C# 原型实现中：
1. `LocalStorageDevice.cs:GetOrAddHandle`（:503-518）经 `ConcurrentDictionary.GetOrAdd` 合流并发创建者，并发败者直接复用胜者已创建句柄；
2. `CreateHandle`（:431-470）使用 `FileMode.OpenOrCreate` 打开段文件，创建失败仅抛出 `IOException` 拒绝，不执行删段；
3. 全设备唯一物理删除点为 `RemoveSegment`（:354-359），创建路径无任何删除动作，且父目录无持久化屏障。

Rust 侧实现中（`wedb/wdev/src/segmented_device/handle.rs`）：
1. 先探测段文件缺失，再以 `create_new(true)`（`O_CREAT | O_EXCL`）排他打开（:198-208），并发败方（`AlreadyExists`）转为打开既有段（:346-382）；
2. 胜者承担 `sync_dir` 父目录持久化屏障（:159-165 定义，:401 调用）；
3. 屏障若执行失败，drop 后以 `remove_file` 回滚删除新建段文件并向底层上抛错误（:401-405）。

裁决与保留理由：
防御收紧与崩溃一致性增强型偏离。此系提交 5678fc71/a2d4c16b 及 r83 漏登补账，非新裁决。
划界声明：注明与 §74 的边界——§74 范围严格限于 `segmented_device/{recover,truncate}.rs` 三分面，`handle.rs` 屏障与创建回滚面不在其内。
已知接受的残余窄窗：胜者 `create_new` 成功至 `sync_dir` 失败 `remove_file` 之间，并发败方若完成打开并写入数据，随后胜方回滚删除段文件可能导致败方写入数据孤儿化；C# 因无目录屏障与创建回滚故无此窗。此为目录强持久化保障下的已知折衷。

## 79. VSIM ELE 缺失元素前置检查激活 C# 会话层死分支（"Element not in Vector Set"，上游死分支激活型）

工单 zcode-r59-vsimpath 发现一登记（对账票 zcode-r87-devsync4 漏登补登，取该票方案 a，同 §23 修复上游臂间分叉的精神）。

C# 一手形态：`Storage/Session/MainStore/VectorStoreOps.cs:VectorSetElementSimilarity`（`garnet/libs/server/Storage/Session/MainStore/VectorStoreOps.cs:312` 起）直调 `VectorManager.ElementSimilarity`（`garnet/libs/server/Resp/Vector/VectorManager.cs:965` 起）无存在性检查，仅产出 OK/BadParams 两值；全仓 `VectorManagerResult.MissingElement` 生产点仅 `TryRemove`（`VectorManager.cs:655`，VREM 链）与 `FetchSingleVectorElementAttributes`（`:1143`，VGETATTR 族）两处，均不在 VSIM 链——会话层 MissingElement 分支（`garnet/libs/server/Resp/Vector/RespServerSessionVectors.cs:914-918`，`NetworkVSIM` 于 `:519` 起）对 VSIM 恒不可达系死码。C# VSIM ELE 缺失元素的实际结局由原生库决定：native `search_element` 返负 → BadParams 且 customErrMsg 空 → 会话回落 "ERR asked quantization mismatch with existing vector set"（`:930-937`，误导文案，即 §23 已判定的上游缺陷回落形态）；返 0 → 空数组。组合面：缺失元素 + 非法 FILTER 时 C# 先编译 FILTER（编译段在 native 调用前）走 BadParams 回落。

Rust 侧裁决：`element_similarity`（`wedb/wnode/src/resp/vector/vector_manager.rs:1133` 起）前置 `check_external_id_valid` 存在性检查臂（`:1153-1163`），未命中即回 `MissingElement` + `ERR_ELEMENT_NOT_IN_SET`（"Element not in Vector Set"，常量 `resp_server_session_vectors.rs:77-78`）；底层 `wedb/wvector/src/service.rs` 的 `search_element`（`:1493` 起，`external_id_exists` 前置检查 `:1503-1509`）与 `check_external_id_valid`（`:1526` 起）。臂序上元素存在性检查先于 FILTER 编译（`:1172` 起 `try_compile`）——缺失元素 + 非法 FILTER 先回缺失文案，参数裁决优先级与 C# 相反，系本条组合面延伸。

裁决与保留理由：
「上游死分支激活型」刻意偏差。C# 会话层该分支文本即 "Element not in Vector Set"（语义准确，仅不可达），rust 激活该死分支；C# 实际可观测回落（量化 mismatch 文案）系 §23 同型的上游缺陷误导形态。后续对账直接引用本条：双侧 VSIM ELE 缺失元素应答发散勿判转写缺陷；严禁按 C# BadParams 空 errorMsg 回落形态回改；亦严禁回摆臂序（元素存在性检查不得后置于 FILTER 编译）。三处失真注释锚（`vector_manager.rs:1153` 头注、`resp_server_session_vectors.rs:77` 常量注、`resp_vector_set.rs` 两测试注）已订正并与本条互指。

锁面：`wedb/wnode/tests/resp_vector_set.rs` 的 `vsim_options_and_output`（`:593` 起，内含 `:797-808` 会话面缺失元素断言）与 `element_similarity_and_attributes_batch`（`:1600` 起，内含 `:1647-1661` 管理面断言）；组合优先级锁（缺失元素先于 FILTER 编译）见 `vsim_missing_element_priority_over_filter_compile`。

## 80. ZSCAN 非有限分值收敛 format_double 文本化（"inf"/"-inf"，不对齐 C# ZSCAN "Infinity" 词形）

工单 zcode-r55-respbyte 发现一登记（对账票 zcode-r87-devsync4 漏登补登）。

C# 一手形态：ZSCAN 分值文本化在 `garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Scan`（`:512-518`）：`Utf8Formatter.TryFormat` 成功写文本、失败 `items.Add(null)`（`:516`）。.NET `Utf8Formatter` 对 ±inf/NaN 按 G 格式恒成功（输出 "Infinity"/"-Infinity"/"NaN"，远小于 38 字节栈缓冲），`items.Add(null)` 系不可达死臂——C# ZSCAN 对 inf 分值恒回 "Infinity" 文本，null 项从不出现。对照同族 ZRANGE WITHSCORES 经 `WriteSortedSetResult` → `garnet/libs/common/RespWriteUtils.cs:TryWriteInfinity`（`:665-677`）输出 "inf"/"-inf"——C# 自身 ZSCAN（"Infinity" 形）与 ZRANGE（"inf" 形）两形并存，上游内部不一致。

Rust 侧现状与裁决：
现状（r55-respbyte 已落地收口，本条按现码登记完成态；票归档见 `task/done/zcode-r55-respbyte.md`）：内存态 `wedb/wcol/src/zset/sorted_set_object.rs:scan`（`:535` 起）与分层态 `wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:exec_tiered_scan`（`:319` 起）两臂只交原始 f64 分值、不再自行分派非有限形态——落帧单点分别是 `wedb/wcol/src/zset/sorted_set_object_impl.rs` 的 `scan_operate`（`:1488` 起，分值帧 `:1525`）与分层臂 `scan.rs:426`（`:418` 注释自证同源），两处同走 `wedb/wresp/src/ext.rs` 的 `RespVecExt::write_resp_double_bulk_string`（`:181`）→ `wedb/wresp/src/resp_memory_writer.rs` 的 `write_double_bulk_string`（`:689`）→ `format_double`（`:24`，`:28` 输出 "inf"/"-inf"）单源；两文件 `is_finite` 与 `write_null` 分值臂现码零命中，本条旧登记的过渡态（push None → `write_null` 回 RESP null 项）已删除。同数据集 ZRANGE WITHSCORES 经 `write_sorted_set_result_payload`（`wedb/wcol/src/zset/sorted_set_object_impl.rs:166`）同函共源，rust 族内 ZSCAN 与 ZRANGE 已同形（均 "inf"/"-inf" 文本）。NaN 分值经 ZADD/ZINCRBY/INCRBYFLOAT 双侧均拒（§2 输入文法），实际可达面仅 ±inf。残余分叉只在词形面：C# ZSCAN "Infinity"/"-Infinity" 对 rust "inf"/"-inf"，即本条偏差旗所指。
裁决：ZSCAN 内存与分层两臂统一走 `format_double` 文本化收敛至 "inf"/"-inf"，与 ZRANGE 同源单入口（分层与内存共用同一文本化函数，防双态再分叉），删除 None 到 `write_null` 的回写臂；C# ZSCAN "Infinity" 词形不复刻。
严禁回改声明：严禁按 C# ZSCAN 侧 `Utf8Formatter` "Infinity"/"-Infinity" 词形回改（该形与 C# 自身 ZRANGE "inf" 分裂，系上游内部不一致）；严禁恢复 null 项形态（RESP null 不在客户端对 ZSCAN 分值文本的解析预期内，标准客户端解析失败或丢项）。双侧对账在 ±inf 分值成员的 ZSCAN 用例必然发散（C# "Infinity" vs rust "inf"），勿判转写缺陷。

锁面：`wedb/wnode/tests/scan_family_dualstate_frames.rs` 的 `test_zscan_non_finite_score_format_and_zrange_parity`（`:857` 起，题注 `:850-856`）——RESP2 ZRANGE WITHSCORES 逐字节锁 `:885`、ZSCAN "inf"/"-inf" bulk 项锁 `:898` 与 `:923`、RESP3 段维持 bulk string「绝非 RESP3 null (`_`)」锁 `:963` 与 `:974`；分层态同族字节锁在 `tiered_scan_frames_byte_exact`（`:457` 起，±inf 分值项 `:530`/`:541`/`:543`），内存信封态在 `envelope_scan_frames_byte_exact`（`:199` 起，inf 项注记 `:277`、断言 `:285` 与升阶复跑 `:334`）。r55-respbyte 已归档，锁面为在位实锁非在途承载。

## 81. --checkpoint-dir 旋钮接线与检查点/WAL 双旋钮拆分（WAL 落点不跟随 checkpoint-dir）

工单 zcode-r59-backup 登记（对账票 zcode-r87-devsync4 漏登补登；源票二选一拍板取方案 a 接线，已落地现码，本条按现码登记残余分叉）。

C# 一手形态：`-c/--checkpointdir` 单旋钮（`garnet/libs/host/Configuration/Options.cs:134-136`）同时钉检查点与 AOF 两处落点：`CheckpointBaseDirectory = CheckpointDir ?? LogDir`（`garnet/libs/server/Servers/GarnetServerOptions.cs:625`）、检查点目录 `{base}/Store/checkpoints`（`GetStoreCheckpointDirectory`，`:692-693`）、`AppendOnlyFileBaseDirectory = CheckpointDir ?? string.Empty`（`:697-698`）——设 `--checkpointdir` 即同时迁移检查点与 AOF 基目录，缺省 AOF 基目录为空串（根相对）；CONFIG GET dir 回显检查点基目录。

Rust 侧裁决（接线已落地）：`wconf` `NodeArgs` 暴露 `--checkpoint-dir` 显式项（CLI + nested_text，`wedb/wconf/src/node_options.rs:415`），经 `checkpoint_base_dir()`（`:1157`，显式优先、缺省回落数据目录）投影 `runtime_server_options.checkpoint_base_directory`（`:1341`）；`wedb/wnode/src/service.rs` 的 `checkpoint_dir_of`（`:893`，非空基目录拼 `{base}/Store/checkpoints` 对齐 C# `GetStoreCheckpointDirectory`；空串回落数据目录默认布局，嵌入式回落臂 `:1228-1229`、生产覆写臂 `:1388-1393`），CONFIG GET dir 同源回显（`wconf/src/runtime_server_config.rs:55`）。WAL 落点为独立 `wal_dir` 旋钮（`open_wal`，`service.rs:977` 起，缺省 `<data>/wal/wal.log`，`WAL_BASE_FILE_NAME` `:964`），不跟随 checkpoint-dir。
裁决与保留理由：旋钮拓扑拆分型有意分叉。检查点与 WAL 各置独立卷（正交双旋钮），能力为 C# 单旋钮联拖的超集（C# 无法只迁其一）；默认部署双侧行为等价（缺省检查点均落数据目录族布局）；基目录空串回落数据目录默认布局而非 C# 空串根相对形态，杜绝嵌入式宿主直传空基目录时落点漂到工作目录。严禁按 C# 单旋钮联拖 AOF 形态把 WAL 落点硬绑 checkpoint-dir（回并即丢独立卷容灾布局能力）。

锁面：`wedb/wconf/src/node_options.rs` 的 `test_checkpoint_dir_knob_parse_fallback_project`（`:1889` 起，CLI/nested_text/缺省回落三态投影锁）。

## 82. SETRANGE 增长臂间隙恒零填充（不对齐 C# InPlace 臂 zeroInit:false 陈旧间隙）

工单 zcode-r64-emptyval 登记（对账票 zcode-r87-devsync4 漏登补登，按第 18 条同型书写）。

C# 一手形态：SETRANGE 四臂对「[旧值长, offset) 间隙」处理两态并存。`garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs` 的 InitialUpdater（`:207-222`，`:213` `if (offset > 0) value.Slice(0, offset).Clear()`）与 CopyUpdater（`:1353-1366`，`:1360-1362` Clear）显式清零间隙；SETBIT（`:582`/`:593`）与 BITFIELD（`:622`/`:633`）InPlace 增长臂传 `zeroInit: true` 同清零。唯独 SETRANGE 自己的 InPlaceUpdaterWorker 臂（`:734-763`）：增长经 `TrySetPinnedValueLength`（`:744-747`）与 `TrySetContentLengths`（`:751-757`）二者 zeroInit 形参缺省 false（`garnet/libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs:849` 与 `:665`），`:758` `newValue.CopyTo(ValueSpan.Slice(offset))` 只拷新值字节，间隙段无人落笔，臂返 `IPUResult.Succeeded` 视作正常更新。后果：hlog 环形页回绕或复活池复用槽位后，间隙是该物理位置更早记录的陈旧字节，GET 原样回给客户端，违背 Redis SETRANGE 恒零填充语义；空写（offset > 旧值长、val 为空串）为最大暴露面——新增尾部整段即间隙，应答长度双侧一致、分叉纯在内容字节。同一命令随记录 inline 与否、槽位新鲜度漂移出两种应答，家族自相矛盾（同第 17/18 条 RemoveExpiration 家族先例），属上游缺陷。

Rust 侧裁决：`network_set_range` 双臂恒零填充、内部单机制一致——原位增长臂闭包（`wedb/wnode/src/resp/basic_commands/set.rs:258-273`，`offset > old_len` 时 `cap[old_len..offset].fill(0)` 再拷新值，内核 `wedb/wkv/src/session/raw/write/rmw.rs:RmwWindow::try_grow_in_place`，`:144` 起），整值回退臂 `existing.resize(required_len, 0)`（`:277-292`），缺失键臂 `vec![0u8; offset + val.len()]`（`:300-302`），慢路径同构。

后果与严禁回改：双侧对账在「SETRANGE 间隙增长 × 槽位复用」场景必然发散（C# 回陈旧字节 vs rust 回零），勿判为转写缺陷，更严禁按 C# InPlace `zeroInit:false` 形态回删填零逻辑（回删即把 C# 跨记录信息残留缺陷引入本仓，才是真回归）。`set.rs` 原位臂注释（`:264-268`）订正裁决声明并回指本条。若后续上游修复 C# 该臂，本条按登记撤销处理。

锁面：`wedb/wnode/tests/string_in_place_grow.rs` 的 `set_range_gap_zero_fill_matches_tail_path`（`:231`，间隙补零原位/尾部两臂逐字节一致锁）与 `set_range_gap_zero_fill_empty_write_semantic_lock`（空写与间隙填零语义锁：SET k abc 后 SETRANGE k 10 "" 回 :10 且 GET 逐字节等于 "abc" + 7 个 0x00；SETRANGE 11/12 间隙写 x 恒填零）。

## 83. RI.CREATE 补 CACHESIZE 与叶页容量关系守卫（不对齐 C# 仅 > 0 数值校验的上游继承缺陷）

工单 wnode-ricreate-cachesize-unwind-budget-leak 登记，wbftree-ricreate-panicpremise-falsify-realign 返工对齐引擎真实契约（初版守卫下限与 catch_unwind 机制的「防引擎 panic」前提经 bf-tree 0.5.6 实源亲码证伪）。

C# 一手形态：RI.CREATE 数值校验仅查四项大于零（`garnet/libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:136-141`），无 CACHESIZE 与叶页容量关系守卫；容量不足组合由 native 层 `BfTree::with_config` 的 bf-tree `Config::validate` 拦截（0.5.4 即含比例判定：cache-only ≥ 4× 叶页、否则 ≥ 2×，`config.rs` circular buffer size 检查段），`bftree_create` 回 NULL，`BfTreeService.cs` 构造函数抛泛化 `InvalidOperationException("Failed to create BfTree instance.")`（`garnet/libs/native/bftree-garnet/BfTreeService.cs:166-172`）——不 panic、无 abort。`CircularBuffer::new` 的 `assert capacity >= leaf_page_size + size_of::<AllocMeta>()` 在建树链上不可达（validate 比例判定严格强于它）。

Rust 侧裁决：`wedb/wnode/src/resp/range_index/resp_server_session_range_index.rs` 的 `RiCreateOptions::validate` 在既有 > 0 / MINRECORD ≤ MAXRECORD 判定后补 `CACHESIZE >= 4 × 叶页` 守卫——即引擎 `Config::validate` 的 cache-only 比例判定（bf-tree 0.5.6 `config.rs` circular buffer size 检查段：cache-only ≥ 4× 叶页、Disk ≥ 2×），取两模式交集的严规则、单一文案 `ERR CACHESIZE must be at least 4 times the leaf page size`（PAGESIZE 显式按显式值判、缺省按 `RangeIndexManager::compute_leaf_page_size(MAXRECORD)` 派生值判），拒绝回协议错误帧；校验收 wnode 单点，wkv 建树臂不加第二套。守卫通过的组合在两种后端下均过引擎 validate，引擎 `InvalidConfig` 穿透帧在本命令面不可达、引擎串无从直出协议面（引擎 `ConfigError` 文案翻译收 wbftree `config_error_to_string` 单点，仅供恢复/升阶运维面）；协议文案系真实引擎容量关系的翻译，非「防引擎 panic」屏障。初版的 `CACHESIZE >= 叶页 + 8B` 下限与 wbftree `create_bftree_internal` / `build_collection_tree_snapshot` 两处 catch_unwind 已随证伪删除：0.5.6 全构造面（`with_config` 直建与 `new_from_cpr_snapshot` 快照恢复，后者在 `CPRSnapShotMgr::new_from_snapshot` 内同样 validate 前置）均先 `Config::validate` 后 `LeafStorage::new`，`CircularBuffer::new` 环断言（`capacity >= leaf_page_size + AllocMeta`）配置面不可达，容量不足组合恒以 `Error::InvalidConfig` 干净拒绝，建树恢复「先预留后实例化、Err 臂归还」原序，`CB_ALLOC_META_SIZE` 随守卫改版一并删除。`compute_leaf_page_size` 乘法饱和收口保留（C# double 大数远超 32KB 封顶同得 32768，usize 回绕属转写失真）。

后果与严禁回改：双侧对账在容量不足场景发散（C# 泛化 InvalidOperationException vs rust 协议层定向 4x 错误帧），且 DISK 后端 2x..4x 区间 rust 保守拒绝而 C#/引擎可接受——交集严规则换单一文案的裁决性偏离，勿判转写缺陷；更严禁按 C# 仅 > 0 形态回删容量守卫（回删即回退泛化文案，且失去预留记账与工件清理之前的快败点，才是真回归），也严禁以「防引擎 panic」为由复活 catch_unwind 包裹（引擎全构造面 validate 前置、断言配置面不可达，catch_unwind 属死机制，违 task/review.md 板块 1 全链路唯一机制纪律）。守卫口径必须与引擎 `Config::validate` 比例检查（cache-only ≥ 4× 叶页、否则 ≥ 2×）保持对齐，引擎侧升级该判定时同步修订本守卫与文案。

锁面：`wedb/wnode/tests/range_index_ricreate_budget_guard.rs`（协议错误帧 + 重复失败创建 cache_reserved 零增长 + 合法调参回归）；`wedb/wbftree/tests/manager_and_stub/budget.rs` 的 `test_instantiate_failure_releases_reservation`（实例化拒绝/失败段记账零滞留锁）。

## 84. 服务端停机排空 5 秒强收护栏（防慢客户端挂死，不对齐 C# 生产环境无限等语义）

工单 zcode-r60-netclose 登记（发现二 P3 登记级）。

C# 一手形态：`libs/server/Servers/GarnetServerBase.cs:DisposeActiveHandlers`（:168-199）在生产形态下无限等待 `activeHandlerCount` 归零（通过 `Thread.Yield` 自旋轮询），其 5 秒滞留诊断逻辑（:171-192 `LogError`）仅包裹在 `#if DEBUG` 编译宏内，生产环境无任何超时放弃面。若遇对端停滞或异常连接拖延，停机流程将被无限期阻塞挂死。

Rust 侧裁决与现状：`wedb/wnode/src/servers/consumer_registry.rs:dispose_active_handlers`（:685-702）以 `DRAIN_TIMEOUT_MS = 5000`（:59）作为生产硬护栏。排空循环中每轮向未退场连接投递 `kill_session` 终止令，若 5 秒到期 `active_handler_count` 仍未归零，则通过 `warn!` 留痕记录滞留端点后强制返回，退出排空循环；残余未完成关闭序的连接任务交由各 worker 线程在退出时的 `compio::runtime::Runtime` 析构阶段统一兜底取消。

裁决与保留理由：
健壮性与可用性防御型有意分叉。C# 生产无限等极易因慢客户端、单向断流或异常连接导致进程优雅关停彻底挂死；Rust 侧 5 秒强收护栏为服务生命周期提供确定性时延上界，超时后交 Runtime 析构兜底，保障运维关停可达性。此为刻意设计取舍，严禁按 C# 生产形态回改为无上界无限等待。

## 85. INFO STATS 两事务计数与 LATENCY TX_PROC_LAT 骨架恒零恒空（过程面删除的忠实镜像，非转写缺陷）

工单 wtxn-transaction-proc-counters-unwired 登记（r115-triage-metrics1 立案五，P3 台账收口，零行为改动）。

C# 一手形态：事务过程执行入口 `TryTransactionProc`（`garnet/libs/server/Custom/CustomRespCommands.cs:18-54`）挂四枚观测钩——:26 `LatencyMetrics?.Start(LatencyMetricsType.TX_PROC_LAT)`、:29 `sessionMetrics?.incr_total_transaction_commands_received()`、:43 `incr_total_transaction_execution_failed()`、:51 `Stop(TX_PROC_LAT)`；唯一调用点 `TxnRespCommands.cs:333`，仅在过程注册查找（`GetCustomTransactionProcedure`）成功后抵达。C# 裸部署（未 RegisterApi 注册任何过程）下过程查找失败、回 `NO_TRANSACTION_PROCEDURE`，两计数同样恒零、TX_PROC_LAT 同样无样本。

Rust 侧裁决：动态注册管理层删除系在册既定改良，过程执行体连承载整体不存在——RUNTXP 校验三门后无条件回 `RESP_ERR_NO_TRANSACTION_PROCEDURE`（`wedb/wnode/src/resp/txn_resp_commands.rs` 的 network_runtxp）。INFO STATS 的 `total_transaction_commands_received` / `total_transaction_execution_failed` 读出链（`wedb/wmetric/src/info/garnet_info_metrics.rs:636-646`）与 LATENCY HISTOGRAM 的 `TX_PROC_LAT` 骨架行（`wedb/wmetric/src/latency/latency_metrics_type.rs:54/:70`）在位保留，两计数的 incr 链与 getter（`wedb/wmetric/src/garnet_session_metrics.rs:321/:331`）保留在位不删，唯生产零调用——两字段对外恒零、TX_PROC_LAT 恒空桶。此为「无过程可执行」的忠实镜像而非转写行为缺陷：与 C# 无注册过程裸部署形态值面等值，字段/行名保留系骨架兼容，监控读者与后续审查轮据此区分「无事务」与「链路断」。

后果与严禁回改：严禁按「接线回 C# 钩子」处理本登记——无可执行过程即无钩可挂，接线面不存在；台账三处旧自述（server.yml 事务过程静态承接段、`wcustom/src/lib.rs` 模块头「已迁至服务层」、`resp_server_session/txn.rs`「静态派发解析实例化执行」）已随本票一并改述现状。若未来静态过程面复活，按 C# 四钩位点（Start/Stop 包夹＋成功/失败分臂 incr）在派发单点回接，并撤销本登记。

## 86. MIGRATE timeout 其余负值解析期显式拒收（基线运行期抛错口径前移）

工单 wedb-migrate-timeout-sentinel-inversion 登记（r117-triage-mig1 立案，P3）。

C# 一手形态：MIGRATE 第 5 参 timeout 仅判整数不限符号（`garnet/libs/cluster/Session/MigrateCommand.cs:85-93`），构造期 `TimeSpan.FromMilliseconds(_timeout)` 原样入会话（`MigrateSession.cs:147`），全部远端 await 统一 `WaitAsync(_timeout, _cts.Token)`——三档语义：`>0` 限时；`0` 即 `TimeSpan.Zero`，等待立即判超时（快失败档）；`-1` 即 `Timeout.InfiniteTimeSpan`，永不自发超时（免超时档）；其余负值在首个 `WaitAsync` 抛 `ArgumentOutOfRangeException`，迁移在运行期半途失败（此时可能已下发 IMPORTING 编排帧），并随 recover 走远端 STABLE 回退。

Rust 侧裁决：三态映射单点 `wait_dur`（`wedb/wedb/src/server/migration/migrate_driver/keys.rs`）返 `Option<Duration>`——`>0`→`Some(限时)`、`0`→`Some(ZERO)`、`-1`→`None`（无限档停等仅随会话取消令牌收敛，`pause_revivification` 的 `Option<Duration>` 形参天然直传）；其余负值（`< -1`）前移到 `network_try_migrate` 解析期显式 ERR 拒收（`-ERR MIGRATE timeout is invalid: must be -1, 0, or positive`），零注册、零远端触达。此为可观测行为改良：错误暴露更早、无半途编排副作用，优于基线运行期抛错口径；`0`/`-1`/正值三档对外语义与 C# 严格一致。

后果与严禁回改：严禁把 `<= 0` 重新折算为任何定值（历史缺陷即一切 `<=0` 折 15s，注释与代码互斥，三档全失配）；哨兵回归用例持于 `wedb/wedb/tests/cluster_migration.rs`（`migrate_wait_dur_sentinel_tri_state`、`migrate_zero_timeout_fails_fast_even_with_eager_target`、`migrate_infinite_timeout_never_self_times_out_and_dispose_converges`、`migrate_negative_timeout_rejected_at_parse`）。

## 87. BITOP 多源折叠读不持非目的源键共享闩（无跨键一致快照，单机制收口）

工单 wnode-bitop-source-read-outside-dest-window 登记（r118-triage-bit1 立案，r117-setfam1 未拍面一同轴，P2）。

C# 一手形态：`StringBitOperation`（`garnet/libs/server/Storage/Session/MainStore/BitmapOps.cs:88-98`）在任何读发生前建事务锁全键集——目的键 keys[0] Exclusive（:94）、全部源键 Shared（:95-96），锁罩「逐源读→折叠→dest SET」全程，读遇 epoch 变更 goto readFromScratch 复验快照新鲜（:106/:124-127）。

Rust 侧裁决：目的键 `dest` 的读改写窗口（`BatchStoreSession::try_rmw_window`/`rmw_window`）已随本票前移至逐源折叠之前、跨「折叠读→求值→落笔」全程持有（快路径 `wnode/src/resp/bitmap/bitmap_commands.rs:network_string_bit_operation`、慢路径 `wnode/src/resp/basic_commands/slow.rs:slow_bit_operation`）——dest∈srcs 自指形（BITOP OR/NOT/AND dst dst）的窗内自读与并发持窗写者互斥，杜绝旧折叠视图尾段盲写顶掉已提交写的非可串行化，此为本票主案链收口。但 dest∉srcs 的其余非目的源键不加共享闩：折叠逐源读各走独立一致读会话（`read_user_sync_with_prefix`/`read_user_with_prefix`，仅一致读非本键排他窗），多源视图非跨键一致快照，理论上≥2 个非目的源键恰跨折叠读序的并发已提交写可致撕裂读（本票附证形 c，低频）。

保留理由：一者，撕裂面仅在「多源 + 多源并发写 + 恰跨读序」三重条件下可达，且 BITOP 源键语义本无 Redis 快照保证；二者，锁源纪律「严禁新建第二张锁表」——若强罩源键共享闩须引入 wtxn 多键计划的共享档（现 `try_rmw_window_sorted`/`rmw_window_sorted` 仅排他闩，强用即把源键整体排他，与 C# Shared 语义分叉且序列化无辜源读者），属过度设计与双机制。与仓内 MGET/多键读「无跨键快照」既有口径同源。后果与严禁回改：本登记钉死「dest 窗罩全程、非目的源键无共享闩」的当前裁决；若后续 r117-setfam1 台账拍定引入源键共享闩的多键计划单点，按其结论回接并撤销本登记。

## 88. AOF 副本事务组重放免锁（栅栏排序，不对齐 C# 逐键锁集读者隔离）

工单 waof-replay-session-id-zero-grouping-dead 登记（r114-triage-aof1 立案，执行方案第 4 步「二者择一」取登记分支）。

C# 一手形态：副本重放事务组经 `AofReplayCoordinator.cs:ProcessTransactionGroup`（:339 起 asReplica 臂）走 `SaveTransactionGroupKeysToLock` + `Run(internal_txn)` + `Commit` 锁集执行链——整组操作在事务锁保护下串行提交，读者在组提交前绝不见 EXEC 多键事务的中间态。

Rust 侧裁决：本票已修复归组键断链（数据条目 AOF 帧头此前恒 session_id:0 与标记 ≥1 永不相交、组恒空），归组、组重放、模糊区组缓冲、分块入组随写侧同键复活。副本 `process_transaction_group` 臂（`wedb/wnode/src/aof/aof_processor.rs:791`）在归组命中后仍采「Acquire/Release 栅栏 + 免逐键锁集」形态：跨物理子日志的事务组经序列号栅栏协调提交次序（组整体按 TxnStart/TxnCommit 序列号对齐），但组内操作顺序重放期间不持事务键锁集。

后果：与 C# 差集为读者隔离粒度——C# 锁集使副本在组重放全程对读者不可见中间态；rust 栅栏仅保证跨子日志组提交的全序对齐，单日志拓扑与崩溃恢复臂（!as_replica）本即顺序重放、读暴露的中间态窗口与 C# 恢复期同语义（恢复期读不暴露局部中间态事务为 C# 同款豁免理由）。副本多日志在线重放（as_replica && multi_log）下，同一事务组内跨键的读者可在组重放中途读到部分键的新值（中间态窗口），系本登记明确的刻意取舍。若后续副本一致性审查要求逐字节对齐读者隔离，按 C# 锁集链（组键集入锁 + internal_txn 上下文重放 + 提交释放）在 as_replica 臂补齐并撤销本登记。

## 89. LPOS 选项词元大小写文法取全兼容形（不收紧至 C# 对象层双写式）

工单 doc-deviations-lpos-option-casefold-registry 登记（r119-triage-list1 立案，P4，纯文档面无行为改动）。

C# 一手形态：对象层 `ReadListPositionInput`（`garnet/libs/server/Objects/List/ListObjectImpl.cs:464-513`）三词元臂 `:479`/`:487`/`:497` 用 `SequenceEqual(CmdStrings.RANK) || SequenceEqual(CmdStrings.rank)` 全大写/全小写双写精确比对（常量对在册 `libs/server/Resp/CmdStrings.cs:99-100`、`:130-133`），混合式 `Rank`/`rAnK`/`CounT` 一律落 `:505-509` 的 `RESP_SYNTAX_ERROR` 臂。该口径与其自家命令层三处互斥：`ListCommands.cs:158`（LPOS 缺键 COUNT 探测）、`:224`/`:897`（LMPOP/BLMPOP COUNT 词元）均 `EqualsUpperCaseSpanIgnoringCase` 忽略大小写；真 Redis 对 RANK/COUNT/MAXLEN 恒不敏感。

Rust 侧裁决：维持 `eq_ignore_ascii_case` 全兼容形（`wedb/wcol/src/list/list_object_impl.rs:read_list_position_input` 词元臂 `:566`-`:574`，`:543` 注释自证刻意放宽），命令面同口径见 `wedb/wnode/src/resp/objects/list_commands/read.rs:136-143` 与 `slow.rs:461`。分叉矩阵：`LPOS 存在键 × Rank/rAnK/CounT/MaxLen 混合式` —— C# 回 `-ERR syntax error`，rust 正常执行选项语义；rust 形为 C# 形的严格超集（多收混合式，畸变词元 `Rankk`/`CountX`/`Max_Len` 两侧同回语法错）。

后果与严禁回改：按 C# 双写形收紧即文法倒退——既丢真 Redis 兼容面又固化 C# 原型内部矛盾，严禁后审据此判为转写宽松缺陷回改。同族顺扫已排除，本面单点不成族（防复勘锚：`SCRIPT FLUSH ASYNC/SYNC` 前置 `ToUpperInPlace`（`LuaCommands.cs:229/:231`）、`SET NX/XX/GET` 两轮转写重试环（`BasicCommands.cs:620`、`:666-696`）、`CLIENT LIST` 过滤词元前置 `ToUpperInPlace`（`ClientCommands.cs:42-44`，rust `wnode/src/resp/client_commands.rs:64`/`:214` 同不敏感）三处实为不敏感无分叉；`SCAN TYPE` 型名值 C# 双写（`ArrayKeyIterationFunctions.cs:60-80`）rust 刻意镜像同款双写（`wnode/src/resp/array_commands.rs:131-146`），系同形无分叉且已在 §20 b 在册）。行为锁双层早在位、本票零夹具新增：对象层 `list_object_impl.rs` 单测混合式块 `:640-653` 与畸变臂 `:656-662`，命令层 `wnode/tests/resp_list.rs` 的 `lpos_with_options`（存在键 `Rank 2`→`:3`、`rAnK -2`→`:1`、`cOuNt 2`→`*2`、`MaxLen 2`→`:1` 及组合臂）与 `lpos_with_invalid_key`（缺键 `cOuNt`→`*0`）——已逐字对读与本机制作成一致。附带勘误并入本条：§3 所称「C# 无 rank==0/count<0/maxlen<0 防线」对本参考树已陈旧（`ListObjectImpl.cs:355-371` 现具三门），§3 已按收口注记注销（本参考树内该偏差从未成立），本条所维持者仅为 rust 拦截形与 C# 现三门同帧收敛后的一致行为。

## 90. requirepass 固定口令档双参 AUTH 用户名门（C# 接受任意用户名 / rust 拒绝，异名+正确口令回 -WRONGPASS）
工单 wauth-requirepass-twoarg-username-gate-unregistered 登记（r113-hello1 立案一、r114-triage-hello1 转票定 P2，登记级处置：不改行为、不回退实现，只修登记+锁测试）。
C# 一手形态：requirepass 部署经 `PasswordAuthenticationSettings.cs:34` 构造 `GarnetPasswordAuthenticator`，其 `Authenticate`（`garnet/libs/server/Auth/GarnetPasswordAuthenticator.cs:29-33`，类头 :9-13 自注 Deprecated）仅 `SecretsUtility.ConstantEquals(_pwd, password)`，username 形参声明即弃用；会话侧 `AuthenticateUser`（`libs/server/Resp/RespServerSession.cs:425-448`）认证成功后经 :440 `GetDefaultUserHandle` 兜底挂载 default 句柄——requirepass 部署下双参 `AUTH <任意名> <正确口令>` 回 +OK；HELLO 一体臂（`libs/server/Resp/BasicCommands.cs:1792-1806`，`!username.IsEmpty` 门透传 AuthenticateUser）同样认证成功，协议升级与 SETNAME 一并落地。
Rust 侧裁决与现状：requirepass 收敛为「带口令的 default 用户」ACL 单档（`wedb/wnode/src/service.rs:with_requirepass` :1756-1764），用户名参与匹配：双参非 default 名先走存储点查 `authenticate_user_via_store`（`wedb/wnode/src/resp/acl_commands.rs:652-700`，存储无记录即 Denied :684-686），ns0 会话 Denied 后回落引导认证器 `authenticate_user`（`wedb/wnode/src/resp/resp_server_session/auth.rs:219-253`，按 username 定位 :243-249），其单点门禁非 default 名一律 None（`wedb/wacl/src/auth/garnet_acl_authenticator.rs:82-84`）——AUTH 臂（network_auth_session :374-448）与 HELLO 臂（process_hello_command_state :517-609）同链失败即 `-WRONGPASS Invalid username/password combination`；HELLO 形认证失败于 :575-583 提前 return，协议升级与客户端名落位段（:595-602）不执行，协议与 SETNAME 皆不落。锁面：`wedb/wnode/tests/requirepass_test.rs`（异名+正确口令双参 AUTH 拒绝、后续命令仍 -NOAUTH；HELLO 3 AUTH 异名拒绝、协议未升级与客户端名未落探测；既有 AUTH default 正确口令 +OK 锁保持绿）。
裁决与保留理由：方向性严向收口，非实现错误——真 Redis requirepass 语义即「须 username==default」，rust 对齐真契约，C# 弃用户名形系 Deprecated 认证器的宽松遗留。收口正确不改分叉事实：自 C#（Garnet）迁 rust 的客户端若在用双参 AUTH 非 default 名+正确口令（部分客户端库默认双参发送），认证由 +OK 变 -WRONGPASS、认证流断裂，HELLO 一体臂连带协议协商与客户端命名不落——对外可见的认证面变化，故立本台账；后续对拍轮遇本案差异直引本条，勿重复疑报。严禁按 C# 弃用户名形回退，回退即安全倒退（异名+正确口令放行等于架空用户名门）。

## 91. COSCAN 域收口：JSON 域扫描 NotImplementedException 以错误帧裁量收口（C# 会话级异常无 RESP 帧对位）

工单 wcol-coscan-object-domain-inversion 登记（r115-triage-objenc1 立案四，P2）。

C# 一手形态：COSCAN/CUSTOMOBJECTSCAN 原域是自定义对象（`RespServerSession.cs:904` 经 `ObjectScan(GarnetObjectType.All)` 下传，仅 `CustomObjectBase.cs:77-89` 的 sealed Operate 接受 All 转对象 Scan；内置三型严格类型检查对 All 一律 WrongType）。JSON 模块对象 `GarnetJsonObject.cs:132-135` 的 Scan 直接抛 `NotImplementedException`——C# 该异常沿会话执行栈上传，无 RESP 错误帧对位（连接级异常面），客户端永远拿不到「JSON 域 COSCAN」的协议化应答。

Rust 侧裁决：COSCAN 域收口反转后，JSON 键（信封标签 0x41）命中扩展标签域即进 `wext_json/src/json_object.rs` 的 `scan_members`，恒落错误帧 `RESP_ERR_NOT_IMPLEMENTED`（文案 `ERR The method or operation is not implemented.`，取 .NET NotImplementedException 默认消息；常量在 `wext_json/src/error.rs` 单点）——组帧内核 `wcol/src/types/scan_input.rs` 的 `custom_scan_operate` 对成员扫描 Err 臂统一写错误帧，杜绝旧假桩 `Ok(())` 空成功吞错。C# 无帧对位而 rust 有错误帧，系协议化裁量：不炸连接、应答恒合法帧，语义（拒绝）与 C# 抛错同向。

后果与严禁回改：严禁以「C# 无此帧」为由回改空成功或连接中断形态——空成功帧即旧票面确认的吞错反转；严禁把该文案改成 WRONGTYPE（JSON 键在 COSCAN 域是「类型命中但扫描未实现」，非「类型不符」）。Roaring 域扫描（恒空收集 + 游标 0）与 C# 上游 `RoaringBitmapObject.cs:64-71` 实现逐字对拍（其 doc 注释自称输出置位十进制键与实现矛盾，以实现为准），不属本条裁量。


## 92. BITCOUNT BIT 口径中间整字节计入（不对齐 C# BitCountDriver >= 早退漏计）

工单 doc-deviations-bitcount-bit-unit-fix-fork 登记（源案 zcode-r117-bitops1 立案二，r118-triage-bit1 双锚复跑坐实）。修复型分叉，与第 12/16/17/18/20/21/82 条同型书写。

C# 一手形态：`garnet/libs/server/Resp/Bitmap/BitmapManagerBitCount.cs` 的 `BitCountDriver`（`:64` 签名）BIT 口径臂（`:84-111`）先经 `:101` `count += BitIndexCount(value, startOffset, endOffset)` 计首末字节部分位，`:104-105` 归一 `startOffset = (startOffset / 8) + 1`、`endOffset = (endOffset / 8) - 1` 剔除首末字节，随后 `:109` `if (startOffset >= endOffset) return count;` 早退——区间恰跨 3 字节时归一后 `startOffset == endOffset` 同等于中间字节下标，`>=` 连这唯一剩余中间整字节一并早退漏计。实例：值 [0x01,0xFF,0x80] 求 `BITCOUNT key 7 16 BIT`，C# 回 2（仅首 bit 7 与末 bit 16 各 1），Redis 真值 10（1+8+1，中间 0xFF 整字节 8 位丢失），属上游缺陷。

Rust 侧裁决：`wedb/wbitmap/src/bit_count.rs` 的 `bit_count_driver`（`:53` 签名，BIT 口径归一 `:86-115`）将同位分叉臂改为 `:123` `if start_offset > end_offset` 早退——`start == end` 时不退，落入 `:131-132` `__scalar_popc` 计入中间整字节，与 Redis 口径对齐；`:117-122` 注释自陈刻意差异与实例数值并回指本条。

后果与严禁回改：双侧对拍在「BIT 口径区间归一后 start == end（恰跨 3 字节）」例上必然发散（[0x01,0xFF,0x80] 之 7..16 BIT 例 C#=2 / rust=10），rust 侧系 Redis 真值口径的修复形，勿判转写缺陷；更严禁按 C# `>=` 形态回改（回改即漏计中间整字节、把上游缺陷引入本仓，在 Redis 口径下计数错误，才是真回归——第 18 条「改回才是真回归」同款话术）。若后续上游修复 C# 该臂，本条按登记撤销处理。

锁面：`wedb/wbitmap/src/bit_count.rs` 的 `driver_byte_and_bit_modes`（测试体 `:325` 起，`:342-343` 钉 `[0x01u8, 0xFF, 0x80]` 求 `bit_count_driver(7, 16, 0x1, .., 3)` 回 10 真值锁）。

## 93. MutablePercent 未裁决分叉留槽（C# 基线 90 vs rust 缺省 0.5）

工单 doc-deviations-wording-and-attribution-five 登记（r121-triage-devbatch 席三订正点五，拟号时 §90-§92 已被占，按现树末节号顺延占 §93）。未裁决分叉留槽形，非行为裁决。

C# 一手形态：默认 `MutablePercent = 90`（`garnet/libs/server/Servers/ServerOptions.cs:77` 与 `garnet/libs/host/defaults.conf:64` 双证），合法区间 10..=95（`GarnetServerOptions.cs:747-748`），装配时 `MutableFraction = MutablePercent / 100.0`（`GarnetServerOptions.cs:757`）。
Rust 现形：引擎默认可变区比例 0.5（`wedb/whlog/src/config.rs:20` `DEFAULT_MUTABLE_FRACTION`，经 `wkv/src/config.rs:393` 装配进 `StoreConfig::default`）；用户旋钮 `--hlog-mutable-percent`（`wconf/src/node_options.rs` `HlogOptions::mutable_percent`，10..=95 区间单点校验对标 `:747-748`），经 `wnode/src/service.rs` 的 `apply_hlog_overrides` 投影为 `mutable_fraction`（生产入口 `store_config_from_node`）。
裁决：未裁决，留槽占号。0.5 vs 0.9 直接影响内存尾部覆写窗口与 Flush 行为节奏，系实质分叉，仓内查无 0.5 缺省的裁决依据（无对标文档、无既往登记）。旧尾注与 rust 码内注释「C# 基线 50／对标值取 50」系假对标宣称（基线 50 不存在），已随本票证伪订正。
后果与严禁回改：留待后续拍板票在本节转正裁决；裁决前严禁按 90 或 50 任何一方径改行为，亦严禁再按「C# 基线 50」口径复核对账。
锁面：无行为锁（本条系登记缺口留槽）；`wconf/src/node_options.rs` 区间校验单测只锁 10..=95 门禁，不锁缺省值。

## 94. 空参 UNSUBSCRIBE/PUNSUBSCRIBE 所有权收口与 PUNSUBSCRIBE 零命中计数纠偏（不对齐 C# 全 broker 键表虚假退订帧与硬编码 `:0`）

工单 doc-deviations-stale-registry-six-anchors 登记（r111-triage-unsubdrain 并案入本票范围补充一、执行方案第 6 步；r112-recheck111 撞号裁决「拟号让位顺编、号以实际先入库者为准」——本票合流期 §89/§90/§91 已由 LPOS 词元文法、requirepass AUTH 用户名门、COSCAN 域收口三票先占，本条顺编取 §94）。行为面缺陷另立 `task/todo/wnode-unsubscribe-mailed-frame-writeback-gap.md`，不入本条。

C# 一手形态：事实一（跨会话频道名泄露面）：`ListAllSubscriptions`/`ListAllPatternSubscriptions`（`garnet/libs/server/PubSub/SubscribeBroker.cs:259-289`，两函数分别起于 `:259` 与 `:278`、`:289` 为后者闭括号）形参收 `ServerSessionBase session` 而函数体从不消费它，恒返回全 broker 有订阅者的频道/模式键表；`NetworkUNSUBSCRIBE`（`garnet/libs/server/Resp/PubSubCommands.cs:270-285`）与 `NetworkPUNSUBSCRIBE`（同文件 `:350-366`）据此对他人订阅的频道逐条回 `unsubscribe`/`punsubscribe` 帧——`:281`/`:361` 的 `Unsubscribe(channel, this)` 对他者订阅返 false、既不减计数也照样发帧并照样带出频道名。事实二（计数纠偏）：`PubSubCommands.cs:378` 零命中尾帧计数硬编码 `TryWriteInt32(0)`，把该会话真实剩余活跃订阅数冲成 0；同位 UNSUBSCRIBE 臂（`:296`）却回写真实 `numActiveChannels`，C# 自家两命令互异。

Rust 侧裁决：空参退订仅回自有帧、零命中回单条 null 名尾帧。`network_unsubscribe` 空参臂（`wedb/wpubsub/src/session_commands.rs:302-325`）遍历 broker 全量隔离键、经 `ns_prefix.strip` 只触本租户通道（`:309` 未命中即 `continue`），逐通道仅在 `broker.unsubscribe(channel, subscriber)` 返真（本会话确曾订阅且退订成功，`:312`）时回帧并置 `owned`（`:313-321`），零 owned 才回单条 null 名尾帧（`:323-325`）。`network_punsubscribe` 空参臂同构（`:428-451`），零命中尾帧经 `write_unsubscribe_null_frame`（`:173` 起，帧体 `:176-178`）回写真实剩余活跃数——`session_commands.rs:449` 注释自证「C# 同位分支硬编码 :0，此处回写真实剩余活跃订阅数（含普通/分片频道）」。

后果与严禁回改：双侧对账在「同 broker 多会话交叉订阅」用例必然发散（C# 向未订阅该频道的会话泄露他会话频道名并回虚假退订帧、PUNSUBSCRIBE 零命中回 `:0`；rust 仅回自有帧、零命中回单条 null 名尾帧且计数为真实剩余数），勿判转写缺陷；严禁按 C# 形态回改（回改即复活跨会话频道名泄露面与计数失真）。本条系登记缺口补登而非在册偏差：`task/review_history/zcode-r14-pubsub.md:40` 的「核对无待办面」清单曾记「PUNSUBSCRIBE 零模式硬编码 0」为双侧一致，该分叉系 r14 之后新引入而未回写台账，旧记录按过时快照处理。

锁面：内联单测 `wedb/wpubsub/src/session_commands.rs` 的 `empty_unsubscribe_only_frames_owned_and_counts_remaining`（`:890` 起，题注 `:886-888` 自证两事实；源档「wnode/tests 侧」为误引，现位在本 crate 的 `mod tests`（`:631`）内）；集成锁 `wedb/wpubsub/tests/unsubscribe_all_ownership.rs` 的 `zero_subscriber_empty_unsubscribe_writes_single_null_frame`（`:84`）、`cross_session_names_never_leak_via_empty_unsubscribe`（`:107`）、`empty_unsubscribe_frames_only_owned_and_preserves_others`（`:139`）、`empty_sunsubscribe_frames_only_owned_shards`（`:181`）、`punsubscribe_zero_hit_frame_counts_remaining_subscriptions`（`:224`）。


## 95. 无盘全量同步扫描键门纪元排空等待有界化：静止未达成即判败重拍（C# 结构上无此窗，rust 自造键门承重件收口）

工单 wedb-diskless-epoch-drain-return-ignored-double-apply（源票 task/issue/zcode-r114-replbk1 立案二，r115-triage-replbk1 定级 P2）登记。

C# 一手形态：C# 无盘全量快照源系时点一致的 StreamingSnapshot 检查点镜像——`garnet/libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs:196-197` `TryPauseCheckpoints`（无界自旋拿暂停锁）后 `:304-305` `TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot)` 取迭代镜像，记录「进快照」与「其日志地址」由同一时点镜像天然同界，结构上不存在「排空未达成仍照样取锚」的窗口，故 C# 侧无对位竞态面。

Rust 侧裁决：rust 快照源为活存储 live scan，该界由本仓自造扫描键门收口，模块头不变量自述「记录效果进快照 ⟺ 记录地址 ≤ 锚」对增量语义非幂等记录（HINCRBY/LPUSH 类）结构成立（`wedb/wedb/src/server/replication/diskless_replication/scan_key_gate.rs:3-19`，两相常量 :42-44），其唯一承重件即 Blocking 相末的纪元静止排空等待 `provider.bump_and_wait_for_epoch_transition_async()`（`wedb/wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs:207`，原语 `wedb/wedb/src/server/cluster_provider/checkpoint.rs:43-61`，上限 `cluster_node_timeout()`，超时语义源 `wedb/wepoch/src/wait.rs:80-90`）。C# 无限自旋在 rust 有界化后，原实现丢弃返值放行（锚照样起算），现裁为**返值承判**：`false` 即 `return Err("diskless scan gate: epoch drain not settled within cluster-node-timeout")`，沿 `main_streaming_snapshot_driver` 既有 Err 收敛臂（`replication_sync_manager.rs:256-263`）全员 `set_status(Failed)`、`ScanGateGuard` Drop 注销整门放行挂起写、清册关窗（:271），副本沿既有节流重连整备重跑下一批窗。零新增机制、零线格式变化、无新配置必选项。

后果与严禁回改：本条为 rust 侧自洽性收口而非对 C# 的行为偏离，后果面在判败取向——排空超时属集群异常信号，此时对全量同步**判败重拍**优于带病续传（放行即静默永久主从键级双重应用发散、不随后续复制自愈，且全链无日志无告警）。**严禁回改为忽略返值放行**，亦严禁在此引入「排空失败后二次验证 tail 是否追平」的旁路机制把判败软化为重试通过（可选加固路仅留档：等待上限与 cluster_node_timeout 脱钩另立 diskless_drain_timeout 旋钮——未启动即按本条维持现状）。同款「忽略返值」形态他域多点（`wedb/wedb/src/server/cluster_provider/assembly.rs`、`cluster_session/failover.rs` 停写应答、`failover/primary_failover_session.rs` 两处、`failover/replica_failover_session.rs` 接管前后两处、`migration/migrate_driver/slots.rs`）：failover 族由工单 wedb-failover-stopwrites-epoch-drain-return-ignored-ack-write-loss（r26）统一收编收口——本条旧句对该族的「用途系配置传播屏障或角色切换整备，无数据收敛不变量挂其上」定性系旧快照失实，更正：`cluster_session/failover.rs` 停写应答与 `primary_failover_session.rs` 让渡后栅栏两处均为「静止达成才采位点回应答／才放行接管」的数据收敛栅栏，判败即 `-ERR`/赎回；族内其余位点（primary 赎回后归位栅栏、replica 接管前后两栅栏、`assembly.rs`、slot_mgmt/replica_of 同步形态 unsafe_bump）逐位点判净依据见该票「逐位点判净依据」节，本条不再枚举。集群迁移链族九处实点同款形态已由工单 wedb-migrate-epoch-drain-return-ignored-revive-window 统一收口（九处实点返值承判，锁面 `wedb/wedb/tests/migrate_epoch_drain_failclose.rs`）：`migration/migrate_driver/keys.rs` TRANSMITTING（:744）与 DELETING（:920）两栅栏走 `recover_and_fail!` 判败，MIGRATED 归位臂（:958）保留放行（对位 C# finally，INITIALIZING/MIGRATED 两态对键门均放行、无数据收敛不变量挂其上，与本条无盘键门口径一致，源码注释已声明防再圈）；`migration/migrate_driver/slots.rs` TRANSMITTING（:211）/DELETING（:271）同形——原枚举所引 `slots.rs:203-206` 定性「配置传播屏障无数据收敛不变量」系失实，此二点正是 SLOTS 链取数/删除收敛栅栏，一并更正；`migrate_session_range_index.rs:88/:137` 与 `migrate_session_vector_set.rs:89/:131` 四栅栏 Err 上抛，由调用侧既有 recover 臂收敛，不另设第二判点。族内注记一笔（工单 wedb-migrate-deleting-unheld-release-tree-claim，r25 案 4，不另立条）：同位 DELETING 收口的两处伴生面已随该票收口——keys.rs 树键循环节对未持有 claim 的盲释与 RI 链同款循环节删除（回归 C# DeleteKeysAsync / DeleteRangeIndex「只删不释」形，键级 claim 系上游 TODO），keys.rs 删除臂 `let _ = storage.delete_string` 吞错改 log::error 留痕（控制流不变，对位 C# delRes 留痕语义），持 claim 键删除被 DEL 闸 MigrationBusy 正当拒绝即落痕留源，源端不静默排空他人在搬运的树。

锁面：`wedb/wedb/tests/diskless_epoch_drain_failclose.rs` 以恒不追平夹具（批首纪元快照 + 极小 cluster_node_timeout）锁死判败面——断言 `run` 即 Err、批内会话全员 FAILED、门已注销、零扇出帧发出；回退返值丢弃（还原 `let _ =`）实测该锁转红（revert-proof 已验）。既有键门单测 `scan_key_gate_blocks_then_releases_per_domain`、`scan_gate_verdicts_via_slot_gate` 全绿不回退。

## 96. 复制快照链 TTL 面对 C# 整记录直拷的行为偏离（expire_unix_ms 亚毫秒下取 / 逐键两读竞态 0-TTL 瞬态幽灵）

工单 doc-deviations-replica-snapshot-ttl-two-behaviors 登记（源票 task/reject/zcode-r113-ttl2 三之4/三之5，r114-triage-reg1 定级 P4；纯登记面零行为改动，并行争号期本条编号以入库实况为准）。

C# 一手形态：C# 盘无全量快照对每条内联记录整条 LogRecord 字节直拷——`garnet/libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSnapshotIterator.cs:140` `DiskLogRecord.DirectCopyInlinePortionOfRecord`（fast path 对齐尺寸整拷、chunked path 经 Serialize 全量排空搬运，两路均不拆记录），RecordDataHeader 原始 expiration ticks 无损上副，副本终态过期刻度与主端逐 tick 同值，既无换算亦无第二次时钟采样。

Rust 侧裁决：rust 快照不搬原始 ticks，改发键级绝对毫秒字段。生成单点 `wedb/wedb/src/server/migration/migrate_driver/live_value.rs:194-199`（string 域调用位 :155、信封域 :167）：`Some(exp) if exp > now_ticks() => unix_time_in_milliseconds_from_ticks(exp)`，否则发 0；换算单点 `wedb/wbase/src/convert.rs:107-113` 整数除法（正域即 floor 下取，`TICKS_PER_MILLISECOND = 10_000`）；帧形 `wedb/wconn/src/record.rs:15-16` kind=1/2 `[i64 LE expire_unix_ms(0=无TTL)]`；副本接收端 `cluster_sync_slow`（`wedb/wedb/src/server/cluster_session/replication.rs:163`）复用迁移导入单点 `frame_import::import_migration_frames` 回填，与迁移面同帧路同款钳位。本条登记该链两宗有意分叉：

宗一（expire_unix_ms 亚毫秒 floor 下取）：主端到期 ticks 落毫秒格内非整毫秒位时，帧值 = floor(主 ticks)，副本键可比主端早过期，上界即 floor 丢的小数部分 **<1ms**（expire 恰落整毫秒 tick 边界零偏差，主副终态逐 tick 同值）。副端回乘 ticks（`wedb/wedb/src/server/migration/frame_import.rs:339` 调 `expire_at_milliseconds_to_ticks`，:335-338 注释自证「毫秒换算产物恒为 16 对齐，与 expire_at 头部 coarse 粗化落库值逐字节恒等」）：ms×10000 恒为 16 的倍数（10000 = 16×625），对 wkv `expire_at` 头部 4-bit coarse 粗化落库域（`convert.rs:193` 粗化单点清低 4 位 1600ns 分辨率；`wkv/src/ttl.rs:566-572` 入口粗化门）幂等合，粗化不再改动回乘值，分叉仅亚毫秒末位、不经存储链放大。

宗二（逐键两读竞态 0-TTL 瞬态幽灵）：逐键快照采集系两读窗——先读值（`read_live_value` 读 string/信封域，域读内置惰性过期裁决），后于独立 await 另读 TTL 并以另一时刻 `now_ticks()` 比较（`expire_unix_ms` :195-196 先后三段采样），迭代器逐键循环 `wedb/wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs:325-365`（`for key in keys` :325、`read_live_value` 调用位 :328）。键恰在值读与 TTL 比较之间到期则 `exp > now_ticks()` 不成立，快照帧发 expire_unix_ms=0，副端先落一无 TTL 常驻幽灵键。自愈靠增量续推：主端 TtlPurge DEL（`wkv/src/ttl.rs:391` `StoreEvent::TtlPurge` → `wnode/src/service.rs:436` 产 Delifexpim DETERMINISTIC|EXPIRED 条目入 AOF）只要晚于快照锚（构造位点 `replication_snapshot_iterator.rs:213-214` `snapshot_anchor = wal.tail_address()`，授予 :215-217）即经增量 AOF 通道必达副本删净该键，故属**瞬态**（窗口 = 该 DEL 条目送达时延）非终态。**自愈前提：自锚至该 DEL 送达段增量 AOF 流不中断**——窗口内增量流断裂且副本唯快照收敛时幽灵退化终态，后续对本面的任何裁决以本前提为准。C# 整拷记录内 ticks 不做第二次时钟采样，无此窗。

后果与严禁回改：两宗均无运行期危害（宗一幅度 TTL 毫秒读回末位噪声级；宗二瞬态自愈），危害在台账——副本对拍现形「副本早过期 <1ms」「快照键丢 TTL（瞬态）」时按本条判有意偏差，**严禁按 C# 整条 LogRecord 直拷形态回改**：wconn 帧协议 `expire_unix_ms` 毫秒字段系既定设计（M1/M2 单一真值源、迁移面与复制面共用同帧形与同导入单点，回改即弃帧协议重写复制+迁移双链）。若后续裁竞态需收紧，可选加固路为值读与 TTL 读并入同一无锁探针窗（源票 zcode-r113-ttl2 既定路，与 §53 成员级两次时钟采样修复同构思路），**非必须项，不启动即按本条维持现状**。

锁面：既有快照链测试 `wedb/wedb/tests/diskless_sync_ttl.rs` 的 `diskless_sync_preserves_ttl` 锁「整毫秒格 ticks 发送端提取=回填帧值、副本终态一致」（源端过期经 `expire_at_milliseconds_to_ticks` 构造恒落整毫秒，即宗一边界零偏差形）与「无 TTL 键帧值 0」，该档头部注释已回指本条号。待落语义锁（留后续行为票，本条零改动）：非整毫秒键副本 expire 不早于主端且差值在 1ms 上界内之逐式断言（帧值 = floor(主 ticks) 换算，逐式锁 `convert.rs` 单点）。竞态宗属时窗复现不作锁测，唯本条文字登记。

扩形后注（§118 入库）：本条「可选加固路」（值读与 TTL 读并入同一无锁探针窗）已在迁移/复制驱动探针 `read_live_value` 面以单探针窗落地，登记为本册 §118（宗三：换代触发臂、迁移链受害、终态形，三别项出本条覆盖面）；本条宗二之快照链时钟越线瞬态形裁决不变，仍按「非必须项、不启动即按本条维持现状」。

## 97. redis.call SET/GET 快路径错误帧折叠（对齐 C# 存储 API 直连语义，与 fallback 臂刻意不对称）

工单 wlua-fastpath-set-get-error-arm-fork（r120-triage-luaext1，P3 契约分叉）登记，采票面方案 1（对齐基线折叠）。

C# 一手形态：脚本侧 `api.SET` 臂显式丢弃 status 恒推 +OK（`garnet/libs/server/Modules/LuaRunner.Functions.cs:3214-3216` `_ = api.SET(key, value)`）；`api.GET` 臂非 `GarnetStatus.OK`（NOTFOUND/WRONGTYPE 等一切，`GarnetStatus.cs:9-35`）一律 `PushBoolean(false)` 令脚本续跑（`LuaRunner.Functions.cs:3246-3259`）。两臂全程走存储 API 直连，结构上不产 RESP 错误帧，脚本对错误应答只见原型语义（false / +OK）。

Rust 侧裁决：rust 快路径经 `dispatch_resp` 重入会话解析整包应答（`wedb/wnode/src/resp/resp_server_session/lua.rs`），`-WRONGTYPE`/`-ERR` 帧无 C# 对位原型态。收口为单机制两段：wnode 适配器只做形态映射 `ReplyError → ScriptApiError{ErrorReply, Protocol}`（`ErrorReply` **不携文本载荷**——C# 语义下该文本对脚本永不可见，携文即死载荷；协议损伤文本常量 `PROTOCOL_TEXT` 迁 wlua 单点）；折叠决策唯一落 wlua 快路径 Err 臂（`wedb/wlua/src/functions/redis.rs`）——GET `Err(ErrorReply)` 折 `false` 续跑、SET `Err(ErrorReply)` 答 `+OK`，唯 `Protocol` 保持上抛。`ScriptingApi::get/set` 错误面签名由 `Vec<u8>` 改该枚举（`wlua/src/api.rs`，`runner/host.rs` vtable 随动）。

后果与严禁回改：脚本不再因存储层错误应答被 `error_wrapper` 转真 Lua 错误中断。本条与 fallback 臂（`'-'` 帧转 Lua 错误上抛，快慢两路在此刻意分叉）的不对称系裁决而非实现遗漏：**严禁后审席按 fallback 形态把快路径 `ErrorReply` 改回上抛“统一”两路**，**严禁给 `ScriptApiError::ErrorReply` 回加文本载荷**，严禁在 wnode 侧折叠（决策点必须唯一在 wlua）。锁测：`wedb/wlua/tests/redis_call_fast_path.rs` 的 `fast_path_get_error_reply_folds_to_false`/`fast_path_set_error_reply_still_pushes_ok` + `wedb/wnode/tests/lua_script_tests.rs` 的 `eval_get_wrongtype_folds_to_false`（真 `-WRONGTYPE` 字节端到端）；三例 revert-proof 实测（临时移除折叠臂即全红，实得 `-ERR Lua encountered an error: ...`）。注：`lua_script_tests.rs` 原例 `eval_get_wrongtype_error_text_passes_through` 自称对标 C# 错误文本透传，实际钉住的正是本票定性的未对齐形态，已随本条改写为折叠方向。

## 98. ACL default 认证器记录在场零回落 / 无记录回落臂保留面（C# 全链零回落 / rust 引导单例装配期不落盘）

工单 wacl-default-store-record-bootstrap-fallback-stale-password 登记（r120b 席立案一、r121-triage-acluser1 复核转票定 P2，修复合入）。

C# 一手形态：`GarnetACLAuthenticator.Authenticate`（`garnet/libs/server/Auth/GarnetACLAuthenticator.cs:58-79`）按 username 直查 ACL 字典——查得句柄即委托 `AuthenticateInternal` 定成败（启用与口令判定在 `GarnetAclWithPasswordAuthenticator.AuthenticateInternal`，`garnet/libs/server/Auth/GarnetAclWithPasswordAuthenticator.cs:25-38`）、查无 return false，全链无任何回落臂；`ACL SETUSER default >newpass` 经共享句柄就地改写（`garnet/libs/server/Resp/ACLCommands.cs:185-226`）。口令文法基线：`>` 系追加非换密（`garnet/libs/server/ACL/ACLParser.cs:171` → `User.AddPasswordHash` 落 `_passwordHashes.Add`，rust `wedb/wacl/src/acl_parser.rs:140` 同义），引导 default 自带 requirepass，单 `>newpass` 后记录 = {旧口令, 新口令}、旧口令下一笔 AUTH 经记录 Success 臂仍有效系双侧一致的正确形态；真换密须 `resetpass` 组合（清空后仅持新口令），彼时下一笔 AUTH 旧口令即死。

Rust 侧裁决：存储为唯一真源，但 requirepass / nopass 的 default 引导句柄驻留装配期内存单例（`wedb/wacl/src/access_control_list.rs` `AccessControlList::new`，`wedb/wnode/src/service.rs:with_requirepass`，装配期不落盘），故 AUTH/HELLO 认证臂保留唯一一条回落臂，判据单点为 `AclAuthOutcome::NoRecord`——仅「存储点查无该用户记录」且 ns0 时坠落引导内存认证器（其单点门只认 ns0 default，`wedb/wacl/src/auth/garnet_acl_authenticator.rs`，与 §90 异名+无记录拒绝锁面同臂）。记录在场即存储为唯一真源：停用/口令不符/规则损坏/用户名非法四态一律 `Denied` 直接 -WRONGPASS，绝不回落（修复前三态与无记录同折叠 Denied，ns0 Denied 坠落回落臂命中引导单例陈旧 requirepass——`off` 停用与 `resetpass` 真换密组合下旧口令会话经回落臂复活、挂引导 +@all 绕开收权；单 `>newpass` 场景旧口令本就经记录 Success 臂放行，与回落臂无涉非本缝）。落点在 `authenticate_user_via_store`（`wedb/wnode/src/resp/acl_commands.rs`）与 `network_auth_session`/`process_hello_command_state` 两臂（`wedb/wnode/src/resp/resp_server_session/auth.rs`）。

后果与严禁回改：本条区别于 §90（§90 锁「异名+无记录 → 回落臂内单点门 None 拒绝」，回落臂本身保留面不变）。严禁把 Denied 任一子态回折进 NoRecord 或恢复 Denied 坠落回落臂（回改即复活「记录在场回落缝」——换密/停用对 default 失效、旧口令永久入场，认证安全面倒退）；严禁另建第二套回落判据或在回落臂外复制 default 名门（唯一回落判据 = NoRecord，唯一 default 门在引导认证器内）。无记录回落保留面本身系对 C# 零回落形态的登记内保留分叉（引导单例不落盘为既定架构，doc/zh/db.md §3.4），勿判转写缺陷。

锁面：`wedb/wnode/tests/wacl_default_record_fallback_test.rs` 的 `setuser_default_newpass_kills_old_requirepass`（单 `>newpass` 追加语义锁——旧口令双参形仍 +OK 经记录 Success 臂；`resetpass >newpass` 真换密后旧口令单/双参形与 HELLO 一体臂 -WRONGPASS、新口令 +OK、协议未升级探测）、`setuser_default_off_denies_all_passwords`（停用后任意口令 -WRONGPASS、认证态不破仍 -NOAUTH）、`setuser_default_revoked_rules_take_effect`（`resetpass >np2 -@all +get` 真换密收权后旧口令 -WRONGPASS、新口令会话 GET 放行 SET -NOPERM）、`store_auth_three_states_split_no_record_vs_denied`（NoRecord/Denied 停用/Denied 口令不符三态拆分直断）；§90 与无记录回落保留面回归由 `wedb/wnode/tests/requirepass_test.rs` 全链生命周期与用户名门双锁及 `wedb/wnode/tests/acl_tests.rs` 的 `basic_whoami_test`（存储无 default 记录回落 +OK）保持绿。锁面出生红订正（`>` 追加误当换密致两测出生即红）见 task/done/wnode-wacl-default-setuser-oldpass-revival-two-red-tests.md。

## 99. 复制快照装载/读值域钉：换号族不经键门，窗内域重绑由读值域钉承接（快照即锚时物理域投影）

工单 wnode-snapshot-swap-window-ungated（P1，终态损坏形）登记；与 §95（键门纪元排空判败）、§96（快照链 TTL 两读宗）同属无盘全量同步链裁决族。编号按现树末节实况让位（§98 为 wacl 在场零回落条）。

C# 一手形态：N.A.——C# 快照源系时点一致 StreamingSnapshot 检查点镜像（§95 已立），迭代与读记录同处冻结镜像内，结构上不存在「枚举一域、读值另域」的两读形；且 vns/vdb 路由格交换式换号族（SWAPDB/FLUSHDB/FLUSHNS = O(1) 换格 + bump_generation + DbMeta 记录）系 wedb 多租户自有面，C# 无对位机制。

Rust 侧裁决：rust 快照源为活存储 live scan，扫描键门只栅「携用户键」的写命令——换号族无用户键可校验、不经本门（本票已把 scan_key_gate.rs 模块头与 Blocking 相注释的「全部写命令」自述订正为「全部键写命令」并补域钉声明，杜绝后席按字面误判）。窗内换号族照常执行路由换格 + bump_generation，使快照会话枚举时物理域失效。原缺陷形态：迭代器装载与逐键读值两处仅 `set_context(逻辑 ns, db)` 即逐调用重解析（get_or_create_db + virtual_domain 代数守卫在 bump 后重解析到新域），构成换代两读形——窗内换号即读值换绑，帧键集来自旧域而帧值取自新域，副本终态静默永久发散且无收敛通道。裁单点域钉：两点逻辑物化 `set_context` 保留（session_slot 逻辑库位判定、冷检、路由重绑均需逻辑身份），其后紧随 `set_virtual_context(vns, vdb)` 钉死枚举时物理对，`is_virtual` 旁路代数守卫后，帧戳、装载、读值、释门四点恒等枚举时物理域（`wedb/wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs` 装载臂 :309、读值臂 :360，域钉律见该模块头「域钉」段）。wkv API 签名零改动（既定协调口径：本票不扩形；扩形备选已灭失）。同族码内正确先例即本案对标向量段——`wedb/wedb/src/server/sync_transport.rs` `export_vector_set_elements`（迁移停等链与无盘快照链共用的向量导出单臂）自备会话后按登记条目自带物理域无条件 `set_virtual_context` 直设，及迁移链接收端 `wedb/wedb/src/server/migration/frame_import.rs:180`；本律系该形态向数据段的推广。「把换号族一并入闸」的备选闸路不推荐：门语义承键写挂起，换号无键可栅，扩域操作挂起属第二套承重机制，违单机制基准。

后果与严禁回改：换号族效果收敛改由单律承接——「锚时物理域投影 + 锚后绝对值记录续推」：窗内换号的 DbSwap/DbMap/GcDeadDb 记录地址 > 锚，经 AOF 续推必达副本由 apply_dbmeta_record 单点回放扳指映射终态，故主从终态恒等（SWAPDB 形：副本先收旧域全量帧、后回放换指；FLUSHDB 形：旧域全量入帧随 GcDeadDb 整域退役）。**严禁后席把装载/读值两臂回退为仅 set_context 逐调用重解析**（重开两读窗），**严禁以「让快照见最新域」为名摘除域钉**；未来若需快照感知窗内换号，须重拍快照而非解钉。

锁面：`wedb/wedb/tests/snapshot_swap_domain_pin.rs` 四 #[test]——SWAPDB 1↔2 于三注入位（懒装载窗 = 冷窗钩承接位、首读前、两键读之间）断言副本与主终态逻辑态全等且等于「换指后映射作用于旧域内容」形（含旧域值入帧非 Gone 跳发）；FLUSHDB 注入案加探副本旧物理域直读锁「旧域全量投影」。注入体采同步 kernel 原语（routing.table.set×2 + bump_generation + try_persist_dbmeta_sync；flush_db + DbMeta 批）沿 `cold_tenant_lazy_load.rs` TEST_COLD_WINDOW_HOOK 注入先例（换号 kernel 为 async、钩为 sync，compio 禁 poll 栈嵌套 block_on，不走 RESP SWAPDB 生产入口）。测试挂子：iterator 内 `#[doc(hidden)]` TEST_SNAPSHOT_READ_HOOK/TEST_SNAPSHOT_READ_AT/TEST_SNAPSHOT_READ_COUNT（同族先例 TEST_COLD_WINDOW_HOOK）。revert-proof 口径：摘除两臂 set_virtual_context 即三 SWAPDB 案副本终态发散转红（未实际执行回退试验，按纪律测试仅编译入库）。

## 100. 双键移动族写回序先目标后源（不对齐 C# Tsavorite 单事务原子，以倒序加重放收敛补偿）

工单 wnode-lmove-writeback-order-replay-idempotency 登记（r120-triage-listmain1 立案、主代理二轮现码复验定 P2，修复合入）。编号按现树册尾实况顺延（§99 为复制快照域钉条）。

C# 一手形态：`ListMove`（`garnet/libs/server/Storage/Session/ObjectStore/ListOps.cs:212-301`）单事务原子体——双键 Exclusive 锁（:229-230）→ `txnManager.Run(true)`（:231）→ 源 GET 判型/判空（:242-251）→ dst 类型预检先于弹出（:263-271）→ ListPop+ListPush 双 RMW（:275/:283）→ finally 无条件 `Commit(true)`（:293-297）；`SetMove` 同构。「源已弹出且持久、目标未推入」的中间持久态在 C# 结构上不可达，唤醒在提交后且仅 dst（:299）。

Rust 侧裁决：本仓无事务打包机制（wkv `BatchStoreSession` 系纪元保护批上下文对标 C# IUnsafeContext，非写批事务），双键移动族两笔 save 独立落库，统一采「写回序先目标后源 + 重放收敛补偿」纪律，五臂同款收口——SMOVE 快臂 `wnode/src/resp/objects/set_commands/write.rs:set_move`（成文注源头）、SMOVE 慢臂 `set_commands/slow.rs`（set_move_cold 同款）、LMOVE/RPOPLPUSH 快臂 `list_commands/write.rs:list_move_core`（本票前移 dst 复验+save 至 src save 之前）、LMOVE/RPOPLPUSH 慢臂 `list_commands/slow.rs:move_core_cold`（本票同前移）、经纪 BLMOVE 出件臂 `collection_item_source.rs`（头注自陈取舍+:251-253 先写目标再写源）。dst 侧失败（升阶/超页门 `Ok(false)` 降级、迁移窗忙错 Err、存储 IO）时源零变异零写入，快臂 `Ok(false)` 经 SlowWait 整命令重放与慢臂错误帧出口均自完整初态整体执行；dst 成功而 src 失败时重放装载到「src 仍含元素、dst 已含」状态续跑。域差异注：集合 `insert` 幂等自收敛，最坏态为 member 双份可重试收敛；列表 `push` 非幂等，重放残留为「元素双份」——非自愈收敛、须再跑一条收敛命令清重（与经纪臂头注自陈同口径），较旧序危害「src 已持久弹出后 dst 门降级、重放弹空回 null 元素蒸发 / 尚余则弹出下一元素首个永久丢失且应答错位」为无害劣化选择，且双份客户端重读可察。

后果与严禁回改：严禁把五臂任一改回「先源后目标」或为对齐 C# 引入事务机制（对标裁决走本登记）；严禁新建第二套重放补偿判据。窗口/复验次序、同键旋转臂、应答帧面不因本裁决变动。

锁面：`wedb/wnode/tests/resp_list.rs` 的 `lmove_dst_promote_count_gate_replay_moves_element_exactly_once`（count 阈界 65536 恰推入界元素：bulk 应答恒弹出元素、src 删空回收、dst 含且仅含一处）、`lmove_dst_envelope_overflow_gate_replay_moves_element_exactly_once`（64KB 小页超页门同形界）、`lmove_dst_migration_busy_leaves_src_untouched_and_retry_converges`（try_swap_in_window 封窗夹具：存储忙错误帧后 src 零变异、释窗重试收敛）与 `lmove_cross_key_four_shapes_and_same_key_rotation`（异键四形/同键旋转/窥视回归锁）；SMOVE 既有界测（`resp_set.rs` set_move 族）零漂移保持绿。

## 101. 服务端 received 计数点前移至 accept 成功即刻含容量门拒绝臂配对计入（INFO 口径族；r14-conn「容量门拒绝除外」括注不采纳）

工单 wconf-net-tls-two-unregistered-deviations 登记（r111-net1 网络生命周期/流控面×C# 对拍席立案一，主代理二轮现码复验 2026-09-25 坐实，P3 登记级：不改码不改行为，只补台账）。编号按现树册尾实况顺编（§100 为双键移动族写回序条；票内拟号 §85/§86 系旧册势，早为 INFO STATS 两事务计数、MIGRATE timeout 二条先占）。

C# 一手形态：`IncrementConnectionsReceived`（`garnet/libs/server/Servers/GarnetServerBase.cs:65`）唯一定义，其唯一调用点在容量门判定通过之后、`handler.Start` 之前（`garnet/libs/server/Servers/GarnetServerTcp.cs:288`）；超限臂（`GarnetServerTcp.cs:236-241` 判负→`:302-307` `Interlocked.Decrement` 回退 `activeHandlerCount` + `AcceptSocket.Dispose`）的被拒连接对 `total_connections_received`/`total_connections_disposed` 双侧无痕——C# 超限连接不计 received。

Rust 侧裁决与现状：`note_connection_received` 计数点前移至 accept 成功即刻（`wedb/wnode/src/server.rs:1354-1359`，位于 accept 成功分支、`try_acquire_connection` 之前，注释自陈「rust 前移一档」），超限臂以 `note_connection_disposed` 配对计入（`server.rs:1371`，UDS 接入环 `:807-824` 同形配对），注册表注释自认分叉（`wedb/wnode/src/servers/consumer_registry.rs:521-541`：「C# 无此形态——其超限连接不计 received」）。`task/review_history/zcode-r14-conn.md:11` 原案明文「received 计数点前移到 accept 成功分支（容量门拒绝除外，与 C# 对齐）」，实现确越出该括注一档：限流触发期 INFO `total_connections_received`/`disposed` 双双高于 C# 同部署口径、rejected 增速被并入 received，属可观测统计面分叉。本席裁决：**维持现计数 + 登记，不改回**。

裁决与保留理由：计数前移系 r14 第 1 条 P1「accept 成功即预注册」修复的配套同动，received 随注册点位走方维持「received − disposed = 活跃条目数」不变式刚性、消除短命连接统计盲区；现形态有测试锁（`rejected_connection_notes_paired_dispose` 与 `register_unregister_cycles_counters` 直依配对臂，`consumer_registry.rs:722-746` 区）。且 C# 在 Start/TLS 失败臂仍计 received（`:288` 先于 `:290` Start 抛错），回改容量门臂只对齐一半、反与握手失败臂再分叉，还牵动删锁测属行为变更，越出登记级零行为变更边界。

后果与严禁回改：`r14-conn.md:11`「容量门拒绝除外」括注经 r111-netreg 席裁决**不采纳**，维持现计数，**严禁按 C# 回改**（回改即删测试锁且与 C# Start 失败臂仍分叉，属半对齐）。后续对拍轮遇「限流触发期 INFO 双侧双高」用例直引本条判有意偏差，勿重复疑报。锁面：`consumer_registry.rs:722-746` 区配对用例与 `server.rs` INFO 统计面既有用例维持绿（本票零行为改动）。

## 102. TLS 握手 10 秒确定性超时（TLS_HANDSHAKE_TIMEOUT 双层竞速，C# 无对物铁边，确定性收口补强）

工单 wconf-net-tls-two-unregistered-deviations 登记（r111-net1 网络生命周期/流控面×C# 对拍席立案二，主代理二轮现码复验 2026-09-25 坐实，P3 登记级：不改码不改行为，只补台账）。编号顺 §101 之后。

C# 一手形态：握手链 `NetworkHandler.Start`→`BlockingWait(AuthenticateAsServerAsync)`（`garnet/libs/common/Networking/NetworkHandler.cs:147-156`），其 `AuthenticateAsServerAsync`（`:180-185`）仅收外部 `CancellationToken`、无时长上界，accept 环 `GarnetServerTcp.cs:237-308` 亦无对位——慢/半开 ClientHello 在 C# 无限占位在途容量额度，客户端可观测恒存活至自弃或 KILL。

Rust 侧裁决与现状：`TLS_HANDSHAKE_TIMEOUT = Duration::from_secs(10)`（`wedb/wnode/src/server.rs:1190-1199`，`#[cfg(feature = "tls")]`，Slowloris 熔断论证注释自证）+ `timeout`×`kill_token` 双层竞速（`:1432-1460`，timeout 置 `with_cancel` 内层三层落值竞速），超时/取消双臂同走 RAII 收口（`_in_flight` Drop 回落容量额度、handler Drop 注销登记、握手 future Drop 撤销在途读），超时即无应答 FIN 收口。生产默认硬编码，三参数不经配置面（仅 `:361-364`/`:411`/`:441-442` 测试短值注入通道）。

裁决与保留理由：此为与 §84 停机排空 5 秒强收护栏同款「rust 确定性收口 vs C# 无限等」档位——对 TLS 握手面提供确定性时延上界，防慢/半开 ClientHello 永久占死在途守卫与套接字。§84 走了补登程序、本臂漏登；不登记则后续 TLS/网络对拍轮把加固读成断连缺陷、把 C# 无限等读成待复刻契约，每轮重复撞面。Slowloris 定性按内网威胁模型口径（可观测分叉论证，非公网风险主张）。

后果与严禁回改：属确定性收口补强，**严禁按 C# 形态回改为无上界等待**；三参数暂不经配置面暴露，与 keepalive 同一单点常量裁决（工单 zcode-r60-netclose 判例口径）。随行终态注：`r14-conn.md:31-35` 原案「为握手加 idle 超时兜底」分支已按 10 秒确定性超时 + 预注册双闸终态落地、非半成品。锁面：`server.rs:361-364`/`:411`/`:441-442` 测试注入位维持绿（本票零行为改动）。

## 103. INFO STATISTICS 段名别名接受（from_name 首臂特判 STATS，接受面自研超集；严禁删别名或按 C# 回改报错）

工单 doc-deviations-metrics-rounding-and-info-alias 登记（宗二；r114 可观测性全谱席立案三，r115-triage 审核现码复验坐实，P3 登记级：不改码不改行为，只补台账）。编号按 merge dev 后现树册尾实况顺编（§102 为 TLS 握手超时条；与在途 wave-integrate 票 connlimit 条预占号存在撞号可能，主代理合并时统一对账让号）。

C# 一手形态：`InfoMetricsType` 枚举（`garnet/libs/common/Metrics/InfoMetricsType.cs:20-80`）全 16 成员无 STATISTICS；段名解析 `TryGetInfoMetricsType`（`garnet/libs/server/SessionParseStateExtensions.cs:25-66`）仅 STATS 臂（:38）无 STATISTICS；`INFO STATISTICS` 落未知段臂（`garnet/libs/server/Metrics/Info/InfoCommand.cs:43-44` 置位错误 → :49-53 回 `-ERR Invalid section STATISTICS. Try INFO HELP`）。真 Redis 亦仅认 stats——STATISTICS 两头皆拒。

Rust 侧裁决与现状：`from_name`（`wedb/wresp/src/metrics/info_metrics_type.rs:133-135`）首臂特判 `STATISTICS`→`Stats` 别名，`INFO STATISTICS` 与 `INFO STATS` 同义成功出段，接受面为 C# 侧超集，属自研接受面而非转写漏项（别名自述测试注释在 :147）。

后果与严禁回改：维持别名 + 本登记，零行为风险；删别名使既有调用方由成功出段降为 `-ERR`，属对外破坏性变更，**严禁**；对拍轮亦**严禁**按 C# 报错形态静默回改。如需撤别名，须先撤销本登记并同步改写两侧锁测为期望 ERR，方为正当变更程序。回归锁面（禁动）：`wedb/wnode/tests/session_metrics_slowlog_tests.rs:237-238`/`:345` 按 STATISTICS 出段钉测；`wedb/wresp/src/metrics/info_metrics_type.rs` tests `from_name_matches_cs_section_names` 钉死别名臂。

## 104. zset 聚合族两处对 C# 的修复/防御性偏离（NaN 归零点产点集 / 交集收缩异常面消除）

工单 doc-deviations-zset-aggregate-three-divergences 登记（r114-triage-reg1 立案、现树双锚逐宗亲验，定级 P3：不改码不改行为，只补台账；原宗三 ZDIFF 负 numkeys 经 r115-recheck114 立案甲翻案除名，不入偏差账，仅留观察句）。编号顺编注记：本票 worktree 合流期初按当时册尾 §102+1 拟取 §103，复跑 `git merge dev` 时 dev 已入 metrics 条（INFO STATISTICS 别名）先占 §103——按先入库者得号让位顺编取 §104；wave-integrate connlimit 条仍预占候号，主代理合并时统一对账（本条内回指锚与代码注释回指均已同步为 §104）。

### a) NaN 归零点覆盖聚合族全产点（C# 唯一门在交集聚合步，其余产点裸奔）

C# 一手形态：聚合链路唯一 NaN 门在 `garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetIntersection`（:1501-1588）多键聚合步 :1574-1576（`if (double.IsNaN(pairs[kvp.Key])) pairs[kvp.Key] = 0;`，:1573 自陈 "Arguably we're doing bug compatible behaviour"，对位 Redis zunionInterGenericCommand 的 isnan 归零）。其余聚合面全部无门：`SortedSetUnion`（:1185-1263）加权种子 :1222 `pairs[key] = weights[0] * score` 与累加 switch :1243-1251 零门；`SortedSetUnionStore`（:1126-1183）经 :1165 `newSortedSetObject.Add(element, score)` 把 NaN 直接落进目标键；交集加权种子块 :1527-1533、无权重共享字典种子 :1524、单键早退臂 :1535-1536（早退在门先于）同样无门。全仓 `double.IsNaN` 另两处命中系输入侧报错臂（`libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:155` ZADD-INCRBY 臂、`:346` ZINCRBY 臂，均回 SCORE_NAN 不落聚合面），聚合产点唯一性坐实。

Rust 侧裁决：`nan0`（`wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:939-940`）把归一点扩到 `combine_sets`（:946-1010）全产点五处——交集种子加权 :977、交集步加权与聚合 :983（一线双产点）、并集加权 :997、并集聚合 :1000。依据是 §2 输入侧恒拒 `nan` 词形之**不变式延伸**（§2 保证入集分值恒非 NaN，聚合产 NaN 只能来自 0×±inf 之类运算），而非 §2 条目本体有聚合归一登记——§2（:11-15）实为输入词形文法条目，通篇无产点集归一，`write.rs` `nan0` 文档注释旧注（订正前现位 :925/:934）「§2 已登记」系假锚，已随本条订正为「§2 不变式延伸」并回指本条。

分叉可达性实证：`ZADD k inf m`（inf 词形分值入集）双侧同收（C# `ParseUtils.TryReadDouble`（`libs/server/Resp/Parser/ParseUtils.cs:141-149`）兜底 `RespReadUtils.TryReadInfinity`（`libs/common/RespReadUtils.cs:1271-1303`，"INF"/"+INF"/"-INF" 大小写不敏感）；rust 按 §2 仅认 `inf`/`+inf`/`-inf`）。执行 `ZUNION 1 k WEIGHTS 0`：C# `0*inf=NaN` 无门直达应答分值文本（`ZUNIONSTORE` 更把 NaN 落进目标键持久化），rust `nan0` 归零回 `"0"`；`ZINTER 1 k WEIGHTS 0` 同型（单键早退臂在门先于，NaN 直出）。

后果与严禁回改：对拍现形「C# 应答 NaN / NaN 落键 vs rust 恒回 0」按本条判有意偏差，**严禁按 C# 形态回改**（复活 NaN 进并集应答与 STORE 目标键系上游缺陷面；C# 交集门为 bug compatible 自陈，rust 门位统一至全产点是维持「键面与聚合产物恒非 NaN」存储不变式的刻意扩面）。`write.rs` `nan0` 文档注释锚已订正回指本条。

### b) 交集收缩异常面消除（C# 迭代中删字典必抛掐连接 / rust 正常回交集成员表）

C# 一手形态：交集步 `foreach (var kvp in pairs)` 循环体内直调 `pairs.Remove(kvp.Key)`（`SortedSetOps.cs:1556/:1560`），.NET `Dictionary` 枚举器版本哨在下一次 `MoveNext` 必抛 `InvalidOperationException`（末元素删除亦抛，文档口径 `EnumFailedVersion`），会话层无 catch——`keys[0]` 含 `keys[i]` 外成员的任何真收缩（如 `ZINTER 2 a b`，a⊄b）C# 现形为掐连接。

Rust 侧裁决：交集重写为最小集合仅作成员遍历基准、逐集合查集、缺席 `continue 'outer`（`write.rs:961-986`，'outer 循环起 :969，头注回指本条 :955-957），彻底消除迭代删字典形态；聚合累加序仍与 C# 同构恒以 `objs[0]` 加权分值种子按索引序累计（:974-975 `objs[0]` 缺席判定、:977 种子、:983 逐键累加，浮点非结合性不动，承 r15 发现五），最小集基准只改遍历顺序不改数值路径。

后果与严禁回改：对拍现形「C# 掐连接 vs rust 回成员表」按本条判修复/防御性偏离，**严禁按 C# 迭代删字典形态回改**（回改即复活 .NET 枚举器版本哨异常面）。C# 对拍段不复刻该异常面，skip 注记直引本条即可，勿判转写缺失。

观察句（不入偏差账）：一、ZDIFF 负 numkeys 双侧同文无分叉——C# `SortedSetCommands.cs:915-918` 明载 `Count<2` 前置 wrong-number-of-args 门（`parseState.Count` 系命令名后纯参数数），带附加参数者 `Count-1≥1` 恒不等 `nKeys(≤-1)` 与 `nKeys+1(≤0)`、:926 对齐门必回 syntax error，负 `nKeys` 无过双门路径、:932 负界构造不可达；rust `write.rs:732` `check_arg_count!(2..)` 与 :739-746 双门同构，`ZDIFF -1` 两侧同回 `-ERR wrong number of arguments for 'ZDIFF' command`、`ZDIFF -1 k` 两侧同回 `-ERR syntax error`，:739-742 系与 C# :926 等位的对称防御非偏差面（r115-recheck114 立案甲翻案）。二、跨运行时 libm 末位 ULP 差系浮点超越函数实现差异的客观限制，不入偏差账，GEO 域后续席免复勘。

锁面：`wedb/wnode/tests/zset_aggregate_nan0_disjoint_locks.rs` 的 `zunion_nan_weight_zero_score_gate`（宗一 rust 侧文本锁）、`zinter_disjoint_no_abort`（宗二回表且会话存活锁）与 `zdiff_negative_numkeys_same_text_locks`（翻案面双侧同文锁，非偏差登记）；本票零行为改动。

## 105. ACL +命令名 携首尾空白 rust 精确拒收（不镜像 C# Enum.TryParse 空白修剪怪癖，Enum.TryParse 怪癖族第二形）

工单 doc-deviations-acl-pluscmd-whitespace-trim-registry 登记（r120b 席立案三，源档 task/reject/zcode-r120b-acluser1.md，r121-triage-acluser1 复核转票、主代理二轮现码复验 2026-09-25 坐实，定级 P4 登记级：不改码不改行为，只补台账）。编号顺编注记：本票 worktree merge dev 后按册尾实况 §104+1 顺编取 §105（票内「现册末节 §96、拟号 §98 起」系旧册势，早已为 §97–§104 各条先占）；编号按入册时实况顺编、先入库者得号、撞号让位顺编不覆写——wave-integrate connlimit 条预占候 §105、doc-deviations-setuser-new-fail-residue 等在途 doc-deviations 族票亦可能争号，主代理合并时统一对账让号。

C# 一手形态：ACL SETUSER 参数不经任何修剪原样入 ops（`garnet/libs/server/Resp/ACLCommands.cs:155-158` `parseState.GetString` 逐条入数组、`:196-198` 逐条 `ACLParser.ApplyACLOpToUser`；RESP bulk 串可合法携带空白），`+` 臂 `op.Substring(1)` 得命令名（`garnet/libs/server/ACL/ACLParser.cs:226`，`" get"` 前导空格原样保留），`TryParseCommandForAcl`（`:274` 起）以 `Enum.TryParse(effectiveName, ignoreCase: true, out command)`（`:281`）解析——.NET Enum.TryParse 沿 Enum.Parse 语义接受首尾空白，`" get"` 内部修剪后解析为 GET；`IsValidParse`（`:324-327`，注释 `:322` 自陈 "handling the weirdness in Enum.TryParse"）仅拒 NONE/INVALID 哨兵与含数字输入、不拒空白；去点重试臂（`:285`）同受该怪癖覆盖。故 `ACL SETUSER u "+ get"` 在 C# 静默授予 GET 并回 +OK（`"- get"` 撤权同形）；非空白污染名（如 "bad!name"）才解析失败落 `AclCommandDoesNotExistException`（`:254`）。

Rust 侧裁决与现状：同链严格拒绝含空白词形——RESP SETUSER 参数经 `ascii_sanitize`（仅非 ASCII 折 '?'，无 trim，`wedb/wnode/src/resp/acl_commands.rs` ops 循环逐条入 `apply_acl_op_to_user`，`wedb/wacl/src/acl_parser.rs:159-186`），命令名走 `try_parse_command_for_acl`（`acl_parser.rs:201-242`）→ `lookup_command`（`:246-249`）→ `catalog::try_get_by_cs_name` 精确名查表（`:247`）无修剪，`" get"` 查表 None 后去点重试臂（`:217-222`）与 SLAVEOF（`:224-228`）、CLUSTER|SET-CONFIG-EPOCH（`:230-234`）`eq_ignore_ascii_case` 别名臂均不命中；自定义名回落臂 `is_valid_custom_command_name`（`:276-287`）首字符空格非字母数字亦拒，`:185` 回 `AclError::CommandDoesNotExist(" get")`，经 `acl_exception_message` 落 `-ERR Command ' get' does not exist`。`" get "` / `"get "` 同形。归属界定：规则串切词路径（C# `ACLParser.cs:83` `input.Trim().Split(...)` / rust `parse_acl_rule`（`acl_parser.rs:69-98`）`split_whitespace`）双侧切词同形、`+ get` 复合形在该面结构性不可达，不属本分叉（源档曾误挂此臂，经 r121 审核订正）；分叉面唯一为 RESP 命令参数臂。

后果与严禁回改：无运行期危害——rust 拒收侧更贴 Redis 上游、无权限放大，危害在治理面：双侧对拍夹具对 `"+ get"` 类用例必然发散（C# +OK 并授 GET / rust -ERR Command ' get' does not exist）无据可查，后续对拍轮易误判漂移或按「对齐原型」名义补修剪——补修剪即引入「空白污染名静默授权」真实权限面回归，才是真危害。**严禁按 C# Enum.TryParse 空白修剪怪癖回改补 trim**（`try_parse_command_for_acl` 与去点重试两臂均不；怪癖面不镜像为在册既定裁决，与 §89 LPOS 词元归一化同族口径）。Enum.TryParse 怪癖族第二形注记：第一形为数字回退怪癖（`IsValidParse` 含数字即拒之对位登记，工单 doc-deviations-getkeys-numalias-keynum-overflow-registry，在途入册号待定，入册后与本条互为同族回指）。锁面：`wedb/wacl/tests/acl_command_whitespace_no_trim_locks.rs` 锁「`" get"` / `" get "` / `"get "` 及子命令形 `" client|list"` 解析 None + ACL SETUSER 活链 `"+ get"` 回 CommandDoesNotExist 精确文案 + 用户权限零变化 + 无空白正对照 `"+get"` 照常授权（证拒收仅因空白）」；`acl_parser.rs` 既有用例 `try_parse_command_for_acl_cases`（`:560` 起）与 `is_valid_custom_command_name_cases`（`:602`，`"bad name"`/`"bad!name"` 拒绝锁）保持绿；`try_parse_command_for_acl` 文档注释锚已随本票补 §105 回指（防读者把 `lookup_command`「对标 Enum.TryParse(ignoreCase)」注误读为全量镜像怪癖）。本票零行为改动。

## 106. COMMAND GETKEYS/GETKEYSANDFLAGS 数字形命令名拒收与 keynum 大值饱和钳制（不对齐 C# Enum.TryParse 数值回退与 int 回绕空回）

工单 doc-deviations-getkeys-numalias-keynum-overflow-registry 登记（两宗并一；来源 task/reject/zcode-r118-cmdkeys1.md 立案一，r119-triage-cmdkeys1 现码复验双锚、两仓 JSON 亲验、csproj checked 设置与零登记 grep 坐实，定级 P3 登记级：不改码不改行为，只补台账）。编号按 merge 后 dev 册尾实况顺编：本票 worktree 执笔时 dev 册尾 §104（zset 聚合族条），尾+1 位 §105 原为在途 connlimit 票（在途连接上限执行粒度）预占候号、本票让位顺编拟取 §106；复跑 `git merge dev` 时 §105 已由 ACL 空白词形票（同 Enum.TryParse 怪癖族）先入库得号（connlimit 候号随之后移），§106 恰为实况尾+1、本票落号 §106 不改，号以实际先入库者为准、主代理合并时统一对账（本条内回指锚与代码注释回指均已同步为 §106）。

### a) 数字形命令名拒收（C# Enum.TryParse 数值回退收敛解析 / rust strum 纯名词法必拒）

C# 一手形态：GETKEYS 门序 `TryGetSimpleCommandInfo`（`garnet/libs/server/Resp/BasicCommands.cs:2041-2063`，GETKEYS :1352 / GETKEYSANDFLAGS :1389 调用）首参 `Enum.TryParse<RespCommand>(cmdName, true)`（:2046）具 .NET 枚举数值回退——纯数字串按基础值直接收敛：`COMMAND GETKEYS 8 k1` 收敛 `RespCommand.DEL`（`garnet/libs/server/Resp/Parser/RespCommand.cs:39` DEL=8，rust 镜像 `Del = 8` 值面对称）取 DEL 键规格（BeginSearchIndex 1、range 形）提取回 `*1` + k1。界内枚举值而无 JSON 条目者，`TryGetSimpleRespCommandInfo`（`garnet/libs/server/Resp/RespCommandsInfo.cs:388-401`）取 `SimpleRespCommandsInfo[cmdId]` 仍回 true 走 Default（仅 cmdId 越界才 false），KeySpecs 空落 `RESP_COMMAND_HAS_NO_KEY_ARGS`（:1357/:1394）；名不收敛才落 :1353/:1390 `-Invalid command specified` 错误帧。

Rust 侧裁决：`RespCommand::from_cs_name`（`wedb/wresp/src/command.rs:697-700`）转发 strum `EnumString` 纯名词法（:16 派生、:21 `ascii_case_insensitive`），数字串必 None → simple_info None → 回 `RESP_INVALID_COMMAND_SPECIFIED`（`wedb/wnode/src/resp/basic_commands/mod.rs:prepare_command_keys_context` 解析点 :338、错误出口 :359-362，GETKEYS :387 / GETKEYSANDFLAGS :407 共用）。差分例 `COMMAND GETKEYS 8 k1`：C# 回 `*1`+k1 / rust 回错误帧——分叉实存；数字形本非合法命令词法，rust 拒收为定义性行为。

### b) keynum 大值饱和钳制（C# unchecked int 加法回绕空回 *0 / rust isize 饱和算式 + 界内钳制回真实键）

C# 一手形态：keynum 型末键 `lastKeyIdx = firstKeyIdx + ((keyNum - 1) * keyStep)`（`garnet/libs/server/SessionParseStateExtensions.cs:1003`）为 unchecked int 算式（全仓 `*.csproj` grep `CheckForOverflowUnderflow` 零命中，复验证实），溢出静默回绕。firstKeyIdx 推导链：`beginSearchIdx = Index - (isSubCommand ? 2 : 1)`（:935-937）、index 形 `firstKeyIdx = beginSearchIdx`（:947）、keynum 臂 `firstKeyIdx += FirstKey`（:1002）。GETKEYS 切片态（`BasicCommands.cs:1361`/`:1398` `Slice(IsSubCommand ? 2 : 1)`）下 bs Index=2 命令 `firstKeyIdx = (2-1) + FirstKey(1) = 2`：numkeys=2147483647 时乘法项 2147483646 不溢、加法 2+2147483646=2147483648 溢出回绕 -2147483648，钳制段（:1006-1009）对负值不命中，提取循环 `i=2 <= -2147483648` 首迭代即假零迭代，恒回空数组 `*0`。

Rust 侧裁决：keynum 臂（`wedb/wresp/src/catalog/simplified.rs:try_get_key_search_args` :163-178）走 `strict_i32` 值域收口（:168）、`key_num <= 0` 早拒（:169-171）、`first_key_idx += first_key`（:172）、:173 `saturating_add((key_num as isize - 1).saturating_mul(key_step))` isize 饱和算式得 2147483648 不回绕，:176-177 钳 `count - 1` 界内回真实键清单。差分例 `COMMAND GETKEYS EVAL s 2147483647 k`（切片态 Count=3）：C# 回 `*0` / rust 回 `*1`+k——分叉实存，C# 回绕系上游算术事故。

可达命令集（两仓 `RespCommandsInfo.json` 复算全等：全库 keynum 型 14 条 KeyStep 恒 1、KeyNumIdx=0、FirstKey=1）：bs Index=2 共**七命令 EVAL/EVALSHA/BLMPOP/BZMPOP/ZDIFFSTORE/ZINTERSTORE/ZUNIONSTORE**（后三者系键规格第二臂，即 KeySpecifications 下标 1），即本宗全暴露面（票面"四命令"不全，已订正）。bs Index=1 共七命令 ZUNION/LMPOP/ZMPOP/ZDIFF/ZINTER/ZINTERCARD/SINTERCARD（票面"八条"系误计，复算为七），其 firstKeyIdx=1、1+2147483646=2147483647 恰 int.MaxValue 界内不回绕、界内后钳 Count-1 双侧同形，不入偏差账。

裁决与保留理由：两宗均为 rust 定义性行为/防御面优于 C# 上游意外（数字形非合法词法、int 回绕系算术事故），无运行时崩溃或数据面危害，危害纯在治理面：GETKEYS/GETKEYSANDFLAGS 系集群客户端提键公开自省命令，双侧同输入必异应答（a 宗 C# `*1`+k1 对 rust 错误帧；b 宗 C# `*0` 对 rust `*1`+k，bs=2 七命令可达）。不登记则后续对拍席无据可查（本轮 r118-cmdfam1 即因只验乘法项漏加法项误判 b 宗不可达），或按「对齐原型」名义回改时静默引入数字形误解析与 int 回绕空回——对齐第 18 条话术，回改才是真回归。定级对齐 bitops 立案二补登记先例（P3 登记级文档票）。怪癖族系谱注：本宗 a 数字回退形与 §105 空白修剪形同属 Enum.TryParse 怪癖族两形（§105 注「入册后与本条互为同族回指」兑现于此），同链旁注：C# `IsValidParse`（ACL 域含数字即拒）与本命令面无关、勿混引。

后果与严禁回改：对拍席遇「`COMMAND GETKEYS 8 k1` 应答分歧」或「`COMMAND GETKEYS <EVAL 形> 超大 numkeys` C# 空回 / rust 回真实键」用例直引本条判有意偏差，勿重复疑报；**严禁**按 C# `Enum.TryParse` 数值回退形态给 `from_cs_name` 加数字解析臂（引入数字形误解析），亦**严禁**按 C# unchecked 回绕形态回改 :173 饱和算式（去掉 saturating 即把上游算术事故引入本仓，恶意 numkeys 空回键清单）。随行注释锚订正（本票落地）：`mod.rs` 父子组合注释原引 `BasicCommands.cs:2044` 订正为 :2046（实测行），并补本宗 a 数字形拒收分叉自述回指本条；`simplified.rs` :205-207「numkeys 溢出安全，PR #2112 语义」自述注释补本条回指。

锁面：新增 `wedb/wresp/tests/getkeys_keynum_bs2_saturating_clamp.rs`（EVAL 形 bs=2 规格 Index=2/KeyNumIdx=0/FirstKey=1/KeyStep=1 × numkeys=2147483647，钉饱和钳制 (2,2,1) 与提键面回 k，与既有 bs=1 截断形对偶）；既有锁应答零漂移不动——`wedb/wresp/src/catalog/simplified.rs` tests `simple_key_spec_numkeys_clamped`（:504-523，ZUNION bs=1 形截断锁）与 `extract_keys_malicious_numkeys_clamped`/`extract_keys_malicious_numkeys_rejected`（:645-673）、`wnode/tests/resp_tests.rs` `command_getkeys_parent_sub_lookup`（父子组合面差分锁）。本票零行为改动。





## 107. 在途连接上限执行粒度：rust 进程级单注册表全局计数（K 端点共享一 L）vs C# 每 GarnetServerTcp 实例独立计数（有效天花板 ~K×L）

工单 doc-deviations-conn-limit-global-count-granularity 登记（来源 task/issue/zcode-r120-accept1.md 立案一，r121-triage-accept1 执笔、主代理第二轮现码复验改判 P4：登记级零行为变更，缺省 network-connection-limit=-1 不限流即无观测差，须多端点+显式有限 L 方现分叉）。编号按 merge 后册尾实况顺编（§100 为双键移动族写回序条，§101–§106 为 received 计数、TLS 握手超时、STATISTICS 别名、zset 聚合族、ACL 空白词形、GETKEYS 夹取六条先入册；票内拟号系旧册势，撞号让位不覆写）。

C# 一手形态：每端点独立一 listener 实例——`garnet/libs/host/GarnetServer.cs:294` 循环内 `servers[i] = new GarnetServerTcp(opts.EndPoints[i], ...` 逐端点传 `opts.NetworkConnectionLimit`；容量门读该实例自有字段——`activeHandlerCount` 为 `garnet/libs/server/Servers/GarnetServerBase.cs:28` protected int 实例字段（:118 构造置零），`networkConnectionLimit` 为 `garnet/libs/server/Servers/GarnetServerTcp.cs:28` readonly 实例字段（:81 构造逐端点赋值），`GarnetServerTcp.cs:237-240` 判定 `currentActiveHandlerCount <= networkConnectionLimit`（:239 实例内递增、:240 双条件）只在单实例计数内比较，超限臂 `:305-307` 回退计数（`Interlocked.Decrement` + `AcceptSocket.Dispose()`）即关不写 RESP。故 K 个 TCP 端点各得独立天花板 L，聚合有效并发上限约 K×L。观测面则跨实例求和——`garnet/libs/server/Metrics/GarnetServerMonitor.cs:283-305` 区内 `total_connections_received += garnetServer.TotalConnectionsReceived`（:294）、`total_connections_active += get_conn_active`（:296），即 INFO 总量为全局、唯门限判定为逐例。

Rust 侧裁决：转写形为全端点全核单一进程级注册表——`ConsumerRegistry`（`wedb/wnode/src/servers/consumer_registry.rs:383` 起，`active_handler_count` 单 AtomicI64 字段 :396、`new()` :426/:428 置零），`try_acquire_connection`（:443-452）对单计数 `fetch_add` 后判 `limit == -1 || n <= limit`、超限回退返 None，`install_global`/`global`（:460-470）钉死进程级单例契约；`wedb/wnode/src/server.rs` 三处 worker（:581 TCP worker0、:672 TCP 其余核、:759 UDS）同读一份 `self.network_connection_limit`（字段 :348、装配 :220），:578/:669/:756 同一 Arc `session_provider` 即共享注册表——TCP accept 环容量门在 `run_tcp_accept_loop`（函数 :1328）内 :1367-1380（try_acquire_connection 调用 :1368，超限臂 `note_connection_disposed` 后 continue 即 drop stream），UDS 环同门 :816。K 端点共享一 L，有效上限即 L 不分端点。配置面：`wedb/wconf/src/node_options.rs` `DEFAULT_BIND="127.0.0.1,::1"`（:42）/`DEFAULT_BIND_ANY="0.0.0.0,::"`（:44）经 protected_mode 分流取用（:1103-1106，默认 true :694-695/:916），两态皆缺省即 2 TCP 端点、unixsocket 尾部追加端点再 +1（:1122-1123）；`network_connection_limit` 旋钮三处齐备可配非硬编码（常量 `DEFAULT_NETWORK_CONNECTION_LIMIT=-1` :91、clap `default_value_t` :499、serde :502-503，启动期 :1244 定界 <-1 拒）。两侧契约差为「拒绝发生的时点/有效上限倍数」，非计数总量口径差。

后果与严禁回改：INFO connected_clients/total_connections_active 两侧总量同额（C# `GarnetServerMonitor.cs:283-305` 跨实例求和 = rust 单注册表单计 `consumer_registry.rs:396,:443-452`），观测数据无分叉，唯一外部可观测量为超限拒绝发生的时点（有效上限 L vs ~K×L 倍数差）。理由钉死：rust 单注册表为架构刚性（CLIENT 族、停机排空双条件判据 `consumer_registry.rs:678-689`/`:715`、received/disposed 配对不变式的共同地基）且更贴 Redis 法定 maxclients 全局语义（实例级总闸而非 per-bind-address 闸），C# 逐 listener 计数系每端点一实例之产物；维持现实现，严禁按 C# 拆回每端点计数（拆回破单全局注册表机制、属过度设计）。后续对拍轮见拒绝时点倍数差直引本条，勿反复立案。随行注记：INFO total_connections_received/total_connections_active/connected_clients 两侧总量同额，分叉仅拒绝时点与有效上限倍数，非观测口径错；received 计数前移分叉另宗（见 §101），本条不复述其裁决。

验证面：纯登记零代码改动、零构建测试运行；consumer_registry 既有用例（:946-970 区排空双条件判据、容量门双条件判定）与 node_options :2277 区 network_connection_limit 解析用例属既有基线复核非新增，本票零夹具。

## 108. PFCOUNT 多键并集累加器恒稠密化（不对齐 C# 稀疏累加器遇稠密源 TryMerge 恒败被静默剔除）

工单 doc-deviations-pfcount-union-dense-source-trymerge-drop 登记（源档 task/reject/zcode-r120b-hll1.md 立案一，r121-triage-hll1 席双锚现树复验坐实，定级 P3 登记级：不改码不改行为，只补台账）。编号顺编注记：本票 merge dev 后册尾实况 §105+1 应取 §106，因 connlimit 条预占候号让位拟 §107；复跑 `git merge dev` 时 dev 已入 §106（GETKEYS 数字形夹取条）与 §107（connlimit 条）先占——按先入库者得号让位顺编取 §108，撞号不覆写（本条内回指锚与代码注释回指均已同步为 §108）。

C# 一手形态：`libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogLength`（:88 起，多键循环 :128-174）的并集累加器编码随首命中键——首命中非尾键经 :164 `Buffer.MemoryCopy(srcHLL, dstHLL, ...)` 原样拷入 dst 缓冲，首命中键稀疏即得稀疏累加器；后续键 :168 `_ = HyperLogLog.DefaultHLL.TryMerge(srcHLL, dstHLL, sbDstHLL.Length)` 以 `_ =` 丢弃返回值、零失败处理；末键计数 :170-173 `count = Count(dstHLL)` 只读累加器。`TryMerge`（`libs/server/Resp/HyperLogLog/HyperLogLog.cs:930-958`）按 **dst 内容头帧**判编码（:932 `GetType(dstBlob)` → :272 `*(ptr+3)`）：dst 稠密臂 :933-938 恒成回 true；dst 稀疏 + src 稠密直落 :957 `return false; // always fail if merging from dense to sparse` 恒假臂，稠密源被静默剔除。`dstLen` 参数仅作稀疏+稀疏原位增长判定上界（:947），增长放不下 :954 同回 false 静默丢弃（姊妹面）；且 `dstLen` 恒为缓冲初始化值 `hllBufferSize = DenseBytes = 12304`（HyperLogLogOps.cs:125 `FromPinnedPointer` 后 `sbDstHLL.Length` 循环内不再被 GET 改写；`StorageSession.cs:31`、`HyperLogLog.cs:111`），dst 内容头帧系首键拷贝所致、与缓冲容量无关。后果：`PFCOUNT k_sparse k_dense`（k_sparse 稀疏在前）C# 静默回 `card(k_sparse)`，稠密键贡献为零，与 Redis「全部键取并集」语义相悖；无并发要求即常态可达、无错误帧纯静默。可达面收窄注记：唯一分叉序为「首命中键稀疏 + 后续含稠密键」；首键稠密序 dst 稠密、后续稀疏键经 `Merge` 的 sparse_to_dense 分派恒成，无此面。C# 测试 `garnet/test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:218-264` 仅稀疏小键双键（mykey/mykey2 各 4 元素），未锁定该缺陷行为。

Rust 侧裁决与现状：累加器恒稠密化——`hll_union_seed`（`wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:201-211`）稠密载荷直接接管、稀疏载荷 `init_dense` + `sparse_to_dense` 一次展开为稠密缓冲；`hll_union_absorb`（:213-229，「稠密化唯一实现」）令其余键一律交 `try_merge` 单点分派；累加器恒稠密时 `whyperlog/src/merge.rs:try_merge` 的 dense-dst 分派臂（:10-15）恒成并入，任何源键永不被剔除。快路径 `hyper_log_log_length` 多键臂（:527 起，:553 行注「累加器恒稠密」）与慢路径 `slow_hll_count` 多键臂（:330 起，:360 同源 absorb 调用点）双臂同源单实现，快慢不漂移。码面原有性能/分配视角刻意差异自述（:201-221 头注「与 C# 的刻意差异」「稠密化唯一实现」、:553 行注）系真锚但非登记锚，本票于模块头 PFCOUNT 段（:26-28，既有第 16 条互引处）补登本条互引一枚，挂钩台账单一真源；不改 `hll_union_seed` / `hll_union_absorb` / `try_merge` 任何逻辑。

裁决与保留理由：rust 真并集即 Redis 一致侧，C# 静默剔除系原型可观测缺陷；属「上游缺陷修复型」家族先例（同第 3/10/12/16/17 条）与「rust 更优侧纯登记」同档（对照集合算术族 doc-deviations-set-arith-absence-shortcut-divergence），纯登记不改码。

后果与严禁回改：双侧对账在 `PFCOUNT k_sparse k_dense`（稀疏在前）用例必然发散（C# `card(k_sparse)` vs rust 真并集≈card(k_sparse)∪card(k_dense)），对拍轮据此判有意偏差、勿判转写缺陷；**严禁按 C# 改回稀疏累加器形态**（改回即复活 :957 恒败臂静默剔除面乃至 :954 稀疏增长姊妹面，才是真回归，同第 16/17 条「改回才是真回归」口径）。若后续上游修复 C# 该臂，本条按登记撤销处理。与第 16 条互引注记：同为 `HyperLogLogLength` 多键循环面的姊妹分叉分条登记——第 16 条钉「尾键 NOTFOUND 恒回 0」面，本条钉「稠密源被恒败臂静默剔除」面（含 :954 稀疏增长姊妹面），裁决独立、勿合并裁决。锁面：`wedb/wnode/tests/hyperloglog.rs:pfcount_single_key_sparse_and_dense_and_missing`（现树 :738 起）钉 `[missing, sparse, dense]` 三键并集 ≈ 10+2000，键序正中 C# 缺陷面（sparse 首命中得稀疏累加器、dense 后到恒败被剔，C# 该序回 card(sparse)），系本条 rust 真并集语义锁，既有测试零新增；本票零行为改动。

## 109. SETUSER 对新建用户施加规则失败不残留占位记录（不对齐 C# AddUserHandle 先于 ops 施加的先建后验形态，失败残留系脏状态非转写缺陷）

工单 doc-deviations-setuser-new-fail-residue-registry 登记（来源 task/reject/zcode-r120b-acluser1.md 立案二，r121-triage-acluser1 复核转票、主代理第二轮现码复验 2026-09-25 坐实，定级 P4 登记级：不改码不改行为，只补台账）。编号顺编注记：本票 worktree 首次 merge dev 后册尾 §107+1 拟取 §108（票内拟号「§97 起」系旧册势，早已为 §97–§107 各条先占）；复跑 `git merge dev` 时 dev 已入 PFCOUNT 稠密化条先占 §108——按先入库者得号让位顺编取 §109，撞号不覆写（本条内回指锚与代码注释回指均已同步为 §109）。

C# 一手形态：`NetworkAclSetUser` 对不存在用户**先建后验**——`GetUserHandle(username)` 查无（`garnet/libs/server/Resp/ACLCommands.cs:164`，:166 null 分支）即 `new UserHandle(new User(username))`（:168）并 `AddUserHandle` 入 ACL 活字典（:172，:166-179 块），此后才逐条施加 ops（:196-198 `ACLParser.ApplyACLOpToUser` 循环）；`new User(username)` 构造默认全空态 `IsEnabled=false / IsPasswordless=false / _enabledCommands=CommandPermissionSet.None`（`garnet/libs/server/ACL/User.cs:54-60`），ops 中途任一 `ACLException`（未知操作/未知分类/未知命令/非法哈希）进 catch 写 `ERR ...` 帧即返回（`ACLCommands.cs:228-235`）——**已入字典的空用户残留**，`GetUserHandles` 直返活字典（`garnet/libs/server/ACL/AccessControlList.cs:118-120`），ACL LIST/USERS/GETUSER 从此可见该名与 off/无口令/无权限空 map。

Rust 侧裁决与现状：`apply_set_user`（`wedb/wnode/src/resp/acl_commands.rs:308`）在独占可变副本上逐条落定（:334-337 `User::from_user(&current_user)` 副本），任一 op 失败即 `?` 早退（:346-348；未注册自定义命令名失败关闭臂同回 Err），尾置 `store.write` 写穿仅在全部成功后执行（:369-372）——失败**零记录残留**：GETUSER 仍回 nil、ACL LIST/USERS 扫存储不含该名。rust 为事务性更优实现，但属对基线的可观察行为分叉：同一「新建用户 + 链中途非法 op」后，C# 侧 USERS 有名、GETUSER 回空用户 map，rust 侧无名、GETUSER 回 null。

裁决与保留理由：维持 rust 尾置写事务形态 + 本登记。C# 先建后验的失败残留是脏状态（off/无口令/无权限僵尸用户占名，此后认证恒失败），rust 失败不留痕为洁净侧，无运行时危害；危害纯在治理面——不登记则后续对拍或上游回归对照轮把「C# 有名 / rust 无名」的失败残留差误判为转写漂移缺陷，或按「对齐原型」名义改回先建后验形态（对照 deviations 第 18 条「改回才是真回归」先例话术），登记即闭环。

后果与严禁回改：对拍轮遇「SETUSER 新建用户中途失败后 ACL USERS/GETUSER 双侧可见性差异（C# 有名+空用户 map / rust 无名+null）」用例直引本条判有意偏差，勿判转写缺陷；**严禁按 C# 先建后验形态回改 `apply_set_user`**（把句柄入存储/字典提前到 ops 施加之前即引入失败残留脏状态，才是真回归）。锁面：本票零行为改动、零新增代码；`apply_set_user` 副本尾置写现状注释（`acl_commands.rs:334-336`）已随本票补 §109 回指锚与本条互引；acl 域既有用例保持绿。

## 110. BITFIELD 未知子命令错误回显编码语义分叉（C# Encoding.ASCII 逐字节 '?' 折叠 vs rust as_str_safe 非 UTF-8 整体空串；回显文案域刻意差异，族级编码语义裁决随条固化）

工单 doc-deviations-bitfield-unknown-subcmd-echo-registry 登记（来源 task/reject/zcode-r119-bitfield1.md 立案一；r120-triage-bitfield1 甄别通过、主代理第二轮现码复验 2026-09-25 坐实转执，P3 登记级：不改码不改行为，只补台账 + 注释锚 + 语义锁测）。编号按 merge dev 后现树册尾实况顺编（§87–§107 为 BITOP 起二十一条先入册；源档拟号 §88 与甄别订正号 §98 起皆系旧册势）。完工前 merge dev 复跑两轮撞号：并行在途姊妹席 PFCOUNT 条与 SETUSER 条先入库分得 §108/§109——按「撞号让位不覆写」本条顺编改取 §110，条内与代码注释锚（ext.rs 头注、garnet_bitmap.rs 锁测注）回指已同步 §110。

C# 一手形态：`garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitField`（:484-485）未知子命令回显 `AbortWithErrorMessage($"ERR Bitfield command {Encoding.ASCII.GetString(command)} not supported")`——.NET ASCII 解码对大于 `0x7F` 的字节**逐字节**替换为 `'?'`（best-fit 回退，多字节 UTF-8 序列每字节各折一个 `?`，非按字符）。同源机制锚：`garnet/libs/server/Resp/Session/ParseUtils.cs:ReadString`（Encoding.ASCII 同族，§60 CLIENT SETNAME 非 ASCII 严拒与 ACL SETUSER 口令折叠两先例的机制出处域）。

Rust 侧裁决与现状：`parse_bitfield_args`（`wedb/wnode/src/resp/bitmap/bitmap_commands.rs:759`）未知子命令回显插值现树实锚 :832-835（源档引 :825-829 行号偏移订正），`format!("ERR Bitfield command {} not supported", command.as_str_safe())`，command 为裸 `&[u8]`（:773 取 `parse_state[curr_token_idx]`），三臂比对皆 `eq_ignore_ascii_case`，非匹配字节必落回显臂。`as_str_safe` 单源实现 `str::from_utf8(self).unwrap_or("")`（`wedb/wresp/src/ext.rs:RespSliceExt for [u8]` :84-86）：非 UTF-8 token 整体吞空串、合法多字节 token 原样按 UTF-8 吐出。快慢双臂共用 `parse_bitfield_args` 单源（慢臂调用点 `wedb/wnode/src/resp/basic_commands/slow.rs:922`，票引 :879-883 行号偏移订正），分叉双臂同暴露。可达性：parse_state 经 arg_in(recv_buffer) 字节切片（`resp_server_session/core.rs:1439/:1514`）二进制安全，RESP bulk string 可携任意字节；未知 token 需非 OVERFLOW/GET/SET/INCRBY 且后随合法 encoding（u8）与 offset（#0）方达回显臂。

分叉矩阵（同一非法输入两侧错误帧字节不等）：
- `BITFIELD k \xFF u8 #0`——C# 回 `ERR Bitfield command ? not supported`；rust 回 `ERR Bitfield command  not supported`（command 与 not 间**双空格**，token 整体吞空）。
- `BITFIELD k F\xC3\xB6 u8 #0`——C# 回 `ERR Bitfield command F?? not supported`（逐字节折）；rust 回 `ERR Bitfield command Fö not supported`（UTF-8 原样）。

裁决与保留理由：维持 rust `as_str_safe` 现形，回显文案域刻意差异。命令一律 `-ERR` 帧拒绝、帧型双侧一致，无数据面危害，分叉纯在错误文案字节；但严格逐字节对账的客户端解析器或双侧对账测试在该输入上必然发散，不登记则每轮重复撞面且易被误判为其他位点的转写缺陷。取向对齐 §61「刻意差异加登记」先例；与 §60 机制同源、域互斥——彼为客户端名落账校验域（严拒），此为错误回显文案域。关键裁量：`as_str_safe` 系**全仓错误回显族级单源**（现树调用点 ≥12：config_commands.rs:62/:269、key_admin_commands/keys.rs:377/:383、basic_commands/get.rs:406、basic_commands/mod.rs:273/:279/:317/:333/:347、resp_server_session/core.rs:1301、server/cluster_session/slot_mgmt.rs:425、bitmap_commands.rs:834），其中 basic_commands/mod.rs:273 拿它作 COMMAND DOCS 查表键、core.rs:1301 作子命令分派键，均非纯文案位——该编码语义属族级裁决而非 BITFIELD 单点，BITFIELD 位点仅为切片内双锚见证，随本条固化，全族对账测试据此有据可查。

后果与严禁回改：**严禁**后审按 C# ASCII 折叠形在调用点散改（散改越出单机制红线、回归面远超登记级零行为变更边界）。如后审评估后决意收口，唯一通道为 `as_str_safe` 单点一次到位升格（返回 `Cow<str>`，字节 ≥0x80 逐字节映射 `b'?'`，对齐 `Encoding.ASCII.GetString`），且**必须复用既有 `wbase::ascii_sanitize` 单机制**（`wedb/wbase/src/ascii.rs:11`，逐字节 '?' 折叠现成实现，纯 ASCII 零拷贝借用），杜绝另立第二套折叠机制；族内调用点随之自然收敛。ACL SETUSER 口令折叠用例（acl_tests.rs:1737-1753，口令哈希落账的数据域裁决，与本案回显文案域互斥）与 §60/§61 锁面不联动、零漂移。收口落地即按登记撤销流程**同票撤销本条**并改钉 `?` 折叠形锁测，两态不许并存。随行注记：源档甄别订正「core.rs:1301 系 abort 文案位、硬查表键在未引的 mod.rs:333/:347 from_cs_name」经复验不采信——core.rs:1301 现形与 mod.rs:273 查表键两位置均属实，族级论据不受影响。

验证面：本票唯一代码注记为 `wedb/wresp/src/ext.rs` `as_str_safe` 头注补裁决声明回指本条（非行为改动）；新增语义锁测 `wedb/wnode/tests/garnet_bitmap.rs:bitfield_unknown_subcmd_echo_encoding_lock` 双形逐字节钉现形（`\xFF` 双空格形 + `F\xC3\xB6` UTF-8 原样形）防漂移；既有 `bitfield_ro_read_only_and_overflow_policies` 用例（现树 :660 起，票引 :657-703 行号偏移订正）应答零漂移。

## 111. 尺寸与几何族旋钮收形登记（五组：内联尺寸对／初始读尺寸／reviv 几何／缓冲池预算对／pagecount 折叠与 tree_cache_budget 反向形）

工单 doc-deviations-memory-shape-five-groups-registry 登记（来源 task/issue/zcode-r117-confsweep1.md 立案二，r118-triage-conf1 席现树逐点亲验通过；定级 P3 登记级：纯台账补登，零行为变更零代码改动）。编号顺编注记：本票按 merge dev 后册尾实况顺编取号、两轮撞号让位——首次 merge 后册尾 §108+1 拟取 §109，复跑时 dev 已入 SETUSER 失败残留条先占 §109，让位顺编 §110；完工前再复跑 `git merge dev`，dev 又入 BITFIELD 回显条先占 §110——按「先入库者得号、撞号让位不覆写」终取 §111（本条内编号与 §69/§70/§77 三处回锚注均已同步为 §111）。

立案背景：C# 尺寸族十员（PageSize/LogMemorySize/PageCount/SegmentSize/ObjectLogSegmentSize/ReadCachePageSize/ReadCachePageCount/MaxInlineKeySize/MaxInlineValueSize/InitialIORecordSize）、reviv 几何族五员、缓冲池双员均为 `garnet/libs/host/Configuration/Options.cs` + `defaults.conf` 在册用户可配面；rust 侧各组以自有收形落地，收形注记真实在场但「行为面偏差即本条旋钮缺席」的法定登记此前仅 §69/§70/§77 三形入册，五组收形与台账脱钩（deviations 全文 grep reviv/MaxInline/PageCount/BufferPool/initial-io/tree_cache_budget 命中处均为命令语义内文 incidental 用词，零登记条目），后续对账席无台账可引必反复疑报或误判转写漏项。本条循 §77 形制一条族目分五小节补登；裁决措辞统一：**行为面偏差即本条旋钮缺席，严禁按 C# 形态回改、勿判转写漏项、勿反复提「补旋钮」工单**。

### a) 内联尺寸对（MaxInlineKeySize/MaxInlineValueSize → 页容纳性裁决形）

C# 一手形态：`Options.cs:625/:629` `--max-inline-key-size`/`--max-inline-value-size` 两旋钮在册（`defaults.conf:485/:488`），键上限区间 [0,1022] 缺省 1022 字节、值上限区间 [0,16777214] 缺省 min(1m, PageSize/2)。
Rust 收形锚：`wedb/wconf/src/node_options.rs:123-124`（page_size 字段注「页容量决定单条内联记录上限：值 ≤ 页容量 − 记录头 − 键 即可内联存储；16MB 页可承载 C# DefaultMaxInlineValueSize = 1MB 基线的大值」）与 `wedb/wkv/src/config.rs:315-318`（规划式第 4 条：页容量随预算自适应 clamp [64KB, 16MB]，生产大机 1GB 预算推导 16MB 页、单页内联承载 C# 1MB 基线大值）双点。rust 侧无 `max_inline_key_size`/`max_inline_value_size` 用户旋钮（全仓 grep 零命中）。
裁决：内联上限不再是独立旋钮，折叠为页容量容纳性裁决；C# 1MB 基线大值在 rust 内联路径原样可承载，无可观测行为分叉，行为面偏差即本小节两旋钮缺席。

### b) 初始读尺寸（InitialIORecordSize → 全仓无对物形）

C# 一手形态：`Options.cs:633` `--initial-io-record-size` 在册（`defaults.conf:491`），缺省 128 字节，控制磁盘记录首读的初始读尺寸。
Rust 收形锚：wdev 读路径按记录实际尺寸读取，结构上无初始读尺寸概念；全仓 grep `initial_io_record` 零命中复验（多次）。
裁决：该旋钮在 rust 无对物亦无需补偿形，缺席即裁决形；行为面偏差即本小节旋钮缺席，勿判转写漏项、勿提议补旋钮。

### c) reviv 几何族五员（→ wreviv 编译期 PowerOf2Bins 预设形）

C# 一手形态：`Options.cs:545/:550/:570/:576/:584` 五旗标（`--reviv-bin-record-sizes`/`--reviv-bin-record-counts`/`--reviv-search-next-higher-bins`/`--reviv-bin-best-fit-scan-limit`/`--reviv-in-chain-only`，`defaults.conf:440-458` 在册），用户可自定义自由列表分桶几何、跨桶检索档数、best-fit 扫描上限与链内原地复活开关。
Rust 收形锚：`wedb/wreviv/src/lib.rs:11-18`（「配置面收缩」自证：C# `RevivificationSettings` 可配面在 wedb 收缩为编译期 PowerOf2Bins 预设——生产唯一构造入口 `FreeRecordPool::new`（wkv store 层）、跨桶检索固定至多探相邻一档（对标 C# `NumberOfBinsToSearch` 默认 0 = 当期桶 + 相邻上一档）、`with_bin_sizes_and_scan_limit`/`find_bin_index` 仅供对标 C# RevivificationTests 的集成测试夹具、生产不使用）与 `wedb/wreviv/src/bin.rs:43-46`（`TAKE_RETRY_ROUNDS=4` 注自证「与 C# TryTakeFirstFit 以 recordCount 折半递减至 MinRecordsPerBin 的多轮重试同构——容量 256 时约 5 轮」）。全仓 grep `reviv_bin_record`/`number_of_bins_to_search` 零命中，wconf grep `best_fit_scan_limit` 零命中（引擎内部字段非配置面）。
裁决：几何五员全收编译期预设，reviv 用户面仅 `--reviv`/`--reviv-fraction` 两旋钮（`node_options.rs:167-179`）保留；行为面偏差即本小节五旋钮缺席。

### d) 缓冲池预算对（BufferPoolMemoryBudget/UseLegacyBufferPool → wbase 单 origin-return 双层常量形）

C# 一手形态：`Options.cs:95/:99` 双旋钮（`defaults.conf:52/:61` 在册）：`--buffer-pool-memory-budget` 缺省 "1g"（small 25%/large 75% 分区）；`--use-legacy-buffer-pool` 可切回 ConcurrentQueue 分级旧池后端。
Rust 收形锚：`wedb/wbase/src/pool/mod.rs:21-22`（双层字节预算对标 C# small/large budget 与 `LargeTierMinBytes`：≤256KB 与 >256KB 配额强隔离、`AtomicI64` 无锁记账）与 :86-94（`LARGE_TIER_MIN_BYTES`=256KiB :87、`DEFAULT_SMALL_BUDGET_BYTES`=32MiB :91、`DEFAULT_LARGE_BUDGET_BYTES`=128MiB :94，:90 注自证「嵌入式场景整体缩 1/4，隔离语义与 C# 一致」）。rust 侧仅存 origin-return 单后端，全仓 grep `use_legacy_buffer_pool`/`buffer_pool_memory_budget` 零命中。
裁决：后端选择旋钮缺席 + 字节预算旋钮收编译期常量形，行为面偏差即本小节双旋钮缺席。

### e) pagecount 折叠形与 tree_cache_budget 反向形

pagecount 折叠：C# `Options.cs:71/:119` `--pagecount`/`--readcache-pagecount` 双旋钮（`defaults.conf:31/:76` 在册）可直接指定主存/读缓存初始页数。rust 侧无独立 pagecount 旋钮，该推导折叠进 `--hlog-memory-size`/`--read-cache-memory-size` 字节预算：`wedb/wconf/src/node_options.rs:129-131` memory_size 字段注自证「对标 C# LogMemorySize 的 pageCount 推导 `num_pages = next_power_of_2(memory_size / page_size)`」、:150-152 读缓存页数推导注、规划器实现 `wkv/src/config.rs:357`。行为面偏差即本面两旋钮缺席（推导式等价承载），勿判转写漏项。
tree_cache_budget 反向形：`wedb/wconf/src/node_options.rs:157-165` `--tree-cache-budget` 旋钮注自证——C# `RangeIndexManager` 无预算机制、`CacheSizeTracker` 只跟主日志与读缓存，本闸为本仓分层架构自研组件，观测面对标 CacheSizeTracker TargetSize 高水位语义（见 `doc/zh/collection.md`），启动期一次性注入、不做热更。本旋钮系 wedb 自有面（C# 无对物，属「对物缺席」反向形，非旋钮缺席），此前仅代码注释登记，本小节补台账回锚、不改其任何语义。

划界声明（循 §77 形制）：本条仅负责登记五组收形事实；若后续决定为任何一组补充用户旋钮，属功能代码变更，归独立代码票承载，本条不预设裁决方向。
查重与互引注记：尺寸族既登面划界——PageSize/ReadCachePageSize 容纳形见 §69（本条 a) 不复裁）、LogMemorySize/IndexMemorySize 自适应规划见 §70（本条 e) 只钉 pagecount 双旋钮缺席形）、SegmentSize/ObjectLogSegmentSize 见 §77、MutablePercent 见 §93 留槽；五组互不重叠，§69/§70/§77 条尾已设回锚。与在途票关系：与 todo/wconf-net-tls-two-unregistered-deviations 同形（漏登补册、登记级、严禁回改实现）但域互斥（网络/TLS vs 内存尺寸族）不并；与 todo/doc-deviations-stale-registry-six-anchors（既有条目订正票）不并——本案为新增条目第二作业型，该票六锚收口判据不受影响。补登后各席 grep 台账一词即停。
验证面：纯登记零代码改动、零构建测试运行；本条五小节全部 file:line 锚点执笔时点 merge dev 后现树亲测新读（node_options.rs 内联注 :123-124／pagecount 折叠注 :129-131／tree_cache_budget 注 :157-162，wkv/src/config.rs :315-318，wreviv/src/lib.rs :11-18 + bin.rs :43-46，wbase/src/pool/mod.rs :21-22 + :86-94；C# Options.cs :71/:95/:99/:119/:545/:550/:570/:576/:584/:625/:629/:633），后续重构漂行以锚词 grep 存活为准（max-inline-key-size、reviv-bin、buffer-pool-memory-budget、initial-io-record-size、tree-cache-budget 已入台账，对账席 grep 命中即停）。


## 112. redis.call/acl_check_cmd 整值 number 形参直写精确 i64 文本（不对齐 C# Lua 5.4 lua_tolstring 形参转串文本）

工单 doc-deviations-lua-number-param-text-registry 登记（立案源 task/reject/zcode-r119-luaext1.md 席审查档立案一，r120-triage-luaext1 席双锚现树复跑坐实、主代理二轮现码复验 2026-09-25 改判定级 P3 登记级：不改码不改行为，只补台账）。引擎归属订正：C# 侧脚本引擎是标准 Lua 5.4（KeraLua 绑定，`DllImport "lua54"`，`garnet/libs/server/Lua/NativeMethods.cs:25` `LuaLibraryName = "lua54"`）而非 Luau；源档「Luau lua_tobuffer / Schubfach 最短往返 lnumprint」全系 rust 侧 vendored Luau（`wedb/wlua/build.rs:1-7` luau0-src 0.21.0+luau736）的转换面，源档把双侧引擎归属对调，本条按现树订正。编号顺编注记：本票首次 merge dev 后册尾实况 §108+1 拟取 §109；完工复跑 `git merge dev` 撞号两轮——SETUSER 失败残留条先入库得 §109 让位取 §110，BITFIELD 回显条又先入库得 §110 再让位取 §111；合入前 dev 实况 §111 已由 MEMORY 尺寸几何族目先入库得号——三让终取 §112，撞号不覆写（本条内回指锚与代码注释回指均已同步为 §112）。

C# 一手形态：number 形参一律经 VM 原生转串。redis.call fallback 循环 `argType is LuaType.Number` 时 `state.TryNumberToString(argIx)`（`garnet/libs/server/Lua/LuaRunner.Functions.cs:3284-3289`）；acl_check_cmd 局部 `PrepareAndCheckRespRequest` 对 `LuaType.String or LuaType.Number` 直接 `KnownStringToBuffer` 并自陈注释「will coerce a number into a string」（`:3037-3043`）。`TryNumberToString`（`garnet/libs/server/Lua/LuaStateWrapper.cs:521-534`）落 `NativeMethods.CheckBuffer`（`garnet/libs/server/Lua/NativeMethods.cs:343-348`，注释自引 lua.org 5.4 手册与 lapi.c）即 Lua 5.4 `lua_tolstring`：integer 子类型出精确整值文本；float 子类型出 `LUAI_NUMFFORMAT "%.14g"` 文本、int 形值补 ".0"（123.0 → `"123.0"`）、-0.0 保留负号（`"-0.0"`）。

Rust 侧裁决与现状：成帧单点 `prepare_and_check_resp_request`（`wedb/wlua/src/functions/redis.rs:356-412`）number 臂（:381-406）对整值 double（`num.fract() == 0.0 && num >= i64::MIN as f64 && num <= i64::MAX as f64`，:389，上界 `i64::MAX as f64` 即 2^63）直写 `write_int64_as_bulk_string(num as i64)` 精确 i64 文本（:390；`wedb/wresp/src/resp_memory_writer.rs:576`），仅非整值回落 `try_number_to_string`（:392-401；`wedb/wlua/src/state.rs:437-443/:446`，经 vendored Luau `luaL_tolstring` → lnumprint 最短往返）。该成帧为三消费点共用：redis.call fallback `dispatch_scripting_command_fallback`（:580-615，成帧调用点 :586-591）、`frame_and_acl_check`（:312-341）与 `acl_check_sub_command` 的 BITOP 展开臂 ACL 成帧（:294-306）。整值直写分支此前码内无「刻意差异」自陈、deviations 零登记，本条补登为唯一真源。float 子类型全域五边界例双侧文本（rust / C#，C# 侧按 %.14g 复算订正）：`123.0` → `"123"` / `"123.0"`；`2^62` → `"4611686018427387904"` / `"4.6116860184274e+18"`；`2^63` → as 饱和 `"9223372036854775807"` / `"9.2233720368548e+18"`（立案档原文「…8547e+18」系四舍五入失手，与 -2^63 例互斥，二轮按 %.14g 复算改判）；`-2^63` → `"-9223372036854775808"` / `"-9.2233720368548e+18"`；`-0.0` → `"0"` / `"-0.0"`。

裁决与保留理由：保留 rust 侧。精确整值文本与第 1 条「保留 double 最短往返特性、C# 截断破坏 IEEE 保真」裁决同向——float 子类型整值直写 i64 无损，C# "%.14g" 文本在 |v|>2^53 即失真（2^62 的 C# 文本 `4.6116860184274e+18` ≠ 原值 4611686018427387904），改回 C# 文本形态系数值失真方向。根因在引擎级设计（transpile SKILL.md:58「lua 用 luau」）：Luau 单 double 数值型、无 Lua 5.4 的 integer/float 子类型之分，C# 的 number 子类型在 rust 侧输入面上不可观测，故本条无 fix 侧备选——rust 侧任何局部改写（含撤销 i64 直写臂、全量改走 Luau 转串）均无法复现 C#「integer 子类型精确文本 + float 子类型 %.14g 补 .0」双形态，登记为唯一收口路径，后审席勿再按 C# 文本形态提报修复。

后果与严禁回改：同一脚本向同键写入的参数文本字节双侧分叉（`redis.call('MSET','k',2^62)` 双侧落键内容不同），后续 GET/STRLEN/INCR 全链界定发散；acl_check_cmd 成帧参数文本同歧，ACL 匹配可能与原型判异。无崩溃无丢写，属形参文本契约分叉在册化。integer 子类型字面量常态域双侧同文（rust 整值直写恰覆盖 123/456 型文本）——源档「|v|≤2^53 双侧同文」仅对 integer 子类型值成立，本条订正；既有测试所钉 123/456 恰系整型字面量故同文。**严禁按 C# "%.14g" 文本形态回改 rust 整值直写臂**（回改即引入 >2^53 数值失真，与第 1 条裁决方向相悖）。锁面：`wedb/wlua/tests/redis_call_fast_path.rs` 新增 float 子类型分叉域五例 fallback 落参断言（`fallback_integral_float_args_write_exact_i64_text`：2^62/2^63/-2^63/-0.0/123.0 钉 rust 期望文本）与 acl_check_cmd 同成帧单点一例（`acl_check_cmd_float_arg_frames_exact_i64_text`），既有 123/456 整型字面量用例零漂移；本票零行为改动。


## 113. 集合算术族 C#「首键缺席/交空早退免检后续键」短路三形分叉（rust 全集判型在先、独占 WRONGTYPE、STORE 错误臂目标键零触达；C# STORE 形 EXPIRE 误删目标键严禁回改）

工单 doc-deviations-set-arith-absence-shortcut-divergence 登记（源档 task/issue/zcode-r117-setfam1.md 立案一，r118-triage-set1 席现树双锚逐点复验 2026-09-25 坐实，定级 P3 登记级：不改码不改行为，只补台账）。编号顺编注记：本票首次 merge dev 后册尾实况 §108+1 拟取 §109；完工前复跑 `git merge dev` 时 dev 已入 SETUSER 残留条与 BITFIELD 回显条先占 §109/§110——按先入库者得号让位取 §111；完工合入前 dev 实况 §111/§112 又由 MEMORY 族目与 lua 形参条先入库得号——再让终取 §113，撞号不覆写（本条内回指锚与 resp_set.rs 夹具注已同步为 §113）。

C# 一手形态：私有 `SetIntersect`（`garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:442-504`）带三条类型免检短路臂——首键 GET 即 NOTFOUND 直返 OK 空集（:452-454，后续键一概不 GET）；循环臂每次 GET 前先判 `output.Count == 0` 即交空早退（:476-481，中途交空后剩余键免检）；后续键 GET NOTFOUND 即 `output.Clear()` 早退（:496-500，其后再有 string 键亦免检）。私有 `SetDiff`（:879-929）首键 NOTFOUND 同形早退（:889-891，无中途空臂）。私有 `SetUnion`（:612-637）逐键判型无短路臂（对照组，与 rust 同构）。传导至 RESP 层三形：a) SINTER/SDIFF/SINTERCARD「首键缺失＋任一后续键为 string」——C# 回空集应答（SINTER 臂 `SetCommands.cs:84-101` `WriteSetLength(0)`；SDIFF 臂 :722-725 `WriteEmptySet`；SINTERCARD 经私有交体 `SetOps.cs:964-967` 回 `:0`）而非 `-WRONGTYPE Operation against a key holding the wrong kind of value.`；b) SINTER/SINTERCARD/SINTERSTORE「中途交集空＋后续 string 键」同 a 形免检；c) SINTERSTORE/SDIFFSTORE 上述形——C# 状态 OK、members 空，对目标键执行 `EXPIRE(dst, TimeSpan.Zero)`（`SetOps.cs:426/:864`），dst 既存值（含 string 键与其 TTL）被真实删除并回 `:0`（`SetUnionStore` :597 同臂而并族不可能出空，免检族外）。族域裁为 SINTER/SDIFF/SINTERCARD/SINTERSTORE/SDIFFSTORE 五命令；SDIFFSTORE 无「中途交空」形、危险形仅首键缺席一类；SUNION/SUNIONSTORE 免检（前席亲验 SetUnion 逐键判型与 rust 同构）。真 Redis 对全键先行判型，C# 短路系原型自身缺陷。

Rust 侧裁决与现状：同步臂 `load_many`（`wedb/wnode/src/resp/objects/set_commands/write.rs:340-355`，锚订正注：源档作 :329-344，已随 wnode-set-store-selfref-load-outside-window「先窗后装」改版漂移）无条件逐键装载，任一 `SetLoad::WrongType` 经 `set_load_sync`（`set_commands/mod.rs:81`）走全模块单点 `write_wrong_type`（`object_store_utils.rs:775-779`）写独占错误帧并以 `Err` 中止——SINTER/SINTERCARD/SDIFF 入口装载调用 `read.rs:220/:245/:292`、三 STORE 入口 `write.rs:278/:302/:326`（源档 :271/:293/:315 同漂，此订）皆 `Ok(true)` 即止不再落笔，STORE 臂目标键零触达、既存值与 TTL 原样保留；慢臂同形（`slow.rs:75-89` `load_many_async`、:288-329 五命令算术臂复用同漏斗），同步/冷双臂应答逐字节全等——本「全等」限**独占错误帧与 STORE 零副作用面**（三形用例应答即错误帧）；成功路径成员序双侧随装载布局、非契约（见 §136）。三形应答与数据副作用均与 C# 分叉。

裁决与保留理由：方向 = rust 更优侧（真 Redis sinterGenericCommand 全键先行判型；C# 系原型短路缺陷族，同第 16 条 PFCOUNT 尾键 NOTFOUND 恒 0 面先例与「上游缺陷修复型」家族）。rust 无运行时危害面，危害落治理面：不登记则双侧对拍夹具遇三形必发散，后审席可能误判转写缺陷「按 C# 修回」——修回即复活尾键类型错免检放行与 STORE 形误删 dst（源键类型错却毁目标键含其 TTL）双危害面，后者系真实数据删除面，故本条立目防回改。裁决维持 rust「全集判型在先、独占 WRONGTYPE、STORE 错误臂目标键零触达」。

后果与严禁回改：后续 SET 族对拍/审查席遇三形用例（「首键缺失/中途交空＋后续 string」rust `-WRONGTYPE` vs C# 空集/`:0`，及 S*STORE 同形 rust dst 保留值与 TTL vs C# dst 已删）直引本条判有意偏差并跳过，勿重复提报；**严禁按 C# 形态回改**（回改即复活免检放行与数据删除面，改回才是真回归）。锁面：`wedb/wnode/tests/resp_set.rs` §113 夹具族——`set_arith_first_absent_key_with_string_tail_wrongtype`（形 a 三命令，错误帧逐字节且独占无渗漏）、`set_arith_mid_empty_intersection_with_string_tail_wrongtype`（形 b）、`set_store_arith_shortcut_error_arm_keeps_dst_value_and_ttl`（形 c SINTERSTORE 两形与 SDIFFSTORE 首键缺席形，断言 dst 预置值与 PTTL 原样保留）、`set_arith_shortcut_slow_arm_matches_sync_arm`（慢臂同夹具冷键降级至 `slow.rs` 臂，同步/冷双臂应答逐字节全等、STORE 冷臂 dst 零触达）；C# 对照形态（三形空集/`:0` 应答与 dst 被删事实）以注释锚记入夹具族头备查。本票零行为改动、零业务码改动（`load_many` 与命令臂禁任何变更）。

尾注（票 zcode-r157c-sintercard 登记向量短路免检形补锚）：本条「先判型后短路」在册裁决射程延伸覆盖登记向量键形——`SINTERCARD 2 miss vk`（首键缺失＋后续键位命中登记表向量）rust 派发层 numkeys 形门（`wresp::command::vector_gate_numkeys_form`，键段 `args[1..=n]` 逐位判型，判据恒 `read_stored_index` 单源）命中即独占泛型 `-WRONGTYPE`，C# 原型首键 NOTFOUND 短路免 GET 回 `:0`——该形分叉直引本条判有意偏差并跳过，勿重复疑报；回归锁 `wedb/wnode/tests/resp_vector_set_wrong_type.rs::sintercard_numkeys_vector_wrongtype` 格 C。

## 114. RESTORE 载荷畸形降级错误帧与长度前缀 32 位档跳信号字节读（修复型分叉两宗并条：不复刻 C# 崩溃/放行 + 修复 C# 写读不往返自洽；纯登记零行为改动）

工单 doc-deviations-restore-payload-degrade-length32-registry 登记（来源 task/reject/zcode-r120b-smallfaces1.md 立案二，r121-triage-smallfaces1 独立甄别通过、主代理第二轮现码复验 2026-09-25 订正锚行号漂移，定级 P3 登记级：不改码不改行为，只补台账 + 代码注释互引锚挂钩）。编号顺编注记：本票 worktree 首次 merge dev 后实况册尾 §108+1 拟取 §109（票内「现册至 §97」系旧册势）；merge dev 复跑五轮撞号——SETUSER 失败残留条、BITFIELD 回显条、尺寸几何族收形条、redis.call 形参文本条、集合算术短路三形条先入库分得 §109–§113，按「先入库者得号让位不覆写」顺编改取 §114（本条内回指锚与代码注释回指均已同步为 §114）。两宗依 §32b 单源纪律并一条登记（同落 DUMP/RESTORE 载荷编解码单链路，长度前缀即载荷解析第一段）。既有相邻条：§21 crc 起算点、§32 ttl 整数文法各已在册，本条不复述其裁决。

C# 一手形态：宗 a——`garnet/libs/server/Resp/KeyAdminCommands.cs:NetworkRESTORE` 类型门无长度守卫，`:45` 直读 `valueSpan[0]`，空载荷（0 字节 bulk）即越界、进程内未处理异常掐连接（`:53` 的 `Length < 10` 长度门排在其后救不了此形）；`:95` `value.ReadOnlySpan.Slice(payloadStart + 1, length)` 以**含 footer 全载荷**为域，声明长度吞入 footer 区（`payloadStart + 1 + length` 落于 `(len-10, len]`）时 Slice 合法——RESTORE 把 rdb 版本/crc64 字节当值收下写键（放行腐蚀面），声明长度越全载荷界则 `:95` ArgumentOutOfRangeException 掐连接；恰 10 字节形（footer 即全载荷）经 `:87` `TryReadLength` 放行空载荷落空值键 `+OK`。宗 b——`garnet/libs/common/RespLengthEncodingUtils.cs:TryReadLength` case 2（`:46-48`）对含信号字节的 `input` 整体 `TryReadInt32BigEndian`，读入位置 `0..4`，信号字节 `2<<6` 混入长度值最高字节；写侧 `TryWriteLength`（`:105-106`）信号字节落 `output[0]`、值落 `1..5`。C# 自家写读不往返自洽：长度 ≥ 16384 字节的载荷自家 DUMP 输出经自家 RESTORE 必错解长度（读得 `(2<<6)<<24 | len>>8` 量级错值，bytesRead 恒 5 再错位切值）。

Rust 侧裁决与现状：宗 a——`wedb/wnode/src/resp/key_admin_commands/types.rs:parse_restore_args`（:216-269，快臂 network_restore 与慢臂 C::Restore 共用推导单源）对畸形三态与放行态一律降级为存活错误帧应答、不复刻崩溃不放行：空载荷取不到类型字节落 `:227-230`「仅支持字符串类型」错误帧（替代越界崩溃）；值切片域收口为 `value.get(1..len-10)`（:253）**排除 footer**，恰 10 字节形 None 落 `:254-255` 同族版本/校验和错误帧（此臂系姊妹票 wnode-restore-10byte-payload-slice-panic 修复落地，注 :248-252 钉「C# 空载荷放行 +OK 残余分叉严禁复刻」），C# 空载荷放行面随之封死；声明长度吞 footer 区或越全载荷界一律 `:261-267` 界内截取失败落 `ERR DUMP payload version or checksum are wrong`——footer 字节被当值收下之放行面在 rust 恒零（payload_body 已去 footer，凡吞 footer 长度必越界）。降级自陈注 :211-215。宗 b——`wedb/wresp/src/length.rs:try_read_length` case-2 臂（:30-34）改跳信号字节读 `input[1..5]` 4 字节大端，与写侧 `try_write_length` 32 位档（:65-71，`output[0]=2<<6` 信号字节、值落 `1..5`）写读往返自洽，输入不足 5 字节即 `None` 而非越界；上限口径两侧一致（`MAX_LENGTH=0xFFFFFF` :11 对标 C# `MaxLength` RespLengthEncodingUtils.cs:17）。自陈注 :17-21。两处自陈注本票各补「登记见 doc/zh/deviations.md §114」互引锚一枚挂钩单一真源，不改任何逻辑。

裁决与保留理由：两宗均属「上游缺陷修复型」修复型分叉（体例对齐 §82 修复型分叉与 §108 HLL 修复族先例）。宗 a 的 C# 崩溃两态系生产路径不可复刻形态（未受控 panic 掐连接，触本仓红线），吞 footer/空载荷放行两态系腐蚀数据之缺陷放行，rust 错误降级侧即更严侧且贴 Redis 法定行为；宗 b 的 C# 写读不对称系自家往返破裂铁缺陷，rust 跳信号字节读方恢复 ≥ 16384 字节载荷 DUMP→RESTORE 往返正确性。维持现实现，**严禁按 C# 形态回改**：宗 a 禁将错误降级改回放行（吞 footer/越界两态存在「改回放行」误改空间，恰 10 字节形禁复刻空值键 +OK）、崩溃态本就不可复刻；宗 b 禁改回 case-2 含信号字节读（改回即重引入往返破裂，才是真回归）。crafted 畸形载荷与 ≥ 16384 字节载荷对拍用例两侧必然发散（C# 崩溃/放行/错解长度 vs rust 错误帧/真长度），后续对拍轮直引本条判有意偏差、勿判转写缺陷、勿反复立案。

验证面：纯登记零行为改动，代码面仅三处注释锚订正（length.rs 自陈注、types.rs 头注与恰 10 字节注、姊妹票测试文件头注各挂 §114 互引）。锁面全部为既有用例、本票零新增：宗 a——`wedb/wnode/tests/restore_10byte_payload_slice.rs`（姊妹票落地锁：恰 10 字节/len=11 错误帧形态、连接存活、快慢臂逐字节一致，:157-255 区；验证点 c :191-222 兼 §21 DUMP→RESTORE 往返语义锁）、`wedb/wnode/tests/hyperloglog.rs:hyperloglog_restore_corrupted_dump_payload_is_rejected`（:1119 畸形载荷拒收）；宗 b——`wedb/wresp/src/length.rs` 内联单测 `roundtrip_all_buckets`（:79-87，含 16 384/0x40_00 与 MAX_LENGTH 桶界往返，即跳信号字节写读自洽锁）与 `decode_rejects_truncated_and_bad_prefix`（:101-111 截断即失败非越界）。

## 115. WATCH 版本轨种子取会话逻辑域、锁轨取物理域双轨分置落点与向量登记表 bump 换算单点（swapnum 换代冻结修复；C# 库身份由版本表实例承载形态的 rust 投影）

工单 wtxn-watch-version-slot-freeze-after-swapnum 落地登记（P2 甄别/审核双通过口径的执行面落点补强，P3 登记级：分叉形态在册，后续对拍轮直引勿重复疑报）。编号顺编注记：本票 worktree 首次 merge dev 后按当时册尾实况 §105+1 拟取 §106（原拟 §103 恰为 dev INFO STATISTICS 别名条先占，撞号让位不覆写）；复跑 `git merge dev` 时 dev 已入 GETKEYS 夹取条先占 §106，册尾已至 connlimit §107、PFCOUNT §108、SETUSER §109——按先入库者得号让位顺编取 §110；合入前 dev 又入 BITFIELD §110、MEMORY 族目 §111、lua 形参 §112、集合算术 §113、RESTORE §114 五条——四让终取 §115，撞号不覆写（本条内编号与回指锚同步为 §115）。

C# 一手形态：每库独持 `WatchVersionMap` 实例终身持有（`garnet/libs/server/GarnetDatabase.cs:55/:156`），库身份由 map 实例承载；`AddWatch`/`ValidateWatchVersion` 以裸键哈希（`TxnWatchedKeysContainer.cs:60-95`）；`FlushDatabase` 零触版本表（`DatabaseManagerBase.cs:301-310`）；锁表同库独持（`MultiDatabaseManager` 各库独立 `OverflowBucketLockTable`），`SaveKeysToLock→GetKeyHash` 运行期取值。

Rust 侧裁决与现状：节点级共享 2^16 单表形态下库身份由哈希种子承载（`wtxn/src/watch_version_map.rs`）。本票修复裁**双轨分置两单点、禁共口互染**：版本轨种子=会话**逻辑域** `(ns, db)` 真值投影（`wkv StoreSession::session_logical_prefix`，FLUSHDB/FLUSHNS/SWAPDB 换号不漂移，复现 C#「改后写必命中同槽必 abort」）；锁轨种子=会话**物理域**（`session_prefix`，含换号代际，与桶闩现域同源），EXEC WATCH 键并锁改按当前物理前缀对裸键现算（`TxnWatchedKeysContainer::save_lock_hashes`，对位 C# SaveKeysToLock 运行期取值）。直设物理域写臂（AOF 回放 `KeyContextGuard`、内置 GC 死域清扫、周期收集、分层降阶）经 `set_virtual_context` 扩形**显式携带**逻辑域入账，禁 bump 链路内映射反查；其入账域由 `VirtualDbManager::version_domain_of` 单点一次性换算——活域正查即映射真值，换号退役死域无活逻辑入口，回物理对本值作**孤域代位**（bump 落活域 WATCH 不消费的孤域槽，与「flush 本身零触版本表」判净口径正合，至多数值重合碰撞、只多 abort 不少 abort，属安全侧）。向量登记表以物理前缀寻址（`registry_key` 复合键随换号域迁移属既定架构），其写面 bump 不经 wkv 用户键写入口，故生产装配改注 `vector_version_watch_hook`（`wnode/src/storage/session/storage_session.rs`）在构槽单点前将入参物理域经 `version_domain_of` 换算为逻辑域再入版本轨——与主存储面落同槽，换号后向量改写对在途 WATCH 同必 abort；该换算为 bump 消费端单点形态（非会话逻辑槽逐次反查，`vdb → 逻辑域` 指向随代际一次性登记、绝不原地改写，无票面所禁「反查与并发换号竞态重引固着漂移」面）。

后果与严禁回改：三侧「同源于此」措辞（登记/校验/推进）已按「版本轨=逻辑域、锁轨=物理域」双轨声明订正，禁半句残留；后续轮遇「换号窗 WATCH 假通过」「EXEC 锁旧代空桶」「向量/回放写 bump 槽位漂移」类疑报直引本条。锁面：`wnode/tests/watch_version_regression.rs` flush×watch 族（含 swapnum 后 DEL 型 `flushdb_then_delete_aborts_inflight_watch`、TTL 型 `flushdb_then_ttl_write_aborts_inflight_watch`、直设臂透传 `direct_set_domain_write_bumps_logical_slot`、锁轨现算 `swapnum_exec_locks_current_physical_domain`）、`tiered_watch_fence.rs::vector_write_after_swapnum_aborts_inflight_watch` 与 `tiered_write_after_swapnum_aborts_inflight_watch`、`aof_flush_replay.rs::test_replica_replay_swapnum_aborts_inflight_watch` 与跨域正交/恰一次推进/换引擎重挂既有用例全绿——验证点 2 改判臂六型（自改/TTL/DEL/回放写/分层写/向量登记）换号后锁测齐备。与邻缝在途票 task/ing/wtxn-wkv-keybucket-hash-scope-desync.md（锁轨会合域面）互引锚钉于 `save_lock_hashes` 文档注释「同根待裁面登记」段，两票危害面正交不并案、前缀单点禁共口互染。

## 116. 故障转移重连 AOF 回放 repl_offset2 钳位——C# sameHistory2 字面恒假死码，rust 采设计意图形（修复型偏离）

工单 doc-deviations-repl-same-history2-fix-fork 登记（源票 task/issue/zcode-r114-replbk1.md 立案一，r115-triage-replbk1 席现树双锚亲验通过、主代理第二轮现码复验 2026-09-25 坐实，定级 P3 登记级：不改码不改行为，只补台账加锁测）。编号顺编注记：本票 merge dev 后册尾实况 §113+1 拟取 §114；完工前复跑 merge dev 两轮撞号——RESTORE 载荷条、wtxn swapnum 双轨条先入库分得 §114/§115——按先入库者得号让位终取 §116，撞号不覆写（本条内回指锚与 replication_manager.rs:790 补注、锁测名回指均已同步为 §116）；failover-entry 席姊妹票仍并行争候，主代理合并时统一对账。

C# 一手形态：磁盘链协商一手——`SendCheckpointAsync`（`garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:162`，锚订正注：ReplicaSyncSession 系重名两档，真实位在 DiskbasedReplication 下）算 sameHistory2 并以 :166-174 实参位（:170 `ReplicationOffset2`）传入 `ComputeAofSyncReplayAddress`（`garnet/libs/server/AOF/GarnetAppendOnlyFile.cs:160-174` 外层逐子日志、:176-233 内层）；sameHistory2 唯一消费者为 :219-220 钳位臂（replayUntilAddress 越 replicationOffset2 即钳回，:218 注释自陈意图 "Bound replayUntilAddress to ReplicationOffset2 to avoid replaying divergent history"）。该 :162 字面 `IsNullOrEmpty(PrimaryReplId2) && PrimaryReplId2.Equals(replicaAssignedPrimaryId)` 恒假：两子式互斥——为真则被 `.Equals` 一侧必为空形。初值 primary_replid2 为空串（`ReplicationHistoryManager.cs:31-32`），空串形须两侧皆空方真，而副本上报值为自身 `PrimaryReplId` 即 `Generator.CreateHexId()` 恒非空 32 字符 hex（`ReplicaDiskbasedSync.cs:181-186` 经 `GarnetClientSessionReplicationExtensions.cs:38` 上线、主端 `RespClusterReplicationCommands.cs:270-271` 定参收取）；null 形则 `.Equals` 直抛。故 :219-220 钳位臂在 C# 生产永不生效，系上游死码缺陷：曾挂旧主、failover 后重挂新主的副本，C# 端把已确认的分叉历史区间照原样回放到 min(rep_tail, committed)。

Rust 侧裁决与现状：`negotiate_resync`（`wedb/wedb/src/server/replication/replication_manager.rs:793-794`，锚订正注：源锚 :791-792 因本票 :791-792 在码补注两行下漂，此订）谓词写成设计意图形 `!primary_repl_id2().is_empty() && primary_repl_id2() == replica_meta.current_primary_repl_id`；消费臂 :866-871 在位有效——failover 后 repl_id2 非空（`replication_history.rs:87-93` `failover_update` 同批转存旧主 ID 并冻结 repl_offset2，:817 读取），副本上报命中旧主时 replay_until 钳至 repl_offset2；未转移态 replication_offset2 为 `i64::MAX` 哨不钳（`replication_history.rs:30`），逐子日志 get 缺项亦回 `i64::MAX` 不钳。同一 failover 重连用例 rust 端 remote-AOF 回放区间上界为 repl_offset2、C# 端为 min(rep_tail, committed)，双侧副本终态可观测发散。在码注释 :790 段已随本票补注恒假链与回指。形态差自登注释另见 :733-754/:766-768（negotiate_resync 与 C# 拆并形态差，非本条范围）。

裁决与保留理由：rust 为正确侧、改良向修复型偏离——钳位语义对齐 C# :218 注释自陈意图与 Redis PSYNC replid2/replication_offset 截断分叉史标准口径；C# 不钳即放行 divergent-history 重放，正是 :218 注释自己要避免的面，属上游缺陷非契约。修复型不改变分叉待登记事实。与 §19/§20 修复型偏离家族同型，该家族先例皆有台账条目在位、本案此前零登记。危害落治理面：无运行期事故面与客户端可观测应答形破坏（故不入 P2）；不登记则后续复制对拍席或 fixloop 在 failover 重连用例现形「C# 多回放、rust 少回放」时按判净口径必误判转写缺陷，维持现状则每轮重复疑报（故不入 P4）。

后果与严禁回改：**严禁按 C# :162 恒假字面回改**——回改即复活上游死码、放行 divergent-history 重放，系真数据危害级回归，改回才是真回归；对拍轮遇「failover 重连后 rust 回放上界 repl_offset2 vs C# 上界 min(rep_tail, committed)」用例直引本条判有意偏差并跳过，勿重复疑报。锁面：`wedb/wedb/tests/replication_pipeline.rs:test_disk_resync_same_history2_replay_until_clamp_three_states` 三态语义锁（未 failover 空串＋i64::MAX 哨不钳／failover 且副本 id 命中 repl_id2 钳至 repl_offset2／failover 但副本 id 不符不钳，经 pub 出口 `disk_resync_strategy` 断言 sync_start_address 形，填此前钳位臂零测试锁空档）；既有字段转移与跨重启恢复锁（`replication_pipeline.rs:155-161`、`replication_manager.rs:197/:199、:268-279` 区）维持绿。本票零行为改动、零协商码改动（除 :790 在码补注一），不触线格式。

## 117. failover 命令入口四处收紧/文案/值域型分叉（从端 TO/TIMEOUT 词拒、TO -1 端口必验、发起失败文案 addr:port、TIMEOUT strict_i64 值域）

工单 wedb-failover-entry-four-forks-unregistered 登记（源档 task/issue/zcode-r119-failover1.md 立案三；r120-triage-fail1 甄别通过、主代理第二轮现码复验 2026-09-25 坐实，定级 P4 登记级：不改码不改行为，只补台账 + 语义锁测试）。编号顺编注记：本票按 merge dev 后现树册尾实况顺编——首次 merge 时 dev 册尾 §113（集合算术短路族）拟取 §114；完工前复跑 `git merge dev` 撞号三轮——RESTORE 载荷条、WATCH 双轨条、姊妹票 repl same_history2 条（§116，failover **重连回放钳位**域，与本条命令入口四宗界互斥不联动）先入库分得 §114–§116，按「先入库者得号、撞号让位不覆写」本条终取 §117（本条内回指锚与代码注释回指均已同步为 §117）。行号注记：票面 §85–§97 册势与旧行号锚（failover.rs:278-301/:233/:240 等）已随 failover 域近三波合案（race_abort 公告臂、is-local PeerSource、cluster_failover 测试面）系统性漂行，本条全部锚点按 merge 后现树重测（TO 三闸 :304-327、strict_i32 :253、顶层 strict_i64 :262，含本票回指注释锚落位后终测），已合语义零回改；顶层 TAKEOVER 拒绝面已在 §13h 在册、strict 文法单源已在 §32 在册，本条互引不重登。

立案背景：四处分叉同聚于集群命令入口文件 `wedb/wedb/src/server/cluster_session/failover.rs`（①③④从端入口 `network_cluster_failover`、②④顶层入口 `network_failover`），方向均为 rust 入口收紧/文案可读形/值域宽，零运行时行为危害，但逐宗有按 C# 面「对齐」回改陷阱（见各小节严禁回改声明），此前四宗零登记（deviations 全文 failover 命中仅 §13h 顶层 TAKEOVER 一臂与 §116 重连回放域，均非命令入口形）。收口形制：循 §13/§111 族目分小节一条收口，不采备选并案形（④ 补 §32 b) 清单第 22 项、①②③ 另立新条）——四点同入口同文件拆读更易错；且 ④ 与 §32 所裁前导零文法不同轴（§32 为文法轴 rust 拒更严，本点为值域轴 rust 收更宽），§32 现文不经由推出对本点位覆盖，④ 入本条不入 §32 清单。

### a) CLUSTER FAILOVER 从端选项词表白名单（C# 七枚举全转+无选项门 vs rust 三词白名单）

C# 一手形态：`TryGetFailoverOption`（`garnet/libs/cluster/SessionParseStateExtensions.cs:52-74`）认 DEFAULT/INVALID/TO/FORCE/ABORT/TIMEOUT/TAKEOVER 全七枚举词表；从端入口（`garnet/libs/cluster/Session/RespClusterFailoverCommands.cs:33-34`）仅拦解析失败与 DEFAULT/INVALID 两值，TO/FORCE/ABORT/TIMEOUT/TAKEOVER 一律放行，`TryStartReplicaFailover`（`garnet/libs/cluster/Server/Failover/FailoverManager.cs:80-88`）无选项白名单门——副本上 `CLUSTER FAILOVER TO`、`CLUSTER FAILOVER TIMEOUT <秒>`（含带秒数第二参）静默按 DEFAULT 语义发起**真实故障转移**（TO/TIMEOUT 语义在从端无从消费，系 C# 顶层词法器误用于从端命令的入口缺陷）。
Rust 裁决锚：`network_cluster_failover`（`failover.rs:42-55`）白名单三词 ABORT(:43)/FORCE(:45)/TAKEOVER(:47)，其余落拒臂回 `ERR Failover option ({词}) not supported`（:50-54，回显 `from_utf8_lossy`；码内注释锚随本票加）。与 §13h 分臂互引：彼裁顶层入口 TAKEOVER 拒绝（`network_failover`），本臂裁从端词表，两入口两形。
严禁回改：不得按 C# 七词全转放宽从端白名单（回改即复活 TO/TIMEOUT 在副本侧的静默放行发起真实故障转移面）。锁测 `cluster_failover_replica_entry_option_whitelist_rejects_to_timeout`。

### b) FAILOVER TO 端口 -1 必过三闸（C# -1 跳验放行+探测集错向 vs rust 无条件三闸）

C# 一手形态：顶层校验门条件 `replicaPort != -1 && replicaAddress != string.Empty`（`garnet/libs/cluster/Session/FailoverCommand.cs:76`）——端口 -1 即整体跳过三闸（端点已知/副本角色/隶属本节点），带未知端点原样入 `TryStartPrimaryFailover`（`FailoverManager.cs:111-131` 无二次校验），会话构造落 `FailoverSession.cs:66-68` 的 `hostPort == -1` 臂取 `GetLocalNodePrimaryEndpoints`（`ClusterConfig.cs:297-310` 全集群主端点集）——**TO 指定的地址被丢弃、探测客户端集错向异主节点**。
Rust 裁决锚：`network_failover`（`failover.rs:304-327`）只要给了地址即无条件三闸——`get_worker_node_id_from_address` 按地址+端口精查（:306），-1 端口查无此端回 `ERR Unknown endpoint`（:308/:312）；角色闸前置另锁于既有 `cluster_failover_abort_on_ordinary_replica_changes_nothing`（CAN-FAILOVER-FROM-NON-MASTER 面）。rust 会话侧同形态三态选集（`primary_failover_session.rs:51-57`，`host_port == -1` 臂 collect 全集群主端点）自命令面**不可达**，仅存为对 C# FailoverSession.cs:66-68 构造形态的纯防御镜像（码内注释锚随本票加）。
严禁回改：不得复刻 `-1` 跳验放行门形（回改即复活跳闸+错向探测集双缺陷）；会话侧 -1 防御臂不得改判为命令面可达路径的语义承接。锁测 `failover_to_minus_one_port_unknown_endpoint`。

### c) 发起失败应答文案 primary(addr:port)（C# ValueTuple 插值双层括号 vs rust 冒号单层）

C# 一手形态：从端发起失败应答（`garnet/libs/cluster/Session/RespClusterFailoverCommands.cs:71`）`$"ERR failed to start failover for primary({current.GetLocalNodePrimaryAddress()})"` 把 `GetLocalNodePrimaryAddress` 的返回值整体作 ValueTuple `(string address, int port)`（`ClusterConfig.cs:268`）插值，ToString 渲染 `primary((addr, port))`——**双层括号+逗号空格**畸形文案。
Rust 裁决锚：`failover.rs:123-128` 解构二元组后渲染 `primary(addr:port)` 单层括号冒号形，为全仓该文案单源；主端点缺席形（`get_local_node_primary_address` 回 `(None, -1)`，`cluster_config/mod.rs:321-326`）渲染 `primary(:-1)`，同一格式。
严禁回改：不做逐字嵌套括号对齐（复刻畸形文案零收益）；双侧对拍遇该文案字节差直引本条判有意偏差。锁测 `cluster_failover_start_failure_primary_addrport_wording` 逐字节钉现形并锁 `primary(` 前缀单源。

### d) TIMEOUT 值域 strict_i64 档（C# TryGetInt int32 值域 vs rust 收界外大值进会话预算）

C# 一手形态：顶层 TO 端口与 TIMEOUT 毫秒（`FailoverCommand.cs:42`/:50）与从端 TIMEOUT 秒（`RespClusterFailoverCommands.cs:47`）均走 `TryGetInt`——int32 值域，界外（如 4000000000）回 not-integer。
Rust 裁决锚：端口档 `strict_i32`（`failover.rs:253`）与 C# 同档（越界拒，既有锁测 `failover_to_port_out_of_i32_range_rejected` 在位）；超时档统一 `strict_i64`——从端秒（`failover.rs:60`）与顶层毫秒（`failover.rs:262`）收下 i32 界外大值，非零不进 600 秒归一、原样入会话预算（归一臂消费面 `failover_session.rs:95-99`，saturating deadline 兜底已由 failover_timeout_bounds 的 i64::MAX 用例锁面）。文法向（前导零拒更严）沿 §32 单源口径；本点登记的系**值域向更宽**（rust 收 i64 全档），与 §32 所裁不同轴，不并入 §32 b) 清单（单条收口形制见本条立案背景），回指锚 `failover.rs:60/:262`。
严禁回改：不得按 C# int32 档回缩超时参数值域（回缩即破坏全仓 strict 档统一与该值域向锁面；`007` 类前导零文法仍沿 §32 拒收不受本条影响）。锁测 `failover_timeout_beyond_i32_range_accepted`（命令面双臂收形）+ `i32_overflow_failover_timeout_flows_into_budget_unnormalized`（会话预算归一臂消费面）。

后果与严禁回改（族目总）：四处均为命令入口形分叉，无运行时行为危害；① 回改复活从端静默发起真实故障转移、② 回改复活跳验放行+错向探测集、④ 回改复活 i32 回缩破 strict 单源、③ 逐字对齐复刻双层括号畸形文案——四陷阱逐宗独立致命，后续 failover 入口对拍/审查席遇四形用例直引本条判有意偏差并跳过，勿重复提报。本票零行为改动、零业务码改动（命令臂禁任何变更，仅 failover.rs 白名单/TO 三闸/文案/strict 四处与 primary_failover_session.rs -1 防御臂加回指注释锚）；语义锁测四例落位 `wedb/wedb/tests/cluster_failover.rs`（①②③）与 `wedb/wedb/tests/failover_timeout_bounds.rs`（④ 命令面），归一臂消费断言落 `failover_session.rs` 既有 tests 模块（④ 消费面）。

## 118. 迁移/复制驱动链跨代 TTL 幽灵：§96 扩形宗三（迁移链换代两读终态形，单探针窗修复入库；快照链 SWAPDB TTL 错配形由案一 §99 域钉消除）

工单 task/ing/wedb-migrate-cross-generation-ttl-ghost.md 登记（甄别 r122-snapswap1 立案二 + 复审 r123-triage-snap1 定级 P3；**修复型入库**非纯登记）。编号顺编注记：本票首跑 merge dev 实况册尾 §114，拟取 §115；完工 merge dev 复跑撞号——swapnum 双轨条、repl_offset2 钳位条、failover 命令入口条先入库分得 §115/§116/§117——按「先入库者得号、后到让位顺编不覆写」纪律终取 §118，撞号不覆写（本条内编号、§96 扩形后注与代码注释回指锚已同步为 §118；锚一律按内容引不钉行号，§96 裁决语纪律）。

与 §96 关系（扩形登记，宗三）：本条承 §96「可选加固路——值读与 TTL 读并入同一无锁探针窗」在迁移链的落地形，三别项俱出 §96 宗二覆盖面——触发臂=换代（域切换，bump_generation）非时钟越线；受害链=迁移/复制驱动一次性帧流非快照链（无锚后 AOF 续推通道，§96 宗二 TtlPurge 自愈前提在本链结构不存在）；终态形=永久非瞬态。§96 宗二本身（快照链时钟越线 0-TTL 瞬态幽灵）维持「非必须项、不启动即按该条维持现状」裁决不动，本条不扩及其面。SWAPDB 形在快照链的 TTL 错配面由案一（§99 快照装载/读值域钉）统一消除——域钉后两读恒同物理域；两票共因（换号族零键直插迁移驱动窗绕门链）只在案一/§99 侧登记一手，本条引用即停，共修法族（前缀捕获单点）互引不并案：§99 域钉对象=快照迭代器会话，本条单探针窗对象=逐键探针（迁移驱动与快照迭代器共用 read_live_value），案一修复不覆盖迁移驱动会话另源，本修亦不消除案一域钉必要性。

C# 一手形态：N.A. 定性——`garnet/libs/cluster/Server/Migration/MigrateScanFunctions.cs:Reader` 整记录原子读（expiration 随 RecordDataHeader 同读），结构上无两读窗；C# 无换号教义即无本形对物，立案依 rust 自洽面与 §96 加固路在码先例。

Rust 侧缺陷形与裁决（修复已入库）：换代唯一 mutator（`wkv/src/vdb/manager.rs:bump_generation`）落 `read_live_value` 值读与 TTL 读两独立 await 之间时，TTL 读逐 op 现解析 `virtual_domain` 慢路径改指——**FLUSHDB 形**：TTL 读落新空域恒发 0 帧（`wconn/src/record.rs` 帧形 0=「无 TTL」语义非「缺失」），目标端 `frame_import.rs` 回填 >0 门跳过，落无 TTL 常驻键，迁移链无锚无增量续推通道→永久 0-TTL 幽灵（键空间泄漏+对账现形目标多键）；**SWAPDB 形**：TTL 读落换入域，邻库同名键真 TTL 错配装帧（提前过期/超期驻留双向，值对 TTL 错形不在 §99 快照域钉纠正范围）。主案修=**单探针窗**：`read_live_value` 每键探针头单次捕获 `session_prefix()`（与 `virtual_domain` 三口径同源），string/信封两臂值读改走 `read_tag_with_prefix` 显式前缀、TTL 读改走 `wkv/src/ttl.rs:raw_ttl_with_prefix` 以捕获前缀调用——值与 TTL 恒同代同域，换代于探针窗内无感，下一探针窗界照常重解析。TieredTree/向量臂 TTL 随流元携载不经本窗，零改动面。残余：同探针内同域并发写形=两侧同权窗口，按 §96「非必须项」口径维持不动；迁移驱动会话窗首部域钉（票方案二小闸）本修不采——DELETING 收口写臂现形=换号后重解析落新代空位 no-op，系「Gone 跳发留痕」断言承重件，钉域后恒落旧域须重走 sketch 门连续性论证，风险高于主案，留拍板席另裁。

锁面：`wedb/wedb/tests/migrate_cross_generation_ttl.rs` 三锁——`flushdb_between_reads_zero_ttl_ghost_locked_and_fixed`（FLUSHDB 形：修复前两独立 await「值在旧域而 TTL 归零」拼接形现形坐实、修复后 string/信封两臂帧 TTL=值读同代真值+窗界新探针 Gone 跳发留痕+目标端导入按真 TTL 消亡与源端逻辑终态全等）；`swapdb_between_reads_never_borrows_swapped_in_domain_ttl`（双库同名异 TTL：修复前邻库真 TTL 串值现形坐实、修复后帧 TTL 恒取自值读同域，补源档自陈未做夹具之欠账）；`generation_bumps_within_probe_are_invisible`（前缀捕获单点原子性：探针窗内换代计数不跨探针生效、窗后重解析照常）。零回归面：`diskless_sync_ttl.rs::diskless_sync_preserves_ttl`（§96 宗一既有锁）与迁移既有族（cluster_migration / tiered_sync_migration / migrate_import_batch_equivalence / expire_replica_replay）复跑全绿。测试面 C# 无对形段不复刻（skip 注记见该档头部）。

## 119. CLUSTER 协议面节点 id 对外渲染 32hex 定长分叉（rust create_node_id u128 16B→32hex vs C# Generator.CreateHexId 20B→40hex；纯登记零行为改动）

工单 task/ing/zcode-r123c-clustershrs1.md 登记（甄别 r124丙-tr-docsclu 席裁宗一通过 P4；**登记级 P4 纯登记**，零行为码改动、零测试改动——编码面本体 r123c 席十点逐字节判净在册）。编号顺编注记：票面 §99 注系甄别时点旧册实况，本票落号以执行沙箱现册册尾为准——merge 前册尾 §118，本票取 §119；merge dev 复跑后若 dev 已带同段新条，按「先入库者得号、后到让位顺编不覆写」纪律改号并同步条内与代码注释回指锚（锚一律按内容引不钉行号）。

C# 一手形态：集群节点 id 由 `garnet/libs/common/Generator.cs:19-24` `CreateHexId(size=40)` 生成——`stackalloc byte[20]` 经 `RandomNumberGenerator.Fill`（CSPRNG）填充后 `Convert.ToHexString(...).ToLowerInvariant()` 成 **40 字符小写 hex（20 字节底座）**；渲染帧头随之钉死长度——SHARDS 节点帧 id 值位硬编码 `"$40\r\n"`（`garnet/libs/cluster/Server/ClusterConfig.cs:742`），SLOTS 三形（Ip/Hostname/Unknown 臂）以 `$nodeid.Length` 成帧（`ClusterConfig.cs:816/:831/:841`，40 字符输入即现 `$40`）。

Rust 侧裁决形：节点 id 底座为 **u128（16 字节）**——`create_node_id`（`wedb/wedb/src/server/cluster_manager.rs:52-54`，两枚 fastrand u64 拼接）替代 C# 20B CSPRNG；hex 形态仅在协议渲染点出现且**定长 32 小写 hex**，单源 `wedb/wbase/src/hex.rs`——`hex_encode_u128`（:65-76，`[u8; 32]` 定长大端）与 `hex_str_u128`（:80-85，恒 32 字符零分歧），反解析门 `hex_u128`（:89 起）仅收恰好 32 字符输入（长度不符即 None 拒收）。RESP 成帧侧**无硬编码宽度**：以 `${nodeid.len()}` 动态宽成帧（`wedb/wedb/src/server/cluster_config/serializer.rs:453` SHARDS id 值位、:584 等 SLOTS 三形臂），32 帧宽纯系编码器定长派生。

消费面清单（对外回显/互令六面同源 `wbase/hex.rs`）：一 MYID（`wedb/wedb/src/server/cluster_session/basic.rs` `network_cluster_myid`，32hex bulk）；二 CLUSTER NODES 线形首列（`serializer.rs` `append_node_info` :235）；三 CLUSTER REPLICAS（`cluster_session/replication.rs` 渲染点）；四 SLOTS 节点帧 id 位（`serializer.rs` `append_node_networking_info` :573/:584）；五 SHARDS `id` 字段值位（`serializer.rs` `append_formatted_node_info` :450-453）；六 FLUSHALL_NS origin 参数（内域文法面，`db.md` 既载非 RESP 对外形，`hex_u128` 32 字符门同源）。另 gossip 互令与 SETSLOT/FAILOVER 等命令入参解析门同锁 32 字符。

承重与裁决（严禁回改）：u128 为本仓身份基座，全链承重——gossip 线格式与连接簿按 u128 键（`wedb/wedb/src/server/gossip/node_connection.rs:24` `pub node_id: u128`、`gossip/connection_store.rs` 同键）、worker 配置字段 `nodeid: Option<u128>`/`node_id: u128`（`wedb/wedb/src/server/worker.rs:34/:58`）、配置装配与 ban 名单等并发表均 u128 键（`cluster_manager.rs` `worker_ban_list: ConcurrentMap<u128, _>` 等）。**不得回改 160bit/40hex**（回改即拆 gossip/存储/ACL 全链键型）；亦不得反向把渲染层「修复」为 40 字符帧——32hex 非编码漂移。与 r102 既裁关系：r102 域为身份生成器双机制，已裁 `create_hex_id`（`cluster_manager.rs` 对 `wbase/hex.rs:131` `generate_hex_id` 20B→40hex 忠实单源）保留于复制 replid 面、明示 `create_node_id` u128 不涉——本条登记的正系 r102 让位的节点 id 面协议形状差，两条各管一面互不覆写。

附带订正（随本票入库，非行为码）：`serializer.rs:234` 原注释「对标 C# 40 hex 字符串形态」系误导措辞（易引后席按 40 字符误判漂移或误修复），已订正为实态形——对标 C# 小写 hex 形制、长度 32 系 u128 底座派生、渲染帧以 `${nodeid.len()}` 动态宽成帧（:453/:584 实况已核），并回指本条。

后果与对拍口径：该长度差是客户端可观测协议形差（id bulk `$32` vs `$40`），双侧对拍遇 CLUSTER 族帧内 id 长度字节差直引本条判有意偏差并跳过，勿重复提报。宗二（SLOTS/SHARDS `cluster_manager()` 缺席臂零应答帧异形）经票面复核实测坐实结构不可达（`ClusterProvider::new()` 唯一装配恒 Some、全生产码无 None 写臂）且 C# 无对物现网不可观测，甄别席裁驳回——不补帧不入册，本条不含该面。


## 120. 分层键后台降阶评估轮寄生缺省 0 的 expired-object-collection-freq 旋钮（旋钮兼职降阶轮启动门纯登记；collection.md 3.2/3.3/8.4 执行位口径随批注实态；C# 集合恒驻对象域无降阶对应物、文档所指紧缩/GC 属常开背景面）

工单 task/ing/zcode-r125c-tierhyst1.md 登记（甄别 r126-tr-tierhyst1 席通过；定级 P3 登记级：**零行为码改动**，只补台账 + 文档/注释订正。编号顺编注记：本票 merge dev 后册尾实况 §118 居末，本条顺编取 §120；撞号不覆写、先入库者得号，完工前复跑 merge dev 若撞号按纪律让位并同步本条内回指锚与码内/文档注释回指）。（落册实况：本批四票同窗入库，clustershrs 先落得 §119，本条让位顺编取 §120，条内与码内/文档回指锚已同步。）

登记事实（旋钮隐性兼职第二职责）：自适应分层冷键的后台降阶评估轮唯一执行体 `wnode/src/resp/objects/tiered_demote.rs:tiered_demote_round`，生产码内全仓唯一挂点为 `wnode/src/primary_tasks.rs:object_collect_loop` 每轮尾臂（Hash/ZSet 过期字段收集两臂之后）；而该宿主任务的启动无条件受 `expired-object-collection-freq` 槽位门控——`try_start_object_collect_task` 读槽 freq<=0 直接回 false **任务根本不 spawn（非空转）**，循环体内 `(freq>0).then_some` 归一后 None 即先落 `object_collect_started=false` 再 break 自退出（退出决定与标志翻转同点，防 disable→enable 窗口任务永久丢失），副本挂起臂与引擎弱引用自退出闸门为宿主任务既有件。旋钮缺省 0 双证：`wconf/src/node_options.rs:DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS=0`（自注对标 GarnetServerOptions.cs:216 与 defaults.conf:197 皆 0）与 `wconf/src/runtime_server_config.rs` ConfigMeta 第 22 项 ExpiredObjectCollectionFreq 缺省 0。后果：**缺省配置下，一经升阶且再无前台写触碰的冷分层键永无降阶评估点**——前台懒降阶臂（`resp/objects/rmw_helpers.rs:apply_rmw_post_operate` 降阶臂）只覆盖被写触碰键；命令表零降阶管理入口、`wkv`/`wcompact` 紧缩面零降阶挂钩（全仓 grep 零命中），`tiered_demote.rs` 模块头旧宣称的「与手动驱动双入口共享唯一执行体（对标 collect_expired 内核与 EXPDELSCAN 双入口单内核先例）」在命令面未接线、属失实注释（随本票删句订正）；wnode/tests 分层降阶族（tiered_background_demote / tiered_demote_registry_discovery / tiered_demote_aof_replay / tiered_promote_demote_ttl）全部测试内手动 block_on round，恰绕过本缺口。运维语义：主动置 0 关闭过期字段收集者，将在不知情下同时掐断分层生命周期降阶半边，迟滞死区「自动回归内存态」（collection.md 3.2「可在后台紧缩/异步调度时降阶」、3.3「后台 GC / 日志紧缩阶段……异步降阶回收」、8.4 第 3 条「由后台紧缩自动回归内存态」）整体不可达，缩水的冷分层集合其树文件与页缓存永不回收。

C# 对照面形态：`garnet/libs/server/StoreWrapper.cs:ObjectCollectTaskAsync`（:722-741，循环体先 `ExecuteObjectCollection` 后 `Delay`，`Debug.Assert(objectCollectFrequencySecs > 0)` 门内常驻跑）经 `TryStartObjectCollectTask`（:987-992，`runtimeConfig.GetInt(EXPIRED_OBJECT_COLLECTION_FREQ) > 0` 方 `taskManager.RegisterAndRun`，CONFIG SET 调停 ReconcilePrimaryTask 取消+按新值重启）拉起。C# 该旋钮语义**仅覆盖** Hash/ZSet 对象域过期字段收集（`DatabaseManagerBase.cs:ExecuteObjectCollection` :343 专用收集会话）；C# 集合恒驻对象域（内存态），全仓零降阶机制对物（RespCommand.cs:1252 系 LRU 注释噪声），该旋钮缺省 0 在 C# 侧无任何生命周期承诺可断。rust 侧 collection.md 承诺条文所指的执行位（后台 GC / 日志紧缩）对应 C# 形态属**常开**背景回收面（内存固有 GC 生命周期与日志紧缩不挂该旋钮），而 rust 实装把降阶轮挂上缺省关闭的收集任务，构成执行位与承诺的三重分叉（3.2/3.3/8.4 句式 vs 实装宿主）；自适应分层为仓内纯自研改良，本议题法定契约文件即 doc/zh/collection.md，属自研面承诺一致性登记级而非对 C# 契约偏离。

裁决与保留理由（取裁 B「登记+订正注释」，驳裁 A「摘除宿主门控」）：裁 A（freq<=0 时任务仍常驻、按编译期常量节拍仅推进降阶臂）被驳——违 C# `TryStartObjectCollectTask`（:987-992）freq>0 门控的逐行对标（rust 宿主任务系该件逐行转写，本票纪律内改动即造对 C# 原型的新增偏离），且在缺省配置下制造**不可关闭的常态后台 IO**（每轮全登记表候选预筛+至多 16 次全树物化预算），运维置 0 关闭收集意图被暗中覆写，越出 P3 承载。裁 B 维持旋钮寄生耦合（「不放第二套调度器、不新增配置旋钮」纪律不动），以登记与文档口径订正显式化实态。

本票治理动作（零行为码）：① 本条登记旋钮兼职与缺省不可达事实；② 订正 `tiered_demote.rs` 模块头（:7-8 双入口/EXPDELSCAN 失实句删改）与 `primary_tasks.rs` 任务注记（spawn_object_collect_task 头注「与手动驱动双入口共享同一执行体」删改、try_start 头注与循环内降阶臂注记补兼职回指）；③ collection.md 3.2/3.3/8.4 第 3 条承诺句随批注实态——3.3 执行位条文订正为周期对象收集任务后台臂（非 GC/日志紧缩阶段）并显式加注「冷键降阶依赖 expired-object-collection-freq>0」；④ `wconf` CONFIG 元数据与 node_options 帮助文本同步副作用说明（ConfigMeta 槽注、apply_expired_object_collection_update 头注、node_options 常量与字段 help 注释、runtime_server_options 字段注），全部回指本条。

后果与严禁回改：无运行期事故面、无客户端可观测应答形破坏（故不入 P2）；不登记则后续分层对拍席/降阶联调席遇「缺省配置冷键不降阶」用例必按实装缺陷误判转写（故不入 P4）。**严禁按裁 A 暗改耦合**（回改面=违 C# 对标 + 缺省常态后台 IO 双陷阱）；**严禁**在后续票中未经订正本条地复活「双入口/EXPDELSCAN 先例」注释——若将来真落一条降阶管理命令入口形成双入口，须同步订正本条「命令表零降阶入口」句与两处模块注记。运维口径：要兑现迟滞死区自动回归须显式置 expired-object-collection-freq>0（该事实 collection.md 3.2/3.3/8.4 与 CONFIG/node_options 帮助文本已在册）；对拍轮遇「缺省配置冷分层键永驻树态」用例直引本条判文档已在册态并跳过，勿重复疑报。


## 121. 事务中止/丢弃臂锁集登记跨事务残留系 C# 原型缺陷，rust 无条件单点收口为正确侧（已修未登，登记即闭环）

工单 task/ing/zcode-r126c-txnqueue1.md 登记（甄别 r127丙-tr-reg2 席双侧现码亲验坐实，定级 P4 登记级：不改码不改行为，只补台账加锁测；rust 生产码零改动）。编号顺编注记：本票 merge dev 后现册册尾 §118，本条取 §121；若完工前复跑 merge dev 撞号，按先例纪律「先入库者得号、后到让位顺编不覆写」（§116/§117/§118 同款）调整并同步条内与锁测回指。（落册实况：本批四票同窗入库，clustershrs 先落得 §119，本条让位顺编取 §121——§120 为同批 tierhyst，条内与锁测回指锚已同步。）

C# 原型缺陷形（本条登记之反证，非对齐目标）：`TransactionManager.Reset(bool isRunning)`（`garnet/libs/server/Transaction/TransactionManager.cs:211`）仅 `isRunning=true` 臂调 `keyEntries.UnlockAllKeys()`（:217），而 `UnlockAllKeys`（`libs/server/Transaction/TxnKeyEntry.cs:152`）系 keyCount 归零唯一出口；`NetworkDISCARD`（`libs/server/Transaction/TxnRespCommands.cs:217`）与 `NetworkEXEC` 的 Aborted EXECABORT 臂（:54）均走 `Reset(false)`——排队窗尾部对每条入队命令 `LockKeys`（TxnRespCommands.cs:197）经 `SaveKeyEntryToLock`→`AddKey`（TxnKeyEntry.cs:87）即时登记进 keyEntries 的键条目在 DISCARD/EXECABORT 退出后原样滞留。复现链 `MULTI; SET a 1; DISCARD; MULTI; SET b 2; EXEC`：第二笔 Run 的 LockAllKeys 按 Slice(0,keyCount) 取锁时把残留的 a 一并排他加锁直至提交（过度取锁）；`IsReadOnly`（TxnKeyEntry.cs:67）被残留 Exclusive 条目污染，令集群槽校验 readOnly 误判（`TxnRespCommands.cs:67 GetSlotVerificationInput` 消费）；`ComputeSublogAccessVector`（:582 按 keyEntries 全量路由）令 TxnStart/TxnCommit 条目虚报无操作的子日志与回放任务参与面。契约上 DISCARD/EXECABORT 退出后本笔事务的一切登记不得外溢到同会话下一笔事务，故缺陷本体在 C# 原型侧，系上游缺陷非转写契约。

Rust 现状形（裁决保持的正确侧）：`TransactionManager::reset`（`wedb/wtxn/src/transaction_manager.rs:219-220`）**无条件**调用 `unlock_all_keys`（`wedb/wtxn/src/txn_key_entry.rs:290` 清 keys/held/plan/latch 并逆序放持闩，未持锁幂等空操作），rust 无 isRunning 型参数分臂——全部退出路径单点收口零残留：DISCARD（`wedb/wnode/src/resp/txn_resp_commands.rs::network_discard`）、EXECABORT（`network_exec` Aborted 臂）、ExecRun::Aborted postlock 失败（`finish_run_postlock` 内部 reset）、集群槽校验失败臂、会话析构 `Drop`（transaction_manager.rs:111-114 复用 reset）；在码注释（transaction_manager.rs:214）早已就地标注「杜绝中止/丢弃事务的锁泄漏与跨事务键污染」。四池票面（todo/ing/reject/done 及 wtxn-keybucket-desync、watch-version-slot-freeze、slot-hooks、wlua-multi-exec-replay）与本台账此前均无本分叉登记条目——属「已修未登」，非新发现缺陷。

裁决：承 §43 c)「已修未登，登记即闭环」既定口径（§61/§62 家族同型），本条登记即闭环，裁决维持 rust 现状无条件收口形。危害落治理面：rust 侧无运行期数据面危害（故不入 P2/P3）；不登记则后续审查/对拍轮遇本形用例必误判转写偏差、每轮重复疑报（故不入 P4 之下）。

后果与严禁回改：**严禁按 C# `Reset(false)` 不清 keyEntries 形态「对齐」回退 rust 的无条件单点收口**——回改即重新引入跨事务锁集残留三缺陷（下一笔事务过度取锁、IsReadOnly 槽校验误判、AOF 子日志参与面虚报），改回才是真回归；后续事务排队窗对拍/审查席遇「DISCARD/EXECABORT 后锁集残留」「残留 Exclusive 污染只读判定」用例直引本条判有意偏差并跳过，勿重复提报。本票零行为改动、零 rust 生产码改动、零 C# 侧改动。

锁面：`wedb/wnode/tests/txn_queue_lockset_residual.rs` 两例真实存储会话语义锁（夹具沿 txn_queue_abort_execabort.rs 同形，断言全部取自会话所挂 TransactionManager 真值源 key_entries 观测面与落库数据面，零 mock）——`multi_discard_lockset_zero_residual_next_txn_registers_only_new_key`（排队即登记面坐实（逐哈希比对排他条目）→ DISCARD 后 key_entries 零残留（count/锁集展示串/只读判据三观测）→ 同会话第二笔登记键集仅含 b（count==1 且逐哈希等值）→ EXEC 仅 b 落库、被丢弃 a 从未执行）；`execabort_lockset_zero_residual_readonly_not_poisoned_by_prev_exclusive`（先入队 SET 登记排他条目再入队错误命令置 Aborted，EXEC 走 EXECABORT 臂收口零残留；另笔只读事务 is_read_only 不被前笔 Exclusive 残留污染，钉死 C# GetSlotVerificationInput readOnly 误判面）。既有邻锁维持绿：EXECABORT 收口族（`wnode/tests/txn_queue_abort_execabort.rs`）、reset 清键集线程/异步臂面（`wtxn/tests/exec_run_async_arm.rs`）、排队拒绝跨库面（`wnode/tests/txn_select_negative_db_aborts.rs`）。


## 122. 持久化装载失败恒拒启全形态：吸收 C# FailOnRecoveryError 旗标（默认关续行）为刻意收紧，逃生门不设配置通路（登记收口，零行为改动）

工单 task/ing/zcode-r127c-perstknob1.md 登记（甄别 r128丙-tr-perstknob1 席，裁决票非行为票；采裁决 A 登记收口、驳裁决 B 新增旗标配置通路）。编号顺编注记：票面拟号「§99 起」系立票时旧册实况已过期，本条按落笔时现册尾 §118 顺编取 §122，先入库者得号、撞号不覆写。（落册实况：本批四票同窗入库，clustershrs 先落得 §119，本条让位顺编取 §122——§120/§121 为同批 tierhyst/txnqueue，条内与码内注释回指锚已同步。）

C# 一手形态：恢复期（AOF 设备面 RecoverAsync 与回放 RecoverReplay、检查点恢复）异常受 FailOnRecoveryError 旗标门控，**生效默认关**——`libs/host/Configuration/Options.cs:637-638` 定义 `--fail-on-recovery-error`，`Options.cs:1028` `bool?` 经 `GetValueOrDefault()` 折 false，`libs/host/defaults.conf:497` 恒先导入且值为 false（生效默认与属性初始化器同向，沿 r94 defaults.conf 生效默认口径）。消费链四处：`libs/server/AOF/Recover/AofRecover.cs:69-75`（catch 落 LogError，仅置位才 rethrow，否则返回 `AofAddress.Create` 的 -1 回退位点继续）、`libs/server/Databases/DatabaseManagerBase.cs:252-257`（ReplayDatabaseAOF catch 吞错继续）、`SingleDatabaseManager.cs:87-91` 与 `MultiDatabaseManager.cs:104、:135`（注释明示「Unless FailOnRecoveryError is set the server continues with whatever was recovered」）。即 C# 默认形态 = 断电/坏段下带已恢复部分数据起库，旗标系运维逃生门的反向（置 true 才拒启）。

Rust 裁决形态：该门整体缺席，恢复失败**恒拒启**。`wconf/src/node_options.rs` NodeArgs 无 fail_on_recovery_error 字段、RuntimeServerOptions 无对应位、全仓 FailOnRecoveryError 零代码消费点。实际行为链：`wnode/src/service.rs` open_recovered_with_config_and_aof 恢复装配段 `aof.log().recover_async().await?` 错误直上抛中止启动；`wnode/src/database/database_manager_base.rs` recover_database_aof_async 设备面恢复错 `?` 即终点直达调用方，重放错沿 `wedb/src/server/boot.rs` open_from_args 链拒启。与 §27（检查点 flushed 前缀短读拒启）、§40c（段大小不匹配统一拒启）同族，方向系对 C# 「默认关 catch 吞错续行」的**刻意收紧**，非缺省漏配。

裁决（A 案收口）：「装载失败恒拒启」全形态自此入册收口，逃生门不设配置通道，维持 fail-fast 单一真源。驳 B 案（NodeArgs 增 `--fail-on-recovery-error`、恢复段错误臂落续行门）——违 §27/§40c 已拍板恒拒启方向，且为单一运维场景引入配置双真源属过度设计。后果与严禁回改：单文件段断电撕裂或段目录损坏（§40 族已载三例形态）下，C# 默认带部分数据起库可应急运维、rust 恒拒启且无旗标可退，灾备演练与故障恢复预期双侧分叉属有意偏差——后续轮次与运维按 C# 心智找该旗标必然落空，遇恢复期错误拒启用例直引本条判有意偏差并跳过，勿重复提报、勿按 C# 默认续行向回改或补旗标。

注释订正（本票实动面）：全仓 FailOnRecoveryError 冒称注释实测 6 处（原票面 2 处系散计，甄别席订正）同批改述，统一口径「rust 恒拒启，C# 默认关续行为刻意收紧」，删「装配层有裁决门／回放驱动承担续行门」幻影指涉与「默认开」反向误载——`wnode/src/service.rs` 恢复装配段注释（原冒称「对标 FailOnRecoveryError 默认开」）、`wnode/src/database/database_manager_base.rs` recover_database_aof_async 注释块（原冒称「穿透续行门控交由 FailOnRecoveryError 在装配层裁决」，装配层全仓无此裁决点）、`wnode/src/aof/single_log.rs`、`wnode/src/aof/waof_sublog.rs`、`wnode/src/aof/garnet_log/commit.rs` recover_async 注释三处（原冒称「续行门控在回放驱动/由 FailOnRecoveryError 承担」，rust 回放驱动无此门）、`wnode/tests/waof_recover_async_error.rs` 文件头注释。六处均已加回指本条锚，双向锚注成立；`task/zcode.review-plan.md` 内调度摘要字样非注释假锚，不在订正面。r6-cli 原票散佚失踪亲验属实（其票文件零残留；惟「fail-on-recovery-error 默认相反」字样在 `task/zcode.review-plan.md:51` 队列摘要与 git log 仍有残留，原票「task 全队列与 git log 零残留」句已注记限缩为票文件零残留），本条系其现势收口非重复立案。零行为码改动（纯注释 + 本登记条 + 票面注记）。


## 123. PERSIST 无 TTL 命中臂观察者推进与 AOF 条目分叉（C# Succeeded 无条件推进版本落条目 vs rust 真 Redis 形零推进零镜像；登记偏差不补齐）

工单 task/ing/zcode-r133c-ttlwrite.md 登记（甄别 r134丙-tr-a 席双侧现码亲验坐实，定级 P4 登记级：不改码不改行为，只补台账 + ttl_sync.rs 头注失实括注订正 + 两把防回摆锁测；裁决定性=登记偏差不补齐，严禁后审按 C# 向补齐 no-op 推进）。编号顺编注记：票面「§99 起批续号」系立票时旧册实况已过期，本条按落笔时现册册尾 §122 顺编拟取 §123，先入库者得号、撞号不覆写（§116/§117/§118 同款纪律）；完工前复跑 merge dev 若撞号让位顺编并同步本条内与 ttl_sync.rs 头注、锁测回指锚。（落册实况：merge dev 复跑后主册册尾仍 §122 居末（dev 新入 MEMORY USAGE 条仅动 §32b 清单第 24 项非顶级条），本条得号 §123 无撞号，码内注释与锁测回指锚统一为 §123。）

C# 一手形态（观测面必推进）：统一存 PERSIST 走 RMW 链（`garnet/libs/server/Resp/KeyAdminCommands.cs:NetworkPERSIST` 458-486 → `libs/server/API/GarnetApiUnifiedCommands.cs:PERSIST` 113-114 → RMW_UnifiedStore），存储侧对「存活键但无 Expiration」的 :0 臂与 :1 臂**同构收口**——内存原位臂 `UnifiedStore/RMWMethods.cs:InPlaceUpdaterWorker`（:176-210）PERSIST case（:199-201）经 `HandlePersistInPlaceUpdate`（:250-259）写 :0 后 ipuResult 保持默认 Succeeded，回到 `InPlaceUpdater`（:157-174）即**无条件** `IncrementVersion`（!Modified 恒真：`RemoveExpiration` 只动 DataHeader 不触 RecordInfo.Modified，`RecordInfo.cs:215-223`）并置 NeedAofLog；拷贝重建臂同形（`CopyUpdater:103` → `HandlePersistCopyUpdate` :232-248 无 Expiration 写 :0 仍 return true → `PostCopyUpdater` :115-154 无条件 :119 IncrementVersion + :151 NeedAofLog）；对象域（ValueIsObject）经 PostCopyUpdater 同一对 :0 臂同形。落条目经 `UnifiedStore/PrivateMethods.cs:WriteLogRMW`（104-118，Deterministic）。即 C# 契约：PERSIST 只要命中存活键（含 :0），观察者版本必推进一次、AOF 必落一条 UnifiedStoreRMW-PERSIST 条目。对照同 RMW 族 EXPIRE 的条件拒绝臂：`EvaluateExpireInPlace`（`UnifiedStore/PrivateMethods.cs:140-154`）expirationChanged=false 回 NotUpdated 不推进不落条目——C# 自身即区分「EXPIRE :0 静默」与「PERSIST :0 不静默」，后者一手代码可证非误读。

Rust 现状形（该臂真 Redis 静默，两径四锚）：快臂 `persist_apply_sync`（`wedb/wnode/src/resp/key_admin_commands/keys.rs`，锚订正注：票面 844-872 系立票时行号，现树 fn 实况 838-866）`ttl_of_sync` 回 NotFound/Success(None) → `Ok(Some(0))` 直返，零 bump、零物理写；慢臂 `wkv::StoreSession::persist`（`wedb/wkv/src/ttl.rs:641-668`）None 臂不删不回推进，会话包装 `persist_key`（`wedb/wnode/src/storage/session/storage_session.rs:825-840`，锚订正注：现树 :863 起）applied>0 才 bump。journal 面为旁路写监听驱动单源（`wedb/wkv/src/session/raw/mod.rs:notify_write_listener_with_version` 281-339，仅 KeyTag::Ttl 物理写产 TtlWrite 镜像，`service.rs` 折成 Pexpireat/Persist 条目）：无 TTL 可删即无写、无事件、无条目。附带注释失实源已随本票订正：`ttl_sync.rs:del_ttl_sync` 头注原括注「PERSIST 经 RMW 面同向」对 :1 臂成立、对 :0 臂与 C# 事实相反，误导后续席按同向判净，现改述为「:1 臂同向、:0 命中臂 C# 无条件推进、rust 采真 Redis 形已登本条」。

裁决与保留理由（二择一裁登记偏差）：rust 现状即真 Redis 语义——PERSIST :0 不动键、不失效 WATCH，非违「版本与观察者栅栏」总则（review.md 该总则限「存在键修改与缺席键删除」径，:0 no-op 不在射程）。向 C# 对齐补齐须在写驱动 journal 单源（raw/mod.rs:281-339）之外另开零写落笔口，违单机制且向 C# 上游冗余回摆（红线 4），故不采补齐案、裁登记偏差 + 注释纠偏。危害定性：观测面而非数据面——同客户端序 WATCH k（k 存活无 TTL）→ 他连接 PERSIST k（两侧皆答 :0）→ EXEC，C# 观察者版本已推进必 abort、rust 无推进必 commit，事务失效栅栏跨实现分叉；AOF/复制面条目序列分叉（C# 从库重放 :0 条目时从端同型推进观察者栅栏；rust 从库无此条目），终态键状态一致、无蒸发无发散，危害限契约与可观测字节流，定 P4。

划界（本条不扩面）：§28 所裁仅「rust 两写对 C# 一次、只多不少」的**超额推进向**，本缺推进向不在其覆盖内、亦不联动；genexpire1 判净的 PERSIST 四臂系应答值面（:0/:1 判定）镜像，本条为其未触的推进尾巴与 journal 尾巴；expireopts1 判净的「Persist 真实删除才推进」为 GETEX-PERSIST 臂（C# 该臂 `MainStore/RMWMethods.cs:786-794` NotUpdated，两侧同向静默），**不在本条扩面**；EXPIRE 族 :0 臂（条件拒绝）两侧同静默判净，EXPIRE 同值重设 C# expirationChanged=true 推进落条目（`SessionFunctionsUtils.cs:44-47`）对 rust `put_ttl_sync` 恒写恒推进 + TtlWrite 镜像判净；SMOVE/对象写臂 KEEPTTL 余面无新缝（信封 upsert 不触旁路、STORE 族清退序在池票 r122c-setstore1）。PERSIST 三域旁路单轨、缺键/过期残键 :0、GETEX 无参保留各臂留痕随档。姊妹形 PFADD 轨（无寄存器变更臂 C# :0 臂推进/镜像与拷贝臂迁移写回 vs rust 零写回零推进零镜像）另立专条（§132），本条不扩面至 PFADD。

后果与严禁回改：**严禁后审/对拍席按 C# UnifiedStore/RMWMethods.cs:157-174 的无条件推进形向 rust 补齐 :0 臂 no-op bump 或手动注条目**——补齐即旁路写驱动 journal 单机制另起第二落笔口、向 C# 上游冗余回摆，改回才是真回归；后续轮遇「WATCH 无 TTL 键 + PERSIST :0 + EXEC」与「AOF :0 PERSIST 零条目」用例直引本条判有意偏差并跳过，勿重复疑报。本票零行为改动、零 rust 生产码改动（仅 ttl_sync.rs 头注一处分臂订正 + 本登记条 + 锁测）。锁面：`wedb/wnode/tests/persist_nottl_zeroarm_locks.rs` 两把真实会话防回摆锁（夹具沿 select_switch_db_invalidates_watch 双连接共享版本表/引擎级 watch 钩子形制与 ttl_ticks_aof_mirror 主副双 NodeService WAL 形制，零假 mock）——`watch_nottl_key_persist_zero_exec_commits`（钉死 rust 现状形：WATCH 存活无 TTL 键 → 他连接 PERSIST 得 :0 → EXEC 提交成功，C# 形必 abort 面不采）；`watch_persist_real_delete_one_exec_aborts`（同址对照锁：:1 真实删除臂 bump 在位、栅栏活性可证，防本锁因接线假绿回摆）；AOF 面 `aof_persist_zero_arm_produces_no_entry`（:1/:0 两臂差分计数锁：同一流水先重放计 N、后行 PERSIST :1 + PERSIST :0 再重放计 N+1，:0 臂零条目；从端重放终态键值与 TTL 全等断言随批）。

## 124. TLS 残面四处登记缺口收口：服务端会话票据恒零产出（含客户端恢复默认观察句）、webpki 链深 6 与 EKU 收紧两面包容钉根+出站两臂、吊销旋钮删员缺席（纯登记+两处注释锚订正，零行为码）

工单 task/ing/zcode-r135c-tls2.md 登记（甄别 r136丙-tr-tls 席通过（收窄）；登记级：不改码不改行为，只补台账加两处注释锚订正，零测试改动）。承 §36 开宗明义「后续任何 TLS 轮次对账直接引用本条，勿重复取证疑报」之单源取证教义：本票 TLS 残面五域（SNI／ALPN／会话恢复／链深与 EKU／版本与套件）全轮对账零行为 defects，登记缺口仅下述四形；五域判净点位与理由以本条尾部随档留痕供后续轮直引。登记取舍收窄留痕：原票方案 b（出站客户端 resumption 256）降为 a) 内观察句——库默认且生产无对端可观分叉不单登；原票 c webpki 收紧族三处收为两处（仅深度上限 6 与 EKU required_if_present），适用面限 inbound 钉根臂与出站校验臂（AnyClientCert 宽松臂无链构建结构上不适用、信任锚无自签强制双侧对等，两形均不登）；原票 d 先例引 §40c 系误引（该节为设备段目录防御分叉，与吊销无涉），甄别席订正循 §56／§68／§69 配置删员·旋钮不设先例族。编号顺编注记：票面拟号 §115 与甄别时号 §117 俱系立票时点册势快照已过期，本条按落笔时沙箱现册册尾 §122 顺编拟取 §123；落册实况：merge dev 复跑见同批 ttlwrite 先入占 §123，本条让位终取 §124——先入库者得号、撞号不覆写（§116-§122 同款纪律），完工前复跑 merge dev 撞号则让位顺编并同步本条号与 wconf／wtls 两处注释回指锚。

### a) TLS 服务端会话票据恒零产出（rustls 默认 NeverProducesTickets 双版本臂门控；「服务端 ticket key 轮换」命题判无标的；客户端恢复默认开为观察句）

C# 一手形态：SslStream 侧零恢复配置面——`GetSslServerAuthenticationOptions`／`GetSslClientAuthenticationOptions`（garnet/libs/server/TLS/GarnetTlsOptions.cs:158-188）服务端不置亦无会话缓存暴露面，客户端仅显式 `AllowRenegotiation=false`（:180），恢复主面双侧对等净。
Rust 形态：服务端 `ServerConfig::builder()` 未配 ticketer（`wedb/wtls/src/server.rs` server_config 装配零置位），落 rustls 0.23.45 库默认 `handy::NeverProducesTickets`（src/server/builder.rs:116；src/server/handy.rs:154-157 `enabled()` 恒 false），TLS1.3 NewSessionTicket 臂（src/server/tls13.rs:125）与 TLS1.2 恢复臂（src/server/hs.rs:209）双双门控于 `ticketer.enabled()`——**本仓服务端会话票据恒零产出**，每次握手恒为完整协商。
裁决与观察句：① 既往疑报「服务端 ticket key 轮换缺失」命题判**无标的**（无票据即无轮换物），后续 TLS 轮遇票据／恢复族用例直引本条判无标的并跳过，勿重复取证；② 观察句（库默认形态、非对位缺陷、勿升级）：rust 出站客户端内存会话缓存默认开启 256 名额（src/client/client_conn.rs:530-541 `Resumption::default()` = `in_memory_sessions(256)`，ClientConfig 装配落点 src/client/builder.rs:172），本仓对席服务端恒零票据下该缓存为静置件，仅对接支持票据的第三方异构对端时才可能现恢复性短握手，且恢复握手的证书校验语义（链构建／SAN／钉根）与全握手完全一致——勿按恢复差异疑报。

### c) webpki 收紧两处：中间证书深度上限 6 与 EKU required_if_present 严判（适用面限入站钉根臂与出站校验臂；§36 尾段双向回指，勿重复制文）

C# 一手形态：`ValidateCertificateIssuer`（GarnetTlsOptions.cs:286-326）`new X509Chain()` 走平台默认深度（远大于 6），仅显式设 RevocationMode=NoCheck、VerificationFlags=AllowUnknownCertificateAuthority、VerificationTime、UrlRetrievalTimeout，不查 EKU。
Rust 形态（两硬收紧，皆库默认落点）：i) 链深——中间证书数上限 `MAX_SUB_CA_COUNT = 6`，路径构建超限即 `MaximumPathDepthExceeded`（rustls-webpki 0.103.15 src/verify_cert.rs:847 常量定义、:803-804 路径 push 门）；ii) EKU——`required_if_present` 严判（src/verify_cert.rs:517-527），入站客户端证书带 EKU 扩展而缺 clientAuth 一律拒（rustls src/webpki/client_verifier.rs:391 `webpki::KeyUsage::client_auth()` 校验点）。
适用面边界（收窄形）：两收紧仅存在于 webpki 链构建路径两面——入站钉根臂（WebPkiClientVerifier，`wedb/wtls/src/server.rs:394-396`）与出站校验臂（`with_root_certificates` 装配 → rustls WebPkiServerVerifier 走 webpki 链校验，`wedb/wtls/src/client.rs`）；AnyClientCert 宽松臂（server.rs:403/:442）不做链构建，深度／EKU 结构上不适用，不入册；「信任锚无自签强制」（rustls src/webpki/anchors.rs:70 经 webpki trust_anchor 转换仅提取 subject/spki）与 C# ExtraStore 提示 + Thumbprint 全等臂（GarnetTlsOptions.cs:295/:312-317）双侧对等，不登（判净见本条尾部留痕 iv）。
裁决（双向回指 §36 尾段，勿重复制文）：§36 尾段已裁钉根与 SAN 两处收紧向分叉为「两处 rust 均更严、刻意分叉、C# 过 rust 拒为预期、勿按 C# 宽松臂回改」——链深 6 与 EKU 两包系该裁决**同族延伸**，本条只补适用面落点与对拍口径，裁决语引彼段不重复制；§36 尾段已同步加同族延伸注记指本条（双向锚成立）。**严禁按 C# 宽松面回改放宽**——放宽即弱化 webpki 校验面，违本仓 fail-fast 收紧向系列裁决（§27／§36d／§57 族），回改才是真回归。对拍口径：深链（中间证书 > 6）C# 过而 rust 报 MaximumPathDepthExceeded、带 EKU 缺对应 usage 证书 C# 过而 rust 拒，两形直引本条判有意偏差并跳过，勿重复疑报。

### d) certificate-revocation-check-mode 旋钮删员缺席（C# 生效默认 NoCheck 与 rust 实际行为全等；先例循 §56／§68／§69）

C# 一手形态：`Options.cs:336-337` 声明 `--certificate-revocation-check-mode`，`defaults.conf:255-256` 生效默认 NoCheck，入站（GarnetTlsOptions.cs:161）出站（:181）一钮共用。
Rust 形态：wconf 无该字段、wtls 无吊销装配链；rustls 0.23.45 仅有 **unstable 性质的 CRL 显式输入面**（入站 `WebPkiClientVerifier` builder `with_crls` 一族臂，webpki `verify_for_usage` revocation 参数穿透），稳定面无吊销开关。
裁决（删员缺席登记）：C# 生效默认 NoCheck ＝不做吊销检查，与 rust 实际行为（无吊销检查）**全等**，Online／Offline 形态无对位面；本缺席属刻意删员非漏配，需裁文护体防对拍轮按假旋钮／漏项误报——先例形制循 §56（cert-subject-name 删员与互拒门不设）、§68／§69（旋钮不设登记族）；票面旧引 §40c 系误引已订正（见本条头部留痕）。
两处注释锚订正（本票实动面，纯注释零行为）：i) `wedb/wconf/src/node_options.rs` `tls_client_cert_required` 字段注（原「登记缺席不随本票落地」自陈）——册文此前实缺席，今随本票在册，自陈改指 §124d) 实条；ii) `wedb/wtls/src/client.rs:36` 模块头四要素清单原「rustls 无吊销检查面」措辞失准（rustls 实有 unstable CRL 输入面），按锚订正为「仅 unstable CRL 面未用」并补 §124d) 回指。
后果：后续配置对拍轮遇 C# `--certificate-revocation-check-mode` 而 rust 无字段用例，直引本条判已在册缺席并跳过；NoCheck 默认面双侧行为等价无需对账；真有 Online／Offline 需求另立单评审，本条不设配置通路。

随档留痕（五域判净点位，供后续轮直引，非登记项）：i) SNI 双侧对等——入站单证书形态 `DynamicCertResolver`（`wedb/wtls/src/server.rs:348-354`，ArcSwap 无锁单点）resolve 弃 ClientHello 恒返活跃证书，与 C# 丢 hostName（GarnetTlsOptions.cs:163-166）严格同构，SNI 缺席或失配双侧行为一致；出站 SNI 与校验名同取 client.rs `server_name()`（:138-150）同一字符串交 rustls SAN 校验，Host 与 SNI 一致性成立；空目标 fail-fast 门与 endpoint host 段回落已由 §36a 收口注记与 §71 在册，不重复取证。ii) ALPN 双侧恒不协商（假旋钮面亦无）——C# 全仓 .cs 零 ApplicationProtocols／SslApplicationProtocol 引用（仅 website/yarn.lock 噪声），SslServerAuthenticationOptions 与 SslClientAuthenticationOptions 均不置 ALPN；rust wtls 全 crate 零 alpn 设置，落 rustls 库默认 `alpn_protocols: Vec::new()`（src/server/builder.rs:117，客户端 src/client/builder.rs:170 同名单源）；wconf TLS 域七字段（tls_cert、tls_key、tls_client_cert_required、tls_client_target_host、tls_server_cert_required、tls_issuer_cert、tls_cert_refresh_freq，node_options.rs:431-493）逐一消费于入站投影（`wnode/src/server.rs:1082-1096`）、出站投影（`wedb/src/server/boot.rs:202-212`）、CONFIG SET 热换（`wnode/src/resp/config_commands.rs:296`）、刷新挂表（`wnode/src/server.rs:530`），无只读不用或置而不用旋钮。iii) 重协商净——rustls 结构性不支持重协商，与 C# 客户端 `AllowRenegotiation=false`（:180）同向；C# 服务端不置（SChannel 平台默认开）在 RESP 单次握手形态不可达。iv) 自签 CA 形态双侧一致——rust `RootCertStore::add` 接受任意可解析 DER 为信任锚、无自签强制（src/webpki/anchors.rs:70），与 C# ExtraStore + Thumbprint 全等臂对称；CA 空集禁静默与装载 fail-fast 已 §36d／§57 在册。v) 版本与套件净——rust 走 rustls 默认版本集 TLS1.2+TLS1.3 与 ring provider 全 ECDHE/TLS1.3 安全套件组（server.rs／client.rs builder 零定制），RC4／3DES／static-RSA 结构不可达，高危套件双侧均无开关亦无暴露；C# 全仓零 SslProtocols／CipherSuites／TlsVersion 引用交平台默认——双侧差异系运行时栈固有（平台策略 vs rustls 固定收紧）而非转写契约分叉，无代码面参数对位。

后果与纪律（总）：四形散在库默认层，不登则后续 TLS 轮必反复取证、反复疑报（§36 教义之直接后果），更实害向为按「对齐 C#」误读把深度／EKU 收紧回退放宽（见 c) 严禁条）；本票零行为改动、零测试改动（登记级，无锁测加装）。silent-downgrade 拆票灭失事实（r13 十一案所载、防并发证书热换 P0 链，issue／todo／ing／done／reject 五池零命中，按 review-plan:214 载 2026-09-22 git 重置事故推断灭失）本条不重立其案（案已立过，防重报），灭失事实仅留痕回报主会话决是否再拆。



## 125. \*STORE 族五同步写臂「清退先行」失败中间态修复型入库收口为「写回先行、清退随后」单点，尾笔清退未落系 C# 单记录整写下不可观测之新残留形（fail-loud 登记）

工单 task/ing/zcode-r122c-setstore1.md 登记（甄别 r123-tr-setstore1 席通过、定级 P3；**修复型入库** ea8bbb5f，非纯登记）。编号顺编注记：票面无预拟号；落册时点 dev 册尾实测 §123（PERSIST 条同窗先得号，tls2 条续占 §124），本条按落册时现册册尾顺编取 §125，先入库者得号、撞号不覆写，落笔时若再撞号按 §116/§117/§118 同款让位纪律顺编并同步条内与 `store_writeback_clear_ttl` 头注「deviation 台账登记」回指锚（锚按内容引不钉行号）。

C# 一手形态：SET 族三 STORE 非空收尾对目标键「写即清 TTL」与值写回**原子共一体**——`SetOps.cs:SetIntersectStore`（:381 起）以 `SET(key, newSetObject)`（:422）单记录整写随载 expiration 归零，空臂 `EXPIRE TimeSpan.Zero`（:426）；`SetUnionStore` 同形（:554 起，SET :593/EXPIRE :597）、`SetDiffStore` 同形（:822 起，SET :860/EXPIRE :864）。GEO 族 `SortedSetGeoOps.cs:GeoSearchStore` 采 Delete 前置：dest 排他锁 :128 罩 `Delete(destination)`（:178，票面 :181 系微漂已订正）→ZADD 两笔融合收尾，且一切错误臂——源 WRONGTYPE :150-153、源 NOTFOUND 空臂 :155-161——先于 Delete 返回，失败命令对 dst 零副作用。即 C# 面上「TTL 已清而值未写」与「值已写而 TTL 未清」两失败中间态均**不可观测**。

Rust 侧形态（修复已入库）：冷臂早已按此纪律收口（`storage_session.rs:obj_save_clear_ttl` 先信封后清 + `store_ttl_clear_critical_section.rs` 判据 1）；同步臂旧形却系清退前置分离式（del_ttl → 复验 → 写回），写故障窗内 del_ttl 闭环真实删除即推进并物理抹除 TTL 后 `set/z/geo_save_or_gc` 返 Err/-ERR 终结——旧值原样而永不过期（失败命令「复活」键）、观察者伪 abort（甄别订正口径：失败臂单次 bump＋降级重放双 bump，对 C# 失败 SET 零推进），同族命令同步/冷双臂对「失败即原态」两结局=单机制违背。本票改与冷臂同序：set `combine_store`（`set_commands/write.rs`，窗句柄由调用方装载前预取承自自指票 f8108eb0）、zset 三臂（`sorted_set_commands/write.rs:262/:347/:705`）、geo 一臂（`sorted_set_geo_commands.rs:710`）五写点统一收敛至清退单点 `store_writeback_clear_ttl`（`rmw_helpers.rs:1292-1306`）——信封写回 Ok(true) 后同一持窗临界区内清退；写回 Ok(false)/Err 臂天然零清退即回错误帧/重放，失败即原态与冷臂对齐；save 降级重放臂经冷臂漏斗重放亦单机制收口。

尾笔新残留形（本条登记正身）：写回 Ok(true) 之后 `del_ttl_sync` 返 Ok(false)（环形页翻转须降级）或 Err（存储错误）——**值已写、TTL 未清**，系 C# 单记录整写形态下不可观测的残留。裁决=与冷臂 `obj_save_clear_ttl` 尾笔 `clear_ttl` 失败上抛同口径 **fail-loud**：单点内 `log::error` 告警留痕并回 false，调用臂落 `RESP_ERR_GENERIC` 错误帧终结命令（禁静默成功、禁降级重放致值双写、禁引入第二把 TTL 闩或新原语）；禁序判据 1 执行期豁免系同步段单线程持窗零 await、无可观测禁序窗，申报于单点头注。危害限 whlog/环形页写故障窗且方向为旧 TTL 残存（键可早亡，非旧形之永不亡），无常规路径发散。

后果与严禁回改：**严禁复犯清退先于写回旧序**（写故障窗复活「抹 TTL 后 -ERR 键被复活永不过期＋WATCH 伪 abort」双缺陷）；**严禁**将尾笔 Ok(false) 改回静默成功或补异步降级二投（违单机制）；对拍轮遇「同步臂 save 故障注入 → -ERR 后 dst 值 TTL 原样（写臂失败形）」与「尾笔清退失败形：值新写、旧 TTL 残存、-ERR」用例直引本条判有意偏差并跳过。锁面：`wnode/tests/store_sync_arm_save_failure_ttl_intact.rs` 六臂故障注入锁（sinterstore/sunionstore/zrangestore/zdiffstore/zunionstore/geosearchstore `_sync_save_failure_keeps_dest_ttl_intact`，真故障存储夹具断言 -ERR 帧后 dst 值 TTL 原样、WATCH 零推进）；既有 `store_ttl_clear_critical_section.rs`（冷臂禁序判据沿用）、`store_overwrite_string_retire.rs` 六臂重放族、`geo_store_ttl_error_arm_ttl_intact.rs` 维持绿；三 STORE 命令同步/冷双臂成功路径 RESP 逐字节全等不回退。

## 126. COMMAND DOCS 跨父重名整表熔断「永 %0」与 C# 命令层异常上抛非等形（两口径分写登记；null/缺键两臂已修码对齐无分叉不赘）

工单 task/ing/zcode-r123c-cmddocs1.md 登记（甄别 r124丙-tr-docsclu 席通过、定级 P4 登记级；三臂中**仅臂三入册**）。编号顺编注记：票面「本票与 clustershrs 同批顺编 §99 起」系立票时旧册实况已过期——clustershrs 落 §119、本批落册时 dev 册尾实测至 §123（PERSIST 条先得号，tls2 条续占 §124），本条顺编取 §126，先入库者得号、撞号不覆写，与代码内回指锚（`resp_command_docs.rs:build_tables` 头注「登记级裁决」句）落笔时同步为 §126。执行注记承票面纪律：本臂登记措辞**分写两口径、勿称「对齐」**。

C# 一手形态：子命令表构建 `AllRespSubCommandsDocs.Add`（`garnet/libs/server/Resp/RespCommandDocs.cs:138`）与 `tmpExternalSubCommandsDocs.Add`（:146）遇跨父重名抛 `ArgumentException`；该异常**不被** `TryImportRespCommandsData` 的 `catch (JsonException)`（`RespCommandDataProvider.cs:164`）覆盖，直穿 `TryInitializeRespCommandsDocs` 上抛至命令层（`RespCommandDocs.cs:108-117` TryInitialize 无兜底 catch，异常下 `IsInitialized` 恒未置位、每个 COMMAND DOCS 请求重试再抛）——可观测形=命令层异常路径。

Rust 侧形态（修码后现态）：`build_tables`（`wedb/wnode/src/resp/resp_command_docs.rs:502-548`）对 all_sub/external_sub `insert` 返旧值即整表熔断 `return None`（:531/:543-547）→ `try_initialize` 永假、`TABLES` 恒 None → COMMAND DOCS 全体回显**永久 %0 空 map**（消费臂 `basic_commands/mod.rs:257-264`）。一永 %0、一逐令上抛，**非等形**——本条所登记即此两口径差。根表 all/external 重名拒于 `wresp/src/catalog/data_provider.rs:try_import_resp_commands_data`（:42 起，空名/大小写不敏感重名整表回 false），与 C# `TryAdd` 拒收同臂不属本叉。

另两臂修码回 C# 原形、零残留分叉不另登：臂一 `RespCommandDocs.sub_commands` 改 `Option<Vec<…>>`（:188），发射与计数判据改 `is_some()`（:216/:257-263），`Some([])` 发射键并写零长 map（%0/*2n 降级 *0）、`None` 不发射——对位 C# `SubCommands != null` 判据（RespCommandDocs.cs:231/:279），与 arguments 字段同形（旧 `unwrap_or_default` 坍缩形系转写漏改非设计）；臂二 `RespCommandDocsImport.command`/`RespCommandsInfoImport.command` 加 `#[serde(default)]`（:346/commands_info.rs:403），缺 Command 键落 `RespCommand::None` 条目保留照常回显（对位 C# STJ 缺省枚举语义），未知枚举名仍整表失败（JsonException 同臂）。COMMAND INFO 面发射臂恒 10 元素数组免疫 null 分叉，仅导入两臂同收敛。

触发条件与定性：现随包数据三臂零触发（`wresp/tests/command_docs_data_shape_lock.rs:bundled_data_triggers_none_of_the_three_arms` 逐字节普查锁：256 共有根条目深比对零差异、无空 SubCommands、无跨父重名；差集 rust-only SUNSUBSCRIBE 已登 §7、cs-only MODULE/REGISTERCS 系转写删除），且导出器 `WhenWritingDefault` 只省 null 不省空数组、playground 重生成管道真实存在——属登记级防御分叉，无运行期危害（故 P4）。裁决与严禁回改：rust 熔断形**禁改回** insert 静默覆盖（错父文档可被 COMMAND DOCS CONFIG|GET 类查询回显）；**禁**按 C# 复刻「命令层每次抛异常」形（unhandled panic/异常路径触本仓红线，静默 %0 系更严侧）；亦禁后续审查席以「一永 %0 一上抛」互判转写缺陷——对拍轮遇跨父重名合成数据用例直引本条两口径判有意偏差并跳过。锁面：`wnode/tests/command_docs_null_arms.rs` 三臂快照（empty_array 出 %1+subcommands %0、missing_command_key 条目保留、duplicate_sub_command_names_across_parents 整表熔断）+ `unknown_command_name_fails_whole_table` 维持臂；`wresp/tests/command_docs_data_shape_lock.rs` info 面同臂四锁。

## 127. enable_quantization 回填上界改「置位→排空→快照→再排空」定点收敛发布（原生写屏障全阻铸造形之 rust 计数屏障等价改造；修复型分叉入库）

工单 task/ing/zcode-r123c-vechain1.md 立案一登记（甄别 r124丙-tr-vechain1 席现码逐行复跑通过、定级 P2 维持且较票面更重；**修复型入库** b785ae57）。编号顺编注记：本条按落册时现册册尾顺编取 §127（先入库者得号、撞号不覆写），与 `fsm.rs:enable_quantization` 头注（现自陈「对账原生 fsm.rs:enable_quantization」）回指锚落笔时同步。查重前提：r27-vectordiskann 发现二（训练窗重启屏障）与 r94-quantguard 长度守卫在册面正交，本条不触及。

C# 参照面形态：C# 向量库经 P/Invoke 转发（`garnet/libs/server/Resp/Vector/DiskANNService.cs:111-118`），量化屏障语义单一真源在原生对账源 diskann-garnet——`fsm.rs:enable_quantization`（:532-545）**取写屏障后**读 max_id 置 quantization_enabled（注释自陈意图：屏障期 next_id 不动、挂起未量化插入数据已全写），写屏障内快照即精确上界、铸造被**完全阻塞**；`next_id`（:322-326）全程持 barrier 读守卫（ReuseGuard 承运）直至数据写完。

Rust 侧形态（修复已入库）：rust 以无锁计数屏障 `pre_switch_inflight` 替换原生读写锁（转写改良基座），**不能阻塞铸造**——旧序「:592 先快照 → :593 置位 → 排空」在「登记早于置位、mint 晚于快照」交错下发布上界之外漏出未量化新 id：`set_element` 仅认铸造时观测的 `should_quantize` 快照不写 Quantized 域、回填分片区间 [0..=上界] 不覆盖、`all_quantized` 标志=1 使重启恢复恒真（`data_provider.rs:377` 读取）免重回填——该 id 量化轨**永久不可见**（VSIM/图遍历静默跳项而 VRANDMEMBER/VGETATTR 存活、同键双态视图分叉，唯 VREM+VADD/drop 重建可收敛，甄别订正「重启收敛不确」）。修复=定点收敛发布（`wvector/src/fsm.rs:enable_quantization`）：**置位→排空→snapshot=max_id()→再排空→复核 max_id 推进则重采→定点后发布**；配合 `next_id` 登记后复读臂（置位即注销计数改走启用臂就地量化）与跨线程 Dekker store-load 握手（两侧统一 SeqCst），达成与原生同等不变量「发布上界恒覆盖全部 should_quantize=false 之已铸 id」。机制分叉点：原生以独占写锁**阻塞写者**取精确快照，rust 以放行写者＋登记复读＋定点循环**收敛**等价——快照时刻、等待形态、内存序协议三处皆非等形，唯终态不变量全等。

裁决与严禁回改：rust 侧系自研并发基座上的改良等价形（原生读写锁守卫跨 await 持守卫违本仓 compio 禁忌，不可复刻），修复型不改变机制分叉待登记事实；**严禁按原生字面回改**「先快照后置位」旧序（回改即复活永久量化洞）；**严禁**另引入第二套写屏障/写时复读双轨（登记时观测单一判据系本裁决组成件）；弱内存序下禁将握手 SeqCst 降为 Acq/Rel（注释自陈不足）。对拍轮遇「enable 期高并发 VADD 后上界外 id 量化轨不可见」用例直引本条判已修+有意机制差并跳过。锁面：`wvector/tests/quant_enable_barrier_race.rs:enable_quantization_snapshot_covers_concurrent_mints`（插入挂起于 reuse 扫描 await 期放行 train+enable、恢复后 all_quantized 置位下全部存活 iid Quantized 域有点读记录、VSIM 逐条命中可见）；`quant_backfill_restart.rs` 既有 r27 锁族零回归。

## 128. train_quantizer 全序「持久化→启用→回填调度」收锁段内闭环＋早退臂重启窗二择＋回填收尾 >= 自愈（对原生「已训臂恒 false 不调度」「恰等收尾」两处刻意改写）

工单 task/ing/zcode-r123c-vechain1.md 立案二登记（同票同甄别席通过、定级 P3 维持；**修复型入库** b785ae57，与本席案一 §127 两票并案不同宗、分条登记）。编号顺编注记：本条按落册时现册册尾顺编取 §128，先入库者得号、撞号不覆写，与 `dynamic_quant.rs:train_quantizer` 头注（现自陈「契约全序」）回指锚落笔时同步。

C# 参照面形态：原生对账源 `diskann-garnet/src/provider.rs:train_quantizer`（:524-613）——`training_lock.try_lock` 失败返 false（:526-530）、**已训练臂一律直返 false 不触发回填调度**（:532-537），serialize→写 `_qnt`→`enable_quantization` 全部在锁段内同步闭环（:594-611）；`backfill_quant_vectors`（:622-730）收尾完成判据**恰等** `fetch_add + 1 == task_count`（:694），各失败臂恒带 log（"Index will operate full precision only mode"，:631-646/:660-665/:727 三臂）；调度面 `garnet/libs/server/Resp/Vector/VectorManager.Quantization.cs:TryProcessQuantizationRequest`（:151-168）仅 BuildQuantizationTable 返 true 方派发分片。

Rust 中间态缺陷形（本票修复对象）：`wvector/src/provider/dynamic_quant.rs:train_quantizer` 曾锁段止于 is_trained 检查、`_qnt` 落盘移锁外 await，且为修 r27 发现二把早退臂改**恒返 true** 并补屏障——并发双建表项下 W1 训成释锁、`_qnt` 未达盘，W2 早退臂即可置位发布上界并派发分片；回填末片读 `_qnt` 缺失**裸 return false 零日志**（原生三 log 臂移植丢失），fetch_add 单调计数恰等条件被弃跑轮次/越界叠加错过即**永不复等**——量化流水线永久停摆全精度模式（is_quantized 恒假）、停摆点静默不可观测、活实例内无再调度源唯重建/重启复位。全序被锁外重排成「启用可先于持久化达」。

Rust 现态（修复入库后本条登记之形态，三处对原生刻意改写）：① `_qnt` 落盘与 `enable_quantization` 收回 `training_lock` 临界段内同步闭环（async_lock 守卫 Send 可跨 await，冷 I/O 全程无同步锁持有，不触 compio 禁忌），落盘/屏障失败即返 false 不派发，锁竞争按弃返 false 由首训者派发（对位原生 :526-530）；② 早退臂二择——**仅真重启窗形态**（本实例回填上界恒 u32::MAX 且自盘中读到非空 `_qnt`）补启用屏障并返 true 派发回填（r27 发现二修复物的语义承接，**原生恒 false 不派发，此系 rust 对重启收敛窗的刻意分叉**），本进程已训/落盘失败/重复建表项（上界已有限）返 false 对位原生「已训练臂直返 false」，杜绝 `_qnt` 未达派发与双轮分片叠加；③ 回填收尾判据 **`>=` task_count 化**（恰等系永久停摆根因，`>=` 使后续重投分片仍可越线收尾、收尾段幂等允许重翻——**对原生 :694 恰等形之刻意收紧分叉**），缺 `_qnt`/写失败两臂补回原生同款 log 留痕上抛。调度消费面（`wnode/src/resp/vector/vector_manager_quantization.rs` 见 true 即 push N 分片）零改动。

裁决与严禁回改：rust 为正确侧（永久停摆系自家锁外重排＋静默弃跑复合缺陷，原生恰等形在其自身锁段全序下无停摆面）；**严禁**按原生字面把早退臂改回恒 false（复活 r27 发现二重启窗永不收敛）或恒 true（复活本票 `_qnt` 未达派发窗）；**严禁**收尾判据回改恰等 ==；**严禁**把落盘/启用屏障再移出训练锁；两枚 log 臂禁删（静默弃跑即本宗病根）。对拍/审查轮遇「双建表项并发下分片不叠加」「_qnt 写故障后恢复通道存在」类用例直引本条。锁面：`wvector/tests/quant_train_barrier.rs` 三锁——`duplicate_table_build_after_success_does_not_redispatch`（在途他实例/重复建表项返 false 不派发）、`backfill_finish_recovers_after_qnt_read_fault`（>= 自愈：读取故障弃跑后重建轮从计数复位终置 all_quantized）、`qnt_write_failure_never_dispatches_backfill`（落盘失败锁段内即返 false 零派发）；`quant_backfill_restart.rs`（r27 锁）零回归。

## 129. SCAN 族分层臂 exec_tiered_scan 收口 tiered_guard 锁窗内刷新单点（修复型入库；claim 窗回退装载快照与 Set 哑值天然豁免两登记面）

工单 task/ing/zcode-r127c-setscan1.md 登记（甄别 r128丙-tr-setscan1 席通过、定级 P2 维持；**修复型入库** a1fc5dfb→431b9605）。编号顺编注记：票面无预拟号，本条按落册时现册册尾顺编取 §129，先入库者得号、撞号不覆写，与 `scan.rs` 刷新段头注回指锚落笔时同步。划界承票顶：与 r6-scan（键空间域）、r120-scanfam（判净五点半点）、done 票 store-selfref/swapnum 族异缝不并案。

C# 一手形态：`SharedObjectCommands.cs:ObjectScan`（:18，:68 入 `storageApi.ObjectScan`）→ `ObjectStore/Common.cs:ObjectScan`（:71 起）经 `objectContext.Read` 记录装载与扫描在**同一持锁窗口内原子完成**——同键并发 SADD/SREM/DEL 被记录锁序列化，任意页应答要么旧全量要么新全量切片，游标代数恒基于锁窗内与树内容互一致之 Count，**绝不出窗外快照、绝不因并发漂移回错误帧**。C# 集合恒驻对象域，分层树/迁移 claim 系本仓自研无对物（本条登记面全部落于自研面对该契约的保真与两处刻意残余）。

Rust 侧缺陷形与修复（已入库）：`tiered_collection_ops/scan.rs:exec_tiered_scan` 曾窗外 `load_collection_stub` 取陈旧 meta → 裸 `acquire_tree_read` 后不刷新，陈旧 `meta.size` 驱动帧宽预留与 `scan_converge_cursor` 收敛 total——装载与取锁 await 缝内同键分层 SADD（set.rs 写臂 tree_put_batch+save_tiered_meta）完整提交即新成员插于旧成员字典序之前、scanned 触陈旧 total 提前归零，**全程存活旧成员跨页永久漏扫**（信封态无此形，双态分叉）；同缝内键删空换出/RENAME claim 则被调用方（`shared_object_commands.rs:slow::object_scan` Ok(false)→Err(()) 漏斗）折 `-ERR` 错误帧，与同并发下 SMEMBERS/SCARD 走 tiered_guard 读臂照常应答三径不同构，且恰违 `common.rs` 读臂成文裁决「读面不扩大忙拒面」。修复复用既有机制禁第二套：先 `acquire_tree_read`、同锁窗内 `session.refresh_tiered_meta`（`wkv/src/range_index/heal.rs:27`，与 `common.rs:tiered_guard` 读臂逐臂同形三态裁决）——Ok(true) 锁内新值承载 total/items_upper/cursor_reserved；**Ok(false) 键消亡** size 记 0 不触碰树、经预留-回填出 [0, 空] ≅ 应答缺失（与 SMEMBERS 消亡臂同构，不新增帧形）；**Err(MigrationBusy) 回退装载快照照常扫**；真实 IO Err 未落帧前上抛撤帧沿既有漏斗（`scan.rs:452-454` 口径不动）；判型检查维持装载态。路由装载门经 common.rs 单点 `load_collection_stub_for_read` 收口与 try_tiered_arm 共用（MigrationBusy 不折错误帧）。

登记面二（claim 回退残余形，裁决级刻意）：迁移窗内换入前旧树冻结自洽，「装载于窗前、窗内扫旧树」仍属有效读——刷新败于 MigrationBusy 时 total 取装载快照，游标代数与树内容可短暂失配，残余仅帧头上界一面由预留-回填（wresp ext.rs 收窄/加宽两向合法）兜底；此系对 C#「绝不出窗外快照」字面之**有界例外**，与本仓读臂「忙拒面不扩大」总裁决同源，非漏修。登记面三（注记随批入码非缺陷）：Set 树记录值恒 `SET_MEMBER_DUMMY_VALUE`（`wcol/src/lib.rs:29` 裸载荷 b"1111"），首字节不在 member_ttl 旗标域 0/1，扫描臂到期复查 `!= List` 判据对 Set 恒假、豁免语义天然成立无需并入 List 显式分流臂（`scan.rs` 扫描回调内注记单源，对照 `member_ttl` 模块头同源表述）；List/Set 记录恒裸载荷豁免理由一处可查。

后果与严禁回改：**严禁**为求同构另起第二套锁窗内刷新机制或第二张锁表（本票纪律=复用 refresh_tiered_meta 单点）；**严禁**把消亡臂 [0,空] 或 claim 回退快照改折忙/错误帧（复犯三径不同构且违 common.rs 读臂成文裁决）；**严禁**把「严禁折空游标页」反向误改——存储 IO 真败仍须上抛撤帧，存储故障不得伪装「扫描完毕」。分层↔信封游标序域切换期不保不重不漏系本臂既有成文说明（scan.rs 头注），非本条新增面。对拍轮遇「并发 SADD 大集合分页覆盖」「消亡/claim 窗 SSCAN 应答形」用例直引本条。锁面：`wnode/tests/scan_tiered_read_arm_refresh.rs` 三锁（`concurrent_growth_scan_pages_cover_persistent_members` 注入缝内五新成员字典序前置、全程页并集覆盖注入前全部旧成员；`dead_tiered_key_scan_answers_zero_empty_not_error` 消亡出 [0,空] 逐字节；`dual_state_quiescent_full_cursor_sets_equal` 双态静默全游标集合全等）；`scan_family_dualstate_frames.rs` 追加 `tiered_scan_claim_window_snapshot_fallback_frames`（claim 窗回退逐字节＋items_upper 越缝帧头加宽用例），全族既有双态帧字节锁零回归。

## 130. INFO server/persistence 段四点对 C# 对外可观测面分叉并条收口：bg_task_health / aof_flush_failures 自研超集行、SafeAofAddress 死字段哨兵反向、os 文本形态（登记＋两枚契约注释订正，零行为改动）

工单 task/ing/zcode-r126c-infosec1.md 立案二登记（甄别 r127丙-tr-infosec1 席两案俱立、案二定级 P3 登记级；**纯登记＋注释订正零行为码**）。编号顺编注记：票面「§99 起让位在途三票」系立票时旧册实况已过期，本条按落册时现册册尾顺编取 §130，先入库者得号、撞号不覆写（§116-§118 同款）。同票案一（P2 混合段扫描段空表虚报对外假数据）系修复型收口已入库 c4ba8e32、分叉随修消除不入本册，本条不含彼面；承 r30-bgthread 承继关系：bg_task_health/aof_flush_failures 两行系 r30 发现五之修复产物（plan 在册），本条立 INFO 超集**台账缺口**非旧案重提。四处此前 deviations 全库 grep 零命中、无 INFO 级行形锁测（bg_task_health 仅 supervisor 快照面有测）。

C# 一手形态：`GarnetInfoMetrics.cs:PopulateServerInfo` 恒 **14 行**（:53-73，其中 `os` 值取 `Environment.OSVersion.ToString()`（:60）运行时 OS 描述串如 "Unix 5.15.0-…"）；`GetDatabasePersistenceStats` 恒 **6 行**（:366-379），末行 `SafeAofAddress` 读 `storeWrapper.safeAofAddress`（:377）——该字段 `StoreWrapper.cs:166` 全库**仅声明赋 -1、零写点**（死字段，AOF 启用时应答恒 "-1"）；commandstats 零计数过滤谓词 `entry.Calls == 0 && entry.RejectedCalls == 0` **两栏判零不含 failed**（:274）。

Rust 现态与四点裁决：其一，server 段条件第 15 行 `bg_task_health`（`wmetric/src/info/garnet_info_metrics.rs:populate_server_info` 尾臂）——真源 `wbase::supervise` 快照＋登记计数（`wnode/src/resp/info_provider.rs:bg_task_health`），生产注册任务非空必出行，C# .NET 无 panic 语义无对位项；其二，persistence 段行尾第 7 行 `aof_flush_failures`（同文件 `get_database_persistence_stats` 尾臂）——真源 `waof/src/aof/waof_sublog.rs:flush_failures`（刷盘失败累计，对标 C# TsavoriteLog cannedException 运维可见面），且 **PERSISTENCE ∈ DEFAULT_INFO**（`garnet_info_metrics.rs:13-26`）＝裸 INFO 开 AOF 即出。两行均属 rust 观测面**自研超集**（默认应答对 C# 基线呈字节超集），承 r115 甄别口径：维持自研面、登记是唯一解除绑架手段——**严禁对拍轮按 C# 十四行/六行形回改删观测行**。其三，SafeAofAddress 值面反向哨兵：rust 该位接**真复制位点投影**（`info_provider.rs:safe_aof_address`——cluster 主侧 `get_primary_info` 首元素、非主/无提供方/无集群回 **0**），C# 恒 -1；-1（C#「未设」死值）与 0（rust「无投影」回退，且 0 兼合法地址域前缀）语义反向——rust 主侧真投影系增强、回 0 形与 C# 恒 -1 属可观测字节差，登记免每轮撞面；**严禁**按 C# 死字段回改恒 -1 或恒 0（回改即弃真观测面）。其四，os 形态一句登记：rust `env::consts::OS` 编译期短名（"linux"/"macos"/"windows"）vs C# 运行时描述串，字段名/位序同、值形态天然分叉，两侧对拍必发散、直引本条跳过。附带注释订正（随本条入库非行为码，两枚虚报冒称现码实锚）：`garnet_info_metrics.rs` trait `command_stats` 契约注释「已过滤 calls/rejected/failed 均为 0」与 C# 及 rust 实码（`info_provider.rs` 聚合臂 `calls > 0 || rejected > 0` 两栏判零、failed 恒透出不参与过滤）俱反，改述为「calls/rejected 两栏判零」；`safe_aof_address` trait 注释「对齐 StoreWrapper.safeAofAddress」诱导按死字段心智回改，改述为实态形（真投影/回 0 哨兵、C# 死字段系 -1）并回指本条；两处订正统一挂 §130 回指锚。

后果：对外应答逐字节可比面之台账缺口闭合；后续 INFO 文本对账/外部解析器轮遇「15/16 行形」「aof_flush_failures 行」「SafeAofAddress 0/真位点 vs -1」「os 短名」四形直引本条判有意偏差并跳过、勿重复提报。验证面：纯登记零行为改动，无新增 INFO 锁测（处方测试点 c 口径＝新登记条目与实现注释 grep 双侧命中对账即收）；`resp_info_mixed_sections.rs`（案一锁族）不受本条触及。

## 131. WATCH 后跨库切库位点作废：rust 切库成功提交点主动清容器对齐 C# 逐库管理器换任形入库，「回原库不复活」系裁决收口之残余分叉（修复型入库登记）

工单 task/ing/zcode-r133c-selectdb.md 立案二登记（甄别 r134丙-tr-b 席案二 P3 维持并附方向注记 a；**修复型入库** 1d5c33a7→5a87c095，本条系简短登记非全宗并载）。编号顺编注记：票面无预拟号，本条按落册时现册册尾顺编取 §131，先入库者得号、撞号不覆写，与 `resp_server_session/core.rs:invalidate_watch_on_db_switch` 头注「裁决收口语（方向注记 a）」回指锚落笔时同步。案一（MULTI 排队 SELECT 负数库号补中止臂，strict_i32 同 C# TryGetInt 档）系转写漏项纯修复、修毕与 C# 零残留分叉，不入本册。

C# 一手形态：每库独持 TransactionManager（`garnet/libs/server/Resp/GarnetDatabaseSession.cs:46/:63`），`SwitchActiveDatabaseSession`（`RespServerSession.cs:1712-1724`，字段清单次行 `this.txnManager = dbSession.TransactionManager`）切库即换任、watch 容器随管理器归库——`WATCH k(db0); SELECT 1; MULTI; GET x; EXEC` 由 db1 新管理器空容器服务，db0 位点**事实上作废**；同库 SELECT 短路不入 Switch（`ArrayCommands.cs:141` `index == activeDbId`）；惟 dbSession 系缓存复用，**SELECT 回原库位点事实复活**。

Rust 侧形态（修复已入库）：事务管理器会话级单实例（`resp_server_session/txn.rs:with_txn_manager` 同实例 take/put），切库单点曾零触 watch_container，而 `wtxn/src/txn_watched_keys_container.rs:add_watch` 登记期即烘会话物理前缀入 scoped_key_hash、`validate_watch_version` 跨库恒校验、`save_lock_hashes` 把旧库派生 hash 并入新库锁集——同序列 rust 反遭**保守误中止**（EXEC 假 nil）＋跨库带外锁条目，与 C# 成败相反。修复采票处方 1 方向 a：切库成功提交点单点主动清容器（`core.rs:invalidate_watch_on_db_switch` 复用 `TxnWatchedKeysContainer::reset`，与 DISCARD/EXECABORT 收尾 `txn_resp_commands.rs` 同型调用），热库当场物化点与冷库异步装载成功回写点**两物化点均经此单点**；同库切换 no-op 保在途位点（对位 C# :141 短路）。

残余分叉（本条登记正身）：**「回原库不复活」与 C# 非全等**——C# 缓存 dbSession 持原容器、SELECT 回 db0 位点复活续校验；rust 单实例容器已一次性清空、回原库位点不可复活。同序列「WATCH k(db0); SELECT 1; SELECT 0;（他会话改 k）MULTI; GET k; EXEC」C# abort、rust commit，属真值源分叉之刻意取舍：裁决=一次性作废收口（保全面消除登记期烘前缀/锁集并入两缺陷面，且不复活形对乐观锁教义为更保守安全侧），**严禁补逐库容器承接 C# 换任形**（票面明禁、违会话级单实例架构属过度设计）；亦严禁后续审查席按 C# 复活形回改。与邻票轴别：§115（版本轨/锁轨双域）、§121（中止/丢弃臂锁集收口）、r132c-watch-keybucket（换号代际冻结）各管一面，本条专掌 SELECT 切库×watch 保全面；deviations 全册此前无「切库作废 watch」登记（§58 系 HELLO 停泊、§32 尾注系库号值域）。锁面：`wnode/tests/select_switch_db_invalidates_watch.rs` 三锁——`switch_db_invalidates_watch_and_no_revival_on_return`（切库作废＋**回原库不复活**双向钉裁决形）、`no_switch_watch_abort_shape_persists`（不切库现中止形不回退）、`same_db_select_keeps_watch`（同库重放臂位点保持）；`txn_select_negative_db_aborts.rs`（案一锁族）不入本条。

## 132. PFADD 无寄存器变更臂 C# :0 臂无条件推版本落 AOF＋拷贝臂迁移写回（vs rust 真 Redis 形零写回零推进零镜像；§123 姊妹形 PFADD 轨，登记偏差不补齐）

工单 task/ing/zcode-r161c-fpfcomp.md 立案登记（甄别 r162c-tr-fpfcomp 席双侧现码亲验坐实，定级 P3 登记级维持；纯登记＋注释台账零行为改动，测试零新增断言）。编号顺编注记：本条按落册时现册册尾 §131 顺编取 §132，先入库者得号、撞号不覆写。

C# 一手形态（:0 臂非全静默，三面）：命令层恒逐元素单 RMW（`HyperLogLogCommands.cs:18-60`，:49-58 仅 pfaddUpdated>0 出 :1）。元素寄存器无变更（重复插入）时：其一，原位成功臂 IPUSucceeded 经派发器 `RMWMethods.cs:421-431` Succeeded 收口——:427 `!logRecord.Info.Modified` 即 `IncrementVersion`（首次触达恒进）、:429 AOF 开启即置 NeedAofLog（对 PFADD 恒置；PostCopyUpdater :1529 有 RIPROMOTE/RIRESTORE 豁免注，PFADD 不涉，勿写「无条件全命令」）。其二，原位余量不足（`InPlaceUpdaterWorker` PFADD 臂 :657-693，Update 判假 :691-692 return Failed）→ `NeedCopyUpdate` 无 PFADD case 落 :1035 default 恒放行 → CopyUpdater PFADD 臂（:1215-1301）四支 CopyUpdate 迁移**无条件执行**（:1237/:1249/:1278/:1290）、:1301 updated 唯决定输出位，新记录照常提交——尺寸源 `GetRMWModifiedFieldInfo` PFADD（`VarLenInputMethods.cs:280-287` UpdateGrow）：count=1 恒 current+128（`HyperLogLog.cs:410-426`，SparseMemorySectorSize=1<<7）、超 SparseSizeMaxCap 升稠密 12304（§108 在册值），STRLEN/DUMP 物理形可观测变长；PostCopyUpdater（:1518-1530）:1521 无条件 IncrementVersion、:1529 置 NeedAofLog。其三，真 Redis hll.c 对照：hllSparseSet 命中既存不小于新值的非零操作码即 NO_UPDATE 早退，无「容量不足即升稠密」之说，键不重写、不 dirty、不传播——C# 拷贝臂与之相悖。

Rust 现态（三闸同源静默，行为面自洽无缺陷）：折叠臂内核 `hll_add_payload` 扩容支 :181-184 `updated.then_some(grown)` 无变更即弃新载荷（`wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:176-187`）、快臂 `hyper_log_log_add` :505-523 None 不触 store 直答 :0、慢臂 `slow_hll_add` :306-315 同款；写回、WATCH 版本、AOF 镜像三处全静默（`aof_processor_store_ops.rs:94-98`「RMW 终值写回条目随真写走＋upsert_raw+bump_watch_version 单点禁第二机制」注在位）。应答帧逐字节全等（:0），分叉唯 C# 侧多写，rust 更贴 Redis 侧。

危害面（纯治理面）：一、双侧对拍夹具在「craft 满容量稀疏键或冷化键＋重复元素 PFADD」形上 STRLEN/DUMP 必红、WATCH 形（另一连接 WATCH k → 本连接 PFADD 重复元素 → EXEC）C# 中止 rust 排队必发散、AOF 条目量分叉；二、后审席误判转写缺陷按 C# 拷贝臂补齐空推进/空迁移写回——补回即把 §17 同谱随记录可变性双结局与 :0 臂空推进空镜像接进 rust，触 review.md「版本与观察者栅栏」真写纪律；三、todo/zcode-r137c-hllsec2 判净段一「寄存器无变更更新两侧均零落笔」表述覆盖面仅钉 C# InPlace 记录字节不变面，其版本推进与拷贝臂重写三面相反，后席勿据该句误判 C# 全臂同 rust（其缓存结论不受损）。

轴别与并册：§123 系 PERSIST-TTL 旁路轨（UnifiedStore/RMWMethods），本条锚 MainStore/RMWMethods 字符串记录轨并另携拷贝臂迁移重写面，独立成条互指（§123 划界段尾已补本条前指）；§17 钉 PF 原位臂 RemoveExpiration 清 TTL 面，本条钉写回/版本/镜像面，同臂不同副作用勿并条勿混引；todo/zcode-r151c-pfconv 案一（折叠分配字节界越界 panic）、案二（变更时折叠迁移形与升稠密切换字节界）各管一轴，本条专掌「无寄存器变更时写不写/推不推/落不落」，单元素重复 PFADD 即复现。

后果与严禁回改：**严禁按 C# 补齐空推进/空迁移写回**（旁路写驱动 journal 单机制外另起零写落笔口，回改才是真回归）；**严禁据 C# 拷贝臂形判 rust 变更时折叠迁移形为缺陷**（归 r151c 案二射程）。对拍轮遇该形直引本条判有意偏差并跳过，勿重复提报。锚注：`hll_add_payload` 头注补 C# 对照行回指本条；锁面 `wnode/tests/hyperloglog.rs:pfadd_grow_branch_duplicates_answer_zero_without_writeback`（:1176）三闸双臂（:0 应答、载荷字节不变、版本不推、快慢双臂）已全钉零新增断言，用例注释补 C# 对照事实注（纯台账）。

## 133. GET 族读漏斗「已过期未清退对象键」过期先于判型恒缺失形（C# MainStore Reader 判型先行回 -WRONGTYPE 且其 UnifiedStore Reader 反序；防回改登记，方向裁 rust 侧）

工单 task/ing/zcode-r145c-getrange2.md 立案登记（甄别 r146c-tr-getrange2 席双侧锚亲验坐实，定级 P4 登记级维持；纯登记＋两处锚回指注零行为改动）。编号顺编注记：本条按落册时现册册尾顺编取 §133，先入库者得号、撞号不覆写。

C# 一手形态（原型自身两读臂门序相反）：MainStore Reader（`MainStore/ReadMethods.cs:16`）:31 ValueIsObject→WrongType 先行、:37 CheckExpiry 后至——对象键过期刻度与值同记录一体（`UnifiedStore/RMWMethods.cs:120-146`/:194 惰性过期臂），已过期未清退对象键永不至 :37，GETRANGE/GET/STRLEN 回 -WRONGTYPE，至主动扫描清退（`ArrayKeyIterationFunctions.cs:219-221`）后收敛缺失形；C# 自家 UnifiedStore Reader（`UnifiedStore/ReadMethods.cs:22`）却 CheckExpiry 先于判型——原型内部不一致。真 Redis 语义过期即键不存在，判缺失不回 WRONGTYPE。

Rust 现态（有承重件的既定架构）：GET/GET_SG/MGET/GETRANGE/STRLEN/GETEX 读臂同挂 read_user 双通道漏斗（`user_read.rs` 头注自证）：快臂 `read_adjudicated_user_sync_with_prefix`（`ttl_sync.rs:332` 起）三域 String→信封→Meta 共用域内 TTL 门先行（`read_adjudicated_tag_sync_with_prefix` :240-246 Due→NotFound、信封闭包不执行）；慢臂 `read_user_with_prefix`（`storage_session.rs:400-428`）同序。已过期未清退对象键恒判 Missing——GET nil / GETRANGE 空 bulk（`get.rs`）/STRLEN :0；未过期对象键两侧同判 WrongType（同向）。承重件：`ttl_sync.rs:374-381` 存活探针同栈口径（过期视缺、残留待惰性清退/GC）、todo/zcode-r141c-msetbig 案一「闩窗内探针=唯一 NX 判据」修复处方承重锚、`ttl_rmw_semantics.rs:784`「过期对象键 HSET 按缺失重建」锁面。

危害面：无数据面危害——两侧至清退点收敛、窗内 rust 对齐真 Redis 过期即缺席。治理面：未登记则对拍遇「对象键已过期未清退 × GET 族」恒红（rust nil/空 bulk/0 vs C# -WRONGTYPE，清退后复同）无据可引；后席按 MainStore Reader :31 先判型形「对齐」即把原型内部不一致与过期幽灵 WRONGTYPE 引入本仓，并击穿 msetbig/ttl_rmw_semantics 在册判据，属真回归。

后果与严禁回改：**严禁按 MainStore/ReadMethods.cs:31 序回改信封/Meta 探针门序**（本仓采 UnifiedStore Reader :22 同侧）。对拍轮遇该窗用例直引本条跳过，勿重复提报。锚回指：`ttl_sync.rs:read_adjudicated_user_sync_with_prefix` 头注与 `user_read.rs:read_user_sync` 漏斗注各补一行刻意采序注回指本条。锁面：`wnode/tests/expired_object_key_get_funnel_missing_shape.rs`（SADD+put_ttl_sync 过去刻度→GET nil/GETRANGE 空 bulk/STRLEN :0，快慢双臂逐字节全等，C# -WRONGTYPE 对照注释备查，仿 §113 夹具族对照形态注释形制）。

写侧补注（票 zcode-r147c-incrovf 案一并册，不另立重号）：同一「过期先于判型」裁决在 RMW 写漏斗的镜像面——C# `MainStore/RMWMethods.cs:388-393` InPlaceUpdater 对对象键 ValueIsObject 判型先行回 -WRONGTYPE，且 C# 单记录一体架构下过期刻度与值同记录，判型先行不产生残域；rust 多域物理布局（String/信封/Meta/TTL 旁域分离）下写臂若先行判型即与读漏斗 §133 分叉。本仓裁**rust 侧按缺失重建**（`rmw.rs` 同步臂 Due→SET 同步内核 `try_upsert_tag_sync_unprotected_with_prefix`、异步臂 `upsert_rmw` 无存活 TTL 支→`upsert_tag`），重建即经 SET 内核单源清退信封/Meta/TTL 旁域与分层树文件，**严禁**在 RMW 臂内另立第二套裸清退或按 C# :388 序回改判型先行。锁面：`wnode/tests/rmw_rebuild_side_domain_retire.rs`。

## 134. ACL #/! 口令哈希携空白 C# byte.Parse HexNumber 收受、rust hex_decode 精确拒收（.NET 解析器怪癖族第三形，严禁补容忍）

工单 task/ing/zcode-r157c-aclsetuser.md 立案登记（甄别席双侧现码亲验坐实，定级 P4 登记级维持；纯登记＋头注＋锁测零行为改动）。编号顺编注记：本条按落册时现册册尾顺编取 §134，先入库者得号、撞号不覆写。系谱命名承甄别席订正：前两形 §105/§106 系 Enum.TryParse 怪癖，本形系 byte.Parse HexNumber 空白位（数字回退非空白形归 §106），族题统一「.NET 解析器怪癖族」。

C# 一手形态：`ACLPasswordFromHash`（`garnet/libs/server/ACL/ACLPassword.cs:47-70`）长度 64 门前置、逐二字切片 `byte.Parse(s, NumberStyles.HexNumber, CultureInfo.InvariantCulture)`。旗组成订正（以 .NET 官方文档为准）：HexNumber=515=AllowLeadingWhite|AllowTrailingWhite|AllowHexSpecifier——票面「不含 AllowTrailingWhite」系旗组成误记，尾随空白同许可；本条登记钉形取「每二字切片首位空白」可达形（如「# 0 1 0 2 …」型 32 组「<空白><hex>」64 字符 bulk 串单参数，参数内空白不经分词直达 '#'/'!' 臂，ACLParser.cs:178-197 原样传 op.Substring(1) 无 trim），不穷举全收受域。该形 C# 解析成功产出每字节恒 <0x10 的 32 字节哈希，SETUSER 回 +OK 落账（ACL LIST 回显 #0… 形小写 hex）；非 hex 字符或非空白位仍抛 correct format。NetworkAclSetUser parseState.GetString 逐条直入 ops 不经修剪（与 §105 同链同证）。

Rust 现态：`from_hash`（`acl_password.rs:41-54`）长度门后交 `wbase::hex::hex_decode`（`hex.rs:108-127`）逐位精确查表，空白 hex_val 必 None 整串拒，Err(Password) 文案与 C# 逐字同（"Unable to parse input password hash. The input is not of the correct format."）；外层唯一处理 ascii_sanitize（`acl_commands.rs:347`）仅折非 ASCII 不修剪、无外层消形。同输入 C# 收 +OK 落账、rust 拒错误帧零残留。

危害面：无运行时崩溃、无认证面 loosening（rust 更严，C# 收出字节全 <0x10 无实用 preimage，无鉴权旁路）；纯治理面——对拍轮该形必现双侧分叉，未登记误判转写缺陷；按「对齐原型」回改引入 hex_decode 空白容忍即破坏 wbase::hex 一处定义的单点折叠与存储哈希真实性。

复验点注记：主代理入册前 dotnet fsi 亲验点（byte.Parse(" 0", NumberStyles.HexNumber, InvariantCulture) 应收受）因执行环境无 dotnet 未跑，落账依据=.NET 官方文档 NumberStyles 旗组成表（HexNumber=515）+ ACLPassword.cs 现码解析链；复验点保留，后续有 dotnet 环境时一行补验即可，不阻本条效力。

后果与严禁回改：**严禁改用带样式数字解析或补空白容忍对齐 C#**（破坏单点折叠，回改即引入哈希真实性缺口）；**严禁据此判 rust 拒收为缺陷**。对拍轮遇「SETUSER 携空白位十六进制哈希口令」用例直引本条判有意偏差并跳过，勿重复提报。锚注：`acl_password.rs:from_hash` 头注补有意分叉注回指本条。锁面：`wacl/tests/acl_hash_whitespace_no_trim_locks.rs` 三用例（from_hash Err(Password) 文案逐字、apply 链 Err(Parsing) 且口令集零变更配纯 hex 大写正对照、! 删臂同形）；勿触碰在途 acl_namespace_admin_tests.rs。


## 135. PFADD 批量折叠终值分配形与升稠密切换字节界（不对齐 C# 逐元素原位/拷贝交错步——折叠单臂自洽，§17/§18/§108/r147c 案二同谱）

工单 task/ing/zcode-r151c-pfconv.md 案二立案登记，案一修复后新形同写入本条为唯一法定形（甄别席 r152c-tr-pfconv 双侧现码亲验坐实，定级 P3 登记级维持；登记＋模块头注限定句＋STRLEN 形锁，零额外行为改动）。编号顺编注记：本条按落册时现册册尾 §134 顺编取 §135，先入库者得号、撞号不覆写。

背景（案一实害面，修复已入库）：C# 命令路径恒逐元素驱动存储算子（`HyperLogLogCommands.cs:18-60` 循环内 :34 `parseState.Slice(i, 1)` 单发 RMW），稀疏尾移写峰 1B 峰值裕度在 count=1 下被 `SparseInitialLength`/`CanGrowInPlace`/`UpdateGrow`（`HyperLogLog.cs:370-379/:405/:410-426`）三分配公式结构性恒满足；rust 转写把 PFADD 折成单发批量（`hyper_log_log_add`/`slow_hll_add` 一次并入全元素），同式公式以 count=N 调用即破界——初始式不含 init_sparse 128B 零段基座、扇区取整于 2N 恰整（N 为 64 倍数）时不含 1B 裕度，命令面可达 slice 越界写 panic（违生产路径红线）。修复收口：以 `CanGrowInPlace` 的 strict-< 界（代数恒等于峰值 1B 裕度）升格为唯一稀疏容纳谓词 `whyperlog::HyperLogLog::sparse_fits`，三分配点同源消费——建键以零段基座为 current 按扇区上探、越 4096 升稠密；`update_grow`/`merge_grow` 出形及等长原位形复检、不足升稠密。`sparse_initial_length` 兼 `frame.rs` 合法下限之职保持 C# 镜像原式不动。

修复后法定形（本条登记正身）：`PFADD k e1..eN` 稀疏轨道的分配长度轨迹为折叠单发形——新建键 = 初始扇区形按容纳谓词扇区上探（如 N=100 → 402，N≤63 维持 274 零漂移）；既有稀疏键扩容 = `current + roundup128(2N)` 过谓词则该形、`roundup128(2N)==2N` 恰缺裕度形直接升稠密 12304（如 N=64 批）；升稠密切换点随之成为「current + roundup128(2N) ≥ 4096」或谓词复检失败，而非 C# 的「current + 128 ≥ 4096」逐元素触发点。C# 对位形是逐元素原位消费（`CanGrowInPlace` 过则分配不动）与 +128 扇区拷贝增长（`UpdateGrow` count=1）随记录可变性交错的轨迹，磁盘驻记录另走 CopyUpdater 冷形——同一命令终态 STRLEN 随元素到达序与记录可变性漂移（§17/§18「结局随记录可变性漂移」同谱；r147c 案二已在 PFMERGE dest 轨登记其谱系一支，本条钉 PFADD 轨，同谱不同形）。分叉实形（最大分叉窗例）：crafted 合法稀疏键 current=3800/alloc=3800（rle=payload 等号过 `IsValidHLLLength`，其校验含等号不查写裕度，双侧逐字节同形）上 `PFADD k <127 个互异新元>`——rust 折叠单发 update_grow=3800+256=4056<4096 且过容纳谓词，终形稀疏 4056；C# 逐元素原位/+128 拷贝交错至第 ~127 元方越 4096 升稠密，终态 12304——STRLEN/GETRANGE/DUMP 逐字节分叉。批规模 100 元时双侧终态恒稀疏（C# ~4054 vs rust 4056 仅 2B 差），窗内逐元素轨迹漂移同谱不另立。新建键轨同理：rust 分配 = 折叠形 + 峰值裕度 + 128B 零段基座（唯一法定形），C# 恒 274 起步逐元素推进——模块头注「写回一律落完整分配载荷」宣言的 C# 对标限定为本条钉死：口径对齐（分配长度全量落盘），形不对齐（分配长度数值随折叠粒度）。

危害面：零运行时数据危害——PFCOUNT 寄存器终值两侧恒同（max 择大幂等，r147c 判净段五与 §108 累加器在册），分叉唯物理长度形。危害全落治理面：不登记则双侧对拍夹具在「既有稀疏键大批量 PFADD」与「新建键 >63 元素」两形必红；后审席可能误判转写缺陷「按 C# 逐元素步修回」——修回即把 §17 已钉死的随记录可变性双结局接进 rust 写侧（重蹈 r147c 案二防回改面），或据 C# 冷臂形判本修复新形为缺陷再立错案。同 §16/§108/§113 修复型分叉家族先例；与 §132 划界：§132 钉「无寄存器变更时写不写/推不推/落不落」，本条钉「发生变更时折叠写回出什么形」，两轴互斥。

后果与严禁回改：**严禁按 C# 逐元素步改回**（折叠单臂自洽是本仓批量接口单次折叠机制的既定架构，改回即接入随记录可变性双结局）；**严禁据 C# 冷臂/逐元素轨迹形判 rust 折叠分配形为缺陷**（修复后形 = 折叠形 + 峰值裕度 + 128B 基座，为唯一法定形）。对拍轮遇上述两形用例直引本条判有意偏差并跳过，勿重复提报。锚注：`hyper_log_log_commands.rs` 模块头注限定句与 `hll_init_payload`/`update_grow`/`merge_grow` 头注各回指本条。锁面：`wnode/tests/hyperloglog.rs` STRLEN 形锁两用例（新建键 100 元素断折叠形 402、C# 恒 274 起步对照注；crafted 3800/3800 键 +127 元断稀疏 4056 与 PFCOUNT 同值、C# 12304 漂移事实注）与案一收口锁（100 元建键快慢双臂、N=64 等界扩容升稠密、crafted 等号合法源 PFMERGE 终形合法）；`whyperlog/tests/sparse_peak_margin.rs` 容纳界代数与对抗形。


甄别附注（r164c-sparsefit 席，随驳回收口）：本条 2N 界的 val>32 双字节操作码疑窗已全锚甄别判净——garnet 稀疏非零码单字节 7 位全值域（cs:636/639 ↔ sparse.rs:298-308），blen≤3 恒成立、尾移峰 E+2 与值大小无关，sparse_fits strict-< 系紧确充要界，Δ=3 不可达；后席勿再按 redis 原生 XVALUE 双字节逃逸形报疑。
## 136. 集合族双态读应答成员序非契约（帧头与成员集合等价口径登记；跨态逐字节全等不可达，严禁出帧点加排序凑字面达标）

工单 task/ing/zcode-r137c-setstore2.md 立案一登记（甄别 r137丙/r138丙 双席现码锚全中零幻觉复验坐实，定级 P3 登记级：不改码不加排序，只落口径单源）。编号顺编注记：本条初次落册按当时册尾实况顺编取 §135；合入前复跑 `git merge dev` 时 dev 已入 PFADD 折叠分配形条先占 §135——撞号让位改取 §136，collection.md §5、`tiered_output_frame_head.rs`/`resp_set.rs` 回指锚同步改号，撞号不覆写。

口径背景：Redis 协议 set 型应答成员序非契约；C# 原型 `SetObjectImpl.cs:SetMembers` 与 `SetCommands.cs` 算术臂（:252-258 foreach result 直写）序随 .NET HashSet 布局，集合无分层态，双侧皆无成员序契约。本仓自设强契约旧文见 `doc/zh/collection.md` §5 第 4 条「分层态与内存态对同一成员集回逐字节全等的应答」，而同文 §8.5 对 SRANDMEMBER 已自认「集合无序契约下语义等价」，口径未贯一；池内现行实测口径实为集合等价（`tiered_output_frame_head.rs` 跨态夹具 `memory_and_tiered_replies_agree_on_head_and_member_set` 先 sort 再比）。

rust 现态（双态成员序源不同构且不可全等，三锚）：信封态 SMEMBERS 按 gxhash 布局序直写（`wedb/wcol/src/set/set_object_impl.rs:43` `set_members`），分层态 SMEMBERS 按树扫描序流式直写（`wedb/wnode/src/resp/objects/tiered_collection_ops/set.rs:137` `SetOperation::Smembers` 臂），算术读臂经折叠裸集布局序出帧（`set_commands/mod.rs` `write_set_members` 借用迭代直写）；`wbase` HashSet 进程级随机种子（`wedb/wbase/src/map.rs:17-27`，既定防碰撞 DoS 改良，票面明令禁按缺陷报）致跨重启序漂移。三源在集合族不可同时收敛为逐字节全等，属架构固有而非转写分叉。

裁决=豁免登记（非改码）：集合型读应答（SMEMBERS/SINTER/SUNION/SDIFF）双态契约口径统一为**帧头（类型与条数）与成员集合等价，成员序非契约**；collection.md §5 第 4 条措辞同步限缩。唯一备选「双出帧点统一树序」须改信封臂为按树键序生成，属改码案，本席审核无反裁决故弃；全量排序方案违 review.md §3.1 热路径零开销与 §6 大键 O(1) 规约，以红线为反证驳回。

后果与严禁回改：后续对拍/审查席遇「双态成员序字节分叉」用例直引本条判非契约并跳过，**严禁按 collection.md 旧「逐字节全等」文义将无序族误判成缺陷**，更**严禁在出帧点（set_object_impl.rs set_members / write_set_members / 分层 Smembers 臂）加全量排序以字面达标**（排序即引入热路径 O(n log n) 真回归）。§113 尾注「同步/冷双臂应答逐字节全等」措辞已限定于独占错误帧与 STORE 零副作用面（成功路径成员序引本条）。锁面：`wedb/wnode/tests/tiered_output_frame_head.rs` 跨态 sort 夹具（头注已改引本条登记口径）；`wedb/wnode/src/resp/objects/set_commands/mod.rs` `write_set_members_tests` 单元族改帧头+成员集合等价断言；:228 `smembers_tiered_reply_is_byte_identical_to_reference` 系分层臂对同源参照树序成帧锁（非跨态字节比），维持不动。本票零行为改动（案二代码面另记票内执行方案，出帧克隆清退不触成员序契约面）。

## 137. 集群对外渲染面非 ASCII 宣告值收口为 wbase::ascii_sanitize 逐字节 '?' 折叠（对 C# Encoding.ASCII 系值域收拢非逐字节等形；防回改＋异形注记登记）

工单 task/done/zcode-r139c-cluenc.md 收口登记（采票面方案 2：值域收拢而非纯登记；编号顺编注记：本条初次落册按当时册尾实况顺编取 §135，合入前复跑 `git merge dev` 时 dev 已入 PFADD 折叠分配形条与集合成员序条先占 §135/§136——撞号让位改取 §137，撞号不覆写）。

分叉原貌与 C# 一手形态：C# 集群渲染链全程经 `Encoding.ASCII` 落字节——CLUSTER NODES 出口 `WriteAsciiLargeRespString`、SLOTS 出口 `TryWriteAsciiDirect`（`garnet/libs/common/RespWriteUtils.cs:297-305`）、SHARDS 出口 `WriteLargeAsciiDirectString`（`RespClusterBasicCommands.cs:532-560`）对已组整帧做 ASCII 编码，>0x7F **逐 UTF-16 字符**折 `?`，帧内 `$len` 头取 `string.Length` 字符数（ClusterConfig.cs:746-752/:816/:831/:841/:848）故头体恒自洽；MOVED/ASK 在组串处 `Encoding.ASCII.GetBytes`（RespClusterSlotVerify.cs:27/:54/:61）。rust 渲染链原为 UTF-8 原样直通且 `$len` 头取字节数，同一非 ASCII 宣告主机名部署下两侧对外字节异形。

rust 收口形态：折叠单源复用 §110 既定 `wbase::ascii_sanitize`（`wbase/src/ascii.rs:11`，>0x7F **逐字节**折 `?`，纯 ASCII 零拷贝），落点为字段入帧点而非整帧出口——C# 折叠发生在帧组装之后（其头为字符数），rust 头为字节数，整帧折叠会制造头体错位畸形帧，故严禁整帧出口套折；`$len` 头随折叠后字节数计算天然自洽。收口臂清单（六渲染臂）：① CLUSTER NODES 行 address/hostname 段（serializer.rs append_node_info）；② SLOTS 三元组端点位与 hostname/ip 元数据臂（append_node_networking_info 取值点，含 append_value_or_null 全部经行者）；③ SHARDS 节点帧 ip/endpoint/hostname 三臂（append_formatted_node_info 取值点）；④⑤ MOVED/ASK 两态——唯一成帧点 `wresp::cmd_strings::cluster::write_redirect_error` 端点实参入帧前折叠（覆盖 get_endpoint_by_preferred_type Ip/Hostname 全臂，取端点处不再二次折叠，杜绝双机制）；⑥ CLUSTER REPLICAS 经 get_node_info 同源承接。入侧零改动：resolve_announce_hostname（cluster_manager.rs）配置非空直取之既有语义保持，收口纯在对外渲染值域。

刻意异形注记（甄别席订正随行）：C# UTF-16 逐字符折（"café"→`caf?`，$4）vs rust 逐字节折（"café" UTF-8 五字节→`caf??`，$5）——本收口系**值域收拢**（对外恒 ASCII、头体恒自洽），非逐字节等形；ASCII 宣告值（常态部署）双侧逐字节全等。对拍轮遇非 ASCII 宣告主机名用例按本条判形：两侧 `?` 个数可差（字符数 vs 字节数），值域与帧形结构恒等。

后果与严禁回改：**严禁**在 serializer 或错误臂另立第二套折叠机制（禁散落手写 map/replace 折环）；**严禁**在 SLOTS/SHARDS 整帧出口 `extend_from_slice` 前对已组串全帧套折（头体错位畸形帧）；**严禁**按 C# 逐字符折叠形回改 ascii_sanitize（该函数系 §110 既定全仓单机制，改动越出集群域）。锁面：`wedb/tests/cluster_resp_session.rs::cluster_render_non_ascii_hostname_folds_to_ascii`（NODES/SLOTS/SHARDS/MOVED 四形 `caf??` 折叠＋全应答 `is_ascii()` 值域钉）与 `wedb/tests/cluster_slot_verify.rs::test_redirect_frames_fold_non_ascii_hostname_endpoint`（MOVED/ASK 帧字节锁）；既有 ASCII 夹具编码锁零漂移。

## 138. lex 族空串边界 C# val[0] 裸读越界掐连形不复刻，rust 恒回 not-valid-string 帧（§5 分值族先例的 lex 五命令延伸；纯登记＋守卫自陈注＋三态空串锁，零行为改动）

工单 task/ing/zcode-r139c-zlex.md 登记（甄别双席通过：主代理现码复跑与 r140丙-tr-zlex 席，定级登记级：不改码不改行为，只补台账＋守卫臂自陈注一行＋既测族扩例；§5 分值族与 §114 宗 a 同族先例）。编号顺编注记：票内拟号 §119 系立票时旧册快照已过期（clustershrs 落 §119），本条按落笔时沙箱现册册尾 §134 顺编拟取 §135，先入库者得号、撞号不覆写。（落册实况：第一次 merge dev 复跑见同窗 pfconv 折叠分配条先落得 §135，本条让位取 §136；第二次复跑见 setstore2 集合族双态成员序条先落得 §136，本条让位取 §137；第三次复跑又见 cluenc 集群 ASCII 折叠值域条先落得 §137，本条再让位终取 §138——本条内、try_parse_lex_parameter 守卫臂自陈注与 resp_sorted_set.rs 锁测回指锚已同步 §138。）

C# 一手形态：lex 界解析单点 `TryParseLexParameter`（`garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:1165-1205`）首段 `switch (val[0])`（:1174）无长度守卫，空串边界直读即 IndexOutOfRangeException；该调用在 `GetElementsInRangeByLex` try 段（:1012）之前裸调（:987-988），`catch (ArgumentException)`（:1060-1066）救不到此形，会话层也无前置守卫（`SortedSetCommands.cs:SortedSetLengthByValue` :588-643 与 `SortedSetRange` :146-204 只查参数个数，空 bulk 合法可达；键缺失走 NOTFOUND 短路，可达前提是键既存）——遇之掐连。覆盖命令面：ZRANGEBYLEX / ZREVRANGEBYLEX / ZRANGE..BYLEX（经 SortedSetRange :531-551）与 ZLEXCOUNT / ZREMRANGEBYLEX（经 SortedSetRemoveOrCountRangeByLex :708-731）五命令同漏斗；ZRANGESTORE..BYLEX 亦汇同一漏斗随本条口径，不与已决 STORE 族案重报。

Rust 现态（守卫天然闭环、双臂四臂同源）：`try_parse_lex_parameter`（`wedb/wcol/src/zset/sorted_set_object_impl.rs`）以 `val.first()` 守卫，空串与非法首字符一律 None，`get_elements_in_range_by_lex` 折 `i32::MAX` 错误码；消费臂——内存信封 `read.rs:sorted_set_length_by_value`（result1==i32::MAX 漏斗回帧）与 `write.rs:sorted_set_remove_range` Lex 臂、`read.rs:sorted_set_range` BY_LEX 臂（对象层 operate 内 reset+回帧），慢臂 `slow.rs` Zlexcount 与 Zremrangebylex 臂同漏斗，分层树内 `tiered_collection_ops/zset.rs` Zlexcount 臂与 Zrange BYLEX 臂经 `ZLexBounds::parse` 复用同一 `try_parse_lex_parameter` 单点。四臂一律恒回 `-ERR min or max not valid string range item`（文案对 `garnet/libs/server/Resp/CmdStrings.cs:273` 逐字节等、无句点尾，`wresp/src/cmd_strings.rs:152`），不掐连、零删除、键存活。

裁决与严禁回改：崩溃不复刻、rust 降级错误帧即更优侧（复刻即触生产路径未受控异常红线），**严禁任何臂回改为复刻 C# 越界掐连**；守卫臂自陈注（`try_parse_lex_parameter` 首守卫上方）回指本条。灰盒对拍夹具遇此形必发散（C# 掐连 vs rust 错误帧），后续对拍轮直引本条判有意偏差、勿判转写缺陷、勿反复立案；后审席重构 lex 守卫臂（如把 first() 改切片匹配）须保空串 None 回帧口径，锁测兜底。§5 枚举口径「仅分值族两命令」保持不变，lex 族五命令由本条补全，两条款互指。

验证面：`wedb/wnode/tests/resp_sorted_set.rs` 两把锁——① `zlexcount_and_invalid_lex_bounds` 夹具族扩例：ZLEXCOUNT/ZREMRANGEBYLEX/ZRANGEBYLEX/ZREVRANGEBYLEX 各钉 min=""、max=""、双空三形，应答逐字节 `-ERR min or max not valid string range item`、键预 ZADD 存活态验（零删除计数不变）、错误后正常界读仍正确（会话存活，对标 `garnet/test/standalone/Garnet.test.collections/RespSortedSetTests.cs` "-ERR min or max not valid string range item\r\n+PONG\r\n" 存活帧形）；② `zlex_empty_string_bounds_three_state_byte_equal` 三态逐字节全等锁：内存信封（快臂线帧）、慢臂直驱（SlowWait::for_command）、升阶树内（promote_collection_to_bftree 后分派）三态各验，仿 §133 三态对照形制。本条各锁比较的均为固定常量错误帧与计数/PING 帧（成功路径零成员回显），与 §136 集合族成员序非契约口径、§137 集群 ASCII 折叠值域两案均不同轴互不干涉。


## 139. EXEC 重放段脚本重入锁器模式：C# 内嵌 processor 恒 basicApi ephemeral、rust 单锁源按持锁桶域会合判定让闩/自取（修复型入库；互斥面等价、自等面收紧）

工单 task/ing/wlua-multi-exec-replay-script-lockmode-inherit.md 立案登记（修复型：修「脚本重入段无条件继承 Running→Transactional 让闩，锁集外键零闩盲写丢更新」缺陷）。编号顺编注记：本条初次落册按当时册尾实况顺编取 §135，合入前复跑 `git merge dev` 时 dev 已入 PFADD 折叠分配形条、集合成员序条与集群 ascii_sanitize 条先占 §135/§136/§137——撞号让位改取 §138；合入前二次复跑 `git merge dev` 又见 zlex 空串越界登记条先落得 §138，本条再让位终取 §139，代码回指锚同步改号，撞号不覆写。

C# 一手形态：MULTI 排队 EVAL 后 EXEC 重放，脚本内 redis.call 落在**内嵌 processor**——`SessionScriptCache.cs:54-70` 构造独立 `RespServerSession`（`RespServerSession.cs:253-309`），其 `txnManager.state` 恒 None，ProcessMessages 派发恒选 `basicApi`：脚本内一切键（无论外层事务是否已锁同桶）均走 `BasicSessionLocker` ephemeral 自取闩，与他连接在同一 `store.LockTable` 锁内存上互斥——无丢写；代价是外层事务已持同桶排他闩时的**自撞**：内嵌同线程 ephemeral 取闩恒失败回 RETRY_LATER 原地重试（非重入桶闩自等），该面 C# 未做让闩判据。

Rust 现态（本条修复后形态）：无内嵌 processor，`lua.rs:dispatch_resp` 重入共享会话、事务镜像滞留 Running，选型点 wnode `garnet_api::exec` 由「Running 恒 Transactional」改为「Running **且**本命令键窗全落本事务持锁桶域才 Transactional」：域内让闩复用已持闩（杜绝 C# 自撞面），域外落 Basic 自取闩（与 C# basicApi 同款经同一份 windex 桶闩互斥，杜绝零闩盲写）。判定单点 `RespServerSession::txn_locks_cover_cmd`（wnode txn.rs）：键窗提取与排队落键段 `txn_queued_command_info` 同源（normalize_for_acls→同目录命令信息→`extract_keys_from_slice` 单点扫描内核），哈希经 `TxnKeyEntryComparison::scoped_key_hash` 全仓唯一构造口，桶判定按钉定索引版本 `bucket_index_for_hash` 与 `held` 升序持锁桶序列二分（`TxnKeyEntries::covers_user_keys`，wtxn）；零新增模式位、零第二锁源、判据点全仓唯一。按桶覆盖与 C# 桶粒度锁语义一致；混合桶多键窗部分在域外时 Basic 自取与已持同桶闩相撞，同步臂 1024 自旋/异步臂 1024 让核预算耗尽回 LockTimeout 错误帧（不挂死，不差于 C# 自撞原地重试）。

危害面：无数据面危害——域外键互斥面与 C# 等价（同锁内存），域内键 rust 消除 C# 自撞面属严格改进；纯治理面——未登记则对拍遇「MULTI+EXEC 携 EVAL，脚本触碰外层事务未锁键 × 他连接同键并发」时 rust（修复前丢写）/ C#（互斥正确）分叉无据可引，后席按「C# 恒 Basic」形把锁集内键自撞风险引入或对无第二锁源纪律回改。

后果与严禁回改：**严禁回改选型点为 Running 无条件下传**（丢更新即该形）；**严禁在 wkv 窗口侧加第二模式位/散落哨兵**（判据单点在分派选型处）；判定哈希**严禁另起手**（必须经 scoped_key_hash 单点，与在途票 wtxn-wkv-keybucket-hash-scope-desync 的桶基统一改造保持先后自洽）。对拍轮遇「MULTI;EVAL 脚本未声明键并发同键写」用例直引本条。锚回指：`garnet_api/mod.rs:exec` 选型点段注、`wkv/src/session/rmw_window.rs` 模块头选型段、`wtxn/src/txn_key_entry.rs:covers_user_keys` 头注各回指本条。锁面：`wnode/tests/multi_exec_script_reentry_lock_mode.rs`（未声明键双消费者同键写无丢写压测＋锁集内声明键让闩不挂死回归＋混触键形串行精确应答）。

## 140. 换引擎钩清单：wkv 引擎实例级三件 OnceLock 钩全枚举 + VectorManager 内存簿记为第四件引擎绑定面，副本 disk-based 收口双臂（C# 原位恢复形 N.A. 之置换形自有簿记；修复+登记并载条）

工单 task/ing/zcode-r137c-snaplock2.md 两宗落地登记（甄别：宗一 P2 钩束漏挂第三臂、宗二 P1 副本换引擎后向量登记永不回建）。编号顺编注记：票面无预拟号，本条按落册时现册册尾顺编取 §132，先入库者得号、撞号不覆写；合入 dev 时册尾 §132–§134 已被 PFADD/GET 族/ACL 三条先占，本条按同规让位改号 §135；合入 dev 时 §135 又被 r151c-pfconv 条先占，二次让位定号 §136；本条合入排队期间 §136 又被 r149c-flushsnap 条先占，三次让位定号 §137；合入排队期间 §137 又被 ascii_sanitize 条先占、§138 又被 lex 族条先占，四次让位定号 §139；合入排队期间 §139 又被 EXEC 重放锁模式条先占，五次让位定号 §140（deviations.md 合并各侧条目全保留）。

C# 一手形态：副本全量为**原位恢复**（`garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs`:TryReplicaDiskbasedRecovery → `StoreWrapper.cs`:RecoverAsync 原位重构），watch 版本表、AOF 门面与 functionsState 接线随引擎同实例跨恢复全程存活，无「换引擎重挂」面；删除缺席观测与向量登记记录驻主存表内、整表随 store 重置天然连续，故副本侧**无回建臂**（该文件仅 Pause/ResumeCleanup 清理闸面，RecoverVectorSets 只在初始化与 --recover 两臂点亮，`SingleDatabaseManager.cs:406`）。

Rust 实例置换形态钩清单（本票统一为单套机制、全链枚举）：
1. **引擎实例级 OnceLock 三件**——全集枚举由 `wkv::EngineHookSlots`（watch_hook/event_sink/delete_miss_hook）承载，宿主钩子束 `wnode/src/service.rs:engine_swap_hook_bundle` 在投槽前对换入引擎逐件重挂（修复前束仅挂两件，第三件 delete_miss_hook 唯一注入口是逐连接 get_session 装饰，换引擎后至首客户端会话前回放删/紧缩丢臂脱钩，嵌入式宿主永不收敛）。治理臂：wkv 新增引擎实例级 OnceLock 钩必同步扩该枚举与钩子束，锁测 `wedb/tests/engine_swap_hook_bundle.rs` 以全结构字面量逐字段断言，漏挂即编译红。
2. **第四件＝VectorManager 内存簿记面**（引擎实例无关的共享单例，C# 无对物）：`key_index_registry` 镜像与专用会话工厂各有引擎绑定肢，本票三臂收口——①副本 disk-based 收口尾段（`replica_diskbased_sync.rs`，end_recovery 后、CleanupPauseGuard 释放前）经启动面同口 `recover_vector_sets` 回建，与无盘臂 `frame_import` 内存面同步投喂双臂形制统一，回建失败即本轮全量收口失败拒授予位点（与启动面恢复失败拒启同口径）；②回建口残影清退臂（`vector_registry_recovery.rs`:rebuild_registry_from_store 段 2 尾）令镜像恒等于日志回建集，指向已弃置物理实例的旧镜像条目经既有删除单点同步摘除（启动形态镜像恒空零成本，杜绝异步清理竞窗）；③专用会话工厂自 node_components 移至 from_parts 尾段挂 `StoreSwapSlot` 现取现用（修复前钉死装配期引擎——换引擎后回建/清理写透落户弃置实例，共享设备半写即毁导入日志；与向量 WATCH 推进臂、事务锁表同源模式）。

后果与严禁回改：登记表镜像「==日志回建集」为置换后内存面唯一真值源，严禁把工厂装配改回装配期实例钉死、严禁把残影清退出回建单点改挂异步清理；后续轮遇「副本换引擎后向量集不显形」「旧集镜像幽灵」「换引擎后写透落错引擎」类疑报直引本条。回锚邻缝：§99（复制快照装载/读值域钉，同 replication 面）、§115（向量登记写面 bump 换算单点，同 vector_registry_recovery 文件）。锁面：`wedb/tests/engine_swap_hook_bundle.rs::swap_in_engine_gets_full_hook_bundle_and_delete_miss_observed`（全枚举换面 + 换入引擎缺席删除观测）、`wedb/tests/replica_diskbased_vector_rebuild.rs::diskbased_full_sync_rebuilds_vector_registry_and_prunes_stale_mirror`（真帧全量收口后主端向量集登记回建、VCARD 元素可达、副本旧集镜像清零），两测无 sleep 无环境门。


## 141. SCAN 多 TYPE 词元末值覆盖对位 C# 直赋形（type_unknown 粘滞缺陷已修复归全等；§20 b 互引纯登记，本面零残留偏差）

工单 task/ing/zcode-r161c-scantype.md 收口登记（编号顺编注记：本条初次落册按当时册尾实况顺编取 §140，先入库者得号、撞号不覆写；合入前复跑 `git merge dev` 见换引擎钩清单条先落得 §140，本条让位终取 §141，§20 b 回指锚同步改号）。

分叉原貌（修复前）：rust `parse_scan_filter`（`wnode/src/resp/array_commands.rs`）TYPE else 臂将 `type_unknown` 置 true 后从不复位，慢路径 `C::Scan` 臂（`wnode/src/resp/garnet_api/slow.rs`）早退判先于 count/type_filter 消费——`SCAN 0 TYPE stream TYPE hash` 回空列表+游标 0；C# `NetworkSCAN` 的 `typeParameterValue` 为局部 `ReadOnlySpan` 直赋整体覆盖（`ArrayCommands.cs:305-311`），`DbScan`（`ArrayKeyIterationFunctions.cs:57-86`）仅依末值判型，同参数正常按 Hash 过滤。属转写疏漏非刻意取舍（修复前无注释无登记无测例）。

收口形态：TYPE 分支每词元解析前置清 `type_unknown` 一行（判型只取末次词元，末值合法覆清、末值非法由 else 臂照常置位），零新机制零新字段，与 C# 末值直赋形全等；`slow.rs` 早退与 count/type_filter 消费序不动（粘滞源头消除后自然收敛）。

边界互引：§20 b 空串 TYPE 归 `type_unknown` 在册裁决在本面**不被动**——多词元形下空串以末值位出现时（如 `TYPE stream TYPE ""`）仍按 §20 b 回空集，严禁按 C# `IsEmpty` 透传形回改；两条互指，§20 b 裁「空串作末值时的裁决」，本条裁「判型取哪个词元的值」。

后果与严禁回改：本面修复后与 C# 行为全等（末值合法）或按 §20 b 收口（末值空串），对拍轮遇多 TYPE 用例直引本条；**严禁**将清位行移至 TYPE 分支外或仅合法臂置 false 的第二机制（粘滞回潮形）。锁面：`wnode/src/resp/array_commands.rs` tests `parse_scan_filter_duplicate_type_last_valid_overrides`（末值合法覆清/末值非法照常置位/空串末值形三锚）与 `wnode/tests/scan_multitype_lastvalue.rs`（SCAN 0 TYPE stream TYPE hash 全表非空帧锁、反序末值非法空帧、单 TYPE 逐字节全等、TYPE 缺省回归）。

## 142. ZADD 数据段中段非浮点错 rust 三臂全退 vs C# 部分提交与缺键建键（原子性收口的刻意偏离，禁按 C# 形回改；括注 zset/hash 面成员级 TTL 剔除固化不对称裁决）

工单 task/ing/zcode-r141c-zaddopt.md 案一登记（纯登记＋注释勘误＋双态数据侧锁，零行为改动）。编号顺编注记：票面预拟 §119 系旧快照，落册时册尾实为 §140（EXEC 重放锁模式条与换引擎钩清单条先占 §139/§140），本条顺位取 §141；合入前复跑发现 r161c-scantype 条已先合入 dev 占得 §141，撞号让位终取 §142，代码回指锚同步改号，撞号不覆写。

分叉形态：对「选项段已解析后数据段内再遇非分值词形」（如 `ZADD k 1 a GT 2 b`、`ZADD k 1 m NX`，含缺键形），C# 是**部分提交**——主循环内前序合法对已就地落进常驻对象，出错误帧不回滚（`garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:121-122` 出 NOT_VALID_FLOAT 帧即 return 无回滚臂，前序对经 :134-145 新增臂/:183-192 更新臂已就地生效并标 NeedAofLog）；缺键经 `InitialUpdater` 连返回值都丢弃、无条件挂残对象建键（`garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:53` `_ = value.Operate(..)`＋:54 挂载，既有键 InPlaceUpdater 同文件 :97-103）。rust 三执行臂对该词形一律**全退**：应答字节双侧逐字一致而数据侧相反——内存臂（`wcol/src/zset/sorted_set_object_impl.rs:338` 出帧点，出帧前 :360-375/:412-422 确已改本地 obj 副本）与冷键异步臂（`sorted_set_commands/slow.rs:70` 同源复用家族唯一回写门）由 `sorted_set_commands/mod.rs:117` `should_write_back` 的载荷首字节 `-` 先于 match 各臂短路整体拒写（信封不覆写、AOF 与复制增量条目同不发，`rmw_helpers.rs:1102` 判写门为 notify 唯一前置）；分层臂以预扫在任何树写之前早退（`tiered_collection_ops/zset.rs:142-145` 中段非分值 `RESP_ERR_NOT_VALID_FLOAT` 早退臂），主循环保留的 NOT_VALID_FLOAT 臂（:197-201）在该词形下为纯防御死码。三臂外象收敛，rust 内部无矛盾，双态自洽。

裁决与严禁回改：属原子性收口的**刻意偏离**，与分层 RI 批量预检「任一成员越契约即整体失败、零树内副作用」同口径（`tiered_collection_ops/zset.rs` 预校验注自陈），也直接承 §19 追加澄记与 §47a 反幻键既定裁决——按 C# InitialUpdater 形回改即复活空/残对象幻键，并把 `should_write_back` 单门拆成「按错臂分类判定」的双机制，两项皆触红线，**禁按 C# 形回改**。未登则后席或双侧对账者按 C# 行实（先改后判、错即留痕）读 rust 必判转写缺陷。`sorted_set_commands/mod.rs` `should_write_back` 头注原「错误回复无状态变更」句在本缝字面失真（前序对确已就地改 obj，拒写是刻意整体丢弃而非「无变更」），本票已改为分臂陈述，出帧点与分层死码臂同步补注。锁面：`wedb/wnode/tests/zset_r15_parity.rs::zadd_mid_option_token_parity` 双态数据侧残余断言（缺键形 EXISTS/ZCARD 恒零、既有键形 contents 零变化）＋flush_and_evict 重装载复验（错误帧不落盘持久化锁）。

括注（案二 b 支裁决，同条登记）：家族回写门 `-` 支与 hash 面对位门（`hash_commands/mod.rs` 已修 `&& !(existed && obj.mutated_by_ttl())` 豁免支，票 wnode-hgetall-envelope-ttl-purge-not-solidified）存在**固化不对称**——hash 面裁「错臂上已发生的成员级 TTL 物理剔除必随豁免支落盘固化」，zset 面本席复验同形缺口在 ZADD 三条错臂（选项互斥/中段非浮点/SCORE_NAN，入口 `delete_expired_items` 先于判定）确实可达（`-` 支短路使 `mutated_by_ttl` 升格支对错臂不可达，剔除不落盘），**裁无需固化、不改码**：理由＝zset 面装载即裁存活（`sorted_set_add` 入口 `delete_expired_items` 与 collection.md §2 双态口径），外部可观测面无分叉、无 hash 面 HEXPIRE 统计旁路差；代价＝信封继续携陈旧成员字节、每次访问重复剔除（堆序与账本），刻意不固化。防幻键第二门（`!existed` 且空对象拒写）保持原状未触。hash 面注释两处「对照 zset 面 Zcard 豁免形」互引随本条撤除，免后席按该句误判 zset 已同款。分层态不受本门辖（树内到期视同缺席，物理覆盖即先删后加），本缺口纯信封面。锁面：`wedb/wnode/tests/zset_r15_parity.rs::zadd_ttl_purge_not_solidified_memory_arm`（内存态锁现态：错误帧逐字节不变、重装载后信封仍携到期成员、访问即裁可见结果不变）与 `zadd_ttl_purge_tiered_arm_zero_impact`（分层态零影响）。ZINCRBY 族括注补全（票 zcode-r153c-zincrby 案三，b 支现文不足由彼席补此一句）：`sorted_set_increment` 的 NOT_VALID_FLOAT 词形拒臂与存活相消 SCORE_NAN 臂两条错臂均排在入口 `delete_expired_items` 剔除之后，与本条所列 ZADD 三错臂完全同形——本门 `-` 首字节短路同样令 `mutated_by_ttl` 升格支对其不可达（信封同步径与 `sorted_set_commands/slow.rs` 冷键径 `zset_rmw_cold` 同源复用该门），键挂成员级 TTL 且部分成员到期时执行 `ZINCRBY k abc m` 或 ±inf 相消形皆「先物理剔除、后出错误帧、整臂拒写」，同判无需固化不改码，且母案 a 支单点位门改落地即自动覆盖 ZINCRBY 两臂、不另建设。

## 143. 键级 TTL 粗化收口 EXPIRE 族命令边界单点：wkv `expire_at` 会话入口头部粗化闸摘除，重放/迁移/复制臂保留原 ticks 精度（修复型登记；附 GETEX epoch 乘加归 `wbase::convert` try_ 单源之案二收口）

工单 task/ing/zcode-r149c-setexabs.md 案一落地登记。编号顺编注记：票面禁预拟号，本条初次落册按当时册尾（§140）顺编取 §141，先入库者得号、撞号不覆写；合入前复跑 `git merge dev` 见 r161c-scantype 条与 r141c-zaddopt 条先落分占 §141/§142，本条让位终取 §143，全链回指锚同步改号。

C# 一手形态：键级 4-bit coarse 粗化 `(ticks >> 4) << 4` **只发生在二参打包构造器** `garnet/libs/server/ExpirationWithOption.cs`:20-24（唯 EXPIRE 族命令入口调用），是 ExpireOption 借低 4 位打包的产物；同文件 word 构造器（:30-33）恒等装载；存储侧重打包点 `libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs`:216、:228 走 word 形，**无第二次粗化**。SETEX/PSETEX/SET EX/PX/GETEX/RENAME 全程存裸全精度 ticks（`libs/server/Resp/BasicCommands.cs`:552-555 取 `DateTimeOffset.UtcNow.Ticks` 原值入 `StringInput`、`LogRecord.TrySetExpiration` 裸写）。

Rust 修复前破口：`wkv::StoreSession::expire_at` 头部另设一道 `coarse_expire_ticks`（原注释称「护住 AOF 重放/迁移导入/复制应用三个外部入口，绝对换算 ticks 天然 16 对齐故幂等恒等」——前半失实于 SETEX/GETEX/RENAME 裸 ticks 族，属第三处失实注释）。主端 SETEX/GETEX 写入的非对齐 ticks 经 TtlWrite 镜像解码直通 → Pexpireat 重放臂回写时低 4 位被会话入口闸摧毁，副本与主端存值不再逐位一致（量级 ≤15 ticks = 1.5μs 提前过期；旧注释「<16μs」为单位口径误差 10 倍）。

收口形态（判据：粗化是**值域裁决**不是存储不变量，唯命令边界一处施加）：
1. 单点保留：`wbase::convert::coarse_expire_ticks` 函数单点不变，施加面唯 wnode `network_expire` 命令边界（`keys.rs` 参数解析段尾，同步快路径与异步慢路径共用同一解析单点，对标 C# 打包构造器唯一调用面）。
2. 会话入口摘闸：`wkv::StoreSession::expire_at` 头部粗化行删除，入口与 `put_ttl`/`put_ttl_sync` 内核同为恒等裸写（对标 C# word 形恒等装载与存储侧无二次粗化）。经核全产线调用方零失护：EXPIRE 族命令值已在边界粗化；`frame_import`/迁移接收走 `expire_at_milliseconds_to_ticks` 等绝对换算（16 对齐天然恒等）；AOF 重放/`expire_in_ticks` 臂本就要求裸值直通。
3. 三处失实注释订正：`wkv/src/ttl.rs` 模块台账与会话入口注、`wnode/src/aof/aof_processor_store_ops.rs` Pexpireat 臂注（含「登记为已知微偏」悬置语删除与 1.5μs 口径改正）、`wnode/src/service.rs` TtlWrite 臂注（「逐位一致」升为全 TTL 族成立）。
4. 案二附注：GETEX EX/PX/EXAT/PXAT 的 epoch 乘加原有第三份手写形（`wnode/src/resp/basic_commands/ttl.rs` `compute_relative_expiry`/`compute_absolute_expiry` 内联乘加 + `target<0`/`exp<0` 命令不可达死臂），归并至 `wbase::convert::try_expire_after_to_ticks`/`try_expire_at_to_ticks` checked 通用形单源；命令层只留 max_val gate 与 INVALIDEXP/OVERFLOWEXP 帧分流，绝对/相对域行为逐位不变（编译期常量断言锁 gate 内不溢出；对拍锁见 `wbase/src/convert.rs` tests、`wnode/src/resp/basic_commands/ttl.rs` tests 与 `wedb/wnode/tests/getex_ttl_convert_parity.rs`）。

严禁回改：会话入口/内核再长出第二道掩码、或命令边界粗化被移入他处。锁测：`wkv/tests/ttl.rs::test_coarse_single_source_expire_at_put_ttl`（入口逐位直通反锁）、`wedb/wnode/tests/ttl_replay_bitwise_parity.rs`（真链路主副本非对齐 ticks 逐位对拍）。回锚邻缝：§4 b)（绝对面钳制登记，同换算链）。

## 144. PFMERGE dest 损坏/异型前置拒裁（不对齐 C# dest 免检 + SET_Conditional 返回值弃置的 +OK 掩盖形，修复型分叉格一至三；崩溃形系窄形非恒常）

工单 task/ing/zcode-r147c-pfmerge.md 案一登记（甄别席三锚实测全中，P3 登记级：不改码不改行为，只补台账 + dest 臂注释锚 ×2 + 锁测三用例；真 Redis pfmergeCommand 对 dest 先行 checkType+hllSparseToDense 失败即 WRONGTYPE 零合并，rust 恒取 Redis 更严侧）。编号顺编注记：票面拟号 §119 系旧册势（§119/§120 已遭 zlex/zaddopt/evaldict/cluenc 诸席争占预占，review-plan :1508 明令禁预钉号），按票订正口径落册时现册尾 §142 顺编拟取 §143；合入前复跑核尾见键级 TTL 粗化条与门禁脚本面条双占 §143——撞号让位不覆写，终取 §144（本条内与代码/锁测回指锚已同步 §144）。

C# 一手形态（帧级 + dest 终态双分叉链）：`HyperLogLogOps.HyperLogLogMerge`（`garnet/libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:191-272`）全程不读 dest——事务仅 dest 独占锁 + 逐源共享锁，源循环逐源 GET→NOTFOUND continue→WRONGTYPE return→读缓冲 -1 哨兵判载荷非法 error break→合法源 `SET_Conditional(dstKey)`（:262 **返回值弃置**）。dest 合法性唯在存储层 RMW 内碰触且三臂后果各异、对上层全部不可见：a) 热形 InPlaceUpdater PFMERGE 臂（RMWMethods.cs:695-728）`IsValidHYLL(dest)` 败 → `*output=0xFF`（:727）→ NotUpdated → 派发器 :431-433 default 分支 return true（就地完结、dest 原样、状态非 WrongType 非 NotFound）；b) 磁盘驻形 NeedCopyUpdate 无 PFMERGE case 落 :1035 default 放行 → CopyUpdater PFMERGE 臂（:1303-1350，票注 :1304-1351 含尾 break 后行微漂）**无任何 IsValidHYLL 前置门**，CopyUpdateMerge 直把垃圾旧值当 HLL 与源择大盲并覆写 dest（污写为磁盘驻 dest 常形）；崩溃窄形：dest offset3 形字节非 0/1 时 Merge 落 `HyperLogLog.cs:987` throw GarnetException("Merge exception")——**须 oldValueLen==newValueLen 方可触发，即 MergeGrow 对 dtype 非稀疏垃圾恒回 DenseBytes=12304 之「恰 12304B 非 HLL string 磁盘驻」窄形，后席勿误读为常形崩溃**（本句系票面前置防注）；c) dest=对象键 ValueIsObject → InPlaceUpdater :390-394 WrongType 动作 → SET_Conditional 返 WRONGTYPE 仍被 :262 弃掉。RESP 层 `HyperLogLogCommands.cs:99-126` 唯判会话函数出参 status==WRONGTYPE||error（皆源自源侧），故凡「dest 坏形/异型 + ≥1 源命中」C# 恒 +OK。

Rust 现状（修复侧）：快臂 `hyper_log_log_merge`（`wnode/src/resp/hyperloglog/hyper_log_log_commands.rs` dest 窗后 load_hll 前置拒臂，票锚 :609-611 现树微漂）——`valid_hyll_payload` 单入口借用切片上校验、对象信封经 read_user_sync→双域读漏斗恒 Err → 独占 `RESP_ERR_WRONG_TYPE_HLL` 即止，任一源不读、dest 零触碰（不写回不推 WATCH）；慢臂 `slow_hll_merge` dest 臂同款（`reply_wrong_type_hll` 与快臂 write_error_raw 同串逐字节全等）。分叉矩阵：格一 dest=非 HYLL string + 源命中——C# +OK（热形原样/磁盘驻污写，含崩溃窄形）vs rust -WRONGTYPE——帧 + dest 终态双分叉；格二 dest 坏形 + 全源缺失——C# 循环零 SET 直 +OK（dest 免检）vs rust 前置拒即 -WRONGTYPE——帧分叉（存量 `pfmerge_without_sources_skips_storage` 现树实测 :263-285 仅钉零源两形〔缺失 dest 与 String dest〕，本格二形无锁在册，本票补）；格三 dest=集合/哈希对象键或 RI 键 + 源命中——C# 弃返回值后仍 +OK 零覆写（无幽灵建值）vs rust -WRONGTYPE——帧分叉、dest 副作用两侧同净（向量键形归 scan_all 派发门 + hllsec2 案一修复域，本条零触达互斥）；格四 dest 坏形且自列源——**判净同构**：C# 源 GET 读 dest 垃圾载荷 → -1 哨兵 break → WRONGTYPE_HLL 零 SET，rust dest 恒单窗仅 dest 取窗（源不取窗）、src==dest 读得窗内未写回旧分配载荷 max 幂等，同串同零触；格五源侧逐序损坏/异型/过期——判净（rust load_hll 三态与 C# NOTFOUND-continue/WRONGTYPE-return/-1-break 同序同串，校验在借用切片闭包内零物化拒、无先拷后拒窗口，快臂 Degrade 整体转慢臂闭环复判多路径同构；TTL 保留/清退写侧归 §17/hllchain1 域）。

裁决与保留理由：方向 = rust（Redis 更严侧、dest 零触达），§16/§108/§113 修复型分叉家族先例、§113（集合算术族「rust 全集判型在先、独占 WRONGTYPE、STORE 错误臂目标键零触达」）系本案一最近似姊妹面——该条明令禁止「修回 C# 形」方向在册，PFMERGE dest 免检面系同族漏网。零运行时数据危害，危害全落治理面：不登记则双侧对拍夹具遇格一至三必红（C# +OK 掩盖、rust 错误帧），后审席可能按 §113 明禁方向「修回 C# 形」——修回即把 dest 免检盲并（Copy 臂污写/崩溃窄形）与源侧错误 dest 已半改的免检放行接进 rust，触本仓「多路径行为同构」「错误契约对齐」双线红线。

后果与严禁回改：后续 SET/HLL 族对拍/审查席遇格一至三用例直引本条判有意偏差并跳过，勿重复提报；**严禁按 C# dest 免检 +OK 掩盖形回改**。注释锚：`hyper_log_log_commands.rs` `hyper_log_log_merge` 与 `slow_hll_merge` 两处 dest 臂上各补回指本条一行（同 §110 ext.rs 锚例形制）。锁面：`wnode/tests/hyperloglog.rs` §144 夹具族——`pfmerge_corrupt_dest_string_wrongtype_fast_slow_arm_parity`（格一：独占帧 + d 逐字节 "xx" 原样 + 源零并入痕迹 + 快慢双臂应答逐字节全等，夹具仿 §113 resp_set.rs 慢臂对拍先例）、`pfmerge_corrupt_dest_all_sources_missing_wrongtype`（格二：≥1 源全缺失 + dest 坏形拒帧，防被当作源循环免检面回归改动旁路）、`pfmerge_object_dest_wrongtype_keeps_members_no_ghost`（格三：帧 + 对象成员仍在 + String 域零幽灵，与 §113 形 c 同判据）；存量 `pfmerge_without_sources_skips_storage` 零源两形与 `pfmerge_direction_matrix_and_multi_source_count` 全族零漂移回归。划界：done whll-pfmerge-storage-error 钉源侧错误臂部分提交（fast/slow 已收敛恒提交，与本条 dest 前置臂互斥）、r137c-hllsec2 钉基数缓存面、hllchain1 钉 RMW 终值重放臂（本条 dest 形仅陈述未写回零触碰）、§17/§18 钉 TTL 轨、r8-sample-a 原子性已核销——均不重叠。

## 145. PFMERGE 后 dest 稀疏分配长度恒取 MergeGrow/CopyUpdater 单臂形（不对齐 C# InPlace/TryMerge 热臂保留原分配形——C# 热/冷两臂物理长度随记录可变性自相矛盾，rust 单臂自洽，§17/§18 同族谱 PFMERGE dest 轨）

工单 task/ing/zcode-r147c-pfmerge.md 案二登记（甄别席漂移数值自洽实测：C# SparseBytes=18+128+128=274、MergeGrow=147+128=275，可达序列 PFADD 至 RLE 占 147 再以单非零稀疏源 PFMERGE 恒触发迁移——STRLEN/DUMP 级分叉可达，模块头注对标宣言半击穿属实，P3 登记级：不改码不改行为，只补台账 + 注释锚 ×2 + STRLEN 形锁一用例）。编号顺编注记：票面拟号 §120 系旧册势，落册时接案一条顺编初拟 §144；合入前复跑核尾见键级 TTL 粗化条与门禁脚本面条双占 §143、案一条让位终取 §144——本条随之让位终取 §145，先入库者得号、撞号不覆写（本条内与代码/锁测回指锚已同步 §145）。§135 头注「r147c 案二同谱」回指望本条；两票划界：§135 钉 PFADD 轨折叠终值形与升稠密字节界，本条钉 PFMERGE dest 轨分配长度取形（热/冷两臂之源），同谱不同形互覆不重叠。

C# 双臂自相矛盾：`SET_Conditional(dest)` 的 dest 新物理长度随记录可变性走两臂——热形（记录在内存）InPlaceUpdaterWorker PFMERGE 臂 → `TryMerge`（HyperLogLog.cs:930-963）稀疏目标仅裸判 `SparseCurrentSizeInBytes(dst) + srcNonZero*2 < dstLen`（:945-947），判过即就地并入、**记录分配长度原样保留**；冷形（磁盘驻 CTR）NeedCopyUpdate→true→新记录尺寸由 `MergeGrow`（:434-447）算得（当前占用 + 扇区取整页数，**无「保留原富余」概念**），CopyUpdater 臂 CopyUpdateMerge（:457-471）迁移。同输入同命令两臂产出不同物理长度：初始 PFADD 建 274B 分配、RLE 占 147B 的 dest 并入 2B 非零稀疏源——热臂保 274、冷臂出 275。系 §17/§18「结局随记录可变性漂移」同一 C# 家族缺陷在物理长度轨的投影。真 Redis PFMERGE 恒把 dest 转稠密 12304，双侧本就不随。

Rust 现状（单臂自洽）：命令臂 `hll_merge_payload` 不设热臂——恒 `new_len = merge_grow(src, dst)`（`whyperlog/src/sparse.rs` 1:1 镜像 C# MergeGrow 含空源 saturating 守卫；§135 折叠面出形复检系同一单臂口径不另形）与 `dst.len()`（装载的分配长度）比对，不等即 `copy_update_merge` 迁移。常见热场景（274B 富余 dest 并入小稀疏源）rust 出 275/276B 而 C# 热形保 274B：PFMERGE 后 STRLEN/GETRANGE/DUMP 逐字节分叉（HLL 键是 String 域记录，物理长度即用户可见值长，触 review.md 5.1「响应逐字节全等」与模块头注「对标 C# 记录的物理长度恒为分配长度」自陈对标语义——rust 单臂实为恒贴 C# 冷臂，头注原未言，本票已补限定句消除歧义）。基数面不损：两臂寄存器终值恒等（max 择大幂等），PFCOUNT/+OK 两侧同值；`whyperlog::try_merge`（1:1 镜像 C# :930-963 热臂裸判）现仅 PFCOUNT 虚拟并集消费、写侧无读者——本条登记写侧不接双形并存，未来若须并 C# 热臂形须另案把 try_merge 并轨为单判据源（§135 单机制纪律，本票明禁顺手双形并存）。

危害面：零数据丢失零应答帧分叉，危害对账面：C# 默认热态夹具下 PFADD→PFMERGE→STRLEN/DUMP 逐字节对拍必红；不登记不定裁决则三形互踩——后审席或「按 C# 热臂修回」使 rust 又漂移于冷臂形（重蹈 §17 未登记前 TTL 双结局覆辙、把随记录可变性双结局接进写侧），或反向据 rust 形判 C# 冷臂/C# 热臂为缺陷再立错案。

后果与严禁回改：**严禁按 C# InPlace/TryMerge 热臂保留原分配形改回，亦严禁据此判 C# CopyUpdater/MergeGrow 冷臂形或 rust 单臂形为缺陷**（双向防回改句，票面裁定）。对拍轮遇「PFMERGE 成功 + 稀疏 dest 有分配富余」STRLEN/DUMP 分叉用例直引本条判有意偏差并跳过。注释锚：`hll_merge_payload` 头注补两行裁决声明（不设热臂保留形、恒取 merge_grow 出形）与模块头注长度宣言限定句，均回指本条。锁面：`wnode/tests/hyperloglog.rs` `pfmerge_dest_alloc_length_follows_merge_grow_single_arm`（STRLEN 形锁：274B 单元素 dest 并入单非零稀疏源后 dest 长度恒等 merge_grow 出形 275/276、非原分配 274，C# 热 274/冷 275 漂移事实注释锚；PFCOUNT==2 与终形载荷同源、源键零触碰、稠密 dest 形恒 12304 零变）；存量 `pfmerge_direction_matrix_and_multi_source_count` 与快慢臂 dest 逐字节对拍 `pfmerge_error_partial_commit_fast_slow_arm_parity` 零漂移回归（慢臂装载同为分配长度 blob、同算同形）。

## 历史既有裁决（参见相关文档）
- HLEN/ZCARD 矫正臂等非恒 O(1) 面：见 `doc/zh/collection.md` 第 6 节（大键 O(1) 计数规约，含 C# 同面非恒 O(1) 对照）。
- MutablePercent 基线：C# 缺省 90（`ServerOptions.cs:77` 与 `defaults.conf:64` 双证，合法区间 10..=95），rust 引擎缺省比例 0.5（`whlog/src/config.rs:20` `DEFAULT_MUTABLE_FRACTION`，经 `wkv/src/config.rs:393` 装配），用户旋钮 `--hlog-mutable-percent`（`wconf/src/node_options.rs` `mutable_percent` 字段）经 `wnode/src/service.rs` 的 `apply_hlog_overrides` 投影为 `StoreConfig::mutable_fraction`（生产入口 `store_config_from_node`）。0.5 vs 0.9 分叉未裁决，已在 §93 留槽占号，待裁决；旧文「C# 基线 50」系错标，勿再据此对账。
- AOF 版本域：`waof/src/aof/header/basic.rs` 的 `AofHeader::AOF_FORMAT_VERSION`（本仓自持版本域，16B AOF 头内 1B `aof_header_version` 字段）。读侧门形分叉注记（r315 补）：C# 拒 `version > MaxSupportedAofHeaderVersion` 上界臂、界内旧代带重映射透明恢复（`AofProcessor.cs:240-241`、`RespAofDownlevelVersionTests.cs:51`）；本仓采**等值门**（凡 `!= AOF_FORMAT_VERSION` 即拒，`wnode/src/aof/aof_processor.rs` `process_aof_record_internal`），上越界、本仓旧代、跨仓（C# 1..=5 戳）文件一律显式判败——理由系本仓 AOF 载荷与 C# 实质异构（物理键前缀、32B 显式分列重放输入头），无代际重映射域可做，放行即静默误读。锁测：`wnode/tests/aof_header_version_gate.rs` 三臂（合法往返读通／uplevel 拒收对标本仓无 C# 恢复对应物的 `UplevelAofVersionIsRejected`／异代际戳恒拒即 C# downlevel 恢复例的分叉形）。C# downlevel 兼容臂（v4→v5 重放）在本仓无对应物，不硬造。


## 146. 门禁脚本面：allow.check 扩 #[expect] 覆盖与两处死分支摘除（zcode-r139c-gateaudit 案四/案五裁决登记）

工单 task/ing/zcode-r139c-gateaudit.md 案四/案五落地登记（工具面单侧票，席位授权 r97/r98 先例；案一/二/三为脚本恒真退出码修复，属门禁自身失效面不改契约，无需台账）。编号顺编注记：本条初落册按当时册尾 §142 顺位取 §143，与键级 TTL 粗化条（setexabs 先入库得号）双立 §143；主理席事后核对按「先入库者得号、撞号不覆写」规让位改号 §146，回锚以本节号为准；并行席（r151c-hincrby、r147c-pfmerge）合入前复跑册尾者按同规让号。

- 案四裁决：`allow.check.sh` 源码侧匹配由仅 `\[allow` 扩为同时匹配 `#!?\[expect`（RFC 2383 lint-reasons 就地压制与 allow 同禁）；`cfg_attr` 人工巡检豁免面不变。现树 `#[expect` 抑制点位零命中，本条为形态覆盖缺口闭合＋防下轮最短绕过路径。
- 案五裁决：`feature.check.sh` 头注悬空法源 `wbase-feature-decl-gate.md`（四池与 git log 零记录）删句注记，判据即脚本自述；`js/check.js` root ignore 分支（读取从未存在的 `js/check/ignore.yml`、`global_ignore_set` 恒空仍被消费）连全链摘除，豁免仅认 `js/check/ignore/` 语料单轨，杜绝双轨豁免并存。
- 拓扑注记：本仓 `wedb/sh` 为指向 `~/.local/share/cargo_sh` 的符号链接且在 .gitignore（fork.sh 有意 link 共享），`udeps.sh`/`clippy_extra.sh` 修复落该独立仓 dev 分支（bd87d92、1a6edb7），不随 wedb 分支合并；后席查门禁脚本史须两仓对账。

## 147. 错误回显净化单点在 DEBUG/LATENCY 族臂的落位注记（C# 裸插值可出双帧 vs rust CRLF 切断＋128 长度帽单点成帧；甲乙并条，纯登记零行为改动）

工单 task/ing/zcode-r145c-dbglat.md 立案甲立案乙合并登记（甄别 r145c-tr-dbglat 席两案通过，登记级、零行为改动提案；同票立案丙系 §32b 清单补第 25 项，随彼条不并入本条）。编号顺编注记：票面禁预钉号，本条初次落册按当时册尾顺编拟 §144；合入前沙箱复跑 `git merge dev` 见 PFMERGE 两案条与 gateaudit 让号条先入库分占 §144/§145/§146，依先入库者得号、撞号让位不覆写纪律终取 §147，条内回指锚随实况同步。

C# 一手形态（回显裸透、无执行门）：DEBUG ERROR 载荷 `garnet/libs/server/Resp/AdminCommands.cs:754` `WriteError(parseState.GetString(1))` 裸插值（等长门 :746-756）；DEBUG 未知子命令回显 :856 `parseState.GetString(0)` 同形；LATENCY HISTOGRAM/RESET 非法事件回显 `garnet/libs/server/Metrics/Latency/RespLatencyCommands.cs:67`（`$"ERR Invalid event {invalidEvent}. Try LATENCY HELP"`）与 :113（`$"ERR Invalid type {invalidEvent}"`）$ 串插值裸回显，无长度帽无 CRLF 净化。上述各点共面写帧单口 `garnet/libs/common/RespWriteUtils.cs:266-277` `TryWriteError(ReadOnlySpan<char>)` 按长度原样写入，函数头注释 :265 自认「The string mustn't contain a CR (\r) or LF (\n) characters」系前置约定无执行门——载荷携 `err`+CRLF+`msg` 时 C# 出双帧（负帧 err 加裸文本帧 msg），帧注入面存在（DEBUG ERROR 受 enable-debug-command 与远端门禁仍可达，门先行非豁免面）；非 ASCII 段经 `Encoding.ASCII.GetBytes`（:273）折 '?'。

Rust 现树实现（净化单点收口超集）：错误成帧单点 `wresp/src/cmd_strings.rs:487` `write_error_raw` 落 `RespWriter::write_error` 统一成帧、载荷首 CRLF 处截断——DEBUG ERROR 臂 `wnode/src/resp/admin_commands.rs:427` `write_error_raw(output, &ascii_sanitize(parse_state[1]))`，非 ASCII 逐字节折 '?' 与 C# 同折形（`wbase/src/ascii.rs:11` 对 `Encoding.ASCII` 语义，本位点无编码分叉）；回显臂先过 `sanitize_error_str` 门面（`wresp/src/ext.rs:70`，CRLF 切断＋`MAX_PARAM_NAME_LEN=128` 长度帽，常量单点 `cmd_strings.rs:482`）再入同一成帧单点——LATENCY 非法事件回显 `wmetric/src/latency/resp_latency_commands.rs:53/:84`、INFO 段名回显 `wmetric/src/info/info_command.rs:123-127`、解析层未知子命令回显 `wnode/src/resp/parser/resp_command.rs:624-633`（`Cluster | Latency` 特判臂 :630）同源引用同一净化机制两臂。

裁决与登记：同一输入（载荷带 CRLF／超帽长名／未知子命令回显）C# 出双帧或裸长帧 vs rust 各点恒出单帧净化形，系确凿对外行为分叉——rust 形为帧注入防御性收口超集，与 C# 自家注释约定（错误串不得含 CR/LF）同向，判「行为分叉，登记收口」；C# 注入形不复刻不补救，**严禁 rust 按「对齐原型」名义把成帧单点改回裸透或调用点散改回显**（拆帧注入防御，改回才是真回归）。甲乙并条单源符合 §32b 族目纪律（两入口一处截断——截断本就在成帧单点，登记面缺的是这条落位锚）；与 §110（BITFIELD 回显编码族条）域互斥不混登：彼条裁 `as_str_safe` UTF-8 原样 vs C# ASCII 折叠之编码域分叉，本条裁 `ascii_sanitize`/`sanitize_error_str` 双侧同折形之上叠加的 CRLF 切断与长度帽净化面，本位点无编码分叉。划界互注：done/etag2 之「慢臂双帧」系 etag WRONGTYPE 叠帧异机制异缝（彼为错误帧后追加第二帧，本为回显载荷注帧），互引不并案；§85 TX_PROC_LAT 恒空桶、§30 DEBUG 降级条不受本条覆写；LATENCY DOCTOR/GRAPH/HISTORY/LATEST 整面缺席系 api-compatibility 明标 ➖（`garnet/website/docs/commands/api-compatibility.md:209/:210/:213/:214`）视同已登记，双侧错误帧同形（rust `resp_command.rs:630-633` 对 C# `garnet/libs/server/Resp/Parser/RespCommand.cs:1438-1442`），不为缺席面设计补实现。

锁面（全部已在册，本票零夹具新增、零行为码改动）：`wresp/src/cmd_strings.rs:649` `write_error_raw_sanitizes_crlf`（成帧单点 CRLF 截断锁）；`wnode/tests/resp_admin.rs:705` `debug_error_non_ascii_folding`（:712-724 钉非 ASCII 折 '?' 形、:726-731 钉 CRLF 载荷单帧 `-err` 形）；`wnode/tests/resp_admin.rs:735` `debug_unknown_and_arity_non_ascii_folding`（DEBUG 未知子命令与 arity 回显折叠形）；`wmetric/src/latency/resp_latency_commands.rs:206-225` `invalid_event_crlf_cannot_inject_frame`（CRLF 单帧与超长名帽形——:216 以 4096 字节输入命中 :221 `MAX_PARAM_NAME_LEN` 帽，帽值 128 防误读）。后续对拍轮遇 DEBUG ERROR/LATENCY 非法事件/未知子命令回显之 CRLF 或多字节载荷用例，直引本条判有意偏差并跳过，勿重复疑报。

## 148. MIGRATE 退化形与残面分叉并条：C# slots 未初始化 NRE 崩溃形/空编排 +OK 形 vs rust -NOKEY 显式拒收，及 IOERR 帧尾句号 C# 双臂自相矛盾取单源规范形（两登记级并条，零行为改动）

工单 task/ing/zcode-r157c-migrate.md 案一/案二并条登记（皆登记级，禁触 migrate.rs 任何门与 cmd_strings 常量）。编号顺编注记：票面所记册尾 §131 系旧快照，落册时册尾实况为 §145 与 gateaudit 条（该条初落撞号 §143，dev 侧已改号 §146 归位），本条初次落册按当时册尾顺编取 §146，合入前复跑 `git merge dev` 见 gateaudit 条先落得 §146，本条让位终取 §147；二次对账复跑见 dbglat 甲乙并条先合入得 §147，本条再让位终取 §148，先入库者得号、撞号让位不覆写。

案一（MIGRATE 退化三形）：C# 顶层 slots 集仅 KEYS/SLOTS/SLOTSRANGE 选项臂初始化（garnet/libs/cluster/Session/MigrateCommand.cs:183/:228/:271 slots = []），三形不达即 null——NONE 形（键参空串且无选项）与 Redis 单键形（MigrateCommand.cs:156-161 仅 sketch.HashAndStore 不触 slots）两形下 scheduleMigration 段无条件呼 TryAddMigrationTask，MigrateSessionTaskStore.cs:94 的 new MigrateSession 实测在 try 块之外（首 catch 远位 :131 之后），构造器 MigrateSession.cs:149-150 即呼 GetRanges、:230 首行 _sslots.Count 对 null 即抛 NRE 逃逸至会话分派层（MigrateCommand.cs 全臂无外层 catch）——连接崩溃形，绝非 +OK 亦非错误帧；KEYS 零后继形 slots=[] 非 null 不抛，空 sketch 各臂零迭代返真（MigrateSessionKeys.cs:27-177）→ 空编排 +OK；单键形缀 KEYS 词元即绕行首键 IsLocal/CROSSSLOT/IsMigratingSlot 三查（三查只在循环臂 :196-216，前置入 sketch 的首键免检）。rust 不对标崩溃形、按 Redis migrateCommand 契约面收敛：NONE 与 KEYS 零后继并入同一显式拒收臂（wedb/wedb/src/server/cluster_session/migrate.rs:544-554）回 -NOKEY 帧（常量 wedb/wresp/src/cmd_strings.rs:99-102，帧形 -NOKEY\r\n）；Redis 单键形系 rust 补行形（激活上游不可成功执行之死分支，同 §79 判例族）——库级定槽（doc/zh/db.md 4.1）下 migrate.rs:360-373 补迁移域门禁 + is_local + is_migrating_slot 会话槽三查，未置 MIGRATING 回 NotMigrating 帧。严禁回改：后续对拍席遇「同命令 c# 断连/rust 回 -NOKEY」「c# 空跑 +OK/rust 拒收」直引本条；按「对齐原型」名义拆 rust 显式门复刻 C# NRE 崩溃形或回落空跑 +OK 投影即触红线 4；禁加 c# 崩溃形复刻断言。锁面：wedb/wedb/tests/cluster_migration.rs::migrate_empty_key_forms_rejected_not_ok（NONE 形与 KEYS 零后继形两 -NOKEY 断言，本形法定锚）与 migrate_command_single_key_without_migrating_state_rejected（单键形 NOTMIGRATING 前置帧锁，本票前既有、按票面有锁免加）。

案二（IOERR 帧尾句号双臂分叉）：C# 同词两句自相矛盾——注册失败臂经 CmdStrings.RESP_ERR_IOERR「IOERR Migrate keys failed」无句号（MigrateCommand.cs:352，常量 garnet/libs/cluster/CmdStrings.cs:80），KEYS 驱动失败臂内联「IOERR Migrate keys failed.」带尾句号（MigrationDriver.cs:106 errorMessage 经 :364 TryWriteError 出帧），同一失败语义双臂异形（§23 臂间文案分叉谱系），系上游一手形非本仓引入。rust 两臂合并单源常量 wedb/wresp/src/cmd_strings.rs:847-848 cluster::ERR_IOERR 无句号规范形（锚注即常量源形），消费点 migrate.rs:515（注册失败）/:537（KEYS 驱动失败慢路径）同引。分叉面：注册失败臂与 C# 逐字节全等；驱动失败臂差一尾句号字符，纯逐字节对拍面残差、零运行期危害。裁决：errstyle 单源提取既定裁形（done/zcode-r135c-errstyle 方案 2），取无句号规范形与 §32/§35 降噪族同向，严禁按 C# 驱动失败臂回改内联散写带句号形（散改越单机制红线，§110 族级编码裁决口径取册不取码）。锁面：本票新增 wedb/wedb/tests/cluster_migration.rs::migrate_command_keys_driver_failure_returns_ioerr_frame（真协议帧远端批次拒触发同步慢路径驱动失败臂，逐字节钉 -IOERR Migrate keys failed\r\n，夹具复用现族无 mock）。

随行注记（不另立目）：1. AUTH/AUTH2 缺参形——rust 选项解析期即回 SYNTAX 错误帧拒收（migrate.rs:383-397 args.get 显式早退），C# GetString 越位读残槽（MigrateCommand.cs:174/:178-179），§72 有界化同族判例，拒收形先于崩溃形属既定方向；2. c# --fast-migrate 旋钮（garnet/libs/host/defaults.conf:112 恒 false、RespClusterMigrateCommands.cs:97-100）本仓全树零接线、行为恒同 C# 默认同步形，缺席旋钮归配置旋钮族登记口（§111 收形先例），本册不立目。


## 149. keynum 型键规格 numkeys=0 空键区在集群槽位门的首键直取分叉：C# 无条件取 firstIdx 读同会话残留槽定槽（残槽族），rust 双闸短路判无键放行（纯登记＋双闸禁回改注释＋两处锁测，零行为改动）

工单 task/ing/zcode-r145c-evaldict.md 案一登记。编号顺编注记：票面预拟 §119/§120 系旧快照过期（两号现册已由 CLUSTER 节点 id 渲染条与降阶旋钮条占得，pfmerge 条注同记该争占），本条初稿按执笔时沙箱册尾（最大号 §143）顺位拟取 §144；合入前沙箱首轮复跑 `git merge dev` 见 PFMERGE 两案条与 gateaudit 让号条、dbglat 错误回显条先入库分占 §144/§145/§146/§147，让位拟取 §148；二次复跑见 MIGRATE 退化形条先合入得 §148——先入库者得号、撞号让位不覆写，本条终取 §149，全链回指锚（本条标题、simplified.rs 双闸禁回改注释与 scan_keys 自述回指、两处锁测注记）同步改号。

分叉形态（C# 一手形）：提键单点 `TryGetKeySearchArgsFromSimpleKeySpec`（`garnet/libs/server/SessionParseStateExtensions.cs:930`）keynum 臂（:988-1011）对 keyNum=0/负值**无空键区判定**——`TryGetInt` 只判解析成功不设下界护栏，`lastKeyIdx = firstKeyIdx + (0-1)*step = firstKeyIdx-1` 负偏，钳制段（:1006-1009）只管上界不触发，:1011 直回 `(firstIdx, firstIdx-1, step)` 且 `return true`。三调用方形态不一：事务键计划 `TxnKeyManager.cs:79` 与提键扫描 `TryAppendKeysFromSpec` 均 `firstIdx..=lastIdx` 循环域形、空区零迭代天然安全（对照非立案）；唯集群槽校验核 `MultiKeySlotVerify`（`garnet/libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:149`）规格命中后 :162 无条件 `GetArgSliceByRef(searchArgs.firstIdx)`＋`HashSlot`＋`SingleKeySlotVerify`，把 firstIdx 当必存在键，亲验无 `firstIdx>lastIdx` 短路。`GetArgSliceByRef`（`SessionParseState.cs:369-373`）仅 `Debug.Assert(i<Count)`、release 直读 `bufferPtr+i`，`MinParams=5`（:22）坐实系**逻辑越界读同会话残留槽**（非物理越界）。门只在集群态进入（`RespServerSession.cs:679`、`RespServerSessionSlotVerify.cs:28-58`），单机不显。

订正形写实（票首甄别段推翻旧立案文案）：live parseState **不含命令名**（C# `NetworkSETNX` 键取 `GetArgSliceByRef(0)` 且 `Count==2` 为铁证）。EVAL 形（bs Index=2、FirstKey=1）numkeys=0 → firstIdx=2（0 基），读到的是 numkeys 后随首参（索引 2）之同会话残留槽；ZUNION 形（bs Index=1）`ZUNION 0` live 态 Count=1、firstIdx=1 同为残留槽读取——两形一律**归残槽族**登记，旧文「numkeys token 本身'0'恒按字面定槽」「Count>2 读 argv[1]」语废，后席勿沿用；EVAL 形旧文「新会话零值即空键 CRC16 恒 0 → 槽 0」仅在缓冲区零值场景成立，本质仍是读残留槽非读声明键。

rust 现形（唯一保留形态）：同一提键单点 `try_get_key_search_args`（`wedb/wresp/src/catalog/simplified.rs`）keynum 臂 :169-171 `key_num <= 0` 显式早拒、末端 :180-182 `first > last` 兜 None，**双闸短路** → `extract_keys_from_slice` 回空 → `evaluate_multi_key_slot_gate`（`wedb/wedb/src/server/cluster_session/slot_verify.rs:124-126`）`keys.is_empty()` 即 Serve，零渲染零等待。门链 `core.rs:909-938` → `resp_server_session_slot_verify.rs:81-117`（空规格 :97-99 直放行）逐位亲验。

后果与裁决：集群态对拍 `EVAL "..." 0` / `ZUNION 0`（皆合法常用零键形态，Arity -3 满足）必发散——rust 本地执行回脚本应答，C# 以假键定槽在不持该假槽属主的分片回 -MOVED（迁移窗口更进 `CanOperateOnKey` 对假键做存在性判定，回 -ASK 或等待），构成无意义跨节点乒乓。裁**保留 rust 无键放行形**（声明零键的命令不应按假键定槽）。

严禁回改：严禁按 C# 无条件首键直取形给 `simplified.rs:169-171` 与 `:180-182` 两闸补首键直取臂——即把假键定槽与参数数组残留槽读取引入本仓（**回改才是真回归**）；严禁在 `evaluate_multi_key_slot_gate` 内另起「无键时取 args[0]/args[first]」兜底（第二机制）。双闸处禁回改注释在位。

同族互列（keynum 臂家族三形）：§106 宗 b 裁同臂「超大 numkeys int 加法回绕空回」形（COMMAND GETKEYS 面）；§72 裁 begin_search keyword 未命中形（其 :928 注记自陈槽校验受污面）；本条裁 numkeys=0 空键区在槽位门裁决面形——册内该面此前零登记，后续对拍席遇「EVAL s 0 双侧一 MOVED 一执行」直引本条。

受控命令集（现树 JSON 本宗命令 KeySpecifications 逐字段全等；两目录命令总数非全等——C# 262 含 MODULE/REGISTERCS/Custom、rust 259 含 RICOUNT/SUNSUBSCRIBE——不成契约勿按总数对账）：首命中规格系 keynum 型者——EVAL/EVALSHA/BLMPOP/BZMPOP（bs Index=2）与 LMPOP/ZMPOP/ZUNION/ZINTER/ZDIFF/ZINTERCARD/SINTERCARD（bs Index=1）；ZDIFFSTORE/ZUNIONSTORE 首规格 range 型（Index=1、LastKey=0）首键即真实目标键、keynum 次规格走循环臂，不入本宗暴露面。

锁面：`wedb/wresp/src/catalog/simplified.rs` tests `numkeys_zero_keynum_spec_yields_no_keys`（目录真源 EVAL/ZUNION 规格，numkeys=0 三例 None＋空键＋numkeys=1 正形对照排空门）；`wedb/wedb/tests/cluster_slot_verify_wait.rs::eval_numkeys_zero_gate_serves_without_redirect`（多键门夹具旁：EVAL 真源 key_specs 入 `ClusterSlotVerificationInput`、args `["return 1","0","<残槽字节>"]` 且该槽属主非本地仍 Serve 零字节，同规格 numkeys=1 形 Redirected 非空字节对照）。判净五点（#! flags 双侧编译拒、只读脚本门结构不存在、EVALSHA NOSCRIPT 逐臂对齐、脚本超时让渡既定面、脚本内逐键门与事务锁集同构）落据于票面不入本册。

## 150. 集合算术族对象装载漏斗对判死异构键（string 影子/RI 到期未清退）恒判缺失按空集吸收，C# ObjectStore Reader 判型先于判死回 WRONGTYPE，形帧与 STORE dst 终态窗内发散（修复型分叉裁决；严禁按 C# Reader 门序回改）

工单 task/ing/zcode-r159c-sdiff.md 立案登记（甄别 r160c-tr-sdiff 席通过，定级 P4 登记级维持；**纯登记＋两处锚回指注＋现状锁，零行为改动**——`obj_load_custom_sync` 首集空折叠形系对 C# 掐链/空集传导语义的修复型分叉裁决，rust 侧严禁回改）。编号顺编注记（三次让号）：本条初落册按沙箱基线册尾 §146 顺位拟取 §147；合入 dev 前复跑核尾见 zcode-r147c-errdisplay 族甲案错误回显净化条先入库得 §147，让位改号 §148；沙箱内 `git merge dev` 二次对账见 zcode-r157c-migrate 并条先合入得 §148，让位 §149；三次复跑见 zcode-r145c-evaldict 条（numkeys=0 残槽族归案）先合入得 §149，再让位终取 §150——先入库者得号、撞号让位不覆写，本条内编号与两处源码注释（`load_many`、`obj_load_custom_sync` 头注）及夹具族回指锚已随实况同步订正为 §150（锚一律按内容引不钉行号）。册尾历次手合附带收口：migrate 条合入遗留之册尾游离冲突残标一行（`|||||||` 机械残迹非条目内容）已由 evaldict 席先行清除，本条并册仅按双保留纪律续排殿后，不触他人条目正文。

与 §133 族关系（互指独立成条，禁各立割裂第三套）：本案与 §133（getrange2，GET 族 MainStore Reader 臂×「判死对象信封键」）及 §133 尾注并册的写侧 INCR 族面（incrovf）共「Reader 门序（判型×到期）」一族三臂——§133 自注「分叉唯 ValueIsObject 信封一族」，本案锚 **ObjectStore Reader 臂×判死非信封键**（string 影子/RI 记录，恰 ValueIsObject 之反极性形）×集合算术键位消费位×STORE dst 数据终态，册面零重叠，依 §118「§96 扩形宗三」先例裁独立立案成立、与本条互指。

C# 一手形态（原型自身两读臂门序相反）：对象 GET 漏斗 `ObjectStore/ReadMethods.cs:Reader` :19-23 `!ValueIsObject→ReadAction.WrongType` 先行——string 影子记录、RI 主存记录（`RangeIndexManager.cs:54` 不置 ValueIsObject，先例引证 reject/zcode-r131c-objfam）到期未清退态皆未及到期门即判错型；ValueIsObject=true 者方入 :25-30 到期门回 NOTFOUND。传导 `SetOps.cs:SetDiff`（:879-929）首键 :892-893、尾键循环 :914-915 WRONGTYPE 即返（无中途空臂）；RESP 层 `SetCommands.cs:SetDiff` :737-740 写泛型 -WRONGTYPE，`SetDiffStore` 错误臂先于收尾 SET（:860）/EXPIRE（:864）（即 §125 锚区），dst 值与 TTL 零触达。对照本仓主存 Reader（`MainStore/ReadMethods.cs` :31 判型先——§133 在册）与 `UnifiedStore/ReadMethods.cs` :22 CheckExpiry 先于判型——原型内部不一致。真 Redis 契约：`lookupKeyRead` 过期裁决先行，死键即缺席，不存在对死键的类型校验。

Rust 侧形态与裁决方向（=维持 rust，对齐真 Redis 过期即缺席）：对象装载三步漏斗 `obj_load_custom_sync`（`object_store_utils.rs`，Meta 分层闸→ObjectEnvelope 信封→String 域反探）每步域读恒先过 wkv 域内 TTL 单点门（`wkv/session/raw/read.rs:try_read_tag_sync_unprotected_with_prefix`，`TtlGate::Due→NotFound`，物理清退留写路径），型裁决（`meta_gate`、`step_string_domain`）皆立于门放行之后——故「SET st v＋过期未清退」或「RI.CREATE ri＋过期未清退」态下键位恒折叠 `ObjLoad::Missing`，`load_many`（`set_commands/write.rs`）Missing→推空集，`diff_sets` 首集空克隆后 retain 恒空、尾集空吸收；SDIFF/SINTER/SUNION/S*STORE 同漏斗同门，族域按族裁写。慢臂 `load_many_async` 同漏斗（异步域 TTL 裁决由各域读口单点惰性清退承担，`ttl_sync.rs` 异步档头注锚），快慢双臂应答全等。本仓 EXISTS/TYPE/TTL 观测面同穿该 TTL 门，与吸收形自洽。

STORE 形 dst 终态窗内发散（如实注记，两形皆可达、窗后同收敛）：尾键判死异构（`SDIFFSTORE dst s1 st`）rust 成功臂 `combine_store` 覆写 dst 旧值并随写清 TTL、回基数 vs C# 错误臂 dst 零触达；首键判死（`SDIFFSTORE dst st s1`）rust 空结果回收删 dst 回 `:0` vs C# 零触——发散限 C# 侧清退窗（主动扫描 `ArrayKeyIterationFunctions.cs:ExpiredKeyDeletionScan` 或写路径惰性清退）内瞬态，清退后两侧同收敛同帧。

危害与严禁回改：无常态数据面危害（窗内瞬态；rust 侧对齐真 Redis）；危害落治理面——未登记则对拍夹具遇「活集合＋判死 string/RI 键位」恒红且无据可引，STORE 形窗内 dst 终态（值＋TTL）双侧不等重于纯应答形。后续审查/对拍席遇「判死异构键×集合算术」用例直引本条判有意偏差并跳过，勿重复提报转写缺陷；**严禁按 C# ObjectStore Reader 判型先行「对齐」回改**——回改即穿 `read.rs` 域 TTL 单点门全漏斗同栈口径（装载/长度/存活探针共用，改一臂穿其余）、复活 §133 已驳之过期幽灵 WRONGTYPE（死键报型错诱导 DEL）、并与 EXISTS/TTL 观测面自相矛盾、击穿 `ttl_rmw_semantics.rs` 写族同序锁，改回系真回归。

边界与划界注记：成员级 TTL 门不适用——SetObject 为裸 HashSet 无字段级到期水位（划界 r155c-smovettl）；向量键判死形无分叉——rust 侧 EXPIRE vk 回 :0（§75 在册）判死不可达，C# Reader 判型先于到期对过期向量仍回 WRONGTYPE，与 rust 登记表门判「记录类型」不叠 TTL 命中即拒（`ttl_sync.rs` 门不叠 TTL 边界头注）恰同构；SINTERCARD numkeys 形门系票 zcode-r157c-sintercard 在册、§113 尾注覆盖向量短路免检形，本条不复报；STORE 窗序/自指装载面系 §125 与 done 票 wnode-set-store-selfref 在册，本条仅拍判死异构键×错误臂 dst 终态形，正交；输入清单同名重复幂等（`SDIFF k k` 恒空、`SDIFF s1 s2 s2`≡`SDIFF s1 s2`，门侧 `any` 探测幂等＋差集代数天然幂等）判净随本票补钉现状锁，不另立案；SINTER/SUNION/SINTERSTORE/SUNIONSTORE 及 ZDIFF/ZINTER/ZUNION 同漏斗余缝逐命令钉形由邻缝席各拍，本条族域按族裁写不扩面。

锁面与锚回指：`wedb/wnode/tests/resp_set.rs` §150 夹具族——`set_diff_dead_hetero_string_absorbed_as_missing`（判死尾 string 回活首集成员、判死首 string 回空数组，C# 对照 -WRONGTYPE 以注释锚记夹具头）、`set_diff_store_dead_hetero_dst_terminal_forms`（尾键判死形 dst 覆写值＋TTL 清退、首键判死形空结果回收删 dst 回 :0）、`set_diff_dead_hetero_slow_arm_matches_sync_arm`（冷键强制降级 slow.rs 臂对拍；成功路径成员序非契约按 §136 口径校基数＋排序成员，整数/空集帧逐字节）、`set_diff_duplicate_key_names_idempotent`（窄二幂等锁）；`wedb/wnode/tests/resp_vector_set_wrong_type.rs`——`sdiff_tail_key_vector_wrongtype`（窄三 SDIFF 尾键位向量形补钉，登记表门直拦、STORE 错误臂 dst 零触达）、`sdiff_dead_range_index_absorbed_as_missing`（判死尾 RI 吸收形＋活 RI 首/尾键对照格，RI 判死残留经 `put_ttl_sync` 裸写过去刻度构造，RESP 面 EXPIRE 对 RI 本不依赖，先例 `rename_nx_expired_residual_parity.rs`）；两处代码注释回指锚立于 `set_commands/write.rs:load_many` 头注「缺失按空集合」与 `object_store_utils.rs:obj_load_custom_sync` 漏斗头注。验收闸：§113 既有四锁、`ttl_rmw_semantics`（写族同序锁）与 `get_slice` 锁面零回归。

## 151. ZADD 数据段奇数尾巴（首 token 即分值形）C# 对象层主循环 GetArgSliceByRef 数组索引越界 UB，rust 双态防御截断回 :N（纯登记＋三处守卫自陈注＋双态现状锁，零行为改动）

工单 task/ing/wcol-zadd-odd-tail-token-out-of-bounds.md 登记级立案（甄别 zc-fix-r16-zaddtail 席通过定级 P3，审核席 zcode-r19-review-zset 三方对照补注随条）。编号顺编注记：本条按执笔时沙箱册尾 §150 顺位取 §151，合入前 `git merge dev` 复跑册尾，先入库者得号、撞号让位不覆写，条内与三处源码注释回指锚随实况同步（锚按内容引，行号容漂移）。

分叉形态（C# 一手形，四锚亲验）：会话层 ZADD 前置门仅 `parseState.Count < 3`（`garnet/libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetAdd` :25-26），「ZADD k 1 m 5」会话 parseState=[k,1,m,5] Count=4 放行；`ObjectInput` 以 startIdx:1 切片（`garnet/libs/server/InputHeader.cs:240-243`）后对象层 parseState Count=3 即「1 m 5」；对象主循环（`garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetAdd`）先 `TryGetDouble` 吃分值再 `GetArgSliceByRef(currTokenIdx++)` 取成员（:128-130），末轮 currTokenIdx==3==Count 时越界——`GetArgSliceByRef`（`garnet/libs/server/Resp/Parser/SessionParseState.cs:369-373`）仅 `Debug.Assert(i < Count)` 单防线，release 直读 `Unsafe.AsRef<PinnedSpanByte>(bufferPtr + i)` 越界槽：debug 构建断言掐连，release 构建读同缓冲越界垃圾 slice，垃圾成员名经 `ToArray` 走 `TryGetValue`/`Add`/`UpdateSize`/AOF 全链（或指针垃圾抛 NRE），行为不可预测。`GetOptions` 的剩余段偶数校验（SortedSetObjectImpl.cs :80-85，`currTokenIdx == Count || (Count - currTokenIdx) % 2 != 0` → syntax error）只在首 token 非分值（走选项段）形态生效，首 token 即分值的奇数尾巴完全漏出。

rust 现形（唯一保留形态）：双态均已防御闭环——内存信封 `wedb/wcol/src/zset/sorted_set_object_impl.rs:sorted_set_add` 主循环取成员前 `if curr_token_idx >= count { break }`（已消费合法对落账、尾巴分值丢弃，收尾按 added_or_changed 回 :N）；分层树臂 `wedb/wnode/src/resp/objects/tiered_collection_ops/zset.rs` 的 Zadd 解析扫描（`curr == args.len()` break）与主循环（`args.get(curr)` else break）同款截断。「ZADD k 1 m 5」双态稳定回 :1、集合恰落 {m:1}，自洽成立且应答确定。

三方对照（审核席补注）：真 Redis `t_zset.c zaddCommand` 对 score/member 奇数尾巴报 syntax error（成对校验先于执行）——本形态三方分立：C# 数组索引越界 UB / rust 防御截断回 :N / Redis 拒绝错误帧。裁决锁 rust 截断现状（确定且无害），不盲从 C# UB 形；rust 截断系防御容忍，非 Redis 对齐形。若后续有对齐 Redis 拒绝形的诉求，另行立案评估（应答 :N → error 属行为面改动，影响面独立），本条登记不预设该方向。

划界：本条裁数据段数组索引越界的奇数尾巴形（首 token 即分值，前置门与偶数校验双双漏出）。§142 裁数据段中段非浮点词形（选项词混入数据段，部分提交轴）；§5/§138 系首字节空串越界族（ZCOUNT/ZRANGEBYLEX 空串界）——均不覆本形。首 token 为选项词的选项段奇数尾巴（「ZADD k NX 1 m 5」形）GetOptions 偶数校验双侧生效同回 syntax error，不在本条。

危害与严禁回改：rust 无实害（防御在位、应答确定、会话存活）。危害落治理面——双侧对拍遇奇数尾巴用例时 C# 侧为 UB（debug 掐连 / release 垃圾成员写入），对账席无登记可引必误判为 rust 转写缺陷；后席若按「对齐 C#」名义撤销截断守卫（改成越界索引或 unwrap/panic 取成员）即引入真缺陷（**回改才是真回归**）。三处守卫自陈注在位回指本条号。

锁面：`wedb/wnode/tests/resp_sorted_set.rs::zadd_odd_tail_token_truncated_defensive`（内存信封真协议往返：「ZADD k 1 m 5」应答 :1 逐字节、ZCARD :1＋ZSCORE m→1＋ZRANGE 单成员钉集合恰 {m:1}、PING 存活、错误帧零输出；「ZADD k 1 m」会话 Count=3 过门后单对完整 :1 对照）与 `wedb/wnode/tests/tiered_cmds_align.rs::test_tiered_zadd_odd_tail_token_truncated`（升阶树内臂同形锁＋合法对追加对照）。

## 152. 停机 worker join 有界上界防御档（15s 超时 error 留痕强推，不对齐 C# 线程 join 无上界形态；与 §84 排空护栏正交并存）

工单 task/ing/zcode-r167c-gracefulstop.md 案三立案（甄别 zc-fix-r16-graceful 席 2026-09-26：案三锚部分成立——主要挂死向量已由 §84 `DRAIN_TIMEOUT_MS`=5s 强收护栏 + `kill_session` + 宿主线程 `Runtime` 析构兜底有界，票面「§84 登记 join 确定性时延上界」系误读（§84 裁决对象为排空循环非 join），残余面仅「worker 线程内部同步死锁」，随票降级 P3 防御档实施）。编号顺编注记：本条按执笔时沙箱册尾 §151 顺位取 §152，合入前 `git merge dev` 复跑册尾，先入库者得号、撞号让位不覆写，条内与源码注释回指锚随实况同步（锚按内容引，行号容漂移）。

C# 一手形态：`GarnetServer.cs:InternalDispose` 经 `servers[i].Dispose()` 收口接入线程，`GarnetServerBase.cs:DisposeActiveHandlers`（:168-199）对活动连接计数轮询诊断（5s 滞留 `LogError` 仅 `#if DEBUG`），线程收敛本体无超时可打断 join 面——worker 线程内部同步死锁时 C# 停机同样无上界。

Rust 侧裁决：`wedb/wnode/src/server.rs` 的 `join_worker_bounded`（中介线程代持 `JoinHandle::join` + 通道 `recv_timeout` 有界收口，返回 `Some(true)` 正常退出 / `Some(false)` panic 退出 / `None` 到期未收敛）与 `join_workers_bounded` 收口单点，`stop` 与 `reclaim_workers` 双臂共用；上界 `WORKER_JOIN_TIMEOUT_MS`=15s（排空护栏 5s 到期强收后另留 Runtime 析构兜底量级余量），到期 `log::error!` 留痕并强行推进后续收尾（AOF 刷盘收尾、锁守护释放不被滞留 worker 绑架），超时句柄随中介线程迁出（防御取舍：挂死线程不可回收，进程退出处置）。此为运维关停确定性边界的防御型有意分叉。

后果与严禁回改：与 §84 正交——§84 护栏既有行为（排空循环 5s 强收）绝不因本档回改；本档只覆其未及的 join 面。严禁回改为裸 `handle.join()`（ worker 线程内死锁即无限挂起停机链，击穿运维关停可达性）；亦严禁据此档反推排空循环可无上界运行（闸门放行先于 join 的前置约束维持）。

锁面：`wedb/wnode/tests/graceful_stop_phases.rs` 的 `join_worker_bounded_escapes_stuck_worker_within_timeout`（挂死线程超时臂 + 正常臂 + panic 臂三分形）；`stop_phase1_blocks_new_connections_before_vector_cleanup_convergence`（案一 Phase 1 前置时序锁）与 `stop_disposes_pubsub_after_workers_joined_with_inflight_publish_delivered`（案二 pubsub 收口后移时序锁）随同票并册。
## 153. ZRANGESTORE 空 src/dst 键：C# 存储层 :0 零触达守卫（内部 API 防御泄漏到 RESP 面）不复刻，rust 正常装载执行（空键合法物理键、缺源删 dst、结果可落空键）为真 Redis 一致侧（纯登记＋快慢双臂锁测，零行为改动；严禁按 C# 守卫形补空键早退门）

工单 task/ing/wnode-zrangestore-empty-key-guard.md 立案登记（甄别 zc-fix-r16-zrstguard 席通过，定级 P3 登记级——**纯登记＋锁测，零行为改动**，rust 侧贴真 Redis 属改良侧，登记不盲从 C#）。编号顺编注记（一次让号）：本条初落册按沙箱基线册尾（最大号 §151）顺位拟取 §152；合入前 `git merge dev` 复跑册尾见 zcode-r167c-gracefulstop 停机 worker join 防御档条先入库得 §152，让位终取 §153——先入库者得号、撞号让位不覆写，条内编号与锁测回指锚随实况同步订正（锚按内容引，行号容漂移）。

C# 一手形态（守卫确在存储层，三锚亲验）：ZRANGESTORE 存储层入口 `SortedSetRangeStore`（`garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:721-725`）首段 `if (dstKey.Length == 0 || srcKey.Length == 0) { writer.WriteInt32(0); return GarnetStatus.OK; }`——src 或 dst 任一空键即回 `:0`，源不读、目标键零触达（dst 旧值与 TTL 原样）。该守卫系 C# ObjectStore 全域形参版 API 的内部防御（HashOps/SetOps 同族普遍带 `Length == 0` 早退）泄漏到 RESP 命令面：RESP 会话层 `libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRangeStore`（:208-248）亲验无守卫，同文件 RESP 版 `SortedSetAdd`（`SortedSetOps.cs:695-701`）亦无空键守卫、空键经 Tsavorite 正常 RMW——守卫属 ZRANGESTORE 存储层入口单点装饰，非 RESP 契约面。

Rust 现形（唯一保留形态，全链无空键门）：快路径 `wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:sorted_set_range_store` 经 `zset_load_or_bail!` 装载先行（装载先判缺、与 288149d zcount 收口后 `read.rs` 同形复用单源宏，非本票新造机制）——空 src 形：空键为合法物理键，键缺失走 Missing 臂，`store_overwrite` 空结果回收删 dst、回 `:0`；空 dst 形：源正常读范围，结果集经 `store_overwrite` 落空名键、回 `:N`。慢路径 `slow.rs:zrangestore_cold` 双臂同形（`load_sealed_tri` 之 `Some(None)` 回收臂与快臂单源语义全等）。

裁决与严禁回改：rust 形贴真 Redis（空串为合法键名；缺源删 dst 系 ZRANGESTORE 正解 `zrangestoreCommand` dbGenericDelete 路径；结果可存空名键）。**严禁按 C# 守卫形给 rust 补「空键 → :0 零触达」早退门**——复刻即与真 Redis 语义反向分叉（空 src 不再删 dst、空 dst 不再成功写入），并与册内既有「取 Redis 一致侧、严禁回改」先例族（§150 STORE 族 dst 回收臂、空集合不落库、GEO 存储回收臂等）同宗抵触，回改才是真回归。后续对拍席遇「ZRANGESTORE 空键两形 c# 恒 :0 零触达 vs rust 空 dst 落键回 :N／空 src 删 dst 回 :0」直引本条判有意偏差并跳过，勿转报写缺陷。

连带注记（ZMPOP 空键形，同族边缘并册不另立目）：C# `SortedSetMPop`（`SortedSetOps.cs:1343`）对空键 `if (key.Length == 0) continue;` 跳过不弹；rust `blocking.rs:zset_pop_first_nonempty` 逐键正常装载弹出、无空键跳过（空键既存且有成员时弹出成立，贴 Redis ZMPOP）。同禁复刻跳过门。

划界：ZRANGESTORE lex 空串界形漏斗系 §138 在册、STORE 族目标键 TTL 轴见 :1710 区条目与 §150 互指，皆不覆本条空键守卫形——空键守卫全册此前零登记，ing/done/reject 各池无同轴票。ZADD 空键形（`ZADD "" 1 m` 双侧同规无守卫、空键正常建）系 r19 已决非分叉面（review_history/zcode-r19-zset.md:51），本条不重复展开，锁测仅作单点现状钉。

锁面：`wedb/wnode/tests/resp_sorted_set.rs::zrangestore_empty_key_forms_both_arms`（票 wnode-zrangestore-empty-key-guard 法定锚）——空 src 形先拍（空键未建态）：dst 预置旧值＋TTL，`ZRANGESTORE zedst "" 0 -1` 快臂逐字节 `:0`、终态 `EXISTS` :0／`TTL` -2 零残留，慢臂 SlowWait 直驱同帧删臂复验；空 dst 形后拍：`ZRANGESTORE "" zesrc 0 -1` 回 `:2`、空键经 `ZCARD`/`ZRANGE` 验存活且内容逐字节钉，慢臂覆写同帧；对照 `ZADD "" 3 c` 空键续建 `:1`（非分叉面单点钉）＋`PING` 会话存活。

## 154. CLIENT KILL ID 过滤器非正值（0/负数）C# 解析成功入过滤器恒不匹配回 :0，rust 同帧回 ERR client-id should be greater than 0（严向收口维持；严禁删 >0 滤片回改）

工单 task/ing/wnode-client-kill-id-nonpositive-gate-divergence.md 登记级立案（甄别席 zc-fix-r16-killgate 通过定级 P4，审核席 zcode-r16-review-killid 双侧现码亲验维持）。编号顺编注记（三次让号）：票面拟号 §151 系甄别时点册尾快照；落册沙箱复跑册尾见 ZADD 奇数尾巴条先入库得 §151，让位拟取 §152；一次 git merge dev 对账见停机 worker join 条先合入占 §152，顺延拟落 §153；二次 merge 复跑见 ZRANGESTORE 空键守卫条先合入抢得 §153，再让位终取 §154——先入库者得号、撞号让位不覆写，条内与两处源码注释回指锚按本节号落笔。

分叉形态（C# 一手锚亲验）：会话层 `NetworkCLIENTKILL` 新式过滤器 ID 臂（`garnet/libs/server/Resp/ClientCommands.cs` :271-284，票面 :268-281 有 3 行漂移）仅 `ParseUtils.TryReadLong` 解析失败（非整数/前导零/超 i64 域）报 `ERR client-id should be greater than 0`（`CmdStrings.cs:GenericErrShouldBeGreaterThanZero` :338），解析成功的 `idParsed` 不做正值校验直接入过滤器（仅查重复定义）；网络会话 ID 恒自 1 起（`Providers/GarnetProvider.cs:61` `Interlocked.Increment(ref lastSessionId)`，票面正文「GarnetServerBase」系笔误），故 `CLIENT KILL ID 0` / `ID -3` 于 `IsMatch`（:416 起，:438 `id.Value == targetSession.Id`）恒不命中，杀零会话后 `TryWriteInt32(0)` 即 `:0`。rust 现形 `parse_kill_filters` ID 臂 `strict_i64(value).filter(|&v| v > 0)`（`wedb/wnode/src/resp/client_commands.rs`）在 C# 解析失败门之上增「解析成功但非正值」同帧拒绝臂，回 `RESP_ERR_CLIENT_ID_GREATER_THAN_ZERO`（`wedb/wresp/src/cmd_strings.rs:342`，与 C# 格式化串逐字节同文）。即 C# 可观测契约 =「ID 非正值解析成功 → `:0`」，rust = 错误帧，单侧分叉真实。

裁决（维持 rust 现形）：该 >0 门与真 Redis `clientKillCommand`（对 ID 0/负数报 client-id should be greater than 0）对齐，属本项目「无旧兼容、严向收口」既定取向（先例 §149/§150 修复型分叉裁 rust 不盲从 C#），拒绝面更贴 Redis 且无运行期数据危害。MAXAGE 臂双侧同收敛零分叉——C# :352-357 仅 TryReadLong 失败回 syntax error，rust 同款仅 strict_i64 无值域门，负值入过滤器（`age > maxAge` 对非负 age 恒真）照常匹配，不随本条扩面、勿外溢误改。

危害与严禁回改：危害纯落治理面——未登记则严格逐字节对拍席遇 `CLIENT KILL ID 0` / `ID -3` 用例现形「C# `:0` vs rust 错误帧」无台账可引，易误判转写缺陷；后审席可能按「对齐原型」名义删 `.filter(|&v| v > 0)` 滤片（回改即丢真 Redis 值域门，非正值静默回 :0），或反向据真 Redis 语义误判 C# 侧缺陷另立错案。**严禁回改**：滤片为本条裁决锁，对拍席遇该形直引本条判有意偏差并跳过。与 §63 划界互指：§63 裁 CLIENT UNBLOCK 负数 ID（双侧同收敛回 `:0`，且系 BlockWaitFace 观察者域别名防护门），本条裁 KILL 过滤器 ID 非正值（单侧分叉，rust 拒帧）——同域异命令异裁，两门勿互抄勿并案；§32b 严格文法清单无涉。


锁面与锚回指：`wedb/wnode/tests/client_commands_tests.rs`——参数错误面块补 `CLIENT KILL ID 0` / `ID -3` 双例（错误帧逐字节 `-ERR client-id should be greater than 0\r\n` + PING 存活零杀；既有 `ID abc` 解析失败臂系双侧同帧非分叉面，正对照 `kill_by_id` 单杀 :1 在位不重复造轮）；`client_kill_nonpositive_id_and_maxage_negative_gates` 新用例钉 MAXAGE 负值形收 `:0` 无值域拒（若误加 ID 同款门即错帧现形）与 `CLIENT KILL ID <自身ID>` SKIPME 默认排除回 `:0` 合法路径不受影响。码内锚两处：`client_commands.rs` `parse_kill_filters` ID 臂回指本条并划界 §63、`network_clientunblock` 负数门注回指本条防混淆。
## 155. SCAN 出帧键一致读包裹仅包出帧键：C# ConsistentUnifiedStoreGetDBKeys.Reader 逐记录（含 base 判 Skip 的非出帧键）跑 pre/post，rust 仅包过 glob/TYPE/TTL 复判门禁即将 push 出帧的键（保守口径裁决；纯登记＋红绿双臂锁测，零行为改动）

工单 task/ing/wnode-scan-consistent-read-protocol-missing.md 立案登记（甄别 zc-fix-r16-scanconsist 席通过，定级 P2——SCAN 主键空间接线面补全，本条只裁接线形态与 C# 的逐记录差形）。编号顺编注记：立案时沙箱册尾 §153，顺位拟取 §155；合入窗复跑册尾若撞号按「先入库者得号、撞号让位不覆写」订正，码内 doc 锚随实况同步。

C# 一手形态：`garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:ConsistentUnifiedStoreGetDBKeys.Reader`（:271-277）对每条被扫记录执行 PreSingleKeyConsistentRead(hash) + 基础判定 + PostSingleKeyConsistentReadCallback——base 判 Skip（墓碑/glob 不中/TYPE 不中）的记录同样跑 pre/post 两侧。

Rust 现形：`wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:scan_cursor` 在键通过 glob、TYPE 过滤与 Degrade 臂 TTL 异步复判三道门禁后、`items.push` 出帧前，经 `wkv::StoreSession::with_session_consistent_read_with_prefix` 单源（内部 `single_key_around` 协议单点）包裹 pre/post；未出帧键（Dead/过滤不中/TTL 到期）零触协议面。

裁决与后果：保守口径成立依据——未出帧键客户端未观察、无撕裂前提；mssn 仅由出帧键抬升，只会低估不会虚高，低估水位仅令后继点读 pre 多等，不破会话前缀一致性（C# 语义中 mssn 抬高方向本就只来自已观察键）。且免对全库历史段死键/墓碑多版本逐条 pre 新鲜度等待的扫速塌缩（SCAN 遍历含大量不可出帧记录）。严禁按 C# 逐记录口径回改为对 Skip 记录也逐条包裹。

锁面：`wedb/wnode/tests/consistent_read_session.rs` 之 `test_replay_sketch_hit_gates_cross_sublog_scan`（红臂：点读抬会话序列号后 SCAN 触及滞后子日志出帧键，pre 超时经 wkv Err 通道上抛 ConsistentReadTimeout）与 `test_replay_sketch_hit_passes_scan_when_sublog_caught_up`（绿臂：前沿追平后 ka/kb 依序全出帧）；SCAN 接线面与点读族共用同一跨子日志草图夹具（Probe 双臂对照）。

## 156. wext_json JSON 空路径 `""` GET 双臂规范化为 `$` 同形回 `[<根>]`（取 C# Reader 快路形），与 C# 通用臂（带格式单路径／多路径内层裸根）之残余差为有意单源收口（登记级；SET 缺键空路径臂系对齐 C#/RedisJSON 之修复侧一并说明；严禁按 C# 自身两臂不一致回改）

工单 task/ing/wext-json-empty-path-set-get-forks.md 立案（甄别席 zc-fix-r16-jsonempty 通过定级 P1，审核裁定采「a 臂对齐 C#/RedisJSON、b/c 臂单点收口取快路形」）。编号顺编注记（一次让号）：本条初落册按执笔时沙箱基线册尾（最大号 §153）顺位拟取 §154；合入前 `git merge dev` 复跑册尾见 CLIENT KILL ID 非正值门条先入库占 §154、SCAN 出帧键一致读条占 §155，让位终取 §156——先入库者得号、撞号让位不覆写，条内与源码注释回指锚按本节号落笔（锚按内容引，行号容漂移）。

分叉形态与三臂裁定（C# 自身两臂即不一致，四锚亲验）：C# 对空路径 `""` 无规范化。SET 臂（`GarnetJsonObject.cs:Set` :358-368）仅 `pathStr.Length==1 && pathStr[0]=='$'` 走根替换，`""` 缺键（rootNode null）落 :364 回 `RESP_NEW_OBJECT_AT_ROOT`；既有键 `""` 经 `new JsonPath("")`.Evaluate（`JsonPath.cs:Evaluate` :122-127 空 filters 回 `[t]`＝[根]）命中根后 ReplaceMatches 替换。GET 臂 Reader 快路（`JsonCommands.cs` :165-178）单路径 `""` 进 `TryGetToWriter` → `SelectNodes("")` 回 [根] → 产出带 `[]` 包裹的 `[<根>]`；而通用路（带格式选项，`GarnetJsonObject.cs:TryGet` :292-299）同路径 `""` 回无包裹裸根，多路径分支（:147-173）内层走通用路同样回裸根。故 C# `GET k ""`（无格式）＝`[<根>]`、`GET k "" INDENT x`＝裸根、`GET k "" $.a`＝`{"":裸根,...}`——三态互不一致。rust 旧形：set（`json_object.rs`）`path_str=="$"||is_empty()` 空路径同走根分支、缺键建根回 OK；try_get 单路径臂 `p.is_empty()` 恒回裸根；多路径臂对 `""` 按空过滤器求值得 `[根]` 带包裹——三处逐臂偏离 C#，其中 SET 缺键建根一侧还偏离 RedisJSON（RedisJSON 拒空路径）。

Rust 侧收口与残余（修复＋登记混合）：a) SET 臂对齐 C#/RedisJSON 属修复非本条登记项——set 根替换分支删 `|| is_empty()`（既有键 `""` 顺延至通用臂经空过滤器命中根做替换，与 C# 等价），缺键 `""` 落 `root_node.is_none()` 守卫回 `RESP_NEW_OBJECT_AT_ROOT`；`json_commands/set_get.rs` 之 `json_set_need_initial_update` 门由「放行 `$` 与 `""`」收为仅放行 `$`，缺键空路径错误帧早退且不建键（守 §19 GEO STORE 族确立的「InitialUpdater 缺键不落空壳」同宗红线，不携 C# 初始更新器回 true 残留空对象壳之缺陷）。b)/c) GET 臂为本条登记项——`json_object.rs` 立 `path_is_root`（`"$"` 或 `""`）单源，try_get 单/多路径两臂统一将 `""` 规范化为 `$` 同形、恒回带 `[]` 包裹的 `[<根>]`（取 C# Reader 快路 `TryGetRoot` 形，消除 rust 自身单臂裸根 vs 多臂带包裹之不一致）。残余差：rust 带格式单路径 `GET k "" INDENT x` 回 `[<美化根>]` vs C# 通用臂裸根；`GET k "" $.a` 回 `{"":[<根>],...}`（键值带包裹）vs C# 多路径内层裸根 `{"":裸根,...}`。裁定取快路包裹形统一到双臂，裸根/带包裹之差为有意收口，非转写缺陷。

严禁回改：`path_is_root` 判定严禁并入 SET 根替换分支或 `need_initial_update` 门（二者仅认 `$` 硬字面量，并入即复活「缺键空路径建根回 OK」数据面收口分叉并偏离 C#/RedisJSON）；GET 双臂严禁退回 C# 通用臂裸根形以「对齐 C#」——C# 两臂本自矛盾，采快路 `[<根>]` 系裁定收口形态，回改即破 `json_empty_path_forks` 锁测。后续对拍席遇「`GET k ""` c# 带格式/多路径臂回裸根 vs rust 恒回 `[<根>]`」直引本条判有意偏差并跳过，勿转报写缺陷；遇「`SET 缺键 "" v`」两侧皆回错误帧（rust 已对齐，非分叉）勿误判。

锁面：`wedb/wext_json/tests/json_empty_path_forks.rs` 六形——`set_missing_key_empty_path_gate_rejects`（门仅放行 `$`、`""` 早退回 `RESP_NEW_OBJECT_AT_ROOT`）、`set_empty_path_on_missing_root_errors_without_creating`（对象层缺键 `set("")` 回错误帧且 `is_empty` 恒真＝EXISTS 0 投影）、`set_empty_path_on_existing_root_replaces_via_empty_filter`（既有键 `""` 经空过滤器替换根，与 C# 等价）、`get_single_empty_path_wrapped_both_arms`（无/带格式双臂同形 `[<根>]` 且与 `$` 逐字节同形）、`get_multi_empty_path_value_wrapped`（`{"":[<根>],"$.a":[..]}` 键值带包裹）、`get_multi_empty_matches_single_root_form`（多路径根臂值与单路径根数组同形，单源锁）；源码回指锚立于 `json_object.rs:path_is_root` 头注与 `set` 根替换分支、`json_commands/set_get.rs:json_set_need_initial_update` 门注。

## 157. 集合项经纪主循环 panic 有界重挂＋事件通道 ArcSwap 换装＋观察者登记定格订阅键组（wcol itembroker 自研自愈面；对标 wkv gc/reclaim.rs 有界重挂先例，C# CollectionItemBroker 无对位）

工单 task/ing/wcol-itembroker-main-loop-panic-dead-no-remount.md（P1）。C# `CollectionItemBroker` 的 `AsyncQueue<CollectionItemBrokerEvent>`（`libs/server/Objects/ItemBroker/CollectionItemBroker.cs:31`）为固定单实例、`StartMainLoop` 以 `Interlocked.CompareExchange` 一次性置位拉起、任务体无 panic 复位——rust 侧 `start_main_loop` 早期同构形态承袭此结构：`let _ = supervise_task(...)` 丢弃 Err panic 载荷，外层循环体（逐事件 `supervise_item` 未兜住的逃逸面，如周期 `clean_keys_to_observers`）一旦整体 panic，`main_loop_task_status` 永卡 `STARTED`，后续 `start_wait` 的 `CAS(NOT_STARTED→STARTED)` 恒失败即拒一切重拉；事件接收端 `rx` 经 `events_rx.lock().take()` 独占取出并随 panic 丢弃、再无重建，无界 `mpsc::List` 队列只增不减、`timeout=0` 阻塞客户端永挂——与 C# 同缺陷，wedb 判为正确性面死亡形态，采有界自愈收口。

Rust 侧三处刻意分歧（均自研，登记于此免重复审视）：
- 通道发送端改 `ArcSwap<MTx<List<CollectionItemBrokerEvent>>>`：panic 臂新建 `(tx, rx)` 后原子 `store` 换装发送端（罕见控制面路径），热路径 `enqueue_event`/`dispose` 仅 `load()` 无锁读最新 tx——遵数据面纪律「通知写入路径不加常驻同步锁」，故不用 `Mutex<MTx>`；C# AsyncQueue 无换装概念（本分歧系 rust crossfire 单消费 `AsyncRx` 不可复用、死亡即丢的必然承接）。
- `start_main_loop` Err 臂进入有界重挂环 `remount_main_loop`（对标 wkv gc/reclaim.rs `remount_reclaimer`：`REMOUNT_LIMIT=3`、连败耗尽留 `NOT_STARTED` 复位态与监督快照 panic 计数退出、绝不禁回「位永真拒重拉」；与 reclaimer 差异在留 `NOT_STARTED` 而非 `mounted=true`，令后续 `start_wait` 可直接重认领）；每轮 `recover_after_panic` 定序为「重建通道 → 存量补扫 → `CAS(STARTED→NOT_STARTED)` 复位」——重建必先于复位（否则并发 `start_wait` 重拉 `take` 到 None 即缺席早退），复位 CAS 只认 `STARTED`，`dispose` 已置 `MAIN_LOOP_DISPOSED` 终态时自然失败返回 false 留终态（保全票面第 4 条）。退避取 `yield_now` 协程让渡若干轮而非计时 `sleep`：compio `sleep` future 非 `Send`，与 `TaskSpawner::spawn` 的 `Send` 未来约束相冲（`ThreadSpawner` 需跨 OS 线程搬任务体），且票面纪律明令重拉走协程让渡、绝不内联驱动调度器。
- `CollectionItemObserver` 新增 `keys: OnceLock<Vec<Vec<u8>>>` 字段（C# 观察者不落键，订阅键组仅随 `NewObserver` 事件传递、`CollectionItemObserver.cs` 无此字段）：一次性 `set_keys` 于 `start_wait` 登记时定格、补扫侧无锁只读。用途——NewObserver 事件滞留死通道随重建丢弃时，`rescan_stale_observers` 据本字段对「仅入 `session_id_to_observer` 未挂任何键队列」观察者重投 NewObserver（类 2），对「仍在 `keys_to_observers` 非空队列」观察者重投 CollectionUpdated（类 1），全程走 `enqueue_event` 入新通道遵逐事件 `supervise_item` 隔离，绝不在补扫体内直接出件。`OnceLock` 承接写一次读多次、无观察者级常驻锁（`papaya`/`parking_lot` 皆不引）。

严禁回改：`recover_after_panic` 的「重建先于复位」定序不可调；复位 CAS 期望值恒为 `STARTED`（改期望即破 DISPOSED 终态保全，见票第 4 条）；`enqueue_event` 严禁改回 `Mutex<MTx>` 常驻锁或裸字段直发（破数据面纪律）；观察者 `keys` 字段严禁并入热路径加锁（`OnceLock` 无锁读是收口形态）。外层循环 panic 面无法经存储注入器确定性触发（单事件 panic 已由 `supervise_item` 兜住），故 `recover_after_panic` 按 gossip 测试面直调臂先例（对标 pub `dispose`）开 `pub` 供回归确定性直调，配套 `reset_main_loop_status`/`drain_events_for_test` 测试面。

锁面：`wedb/wcol/tests/collection_item_broker_tests.rs` 四形——`panic_recovery_rebuilds_channel_discards_dead_backlog_and_replays_stock`（千级洪峰随旧通道丢弃、新通道仅补扫 1 CollectionUpdated＋1 NewObserver 有界量、类 2 键组取 `observer.keys()`）、`dispose_terminal_state_rejects_main_loop_reset`（DISPOSED 拒复位/拒恢复）、`panic_reset_leaves_relaunchable_not_started_state`（NOT_STARTED↔STARTED 往复可重认领，根除死亡形态）、`remount_after_panic_serves_stuck_observer`（GateSpawner 泵起新循环消费补扫库存、滞留观察者出件解除悬挂）；模块内 `wedb/wcol/src/itembroker/collection_item_broker.rs::main_loop_panic_registers_in_supervision_snapshot` 钉外层循环 panic 经 `supervise_task` 计入 `bg_task_health` 快照 panic 计数。


## 158. wconf defaults.conf 配置旋钮缺席族与 CONFIG 回显两员刻意删员登记（登记级族目，零行为改动；循 §111 族目形制补 §148 随行注记二「配置旋钮族登记口」剩余覆盖）

工单 task/ing/wconf-defaults-knobs-absence-unregistered.md 登记（P4 登记级：纯台账＋两处码内注释锚，零行为改动零新机制。编号顺编注记：落册时册尾实况 §157，顺编取 §158，先入库者得号、撞号让位不覆写）。

立案背景：C# `garnet/libs/host/defaults.conf` 在册且 `garnet/libs/host/Configuration/Options.cs` 注册 CLI 的一批旋钮，与 `CONFIG GET *` 回显两成员，构成对账基准面；rust 侧均无对位旋钮或回显槽，此前本册全卷零登记（compaction-force-delete / aof-null-device / AofReplayDrift / FastCommit / LoggingFrequency / ParallelMigrate / PubSubPageSize / MainMemoryReplication / CheckpointThrottle / CompactionFrequency 各词全册零命中）。全部缺席项行为恒同 C# 默认部署形态（C# 默认不注册 CompactionTask、漂移屏障默认 -1 禁用、日志频率 5 仅影响进度日志节奏、串行迁移即 ParallelMigrateTaskCount=1；原稿另含「副本恒等待重同步即 ResumeWithData=false」一断言，经 wedb-boot-replica-recover-local-resume-ungated 案亲验与现状行为相反，已订正摘出归 §162，见 b) 族目随行注记），唯二可观测差异为回显少两员与该二名单查回空列表——属已裁决删员的协议回显投影，非缺陷，零运行期危害。危害纯在治理面：后续 defaults.conf 对账席与 CONFIG 面对拍席无台账可引，必然重复疑报、将已裁决删员误判转写漏项、或将恒默认字段误报假旋钮。本条裁决措辞全族统一：**行为面偏差即本条旋钮缺席或回显删员，严禁按 C# 形态回改、勿判转写漏项、勿重复疑报**。

### a) CONFIG GET * 回显删员两项

C# 一手形态：`ServerConfigType` 枚举双员在册——`COMPACTION_FORCE_DELETE`（判别 16，garnet/libs/server/Config/ServerConfigType.cs:30）与 `AOF_NULL_DEVICE`（判别 37 尾员，ServerConfigType.cs:62）。RuntimeServerConfig.cs:185 为前者注册 `Set("compaction-force-delete", Bool, 0, 1)` 运行时可设槽（Set 臂 `IsRuntime: true`，:101）、:157-158 为后者注册 `SetReadOnly("aof-null-device", Bool)` 只读槽（SetReadOnly 臂亦 `IsRuntime: true`，:109；formatter 读 `GarnetServerOptions.UseAofNullDevice`，defaults.conf:364 默认 false → 恒回 "no" 形）。`BuildRuntimeTypes`（:233-240）收全部 IsRuntime 成员入 `CONFIG GET *` 回显，默认部署下 `CONFIG GET compaction-force-delete` / `CONFIG GET aof-null-device` 单查各回 "no"。

Rust 删员裁决（wconf/src/server_config_type.rs:5-12 枚举刻意删二员，META 判别槽 36 席对 C# 38 席，差值即删二员、自 CompactionType 起判别值整体前移一位；wconf/src/runtime_server_config.rs 的 NAME_LOOKUP 纯编译期自 RUNTIME_TYPES 铸造（build_name_lookup :414-426），RUNTIME_TYPES 33 员（:431）——二名均不在册，单查回空列表、`GET *` 少二员，回显面系已裁决删员的投影）。严禁回改：在 rust 枚举/META 复活二员即把无对位行为投影回 CONFIG 面；亦勿把「单查回空」误判为 CONFIG 分派缺臂。
- compaction-force-delete：C# CompactionForceDelete 不移植——wedb 紧缩/移位经设备截断无条件物理回收历史段，C# forceDelete「紧缩后 commit AOF + Truncate 才真正删文件」次序无对位需求（AOF 为独立日志，hlog 可由 checkpoint+AOF 重建），旋钮无可承接行为，按「不留可写不可用的旋钮」直接删除。码内详注锚 wedb/wconf/src/server_config_type.rs:9-12，本条入册补台账回指。
- aof-null-device：rust 无 null AOF 设备对物——wdev 设备层（wedb/wdev/src）无 NullDevice 实现；C# `AllowDataLoss` 派生式 `UseAofNullDevice || (FastAofTruncate && !OnDemandCheckpoint)`（GarnetServerOptions.cs:654，UseAofNullDevice 字段 :435 默认 false）在 rust 只剩后项（全仓唯一算式自陈「本仓未移植 null AOF 设备」，wedb/wedb/src/server/cluster_provider/flags.rs:155-166 `allow_data_loss`）。紧缩经设备截断无条件回收，旋钮无可承接行为，同 force-delete 裁决直接删除。码内原仅半句注，本票随 §158 回指扩为完整删员理由。

### b) defaults.conf 配置旋钮缺席族九项（漂移双员并一条；随行订正注：原列第 7 项 ClusterReplicaResumeWithData 经 wedb-boot-replica-recover-local-resume-ungated 案改判「行为等效 C# 置 true（副本 --recover 重启恒本地续用）」非「恒同默认」，已从本族摘出另立 §162，勿重复登记；后续项次顺移）

统一形制：C# 一手锚（Options.cs 注册行 + defaults.conf 默认值行）→ rust 现状锚（码内自陈处或恒默认字段处）→ 恒同 C# 默认理由。
1. CompactionFrequencySecs：Options.cs:265 注册、defaults.conf:194 默认 0；消费 StoreWrapper.cs:967-969 注册门（`CompactionFrequencySecs > 0 && CompactionType != None` 才注册 CompactionTask，默认 0 即默认不注册常规紧缩周期任务）。rust 紧缩判定不设独立周期旋钮，码内自陈双点：wkv/src/gc/mod.rs:22-26（节奏单点为 GC 驱动轮次、阈值档位由 `GcConfig` 承载、默认 None 关闭常规阈值紧缩，对标 C# CompactionTask 默认不注册）与 wkv/tests/config_defaults.rs:2-5 头注（锚定收敛后默认值约定）。行为恒同。
2. CheckpointThrottleFlushDelayMs：Options.cs:418 注册、defaults.conf:319 默认 0（节流延迟禁用）。rust 全树零命中、无对位机制，恒同 C# 默认 0 形态；码内无自陈，本条登记即其唯一台账（勿判漏项）。
3. FastCommitThrottleFreq：Options.cs:423 注册、defaults.conf:322 默认 1000。rust 全树零命中；rust 提交路径无该节流频率概念，C# 默认值亦仅节流采样面参数，行为恒同默认形态（勿判漏项）。
4. LoggingFrequency：Options.cs:371 注册、defaults.conf:283 默认 5；消费 StoreWrapper.cs:125/:236（进度日志频率，仅影响日志节奏、零数据面）。rust 全树零命中，进度日志按 rust 自身日志纪律输出（勿判漏项）。
5. PubSubPageSize：Options.cs:146-148 `--pubsub-pagesize` 注册、defaults.conf:97 默认 4k。rust 全树零命中；pub/sub 日志页面无独立尺寸旋钮对物（勿判漏项）。
6. ParallelMigrateTaskCount：Options.cs:164 注册、defaults.conf:109 默认 1。rust 迁移为串行单任务投影，码内自陈 wedb/wedb/src/server/migration/migrate_driver/slots.rs:64（C# 并行扫描投影为串行，恒等效 C# 默认 =1）。
7. AofReplayDriftThreshold / AofReplayDriftCheckFreq（双员并条）：Options.cs:230-237 两旋钮 `--aof-replay-drift-threshold`（默认 -1 = 屏障禁用）/ `--aof-replay-drift-check-freq`（默认 1），defaults.conf:170/:173。rust 字段在册且 Default 播种对齐 C#（wconf/src/runtime_server_options.rs:138-141 字段、Default :203-204，常量 :40/:43 = -1/1），消费闭环 garnet_append_only_file.rs:83-84 → :339-344（`ReadConsistencyManager::new`）→ wnode/src/aof/readconsistency/read_consistency_manager.rs:62-81 门判定；唯 NodeArgs 无 CLI 旋钮（wconf/src/node_options.rs 零 drift 命中）、`runtime_server_options()`（:1424）投影无赋值——恒默认 -1/1，漂移屏障恒禁用，即 C# 默认部署形。字段注已随本条补「恒默认、CLI 面缺席」自陈。严禁把该恒默认字段误报假旋钮，严禁补 CLI 旋钮使其可动。
8. AofReplayBarrierSpinUs：Options.cs:239-241 注册、defaults.conf:176 默认 0（never spin）。rust 无字段无对物、全树零命中；C# 默认值即纯休眠臂，行为恒同（勿判漏项）。
9. MainMemoryReplication：Options.cs:446 注册、defaults.conf:340 默认 false——该旋钮 C# 系弃用别名：`GetFastAofTruncate`（Options.cs:1080-1087）日志自认 "MainMemoryReplication is deprecated. Use --fast-aof-truncate instead"，置真仅并入 fast-aof-truncate 语义。rust 弃用别名不接、无独立旋钮：码内自陈 wconf/src/node_options.rs:1441（FastAofTruncate 投影注「rust 不接 --main-memory-replication 弃用别名」），另有移植日志文案残字 wedb/wedb/src/server/replication/cluster_replication_session.rs:296；行为恒同 C# 默认 false＋`--fast-aof-truncate` 直取形（fast_aof_truncate 字段在册 runtime_server_options.rs:149-150）。台账措辞系「弃用别名不接」，非「全树零命中」。

随行注记（域互斥，不并案）：fast-migrate 旋钮缺席已登 §148 随行注记二（该注所称「配置旋钮族登记口（§111 收形先例）」即本条所补之剩余覆盖）；lua 相关四旋钮系 P2 真断链补线票（todo/wlua-lua-options-config-disconnect.md），`CONFIG SET index` 的 auto-grow 运行门系 zcode-r167c-confwire.md 案二（wnode/src/resp/config_commands.rs `handle_index_size_change` 现形已带 auto-grow 门注释，本票未触碰该面）——均不属本条。

锁面：登记级零新套件；既有锁钉住默认形——wconf/tests/default_single_source.rs:93-95 与 wconf/tests/garnet_server_config_tests.rs:54-55 断言 replay_drift 双员恒 -1/1（该二字段除 Default 播种外无其他写路径，投影侧零赋值，恒默认形态由构造钉死），wconf/src/node_options.rs::test_node_args_defaults（:1924）钉 NodeArgs 默认形，wkv/tests/config_defaults.rs 钉紧缩默认值约定；对账席后续 grep 台账词（compaction-force-delete、aof-null-device、replay-drift、LoggingFrequency、FastCommitThrottle、PubSubPageSize、MainMemoryReplication、CheckpointThrottle、CompactionFrequency、ParallelMigrate）命中本条即止；ResumeWithData 词经订正摘出后归 §162（命中该条即止，本条不再承接，亦勿以本条「恒同默认」措辞覆盖 §162「等效置 true」改判）。

## 159. 脚本窗内并发 ACL 改权停车三消费面统一收口为确定性窗内改权错误（中止语义 vs C# 逐条 fresh 续跑；架构根因同 §98 会话本地句柄谱系）

工单 wnode-lua-script-window-acl-park-starvation（P2，甄别席 zc-fix-r25-乙）登记，按 r25 审核裁定执行方案落地（三形 fail-closed、无撤权续跑逃逸、假违规仅中断脚本不断连）。

C# 一手形态：`ACL SETUSER` 对全局共享 `UserHandle` 就地 CAS 换引（`garnet/libs/server/Resp/ACLCommands.cs:226` while !TrySetUser、`garnet/libs/server/ACL/UserHandle.cs:49-56` Interlocked.CompareExchange），脚本面每条 `redis.call` 在调用点逐条现读新句柄（`garnet/libs/server/Lua/LuaRunner.Functions.cs:2879/:2959/:2993` acl_check_cmd 与 fallback 解析臂、`:3169` SET 快路、`:3221` GET 快路）——改权落在脚本执行中段时，下一条 redis.call 即以新规则裁决，放行照常执行、撤权 NOPERM，脚本其余部分正常推进。

Rust 现状与收口裁决：句柄连接本地挂载（`AclMount` 代数比较 + 网络泵 await 点查刷新，`wnode/src/resp/resp_server_session/auth.rs`），wlua VM 同步窗口内刷新臂不可达（存储点查禁同步收割红线），跨连接改权（含 AOF/回放 bump，`wnode/src/aof/aof_processor_store_ops.rs` ACL 臂）一经命中在飞脚本，门链 Parked 臂（`core.rs` process_messages 门链）回退游标出窗，旧形态三消费面各出异象：①fallback 空应答折 nil（`wlua/src/functions/redis.rs` dispatch_scripting_command_fallback 的 is_empty 臂）——脚本以 nil 续跑产出错值；②GET/SET 快路空应答落 Malformed → §97 Protocol 折叠文案 `protocol error`——并发改权伪装成协议违规误导排障；③`redis.acl_check_cmd` 纯位图判定在落后挂载上恒给陈旧布尔（撤权后仍回 true，与 C# :2993 现读反向分叉）。收口单机制：`RespScriptingApi::dispatch_resp`（`wnode/src/resp/resp_server_session/lua.rs`）消费环尾以无副作用判据（`acl_refresh_park_needed` 现读 × 本轮无应答产出）命中窗内停车时向 response 写专属错误帧，文案单源 `ScriptApiError::ACL_CHANGED_TEXT`（`wlua/src/api.rs`：`ERR ACL configuration changed during script execution, please retry the script`）；快路 get/set 以整帧判别上报新增形态 `ScriptApiError::AclChanged`（`runner/host.rs` vtable 随动；两快路径 Err 臂上抛该文案，不入 §97 ErrorReply 折叠臂）；`frame_and_acl_check` 经新预门 `ScriptingApi::acl_mount_stale`（默认 false，会话实现直读 `acl_refresh_park_needed`）同判据成立时不回陈旧 bool、改报同文案错误。游标已回退保停车命令存储零执行；`pending_acl_refresh` 标志不以 take 取走（审核裁定：取走即致 EVAL 收尾泵不再即时刷新），EVAL 收尾泵刷新臂照常闭环，下一笔外层命令按新规则 fresh 裁决。逐 call 协程重放全对齐 C# 续跑形态（ScriptYieldTag 刷新形 + 同点重跑语义）另立票评估，本票不做。

后果与严禁回改：窗口内并发改权即确定性中断在飞脚本（相对 C# 续跑形态为登记内中止语义分叉，架构根因=会话本地挂载+同步窗口不可 await，与 §98 会话本地句柄裁决同谱），三形态错误文案统一、假协议违规灭失，§97 Protocol 折叠语义据此收缩至真协议损伤面。严禁：①把 `AclChanged` 回折进 `ErrorReply`/`Protocol`（假违规文案回潮）；②改以 `take_pending_acl_refresh` 作窗口判据（EVAL 收尾泵刷新丢失）；③给窗口内同步 await 点查刷新开闸（收割红线）；④`acl_check_cmd` 回改陈旧布尔终审；⑤按 C# 续跑形态在本票面私自引入重放语义。口径注记随修：`auth.rs` 预门口径注释与 `acl_permits` 位图面注记已订正为「命令边界闭环成立、窗内为中止语义」。

锁面：`wnode/tests/lua_script_window_acl_park.rs` 四锁（fallback 形错误帧+键未落+收尾泵刷新后外层 NOPERM、SET 快路形、GET 快路形断非 PROTOCOL_TEXT、acl_check_cmd 形不复报陈旧 true）——真实驱动：脚本内 BLPOP 协程挂起打开注入窗 × AclStore 真源写口 bump × `wnode_test::drive_pending_parks` 泵臂闭环，零假 mock；回归保持绿：`lua_script_tests`/`redis_call_fast_path`/`acl_tests`/`requirepass_test`/`parked_auth_acl_failed_calls`/`multi_exec_script_reentry_lock_mode`/`exec_replay_hello_acl_txn_gate`。

## 160. CLUSTER MIGRATE 接收臂 C# migrateState 四臂吞错恒 +OK vs rust 全臂显式 ERR 判败的修复型分叉（登记级；严禁按 C# 形态回改 rust 应答面）

工单 task/ing/cluster-migrate-recv-swallow-ok-unregistered.md（P3 登记级，甄别席 zc-fix-r25-甲/审核席 zcode-r20-review-migratereg 双侧源码亲验）登记，零行为改动：纯台账 + 两处注释回指锚（`wedb/src/server/cluster_session/migrate.rs` 槽门臂、`wedb/tests/cluster_migration.rs::cluster_migrate_recv_requires_importing_slot` 文档注释）。编号顺编注记：落册时册尾实况最大号 §159，顺编取 §160，先入库者得号、撞号让位不覆写，条内与两处码注释回指锚随实况同步（锚按内容引，行号容漂移）。

C# 一手形态（符号锚，防行号漂移）：`garnet/libs/cluster/Session/RespClusterMigrateCommands.cs:NetworkClusterMigrate` 之 Process 局部函数四臂全走 migrateState 置位吞错、不参与应答——a) 载荷截断早退（GetSerializedRecordSpan 失败 return）；b) 槽位非 IMPORTING 拒收（IsImportingSlot 不成立仅置 migrateState=1 跳过写入）；c) 记录写回结果丢弃（`_ = basicGarnetApi.SET(in diskLogRecord)`）；d) RangeIndex 流处理失败（功能未启用臂与 ProcessRecord 回 false 臂均置 migrateState=1）。Process 返回（含早退）后主命令臂在 `TryWriteDirect(CmdStrings.RESP_OK)` 应答点**无条件回 +OK**——发送端对上述任何错误形态一律收 +OK；仅意外 kind 抛 InvalidOperationException 掐连一途可察觉（FastMigrate 恒 false 走同步形，§148 随行注记 2 在案）。TrackImportProgress 纯日志计数不参与应答。上游存在真实丢键窗口：目标端槽位被并发 recover 回 STABLE 后收到在途批次，吞记录回 +OK，发送端停等收 +OK 即删源键并继续交权编排，该批键两端皆无。

Rust 现状（修复型分叉，方向更严）：接收臂 `cluster_migrate_slow`（`wedb/src/server/cluster_session/migrate.rs`）全臂显式 ERR 判败——载荷解码失败回 `ERR Invalid migration payload`；头声明槽门任一非 IMPORTING 整体拒收回 `ERR Slot X is not in importing state`（先复位双接收态、绝不部分写入）；导入核心 `import_migration_frames`（`wedb/src/server/migration/frame_import.rs`）旧 TTL 清退/记录写回/TTL 回填三败臂均回 `RESP_ERR_SLOW_PATH_STORAGE`，RI 流失败回显式错误帧。发送端停等闭环：`send_payload_and_wait` 非 OK 即判败 → `try_recover_from_failure` 置远端 STABLE、本地 Fail、dispose，不删源键不交权，丢键窗口结构性消除。

裁决与严禁回改：rust 侧属上游缺陷修复型分叉（与 §48 DELKEYSINSLOT 过滤、§143 会话入口摘闸同族），**严禁按 C# 吞错形态回改 rust 应答面为恒 +OK——回改即复活「源端删键后目标端静默丢键」窗口，改回才是真回归**。双侧对拍遇「同批次 C# 回 +OK / rust 回 -ERR（Invalid migration payload／is not in importing state／RESP_ERR_SLOW_PATH_STORAGE 各臂）」直引本条判有意偏差并跳过，勿重复立案、勿转报转写缺陷。查重净：§13（槽管理命令族）、§86（MIGRATE timeout 三态）、§148（发送侧命令面）均非接收臂应答面，本面全册此前零登记。

锁面：既有帧锁 `wedb/wedb/tests/cluster_migration.rs::cluster_migrate_recv_requires_importing_slot`（-ERR Slot … is not in importing state）与 `migrate_fail_inject.rs` 失败注入族复跑全绿即闭环，本条零新增行为断言；两处原半真锚注释（仅写「对标 C# IsImportingSlot 拒收」未提恒 +OK）已订正补回指本条。

## 161. wext_json JSON 解析深度接受带与 C# 分叉：C# 默认 MaxDepth 64 拒超深、rust wext_json 实际无逻辑深度门（sonic `Value` 原生快路绕过 MAX_ALLOWED_DEPTH），>64 层 C# 拒/rust 收为预期宽向分叉（登记级；严禁按 C# 加 O(n) 预扫回改；订正本票议题「64–255 带 / 256 拒」前提）

工单 task/ing/wext-json-parse-depth-band-64-255.md 立案登记（甄别席 zc-fix-r25-乙 定级 P4 登记级、审核裁定乙案：零行为改动＋头注单源说明＋测试锁三档）。编号顺编注记：落册时沙箱册尾实况最大号 §160（cluster-migrate-recv 条），顺编取 §161，先入库者得号、撞号让位不覆写，合入 dev 前 `git merge dev` 复跑册尾随实况同步（锚按内容引，行号容漂移）。

C# 一手形态（符号锚）：`garnet/modules/GarnetJSON/GarnetJsonObject.cs` 四处 `JsonNode.Parse`（:360 Set 根替换、:391 Set 补插子节点、:409 Set 覆写匹配节点、:95 反序列化）全链不带 `JsonDocumentOptions`，吃 System.Text.Json 默认 `MaxDepth=64`——嵌套 >64 层抛 JsonException，被 Set/TryGet 的 catch(JsonException) 收为错误帧、连接存活。锚点订正（审核席）：议题原列三处 Parse 实为四处（漏计 :409）；且 `:95` 系持久化恢复（Deserialize/Recover）构造臂，非命令 SET/GET 错误帧证据位，本条只引作「C# 默认口径 64」之构造锚，不宜作错误帧证据。

Rust 现状（本机沙箱现码亲验订正，方向更宽）：wext_json 唯一解析漏斗 `parse_dom`（`wedb/wext_json/src/json_object.rs`）以 `sonic_rs::Deserializer::from_slice(payload).use_rawnumber().deserialize::<Value>()` 装载，全 crate grep 独立深度门零命中（仅 json_path/parser.rs 无关 array_depth）。**本票议题与审核初判「接受带全由 sonic-rs 0.5.10 `src/serde/de.rs:23` MAX_ALLOWED_DEPTH=u8::MAX=255 承载、256 层回 RecursionLimitExceeded」经现码亲验不成立**——`de.rs:23` 之 `with_depth_limit`（:228-239，:42 装载）仅挂在通用 `Deserializer` 的 `deserialize_any` 之 visit_seq/visit_map 递归臂（:481/:488），而 `sonic_rs::Value` 的 `Deserialize`（`value/de.rs:62-79`）走 `deserialize_newtype_struct(TOKEN, ValueVisitor)` 原生 DOM 快路、整段绕开 `with_depth_limit`，实测 `from_slice` 对 255/256/300/500/1000/5000 层对象（`{"a":{…}}`）与数组（`[[[…]]]`）载荷全部 Ok。故 wext_json 实际接受带非「64–255」而是「64–∞（无逻辑上限）」：唯一残余约束是原生 DOM 递归装载与 `Value` 递归 Drop 的 OS 线程栈深——默认 libtest 线程栈（8MB）下对象嵌套约 180–200 层即触发栈溢出 abort（SIGSEGV/SIGABRT），属崩溃面而非错误帧，与本条「接受/拒绝」应答轴正交、另轴评估。宽向事实层对拍结论不变：>64 层载荷 C# 一律拒（错误帧）、rust 一律收（落库＋GET 正常出帧），65–255 带系此无界宽接受之严格子集。

裁决与严禁回改：登记级，维持 rust 现形（宽向），循 §117d「rust 值域向更宽」宽向分叉在册先例。**严禁按 C# MaxDepth=64 在 SET/GET 热路径（`json_set_need_initial_update` 载荷预检、`from_slice`/`set`）加 O(n) 深度预扫回改**——甲案预扫给 SET 热路径加整串遍历，违零开销纪律；且预扫亦无 255 门可依（议题前提已订正），属双错回改。深度承载唯一性说明随本票落 `json_object.rs::parse_dom` 头注（引 sonic `value/de.rs:62` 快路为单源事实、点明 de.rs:23 门不经由触发），杜绝后人误信「有 255 门」或散落裸数字/补预扫。

对拍口径：遇 65 层及以上（含 65–255 带与 >255 带）「C# 回错误帧拒写 / rust 收并落库、GET 正常出帧」直引本条判有意宽向分叉并跳过，勿转报转写缺陷；勿据议题旧「255」数字误建「256 必拒」假锁（现码 256 收，锁测须钉「255/256/300 全收」之真形）。查重净：deviations 全册 JSON 深度面此前零登记（§124c 链深系 TLS webpki 他域、§156 系空路径 GET 归一他轴）。

锁面：`wedb/wext_json/tests/json_deviation_locks_tests.rs` 深度接受带锁族——`depth_64_65_csharp_reject_band_rust_accepts_set_get`（64/65 层 SET 根替换臂与 GET 回读双臂均收，default 栈安全区，钉 C# 界带 rust 收）、`depth_255_256_300_no_sonic_gate_rust_accepts`（于大栈线程内 from_slice＋set 三层全 Ok，订正议题「256 拒」前提、钉 MAX_ALLOWED_DEPTH 对 Value 快路不触发之真形；线程大栈系规避默认栈崩溃面，非放宽契约）。零行为改动票，不动 set_get.rs/from_slice 行为码。


## 162. 集群副本 `--recover` 重启恒本地续用数据面无 ClusterReplicaResumeWithData 门控（boot 装配序角色无关前置恢复，等效 C# 置 true 恒开形；刻意改良裁决，严禁未重构装配序按 C# 门控回改）

工单 task/ing/wedb-boot-replica-recover-local-resume-ungated.md（P4 登记级，甄别席 zc-fix-r25-乙 双侧源码亲验双失真）登记，零行为改动：纯台账 + 一处码内注释订正锚（`wedb/wedb/src/server/replication/replication_manager.rs::recover_async` 文档注释自陈失真已改判回指本条）+ 双节点现状锁测（`wedb/wedb/tests/replica_recover_local_resume_ungated.rs`）。编号顺编注记：本棒落册时沙箱册尾实况最大号 §160、初取 §161；追平 dev 见 §161（wext_json 深度带条）先入库得号，本条撞号让位顺编 §162、不覆写，条内与码注释/锁测回指随实况同步（锚按内容引，行号容漂移）。

C# 一手形态（符号锚）：`garnet/libs/server/StoreWrapper.cs:RecoverAsync`（:377-399）集群分支 `EnableCluster && Recover` 委托 `clusterProvider.RecoverAsync()`、单机分支（无角色概念）才无条件检查点+AOF 恢复全量重放；`garnet/libs/cluster/Server/Replication/ReplicationManager.cs:RecoverAsync`（:511-531）按角色分派——PRIMARY 臂恒 `RecoverCheckpointAndAOFAsync()`，REPLICA 臂仅当 `serverOptions.ClusterReplicaResumeWithData` 为真才执行本地数据面恢复（检查点+AOF 设备恢复+InitializeIf+全量重放+`replicationOffset.SetValue(replayedUntil)` 位点回填），否则空库等待 `rm.Start()` attach 后由主端全量重同步；`garnet/libs/server/Servers/GarnetServerOptions.cs:ClusterReplicaResumeWithData`（:646）默认 false（defaults.conf:530 同、Options.cs:700 注册 CLI）。即 C# 默认部署形态：副本带 `--recover` 重启为空库、位点 0（位点回填仅发生在 RecoverCheckpointAndAOFAsync 内）。

rust 现状（登记锚）：数据面恢复整体前置于角色可知之前——`wedb/wedb/src/server/boot.rs:run_cluster_server` 无条件调用 `StorageSessionProvider::open_from_args`（现册 :90，原案 :78 行号漂移），其 (recover, aof) 四路分派（`wnode/src/service.rs:open_from_args_with_config` :1640-1677）仅凭命令行旗标：(true, true) 臂 `open_recovered_with_config_and_aof`（:1501-1611）对未来的副本角色同样执行检查点恢复＋向量回建＋WAL 设备恢复＋initialize_if＋`replay_aof(u64::MAX)` 全量重放并点亮 `recovered_aof_tail`；节点角色迟至 boot 装配序 `initialize_cluster_config` 载入集群拓扑后（现册 :276 前后，原案 :260-269）方可知；装配尾段（现册 :291-300，原案 :281-283）对主/副本两角色一律 `rm.set_current_replication_offset(recovered_aof_tail)` 回填位点再 `recover_async`；attach 臂 `start_replication_attach`（`wedb/wedb/src/server/cluster_provider/replication.rs:290-356`）再按协商链（sameHistory/trunc_floor 预检，§116 在案）裁决增量或全量。净效果：**rust 集群副本 `--recover` 重启恒为「本地续用」形态，等效 C# `ClusterReplicaResumeWithData=true` 恒开**；该旋钮 rust 配置面未落地无对位旗标。

裁决：确认上述形态为**刻意改良**并据此钉死——消除副本重启至 attach 完成窗口的空库读窗（C# 默认该窗内副本 GET 皆空，rust 恒可读到本地重放尾数据），并免主端每次副本重启必全量重同步的负载压力（rust 走增量接续）；防历史发散由 §116 协商链（sameHistory/trunc_floor 预检）与检查点置换链承接，无新增数据危害面。三面可观测分叉（a. 重启窗内陈旧本地读 vs C# 空库；b. replicationOffset 基线 = 本地重放尾 vs C# 默认 0，为 gossip 广播与 failover 判定输入；c. 主端负载增量 vs 恒全量）统一归本条收口：后续对拍席见「副本重启后即刻读旧键命中／INFO replication `master_repl_offset` 非零且等于停机前本地尾／主端无全量重发」一律判本条有意偏差，勿重复立案、勿判转写漏项。

护栏（严禁裸回改）：若日后按 C# 门控形态回改（REPLICA 臂默认不本地续用），**必须同步重构 boot 装配序——把「角色可知」（`initialize_cluster_config` 拓扑载入）前移至 `open_from_args` 之前，使恢复臂获得角色输入**；严禁在现行装配序下仅给 `open_from_args` 或 `recover_async` 加角色门——恢复时刻角色恒不可知，门控对两角色恒判非主，会把主角色也打成空库、制造比所修更重的新分叉。回改同时系对本条裁决的行为回退，须先在台账翻案。

分叉订正链：原 §158 b) 族第 7 项与立案背景断言「该旋钮缺席行为恒同 C# 默认部署形态、副本恒等待重同步即 ResumeWithData=false」经本票亲验失真（现状恒等效置 true），已随本条就地订正并从族目摘出归本条；原 ing 票 wconf-defaults-knobs-absence-unregistered 已归档（task/done/），归档票不回改，以 §158 现册为准。

锁面：新增专测 `wedb/wedb/tests/replica_recover_local_resume_ungated.rs::replica_recover_restart_resumes_locally_before_attach`（双节点真 TCP 生产装配拓扑：副本 attach 追平后停机 → 同目录 `--recover` 重启，断言 attach 未发起态下本地键引擎直读可读、INFO replication `master_repl_offset` 等于停机前本地重放尾（非零）且主端 `connected_slaves` 为 0；对照组同目录不带 `--recover` 冷重启为空库待全量）锁死现状形态，任何回改须先过该锁与本条。