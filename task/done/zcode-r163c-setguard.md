甄别结论：通过（甄别席 zc-fix-r16-setguard，2026-09-26）定级 P2
核验记录（逐锚现码复跑）：
rust 锚全成立：raw.rs:dispatch(198)/set_vector_guard(59) 确在锁窗外裁决，Degrade 前先判；set.rs:network_set(212)/network_setexnx(452)/network_set_ex(479)/network_set_conditional(523) 签名均不接 vector，窗内仅 probe_alive_domain（ttl_sync.rs:591-658 只走 String/ObjectEnvelope/Meta+TTL 三域无第四态），同窗 network_setnx(433) 已用 probe_alive_with_registry 对照坐实缺口；VADD 路径（resp_server_session_vectors.rs:862 起）纯登记表写入不取 wkv rmw 窗，派发层放行至取窗间并发 VADD 双成功竞态真实。
slow.rs:452-458 锚成立：reg_hit 窗外无条件 clear_vector_registry 先于 slow_set_conditional 之 rmw_window(284)；现序推演 SET k v XX 命中向量键：清退→三域探针判缺→回 nil 且向量索引已毁，确定性数据丢失；SET NX 同臂误判缺写 +OK，NX 契约背离。
slow.rs:120-132 blind_write_gate 锚成立：clear(130) 先于 rmw_window(131)；array_commands.rs:1179-1196 MSET 慢臂确为 rmw_window_sorted 持窗后窗内清退，机制割裂对照成立。
c# 锚全成立：BasicCommands.cs NetworkSET:387/NetworkSETEX:533/NetworkSETEXNX:605/NetworkSET_Conditional:772、MainStoreOps.cs SET_Conditional:258 亲验；向量记录确驻 MainStore 同槽（VarLenInputMethods.cs:194 置 VectorManager.RecordType，ReadMethods.cs:130/UpsertMethods.cs:72 同判），C# 锁内 DELETE 重投无锁外裸清退，契约引用无误读。
非重复非灭失：deviations.md §99 仅裁 TTL 族三域收敛可观测代价，未覆盖本轴；task 各池仅 reject/zcode-r163c-rangecold（SETRANGE 冷读轴）同批异轴，r161c 案一仅收编 SETNX/MSETNX/RESTORE，本票为 SET 覆写族留口增量。
架构合规：方案一复用 probe_alive_with_registry 既有单源折叠（与 r161c 同形），方案二三对齐 MSET 窗内清退既有标准，诚实降级 Ok(false) 非假桩，无第二套机制、无过度设计；测试计划三项硬断言可闭环。
小疵不挡：票面「dispatch_fast」现树函数名为 dispatch（涉及代码清单已正写 raw.rs:dispatch），行文笔误，锚义无误。

审核结论：通过，定级 P2。
确证快臂锁窗内探针缺失第四态（VectorRegistry）致与并发 VADD 双成功并被永久 WRONGTYPE 遮蔽；慢臂 string_slow 破坏性预清退倒置致 NX/XX 语义反转与存活向量索引静默损毁；blind_write_gate 预清退漏在锁窗外。方案执行路径清晰，供 task/fix.md 消费。

SET 覆写族锁窗内向量第四态探针缺失与慢臂预清退乱序致 NX/XX 语义反转及永久 WRONGTYPE 遮蔽

问题分析：
1. Garnet 契约对齐：
C# Garnet 中，向量索引记录（VectorManager.RecordType）同驻 MainStore 主记录槽位，与 String 共享同一底层存储与记录锁。在 BasicCommands.cs 的 NetworkSET / NetworkSETEX / NetworkSET_Conditional 执行路径中，所有针对记录的读写、NX 存在性条件判定以及旧记录清退，均在统一的记录闩/事务临界区内原子执行。当键为向量记录时，SETNX 直接返回 0 且绝不损坏向量记录；无条件 SET 与覆写在排他锁临界区内先删除旧向量记录再写入新字符串值，绝不存在锁窗外裸清退或检查与写入分离的竞态窗口。
Redis 标准协议中，SET key val NX 语义与 SETNX 完全同构，当且仅当目标键完全不存在时方可写入；若目标键已存在（无论何种类型），必须保持原键完整且返回 nil；SET key val XX 当且仅当目标键存在时覆写，若不存在返回 nil 且绝不产生破坏性副作用。

2. 工程现状确证：
wedb 中向量登记表独立驻留于内存 VectorManager（ConcurrentMap / VectorRegistry），未在 wkv 主键值域建槽，需依赖第四态探针进行统一存活裁决。
在轮 161 丙（zcode-r161c-msetnx.md 案一）收敛了 SETNX、MSETNX、RESTORE 窗内存活探针之后，SET 覆写族留口仍存在以下三处同形缝：
第一，快臂锁窗内探针缺失第四态与 TOCTOU 竞态。wedb/wnode/src/resp/garnet_api/raw.rs 在派发层 dispatch_fast 中仅在锁窗外通过 set_vector_guard 检查登记表；若派发层检查时键尚不在向量表中，放行进入 network_setexnx -> network_set_conditional。快臂获取 try_rmw_window 后，窗内仅调用 probe_alive_domain（仅扫描 String、ObjectEnvelope、Meta 三域与 TTL），未传入 vector 句柄，完全缺失向量登记表第四态。若在派发层放行后至快臂取窗之间，并发的 VADD 命令向 VectorRegistry 注册了键，窗内 probe_alive_domain 判定键不存在，SET NX 条件成立并写入字符串回复 +OK，造成 VADD 与 SET NX 并发双成功，新写入的字符串值被派发层值域门永久报 -WRONGTYPE 遮蔽。同样，无条件盲写 network_set / network_set_ex / network_setex 快臂在锁窗内亦无登记表复验，并发 VADD 逃逸致双域并存。
第二，慢臂 string_slow 窗外破坏性预清退致 NX/XX 语义反转与数据静默损毁。wedb/wnode/src/resp/basic_commands/slow.rs:452-458 在未获取 rmw_window、未裁决 NX/XX 条件之前，无条件调用 clear_vector_registry 删除了存活的向量索引。对于 SET key val NX，向量索引被提前抹除，导致其后 slow_set_conditional 判定键不存在、NX 错误通过并覆写为字符串回复 +OK，彻底背离 NX 契约；对于 SET key val XX，向量索引被提前抹除后，slow_set_conditional 判定键不存在，回复 nil 拒绝写入，但原向量索引已遭静默删除，造成用户存活数据丢失。
第三，盲写慢臂 blind_write_gate 预清退与取窗乱序。slow.rs:120-132 中 clear_vector_registry 在 storage.batch.rmw_window 之前执行，清退漏在锁窗之外，清退与取窗之间存在并发天窗，并发 VADD 可再次插入导致双域并存。相比之下，array_commands.rs:1179-1196 的 MSET 慢臂是在持窗后于临界区内清退，blind_write_gate 机制未对齐。

3. 逻辑危害确证：
其一，并发数据遮蔽与静默错态。快臂 SET NX 与 VADD 并发时双成功，已确认 +OK 的数据被 WRONGTYPE 永久遮蔽不可读。
其二，数据丢失与语义破坏。慢臂 SET XX 在回复 nil 的同时静默摧毁已存在向量索引；SET NX 在目标键存在时摧毁向量并覆写成功回 +OK。
其三，锁窗外状态篡改与机制割裂。盲写慢臂与条件写慢臂在锁窗外提前执行破坏性清退，违反临界区内清退原则。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/basic_commands/set.rs:network_setexnx
wedb/wnode/src/resp/basic_commands/set.rs:network_set_conditional
wedb/wnode/src/resp/basic_commands/set.rs:network_set
wedb/wnode/src/resp/basic_commands/set.rs:network_set_ex
wedb/wnode/src/resp/basic_commands/slow.rs:string_slow
wedb/wnode/src/resp/basic_commands/slow.rs:slow_set_conditional
wedb/wnode/src/resp/basic_commands/slow.rs:blind_write_gate
wedb/wnode/src/resp/garnet_api/raw.rs:dispatch
wedb/wnode/src/resp/garnet_api/raw.rs:set_vector_guard

对应 c# 文件与函数：
garnet/libs/server/Resp/BasicCommands.cs:NetworkSET
garnet/libs/server/Resp/BasicCommands.cs:NetworkSETEX
garnet/libs/server/Resp/BasicCommands.cs:NetworkSETEXNX
garnet/libs/server/Resp/BasicCommands.cs:NetworkSET_Conditional
garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:SET_Conditional

精炼执行方案：
1. 传导 vectorManager 句柄至 SET 快臂全族，快臂锁窗内对齐第四态探针。修改 raw.rs dispatch 传递 vector 至 network_set、network_setexnx、network_set_conditional、network_set_ex；在 network_set_conditional 锁窗内将 probe_alive_domain 升级为折叠第四态判定（或复用 probe_alive_with_registry）：若为 NX 且第四态在场，直接出 nil 帧并返回 Ok(true)，绝不写值亦不降级；若为 XX 或盲写且第四态在场，因快臂无法同步清理登记表，在锁窗内返回 Ok(false) 诚实降级慢路径。
2. 纠偏慢路径条件写判定序并移入锁窗内。删除 slow.rs string_slow 选项形态分支在窗外的 clear_vector_registry 预清退；改由 slow_set_conditional 在获取 rmw_window 后统一裁决第四态：NX 命中第四态直接出 nil 成功帧返回，零副作用保留原向量记录；XX 或盲写在锁窗内且条件满足后，复用 clear_vector_registry 完成异步清退，再行覆写。
3. 修正 blind_write_gate 锁序。调整 blind_write_gate 执行顺序为先获取 storage.batch.rmw_window，再在持窗临界区内调用 clear_vector_registry，严格对齐 MSET 窗内清退标准。
4. 编写回归与并发测试验证点。覆盖 SET NX 命中向量键回复 nil 且向量完整、SET XX 命中向量键覆写成功且向量清理、SET 盲写与 VADD 交叠无双域并存三项硬断言。

视角结论:有增量

合入哈希：a69ec3c 收口形态：SET 覆写族快臂窗内第四态折叠（NX 出 nil 保留登记、XX/盲写窗内清退后覆写、GET 族 WRONGTYPE），slow.rs 窗外预清退删除、blind_write_gate 对齐 MSET 窗内清退单源标准。
