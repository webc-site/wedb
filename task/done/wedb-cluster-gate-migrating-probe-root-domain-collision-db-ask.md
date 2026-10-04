终态：已合入 dev（merge fx0N 系，2026-09-27）。8b4d5c9 SlotVerifyRequest/ClusterSlotVerificationInput 补 ns/db,探针 enter_batch 前 set_context 落域(同 worker_state 先例);4 锁测含碰撞库 Serve 断言

甄别结论：通过 | 定级 P1 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：复用 set_context 既有形态零新机制；txn.rs 实路径多一层目录 wnode/src/resp/resp_server_session/

审核结论：通过（锚点分置订正已并入：SlotVerifyRequest 实定义于 cluster_manager_slot_gate.rs:52-64，slot_verify.rs:52-62 系 SlotVerifySessionState）（亲验：探针二臂 store.new_session() 不落域、会话构造 set_context(0,0)、session_prefix 取 virtual_domain 根域物理键实读坐实；鸽笼成立——slot_of 将 (u64,u64) 混入 14 位槽、MIGRATION_SUPPORTED_SLOT 只放 (0,0) 域进迁而门评按槽不甄别域，危害链票面已诚实限定迁移窗非夸大；C# Exists 经会话当前库每库独立实例天然同域反证不成立；五池独案，域钉族 §96/§99/§118 不罩请求侧门评探针；方案复用既有 set_context 单点、探针仅 MIGRATING 本地臂执行、补两标量栈上形零堆分配不违热路径零开销）

MIGRATING 门评存在性探针恒锚根域：同槽碰撞库被默认域迁移窗口误判键已迁走回 ASK，跨域误路由与数据发散

问题分析：
1 Garnet 契约对齐：C# 集群门评 CanOperateOnKey（garnet/libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:116-128）在 CanAccessKey 放行后以本会话存储口 Exists(key) 判键在场，再由 :51/:101 两臂落 OK 或 ASK。C# 每库独立存储实例、会话 basicGarnetApi 恒绑连接当前 activeDB，定槽与存在性判定天然同域，不存在探针落错库的形态。
2 工程现状确证：wedb 库级定槽将 (ns, db) 混入 14 位槽空间（wbase/src/hash_slot.rs:62 slot_of），不同域对同槽碰撞按鸽笼必存；迁移域门禁（wedb/wedb/src/server/cluster_session/mod.rs:520-531 MIGRATION_SUPPORTED_SLOT）只放默认域 (0,0) 槽进 MIGRATING，但门评按槽号裁决、不甄别请求域：(0,0) 库迁移窗口内，slot_of(ns', db') == slot_of(0, 0) 的碰撞库每条数据命令经 evaluate_single_key 臂（wedb/wedb/src/server/cluster_manager_slot_gate.rs:241）与 evaluate_multi_key_gate 臂（同文件 :287、:332）进 resolve_can_operate（:403-441），其存在性探针 probe_key_alive（:449-459）与 probe_key_alive_async（:463-477）就地 store.new_session() 后从不落域，会话恒锚根域 (0,0)（wkv 会话构造 vns/vdb 初值皆 0，wedb/wkv/src/session/mod.rs:270 附近），以根域前缀读裸用户键——碰撞库键在其自身 (vns', vdb') 域的真实在场态全被漏判。请求侧组装臂 input 仅携 slot 不携域（wnode/src/resp/resp_server_session_slot_verify.rs:100/:136/:176 经 active_db_slot，wnode/src/resp/resp_server_session/core.rs:740；事务臂 txn.rs:379 同形），门评链 SlotVerifySessionState（wedb/wedb/src/server/slot_verify.rs:52-62）与 SlotVerifyRequest（cluster_manager_slot_gate.rs:52-64，锚点分置订正）均无域字段，探针无从落域。同仓既有落域先例两枚：wedb/wedb/src/server/cluster_manager_worker_state.rs:73-75 与 wedb/wedb/src/server/cluster_session/slot_mgmt.rs:90-91 皆为临时会话逐域 set_context 形态，独此臂漏接。
3 逻辑危害确证：默认域任一迁移窗口内，碰撞库读命令键明明在场仍被误判已迁走回 -ASK 重定向至迁移目标；目标端 ASKING 放行后在碰撞库域执行——该库数据从未迁移，读 miss 回 nil（在场键读到空），写落错节点与源端同键各写各的，主从与分片间静默数据发散；根域恰有同名键在场时反向放行，sketch 布隆判据按裸键名折叠（wedb/wedb/src/server/migration/migrate_session.rs:175-202 can_access_key，键哈希不含域），碰撞库同名键写被默认域在迁条目挂等（Transmitting 拒写/Deleting 全等）。危害随迁移窗口与碰撞库名同键集放大，非迁移期不可见，现锁测（wedb/wedb/tests/cluster_slot_verify_wait.rs 等）全为根域或异槽用例，无同槽碰撞域对拍。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/cluster_manager_slot_gate.rs:probe_key_alive（:449-459）/probe_key_alive_async（:463-477）/resolve_can_operate（:403-441）/evaluate_single_key 迁移臂（:241）/evaluate_multi_key_gate 迁移臂（:332）
wedb/wedb/src/server/slot_verify.rs:SlotVerifySessionState、SlotVerifyRequest（:52-62 起，无域字段）
wedb/wedb/src/server/cluster_session/slot_verify.rs:evaluate_multi_key_slot_gate/park_gate_wait（键裸字节入 req，域缺位）
wedb/wnode/src/resp/resp_server_session_slot_verify.rs:can_serve_slot 等 input 组装点（:100/:136/:176）

对应 c# 文件与函数：
garnet/libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:MultiKeySlotVerify 内 CanOperateOnKey（:116-128，Exists 经会话当前库 basicGarnetApi，C# 每库独立实例天然同域）
garnet/libs/server/Resp/RespServerSessionSlotVerify.cs:CanServeSlot（定槽与存在性判定同会话域）

精炼执行方案：
1 门评链补显式逻辑域：ClusterSlotVerificationInput 与 SlotVerifyRequest/GateCtx 各携 (ns, active_db) 两标量（组装臂自会话现取，与 active_db_slot 同一时刻同源；wait_key_gate 挂起体内标量随 req 装箱，无新分配），SlotVerifySessionState 不动
2 probe_key_alive/_async 两臂临时会话 enter_batch 前 set_context(ns, db)（同 worker_state.rs:73-75/slot_mgmt.rs:90-91 既有形态；请求会话过执行域门禁时该域路由必已装载，纯内存直设零挂起），三域+登记表折叠式与 TTL 裁决内核一字不动
3 锁测补同槽碰撞闭环（wedb/tests 层）：现算一枚 slot_of(ns', db') == slot_of(0, 0) 的碰撞库对，碰撞库存键 + 默认域槽置 MIGRATING（构造会话态，可无驱动在迁），断言碰撞库 GET/SET 本地 Serve 不回 ASK、根域异键在场不改判碰撞库缺席裁决；异步裁决臂（磁盘候选）同域断言；根域在迁键 ASK 现状不回退
4 同根残余登记不扩面：can_access_key sketch 裸键名折叠的域甄别（键入 sketch 时携域或按会话域反查）另留拍板席，本票只收探针落域主臂
