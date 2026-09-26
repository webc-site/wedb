审核结论：通过（P1 真案。六项判定全过：非根域量化链全域错位坐实——worker 自持会话恒根域（Node 形态覆写工厂 service.rs:1257-1263 同源），建表回填链未落域而回调面统一取 TLS 会话前缀，训练取样恒 miss 按 Failed 终态消费零报错，危害链闭环成立；落域复用既有 set_virtual_context 单点（mod.rs:607）+version_domain_of 换算，不引入新机制，冷路径零开销；deviations 查重无覆盖（§128 在册面为屏障序，与域问题正交）；现锁族全为根域用例漏检属实。方向裁决：单点落域收口）

复审席亲验补强（逐行复跑全链，结论维持 P1）：
1 翻案排查做尽不翻案：worker 绑会话→工厂（service.rs:837-845 与 Node 覆写点 :1257-1263 皆 store.new_session 裸根域）→read_vector_index_core（登记读 stored_index_of 纯内存复合键，域无关）→recreate_index_locked（:497-518）→create_index→train_quantizer/backfill（wvector/src/service.rs:1200-1220 直下 provider）全链 grep 无 set_virtual_context/set_active/bind 族落域点；记录面唯一域源即 TLS 会话前缀（callbacks :577/:595/:662/:681/:728）。
2 锁测缺口补充定性：quant_train_barrier/quant_enable_barrier_race 系 wvector 层测试，callbacks 为 FaultStore 无会话域面，该层补不了本缺口；非根域闭环锁测必须落在 wnode 层。
3 传导约束：version_domain_of 需 store 句柄而 VectorManager 不持 store，经既有工厂闭包捕获的 store 传导（守卫或工厂侧扩展均可），严禁为取换算新开全局可变洞。

整理执行方案（审核席订正版，复审席确认）：
1 try_process_quantization_request 绑定自持会话后、读索引前，按 split_registry_key 解出 (vns, vdb) 经既有落域单点落域（逻辑域经 vdb.version_domain_of 换算直设臂，形态对齐 tiered_demote.rs:151-156 单键落域窗）；该单点天然覆盖重建/建表/回填全链，recreate_index_locked（实在 :497-518，其调用点 :413 已在守卫内）勿在内部二次落域免双点
2 锁测补非根域闭环（wnode 层）：非根域 VADD（尾参定槽）→ 训练 → 回填 → VEMB RAW 命中断言，根域既有锁测全绿不回退；wvector 层现有锁族无会话域面，勿在该层加假域测
3 票面订正：C# ActiveThreadSession 实在 VectorManager.Callbacks.cs:254 非 VectorManager.cs；更强契约证据为 VectorManager.Quantization.cs:84-88 worker 显式 TrySwitchActiveDatabaseSession 切 self.dbId；建表锚订正 :168、回填锚订正 :188-193；票首 wvedb 路径系笔误

量化 worker 自持会话未落条目域：非根域向量集（SELECT 非 0 库/命名空间）量化链建表回填全域错位静默失能，训练取样恒 miss 按 Failed 终态消费

问题分析：
1 Garnet 契约对齐：C# 量化 worker 经 VectorManager.Quantization.cs:TryProcessQuantizationRequest 处理请求，依赖 VectorManager.cs:254 的 [ThreadStatic] ActiveThreadSession——C# 每库独立日志加每线程会话，worker 会话天然与条目同库，不存在域错位面。rust 单日志前缀域拓扑下，向量元素/量化/FSM 记录物理键携带会话前缀（wedb/wkv/src/session/keys.rs:44-47 vector_key 以 session_prefix 起头，模块头 wedb/wnode/src/resp/vector/vector_store_callbacks.rs:72-75 自陈元素记录键随当前连接 (namespace, active_db) 落位），后台 worker 以何域会话执行即读何域记录，落域为必需肢——仓内同型纪律先例：wedb/wnode/src/resp/objects/tiered_demote.rs:153-157「落条目物理域再评估与写回……storage.batch.set_virtual_context(vns, vdb, lns, ldb)」与 aof_processor.rs:1045-1094 KeyContextGuard 同型。
2 工程现状确证：量化 worker 每次尝试绑定自持专用会话（wedb/wnode/src/resp/vector/vector_manager_quantization.rs:138 let _domain = self.bind_dedicated_session()），而该会话恒根域——工厂 wedb/wnode/src/service.rs:837-845 bound_dedicated_session 即 store.new_session()，wkv 会话构造初值 active_vns/active_vdb 均 0（wedb/wkv/src/session/mod.rs:270-272）。:140-141 解出 (domain, user_key) 后，domain 前缀仅消费于 :151-153 read_vector_index_core 的登记表读；其后 :174 build_quantization_table(context) 与 :193-197 backfill_quantized_vectors(context, ...) 全程未将会话落域到 (domain.vns, domain.vdb)。下游链全走会话前缀：回调面统一经 TLS 会话取前缀（vector_store_callbacks.rs:577 let prefix = session.session_prefix()、:595 vector_key_with_prefix），训练取样 wedb/wvector/src/provider/dynamic_quant.rs:236-244 read_single_iid 逐 id 读向量行。次生窄窗：worker 侧非阻塞读命中 ptr=0 先于用户重建时，recreate_index_locked（vector_manager_locking.rs:360-421）以同一根域会话构造 provider，FSM 块/起点读根域全 miss 得空表，index_ptr=1 落桩，后续铸造复用已占用内部 id。
3 逻辑危害确证：非根域（租户命名空间或 SELECT 非 0 库，VADD 尾参定槽域即为此设计）向量集开启量化：训练取样 read_single_iid 全 miss 静默 return false，请求按 Failed 终态消费（ReadIndexOutcome::Failed 同口径终态），零派发零报错，该集量化永久停摆全精度；§128 早退臂（quantization :198-207 读 _qnt）亦走根域，重启收敛窗对非根域集恒判假；若建表侥幸达成，回填把 Quantized 记录写根域而 VEMB RAW 经连接域读（vector_manager.rs:1406）恒 miss 静默回退全精度。现锁族 quant_train_barrier/quant_enable_barrier_race 全为根域用例，故全绿漏检。根域默认部署不受影响，非根域量化面整体静默失效。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/vector/vector_manager_quantization.rs:try_process_quantization_request（:138 绑定自持会话、:140-141 解域、:151-153 仅登记读用前缀、:174/:193-197 建表回填未落域）
wedb/wnode/src/service.rs:bound_dedicated_session（:837-845 根域会话工厂）
wedb/wkv/src/session/mod.rs:会话构造初值（:270-272 vns/vdb 恒 0）
wedb/wnode/src/resp/vector/vector_store_callbacks.rs:回调前缀取会话域（:577/:595，另 :662/:681/:728 同型）
wedb/wvector/src/provider/dynamic_quant.rs:训练取样（:219-244）
wedb/wnode/src/resp/objects/tiered_demote.rs:落域纪律先例（:153-157 set_virtual_context）
wedb/wnode/src/resp/vector/vector_manager_locking.rs:recreate_index_locked（:360-421 次生窄窗）

对应 c# 文件与函数：
garnet/libs/server/Resp/Vector/VectorManager.Quantization.cs:TryProcessQuantizationRequest
garnet/libs/server/Resp/Vector/VectorManager.cs:ActiveThreadSession（:254 [ThreadStatic]，worker 会话天然同库无错位面）

精炼执行方案：
1 try_process_quantization_request 在 :138 绑定自持会话后、:151 读索引前，按 :140 解出的 (domain.vns, domain.vdb) 经既有落域单点落域（逻辑域经 vdb.version_domain_of 换算直设臂，形态对齐 tiered_demote.rs:153-157 单键落域窗）；recreate_index_locked 重建臂同判据一并落域
2 锁测补非根域闭环：非根域 VADD（尾参定槽）→ 训练 → 回填 → VEMB RAW 命中断言，根域既有锁测全绿不回退
