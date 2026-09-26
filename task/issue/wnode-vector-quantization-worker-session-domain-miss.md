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
