归档注记：合入 64a87c56，from_parts+reattach_checkpoint_dir 两诞生点补挂向量管理器，OnceLock 首挂幂等，同 Arc 冗余清理

甄别结论：通过（甄别席 J2，2026-09-27，定级 P2——默认装配臂漏挂向量管理器，回收注册表静默旁路）。装配面穷举亲核：attach_vector_manager 全仓恰三处 service.rs:1430/:1482/:1558 分属 (false,true)/(true,false)/(true,true) 臂，默认 (false,false) 臂经 open_with_config → from_parts（:1212，仅 :1292 attach_primary_tasks）无补挂；with_vector_set_preview :1786-1791 仅 is_enabled.store 亲读成立；reclaim_registry 未注入静默跳过（single_database_manager.rs:118-122）；C# 单装配路径恒配对亲核 GarnetServer.cs:420/:426。SWAPDB 肢作废订正方向保守，不动摇主体。派沙箱席 c01c。

审核结论：通过（P2）

理由：默认臂 (false,false)（open_with_config → from_parts）的 database_manager 确无 attach_vector_manager，全仓生产注入点仅 :1430/:1482/:1558 三臂（service.rs），node_components/open_node_with_config（:789/:881）与 boot.rs（:138/:142 仅 set_database_manager/attach_flush_gate）均无补挂路径，反证排查不成立；回收侧四口经 reclaim_registry（single_database_manager.rs:118-122）未注入即静默跳过，默认臂 FLUSH 族登记域回收确实旁路；C# 侧 VectorManager 单装配路径恒配对（命令面与管理面无漏挂形态），属装配接线缺陷非既定改良。会话面反证：aof_processor.rs:576-655 副本重放面对 Flush 条目主动调 reclaim_registry_domain，反证该回收机制为主库必需，默认臂缺席即不对称旁路。

票面两处失真随本审核订正，不动摇主体：
1 SWAPDB 肢作废：slow.rs:1305 首分支 if let Some(ref vs) = self.vector_session 对全部生产客户端会话恒命中（get_session :2126-2127 无条件 with_vector_manager，四臂同形；生产唯一 StoreGarnetApi::new 即该点），swap_database_slots（vector_manager_context_metadata.rs:495，无 is_enabled 门）在默认臂经会话面照常可达；else if try_vector_manager() 回落臂为生产死支。换库登记错位危害不成立，执行方案 SWAPDB 锁测项随删。
2 C# 路径勘误：实际为 garnet/libs/host/GarnetServer.cs（票面原写 libs/server/Servers/），:420 无条件构造与 :468 CreateStore 形参两行号属实。

定级 P2：管理面状态闭环缺陷（板块 4.2），真实生产形态（冷启动无 AOF + --enable-vector-set-preview）可触发，有登记幽灵驻留、清理/量化任务无效巡游、--recover 回建复活面；无数据丢失、无 panic、命令面不受影响，未达 P1。

冷启动默认臂 database_manager 缺 attach_vector_manager：FLUSH 族登记域回收静默旁路（其它三臂齐备唯默认臂漏），向量登记幽灵驻留死域

问题分析：
1 Garnet 契约对齐：C# VectorManager 无条件构造并直入 StoreWrapper（garnet/libs/server/Servers/GarnetServer.cs:420 new VectorManager → :468 构造形参），单装配路径下命令面与管理面恒配对，无「漏挂」形态；rust 向量登记域回收（FLUSH 族）与槽换面依赖 database_manager 持 vector_manager 引用，注入缺失即静默旁路。
2 工程现状确证：wedb/wnode/src/service.rs:1786-1790 with_vector_set_preview 仅 is_enabled.store(true)，不补挂 attach；from_parts（:1288-1292）只 attach_primary_tasks。三臂对位齐全：(false,true) :1430、(true,false) :1482、(true,true) :1558 均有 attach_vector_manager，唯默认 (false,false) 臂无。回收侧 single_database_manager.rs:118-122「未注入即静默跳过」→ flush_database/flush_namespace/flush_all_databases/reset 四口（:543-556/:562-570/:594-601/:608-612）回收臂全空转。危害链：默认臂 + --enable-vector-set-preview → VADD/VREM 正常可用（aof_sink None 早退仅跳过镜像，数据与登记旁路记录照常落存储）→ FLUSHDB 换号只回收 (ns,db) 域数据，根前缀登记元数据与内存镜像 key_index_registry 不回收 → 运行态向量登记幽灵驻留死域（清理/量化任务持续巡游）；重启 --recover 时 rebuild_registry_from_store 扫旁路记录复活已清库集合的登记（幽灵登记）。（审核订正：原票 SWAPDB 臂同缺一段作废——slow.rs:1305 首分支 vector_session 对生产客户端会话恒命中，SWAPDB 向量槽换面四臂恒通，见顶部结论。）
3 逻辑危害确证：删空自愈与生命周期闭环破缺（板块 4.2 严禁悬挂空对象与孤儿存储）——死域登记驻留加量化/清理任务无效巡游、重启幽灵复活、SWAPDB 登记映射错位；三臂齐全唯默认臂漏系装配接线缺陷非既定改良。

涉及代码：
rust 文件与函数：
wedb/wnode/src/service.rs:with_vector_set_preview（:1786-1790）、from_parts（:1288-1292）、三臂对照（:1430/:1482/:1558）
wedb/wnode/src/database/single_database_manager.rs:未注入静默跳过（:118-122）、四回收口（:543-612）
wedb/wnode/src/resp/garnet_api/slow.rs:SWAPDB 臂静默跳过（:1305-1315）

对应 c# 文件与函数：
garnet/libs/host/GarnetServer.cs:VectorManager 无条件构造（:420、:468；审核勘误：实际路径在 libs/host，原票写 libs/server/Servers/）

精炼执行方案：
1 默认臂补 attach_vector_manager（对齐三臂形态，收口点与 (false,true) 臂同位）；复核 with_vector_set_preview 调用序与 attach 的先后依赖
2 锁测：默认臂 + vector preview 开 → VADD 建集 → FLUSHDB → 断言登记域回收（in_use 归零、rebuild 不复活）；SWAPDB 换库断言登记域映射随迁

审核裁定执行方案：
1 落点改单点收口：from_parts（service.rs:1292 attach_primary_tasks 同点）追加 database_manager.attach_vector_manager(Arc::clone(&vector_manager))——from_parts 形参已持 vector_manager，database_manager 诞生点即配对点，一针覆盖默认臂与嵌入式裸装配，杜绝第五装配形态再漏；三臂既有显式挂载经 OnceLock 保首幂等共存：(true,false) 臂 :1480-1482 同 Arc 变冗余可顺手清，(false,true)/(true,true) 臂换装新 manager 的挂载（:1430/:1558）保留
2 先后依赖复核结论：attach 与 with_vector_set_preview 无时序约束——attach 只设 OnceLock Arc，is_enabled 运行期现读（vector_manager.rs:354-355），装配尾段 ：1682 顺序即安全
3 锁测（wedb/wnode/tests 沿 vector_key_domain_ops.rs 夹具形）：
a 默认臂 + 预览开 → VADD 建集 → FLUSHDB → 断言 database_manager.try_vector_manager() 在场、登记域 in_use 归零、VCARD 不可达
b 同形数据落盘后 --recover 重启 → 断言 rebuild 不复活已清域登记
c 原方案 SWAPDB 断言项删除（首分支四臂恒通，与本票无关，留之即假测）
