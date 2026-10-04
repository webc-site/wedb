归档注记：合入 c027dbee，悬垂邻接 vector_iid_exists 前置过滤+残余失败 warn 留痕跳过，WITHSCORES 成对透传，null 单源不动

甄别结论：通过（甄别席 J2，2026-09-27，定级 P2——悬垂邻接毒化 VLINKS 应答，删元素图边回收面）。cache.rs neighbors :186-212 仅跳 id 0、:205-206 ? 整链上抛；links_of service.rs:1433-1446 .ok()? 折 None；network_vlinks resp_server_session_vectors.rs:1477-1486 None 与缺席同形 null；delete_element data_provider.rs:456-492 先摘数据后 mark_free :485、图边回收在 service.rs:486 inplace_delete 后；remove :1196 is_ok 吞错；共享锁族 vector_manager.rs:825-833 注释亲读 VREM/VLINKS 同族可交错；WITHSCORES 接受即丢 :1466-1468 亲核；C# VectorSetLinks「TODO: Implement!」VectorStoreOps.cs:506-520 恒 +OK 桩属实。勘误：webc-diskann graph/index.rs 锚系外部 crate 依赖非仓内文件不可现码复验，属外围佐证，修法单源 cache.rs 前置过滤（data_provider.rs:521-525 现码在案）不受影响。派沙箱席 c01c。

审核结论：通过（P2 真案。cache.rs:205/206 ? 整链上抛、悬空 iid 回 Err（callbacks.rs:99-105、data_provider.rs:618-625）经 links_of service.rs:1439 .ok()? 折 None、network_vlinks:1480 与缺席同形 null 毒化整包（resp_vector_set.rs:1012 锁存活必 Array 矛盾坐实）；delete_element 先删数据释放 id、库边回收在后（index.rs:1598/1608）、半途失败 service.rs:1196 is_ok 吞错成稳态；VREM/VLINKS 同共享锁读态可交错；fsm.rs:408 复用 id 错报新成员为邻居（与 refill 票正交）；C# VectorSetLinks 系 TODO 桩恒 +OK，WITHSCORES 属超集自开臂，透传已算距离或显式拒二择恰当）

整理执行方案（供 fix 消费）：
1 修法单源：cache.rs neighbors 臂前置 vector_iid_exists（data_provider.rs:523，与库遍历 status_by_internal_id 同 fsm 单源，index.rs:1086/1316 先例），残余失败 continue 留痕对齐 dynamic_quant.rs:60 先例；null 单源留缺席

VLINKS 回链对悬空邻接无墓碑跳过臂：单个已删邻居毒化整包，存活成员伪回 null

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 侧 NetworkVLINKS 与 VectorStoreOps.VectorSetLinks 回包均为 TODO（恒 +OK），回包面本仓自开超集臂并已锁形：键缺失/元素缺失回 null，存活元素回层 0 邻接数组（仓内测试 wnode/tests/resp_vector_set.rs:1012-1023 锁定「存活元素必 Array、null 仅缺席」；vector_cold_key_read_parity.rs:138 锁定缺席 $-1）。C# 读完成面一贯带「元素可能已被删，读完校内部 id」纪律（VectorManager.cs:TryGetEmbedding :1326 后置 CheckInternalIdValid），底层 diskann 遍历臂对墓碑邻居以 status_by_internal_id 就地跳过（webc-diskann graph/index.rs:1086/1316）——回显臂须与同承诺同源。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wvector/src/provider/cache.rs:WedbProvider::neighbors（:186-212）遍历邻接仅跳过起点 id 0，get_full_vector(:205)/to_external_id(:206) 失败以 ? 整链上抛；wvector/src/service.rs:links_of（:1433-1446）将任意 Err 经 .ok()? 折成 None；wnode/resp/vector/resp_server_session_vectors.rs:network_vlinks（:1473-1481）把 None 映射为 null——与「元素缺席」同形，无差别臂。而 VREM 与 VLINKS 同持共享锁可同索引并发（vector_manager.rs:830 注明示 VREM 在共享锁族内），删除体 delete_element（data_provider.rs:456-492）先摘数据并释放 id，边回收（webc-diskann inplace_delete 的 add_edge_and_prune/drop_adj_list）在其后 await 才完成——他成员邻接表在窗内必现悬空 id；VREM 半途失败（service.rs:remove :1196 is_ok 塌缩 false，边残留无复位路径）把窗永久化。全仓无图完整性巡检臂（无 repair/consistency 命令、无后台清扫，grep 零命中），悬空边不自愈。

3. 逻辑危害确证
一、并发 VREM 窗内，仍引用该 id 的存活成员 VLINKS 伪回 null：同刻 VISMEMBER 真、VLINKS null，命令面自相矛盾，客户端误判成员丢失。二、悬空 id 被 fsm 快速队列复用（fsm.rs:reuse_or_mint）后，新成员被当作无关元素的邻居回显并计距离——数据错报，非仅可用性降级。三、VREM 失败残留即成稳态毒化，该成员 VLINKS 永久 null 且无巡检臂可收。同回显链未接线面一并裁决：WITHSCORES 选项被接受但 links_of 就地算出的全精度距离在命令层整体丢弃（应答恒仅元素名数组），属「接受+静默丢」中位臂。

涉及代码：
rust 文件与函数：
wedb/wvector/src/provider/cache.rs:WedbProvider::neighbors
wedb/wvector/src/service.rs:links_of
wedb/wnode/src/resp/vector/resp_server_session_vectors.rs:network_vlinks

对应 c# 文件与函数：
garnet/libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVLINKS
garnet/libs/server/Storage/Session/MainStore/VectorStoreOps.cs:VectorSetLinks
garnet/libs/server/Resp/Vector/VectorManager.cs:TryGetEmbedding（读后 CheckInternalIdValid 纪律原型）

精炼执行方案：
1. cache.rs:neighbors 迭代臂改跳过式：每个邻居先 vector_iid_exists（fsm 占用位，data_provider.rs:523 现成单点）前置过滤，get_full_vector/to_external_id 失败 continue 不再 ? 上抛（与遍历谓词及 dynamic_quant.rs:65「长度异常跳过留痕」同臂先例对齐）；null 单源保留给键/元素真缺席，links_of/network_vlinks 错误映射面不动。
2. WITHSCORES 单臂裁决：透传 links_of 已算距离（元素/分数对，形同 VSIM WITHSCORES 布局，零新机制），或命令层显式拒绝；删「接受+丢弃」现状。
3. 测试验证点：并发用例（近邻成员 VREM 在飞时存活成员 VLINKS 恒 Array 非 null，接 vector_vadd_guard_blocks_delete.rs 同族形态）；悬空边注入用例断言跳项回显（接 wvector/tests/insert_graph_stage_failure.rs 族）；WITHSCORES 布局断言入 resp_vector_set.rs。
