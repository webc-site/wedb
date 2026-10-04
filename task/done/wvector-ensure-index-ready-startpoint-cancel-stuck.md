甄别结论：通过（甄别席 J2，2026-09-27，定级 P1——KILL/注销取消窗下索引状态机永卡非 Ready，等待臂无超时自旋）。service.rs:878-921 状态机全文亲读：CAS :897 后仅 Err 复位 :907-910 与 Ready :912-914 两落定臂，IndexState 全仓写点穷举无旁路复位；等待臂 :886-894 自旋让位无超时；init 体 cache.rs:79 maybe_set_start_point 多 await；取消面自证 slow_path.rs:7-8 与 resp_server_session_vectors.rs:1004-1008 共享锁跨 await 契约注释亲读成立。勘误：票体 :901-903/:905-910 为行漂，实际 :907-910/:912-914。派沙箱席 c01c。

审核结论：通过（P1）

定级理由：单连接 KILL/断连一次取消即永久冻结整个向量集写面（VADD 永挂＋DEL/VDROP 键级冻结），重启前不可自愈，每个新集合首插必经装载窗，触发面与危害相称。

亲验与反证排查记录（审核席复核成立）：
1 状态机全文亲读（service.rs:878-921）：CAS 成功后仅两落定臂——Err 复位臂实际 :907-910、Ready 臂实际 :912-914（票面行号微漂 :901-903/:905-910，机制描述无误）；init 体（cache.rs:79-178 maybe_set_start_point）含 read_single_iid、fsm.next_id、三次 write_iid 多个 await 点，任一点被丢弃即 state 永久滞 1；等待臂 :886-894 spin<32 自旋否则 yield_now().await continue，无超时无退出，亲读确认。
2 反证排查一（其它复位旁路）：IndexState 全仓写点穷举仅 service.rs 构造点 :946/:948 与状态机三臂，无 drop_index/recreate 臂重置 state；drop_index（vector_manager.rs:1119）只摘登记不触 state 字段；即便 VDROP 重建亦是销数据换新实例非自愈。冻结唯一出口＝VDROP 销集或重启（重启按盘面 start_points_exist 重导 state），票面「重启前不可自愈」成立。
3 反证排查二（取消面真实性）：生产 RESP 臂经 network_vector_write_slow 挂起驱动（resp_server_session_vectors.rs:855-870），slow_path.rs:7-8 头注自证 KILL/注销胜出即丢弃执行体；deviations §84（:1130-1145）关停排空 5 秒强收后残余任务交 Runtime 析构兜底取消。丢弃路径真实。
4 条带锁覆盖面亲读成立：network_vadd_slow 栈帧 _lock 自 read_or_create_vector_index 起跨 await 存活至函数返回（:1000-1008 头注自证「manager 契约假定索引已锁定」）；永挂 VADD 各持共享锁，并发 DEL/VDROP 独占锁永阻。
5 查重非重复：与同根 B 票（fsm 计数票）分属两台独立状态机（index.state vs pre_switch_inflight），须并修非互代；与 todo 在案 wvector 三票（refill 丢弃/哨兵溢出/VLINKS 毒化）、§127/§140、五池族均正交。
6 六项判定：真实性✓；架构纯洁✓（纯执行体内守卫，不动分层）；单机制✓（CAS 后守卫兜底复位单点，对齐 deviations:873 ObserverDropGuard 先例，禁第二机制）；数据面零开销✓（守卫仅在冷路径装载窗）；可落度✓（锁测注入点明确，同域挂起注入先例 wvector/tests/quant_enable_barrier_race.rs 在案）；格式纯粹度✓（纯文本、路径双向齐全、C# 侧 N.A. 附取消窗不存在对账理由）。

问题分析：
1 Garnet 契约对齐：C# 侧命令同步执行完毕无 mid-flight 丢弃语义，起点装载在原生操作栈内完成（diskann 原生 create），不存在取消丢弃窗；本面系 rust 异步化独有取消安全缺口，仓内取消卫生惯例先例在册（deviations:873 ObserverDropGuard、甲轮7 vector 席同族两票同根）。
2 工程现状确证：wedb/wvector/src/service.rs:878-921 ensure_index_ready_or_init——NoStartPoints 臂 CAS 置 SettingStartPoints(1) 后，仅 init() 正常返回两臂复位/推进状态（:901-903 Err 臂复位、:905-910 成功臂置 Ready）；init（provider/cache.rs:79-178 maybe_set_start_point）内部含多次 await（fsm.next_id 的 rmw、三次 write_iid），future 在任一 await 点被丢弃即 state 永久滞 1。等待臂（:886-894）spin_count<32 自旋否则 yield_now().await continue——无超时无退出。取消面真实：slow_path.rs:7-8 头注自证「KILL/注销胜出即丢弃执行体断连收口」、deviations:1138 关停排空 5 秒兜底取消；新集合首个 VADD 必走起点装载。危害链：首个 VADD 在 init 的存储 await 中被 KILL/断连丢弃 → state 恒 1 → 该 context 后续一切 VADD 无限等待永不返回；且 VADD 持共享条带锁覆盖 try_add 全程（resp_server_session_vectors.rs:1008「manager 契约假定索引已锁定」）→ 并发 DEL/VDROP 的独占锁排队永阻 → 键级冻结，量化/检索后续写链全断。
3 逻辑危害确证：单次连接取消即永久冻结整个向量集（重启前不可自愈），量化/清理后台链连带巡游卡死；纯内存状态无持久损伤。

涉及代码：
rust 文件与函数：
wedb/wvector/src/service.rs:ensure_index_ready_or_init（:878-921 状态机取消不安全）、insert 调用点（:1131-1138）
wedb/wvector/src/provider/cache.rs:maybe_set_start_point（:79-178 多 await init 体）
wedb/wnode/src/resp/slow_path.rs:取消丢弃自证（:7-8）

对应 c# 文件与函数：
N.A.（C# 同步执行无 mid-flight 丢弃；rust 异步化独有取消安全面）

精炼执行方案：
1 状态机补取消安全：init 体挂取消守卫（ObserverDropGuard 先例形态）或 init future 丢弃路径由 await 点 Drop 臂复位 NoStartPoints（对 CAS 成功后到状态落定窗兜底），等待臂保语义不变
2 锁测：init 桩在 await 点注入取消丢弃，断言后续 VADD 仍能完成起点装载、无永挂；并发 DEL/VDROP 在装载窗不永阻

审核裁定执行方案（审核席整理，供 task/fix.md 直接消费）：
1 单机制收口定形：守卫挂点收口在 ensure_index_ready_or_init CAS 成功后单点（service.rs:905 之前），Drop 臂复位 NoStartPoints（Release）、正常落定两臂解除守卫后照旧 store——覆盖 init 体全部 await 点，含未来新增；严禁 init 体（cache.rs）内另挂第二处守卫形成双保险双机制，严禁改动等待臂自旋语义与 CAS 内存序
2 守卫形态对齐 deviations:873 ObserverDropGuard 先例：栈上零分配守卫（存 &index.state 引用＋disarmed 标志），drop 时按 armed 复位；Err 臂/成功臂先 disarm 再 store，杜绝 drop 与 store 竞写
3 锁测落点：wvector 测试域新增，存储回调桩在 write_iid await 点注入取消（drop future），断言 state 回 0 且后续 VADD 完成装载；并发臂断言装载窗 DEL 拿到独占锁；回归 quant_enable_barrier_race 零回退

收口记录（收票席 R3 批次，2026-09-28）：合入 52ba9035（验货 bf8a7e31+d27d3ac0）。收口形态=CAS 成功点单挂 StartPointLoadGuard（对齐 wnode ObserverDropGuard 先例，Drop 兜底复位 NoStartPoints，两落定臂先解除再 store）+ fsm.claim_start_id 幂等认领位点（cache.rs 起点臂弃 next_id，杜绝取消残影重铸非零 id 死路），单机制零新锁。锁测 tests/startpoint_cancel_reset_reload.rs 双臂（丢弃后重试装载收敛/等待窗等待者随丢弃解冻），旧码必红实测。偏差登 §174。
