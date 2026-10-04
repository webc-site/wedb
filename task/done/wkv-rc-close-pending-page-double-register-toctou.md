甄别结论：通过（甄别席 J6，2026-09-27，定级 P3——close 挂起页双注册 TOCTOU，资源退化非丢数，RC 生产默认关；604eb5e 合入后复验成立）。亲验：drain 槽级 CAS 认领与 act.call 无全局锁形态未变（epoch.rs:610-617/:625-648）；close_armed 每次页满重武装（append.rs:178-180）、pump swap 单点消费（:251-260）、want 读取与幂等短路均在 turn_lock 之前、锁内无复核（:266-271），护栏 is_page_loaded 自陈「理论不可达」；safe_head 发布确在持锁内，一行锁内复核收口完备。派沙箱席 c01l。

审核结论：通过（P3）
审核裁定（独立复核确证，非背书票面）：
1 双注册可达性成立：append.rs:178 close_armed 为无条件 store(true)，冻结窗内每次 append 重试重武装同一边界；pump_close_barrier（append.rs:241）swap 单点消费，生产注册点两处（session/raw/read.rs:835 与 :1029，多会话线程各自泵入），武装到落定窗内两次泵入即注册 A、B 两个延迟动作。
2 TOCTOU 确证：want 读取（append.rs:266）与幂等短路判（:267-270）均在 turn_lock（:271）之前，锁内无复核。:284-289 防御护栏不拦截该形态——buffer.rs:217 is_page_loaded 按 page_ids[slot]==page_id 判定，B 重算的 next_page_id_B 未换装（槽内仍为存活旧代页号 next_page_id_B - num_pages），护栏不触发，B 走全量关闭：清洗存活在窗页（cleanse.rs evict_chain seal_atomic 加槽位恢复主日志地址）、清其物理槽、tail 发布至 B 的 next_page_start 跳过 A 新开页剩余容量、head/safe_head/closed_until 越界一页发布。
3 并发形态裁定：wepoch drain 为槽级 CAS 认领（epoch.rs:615-618 try_claim_ready_slot、:634-641 act.call 无全局收割锁），生产调用面遍布任意会话线程（participant.rs enter/refresh/resume 的 drain_if_pending、exit 的 after_release 接 suspend_drain、epoch.rs:493 help_drain、cpr_host.rs:500、reviv_host.rs:73、whlog shift.rs:250、wcpr create.rs:455）——双线程同入 drain 真并发成立，非恒单线程顺延。危害严格依赖真并发形态：单线程顺延下 B 的 :266-267 读取必后于 A 的 safe_head 发布，现行 :267 检查已短路无害。票面「罕见双线程交错」表述准确。
4 危害链复核：读结果无污染成立（close 序列 cleanse、closed_until、clear、tail 的发布顺序保证读者重探主日志）；safe_head 越界现无生产消费者确证（window.rs:45 访问器仅测试消费，生产仅 append.rs 自用作幂等判据）；存活页缓存整页丢弃回退冷读、环容量跳空一页成立。非丢数非挂死，read_cache 生产默认关闭进一步收窄暴露面，P3 恰当。
5 查重：deviations.md 与 task 五池无同题登记（deviations 唯一 TOCTOU 条目为 SO_REUSEPORT 多实例，无关；todo 池两条 read_cache 票为纪元守卫与扫描跳读主题，不重叠）。
6 六项判定：真实性成立；一行锁内复核不破坏架构纯洁与单向分层；幂等判据收进临界区是补全 mod.rs:96 已声明「safe_head 兼作幂等短路判据」的既有单一机制，非新增双机制；复核为控制面冷路径单次 Acquire load，数据面零渗透；方案与测试可落（page_inflight 门控可造确定性交错，read_cache/mod.rs tests 已有同款手法）；格式纯粹合规。

RC close_pending_page 幂等短路检查在 turn_lock 临界区外：纪元双注册延迟动作交错以陈旧 want 重跑整段换页关闭序列，存活在窗页被越界清洗、RC 环跳页容量、safe_head 越界发布（一行复核收口）

问题分析：
1 Garnet 契约对齐：C# 无此自研两阶段关闭形态——对标锚 AllocatorBase.cs:ShiftHeadAddress 的 epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress)) 每次换页动作单次注册、MonotonicUpdate 单调推进，天然无重复执行面；rust 的 close_armed 边沿加每次 append 页满臂重武装产生了 C# 不存在的双注册可能，幂等性由 close_pending_page 自查协议自担，故该自查必须闭环。
2 工程现状确证：双注册路径真实——wedb/wkv/src/read_cache/append.rs:178-179 页满武装臂在关闭未落定前被每次 append 重试反复置位 close_armed（fetch_max 同边界加 store true），pump_close_barrier（append.rs:236-241）以 swap 单点消费边沿——武装两次 pump 两次即注册 A、B 两个动作。并发执行真实——wepoch/src/epoch.rs:630-648 drain 以 try_claim_ready_slot 逐槽 CAS 认领（槽级独占非全局串行），act.call() 在无全局锁下执行，A、B 可两线程同时运行。TOCTOU 窗——append.rs:267-271 want = pending_close_until.load() 与 safe_head >= want 幂等短路判均在 let _lock = self.turn_lock.lock() 之前，取锁后无二次复核。时序：B 先取 want 见 safe_head 未发布通过检查阻塞于 turn_lock；A 完成全套关闭（safe_head/closed 发布至 end、tail 发布至 A 新页起点）放锁；B 持锁后以陈旧 want、已推进的 tail 重算几何（next_page_id_B 为 N+1、end_B 多一页）重跑：head 越界一页、safe_head 越界发布、对 next_page_id_B - num_pages 这一从未武装的存活在窗页就地 cleanse（槽位恢复主日志地址加 seal_atomic，cleanse.rs:110-131）、清该页物理槽、tail 跳过 A 新页剩余容量。
3 逻辑危害确证：读结果无污染——被越界清洗页的槽位已恢复主日志地址，读者按 Retry 重探走主日志链，closed_until 于清洗后方发布等待协议不变量成立。危害收敛为：存活 RC 页缓存被提前整页丢弃（该页全部键回退冷读）、RC 环跳空一页容量、safe_head 越界发布（现无外部消费者暂为惰性越界）。罕见双线程交错下的资源与效能退化，非丢数非挂死。

涉及代码：
rust 文件与函数：
wedb/wkv/src/read_cache/append.rs:close_pending_page（:267-271 检查锁序、:272-281 陈旧几何重算）、append 页满武装臂（:178-179）、pump_close_barrier 边沿消费（:236-241）
wedb/wepoch/src/epoch.rs:drain 槽级并发执行（:630-648）
wedb/wkv/src/read_cache/cleanse.rs:越界清洗面（:110-131）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftHeadAddress / OnPagesClosedWorkerCore（单次注册加 MonotonicUpdate 无重复执行面对照）

精炼执行方案：
1 close_pending_page 取得 turn_lock 后补同判据复核（if self.safe_head_address.load(Acquire) >= want { return; }），幂等判定收进临界区，一行收口
2 锁测：并发双注册加双线程收割交错下，A 完成后 B 短路直返、tail 不跳页、存活页不被越界清洗（现 RC 撕裂窗测试族扩一用例）

审核裁定执行方案（审核优化定稿，供 task/fix.md 直接消费）：
1 append.rs close_pending_page 于 :271 取得 turn_lock 之后、:272 重算几何之前，补一行锁内复核，沿用锁前 :266 已取的 want，无需锁内重取：
  if self.safe_head_address.load(Acquire) >= want { return; }
  完备性论证：safe_head 发布（append.rs:295）在持锁内完成，与复核经 turn_lock 互斥——同边界双注册两种交错均收口：B 先于 A 运行则 B 以冻结 tail 重算出与 A 恒等的正确几何代行关闭（tail 仅由 close 序列发布，冻结窗内几何确定），A 后跑经 :267 现有检查短路，无害；B 后于 A 运行（陈旧通过锁外检查后阻塞于锁）则锁内复核短路。合法后继边界 close 不受影响：后边界武装必以前一边界 close 落定发布 tail 为前提，不存在 safe_head 已越本边界而本边界页未清洗的误短路形态。
2 锁测（现 RC 撕裂窗测试族扩一用例，建议 read_cache/mod.rs tests）：按既有 turn_waits_for_inflight_registration 手法造确定性交错——武装一次、泵入两次得双注册动作；以 page_inflight 目标槽门控钉住首个动作持锁自旋，第二个动作阻塞于 turn_lock；放行门控后断言：closed_until_address 精确停在首个被驱逐页页末（不越一页）、tail 不跳页、存活在窗页哈希索引不被恢复（该页缓存仍可命中）、safe_head_address 等于同一边界值。
3 回归项：rc_epoch_drain.rs 与 read_cache_eviction_barrier.rs 既有断言全数保持（closed <= head 单调不变量、closed 精确页界）。

收口记录（收票席 R5 批次，2026-09-28）：合入 40f2cc40（首次 ort 被他席未提交注记暂挡、重试成——验货 1e1f39d6/38d2ba32+dev 前进复查零警）。收口形态=close_pending_page 取 turn_lock 后补锁内复核 safe_head>=want 即返（判据同源单机制，无新锁无字段），双注册两交错闭合；锁测 double_registered_close_actions_recheck_under_turn_lock（turn_lock 停车窗+page_inflight 门控确定性交错，摘复核实测 closed 越界一页 1024≠512 即红，8 连绿零抖）。deviations 无需。
