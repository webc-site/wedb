归档注记：合入 2486cc0a，ReuseGuard 延后附着形：登记面=守卫存活面，release/Drop 单出口，取消窗泄漏闭环

甄别结论：通过（甄别席 J2，2026-09-27，定级 P1——取消窗泄漏飞行计数致量化排空环自旋挂死）。现码复跑成立：登记臂 fetch_add fsm.rs:378 后横跨 reuse_or_mint 全 await 方达守卫构造 :392-396，future 丢弃即泄漏；全仓写点穷举 :191/:378/:382/:398-401/Drop :105-112 无归零路径；排空环 :612/:617 只 load 自旋，max_id_for_backfill :624 恒 u32::MAX、enable_reuse :628 不可达；dynamic_quant.rs:205/:265 调用点与 data_provider.rs 构造臂亲核成立。§127（deviations:1749-1753）只裁收敛序与 SeqCst 握手，不覆盖取消安全洞，查重清白。勘误：Err 臂注销实际 :398-401。派沙箱席 c01c。

wvector FSM next_id 在途计数跨 await 取消泄漏：pre_switch_inflight 永久 +1，enable_quantization 排空环永久自旋，量化管线整体挂死、已删 id 永不复用 fsm 块无界膨胀

审核结论：通过（P1）

定级理由：训练前窗口一次 VADD 取消即埋雷，训练期必爆——量化 worker 排空环自旋占核永挂、回填上界永不发布、enable_reuse 不可达致 fsm 块无界膨胀，重启前不可自愈；与甲轮1 量化句柄票（worker 即生即灭）同档 P1（量化通道整体失能级），且互为前提须并修。

亲验与反证排查记录（审核席复核成立）：
1 计数兜底复位反证结论：无。pre_switch_inflight 全仓写点穷举仅五处——初始化 0（fsm.rs:191）、登记臂 fetch_add（:378）、启用臂抵消 fetch_sub（:382）、Err 臂 fetch_sub（实际 :398-401，票面 :392-395 实为 Ok 臂守卫构造，行号微漂机制无误）、ReuseGuard::drop fetch_sub（:105-112）。enable_quantization 双排空环（:612/:617）只 load 不 store，定点后仅写 max_id_for_backfill（:624，该字段初值 u32::MAX 外唯一写点），计数永不清零——泄漏 +1 无任何路径归零，无自愈，反证不成立，票面成立。
2 泄漏窗亲读成立：fetch_add（:378）与 ReuseGuard 构造（:392）之间横跨 reuse_or_mint 全 await（mark_used :413/:421、扫块重填 :419、mark_id_unchecked :451，复用/铸造两臂均至少一处必经），future 被丢弃时注销臂与守卫均不存在。
3 启用臂抵消面核实：quant_enabled 置位后新 VADD 走 :374-375 break true、counted=false 不登记——泄漏只累积于置位前（受训量化族 quant_enabled=false 起，data_provider.rs:398-401），票面口径准确；无训面（Q8）计数无排空方、泄漏无观察者，危害精确落于受训量化族。
4 危害链亲读成立：train_quantizer 两臂调 enable_quantization（dynamic_quant.rs:205 重启窗臂/:265 锁段收口臂）→ 排空环永挂 → max_id_for_backfill 恒 u32::MAX、enable_reuse（:347）不可达、reuse 恒禁 → 已删 id 永不复用 fsm 无界膨胀；排空环 yield_now().await 自旋占核属实。
5 查重非重复：§127（deviations:1753）登记的是置位→排空→快照→再排空收敛序与 SeqCst 握手面，未覆盖计数取消安全洞，本票修法归并 ReuseGuard 单机制族、不触握手序不回改 §127 裁决；甲轮1 量化句柄票（todo wnode-quant-worker-handle-discard-instant-cancel）系 worker 生命周期面（拉起即灭），与本票正交——该票修复后方现形、本票修复后方安全，须并修非双立；与 todo 在案 wvector 三票（refill 丢弃/哨兵溢出/VLINKS 毒化）、§140、五池族正交。
6 六项判定：真实性✓；架构纯洁✓；单机制✓（登记点即守卫构造点，正常路径 ReuseGuard 接管、丢弃路径守卫兜底，同一计数同一注销机制，无第二机制）；数据面零开销✓（复用既有守卫仅前移构造点，零新增分配面）；可落度✓（同域挂起注入先例 wvector/tests/quant_enable_barrier_race.rs 在案）；格式纯粹度✓。

问题分析：
1 Garnet 契约对齐：C# 无对位取消窗（同步执行）；本面与 wvector-ensure-index-ready-startpoint-cancel-stuck 同根——async 取消安全缺失，正常路径注销完备唯丢弃路径漏守卫；§127（deviations:1753）登记的是量化「序」（置位→排空→快照→再排空与 SeqCst 握手），亲读复核握手本体无新洞，本条是同一计数器的取消安全洞，§127 未覆盖。
2 工程现状确证：wedb/wvector/src/fsm.rs:377-390 next_id——counted 登记臂 self.pre_switch_inflight.fetch_add(1, SeqCst) 之后横跨整个 self.reuse_or_mint(ctx).await（至少含一次 mark_used/mark_id_unchecked 的 rmw await，重填路径含扫块 await）才到 ReuseGuard 构造；错误臂注销（:392-395 fetch_sub）只覆盖正常 Err 返回，future 被丢弃时注销不执行且守卫尚不存在——计数永久 +1。每次被取消的 VADD 都泄一个计数（该窗口每插入必经）。危害链：量化 worker 建表走 train_quantizer → fsm.enable_quantization()（provider/dynamic_quant.rs:205/:265）→ fsm.rs:612 while self.pre_switch_inflight.load(SeqCst) > 0 { yield_now().await } 永不退出（:617 第二道排空同理）→ try_process_quantization_request 永挂 worker CPU 自旋 → 建表/回填永不完成、enable_reuse 不可达 → 已删 id 永不复用、fsm 块无界膨胀。任何一次训练前被 KILL 的 VADD 即埋雷，训练期必爆。
3 逻辑危害确证：量化管线整体挂死（worker 自旋占核）、fsm 无界膨胀，重启前不可自愈；纯内存态。

涉及代码：
rust 文件与函数：
wedb/wvector/src/fsm.rs:next_id 计数登记与注销窗（:369-405）、enable_quantization 排空环（:610-625）、ReuseGuard（Ok 臂唯一注销点）
wedb/wvector/src/provider/dynamic_quant.rs:enable_quantization 调用点（:205/:265）、enable_reuse（:347）

对应 c# 文件与函数：
N.A.（C# 同步执行无取消丢弃窗；§127 登记序面不覆盖本取消安全洞）

精炼执行方案：
1 fetch_add 后即挂计数守卫（Drop 注销，对齐 ReuseGuard 单机制族：守卫构造点即登记点，正常路径 ReuseGuard 接管、丢弃路径守卫兜底），消除裸计数窗
2 锁测：reuse_or_mint 桩在 await 点注入取消丢弃，断言 pre_switch_inflight 归零、enable_quantization 可完成；正常路径排空回归不回退

审核裁定执行方案（审核席整理，供 task/fix.md 直接消费）：
1 守卫构造点前移定形：next_id 登记臂 fetch_add（fsm.rs:378）后立即构造计数守卫，注销唯一出口收敛为 Drop——启用臂复读命中（:381-386）改为置守卫 counted=false（不再裸 fetch_sub）、Err 臂（:398-401）与 Ok 臂统一交守卫接管（ReuseGuard 增 id 延后附着的形态或拆「计数守卫＋id」两段同族结构，取其一，禁两套并存）；严禁在 enable_quantization 排空环加超时/清零兜底（治标洗账，掩盖泄漏本体，违单机制红线）
2 握手序零改动：quant_enabled 置位点、登记复读臂、两侧 SeqCst 内存序保持 §127 在案形态，本票只收注销窗
3 锁测落点：wvector 测试域新增，存储回调桩在 mark_used/mark_id_unchecked await 点注入取消（drop future），断言 pre_switch_inflight 归零、enable_quantization 定点完成、enable_reuse 可达；回归 quant_enable_barrier_race 零回退
