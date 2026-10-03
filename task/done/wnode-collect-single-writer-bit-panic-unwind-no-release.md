主控收票审计（2026-10-01，交付 commit cff1859，改动面 primary_tasks.rs 21 / garnet_api/{mod.rs 3,objects.rs 50,slow.rs 67} + 新册 tests/object_collect_panic_unwind.rs 282）：
- 守卫形制合规：`CollectLockGuard<'a>(&'a AtomicBool)` 一处定义（objects.rs:40，try_acquire 于 :42 起 CAS 抢占、
  Drop 臂 :63 store(false, Release)），周期侧 primary_tasks.rs:395 持 PrimaryTasks 本侧位、
  手动侧 slow.rs:756 持 StoreGarnetApi 每连接位——两站各持本侧位实例，未合并两位，票面红线守住。
- 依赖方向复核：primary_tasks.rs:54 本已 import 同模块的 object_collect_all，守卫随宿主就近取用，
  未新增「生命周期域→resp 会话面」的新的反向依赖（非分层劣化，不另开票）。
- 早释放臂：正常路径以 `drop(_guard)` 显式归还（objects.rs/slow.rs 各点），保留 C# finally 同效且不多持锁；
  panic 展开由 Drop 兜底——与 C# TryWriteLock + try/finally 语义 1:1。
- 待补核（归档席自证或终轮门禁覆盖）：wnode 定向 nextest 未由主控跑成（主树 clippy 长期占锁），
  由波次终轮 test.sh 承接；object_collect_panic_unwind.rs 在册须含 panic-unwind 断言例（:326-335 已见）。

锁定注记（2026-10-01 r8 波主控补锚，基线 f89369e；票面行号 +2 微漂，以本注记为准）：
- primary_tasks.rs 现位：位字段 :96/:99（原 :94/:97）、collect_family 函数体起 :377（原 :375）、
  CAS 抢占 compare_exchange :392-395、手工释放两臂 :410/:414（原 :408/:412）。
- 手动侧未漂：slow.rs:740 Hcollect|Zcollect 臂（体至 :789 释放臂同位）、garnet_api/mod.rs:439/:442 同位。
- 漂因：本波 e7efacb 仅改模块头注释 +2 行，零逻辑改动；票面守卫类型「一处定义两处各持本侧位实例严禁合并」口径照旧。
- 禁触域：wedb/wnode/src/resp/vector/vector_store_callbacks.rs、wedb/wedb/src/server/replication/**、
  wedb/wnode/src/storage/session/common/ttl_sync.rs（同侪在途）。

审核结论：通过（2026-09-30 甲轮48 审核席；P3。危害段已按审核席订正改写：周期位/手动位两位拓扑（PrimaryTasks 字段 vs StoreGarnetApi 每连接字段）、单族卡死、降阶轮不受收集位门、方案 1 钉守卫类型一处定义两处各持本侧位实例严禁合并）

HCOLLECT/ZCOLLECT 单写位无 unwind 释放臂，收集体 panic 后收集面静默永久封死

问题分析：
1. Garnet 契约对齐：ObjectCollect（libs/server/Storage/Session/ObjectStore/Common.cs:807-841）collectLock.TryWriteLock 成功后整个扫描体包 try/finally，WriteUnlock 在 finally——任务体任何异常锁自释，手动收集随即可再入。
2. 工程现状：周期侧 collect_family（primary_tasks.rs:375-412，位为 PrimaryTasks 字段 :94/:97）与手动侧 slow.rs Hcollect|Zcollect 臂（:740-772，位为 StoreGarnetApi 每连接字段 garnet_api/mod.rs:439/:442，跨位不互斥系已登记 C# 对齐粒度）均为 CAS 抢占单写位 + 正常臂/错误臂手工 store(false)，无 unwind 释放臂。object_collect_all 途中 panic 时 supervise_resumable（wbase/src/supervise.rs）只复位 object_collect_started，panic 族对应位恒留 true（两族先后串行，一次 panic 只卡一族），任务复活后 collect_family 该族每域 compare_exchange 必败静默 continue（零日志）；手动侧位仅本连接自身 panic 封死本连接，断连自愈。周期与手动两侧释放纪律同缺位，即 C# finally 语义的单一缺口；bg_task_health 如实报活但收集面失效，观测不可区分。
3. 逻辑危害确证：可达路径需收集/扫描路径内先存在一个 panic（以另一缺陷为先决），属防御纵深缺口而非独立可达缺陷；后果为周期侧单族收集面静默停摆（自愈触发点也仅重跑 collect 循环体、位仍卡死）与手动侧连接级封死，观测失真。定 P3。

涉及代码：
rust 文件与函数：
wedb/wnode/src/primary_tasks.rs:375 collect_family（CAS 抢占、:408/:412 手工释放两臂）
wedb/wnode/src/resp/garnet_api/slow.rs:740 Hcollect|Zcollect 臂（CAS 抢占、成功臂尾手工释放）

对应 c# 文件与函数：
libs/server/Storage/Session/ObjectStore/Common.cs:807 ObjectCollect（:810 TryWriteLock、:834 finally WriteUnlock）

精炼执行方案：
1. 单写位 RAII 守卫（Drop 臂 store(false, Release)）守卫类型一处定义、两处各持本侧位实例（§174 StartPointLoadGuard 同形态先例）；严禁周期位与手动位合并为同一位实例——那会把互斥粒度从 per-session（C# 契约）改为全局跨会话，违反 primary_tasks.rs:91-93 既有登记
2. 锁测：收集体注入 panic 形态，断言守卫 Drop 后位归零、下轮可再入
3. 复跑既有 HCOLLECT/ZCOLLECT 互斥锁测确认零行为漂移

终态注记：
- 合入收口形态：在 wedb/wnode/src/resp/garnet_api/objects.rs 定义单写位 RAII 守卫 CollectLockGuard，在 Drop 臂通过 store(false, Ordering::Release) 保证 unwind 时释放；primary_tasks::collect_family 与 resp::garnet_api::slow 的 Hcollect/Zcollect 均接入该守卫，周期位与手动位各持独立位实例保持既有 per-session 粒度；补齐 panic 展开与再入锁测。
- 合入哈希：cff1859
- 状态：已收口归档。

