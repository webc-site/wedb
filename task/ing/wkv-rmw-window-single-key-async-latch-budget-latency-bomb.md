甄别结论：通过（fix 席 r27，2026-09-27，定级 P1 维持审核席自定级）。逐锚现码复跑双侧属实：wedb/wkv/src/session/rmw_window.rs 异步单键臂 :543-559 预算环每轮经 try_rmw_window(:300-312) → try_lock_key_bucket(:528-535) → try_lock_bucket_exclusive(:511-522，内嵌 for _ in 0..RMW_LATCH_SPIN_ATTEMPTS 的 spin_loop+try_lock_exclusive)，常量 RMW_LATCH_SPIN_ATTEMPTS=1024 在 :137、RMW_LATCH_YIELD_BUDGET=1024 在 :141 全实；已修先例 try_acquire_rmw_plan(:366-385) 轮内单次 try_lock_exclusive(:374) 与其注释 :354-365 的 1024×10 让渡 ≈3.5s/轮炸弹数学在位；windex/src/bucket.rs try_lock_exclusive(:109-124) 内嵌 MAX_LOCK_SPINS=10 让渡承接瞬态争用属实。唯一漏网臂经全仓 grep 复核成立：RMW_LATCH_SPIN_ATTEMPTS / try_lock_bucket_exclusive 消费链唯一 :305→:531→:511，其余 spin_loop 位点（aof/whlog/wvector/backoff）均非桶闩预算环，ttl.rs:232/:515 与 wtxn acquire_plan:192-205 皆单次尝试；同步单键臂同根由票面 :7 自觉披露并纳入方案 2，非漏计。C# 对照亲验：InternalRMW.cs:67-71/:82 单次取闩 + try/finally 放闩、ISessionLocker.cs:38-45 BasicSessionLocker 单次 TryLockExclusive、TransientLocking.cs:19-24 失败置 RETRY_LATER，票面对「rust 自加层而非忠实转写」的定性成立。危害可达：持闩三形俱在（wnode/tests/pair_bucket_order_latch.rs:78-83 钉闩夹具、事务持闩至 EXEC 已在 deviations.md:1901 登记、TTL 窗持闩跨 await），算式 1024×10 让渡 ≈3.5s/轮 × 1024 轮 ≈3584s 与同族定案在库口径自洽。查重：deviations.md 全册与 task 四池无同案在册；task/issue/w2-gate-red-batch-pairlatch-hang-and-inflight-domain-reds.md 系多键 pair 臂挂死红，其定因修复即本票所引先例，同族新增漏网而非重复立案。架构合规：方案 1-2 复用 locate_bucket_by_hash/try_lock_exclusive/yield 预算环，零新机制零新锁表零全局态，收口后 try_lock_bucket_exclusive 与 RMW_LATCH_SPIN_ATTEMPTS 全链退役不留死代码；格式纯粹、双侧路径齐全。随票移交 fix 席三处勘误：其一，票面 :8「task/done 波次2 pairlatch 案」实位 task/issue/（本票亲验记录 :30 自书 issue/，前后不一致，案属实非灭失）；其二，删 :514 自旋核后 :62 的 spin_loop import 须同删，否则留未用导入；其三，doc/zh/deviations.md:1901 关于「同步臂 1024 自旋」的表述在本票收口后失准，须同步改写为单次尝试契约。派沙箱席 b02b。

审核结论：通过（本轮审核席，定级 P1，裁定理由：单键异步臂嵌套 1024 自旋核逐锚亲验属实、为多键臂已修先例之外唯一漏网臂，与 pairlatch 定案同族同形，修法复用既有单机制零新增面）

单键异步读改写窗口 rmw_window 让核预算环每轮内嵌 1024 次自旋取闩，永久持闩下退化为小时级时延炸弹（多键臂已修为轮内单次尝试，单键臂漏网）

问题分析：
1 Garnet 契约对齐：C# 非事务会话 RMW 经 BasicSessionLocker.TryLockEphemeralExclusive 在读—算—写全程持本键桶排他闩（garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs 的 FindOrCreateTagAndTryEphemeralXLock + try/finally UnlockEphemeralExclusive），取闩失败回 RETRY_LATER 转会话级 pending 重试（锁冲突让核等持闩者推进），取闩本体是单次尝试、重试由 epoch 保护下的调度让渡承接，不存在嵌套忙自旋。rust 已修先例（多键臂）与该契约同构：轮内单次尝试 + 轮间 yield_now。
2 工程现状确证：多键异步臂 rmw_window_sorted（wedb/wkv/src/session/rmw_window.rs:443-498）已按 pair_bucket_order_latch 挂死案定因修复——轮内执行体 try_acquire_rmw_plan（:366-385）对每槽单次 try_lock_exclusive，瞬态争用由 HashBucket 内嵌 kMaxLockSpins 让渡承接（windex/src/bucket.rs），跨轮持闩由轮间 yield_now 承接，预算耗尽回 LockTimeout，其注释（:354-365）明载炸弹数学：轮内若嵌 1024 自旋 × 每次 10 次线程让渡，单轮实测 ~3.5s，1024 轮预算环退化为小时级。单键异步臂 rmw_window（:543-559）漏网：预算环每轮调 try_rmw_window → try_lock_key_bucket（:528-535）→ try_lock_bucket_exclusive（:511-522），后者内嵌 for _ in 0..RMW_LATCH_SPIN_ATTEMPTS(1024) { spin_loop(); try_lock_exclusive() } ——每轮即 1024×10 次线程让渡 ≈3.5s，外层 RMW_LATCH_YIELD_BUDGET(1024) 轮全耗 ≈1 小时才回 LockTimeout。同根辐射面：同步快路径 try_rmw_window（:300-312，wnode 写族约 40 处调用点）失闩时也先白付同款 1024 自旋（同核 reactor 线程忙等 ~3.5s，thread-per-core 下阻塞本核全部任务 poll）才降级异步臂；多键同步臂 try_rmw_window_sorted 已走 try_acquire_rmw_plan 单次尝试不沾。
3 逻辑危害确证：外部件长期持闩（EXPIRE 键闩、事务持锁、迁移协同窗、测试钉闩）或持闩者卡死时，单键写族命令（SET/GETEX/SETRANGE/INCR 等 ~40 处）先在同步段忙等 3.5s 阻塞本核 reactor，再入异步臂以 1024 轮 × 3.5s ≈ 小时级才得 fail-closed 忙应答，客户端连接假死、门禁压测出现小时级挂死（pairlatch 案同形）；compio thread-per-core 下同步忙等段连本核其它连接一并卡顿。该危害已有同族定案（task/done 波次2 pairlatch 案：非死锁、时延炸弹），修法先例在库。

涉及代码：
rust 文件与函数：
wedb/wkv/src/session/rmw_window.rs:rmw_window（:543-559 让核预算环，轮内经 try_rmw_window 嵌套自旋）、try_lock_bucket_exclusive（:511-522 内嵌 1024 自旋核）、try_lock_key_bucket（:528-535）、try_rmw_window（:300-312 同步臂同根）、try_acquire_rmw_plan（:366-385 已修先例）、RMW_LATCH_SPIN_ATTEMPTS（:137）/RMW_LATCH_YIELD_BUDGET（:141）
wedb/windex/src/bucket.rs:try_lock_exclusive（kMaxLockSpins=10 内嵌让渡，瞬态吸收层）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:FindOrCreateTagAndTryEphemeralXLock（单次尝试取闩 + RETRY_LATER）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionLocker.cs:BasicSessionLocker.TryLockEphemeralExclusive（单次尝试语义）

精炼执行方案：
1 单键异步臂轮内改单次尝试：rmw_window 预算环内不再经 try_rmw_window（嵌 1024 自旋），改走「ensure_split_by_hash + bucket_index_for_hash + try_lock_exclusive 单次」直取（locate_bucket_by_hash 已有 :502-507 可复用），轮间 yield_now 重试机制不变，预算耗尽回 LockTimeout 契约不变——与 try_acquire_rmw_plan 先例同口径，瞬态争用仍由 HashBucket 内嵌让渡承接
2 同根收口评估：try_lock_key_bucket/try_lock_bucket_exclusive 的 1024 自旋预算系同步臂降级契约（:132-137 注释），但永久持闩下同步段 3.5s 忙等阻塞本核 reactor 危害更甚——评审裁夺是否同步臂一并改单次尝试（失闩即降级异步臂，让核等闩），或维持同步预算另案
3 测试验证点：仿 tests/pair_bucket_order_latch.rs 夹具（外部件钉闩 + scoped_hash 寻桶），新增单键案：钉闩下 rmw_window 须在秒级预算内回 LockTimeout（断言耗时上界而非仅终态）；同步 try_rmw_window 失闩降级路径断言无 3.5s 级忙等；无争用路径点查写回归全绿

审核裁定执行方案

审核席亲验记录（逐锚属实）：
1 rmw_window.rs :543-559 预算环每轮经 try_rmw_window(:300-312) → try_lock_key_bucket(:528-535) → try_lock_bucket_exclusive(:511-522，内嵌 0..RMW_LATCH_SPIN_ATTEMPTS 次 spin_loop+try_lock_exclusive，常量=1024 在 :137) 逐锚核验属实；RMW_LATCH_YIELD_BUDGET=1024 在 :141 属实；try_acquire_rmw_plan(:366-385) 轮内单次尝试及其注释 :354-365 的 1024×10 让渡 ≈3.5s/轮炸弹数学原文在位；windex/src/bucket.rs try_lock_exclusive(:109-124) 内嵌 MAX_LOCK_SPINS=10 次 yield_now 让渡属实。多键两臂（try_rmw_window_sorted / rmw_window_sorted）均走 build_rmw_lock_plan + try_acquire_rmw_plan 单次尝试，异步单键臂为唯一漏网臂确证；rmw_window_sorted 单键入参退化委托 rmw_window(:462)，本票修复同时覆盖该入口
2 同步臂调用面：wnode grep try_rmw_window 计 49 处（票称 ~40 同量级），抽查 set.rs(:256/:322/:414/:454) 与 incr.rs(:130/:179) 六处，失闩形态均为 let Some(window) = store.try_rmw_window(key) else { return Ok(false) } 降级异步慢路径，与票面一致
3 C# 对照：TransientLocking.cs TryEphemeralXLock 失败置 RETRY_LATER（单次尝试）；ISessionLocker.cs BasicSessionLocker.TryLockEphemeralExclusive 为单次 LockTable.TryLockExclusive，无嵌套忙自旋；InternalRMW.cs:70-71 失闩即 return status 转调度级 pending 重试，契约同构确证
4 查重：doc/zh/deviations.md 全册与 task 各目录无同案在册；task/issue/w2-gate-red-batch-pairlatch 票为多键臂挂死案（其定因修复即本票所引 try_acquire_rmw_plan 先例），与本票单键臂不重叠，无重复立案

六项判定：
1 真实性 成立（逐锚亲验，非幻觉非误读）
2 架构纯洁与单向分层 成立（复用 locate_bucket_by_hash / try_lock_exclusive / yield 预算环，零新机制零新锁表零全局态）
3 单机制与反过度设计 成立（与 try_acquire_rmw_plan 先例同口径；try_lock_bucket_exclusive 与 RMW_LATCH_SPIN_ATTEMPTS 的唯一消费链 :305→:531→:511 随收口全链退役，零死代码）
4 数据面零开销 成立（环内 1024×10 次让渡降为单次 CAS，零新增分配）
5 方案可落度 成立（两处既有语义须钉死：入口 ensure_split_by_hash 的 ? 显式上抛 :544-548 与事务模式 held=None 让闩分支，票面已自觉标注）
6 格式纯粹度 成立（纯文本，路径双向齐全）

方案 2 裁定：同步臂一并收口（采纳）。
危害评估成立：:132-137 注释「微秒级即放闩」前提在永久持闩（事务持锁 / EXPIRE 协同窗 / 卡死持有者）下失准，1024×10 让渡即 ~3.5s 同核忙等，compio thread-per-core 下阻塞本核全部任务 poll，与本模块 INNER_LATCH_RETRY_BUDGET 注释(:152-153)「同核跨任务互阻一并收口」定案同构；C# 契约本为单次尝试 + RETRY_LATER，同步臂 1024 自旋系 rust 侧自加层而非忠实转写，删除即回契约；失闩降级通道（Ok(false) → 异步臂）全部调用点在位，收口零新增面，瞬态争用仍由 HashBucket kMaxLockSpins=10 让渡承接（亚微秒持有者单次即得），取舍与多键臂已修先例完全一致

定案执行方案（供 task/fix.md 直接消费）：
1 rmw_window(:543-559) 预算环内改单次尝试执行体：保留入口事务判位与 ensure_split_by_hash 的 ? 显式上抛语义（:544-548 不动）；环内不再调 try_rmw_window，改「scoped_hash → 钉定 index（load_full）+ bucket_index_for_hash → index.bucket(b).try_lock_exclusive() 单次」，失闩 yield_now 进下一轮；轮内比对 Arc::ptr_eq（仿 rmw_window_sorted :480-483），版本推进即重定位，环内扩容协同沿用 locate_bucket_by_hash 既有 .ok() 吞错为失闩的口径，勿开环内新错误通道；预算耗尽回 LockTimeout 契约不变
2 try_lock_key_bucket(:528-535) 同步臂改「locate_bucket_by_hash + 单次 try_lock_exclusive」，try_lock_bucket_exclusive(:511-522) 整函数与 RMW_LATCH_SPIN_ATTEMPTS(:137) 常量一并删除，:132-136 同步域注释同步改写为单次尝试契约（禁留复述已删实现的死注释）
3 测试验证点（仿 wnode/tests/pair_bucket_order_latch.rs 夹具）：钉闩下 rmw_window 须在秒级预算内回 LockTimeout（断言耗时上界，非仅终态）；钉闩下同步 try_rmw_window 须即刻回 None（断言无 3.5s 级忙等的耗时上界）；rmw_window_sorted 单键入参退化路径一并覆盖；无争用点查写回归全绿
