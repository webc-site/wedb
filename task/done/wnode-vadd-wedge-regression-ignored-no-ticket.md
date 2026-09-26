归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 5db78d8＋fmt 补稳 783205b（测试面单文件 +52/−17，生产零改动），收口形态：复跑定因证伪票面「~9k 楔死/量化让出未落地」归因（该机制已在 run_quantization_task_loop 落地且本装配不拉量化协程），实锤新根因——VADD 慢路径冷读臂经 wkv session/raw/read.rs 复检预算尽上抛可重试 Index(LockTimeout)、调用方不收敛致间歇 -ERR（5/8 实测；桥层外层重投档实验 32 轮预算尽且 5/8→12/12 恶化已撤）；ignore 保留转该业务缺陷回归载体，定因改述入用例文档与 ignore 文案，装配订正 wnode_test::auto_exec 快慢两臂收口＋TEST_DEADLINE(40s) 有界收割。续排注：wkv 冷读收敛业务缺陷票由主代理另立（收敛机制须含链头单调收窄/让位序），落地后删本 ignore 即转常规活性回归；read_multi 批量冷读臂同契约断链面未演练在案。

甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P2
核验记录：rust 亲验——tests/vector_set_concurrent_vadd_disk_spill.rs:69 #[ignore] 现读在位（:67 「未落地。修复后移除」注释）；vector_manager_quantization.rs:100-127 非阻塞取锁+yield_now/sleep(1ms) 让步形态已落地（注释归因与现码矛盾坐实）；grep start_quantization_tasks 全测试文件零命中、生产唯一拉起点 service.rs:2006（get_session 惰性路径）亲验——注释所述楔死机制在装配内不可能发生，真因未定因属实。C# 亲验——ConcurrentVaddDiskSpillTests.cs:56 [Test] 常规活动回归（无 Ignore/Explicit）现读亲见。查重：全 task 池 grep concurrent_vadd/disk_spill 零追踪票；deviations §22/§23/§44/§75 均不触及并发 VADD 活性面；ing wnode-quant-lock-drop 票系删锁竞态轴且验证点不含本测试复跑，非并案。架构：复跑定因→订正注释→移 ignore 转常规+有界时限包装（handshake_timeout.rs 先例），三步最小闭环、无编造测试、若楔死立业务票承接根因——符合测试对标与活性回归纪律。格式：纯文本、双侧齐全。定级 P2：生产同形态负载活性缺陷无闭环载体+失真归因误导修复方向，属防护面缺口（楔死未复现定案前不升 P0/P1）。

并发 VADD 活性回归测试被 ignore 雪藏：楔死实测记录无票据追踪，注释归因与现码矛盾致定因链断裂

审核结论：通过（席位 zcode-r21-review-ignoredtest，2026-09-26，dev 分支现树双侧亲验）

逐点亲验记录：
1. ignore 雪藏与零追踪属实：vector_set_concurrent_vadd_disk_spill.rs:69 #[ignore] 在位；git log --follow 确认该文件自 init 提交 72aeb39 即带 ignore 入库（从未在 CI 活动集中运行）；grep 全 task/ 池（todo/issue/ing/reject/done）+ doc/zh/deviations.md + review_history 对 concurrent_vadd/disk_spill 零命中，活性缺陷确无任何追踪载体。
2. 注释归因与现码矛盾属实且加重：git show 72aeb39:wedb/wnode/src/resp/vector/vector_manager_quantization.rs 证实 init 同一提交内 run_quantization_task_loop 已含 WouldBlock => return false + attempt<16 yield_now + 其后 sleep(1ms)（现树 :100-127 同形态）——即「量化 worker 非阻塞取锁 + 让出修复未落地」的 ignore 注释从写下那一刻起就与同提交代码矛盾，非事后过时，属先天失真。
3. 测试装配不启量化协程属实：全测试文件无 start_quantization_tasks 调用；run_quantization_task_loop 唯一 spawn 点是 start_quantization_tasks，后者全仓生产调用面仅 service.rs:2006（get_session 内、首会话惰性拉起，quota 递减分摊）；本测试经 store.new_session() + StoreGarnetApi::new 直连不经 get_session；vector 模块 spawn 仅量化 worker 与 cleanup 两处且均为该惰性路径——注释所述「量化协程与本 worker 同处一个 compio runtime」的楔死机制在装配内不可能发生，~9k 次楔死（init 注释自陈实测：加大让出比例亦不恢复）的真因从未被定因。
4. C# 对位属实：ConcurrentVaddDiskSpillTests.cs:56 系 [Test] 无 Ignore/Explicit（45s 停摆判 DEADLOCK :120/:130、minInserts=100 下界 :137），常规活动回归；守护修复形态在 VectorManager.Quantization.cs:133 ReadVectorIndexCore(nonBlocking: true) + :105-107 Task.Yield/Task.Delay(1)。
5. 查重干净：deviations 向量条目（§22 冷态 WRONGTYPE、§23 相似度文案、§44 重放幂等、§75 向量键 TTL）均不触及并发 VADD 活性面；r17 两张锁早放票（ing）系 VADD/quant 删锁 DEL 竞态孤儿写轴，验证点均不含本测试移除 ignore 复跑；r21-timer 监督票系 panic 死亡观测轴（楔死形态任务活着不 panic）。有界收口先例 handshake_timeout.rs:37 TEST_DEADLINE 30s 在位。

执行方案优化裁定：
原方案三步已最小且闭环，照准并补三点：
1. 第 1 步复跑命令照准（--run-ignored only 单测，20s 运行 + 15s 停摆窗，非长测试）；楔死复现时抓栈定因立业务缺陷票并将本票转为其回归载体；复现不稳定时可临时加大 RUN_SECONDS 再抓（不入库）。
2. 第 2 步订正注释为无条件前置项：依亲验记录 2，量化让步失真表述与装配矛盾描述无论复跑结果如何均须删除改述（先天失真与复跑结论无关），改述内容以第 1 步定因结果为准。
3. 第 3 步有界时限包装照 handshake_timeout.rs 先例，deadline 取 RUN_SECONDS + STALL_LIMIT_SECONDS + 余量（约 40s 量级）；若定因归属在办票领地，在其验证点登记本测试转绿为验收项。

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 侧 ConcurrentVaddDiskSpillTests.cs:ConcurrentVaddToSpilledSetMakesProgress 是常规运行的活性回归（NUnit [Test] 无 ignore）：8 线程并发 VADD 至溢写集合，停摆监视 45s 零进展即判 DEADLOCK，minInserts 下界证明溢写路径真实演练。其守护的生产修复形态是 VectorManager.Quantization.cs:QuantizationTaskAsync 的量化 worker 非阻塞取锁（ReadVectorIndexCore(nonBlocking:true)）加协作让步（前 16 次 Task.Yield、其后 Task.Delay(1)），保证 VADD 持独占集锁等盘读时盘读完成项永远可被调度。C# 侧该回归始终在 CI 活动集中，缺陷与修复永不脱钩。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧转写测试 wnode/tests/vector_set_concurrent_vadd_disk_spill.rs:concurrent_vadd_to_spilled_set_makes_progress（真活性断言：STALL_LIMIT 15s 停摆判 DEADLOCK、计数一致、minInserts 下界）被 #[ignore = "并发 VADD 死锁复现（~9k 次后楔死于 VADD，量化锁竞争）；src 修复后移除"]（:69）雪藏。三点断链确证：
a) 实测楔死记录（8 线程 ~9k 次插入后全部楔死于 api.exec、加大让出比例亦不恢复）自 init 提交入库后，全 task/ 池（todo 6 票、issue 6 票、ing 35 票、reject 9 票、done 2 票）与 doc/zh/deviations.md 及 review_history r15-r21 共 41 档均无对应追踪票，活性缺陷无闭环载体；
b) ignore 注释归因与现码矛盾：注释称「rust 侧『量化 worker 非阻塞取锁 + 让出』修复未落地」，而 wnode/src/resp/vector/vector_manager_quantization.rs:100-127 的 run_quantization_task_loop 已 1:1 落地非阻塞取锁（read_vector_index_core(…, true) WouldBlock 返 false）加协作让步（attempt<16 yield_now、其后 sleep(1ms)），归因失真；
c) 注释称「量化协程与本 worker 同处一个 compio runtime，同步 exec 循环不让出则量化任务永久饿死」，但该测试装配为 VectorManager::new 直用加 api.exec，全测试文件无 start_quantization_tasks 调用（量化协程唯一生产拉起点是 wnode/src/service.rs:2006 的 get_session 首会话惰性路径，本测试不经网络会话），量化协程在测试内根本不会运行，注释所述楔死机制与装配矛盾，真实根因（8 worker 各持独立 compio Runtime 共享同一 WedbStore 的盘溢写竞争面）从未被定因。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
已知活性缺陷（生产负载同形态：多连接并发 VADD 大向量集合触发盘溢写）无票据、无定因、无验证闭环，#[ignore] 一次性切断「缺陷—修复—回归转绿」生命周期；两张开办票 wnode-vadd-lock-drop-before-try-add 与 wnode-quant-lock-drop-before-backfill 的验证点均不含本测试移除 ignore 复跑，修复落地后亦无强制复验钩子，楔死若仍在将永久静默；注释归因失真直接误导后续修复方向（朝已落地的量化让步面错误排查）。另测试尾部 handles.join().unwrap() 在 worker panic 时会传播，但停摆楔死形态下 join 永不返回，测试进程需依赖 nextest 超时兜杀，无自身有界收口（对比同仓 handshake_timeout.rs:37 的 30s TEST_DEADLINE 包装先例）。

涉及代码：
rust 文件与函数：
wedb/wnode/tests/vector_set_concurrent_vadd_disk_spill.rs:concurrent_vadd_to_spilled_set_makes_progress（:69 #[ignore]、:104-137 楔死归因注释、:96-131 worker 装配）
wedb/wnode/src/resp/vector/vector_manager_quantization.rs:VectorManager::run_quantization_task_loop（:100-127 非阻塞加让步已在位，注释归因的反证）
wedb/wnode/src/service.rs:StorageSessionProvider::get_session（:2006 量化协程唯一生产拉起点，测试不经此路径）

对应 c# 文件与函数：
garnet/test/standalone/Garnet.test.vectorset/ConcurrentVaddDiskSpillTests.cs:ConcurrentVaddToSpilledSetMakesProgress（常规活动回归，无 ignore）
garnet/libs/server/Resp/Vector/VectorManager.Quantization.cs:QuantizationTaskAsync（nonBlocking 取锁加让步修复形态）

精炼执行方案：
1. 单点复跑定因：cargo nextest run -p wnode --run-ignored only concurrent_vadd_to_spilled_set_makes_progress（约 20s+15s 停摆窗，非长测试）。仍楔死则抓现场（worker 栈/锁等待链，重点查 8 Runtime 共享 WedbStore 盘溢写与 SlowWait 驱动路径）立对应业务缺陷票并将本票挂为其回归载体；已绿则直接进入第 2 步。
2. 订正 :104-137 两段归因注释：删除「量化 worker 非阻塞取锁加让出修复未落地」失真表述（该形态已在 vector_manager_quantization.rs 落地），按第 1 步定因结果改述真实机制；同步修正「量化协程与本 worker 同处一个 compio runtime」的装配矛盾描述。
3. 移除 #[ignore] 转常规回归，并按 handshake_timeout.rs 先例为整体用例加有界时限包装（停摆楔死形态下确定性失败而非依赖 nextest 兜杀）；若复跑确认根因为某在办票领地，则在其验证点中登记本测试转绿为验收项。
