甄别结论：通过（甄别席 zc-fix-r30-wkvcold，2026-09-26）定级 P2

核验记录（逐点现码亲验，不背书票面）：
1. 契约断链主锚属实，行号按现码订正：wkv/src/session/raw/read.rs 复检预算尽上抛臂在位——read_from_disk 走尽臂 Retry 超 MAX_DISK_RECHECKS（:902 内核常量 =16）后 :1034 `return Err(Error::Index(WindexError::LockTimeout))`，注释 :1028-1033 自陈「上抛可重试 LockTimeout 交调用方重投收敛」（票面席注 :1021 系 5ee53a4 时点，现码 +13 漂移）；同文件 drive_mem_read 预算尽臂 :299-300 同形上抛（INNER_LATCH_RETRY_BUDGET 尽，文档 :271-277 自陈「由调用方按既有错误通道应答可重试错误」）。预算常量来源与量级：INNER_LATCH_RETRY_BUDGET=1024（session/rmw_window.rs:155，pub(crate)，drive_mem_read 每轮 Retry 经 Backoff 退避计步），复检预算 MAX_DISK_RECHECKS=16 轮（每轮内又含 1024 步取闩环）。
2. 消费面属实：向量桥锚补全——wnode/src/resp/vector/vector_store_callbacks.rs `WedbVectorStoreCallbacks::read_outcome_async`（:438-464，冷臂 `read_raw_with` Err → `ReadOutcome::Failed`）、`rmw`（:720-754，Failed → return false :749-751，条带闸内无任何重投环）、`read`（:650-666）、`filter`（:770-801）同臂同形；false 上溯 wvector/src/service.rs `DiskANNService::insert`（:1109-1176，`DiskAnnInsertResult::StoreError`）→ wnode vector_manager.rs:727 `VectorOpError`/`ERR_VECTOR_SERVICE_RESPONSE`（:56）→ VADD 回 `-ERR Error indicating response from vector service`。全链确无收敛环亦无让位序，票面「直抛」坐实。read_multi 批量冷读臂（:568-641）：内存批直读 Err 未交付项并入冷批（天然重探一次），冷批 `read_batch_raw_with` Err → log + return false（:636-639），断链同谱——判并案（同一引擎修复点收口，勿另立；验收面补演练本臂）。
3. 实锤证据链文献一致：归档票 task/done/wnode-vadd-wedge-regression-ignored-no-ticket.md 归档注记与 wnode/tests/vector_set_concurrent_vadd_disk_spill.rs 用例文档 :68-83、ignore 文案 :85 三处在册互洽——5/8 轮间歇 -ERR、全程有推进 6k–21k 非楔死、桥层外层重投档 32 轮预算尽不收敛且恶化 5/8→12/12 已撤；提交 5ee53a4（合入 merge 5db78d8，+52/−17 测试面单文件生产零改动）git 亲验在册。与代码形态自洽：read_outcome_async 无重试、失败即整条回调 false，重投即把 16×1024 预算整段重跑，持续同桶写下复现不收敛顺理成章。
4. C# 对位亲验：garnet 树内锁重试收敛内聚于原语层——HandleOperationStatus.cs:15 `HandleImmediateRetryStatus`/:46 `HandleRetryStatus`（RETRY_LATER 刷纪元 + Thread.Yield 返 true），Tsavorite.cs:511/:550/:666/:709/:731/:753 与 ContinuePending.cs:108/:314 各操作臂入口 `while (HandleImmediateRetryStatus(…))` 原语内无界自旋环，从不上抛「可重试」错误给出调用方；重发收敛 ContinuePending.cs:ContinuePendingRead :76-123（注释 :78-82 明言「Reissue the Read(), using the LogicalAddress we just found as minAddress」，:83-84 谓词 `LogicalAddress > initialLatestLogicalAddress && (…|| LogicalAddress >= pendingState.minAddress)` 即链头单调收窄，Read() 更新 InitialLatestLogicalAddress 使下轮只搜更新版本）。C# 无「外置调用方重投」契约对物——rust 侧 read.rs 把收敛外置给调用方却连收窄机制本身也未落，属 rust 自有机制缺陷，票面 C# 契约断言改判为「C# 内聚形态锚 + rust 自创外置契约无承接」。
5. 查重干净：doc/zh/deviations.md 对 LockTimeout 唯一命中 :1901（wlua 事务锁模式条，锁相撞回错误帧判净语，非本族登记）；task/ 四池 grep LockTimeout/重投/复检预算/cold read 除本票外仅 issue/w2-gate-red-batch 提及 exec 自给键争用重投臂（他轴），无同案。全仓 Index(LockTimeout) 上抛点穷举（生产码五处）：read.rs:300（drive_mem_read，经一切内存读内核含批读 batch.rs）、read.rs:1034（冷读走尽臂）、write/inplace.rs:24（upsert/原位写预算环，:260/:487/:696 三调用点）、rmw_window.rs:496/:557（scoped 取闩）、ttl.rs:234（check_expired RETRY_LATER 折算）。消费点穷举：全仓零调用方有收敛环——各命令臂一律折 -ERR 交客户端重投（wnode garnet_api/slow.rs:424/:454 MSETNX bail_frame、key_admin_commands/slow.rs:266 等同形），此系 read.rs:275「调用方按既有错误通道应答可重试错误」的设计本意，单记录命令面客户端重投即等价 C# 客户端重试语义，不判断链；真断链=复合操作内多记录串联、单次失败整命令作废且内部无环面——即向量桥 rmw/read/filter 与 read_multi 冷批（本票并案），向量命令族为已知实锤受害族；写臂 inplace/rmw_window/ttl 三面同契约但单发命令客户端重投可承接，无实测恶化证据，随本票引擎侧修复自然受益，不单列危害。
6. 定级理由 P2：数据面可用性间歇缺陷（VADD/VSEARCH 冷读臂 -ERR）有实测复现与回归载体在位，但无数据丢失/损坏，触发需持续同桶并发写＋记录溢盘，向量 Set 属预览特性面；修复涉 wkv 冷读复检收敛机制核心（须设计链头单调收窄/等价让位序），复杂度中偏高。P1 不采：危害窗窄且 -ERR 对用户可重试（间歇非停摆）；P3 不采：定因实锤、载体就位、验收闭环俱全，非登记级。

精炼执行方案（甄别席按现码定案）：
硬约束（勿违）：收敛机制须含链头单调收窄或等价让位序——朴素外层重投已被实测证伪（32 轮预算尽且 5/8→12/12 恶化），不得作方案。
建议：第 1 步 wkv read_from_disk 走尽臂内聚 C# ContinuePendingRead 同形收窄——每轮 MemRecheck::Retry 重投时以已见最高链头为 minAddress 下界（new_cands 滤除 < minAddress 项，且要求下轮复检产出的 Retry 触发地址 strictly > 上界），使候选窗单调收窄、rechecks 预算内可达收敛（对齐 :1030-1033 注释自陈的 C# 缺口），预算尽兜底上抛形态保留（零缺席证据纪律不变，coldread_recheck_budget.rs 两测契约随收窄语义更新）；第 2 步 read_multi 冷批面并案验证（同一引擎修复点收口，锁测补批量冷臂在持续写窗内的收敛断言）；第 3 步验收=删 wnode/tests/vector_set_concurrent_vadd_disk_spill.rs:85 ignore 转常规活性回归（TEST_DEADLINE 40s 有界收割转绿，8 worker×20s 零间歇 -ERR）。

主代理新立（2026-09-26，来源：票 wnode-vadd-wedge-regression-ignored-no-ticket 收口席复跑定因，归档 5a798f7；本票未经甄别，甄别席按 fix.md 流程核锚定级）

wkv 冷读复检预算尽上抛可重试 Index(LockTimeout) 后调用方无收敛机制：持续并发写下同桶重试不收敛（32 轮预算尽），VADD 慢路径间歇 -ERR 回包

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 存储层读路径无该「可重试上抛后靠调用方收敛」契约形态（HdrHistogram 之外参考 garnet/libs/storage 读臂自旋重试内聚于原语层）。rust 侧 wkv 把复检让位收敛契约外置给调用方（session/raw/read.rs 复检预算尽臂注释自陈「交调用方重投收敛」），而调用方各面（向量桥 rmw/read 冷读臂等）无重投循环亦无让位序，契约断链。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   a. wedb/wkv/src/session/raw/read.rs 复检预算尽臂（席注 :1021 附近，甄别席按现码复核）上抛 Index(LockTimeout) 可重试错误，声明交调用方重投收敛。
   b. 向量桥消费面：graph_insert → 向量桥 rmw/read 冷读臂直通上抛，无收敛环。VADD 回 -ERR Error indicating response from vector service。
   c. 实测定因（vaddwedge 席 8 worker×20s 复跑，装配 test_store_config 小预算＋GC 关）：失败形态间歇 5/8 轮 VADD 出错，非楔死（全程有推进 6k–21k 插入）；桥层加外层重投档实验不收敛（32 轮预算尽）且失败率恶化 5/8→12/12——持续写负载下重试滞留推高冲突，朴素重投非解。
   d. 回归载体已就位：wedb/wnode/tests/vector_set_concurrent_vadd_disk_spill.rs（#[ignore]，装配为 wnode_test::auto_exec 快慢两臂＋TEST_DEADLINE 40s 有界收割）；本票修复落地后删 ignore 即转常规活性回归。
3. 逻辑危害确证
   并发写热桶上的向量 VADD（及同契约面读）间歇对用户报 -ERR，属数据面可用性缺陷；read_multi 批量冷读臂同契约断链面未演练，疑同谱（本票甄别席圈面裁定是否并案）。

涉及代码：
rust 文件与函数：
wedb/wkv/src/session/raw/read.rs：复检预算尽上抛臂（交调用方重投收敛契约出处，行号按现码复核）
wedb/wnode/src/resp/vector/vector_store_callbacks.rs：WedbVectorStoreCallbacks::read_outcome_async（:438-464 冷读臂 Err→Failed）/rmw（:720-754）/read（:650-666）/filter（:770-801）/read_multi（:568-641 冷批同谱并案）
wedb/wvector/src/service.rs：DiskANNService::insert（:1109-1176，StoreError 承载）；wedb/wnode/src/resp/vector/vector_manager.rs:727 + ERR_VECTOR_SERVICE_RESPONSE（:56，-ERR 帧出口）
wedb/wkv/src/session/rmw_window.rs:155（INNER_LATCH_RETRY_BUDGET=1024）、read.rs:902（MAX_DISK_RECHECKS=16）
wedb/wnode/tests/vector_set_concurrent_vadd_disk_spill.rs：回归载体（ignore 移除即验收锁）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/HandleOperationStatus.cs:HandleImmediateRetryStatus/HandleRetryStatus（RETRY_LATER 刷纪元+Yield 原语层内聚）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs（:511/:550/:666/:709/:731/:753 各操作臂 while(HandleImmediateRetryStatus) 内聚自旋环）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ContinuePending.cs:ContinuePendingRead（:76-123 重发以 minAddress 链头单调收窄——rust 复检面所缺机制原型）

精炼执行方案：
（待甄别席按现码定案；执行席注意硬约束——收敛机制须含链头单调收窄或等价让位序，朴素外层重投已被实测证伪；修复落地同时演练 read_multi 批量冷读臂同契约面或另立票；验收含删载体 ignore 转绿）

合入哈希：9031ecfb 收口形态：wkv read_from_disk 复检重投内聚 C# ContinuePendingRead 同形让位序（Retry 触发链头须严格越过已见最高链头界，界随轮单调升；票面 new_cands 地址阈滤窗经 collision_chain 三测＋reviv floor 测实测证伪弃用，重投窗取磁盘再入点全深度下扫），稳态链头冷读数轮收敛真实走尽/命中、预算尽兜底上抛与零缺席纪律保留；coldread_recheck_budget.rs 两测随收窄语义更新并增批量冷臂 read_batch_with 持续写窗收敛断言（read_multi 并案面）；wnode vector_set_concurrent_vadd_disk_spill.rs 摘 #[ignore] 转常规活性回归沙箱三连绿，wkv 全量套 168+全绿。
