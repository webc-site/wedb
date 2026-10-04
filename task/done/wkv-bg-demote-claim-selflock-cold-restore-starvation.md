归档注记：合入 dbd82a8c，SwapInWindowGuard 持有者身份沿物化通道透传，恢复复验免自锁，零新原子零新全局态

审核结论：通过（一订正已并入：契约引文行号订为 tiered_demote.rs:253-255。全锚亲验吻合：stub.rs:106 复验经 load_collection_stub 门禁 :155-156 命中自家 claim 回 MigrationBusy、claim 与门禁判据字节恒等（promote.rs:285/stub.rs:155 同表同键）、lazy_restore 无豁免臂无反例、每轮复现成立（守卫 RAII 轮末释放）；C# 外证属实（Locking.cs:273/:281 X 锁内 plain Read_RangeIndex 零封窗面）；§120 仅登 freq=0 缺省不跑，与本票独立；回收轮 200ms 恒常驻、命令错面缺省即达，P1 配位不降。查重：promote-ri-chain/transfer-out/orphan-domain-leak 三票轴正交、全仓零 MigrationBusy 自封先例，无需并案。处方为 claim 线性持有证明透传单点收口、Option 引用零开销，合单机制纪律）

后台降阶轮自封窗拒主：物化窗内取树的懒恢复走门禁装载入口，registry 缺席冷键永世不降阶（wkv 分层存储迟滞死区收敛面失效）

问题分析：
1. C# 契约对齐：C# 无分层降阶（集合恒驻对象域），亦无迁移 claim 概念——
RestoreTree（garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:273）
的恢复前置复验在 X 锁内走 plain session.Read_RangeIndex，无任何封窗判定，
「持有者自装载被自身封窗拒绝」的形态在 C# 结构上不可达。claim/门禁系 rust
转写新增面（RENAME/自迁移安全换入窗），其封堵契约在 rust 侧自闭且两侧均已
明文：门禁入口 load_collection_stub 注（stub.rs:149-153）声明对并发同键写臂
「读写一致按迁移忙拒绝」；未门禁入口注（stub.rs:161-164）与降阶模块头
（tiered_demote.rs:253-255）、「封窗单点」自身契约（scan.rs:30-32）三处一致
承诺「claim 持有者自装载不得被自身封窗拒绝」。故本票对照的不是 C# 原型，
而是 rust 自己两处已登记的契约——现状违背后者。

2. 工程现状确证（双侧读码至行）：
封窗登记：demote_candidate（wedb/wnode/src/resp/objects/tiered_demote.rs:265）
  经 try_swap_in_window（wedb/wkv/src/range_index/promote.rs:284-294）在
  session_meta_key(key) 上 try_claim_migration（wedb/wbftree/src/manager/mod.rs:647-653）。
基线自装载合规：:269-276 正确走未门禁 load_collection_stub_in_window
  （stub.rs:165-186）。
破口：物化通道 tiered_materialize_blob（scan.rs:36）:54 经 acquire_tree_read
  取树；树不在注册表时落入 lazy_restore_tree（stub.rs:100-129），其 :106
  恢复前置复验用门禁形态 load_collection_stub（stub.rs:144-159），:155-156
  的 migration_claimed（manager/mod.rs:657-658，与登记侧同 key_id_of 判据、
  同 map）命中自家 claim 返回 Err(Error::MigrationBusy)，经 acquire_tree_read
  （stub.rs:291；写臂同型 :347）上抛 → scan.rs:54 map_err → demote_candidate
  :287-288 `?` → Err(())。
轮面处置：tiered_demote_round :160-167 将 Err(()) 记 warn「后台降阶评估单键
  写回失败，留待下轮」+ aborted++，零状态变更；下一轮起窗、装载、物化全同
  重演，无任何逃逸面（封窗互斥登记保证自 claim 必自命中）。
同型面二：封窗变体 tiered_materialize_blob_sealed（scan.rs:155-176）:160
  先封窗、:172 调同一物化通道，同样触 :106 门禁；调用点 run_async_rmw 物化
  降级臂（rmw_helpers.rs:439-441）与 load_typed_sealed（rmw_helpers.rs:1001-1005）
  ——树摘除态下写回族命令每次都 Err(()) 应答慢路径存储错误，客户端重试不
  收敛（重试仍自拒），仅当别的原生臂/只读物化臂（slow_load_eval 不封窗，
  :894-898）偶然先行懒重登注册表才间接解堵。
纪律反例在册：common.rs 到期出账臂「封窗登记先于放守卫」（:530-535）即窗内
  必已持树守卫、不走懒恢复——降阶/物化通道是「先封窗后取树」，而 lazy_restore_tree
  的复验装载未参数化持有者身份，为唯一收口缺口。
测试盲区：demote 族测试（tiered_background_demote.rs:389、
  tiered_demote_registry_discovery.rs:83 等）一律「wkv promote 直接灌」构造
  候选，轮执行时树恒在注册表，acquire_tree_read 于 stub.rs:272 get_tree Some
  早返回，从不触懒恢复分支——缺陷对现册测试不可见。
未登记确证：deviations.md §120 只登记降阶轮寄生 expired-object-collection-freq
  旋钮（缺省 0 后台臂不跑）；本票是 freq>0 按运维口径开启后仍自封窗 starvation，
  两者独立。task/todo/ 现有 wkv/tiered/promote/demote 族票（promote-ri-chain、
  range-index-transfer-out、reviv 族、cold-bftree-observed-orphan-domain-leak 等）
  均未覆盖此面。

3. 逻辑危害确证（生产触发链）：HSET 将某 hash 撑过 65536 升阶建树 → 此后删
   成员走分层原生树内写臂（exec_tiered_by_op_code → finish_tiered_arm；
   apply_rmw_post_operate 不参与该臂，rmw_helpers.rs:641，:668 懒降阶 else 臂
   仅覆盖物化对象层写回），meta.size 跌到 ≤32768 而无前台即时降阶——迟滞死区
   自动回归唯一由后台降阶轮兑现（collection.md 3.2/3.3/8.4 与 §120 运维口径：
   freq>0）→ 键转空闲：日志持续推移使元记录出驻留区，recycle_cold_bftrees
   （wedb/wkv/src/gc/cold_tree.rs:72-133，200ms 常驻轮）按 1s 迟滞
   dispose_tree_under_lock(id_key,false) 摘树注册（文件保留、下次访问经
   get_or_open_tree 懒重开——设计本意即冷键必走懒恢复）→ 后台轮预筛入围
   （tiered_demote.rs:209-224 零树访问，不会再注册）→ :265 封窗 → :287 物化
   → 懒恢复复验命中自家 claim → Err(()) → warn 留待下轮，每轮同败。危害：
   (a) 迟滞死区唯一收敛机制对「真冷键」全体失效，键永久卡树态、信封形态不
   回归，恰是该机制存在的目标人群零命中；(b) 树数据文件与 bftree_domains
   登记永不释放（有键存续即泄漏，回收依赖降阶/删除，降阶已死）；(c) 每轮
   每键 warn 日志刷屏；(d) 同型面二客户端可观测：摘除态下物化写回族命令
   确定性报错。fail-closed 方向保证零数据丢失（树态保守保全），故不涉
   ACK 丢失面，但特性承诺与资源释放全面失效。

涉及代码：
rust：
wedb/wnode/src/resp/objects/tiered_demote.rs（demote_candidate:265/269-276/287-288、轮面 Err 处置:160-167、契约文:253-254、预筛:209-228）
wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs（tiered_materialize_blob:36/:54、tiered_materialize_blob_sealed:155-176、契约文:30-32）
wedb/wkv/src/range_index/stub.rs（lazy_restore_tree:100-129 尤 :106 门禁复验、load_collection_stub:144-159 尤 :155-156、load_collection_stub_in_window:161-186、acquire_tree_read:256-293 尤 :291、acquire_tree_write:315-349 尤 :347）
wedb/wkv/src/range_index/promote.rs（try_swap_in_window:284-294）
wedb/wbftree/src/manager/mod.rs（try_claim_migration:647-653、migration_claimed:657-658）
wedb/wkv/src/gc/cold_tree.rs（recycle_cold_bftrees:72-133）
wedb/wnode/src/resp/objects/rmw_helpers.rs（run_async_rmw 物化降级臂:439-441、load_typed_sealed:1001-1005、slow_load_eval 不封窗对照:890-898、finish_tiered_arm 边界注:641、懒降阶判据:668）
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs（封窗先于放守卫纪律反例:530-535）
wedb/wnode/tests/tiered_background_demote.rs:389、wedb/wnode/tests/tiered_demote_registry_discovery.rs:83（构造盲区）
c#：
garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs RestoreTree:273（锁内复验走 plain Read_RangeIndex，无 claim 面；自封窗形态 C# 结构不可达，rust 新特面须自闭）

精炼执行方案：
单一机制：懒恢复复验装载按持有者身份收口——tiered_materialize_blob 及其
sealed 变体把调用方持有的 SwapInWindowGuard（拿到守卫本身即该键 claim 的
排他持有证明，try_claim_migration 互斥保证他 claim 不可能并存）透传至
acquire_tree_read/acquire_tree_write，再到 lazy_restore_tree：携带本键守卫
时恢复前置复验改用未门禁 load_collection_stub_in_window，未携带者仍走门禁
load_collection_stub，并发迁移者的封堵拒绝面零变化。零新原子、零新全局态、
零第二套判定，非持有臂（热路径）判据指令数不变。
测试验证点：1) 降阶回归：构造升阶候选后先经 recycle_cold_bftrees（或直调
dispose_tree_under_lock(id_key,false)）强制摘除注册表，再跑 tiered_demote_round，
断言 demoted=1、键回信封形态可读、树注册与域表登记释放、数据逐成员保真
——现册只测 live 树形态，必须补 registry-missing 形态防复发；2) 前台同型面：
对摘除态分层键发一条物化降级写回族命令（经 run_async_rmw :440 通道），断言
首次即正常应答而非存储错误。

级别 P1
