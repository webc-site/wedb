终态注记: 已合入 main（commit: 1f8d6bb）。收口形态：Smembers 触树前增 size==0 门直接写 0 长 set 响应；Srandmember 单成员形触树前增 size==0 门直接写 null；Lpos 语法解析后触树前增 size==0 门直接写 nil 或空数组；common.rs 补守约清单指引；新增 tests/tiered_dead_key_read_guards.rs 断言 meta 死树活时零触树。

甄别结论:通过(P2,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 审核席）。三臂缺门逐点坐实：tiered_guard Ok(false) 读臂仅记 size=0 仍返回 Some(guard)（common.rs:918-923）；OutFace::from_head count 恒 usize::MAX（common.rs:129）、meta.size 只进帧头位宽预留（:167），Smembers 全树扫；Srandmember 单形 ：154-175 无门与 count 形 ：185-192 有门不对称；Lpos :329 无条件触树但 len=0 时双支均不进扫（:331-335/:350、:390-397），危害仅触树无错输出（缺省形 nil/COUNT 形空数组恰为合法缺失应答），守约对照 Hrandfield hash.rs:614-621、Lindex :202-205、Lrange :264-267、Llen :189 亲验。可达性定谳（本票最关键修正）：meta 死而树活的分裂窗真实可达，但仅限排空在途交错窗——探测 live → 元记录墓碑（drain.rs:184-192 守卫形条带 X 锁内 / collection.rs:382-384 无守卫形裸 delete 连锁都不取，窗更宽）→ 读臂锁内 refresh 判死（heal.rs:61-70 三判据）→ delete_index（drain.rs:123-130）被共享读锁挡后；票面「Error::Swapped 中断与崩溃残留」措辞作废——该持久态下读命令路由探测 load_collection_stub 先见墓碑走缺席臂根本不进 tiered_guard，lazy_restore_tree 复核存活（stub.rs:416-418）也挡住。方案维持三臂 size==0 门（Srandmember count 形与 Hrandfield 既有同款单机制，guard 层统一短路因 output 形参与应答形态逐命令异构不可行、改 Ok(None) 混入非分层态降级漏斗非最小改，均否）；Lpos 门置于 :317 词元解析之后保错误帧优先；测试点修正：Swapped 残留夹具驱动不了三臂，改走 wkv 层 refresh_tiered_meta 直驱（tests/tiered_read_stale_meta.rs:678 先例，可断言 Ok(false) 时树仍在注册）或仿 zset_load_type_rmw_window_race.rs 真交错竞态夹具。查重：deviations §47 系号位空缺在册条不覆盖本面；wcol-random-family 票实存 task/done（负 count 参数放大面，与本票正交）；task 五池零同面。

原票面：
tiered_guard 判死读臂仍放行树句柄而 Smembers、Srandmember 单形、Lpos 三臂无 size==0 门直触树，窄窗回放已回收键陈旧成员集，违反 common.rs 读臂契约

问题分析：
1 Garnet 契约对齐（C# 原型行为与协议约定）：C# 对象恒驻内存无「元记录已死而树句柄仍在」态——libs/server/Resp/Objects/SetCommands.cs SetMembers/SetRandomMember 与 libs/server/Objects/List/ListObjectImpl.cs ListPosition 对缺失键一律空集/nil/整数 0 应答，不存在回放已删键内容的形态。本仓自定契约同向：tiered_collection_ops/common.rs:883-884 明文「Ok(false) = 键已被并发排空回收……size 为 0 的读臂不触碰树」。
2 工程现状确证（Rust 现有实现路径与代码缺陷）：tiered_guard（common.rs:889-931）读臂刷新返回 Ok(false) 时仅记 ctx.meta.size = 0 仍返回 Some(guard) 放行树句柄（:918-923），守约责任落在各臂自设 size==0 门。三臂缺门：Smembers（tiered_collection_ops/set.rs:121-137）经 stream_scan_face 全树扫——OutFace::from_head（common.rs:126-134）count 恒 usize::MAX、meta.size 仅作帧头预留提示不作扫描上界，size=0 不短路；Srandmember 单成员形（set.rs:154-175）reservoir 全树扫 SCAN_FROM_HEAD + usize::MAX 无门（count 形 :185-192 有门，同命令两形不一致）；Lpos（tiered_collection_ops/list.rs:329）在任何 len 判定前无条件 list_head_seq(tree) 取树头。同族守约对照：Hrandfield（hash.rs:617）显式 size==0 早退、Lindex/Lrange/Llen 经 len 折算天然短路。
3 逻辑危害确证（陈旧数据回放与同族契约失守）：刷新判死而树仍在注册表带数据的窄窗（drain Error::Swapped 中断、崩溃残留）下，Smembers 回放已死键全量陈旧成员集、Srandmember 单形回放陈旧单成员（内存态同刻应空数组/nil），以存储错误替代合法空应答的形态同窗存在；同族读臂契约执行不一致使「读臂不触死树」无法作为不变式引用，后续读臂增量失去判据锚。非内存态执行面，系恢复/中断窗对外数据泄露面。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs: tiered_guard（:889-931，Ok(false) 读臂放行点 :918-923）、stream_scan_face（:160-184，无零门全树扫）、OutFace::from_head（:126-134，count 恒 MAX 语义锚）
wedb/wnode/src/resp/objects/tiered_collection_ops/set.rs: tiered_set_arm（Smembers 臂 :121-137、Srandmember 单形臂 :154-175、count 形守约对照 :185-192）
wedb/wnode/src/resp/objects/tiered_collection_ops/list.rs: tiered_list_arm（Lpos 臂 :329 无条件取树头）
wedb/wnode/src/resp/objects/tiered_collection_ops/hash.rs: Hrandfield size==0 早退守约对照（:617）

对应 c# 文件与函数：
garnet/libs/server/Resp/Objects/SetCommands.cs: SetMembers、SetRandomMember（缺失键空集/nil 语义锚）
garnet/libs/server/Objects/List/ListObjectImpl.cs: ListPosition（缺失键整数 0/nil 语义锚）

精炼执行方案：
1. 三臂统一 size==0 门（与 Srandmember count 形 :185-192 同形、与 Hrandfield :617 同款，禁第二机制）：Smembers 直接 write_set_len 0 出空集帧；Srandmember 单形出 nil 帧（RESP2 空 bulk / RESP3 null，与现 :172 口径同源）；Lpos 判死即出 nil（缺省形）或空数组（COUNT 形）应答，三臂均在触树前短路。
2. tiered_guard 头注契约行 :883-884 后补一句守约清单指引（现三臂违例收口后同族全守约），防后续增量再漏。
3. 测试验证点：夹具构造 meta 死而树残（drain 注入 Swapped 窗或崩溃残留形态），Smembers/Srandmember 单形/Lpos 应空集/nil 零触树（扫描计数为 0 断言）；live 键三命令回归不回摆；Srandmember count 形既有门回归。
