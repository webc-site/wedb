终态注记: 已合入 main（commit: db16b14）。收口形态：wkv/src/range_index/ops.rs range_index_create 存根落盘后、入队 AOF 前调用 self.bump_watch_version(key)；预检与回滚分支零推进；wnode/tests/range_index_watch_fence.rs 增加 WATCH 缺席键 + RI.CREATE + EXEC 中止及预检拒绝测试全绿。

甄别结论:通过(P2,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 审核席）。全链亲验闭合：create 全链（ops.rs:62-240）零 bump，隐藏推进排查定论——save_bftree_meta_stub_with_prefix（stub.rs:223 起）、register_bftree_key（bftree_release.rs:97 起）、emit_event（event.rs:216 起）、rollback_fresh_bftree（ops.rs:247-252）四链内部零命中，五臂 bump 点全仓 grep 恰为 :372/:482/:621/:684/:688（migration.rs:426 系 RENAME 另案）。C# 双侧闭合：RangeIndexOps.cs:51/:107 RMW 入局，RMWMethods.cs:44/:288 RICREATE 落 InitialUpdater 分支、:377 PostInitialUpdater 首语句无条件 IncrementVersion 无命令型过滤，EXEC 校验链 TransactionManager.cs:494 同序必 abort，rust 放行确系偏离。RI 键用户键空间可见性三证钉死：TYPE 经 Meta 域答 RangeIndex（array_commands.rs:805-821）、EXISTS 三域探针计数（types.rs:206-209）、SET 族 ri_write_gate 拒写 WRONGTYPE（slow.rs:125-138、set.rs:105）。执行席遵照四点：一、bump 锚 wkv/src/session/mod.rs:817（票面 :808 系文档注释锚）；二、安置点取 save 成功（:221）后 emit_event（:227）前，与 set 臂「落盘 :355→放守卫→bump :372→AOF :378」同序；三、AlreadyExists/WrongType 早退臂（:93-110）与三回滚臂（:189-192/:208-211/:213-221）零推进，对齐 del 拒绝臂纪律（fence 测试 :149-156 同款）；四、回放面回放推版本系在册既定纪律（aof_processor_store_ops.rs:131 显式 bump 且头注钉「与主存用户键写入口同收口恰一次推进」，RI.SET 回放经 range_index_set 已带 bump），同根经方案 1 自然承接，无需另设回放专臂。测试落 wnode/tests/range_index_watch_fence.rs 同族 Env（真引擎+version_map_watch_hook+TransactionManager watch/run，:56-96 现成），RESP 注册名 RI.CREATE（command_table.rs:168）。查重：deviations §115/§131/§48/§123 均不覆本面，五张 RI.CREATE 系 done 票与既有 range_index_watch_fence.rs（头注自陈射程仅 set/set_batch/del 三臂）零覆盖，task 五池零同面。

原票面：
RI.CREATE 全链零递增观察者版本，WATCH 缺席键被并发建索引穿透放行 EXEC，偏离同文件 set/del 五臂栅栏纪律与 C# InitialUpdater 必推进语义

问题分析：
1 Garnet 契约对齐（C# 原型行为与协议约定）：C# RI.CREATE 走存储层 RMW——libs/server/Storage/Session/MainStore/RangeIndexOps.cs:107 RangeIndexCreate 经 stringBasicContext.RMW（头注自陈触发 RMWMethods.cs InitialUpdater），MainStore RMWMethods.cs PostInitialUpdater 在 CAS 挂链成功后恰一次 IncrementVersion（rust 侧 wkv/src/session/mod.rs:801 注释自证该族为无条件版本推进单点）。即 C# 建索引必然推版本：WATCH k（k 不存在）后他客户端 RI.CREATE k，EXEC 必判版本变更中止。本仓轴面规范同向：用户键变更包含缺席键创建，全路径无条件递增观察者版本。
2 工程现状确证（Rust 现有实现路径与代码缺陷）：range_index_create（wedb/wkv/src/range_index/ops.rs:62-240）全链——建树（:137-167）、换号旁表登记（:174）、存根落盘（:213-221）、AOF RangeIndexCreate 事件（:227-239）——零 bump_watch_version 调用。对照组同文件五臂全带：range_index_set :372、range_index_set_batch :482、range_index_del :621/:684/:688（含删空自愈臂与换代 Swapped 臂）；bump 内核单点现成（session/mod.rs:808 bump_watch_version，引擎钩子空时零开销旁路），且 ops.rs 内即有现成调用形态（self.bump_watch_version(key)）。stub.rs/heal.rs/promote.rs/drain.rs 四文件 grep bump_watch_version 零命中，责任不在内核下沉层。同根延伸：副本回放臂 handle_range_index_create_replay（wedb/wnode/src/range_index/range_index_manager_replication.rs:197）直调同一 range_index_create 同缺。命令层 network_ricreate（wedb/wnode/src/resp/range_index/resp_server_session_range_index.rs:361-390）亦无补推。
3 逻辑危害确证（事务与 WATCH 机制脱节）：WATCH k（k 不存在）→ 他客户端 RI.CREATE k 建索引成功 → EXEC：rust 侧版本槽未动，TxnWatchedKeysContainer::validate_watch_version（wtxn/src/txn_watched_keys_container.rs:86-91）比对新旧版本相等判「未被修改」放行，事务照常提交；C# 同序必 abort。乐观锁隔离被破坏——客户端基于键缺席快照的事务在键已变存活后静默穿行，与「缺席键变更亦须递增」纪律相悖（对位 DEL 缺席臂已无条件推进、RI.CREATE 作为缺席键变存活的最重形态反而零推进），且与同文件五臂栅栏纪律自相矛盾。

涉及代码：
rust 文件与函数：
wedb/wkv/src/range_index/ops.rs: range_index_create（:62-240 全链零推进）、range_index_set/del 五臂（:372/:482/:621/:684/:688 守约对照）、bump_watch_version 调用形态（同文件现成）
wedb/wkv/src/session/mod.rs: bump_watch_version（:808 推进内核单点）
wedb/wtxn/src/txn_watched_keys_container.rs: validate_watch_version（:86-91 比对放行点）
wedb/wnode/src/range_index/range_index_manager_replication.rs: handle_range_index_create_replay（:197 同根回放臂）
wedb/wnode/src/resp/range_index/resp_server_session_range_index.rs: network_ricreate（:361-390 命令层）

对应 c# 文件与函数：
garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs: RangeIndexCreate（:107 RMW 入口）
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs: PostInitialUpdater（CAS 挂链后恰一次 IncrementVersion）

精炼执行方案：
1. range_index_create 在存根落盘成功提交点后调 self.bump_watch_version(key)（推进序对标 set 臂 :372 相对落盘/事件的确切次序同形安置）；三处回滚臂（GenerationMoved 换代 :189-192、三域冲突 :208-211、落盘失败 :213-221）不推进——未提交不推进与既有删空自愈臂纪律一致。
2. 副本回放臂同根一并覆盖：handle_range_index_create_replay 链经同一 range_index_create 自然承接方案 1；执行席核回放面是否另有版本收口在册裁决，若有按裁决口径。
3. 测试验证点：WATCH 不存在键 + 并发（或窗注入）RI.CREATE + EXEC 必中止（nil 应答）；RI.CREATE 命中 AlreadyExists/WrongType/回滚臂不推进回归；set/del 既有 WATCH 栅栏测试（wnode/tests/delete_miss_watch.rs 同族）回归不回摆。
