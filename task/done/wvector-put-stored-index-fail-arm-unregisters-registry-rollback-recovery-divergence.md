归档注记：合入 80b1e54d，PutFailArm 语义位单点分流(Revert 撤除/RestorePrior 复原)，开窗失败中止迁移+新名剥窗口标志，双执行面闭合

甄别结论：通过 | 定级 P2 | 2026-09-27 主席现码复跑（逐点亲验）：put_stored_index 失败臂 remove（vector_manager_locking.rs:594-608，先 insert 后写透、败则撤）坐实；rename_vector_set_of 开窗臂 SUPPRESS 写透失败仅 log::error 续行（vector_manager.rs:1086-1088）、回滚臂原形重写再败亦仅 log（:1095-1099）坐实；三重叠加链控制流可达（开窗败撤→回滚写又败撤→旧集内存登记消失），且甄别补记：开窗臂单点写透失败即撤旧键登记，无需三重叠加即显形，执行面须一并罩住。SUPPRESS 删除抑制（:805）与重启 reconcile 收敛链（recovered_indexes/sweep_cleanable_contexts :1502-1531）亲验存在。裁定采方向 A、驳方向 B：回滚/恢复类「重写复原形」（含开窗标记重写与回滚原形重写两调用位）失败时保留内存原值不入账新值，常规写路径撤除契约不动；B 之运行期定向 reconcile 属扩面重设计，重启收敛路既存，不引入。实现须在 put_stored_index 单点上以最小语义位收口（单套机制，禁复制第二写透链），执行席先证形态再落笔。票面订正两处：C# 锚路径实为 garnet/libs/server/Resp/Vector/VectorManager.cs（非 Objects/Vector），登记为 ConcurrentDictionary 内存语义无失败撤除对位亲验（:175-176 型）；票引合入哈希 62a38af 因历史 init 重置不可考，回滚臂现码形态实测存在为据。并案核查：与 todo 邻票 wnode-vector-registry-recovery-put-fail-half-recovery-context-leak（恢复链标记不撤面）异点不并案；deviations 无在册条款。

问题分析：
1 Garnet 契约对齐：C# 主存写不失败（VectorManager 登记写透经 MemoryStream 单进程内存语义），无「登记失败撤内存」对应物；rust put_stored_index 失败臂撤内存登记（wedb/wnode/src/resp/vector/vector_manager_locking.rs:604-608 失败即 remove）系自带回滚，但其与回滚/恢复类「重写恢复原形」路径的组合语义未定义。
2 工程现状确证（c01c 票 rust_review 审查席上报，两未决并票）：其一，回滚重写再败交互——票 wnode-vector-rename-new-key-writeback-fail-still-deletes-old（合入 62a38af）的回滚臂经 put_stored_index 原形重写旧键，若重写再败，该失败臂会撤除旧键内存登记（用户侧集合消失，盘面带 SUPPRESS 记录滞留至重启 reconcile）——C# 主存写不失败故无此面。其二，开窗臂三重失败叠加——rename 开窗写透失败（vector_manager.rs:1083-1085 仅 log）+ 新键写透亦败 + 回滚重写再败的组合下，旧集内存登记同样消失，盘面记录在、重启可回建。显形条件均为同一故障域连续多次写透失败，窄时序非系统性。
3 逻辑危害确证：存储写透失败域内，回滚/恢复路径反而放大为「旧集用户侧消失」；盘面记录带 SUPPRESS 滞留，重启 reconcile 可收敛但运行期不可恢复，客户端视图与盘面真值分叉无报痕。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/vector/vector_manager_locking.rs:put_stored_index 失败臂（:604-608 撤内存登记）
wedb/wnode/src/resp/vector/vector_manager.rs:rename_vector_set_of 回滚臂（62a38af 引入）与开窗臂（:1083-1085）
wedb/wnode/src/resp/vector/vector_registry_recovery.rs:recovered_indexes 标记链（关联面）

对应 c# 文件与函数：
garnet/libs/server/Objects/Vector/VectorManager.cs（登记内存语义，无失败撤除对应物）

精炼执行方案：
1 方向 A（变体口）：为回滚/恢复类重写增「失败保留内存原值」的 put_stored_index 变体（不入账新值但也不撤既有登记），回滚臂与恢复链改用变体；常规写路径失败契约不变——单套机制新增语义位，须审核裁定是否构成第二形态。
2 方向 B（reconcile 定向收敛）：reconcile 对带 SUPPRESS 盘面记录的孤儿做运行期定向回收/重建（替代仅重启收敛），登记撤除契约不动。
3 两方向由审核裁定（A 治运行期视图、B 治收敛时效，可并取）；严禁放宽常规写路径的失败撤除契约。
4 测试验证点：三重写透失败注入下旧集登记留存（方向 A）或运行期定向收敛（方向 B）；常规写失败撤除契约回归不回退；c01c 票三案回归。
