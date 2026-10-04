终态：合入 2045853（实改 98960c9），run_exec 复入轮比对入参 lock_prefix 与锚定值，异源走「注销旧票据→try_acquire_barrier→unlock_all_keys→register_run_preamble 重展开→try_lock_all_keys_once」单机制收口，watch_container/txn_keys 跨轮保全，exec.rs:70 补锁点保持纵深兜底；新增 tests/exec_relock_contended_generation_swap.rs 三用例，cargo check --all-targets 全绿。

审核结论：通过（2026-10-01 独立复核，双侧锚亲验成立，五池无重复，deviations 无同面在册裁决）

判定要点：
1. 真实性确证：transaction_manager.rs:278-293 run_exec 的 armed 门（:279-285）仅首轮消费 lock_prefix（register_run_preamble :330 钉定 self.lock_prefix 快照、:357 按其现算排队键哈希），重驱轮整段跳过、入参前缀被忽略——network_exec 重驱轮仍传现值 session_prefix（txn_resp_commands.rs:202）但 armed 门下不达消费面，票面链成立。争用态零闩确证：try_lock_all_keys_once（txn_key_entry.rs:233-248）失败经 acquire_plan :203 → release_held（:169-181）逆序放尽本轮已取前缀，键集/计划/钉定索引保留（ensure_plan :213-223 一次钉定缓存复用，跨轮不重算）。取锁成功点到 :718 补锁点间同步段确证：finish_run_postlock WATCH 校验（:395）+ TxnStart 入账（:403-414）→ 数组头（txn_resp_commands.rs:205）→ 消费循环推进（core.rs:1252）→ 首条重放命令分派至 garnet_api/mod.rs:718，全程无 await；txn_prefix_needs_rearm 仅 Running 态生效（txn.rs:240-252），窗内他 worker 写按新前缀落异桶、不被旧代桶闩阻（物理前缀含虚拟库版本，db.md 1.1；vdb flush.rs:32/71/100 bump_generation；WATCH 版本轨=逻辑域 session/mod.rs:452-464，末次校验已在窗前，双闸均不拦截的定性准确）。单 worker 不可达、多 worker 可达定性准确（server.rs:646 start_tcp_workers SO_REUSEPORT thread-per-core 多线程形态在册）。C# 结构性免疫确证：TransactionManager.cs Run（:470-521）内联无重驱分相，TxnKeyEntry.cs :88-90/:106-124 锁身份=store 裸键哈希无代际维度，DatabaseManagerBase.cs FlushDatabase 仅 ShiftBeginAddress（实际 :305，票面引 :301 为同方法邻行）不改锁身份。
2. 非重复确证：三近邻票判据面均不覆盖——wtxn-multi-queued-lock-hash-stale-across-generation 收口 EXEC 起点首轮单次展开；wtxn-exec-replay-window-generation-swap-lock-mode-degrade 收口 Running 重放窗（即 :718 补锁点本体）；wtxn-queued-keys-push-dedup 系排队期 CPU 放大与重驱键集膨胀面。取锁成功点至补锁点之间的获取窗三票皆未触及。deviations §115（WATCH 键臂重展开登记）/§121（排队期锁集跨事务零残留）/§139（covers 判定与慢体跨段界延伸）均不同面。
3. 定级 P3 恰当：隔离承诺真破口，但可达窗微秒级、需争用∩换代∩他 worker 写三事件精确交叠，无确定性触发臂（对照两张 P2 近邻票均有确定性触发或全重放窗宽暴露）。
4. 方案可落：复验锚 self.lock_prefix 已在 :330 落存，复入轮比对即判换代；注销/重注册复用既有原语（txn_barrier 票据 take + end_txn、key_entries.unlock_all_keys 幂等且争用态零闩、register_run_preamble 全量重展开），watch_container 与 txn_keys 须跨轮保全（禁走全量 reset——其清 txn_keys）；:718 保留纵深兜底正确。格式纯粹合规。

整理优化执行方案：
1. run_exec 复入轮（exec_lock_armed 已真）先比对入参 lock_prefix 与 self.lock_prefix：同源直入 try_lock_all_keys_once；异源（会话域换代）则重走首轮同款门序——注销旧票据（txn_barrier.take + end_txn）→ try_acquire_barrier（PREPARE_GROW 争用回 Contended 让步）→ key_entries.unlock_all_keys（清 keys/latch/plan，争用态本无闩，幂等）→ register_run_preamble(lock_prefix, false) 重展开（WATCH 键 save_lock_hashes 与排队键 iter_with_lock 同一现值前缀，watch_container/txn_keys 原样保留）→ try_lock_all_keys_once。单机制收口，杜绝旧代桶计划跨代取锁。
2. 补锁点 garnet_api/mod.rs:718 保持不动（纵深兜底），严禁撤除。
3. 测试验证点：多线程注入「首轮 Contended → 换号 → 重驱」三步序列（先例 wtxn/tests/exec_replay_generation_swap_lock.rs、wnode/tests/exec_replay_generation_swap_lock.rs），断言重驱轮持新代桶闩（held_bucket_exclusive 对新代桶下标断言）且他线程对排队键写入被阻至事务提交；对照无换号重驱路径行为不变、同源重驱零重注册开销。

EXEC 取锁争用重驱轮不复验物理前缀，换号窗后按旧代桶计划完成取锁，取锁成功到补锁点之间存在无保护写窗

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 锁轨身份是 store 裸键哈希（TransactionManager.cs:Run :470-521 + TxnKeyEntry.cs:LockAllKeys），无物理代际维度；FLUSHDB/SWAPDB 换号（DatabaseManagerBase.cs:301 仅 ShiftBeginAddress）不改锁身份，取锁重试无论何时成功，持锁桶与数据所在桶恒同一身份，EXEC 取锁成功后他连接写必被阻——C# 结构性免疫，本仓物理前缀编码（db.md 1.1：前缀含虚拟库版本，换号同键落异桶）引入了 C# 不存在的代际维度，重试路径须自证前缀新鲜度。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   wedb/wtxn/src/transaction_manager.rs:279-293 run_exec：exec_lock_armed 门只让首轮消费 lock_prefix（try_acquire_barrier + register_run_preamble），首轮键展开与 ensure_plan 钉定后争用回 Contended，键集、计划、钉定索引原样保留；重驱轮 armed 已置真，入参 lock_prefix 被整段忽略，不重验前缀新鲜度，按旧代物理前缀桶计划完成 try_lock_all_keys_once。唯一兜底是取锁成功后重放首条命令前 wedb/wnode/src/resp/garnet_api/mod.rs:718 txn_prefix_needs_rearm → rearm_txn_locks_for_generation_swap 补锁新代桶，但该补锁是同步段（TxnStart 入账 + 数组头 + 首条命令解析分派，无 await）前的固定点，取锁成功点到补锁点之间存在无保护窗。
3. 逻辑危害确证
   危害链需三事件复合：EXEC 首轮桶闩争用（Contended）∩ 争用窗内他连接 FLUSHDB/SWAPDB 换代（新代写入畅通，事务此刻无闩可阻）∩ 取锁成功到 :718 补锁的微秒级窗内他 worker 对同键写入落盘。多 worker（compio thread-per-core）部署下他 worker 写不被任何闩阻挡即落盘，事务随后覆写——EXEC 取锁后他连接写必被阻的隔离承诺在此窗破口。旧代桶闩取锁恒成功（旧域无写者），WATCH 校验走逻辑域版本轨不受代际影响，双闸均不拦截。可达窗微秒级、需争用与换号精确交叠，定级 P3；单 worker 形态不可达。

涉及代码：
rust 文件与函数：
wedb/wtxn/src/transaction_manager.rs:run_exec
wedb/wtxn/src/txn_key_entry.rs:TxnKeyEntries::ensure_plan
wedb/wtxn/src/txn_key_entry.rs:TxnKeyEntries::try_lock_all_keys_once
wedb/wnode/src/resp/garnet_api/mod.rs:exec（:718 txn_prefix_needs_rearm 补锁点）

对应 c# 文件与函数：
garnet/libs/server/Transaction/TransactionManager.cs:Run
garnet/libs/server/Transaction/TxnKeyEntry.cs:LockAllKeys

精炼执行方案：
1. run_exec 复入轮（armed 已置真）复验 lock_prefix 与注册轮物理前缀同源：会话域换代（前缀变）则先注销旧票据重走 register_run_preamble 再取锁（复用既有注销/注册原语，单机制），杜绝旧代桶计划跨代取锁。
2. 补锁点 :718 保持不动（纵深兜底），严禁撤除。
3. 测试验证点：注入「首轮 Contended → 换号 → 重驱」三步序列（多线程测试），断言事务最终持新代桶闩且他线程对排队键写入被阻至事务提交；对照无换号路径行为不变。
