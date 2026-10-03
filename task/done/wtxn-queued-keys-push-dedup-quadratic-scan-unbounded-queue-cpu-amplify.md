终态注记: 已合入 main（commit: 86844da）。收口形态：TxnKeysBuffer::push 摘除 O(N) 逐键比对改 O(1) 尾追（对齐 C#），同桶归并保留在 lock_plan 单点；TransactionManager 暴露 is_exec_lock_armed；network_exec 以 !is_exec_lock_armed 门控首轮推 WATCH 键防止争用重试重复膨胀；配套单元测试与回归测试全绿。

甄别结论:通过(P3,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-H，P3 级）。排队期 TxnKeysBuffer::push 逐键线性扫描全字节比较去重导致 O(N²) 超线性 CPU 放大事实确证，且同桶最强锁型归并已由 EXEC 期 lock_plan 单点承担，去重冗余。执行席遵照：push 摘除逐键去重改 O(1) 尾追（对齐 C#）；同时对 network_exec Started 臂重驱轮重推 WATCH 键做首轮门控收口，防止键集无界增长。

原票面：
排队期 TxnKeysBuffer::push 逐键 O(N²) 去重线性扫描，无界排队深度下服务端 CPU 超线性放大（C# 排队期 O(1) 追加、去重归并单点在 EXEC 期排序归并）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# 排队期 LockKeys（TxnKeyManager.cs:64-86）对每个键位做两笔 O(1) 摊还追加——TxnKeyEntries.AddKey（TxnKeyEntry.cs:84-106，尾槽写入 + 倍增扩容）与 SaveKeyArgSlice（TxnClusterSlotCheck.cs:18-30，txnKeysParseState 尾槽写入），全程不去重；重复键与同桶最强锁型归并由 EXEC 期 LockAllKeys 的排序 + 线性归并单点承担（TxnKeyEntry.cs:108-133，KeyHashComparer 升序后同键合并取最强锁型）。排队期成本与排队键数线性同阶，客户端可无限 MULTI 入队下无超线性放大。
2. 工程现状确证（Rust 实现路径与代码缺陷）：rust 排队期锁登记统一走 save_key_entry_to_lock → TxnKeysBuffer::push（wtxn/src/txn_keys_buffer.rs:89-101），push 内先线性遍历现有全部键逐键全字节比较去重并做锁型升级，再尾追——每键排队成本 O(现有键数)，整笔事务排队期合计 O(N²·L)（N 键数、L 键长）字节比较。该去重在语义上冗余：a) 同桶最强锁型归并已由 EXEC 期 lock_plan 排序归并单点承担（wtxn/src/txn_key_entry.rs:119-143，同桶 exclusive |= 合并），与 C# 归并位同构；b) 集群槽校验键表带重复无害（C# 同样带重复，NetworkMultiKeySlotVerify 承接）；c) perform_writes/is_read_only 判据对重复不敏感。唯一被去重「顺手」掩住的点是 network_exec Started 臂在取闩争用重驱轮会重推 WATCH 键入 txn_keys（wnode/src/resp/txn_resp_commands.rs:179-181，Contended → park_exec_lock_wait → 重驱复入同一分支），当前靠 push 去重幂等兜底——摘除去重时该重推面须同门控收口，否则键集随重驱轮次无界增长。工程危害面：MULTI 排队窗无界（客户端可无限入队，接收缓冲与 txn_keys 双双随行增长，与 C# 同形无界），rust 在同一无界面上叠加超线性 CPU——十万异键 MULTI 排队期触发 5×10^9 量级字节比较，单 worker 秒级到分钟级停摆（排队校验在泵消费循环内联），C# 同输入为 2×10^5 次尾追。前缀相近键（常见命名空间形态）使逐键全字节比较退化至最坏档。严重度 P3：无正确性危害（归并语义等价），系无界排队面上的服务端 CPU 超线性放大与 C# 成本形态分叉。
3. 逻辑危害确证：排队期停摆在泵消费循环内联执行，停摆期间同 worker 其他连接命令与心跳全部受阻（thread-per-core），恶意/病态客户端以一条大 MULTI 即可放大为单核拒绝服务窗口；C# 同输入无此放大，属可对拍的效能契约分叉。

涉及代码：
rust 文件与函数：
wedb/wtxn/src/txn_keys_buffer.rs:TxnKeysBuffer::push（逐键线性去重扫描）、clear
wedb/wtxn/src/txn_key_manager.rs:lock_keys、save_key_entry_to_lock（排队期逐键 push 消费点）
wedb/wtxn/src/transaction_manager.rs:register_run_preamble（EXEC 期 iter_with_lock 展开，归并真源在 lock_plan）
wedb/wtxn/src/txn_key_entry.rs:TxnKeyEntries::lock_plan（既有同桶最强锁型归并单点）
wedb/wnode/src/resp/txn_resp_commands.rs:network_exec（Started 臂重驱轮重推 WATCH 键面，去重摘除后须门控）

对应 c# 文件与函数：
garnet/libs/server/Transaction/TxnKeyManager.cs:LockKeys、SaveKeyEntryToLock
garnet/libs/server/Transaction/TxnKeyEntry.cs:TxnKeyEntries.AddKey（O(1) 尾追）、LockAllKeys（排序 + 线性归并单点）
garnet/libs/server/Transaction/TxnClusterSlotCheck.cs:SaveKeyArgSlice（O(1) 尾追）

精炼执行方案：
1. TxnKeysBuffer::push 摘除逐键去重扫描，改纯 O(1) 尾追（对齐 C# AddKey/SaveKeyArgSlice 形态）；is_read_only 对重复 Exclusive 不敏感无需改，同桶最强锁型归并由 EXEC 期 lock_plan 既有单点独占承担（单机制不双轨）。
2. network_exec Started 臂的 WATCH 键入 txn_keys 重推面以首轮门控收口（exec_lock_armed 同判据只读判据暴露或 push 前先判），杜绝去重摘除后重驱轮键集无界增长；verify_cluster_txn_keys 与 is_read_only 消费面对重复键语义回归确认。
3. 测试验证点：a) 十万异键 MULTI 排队期耗时线性回归（对比摘除前后）；b) Contended 重驱多轮后 txn_keys 键数恒定（重推门控锁测）；c) transaction_tests / txn_queue_lockset_residual / watch_version_regression 全绿。
