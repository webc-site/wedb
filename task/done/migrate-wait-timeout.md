# 迁移停等无超时、参数与结果吞没（next/ds.net.md 条 11 / next/net.md 条 7）


## 甄别结论

四点全部成立，其中一点按 C# 证据修正口径后执行。


### 1. 批次 ACK 停等无超时：成立

C# 停等原语是 `Task.WaitAsync(_timeout, _cts.Token)`，覆盖迁移会话全部远端 await：
MigrateSessionCommonUtils.cs:HandleMigrateTaskResponseAsync:347（批次 ACK）、
MigrationDriver.cs:40（SETSLOTSRANGE）、MigrateSession.cs:202（AUTH）。
超时 → OperationCanceledException → Status = FAIL → 迁移失败。

rust 侧 run_keys_migration_driver 的 execute_cluster_migrate_async /
set_slot_range_async 全部裸 await，目标挂起则任务永挂。

停等形态核实：wconn 会话 execute_for_*_result_async 是普通 compio future，
await 挂起让出单线程 reactor，不死锁；加超时用 compio::time::timeout
（wedb/src/client.rs:86 connect_async 已有先例，200ms）。超时后迟到 ACK 会
污染连接 → 失败路径显式 client.dispose() 弃连（对标 C# MigrateSession.Dispose
的 _cts.Cancel + gcs.Dispose 语义）。

timeout 死字段（migrate_session.rs:23）：C# _timeout 由 MIGRATE 命令第 5 参
流入（MigrateCommand.cs:113），存 TimeSpan 后消费于上述全部停等点。
rust 侧接法：driver 全部远端 await 包 compio::time::timeout，时长取
spec.timeout；<= 0 时取默认 60000ms（redis-cli MIGRATE 默认口径，防零值
退化成立即超时）。字段转为真实消费，不删。


### 2. STABLE 回滚三元组复制三份：成立

migrate_driver.rs :259-262 / :311-314 / :341-344 三份相同循环。
提取私有辅助 try_recover_from_failure，对标
MigrationDriver.cs:TryRecoverFromFailureAsync：
远端逐 range SETSLOTSRANGE STABLE（nodeid=None，失败仅 log 留痕不阻断）→
本端 reset_local_slot → 会话 status 置 MigrateState::Fail。
顺序对齐 C#：先远端 STABLE，再本端回退（现 rust 顺序相反，一并修正）。


### 3. 完成哨兵 / NODE / relinquish_ownership 吞没：成立

- 哨兵（空载荷帧）：C# 中每次传输收尾的响应都过
  HandleMigrateTaskResponseAsync 非 OK 判败；rust `let _ =` 吞没哨兵应答，
  远端导入残缺时源端照常交权。改为判败 → recover + Err。
- 远端置 NODE：C# SLOTS 路径 BeginAsyncMigrationTaskAsync:194 失败 →
  log + TryRecoverFromFailureAsync + FAIL。rust 吞没 → 同改。
- relinquish_ownership：C# 同处 :203 失败 → log + recover + FAIL。
  rust 吞没 → 同改。
- 数据回滚的甄别拒绝：C# TryRecoverFromFailureAsync 只回滚槽位状态
  （远端 STABLE + ResetLocalSlot），不回收目标端已导入键数据；且 KEYS 路径
  失败时 C# 连槽位 recover 都不做（仅 Status=FAIL + remove task + IOERR）。
  「远端已导入批次数据回滚」在 C# 语义中不存在，不硬造。


## 对标与改动点

文件：wedb/wedb/src/server/migration/migrate_driver.rs

1. 新增常量 DEFAULT_MIGRATE_TIMEOUT_MS = 60000 与 wait_dur 辅助
   （spec.timeout <= 0 取默认）。
2. 新增 wait_remote 停等包装（compio::time::timeout，超时 →
   Error::Io(TimedOut)）。
3. 新增 try_recover_from_failure（对标 MigrationDriver.cs:TryRecoverFromFailureAsync），
   内含 client.dispose() 弃连防错位 ACK，替换三份内联回滚。
4. 新增 set_slot_range_checked（对标 MigrationDriver.cs:TrySetSlotRangesAsync：
   WaitAsync(_timeout) + 非 OK 判败），IMPORTING/NODE 共用。
5. 新增 send_batch_and_wait（对标 HandleMigrateTaskResponseAsync 的
   WaitAsync(_timeout) + 非 OK 判败），批次 / 收尾 / 完成哨兵共用。
6. 五个失败点（IMPORTING、本端 MIGRATING、批次、哨兵、NODE）与
   relinquish 失败统一走 recover + 显式 Err；log 留痕。

范围外仅记录不修改：run_keys_migration_driver 结束后不从
MigrationManager 移除任务（C# TryStartMigrationTaskAsync finally 的
TryRemoveMigrationTask），待 MIGRATE 命令发送侧转写时定归属。

测试：wedb/wedb/tests/cluster_migration.rs 补脚本化假目标端
（compio TcpListener 逐帧应答，对标 tests/appendlog_reject_disconnect.rs 的
reject_after_handshake_node 模式），用例：
1. 目标端静默 → 停等在 spec.timeout 量级超时报错（非永挂），源端键保留；
2. 批次 -ERR → recover（假端观察到 SETSLOTSRANGE STABLE 帧）+ 显式报错
   + 键保留；
3. 全链成功（批次 + 哨兵 + NODE 依次 +OK）→ Ok(条数)，非 copy 模式下
   已传输键删除，帧序 IMPORTING → MIGRATE → MIGRATE → NODE。


## 验收口径

1. ./clippy.sh 零警告（禁 allow）。
2. ./test.sh 全过。
3. bun ./js/check.js 无新增缺失（本改动不删除任何 rust 符号，无 ignore 登记）。
4. MigrateTaskSpec.timeout 有真实消费点（driver 停等）。


## 验证结果

实现落地（wedb/wedb/src/server/migration/migrate_driver.rs）：

1. 全部远端 await 经 wait_remote 限时，MigrateTaskSpec.timeout 接入真实
   消费（<= 0 取 DEFAULT_MIGRATE_TIMEOUT_MS = 60000，redis-cli 默认口径）。
2. try_recover_from_failure 替换三份内联回滚三元组：远端 STABLE（nodeid=
   None）→ 本端 reset_local_slot → status 置 MigrateState::Fail → 弃连。
3. 实现中发现并修复 C# 无对应物的传输层约束：wconn 为严格停等客户端，
   停等超时后连接残留未决响应，恢复帧在同连接上永远排队——poisoned
   （is_timeout_err）时 recover 先 reconnect 再发 STABLE（对标 C#
   CheckConnectionAsync 的 ReconnectAsync 保供语义），测试帧序证实生效。
4. 完成哨兵 / 远端 NODE / relinquish_ownership 失败不再吞没，统一
   recover + 显式 Err。

测试（wedb/wedb/tests/cluster_migration.rs，新增脚本化假目标端基建）：

- migrate_driver_silent_target_times_out_instead_of_hanging：批次静默 →
  300ms 停等超时（非永挂）→ recover 重连发 STABLE → 键保留
- migrate_driver_batch_reject_recovers_and_keeps_keys：批次 -ERR → recover
  STABLE 帧 + 显式报错 + 键保留
- migrate_driver_sentinel_failure_fails_explicitly：哨兵 -ERR → 显式报错
  （两帧 MIGRATE 计数正确）+ recover + 键保留
- migrate_driver_node_assignment_failure_fails_explicitly：NODE 非 OK →
  显式报错 + recover + 键保留
- migrate_driver_full_flow_success_deletes_transferred_keys：全链成功，
  帧序 SETINFO → SETNAME → IMPORTING×2 → 批次 → 哨兵 → NODE×2，
  非 copy 模式已传输键删除

甄别修正记录：

- 「批次失败时远端已导入批次不回滚」：C# TryRecoverFromFailureAsync 只
  回滚槽位状态，不回收目标端已导入数据；且 KEYS 路径失败时 C# 连槽位
  recover 都不做。数据回滚不实现（硬造即偏离 C#），仅状态回滚。
- check.js 曾报 wait_remote / set_slot_ranges_checked 重复登记
  TrySetSlotRangesAsync 映射：wait_remote 为机制辅助，注释改为机制描述
  不再登记具体 C# 函数。

验收：

- ./clippy.sh EXIT=0 零警告（无 allow）
- ./test.sh EXIT=0：wedb 2007 tests + regress 2 tests 全过
- bun ./js/check.js 无输出（无新增缺失、无重复定义）

合并：分支 w2-migrate-timeout 先 merge dev（无冲突）后合入主目录，
worktree 已清理。
