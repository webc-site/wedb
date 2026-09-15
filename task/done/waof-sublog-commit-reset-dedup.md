# waof-sublog-commit-reset-dedup

来源：next/glm.md 条 22、23（主代理预清理后下发）。

## 甄别结论

一、条 22（reset 绕过 commit_lock 手抄四原子）：成立，执行。
二、条 23（第三处刷盘防重入状态机）：不成立。上一轮 aof-sublog-commit-dedup 已删 FLUSH_IDLE/RUNNING/PENDING 三态状态机，现状 waof_sublog.rs 的 commit 已是纯转发 wal.commit / wal.commit_to（Leader/Follower 级联在 waof/src/log.rs:CommitPipelineState 单点），无第三份拷贝可删。拒绝原因与证据见 task/reject/waof-sublog-commit-reset-dedup.md。

## 对标 C#

- libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:Reset
  同步复位位点（CommittedUntilAddress / CommittedBeginAddress / cachedSafeTailAddress /
  beginAddress / commitNum），allocator.Reset() 内部持锁；注释 WARNING: Run after
  database is quiesced。
- libs/server/AOF/GarnetLog.cs:Reset → SingleLog.cs:Reset / ShardedLog.cs:Reset
  路由链；上层调用点 DatabaseManagerBase.cs:ResetDatabase（FLUSHDB 管理面）。

## 现状问题（rust 侧）

wedb/wnode/src/aof/waof_sublog.rs:WaofSublog::reset 手抄
begin_address / tail_address / flushed_until_address / committed_until_address 四 store +
inflight_slots 清空 + commit_event notify，无 commit_lock、无 sync_data。
与权威实现 wedb/waof/src/log.rs:WalLog::reset（持 commit_lock + sync_data + 复活窗口 warn）并存：
1. 重复实现，违一处定义。
2. 并发不安全：与在途 commit_to 刷盘竞争时位点撕裂（WalLog::reset 持锁，手抄版不持）。

## 改动点

意见原文给的手段是「WalLog 暴露同步 reset 变体，waof_sublog 转发」。拒绝该手段，
理由：Device::sync_data 为 compio async，同步变体必然引入 block_on（async 上下文嵌套
panic 风险）或第二套锁语义（commit_lock 是 async_lock::Mutex，同步无法阻塞获取），
与"一处定义"红线冲突。目标不变（复用 WalLog::reset 权威、删手抄四原子），手段改为
调用链 async 化——SublogBackend trait 已有 5 个 impl Future 方法先例
（scan_async / truncate_until_async / commit_flush_async / recover_async /
wait_for_commit_async），无 dyn（Sublog 为静态 enum 分发），整链无生产调用方，
波及面封闭。

1. wedb/wnode/src/aof/garnet_log.rs
   - SublogBackend::reset 删除，新增
     fn reset_async(&self) -> impl Future<Output = ()> + '_（无默认体，两后端各自实现）。
   - InMemorySublog：同步 reset 逻辑移入 async reset_async（纯内存原子复位，无 I/O）。
   - GarnetLog::reset 改 async reset_async（对标 C# GarnetLog.Reset，语义不变）。
2. wedb/wnode/src/aof/sublog.rs
   - Sublog::reset 删，新增 pub async fn reset_async 分发；
     impl SublogBackend for Sublog 同步 reset 改 async reset_async match 分发。
3. wedb/wnode/src/aof/single_log.rs、sharded_log.rs
   - reset 改 async reset_async。
4. wedb/wnode/src/aof/waof_sublog.rs
   - WaofSublog::reset 手抄四原子整段删除，改 async reset_async：
     cookie 复位哨兵 + wal.reset().await（WalLog Deref 直达）。
5. 测试
   - wedb/waof/tests/wal/truncate_and_evict.rs 补并发回归：reset 与 commit_to 并发下
     位点一致性（持锁串行化、无撕裂），对标 TsavoriteLog.Reset 语义。
   - wedb_standalone/tests/aof_domain.rs 补 WaofSublog 链路 reset_async 冒烟
     （single_log_aof 装配 → reset → 位点归零 → 复写可扫）。

## 验收口径

1. ./clippy.sh 零警告（禁 allow）。
2. ./test.sh 全过。
3. bun ./js/check.js 无新增缺失（reset 实现仍存在，仅签名异步化，不需 ignore 登记）。
4. waof_sublog.rs 中不再出现对 wal.begin_address / tail_address / flushed_until_address /
   committed_until_address 的裸 store（reset 路径）。

## 验证结果

1. 分支 w1-sublog-commit-dedup 三个提交（收敛 / rustfmt / 注释校正），
   分支内 git merge dev 无冲突（基点即最新 dev@096d9da），主目录
   fast-forward 合并至 d8f0d74，worktree 与分支已清理。
2. ./clippy.sh：三任务（bench / regress / wedb）全部通过，-D warnings
   硬门禁零警告，无 allow。
3. ./test.sh：wedb 侧 nextest 1994 passed / 1 skipped，regress 侧回归门禁
   2 passed；复跑确认无 flaky（首轮 summary 中 1 flaky 为 whlog::hlog
   既有族偶发，复跑两轮均稳定全过）。
4. bun ./js/check.js：退出码 0，无输出，无新增缺失与重复。
5. 新增测试：
   - waof/tests/wal/truncate_and_evict.rs::test_reset_concurrent_with_commit_lock_serialization
     （后台线程 200 次 commit_to(旧尾) 与主循环 200 次 reset 真并发竞争提交锁，
     静默后四原子一致归零，复用扫描闭环）——passed。
   - wedb_standalone/tests/aof_domain.rs::garnet_log_reset_async_zeroes_wal_and_reusable
     （GarnetLog.reset_async → SingleLog → WaofSublog → WalLog::reset 路由，
     位点归零后复写可扫）——passed。
6. 验收口径第 4 条：waof_sublog.rs reset 路径已无任何对四原子的裸 store，
   仅剩 wal.reset().await 单点转发。
