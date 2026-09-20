# 副本重放任务死亡后 resync 起点错位:[applied, tail) 静默丢失

来源:next/zcode-r10-idem.md 问题 1(已随本票认领从 next 移除)。

## 问题(非幂等,数据级)

触发链:副本稳态 AOF 重放 → 重放链单错退出
(replica_replay_task.rs run_replay_loop:processor Err 即 warn 后 break,
任务永久退出;applied 位点冻结)→ 副本会话继续入队(本地尾前进)→
断连重连 PartialResync → INITIATE_REPLICA_SYNC 上报 aof_tail = 本地 wal 尾
(enqueued;assembly.rs)→ negotiate_resync 取 sync_start = min(rep_tail,
committed),全程不消费 applied 位点 → 新重放任务从 rep_tail 起扫
→ [applied, tail) 段只存在于本地日志、永不入存储 → 主从静默发散。
集合域 LPUSH/HINCRBY/ZINCRBY 等 input 重放型条目表现为缺效果。

死亡任务不能重生:replica_replay_driver.rs initialize_background_replay_task
的 background.is_some() 幂等启动闸在任务死后槽位不清(仅 dispose 清),
合法重入被闸吞。闸的影子:背景重放启动闸同时挡死死后重生。

C# 对位:ReplicaSyncSession.cs:ValidateMetadata 与
ReplicaReplayDriver.cs:InitializeBackgroundReplayTask 同形(1:1 继承);
task/done/replica-offset-semantics.md 只登记 enqueued 超前半边,
死任务 resync 跳窗半边未登记。

## 修法(二选一)

1. resync 重放起点取 min(rep_tail, applied 位点),并对本地日志
   safe_initialize 丢弃未应用尾段(重放从头来);
2. 背景任务 processor 出错即置 fatal_disconnect 断流,逼 FullResync
   (不依赖位点协商)。

两案任一;另须处理启动闸:任务死后槽位清理或闸随任务存亡。

## 验收

1. cargo check -p wedb -p wnode 零 error 零 warning。
2. 定向测试:注入重放链单错(损坏条目)→ 任务死 → 重连 →
   断言 [applied, tail) 段被重放(案1)或触发全量(案2),主从不发散。

## 门禁

只跑 cargo check(-p 收窄)与定向测试。严禁 ./test.sh 与 ./sh/clippy.sh。
