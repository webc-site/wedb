拒绝 qcode10.dev-replica-read-session-test-red（票据前提过时：dev HEAD 该测试已绿）

取证基线：主仓 /Users/z/git/db/wedb，dev HEAD 306d32da。子代理 fork 后仅核验、未提交。

## 结论
该「确定性红」票按旧 HEAD 写成，与当下 dev 不符，无需改动。

## 证据
- 票面引用的 dev 提交 3e51888f「副本一致读消费链装配」在本仓不存在（历史已压扁为单 init 提交），
  失效行号 :480 已漂移至 :488（其上方新增了修复代码）。
- read_session_state_lifecycle 已带真正的 Option-1 修法：update_physical_sublog_max_sequence_number
  (0,1) 与 (1,1) 推进两个子日志前沿，使 mssn(0) < frontier(1)，跨子日志新鲜度判定跳过等待直接完成；
  断言为 .unwrap() + assert!(post_batch…)，成功契约被真实断言，未被放宽。
- 追踪 wait_for_sequence_number：前沿=0/mssn=0 时 0<0=false 阻塞 → Err(ConsistentReadTimeout)（即旧红）；
  前沿=1 使该分支静态不可达，确定性、无计时依赖。
- 门禁实测：cargo test -p wnode --lib aof::readconsistency::replica_read_session_context
  → 5 passed / 0 failed（连跑 3 次，各约 0.01s）。cargo check -p wnode --all-targets 零警告。
- C# 对位（已实读核验，非凭票）：garnet/libs/server/StoreWrapper.cs:903-904
  EnforceConsistentRead() => enforceConsistentRead && IsReplica()；enforceConsistentRead =
  EnableCluster && EnableAOF && MultiLogEnabled（:230）。生命周期测试取 Option 1 语义正确
  （Option 2 会掏空该测试）。

## 处置
不新增/不改代码。删除 next/qcode10.dev-replica-read-session-test-red.md。
清理其空 worktree /tmp/fork/replica-read-session-test-red（无提交）与同名分支。
连带利好：合并后统一 test.sh 门禁不再被该项确定性阻挡。
