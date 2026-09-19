CLUSTER RESET HARD 清空 ban list 属无对位追加：C# TryReset 全程不触 workerBanList

来源：glm.net 第 5 条（分拣判定成立）。取证基线：主仓 HEAD 1b944517，行号为当下实况。

现状
- rust：/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_manager_worker_state.rs:117-119
  try_reset 在 CAS 环之外、flush_config 之前追加
  `if !soft { self.worker_ban_list.write().clear(); }`。
  该函数其余段逐行对标 C#（:55-57 SuspendConfigMerge、:60-63 ResetRecovery 对 C# :106、
  :64-83 键检查对 :111-116、:77-79 CloseAll 对 :118、:84-115 取代 CAS 重试的锁形态说明），
  唯 ban list 分支无任何对位说明，也无刻意差异声明。
- C#：/Users/z/git/db/wedb/garnet/libs/cluster/Server/ClusterManagerWorkerState.cs:99-146 TryReset
  不触碰 workerBanList。全仓该字典只有三类访问：
  AddOrUpdate 写封禁（同文件 :80，CLUSTER FORGET 路径）、过期清理 TryRemove
  （/Users/z/git/db/wedb/garnet/libs/cluster/Server/Gossip/Gossip.cs:421-435）、
  门禁 ContainsKey（Gossip.cs:122 接收侧、Gossip.cs:394 建连侧、
  /Users/z/git/db/wedb/garnet/libs/cluster/Server/ClusterConfig.cs:1111 Merge 内防并回）。
  无 Clear 调用点。
- 后果：CLUSTER FORGET 写入的 60s 封禁被 RESET HARD 提前解除。被忘节点若仍在线，
  其 gossip 立即能经 :132 TryMerge 重新并入本节点视图（rust 对位
  /Users/z/git/db/wedb/wedb/wedb/src/server/gossip/gossip_manager.rs 的 merge 门禁读同一张表），
  「FORGET 后 HARD 复位」的收敛行为与参考实现分叉。

修法（默认采按 C# 原样对齐）
1. 删 cluster_manager_worker_state.rs:117-119 的 clear 分支，封禁保持自然过期（Gossip 侧过期清理
   链已在位），RESET 只换配置与关连接（对标 C# :103/:106/:112/:118/:139 五步）。
2. 若复核判定「HARD 复位 = 本地一切集群状态归零」是本仓刻意语义，则保留分支但必须在函数文档
   点名该差异并补一条「RESET 后 ban 窗口内不 merge」的反向用例锁定行为；两案择一，
   不留静默分叉（默认走 1，1:1 对标优先）。
3. 顺带核对 rust 侧 FORGET 写封禁与过期清理的时长口径与 C# 一致
   （ClusterManagerWorkerState.cs:80 expirySeconds 默认 60，rust 同函数 try_reset 邻近的 forget 路径）。

优先级
功能缺口（对外命令行为偏离参考实现，影响集群拓扑收敛与 FORGET 语义）。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/cluster/Server/ClusterManagerWorkerState.cs:99-146
- /Users/z/git/db/wedb/garnet/libs/cluster/Server/Gossip/Gossip.cs:122、:394、:421-435
- /Users/z/git/db/wedb/garnet/libs/cluster/Server/ClusterConfig.cs:1100-1111（Merge 内 ban 门禁）

协调
- cluster-hostname-announcement-chain、cs-anchor-dup-single-mount 等在册票不触 try_reset，
  本票改动仅一分支 + 注释。
- 与 cluster-suspend-await-lock 票（suspend_config_merge 锁形态）同函数域，若其先落地须在其
  合并后的位点重取行号。

验收
- CLUSTER FORGET n1 → CLUSTER RESET HARD → n1 仍在线：断言 worker_ban_list 仍含 n1、
  其 gossip 到达时 merge 被拒，封禁到期后方可并回。
- soft 复位路径行为不变；RESET 的其余五步（挂起合并/复位恢复/键检查/关连接/换配置/落盘）逐字节不变。

细化方案（甄别后，采修法 1）
- 甄别核实（基线 dev，try_reset 当前实况与票据行号一致）：
  1. C# TryReset（ClusterManagerWorkerState.cs:99-146）函数体仅六步：
     SuspendConfigMerge / ResetRecovery / GetSlotList / HasKeysInSlots 拒否 /
     CloseAll / 换配置 CAS / FlushConfig，全程不触 workerBanList；
     函数内计算的 expiry 局部变量亦未被 InitializeLocalWorker 消费（C# 死值）。
  2. C# 全仓 workerBanList 仅四类访问：AddOrUpdate 写封禁
     （ClusterManagerWorkerState.cs:80，TryRemoveWorker 即 FORGET 路径）、
     ContainsKey 门禁（Gossip.cs:122 接收侧 TryMerge、:394 建连侧、
     ClusterConfig.cs:1111 Merge 内防并回）、TryRemove 过期清理
     （Gossip.cs:421-435 DisposeBannedWorkerConnectionsAsync）。无 Clear。
  3. rust 侧对位链路完整在位：ban_node（cluster_manager.rs:600，
     FORGET 调用 cluster_manager_worker_state.rs:37）、过期清理
     cleanup_ban_list（cluster_manager.rs:620，gossip_manager.rs:230 周期
     调用后对未过期节点断连，对齐 C# DisposeBannedWorkerConnectionsAsync
     双臂）、门禁 is_banned + merge 内 contains_key
     （cluster_manager.rs:587/655、cluster_config/mod.rs:859）。
  4. 时长口径一致：双侧 FORGET 默认 60 秒
     （C# RespClusterBasicCommands.cs:67 expirySeconds = 60；rust
     basic.rs:450 expiry_seconds: i64 = 60），存值均为 now+expiry 绝对秒。
  5. 无刻意差异声明：rust 侧 clear 分支无任何注释说明，修法 2 的
     「本仓刻意语义」不成立，按 1:1 对标优先删分支。
- 改动（仅一处）：删 wedb/wedb/src/server/cluster_manager_worker_state.rs
  try_reset 内 flush_config 之前的 `if !soft { self.worker_ban_list.write().clear(); }`
  三行。RESET HARD 后封禁保持自然过期，被忘节点 gossip 依门禁拒绝并回。
- 不改项：soft 复位路径、RESET 其余六步、_expiry_seconds 形参保留
  （对齐 C# 签名，C# 同名形参亦未消费）、FORGET 写封禁与过期清理链、
  is_banned 过期即放行语义（C# ContainsKey 惰性清理前的窗口差异不属本票，
  不顺带扩面）。
- 现有测试核查：tests/cluster_management.rs:60/:218/:100/:104 均未断言
  reset 清 ban 行为，删分支无测试回归。
