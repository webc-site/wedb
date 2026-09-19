# CLUSTER ADVANCE_TIME 脉冲节流改读 AofTailWitnessFreqMs 配置（消除装配注释与实现漂移）

来源：next/aof-tail-witness-freq-config-wiring.md。基线：主仓 dev。

排队说明：本条要改 wedb/wedb/src/server/cluster_provider.rs 与 boot.rs 装配面，属复制/集群热区
（sibling 正在该域密集落地）。必须在 qland5（resp3/incr/acl/simd/hlen）合并进 dev、复制区稳定之后再开工，
避免与在途 cluster/replication 改动对撞。

## 问题（已核实成立）

配置字段 RuntimeServerOptions::aof_tail_witness_freq_ms（wconf/src/runtime_server_options.rs:19，默认 100）
已声明、已入 ServerConfigType::AofTailWitnessFreq 序列化（runtime_server_config.rs:586-587），
但全仓对其运行期唯一「读」出现在测试 garnet_server_config_tests.rs:26（断言默认值）。
也就是说该旋钮从未被真正的消费者读取。

而装配/节流链的注释明确声称它在被读：
- boot.rs:169「AofSyncTask 每轮实时读 AofTailWitnessFreqMs 的 CLUSTER ADVANCE_TIME 节流频率源」；
- cluster_provider.rs:144「如 AofSyncTask 脉冲节流读 AofTailWitnessFreqMs；装配期自…」；
- cluster_provider.rs:484-590 有完整脉冲节流判定链（:534 到期纯读判定、:587 真正发起处 CAS 消费窗口，
  对标 C# ReplicationManager.cs:251-256 判定到期不消费、发起处才消费）。

结论：节流链用的频率窗口并非取自 aof_tail_witness_freq_ms 配置（很可能是常量或另一来源），
注释承诺的「读配置」与实现不符（spec-drift 同型先例：spec-doc-drift-gcbarrier-ri-promote）。

## 修法（落地前先读 chain 实际取值源核实，若确已读配置则本条作废、写驳回到 task/reject/）

1. 定位 cluster_provider.rs 节流链当前频率窗口取值源（:484-590 附近 last-attempt 时间戳与阈值来自何处）。
2. 若为硬编码常量：改为经 RuntimeServerOptions/共享的 StorageSessionProvider 读 aof_tail_witness_freq_ms
   （boot.rs:170 已注明「与 StorageSessionProvider 共享同一…源」，即复用既有热更新单源，勿另立第二配置入口）。
   单位 ms 与既有判定阈值口径对齐。
3. 打通装配链：boot.rs 把该配置随 provider 注入，使 AofSyncTask 每轮实时读到最新值（热更新语义）。
4. 若核实链上某处其实已正确读该字段（则漂移仅在注释措辞），把注释与实现对齐并写驳回到 task/reject/。

## 边界与验收

- 只接既有单源读取口，不新增第二配置源、不改节流判定/CAS 消费语义（那是 done 票 ReplicationManager 对位成果）。
- 默认 100ms 下行为与现硬编码等值时零回归；补一条「改配置→节流窗口随之变化」方向的断言（能单测层证明读到配置即可）。
- 子代理仅在 fork worktree 开发，仅 cargo check（私有 CARGO_TARGET_DIR），禁 test.sh/clippy/fmt，禁碰主树，禁 git add -A。
- 报告附 git log dev..HEAD、rev-parse HEAD(40)、diff --name-only、REAL_EXIT；若驳回须附实际取值源 file:line。

## 处置：查重让路（重复）

命中在途 worktree /tmp/fork/dev5/aof-tail-witness（分支 dev5/aof-tail-witness，提交 dcd47924
「reject: aof-tail-witness」，改动面即本票），该会话已甄别并驳回，未改代码。

独立复核驳回属实，票面自判作废条款（修法第 4 步）触发：

- 运行期真实消费点：wedb/wedb/src/server/replication/aof_sync_task.rs:352
  `.get_milliseconds(ServerConfigType::AofTailWitnessFreq)`，send_advance_time_pulse
  每轮实时读（对标 C# AofSyncTask.cs SendAdvanceTimePulse 读 serverOptions.AofTailWitnessFreqMs）。
  票面按字段名 aof_tail_witness_freq_ms grep，漏掉经 ServerConfigType 槽位的间接读。
- 播种链一处定义：wconf/src/runtime_server_config.rs:587 ← runtime_server_options.rs:19（默认 100），
  boot.rs 经 set_runtime_config 注入共享 Arc，CONFIG SET 热更即时生效。
- cluster_provider.rs:520-593 ensure_replication 是另一条节流链
  （replication_reestablishment_timeout_secs），与 AofTailWitnessFreq 无关，票面误认。
- 注释与实现一致，无 spec-drift，无需改代码。
