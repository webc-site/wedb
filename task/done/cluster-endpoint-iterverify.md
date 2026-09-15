# 集群会话小修批：端点偏好配置接线 + 迭代式槽位校验落地（next/ds.net.md 条 7 + 迭代槽位校验条）

## 甄别结论

两条全部成立，一处按 rust 架构修正执行口径。

### 1. MOVED/ASK 端点偏好硬编码 Ip：成立

C# 消费面全部经 clusterProvider.serverOptions.ClusterPreferredEndpointType：

1. RespClusterSlotVerify.cs:23 Redirect（MOVED 重定向）
2. RespClusterSlotVerify.cs:53/60 GetSlotVerificationMessage（MOVED/ASK 错误构造）
3. RespClusterBasicCommands.cs:345 CLUSTER SHARDS（GetShardsInfo）
4. RespClusterSlotManagementCommands.cs:350/622 CLUSTER SLOTS（GetSlotsInfo）
5. 枚举 libs/server/Cluster/ClusterPreferredEndpointType.cs（Ip=0/Hostname=1/Unknown=2，Description ip/hostname/unknown，默认 Ip）
6. 选项 libs/host/Configuration/Options.cs:60 cluster-preferred-endpoint-type

rust 现状：ClusterPreferredEndpointType 枚举已建（wedb/wedb/src/server/cluster/
cluster_preferred_endpoint_type.rs，Default=Ip）；cluster_config.rs
get_endpoint_by_preferred_type 三分支已实现；缺口是配置面与读取面：
ClusterSession 四处硬编码 Ip（redirect_slot、network_multi_key_slot_verify
两处、ClusterSlots 臂、ClusterShards 臂），全仓无 --cluster-preferred-endpoint-type
选项。ClusterProvider 已有原子注入选项模式（cluster_node_timeout_ms 等），
照此加 preferred_endpoint_type 原子面。

### 2. 迭代式槽位校验慢路径未落地：成立

C# 链路闭环：

1. RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify：会话级缓存
   （cachedVerificationResult + configSnapshot + initialized），首键初始化缓存；
   后续键 slot 不一致 → CROSSSLOT，状态类别变化 → TRYAGAIN，一致透传
2. TxnKeyManager.cs:VerifyKeyOwnership：事务域逐键入口（!clusterEnabled ||
   IsReplaying 短路，失败置 state=Aborted）；ResetCacheSlotVerificationResult /
   WriteCachedSlotVerificationMessage 配套（事务运行前后）
3. 消费场景：CustomTransactionProcedure.AddKey（Prepare 段逐键
   SaveKeyEntryToLock + VerifyKeyOwnership）；TransactionManager.
   RunTransactionProcInternal 开头 reset、prepare 后 Aborted 写缓存错误消息

rust 现状：single_key/multi_key 校验状态机齐备（slot_verify.rs），批量门评
（evaluate_multi_key_gate + 挂起等待体）齐备；缺迭代缓存态与会话级迭代入口，
CustomTransactionProcedure::add_key 只做 save_key_entry_to_lock 无所有权校验，
server.yml 豁免三组（IClusterSession.NetworkIterativeSlotVerify 等、
RespClusterIterativeSlotVerify.cs 全组、TxnKeyManager.cs 三函数）。

口径修正（迭代链宿主承接形态）：C# TxnKeyManager 是 TransactionManager 的
partial，持 respSession 直达 clusterSession；rust wtxn 是纯事务层不持集群
引用（单机解耦既有豁免声明维持）。投影为：wtxn TransactionManager 增
TxnSlotVerifyFace 切面槽（Option，单机 None 剥离，同 clusterEnabled 判定），
wnode 会话侧在 RUNTXP 入口注入切面（对标 C# TryTransactionProc 在
RespServerSession 上的形态），逐键校验落 wcustom add_key（对标 C# AddKey）。
迭代缓存态落 wedb::ClusterSession（对标 C# partial 成员位置），
wnode::ClusterSessionFace 增三方法（默认放行，单机 stub 免改）。

CanOperateOnKey 自旋投影：事务 Prepare 是同步上下文不可挂起重评；同步段用
thread::yield 让出重试（对标 C# Thread.Yield 自旋），超时上限
cluster_node_timeout_ms，超时按 NotOperable 终评（MIGRATING → ASK）。
磁盘候选键（同步探测 None、异步裁决不可达）按超时终评降级 ASK，登记差异
（C# Exists 同步等 IO 完成即时裁决）。

configSnapshot 不 clone 投影：C# 快照为引用语义防事务期间 gossip 换配置；
rust ClusterConfig clone 256KB 级不可取，逐键取 current_config 读锁
（毫秒级窗口，RwLock 写阻塞保证无撕裂），缓存只存 state 类别与 slot，
差异登记。

## 对标与改动点

1. wedb/wedb/src/server/cluster/cluster_preferred_endpoint_type.rs
   repr(u8) 显式判别 Ip=0/Hostname=1/Unknown=2 对齐 C#，clap ValueEnum derive
2. wedb/wedb/src/args.rs ClusterArgs 增 cluster_preferred_endpoint_type
   （--cluster-preferred-endpoint-type，默认 ip；集群扩展参数命令行面既有格局）
3. wedb/wedb/src/server/cluster_provider.rs 增 preferred_endpoint_type
   AtomicU8 + set/get（对标 serverOptions.ClusterPreferredEndpointType 读取面）
4. wedb/wedb/src/main.rs 装配期注入（set_cluster_node_timeout_ms 同区）
5. wedb/wedb/src/server/cluster_session.rs 四处硬编码 Ip 改取 provider 配置；
   增 iterative_slot_verify 缓存字段 + 三方法（reset /
   network_iterative_slot_verify / write_cached_slot_verification_message）
6. wedb/wedb/src/server/slot_verify.rs 增迭代缓存态与步进函数 +
   ClusterSlotVerificationState 状态类别比较
7. wedb/wedb/src/server/cluster_manager.rs 增迭代单键门评
   evaluate_iterative_key_gate（同步自旋 CanOperateOnKey 投影）
8. wnode/src/cluster_session.rs ClusterSessionFace 增三方法（默认放行）+ vtable
9. wtxn 增 TxnSlotVerifyFace + TransactionManager 槽位（set_slot_verifier /
   verify_key_ownership）；run_transaction_proc 开头 reset、Aborted 写缓存错误
10. wcustom add_key 增 verify_key_ownership（对标 C# AddKey 调用序）
11. wnode/src/txn_resp_commands.rs try_transaction_proc 注入切面
    （集群切面在场时，session_asking 绑定）
12. js/check/ignore/server.yml 删三组既有豁免（迭代落地）
13. 测试：hostname 重定向（redirect_slot hostname 形态）、迭代校验跨槽
    （CROSSSLOT/TRYAGAIN/MOVED 缓存消息写出）、选项解析默认值

## 验收口径

1. ./clippy.sh 零警告（禁 allow）
2. ./test.sh 全过
3. bun ./js/check.js 无新增缺失/重复；server.yml 三组迭代豁免消除
4. 全仓 grep 确认 ClusterPreferredEndpointType::Ip 生产硬编码清零（测试显式构造除外）

## 验证结果

实现落地：

1. wedb/wedb/src/server/cluster/cluster_preferred_endpoint_type.rs
   repr(u8) 显式判别 Ip=0/Hostname=1/Unknown=2 对齐 C# 声明序，clap
   ValueEnum + strum Display derive（命令行取值 ip/hostname/unknown）。
2. wedb/wedb/src/args.rs ClusterArgs 增 cluster_preferred_endpoint_type
   （--cluster-preferred-endpoint-type，default_value "ip" 对齐 C# enum
   首成员默认；default_value_t 走 Display 输出 "Ip" 与值表不符故用字面量）。
3. wedb/wedb/src/server/cluster_provider.rs 增 preferred_endpoint_type
   AtomicU8 + set/get（判别值 match 读出，对标 serverOptions 读取面）。
4. wedb/wedb/src/main.rs 装配期 set_preferred_endpoint_type 注入。
5. wedb/wedb/src/server/cluster_session.rs 四处硬编码 Ip 清零（redirect_slot、
   network_multi_key_slot_verify 两处、ClusterSlots 臂、ClusterShards 臂改取
   provider 配置）；增 iterative_slot_verify 缓存字段 + 三固有方法。
6. wedb/wedb/src/server/slot_verify.rs 增 SlotVerifyKind 状态类别枚举、
   kind()、IterativeSlotVerifyCache 缓存态与 iterative_slot_verify_step
   步进函数（首键初始化、跨槽 CROSSSLOT、状态漂移 TRYAGAIN、首错短路）。
7. wedb/wedb/src/server/cluster_manager.rs 增 evaluate_iterative_key_gate
   （CanOperateOnKey 自旋内联为 thread::yield 让出重试，超时按 NotOperable
   终评）与 iterative_slot_verify 整批步进。
8. wnode/src/cluster_session.rs ClusterSessionFace 增三方法（默认放行，
   单机 stub 免改）+ vtable 三指针 + 擦除盒三方法。
9. wtxn 新文件 txn_slot_verify.rs：TxnSlotVerifyFace 切面；TransactionManager
   增 slot_verifier 槽（None = 单机剥离，同 clusterEnabled 判定）+
   set_slot_verifier + verify_key_ownership（失败置 Aborted）；
   run_transaction_proc 开头 reset、prepare 后 Aborted 写缓存错误
   （对标 RunTransactionProcInternal 两接线点）。
10. wcustom add_key 增 verify_key_ownership（对标 C# AddKey 调用序；
    SetTxnProc::prepare 改经 add_key，原直调 save_key_entry_to_lock 绕过校验）。
11. wnode/src/txn_resp_commands.rs RUNTXP 执行前注入 IterativeSlotVerifyAdapter
    （集群切面 + session_asking 绑定，对标 C# txnManager 持 respSession 形态）。
12. js/check/ignore/server.yml 删三组豁免：TxnKeyManager.cs 三函数、
    IClusterSession.cs 的 NetworkIterativeSlotVerify 族、
    RespClusterIterativeSlotVerify.cs 整组。

测试（wedb/wedb/tests/cluster_iterative_slot_verify.rs 四用例 + args 选项解析）：

- iterative_step_state_machine：步进状态机全分支（初始化/同槽透传/TRYAGAIN/
  首错短路/CROSSSLOT 缓存槽位保持/reset）
- cluster_session_iterative_verify_and_cached_message：会话级三方法 + 缓存
  错误写出（Ip 与 hostname 两形态）+ 首错短路
- moved_redirect_prefers_hostname：会话数据命令 MOVED 通告 hostname；
  hostname 缺失回退 "?"
- runtxp_cross_slot_transaction_aborts_with_crossslot：RUNTXP 端到端，异槽
  键对第二键 CROSSSLOT 中止、同槽键对（hash tag）真执行、中止后消费链完好

甄别修正：迭代校验链宿主承接形态——C# TxnKeyManager 是 TransactionManager
partial 持 respSession 直达 clusterSession；rust wtxn 为纯事务层，投影为
wtxn 切面槽 + wnode 会话侧 RUNTXP 注入 + wcustom add_key 调用，单机 None
剥离（既有单机解耦豁免声明维持）。configSnapshot 不 clone 投影（ClusterConfig
整体克隆过重，逐键取读锁，毫秒级 Prepare 窗口 RwLock 写阻塞保证无撕裂）。
CanOperateOnKey 自旋内联为同步让出重试；磁盘候选键沿超时终评降级 ASK
（C# Exists 同步等 IO 即时裁决，差异登记）。

验收：

- ./clippy.sh 口径四 crate（wtxn/wcustom/wnode/wedb）零警告（无 allow）；
  顺修 merge 带入的 dev 侧 handler.rs while_let_loop 一处
- ./test.sh 全过：wedb 工作区 2061 项 + regress 2 项（wkv
  test_concurrent_sessions_upsert_read 曾瞬时失败，单包复跑与全量复跑均过，
  定性为共享 target 并发互踩非代码问题）
- bun ./js/check.js 无输出（0 缺失 0 重复）；server.yml 三组迭代豁免消除
- 全仓 grep 确认 ClusterPreferredEndpointType::Ip 生产硬编码清零、探针残留清零

合并：分支 w5-clsmall 3 commit（实现 + merge dev + scratch 消费面适配），
先 merge dev（带进 w5-consume 的消费面 API 变更，测试基座同步适配）后合入
主目录 dev，worktree 与分支已清理。
