终态：合入 15fbaee5，cluster_publish_ns_caller_gate.rs 三用例改测试内自带 start_acl_node（nopass ACL 档）+ assign_local_slots 全槽 Stable 指派（单锁复用，wire_pair 同款），worktree 实跑三用例全绿零 warning，工作区 wedb_test 半成品 diff 已按选型还原。

甄别结论：通过（P3 测试装配）。选型：票面方案 a（仅本测试文件内自带装配补 ACL 注入与全槽指派），还原工作区 wedb_test 半成品 diff。论证：1) 现码复跑成立——HEAD 三用例（:193/:240/:296-299）裸 start_node 无 with_acl，绿灯先例 cluster_flushall_ns_caller_gate.rs:76 在册；2) C# 锚路径一级笔误，实际为 garnet/test/cluster/Garnet.test.cluster/ClusterPubSubForwardTests.cs（:97 500ms 锚实在），TestUtils.cs:548-556 useAcl 显式启用认证器先例成立；3) 甄别追加第二装配缺陷——SSUBSCRIBE 双侧 RespCommandsInfo.json 同口径持 key spec（channel 提键参与槽校验，slot_verify.rs:43 port==0 回 CLUSTERDOWN），HEAD 装配无槽指派，仅补 ACL 用例 1/2 的 SSUBSCRIBE ack 断言仍红，须一并补全槽 Stable 指派（对齐 cluster_shard_sub_unsubscribe_on_slot_migration.rs wire_pair 同款）；4) 弃方案 b（公共 start_node 点亮 nopass ACL 档）：波及全部 start_node 调用方须逐个零回归评估，且与在册「测试自带起服 + with_acl」单形态构成双轨，其半成品配套仍需改本测试文件（槽指派），改动面严格大于 a。

cluster_publish_ns_caller_gate 三个用例装配漏 ACL 注入，ACL SETUSER 被 ERR ACL Authenticator is disabled 拒绝，集成门禁 fail-fast 截断后续 4292 用例

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 测试装配对 ACL 用例显式启用认证器：garnet/test/ClusterTests/ClusterPubSubForwardTests.cs 等集群 pubsub 判据经 TestUtils.CreateGarnetServer 带 aclAuth 配置起服，集群 ACL 门禁测试（如 FlushAll 安全面）均在启用 ACL authenticator 的服务端上执行，ACL SETUSER/AUTH 方可用。测试装配与服务端能力面一一对应，缺注入即测不了 ACL 判据。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wedb/wedb/tests/cluster_publish_ns_caller_gate.rs 系 cluster publish 门票（合入 2d85a41a）新增测试，装配复刻 cluster_pubsub_peer_shutdown.rs 的 start_node + set_pubsub 形态，未注入 ACL（无 with_acl 调用）。而同门在册绿灯 wedb/wedb/tests/cluster_flushall_ns_caller_gate.rs:76 有 .with_acl(Arc::new(AccessControlList::new("")?)) 先例，其 authed_session/mallory 判据同形态可跑。三用例（cluster_publish_denies_foreign_tenant_session / cluster_publish_ns0_frame_reaches_tenant_subscribers / cluster_publish_gate_keeps_peer_forwarding）第一步 seed_user 即 ACL SETUSER，服务端无 ACL authenticator 回 ERR ACL Authenticator is disabled，断言失败 abort（SIGABRT）。子代理沙箱纪律只跑 cargo check 未实跑测试，缺陷漏出到主代理集成门禁：./test.sh 1182/5474 后 fail-fast，4292 用例被截断未跑。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
无生产危害，纯测试装配缺陷。危害面为门禁瘫痪：三个用例恒红导致 ./test.sh fail-fast，遮蔽其后全部用例结果，集成门禁失去信号价值。

执行方案：
1. wedb/wedb/tests/cluster_publish_ns_caller_gate.rs 装配补 ACL 注入，对齐 cluster_flushall_ns_caller_gate.rs:76 先例（with_acl(AccessControlList)），三个用例的节点装配（含双节点 wire_pair 形态两处）全部补齐，其余测试逻辑不动。
2. 验证：wnode/wedb 测试目标下三用例实跑通过（不再 abort），静默窗/NOPERM/放行/转发四判据真实生效；无新 warning。

涉及代码：
rust 文件与函数：
wedb/wedb/tests/cluster_publish_ns_caller_gate.rs:seed_user（:128 断言位）
wedb/wedb/tests/cluster_publish_ns_caller_gate.rs:三用例装配段（:193/:240/:296-302 start_node 位）
wedb/wedb/tests/cluster_flushall_ns_caller_gate.rs:76（with_acl 先例锚）
csharp 文件与函数：
garnet/test/ClusterTests/ClusterPubSubForwardTests.cs（ACL 启用装配契约先例）
