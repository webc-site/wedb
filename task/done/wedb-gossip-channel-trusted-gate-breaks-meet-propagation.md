终态：合入 2af58fea。gossip_channel_trusted 拆 (merge 门, 置位门) 双门：merge 门可信性绑定发送信道（无凭证部署判对端源 ip 命中任一在册注册端点 ip，弃载荷节点在册性前提），恢复 C# TryMerge 传播语义；置位门（remote_node_id）维持 flushall 票原判据，租户 WITHMEET 自封路径不回退。附带收口 gossip_panic_repull 入站用例直驱形态基线红（补 PeerSource 回环对端回填）。七套关联测试 30 用例全绿。

甄别结论：通过。定级：产品级（集群扩建/副本入网断路）兼门禁阻断（fail-fast 截断在册用例）。甄别复验：C# 双侧锚亲验成立（NetworkClusterMeet :161 RunMeetTask 无条件登记；NetworkClusterGossip :383-427 gossipWithMeet||IsKnown 即 TryMerge，无在册性门）；rust 锚亲验成立（basic.rs:185-207 判据 (b) 以 get_endpoint_from_node_id 在册性为前提，None 即整帧拒）；dev HEAD（0df9b377）实跑复现红（SIGABRT exit 134，断言位 gossip_manager.rs:599）；todo/ing/reject 池无并案，done 池 flushall 前序票（4f936c5）即本票修复对象；判据未灭失。

gossip_channel_trusted 门以在册性拒未知节点，CLUSTER MEET 引导与三节点 gossip 传播语义破坏，在册测试 test_meet_from_replica_propagates_across_three_nodes 基线红

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# MEET 与 gossip 均无条件合并：NetworkClusterMeet（libs/cluster/Session/RespClusterBasicCommands.cs）收方把发起者登记入配置；NetworkClusterGossip（同文件 :383-427）TryMerge 载荷配置时未知节点条目直接合并——gossip 传播的意义即经可信对端认识新节点（C# 互信凭 TLS/口令层，协议层不重复设门）。三节点拓扑 A-B-C，C 经 B 的 gossip 载荷认识 A，属标准传播路径。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
task/done/wedb-cluster-flushall-ns-caller-gate-gossip-forgery（合入 4f936c5）引入 gossip_channel_trusted 硬门（wedb/src/server/cluster_session/basic.rs，置位与 merge 共同前置）：(a) 集群凭证身份比对；(b) 无凭证部署判「帧节点 id 在本机配置注册端点与对端源 ip 同源」。判据 (b) 以 get_endpoint_from_node_id 在册性为前提：收方册内无该节点（get_endpoint_from_node_id 得 None）即整帧拒绝 merge。实测 dev HEAD：cargo test -p wedb --test gossip_manager test_meet_from_replica_propagates_across_three_nodes SIGABRT 于 wedb/tests/gossip_manager.rs:599「replica 收方应合并 MEET 发起方」，日志确证 Rejected gossip from untrusted channel: node ...0f01/...0f02 user Some("default") ns 0 peer 127.0.0.1:xxxxx——发送信道本身是本机真实节点（default 用户、ns0、127.0.0.1 同源），仅因册内无该节点被拒。该票遗留风险声明「无凭证部署首次 MEET 帧两判据皆不中→拒绝，新节点入网须双向 MEET 或收端预注册」把 MEET 单向引导与 gossip 传播断路当必要代价，与 C# 契约及在册测试正面冲突，代价不可接受。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
产品级：CLUSTER MEET 单向引导（Redis/Garnet 标准操作）后新节点无法经 gossip 融入拓扑，三节点及更深传播链断；集群扩建、副本入网全受阻。门禁面：./test.sh fail-fast 于该在册用例，其后数千用例被截断。安全面须保住：flushall 票防的是「租户会话自称节点 id 的 WITHMEET 两帧自封」，修复不得回退该防伪造判据。

执行方案（方向，执行席现码论证定稿）：
1. 门判据对象校正：可信性应绑定「发送信道」而非「载荷/发起节点在册性」——经可信信道（凭证身份或发送端点与在册节点同源）到达的 gossip 载荷恢复 C# TryMerge 无条件合并语义（含未知节点条目）；发送信道自身不可信（租户会话）维持 deny-by-default。注意 flushall 票攻击面复验：租户 WITHMEET 自封路径在新判据下仍须被拒（发送信道是租户会话，非在册节点连接）。
2. remote_node_id 置位门维持原票判据（在册性或建链确立），仅 merge 面恢复传播语义；两者解耦论证写入票面。
3. doc/zh/db.md 3.5「新节点入网须双向 MEET 或收端预注册」残留面声明随修复订正。
4. 验证：cargo test -p wedb --test gossip_manager 全绿（含三节点传播）；cluster_flushall_ns_caller_gate 三用例回归绿（防伪造判据不回退）；新增传播面判据（若在册用例未覆盖「经可信信道认识未知节点」形态）。

涉及代码：
rust 文件与函数：
wedb/src/server/cluster_session/basic.rs:gossip_channel_trusted（4f936c5 引入门）
wedb/src/server/cluster_session/basic.rs:cluster_gossip_slow（merge 前置门调用位）
wedb/tests/gossip_manager.rs:test_meet_from_replica_propagates_across_three_nodes（:599 断言位）
wedb/tests/cluster_flushall_ns_caller_gate.rs（防伪造回归判据）
doc/zh/db.md 3.5（残留面声明订正位）
csharp 文件与函数：
garnet/libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterMeet（登记语义）
garnet/libs/cluster/Session/RespClusterBasicCommands.cs:383-427 NetworkClusterGossip（TryMerge 传播语义）
