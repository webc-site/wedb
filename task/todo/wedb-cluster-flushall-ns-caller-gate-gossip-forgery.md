审核结论：通过

判定要点：
1. 攻击链亲验成立，一处表述修正：WITHMEET 空载荷帧本身不置 remote_node_id（空载荷 other = None，wedb/wedb/src/server/cluster_session/basic.rs:182 与 :286-287 不进置位臂），但 with_meet = true 使 :212-218 回传本机真实配置字节；第二帧 CLUSTER GOSSIP <该字节> 转投即置位——is_known 含本机自身条目（wedb/wedb/src/server/cluster_config/mod.rs:87-97 写 workers[LOCAL_WORKER_ID].nodeid，:147-149 is_known 即 worker_by_node_id().is_some()）。两帧自封，无需自造任何字节格式，票面「转投集群内真实节点配置字节」路径准确。
2. 门三条件 wedb/wnode/src/resp/admin_commands.rs:338-344 亲验确认；remote_node_id 全仓唯一置位点 basic.rs:191，判据 with_meet || known（:185-192），无共享密钥、无端点一致性校验；现存集群互信凭据 auth_container（wedb/wedb/src/server/cluster_provider/assets.rs:69-92，gossip/复制/failover 五类出站握手共用）在收侧置位臂零消费。C# 对位 garnet/libs/cluster/Session/RespClusterBasicCommands.cs:408-419 同判据同置位，RemoteNodeId 唯一消费 EnsureReplication，无安全语义，票面对位引用成立。
3. 内层四门 wedb/wedb/src/server/cluster_session/replication.rs:289-328 逐门亲验：ns != 0（:307）、origin 非本机（:315）、本机 Primary（:317-320）、origin 在册且 epoch >= config_epoch（:323-325，strict_i64 收 i64::MAX 恒过）。攻击者选参全可满足。
4. ACL 定级复核：CLUSTER|GOSSIP 与 CLUSTER|FLUSHALL_NS 同类别 Admin+Dangerous+Slow+Garnet（wedb/wresp/RespCommandsInfo.json）。缺省 default 恒 +@all（wedb/wacl/src/access_control_list.rs:28-30）但属 ns0 不构成攻击者；攻击者限定为 ns != 0 且被显式授予管理位的租户会话（锁测 mallory 即该形态，wedb/wedb/tests/cluster_flushall_ns_caller_gate.rs:164-172，正是 doc 3.5 门要拦的对象）。危害面较「任意租户」收缩，门禁设计缺陷定性不变，定级高危（受控前提：需租户管理位）。
5. 信道面亲验：CLUSTER 子命令分派单入口（wedb/wnode/src/resp/resp_server_session/core.rs:1490-1500，租户会话同路径），gossip 走客户端 RESP 端口无独立总线（wedb/wedb/src/server/gossip/node_connection.rs:65 连对端客户端端口），租户会话可发 GOSSIP。
6. 架构基准成立：doc/zh/db.md 3.5（:310-312）语义为「gossip 建链确立的在册对端节点」，收帧自封是「自称值」非「建链确立值」，不兑现契约。
7. 非重复确认：issue/todo/reject 池无同判据票；done 池 gossip 相关票均为复制信道/egress TLS/boot 参数主题；r9-acl-auth-namespace-screening（task/done/r9-acl-auth-namespace-screening-20261001.md）判据为 ACL 裸名跨 ns screening，无交集。锁测仅裸发拒绝 + ns0 放行两判据，旁路未覆盖，票面判断准确。

优化执行方案（收口单机制化，覆盖原第 1 点）：
1. 置位臂信道身份核验为唯一收口：cluster_gossip_slow 的 try_merge 与 remote_node_id 置位共同前置「节点间身份」硬门，按序判定：(a) 本会话认证身份 == cluster_username（复用 auth_container 既有认证面，出站侧 node_connection.rs 已带该凭证，收侧补比对单点）；(b) 无凭证部署退 (b) 帧载荷节点 id 的注册端点（config.get_worker_from_node_id 可查）与会话对端地址同源（对端地址 server 侧 peer_addr 已可达，wnode/src/server.rs:1584 形态）。租户会话（两判据皆不中）GOSSIP 一律不置 remote_node_id 且不 merge（deny-by-default，顺带收窄假配置 merge 污染面）。注意 (b) 残留面：同机明文部署（127.0.0.1 源）端点同源恒真，文档声明该形态须配集群凭证。原「总线端口」支线不适用（本仓无独立总线端口），TLS 对端证书不作主判据（需 mTLS 证书绑定节点 id，改动面大）。
2. FLUSHALL_NS 门三条件不动，单点收口在第 1 点，不建第二套白名单；origin 可达性核验可选。
3. 锁测扩展（wedb/wedb/tests/cluster_flushall_ns_caller_gate.rs，票面原路径少一层）：租户会话 WITHMEET 拿字节后转投再发 FLUSHALL_NS，断言仍拒；真实 gossip 建链（NodeConnection 形态）会话发帧，断言放行；无凭证部署下 WITHMEET 转投，断言不置位不 merge。

以下为原票面。

CLUSTER FLUSHALL_NS 调用方身份门可被任意租户会话借 CLUSTER GOSSIP 自封对端节点穿透（跨租户清库）

问题分析：
1. 契约对齐（自研面，对账基准 doc/zh/db.md 3.5 与 doc/zh/db.md 4.5）
   db.md 3.5 明文：多租户集群总线收令帧（CLUSTER FLUSHALL_NS）仅节点间连接（gossip 建链确立的在册对端节点）或 namespace == 0 的会话可发起，其余已认证租户会话一律按权限错误拒绝。db.md 4.5：协调者经集群 gossip 连接向全网其余活跃主节点扇出，各主节点收帧先过同步守卫。C# NetworkClusterGossip（RespClusterBasicCommands.cs:417-419）同样记 RemoteNodeId 为收帧节点 id，但 C# 该字段唯一消费是 EnsureReplication 防假重同步，无安全语义——本仓把它当安全门是自研面新增语义，须自证其不可伪造，现状不可证。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   门判据 wedb/wnode/src/resp/admin_commands.rs:338-340 仅三条件：cmd == ClusterFlushallNs && self.namespace != 0 && cluster.remote_node_id().is_none()。而 remote_node_id 的置位 wedb/wedb/src/server/cluster_session/basic.rs:185-195：cluster_gossip_slow 收到 GOSSIP 帧，with_meet（CLUSTER GOSSIP WITHMEET 空载荷即可）或载荷中节点已 known（收敛集群内任一真实节点配置字节都 known）即 *remote_node_id.write() = Some(id)，对该帧无任何节点身份认证（无共享密钥、无来源端点核验、无 TLS 客户端证书绑定节点 id）。任意已认证租户会话两步自封：先 WITHMEET 空帧或转投集群内真实节点配置字节拿 remote_node_id，再发 CLUSTER FLUSHALL_NS <受害者ns> <已知节点id> i64::MAX。内层四门（replication.rs:301-325）全可被攻击者选参满足：ns 非零、origin 非本机（:315）、origin 在册（:323）、epoch >= origin_worker.config_epoch（:324，传 i64::MAX 恒过）。
3. 逻辑危害确证
   跨租户清库：攻击者租户会话逐节点重复上述两步，受害租户数据在全网主节点被 flush_namespace 换号清空并经 AOF FlushNs 复制到各副本，无需任何伪造比特。锁测 wedb/tests/cluster_flushall_ns_caller_gate.rs 只锁「未 gossip 裸会话被拒」顺路径，未覆盖此旁路。利用前提仅攻击者 ACL 可达 CLUSTER 命令族（缺省 +@all 用户恒可；门测试用例本身证明租户管理位用户正是该门要拦的对象）。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/admin_commands.rs:network_process_cluster_command
wedb/wedb/src/server/cluster_session/basic.rs:cluster_gossip_slow
wedb/wedb/src/server/cluster_session/replication.rs:network_cluster_flushall_ns

对应 c# 文件与函数：
garnet/libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterGossip（RemoteNodeId 无安全语义的对位证据）

精炼执行方案：
1. 对端节点身份改为由连接信道确立而非收帧自封：CLUSTER 总线端口/TLS 对端证书或 gossip 建链（MEET 握手双向确认后的注册表项）承载节点身份，cluster_gossip_slow 置 remote_node_id 前核验帧来源端点与该节点注册端点一致（或至少要求帧载荷节点 id 的注册端点与本会话对端地址同源），非总线信道上的租户会话 GOSSIP 一律不置 remote_node_id。
2. FLUSHALL_NS 门保持三条件不动，新增 origin 可达性核验无害收紧可选；核心收口在第 1 点单机制，不建第二套白名单。
3. 测试验证点：锁测补旁路用例——租户会话 WITHMEET 后再发 FLUSHALL_NS，断言仍被拒；真集群总线会话（gossip 建链）发帧，断言放行。既有 cluster_flushall_ns_caller_gate.rs 扩展。
