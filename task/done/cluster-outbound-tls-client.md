集群出站连接 TLS 全缺位：五类节点间连接在 C# 透传 TlsClientOptions，rust 侧零 TLS 面

来源：glm.net 第 4 条（分拣判定成立）。取证基线：主仓 HEAD 1b944517，行号为当下实况。

现状
- 唯一底层出站通道：/Users/z/git/db/wedb/wedb/wconn/src/client.rs:83-84
  `pub async fn connect_async(&mut self) { let stream = TcpStream::connect(&self.end_point).await?; ... }`
  ——无 TLS 包裹；构造口同文件 :46-67 `new(endpoint, auth_username, auth_password, client_name,
  max_outstanding_tasks, timeout_millis, record_latency)` 无 TLS 入参；wconn 全 crate grep tls 零命中，
  /Users/z/git/db/wedb/wedb/wconn/Cargo.toml:12-13 `[features] default = []` 无 tls 特性。
- 集群侧包装层：/Users/z/git/db/wedb/wedb/wedb/src/client.rs:91-105 connect_async 构造 ConnClient 时
  同样无 TLS 配置位。
- 入站面已有 TLS：/Users/z/git/db/wedb/wedb/wnode/src/tls/（config.rs、mod.rs），
  经 wnode/Cargo.toml:16 `tls = ["dep:compio-tls","dep:rustls-pki-types","dep:rustls-pemfile"]`
  启用（compio-tls 0.10 带 rustls+ring 特性，同文件 :23）——即本仓已有 rustls 栈，只缺客户端方向。
- 四类现役出站消费点全部明文（同一条 connect 通道）：
  /Users/z/git/db/wedb/wedb/wedb/src/server/gossip/node_connection.rs（gossip 节点连接，
  集群全量配置经此传输）、
  /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_sync_session.rs（AOF 增量推流 /
  副本同步，用户数据）、
  /Users/z/git/db/wedb/wedb/wedb/src/server/migration/migrate_driver/keys.rs:244 connect_migrate_client
  （迁移流，用户数据）、
  /Users/z/git/db/wedb/wedb/wedb/src/server/failover/primary_failover_session.rs:77 与
  /Users/z/git/db/wedb/wedb/wedb/src/server/failover/replica_failover_session.rs:77（failover 控制流）。
- C# 五类连接构造期一律透传客户端 TLS 选项：
  /Users/z/git/db/wedb/garnet/libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:180 与 :195
  （`new GarnetServerNode(..., tlsOptions?.TlsClientOptions, ...)`）、
  /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:138、
  /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:112、
  /Users/z/git/db/wedb/garnet/libs/cluster/Server/Migration/MigrateSession.cs:172、
  /Users/z/git/db/wedb/garnet/libs/cluster/Server/Failover/FailoverSession.cs:75 与 :80。
- 该缺口未在 ignore 登记：/Users/z/git/db/wedb/js/check/ignore/cluster.yml 只登记 TLS 测试工程
  （:56、:147、:150），无「集群客户端 TLS 不实现」条目；SKILL.md:12-14 的豁免清单只有
  向下兼容/微软 AAD 认证/动态模块加载三件，不含 TLS。

后果
开启 TLS 的部署里，服务端入站加密但节点间全部流量（gossip 全量集群配置、AOF 复制流用户数据、
迁移流、failover 控制流）明文传输，与本仓要对标的安全语义相反；`--tls-*` 集群态配置形同虚设。

修法
1. wconn 加 `tls` 特性（复用 wnode 既有栈：compio-tls 的 rustls 后端，禁引入第二套 TLS/pem 解析栈；
   依赖按仓规只经 `cargo add` 增补，不手改 Cargo.toml）。
2. wconn::GarnetClient 构造面加 TLS 配置入参（Option 形态，对标 C# GarnetClient 构造的
   `GarnetTlsOptions? tlsOptions` 位置），connect_async 内按该配置在 :84 建连点包裹 TLS 流，
   证书/CA/主机名校验取自配置；无配置分支逐字节保持现状（Option::None = 今日行为）。
3. 集群包装层 /Users/z/git/db/wedb/wedb/wedb/src/client.rs 的构造族（:30 new / :33 with_endpoint /
   :38 with_auth 等）透传同一配置，:95 构造 ConnClient 处单点注入。
4. 五类消费点的配置来源单点化：由 ClusterProvider 暴露一个只读 TLS 客户端配置访问口
   （装配期自 wconf 的 TLS 配置投影，形态对标同文件 :915/:920 runtime_config 注入口），
   gossip / replication / migration / failover 四处各自取用，禁在四个消费点重复读 server options。
5. 落地后在 /Users/z/git/db/wedb/js/check/ignore 相关块保持「已实现」口径（不新增豁免），
   并同步 cluster.yml 中 TLS 测试工程的登记（现登记为不转写的测试，若新增 e2e 则按实际改注释）。

优先级
功能缺口（安全语义缺位，TLS 集群部署下节点间数据明文；非美学、非微软绑定项）。

边界
- 不做证书热轮换、不做 C# 侧 ServerCertificateSelector 等托管形态
  （对应 ignore 已登记 /Users/z/git/db/wedb/js/check/ignore/server.yml:513-516）。
- 不改动入站 TLS 面与 RESP 协议栈；不做与 TLS 无关的连接池重构。

验收
- TLS 集群 e2e 四路（gossip 握手、diskbased/diskless 全量同步、MIGRATE、failover 编排）
  在启用 TLS 的两节点环境跑通，且明文客户端连集群端口被拒/被断。
- 未启用 TLS 的部署回归零变化（含现有 replication/cluster 测试全绿）。
- grep 集群四类消费点无第四处自行读 TLS 配置（单点出口唯一）。

## 细化方案（甄别后追加，基线 05dc7ec9 复核属实）

甄别结论：成立。C# 五类连接点 TLS 透传复核命中（GarnetClusterConnectionStore.cs:195、
AofSyncTask.cs:138、ReplicaDiskbasedSync.cs:112、MigrateSession.cs:172、FailoverSession.cs:75/:80，
全部 `serverOptions.TlsOptions?.TlsClientOptions` 形态）；rust 侧 wconn 零 TLS 面、无 tls feature，
票据所述属实。C# 客户端 TLS 语义（GarnetTlsOptions.cs:GetSslClientAuthenticationOptions）：
- TargetHost = ClusterTlsClientTargetHost（SNI + 名字校验目标）
- ServerCertificateRequired=false → 远端证书校验恒真；true → 系统信任链或
  （链错误 + 名字匹配 + IssuerCertificatePath 签发者匹配）
- 客户端证书复用服务端证书选择器（同一 cert/key 做 mTLS 对等）
配置来源：Options.cs:307/:311/:340（cluster-tls-client-target-host、server-certificate-required、
issuer-certificate-path），defaults.conf:253 ServerCertificateRequired 默认 true。

### 改动清单

1. wconf/node_options.rs：+tls_client_target_host（Option<String>）、
   +tls_server_cert_required（bool，默认 true）、+tls_issuer_cert（Option<PathBuf>）；
   defaults 块与 over! 宏清单同步。
2. wconn 加 tls feature（cargo add 落 optional 依赖 + [features] 行）：
   compio-tls 0.10.0(rustls,ring)、rustls-pemfile 2.2.0、futures-util(bilock,io,unstable)、
   webpki-roots 0.26——与 wnode 入站同一套栈，不引入第二 TLS/pem 栈。
3. wconn/src/tls.rs：ClientTlsConfig（TlsConnector 包装）。
   - new(cert,key 可选成对, target_host, server_cert_required, issuer)：
     required=true → WebPkiServerVerifier（roots=issuer pem 或 webpki_roots）；
     false → dangerous 恒真（对标 C# 校验回调恒真臂）；cert+key 成对 → with_client_auth_cert
     （mTLS 对等），缺席 → with_no_client_auth；半配对报错（wnode run_node 同口径）。
   - connect(stream, endpoint)：server_name 取 target_host，空回落 endpoint host 段。
4. wconn/src/network/stream.rs：OutStream 枚举（Tcp | Tls）拆 ReadHalf/WriteHalf；
   TLS 臂 BiLock 互斥句柄对 + poll 内核（chunk 读 / 写尽+flush / shutdown close_notify），
   内核形态对齐 wnode/src/net/stream.rs 既有模式（crate 平级不互依，注释标对应关系）。
   UnexpectedEof 归一 Ok(0)=EOF（compio-tls read_futures 同口径）。
5. wconn/src/network/pump.rs：network_loop 签名 TcpStream→OutStream，双泵参数改
   ReadHalf/WriteHalf；读泵 stream.read / 写泵 write_all+shutdown 调用点换枚举方法；
   Tcp 臂零开销转发，行为逐字节不变。
6. wconn/src/client.rs：new 加 tls: Option<Arc<ClientTlsConfig>> 末参（对标 C# 构造
   tlsOptions 位）；connect_async 在 TcpStream::connect 后按配置包裹；None 分支保持现状。
   crate 内 5 处测试构造点补 None。
7. wedb/Cargo.toml：tls = ["wnode/tls", "wconn/tls"]。
8. wedb/src/client.rs facade：with_config 加 tls 末参存字段，connect_async 构造 ConnClient
   单点注入；new/with_endpoint/with_auth 便捷口保持无 TLS。
9. cluster_provider.rs：+cluster_tls_client RwLock<Option<Arc<ClientTlsConfig>>> +
   set/try 访问对（形态对标 :918 set_runtime_config 注入口）。
10. boot.rs：装配期从 node 的 tls 三字段 + tls_cert/tls_key 投影构造（集群 + 任一 TLS
    字段在位才建，否则 None=现状明文），set_cluster_tls_client 注入。
11. 五类消费点单点取用（grep 无第四处自行读配置）：
    - gossip/node_connection.rs:57 with_config 补 tls 参
    - replica_sync_session.rs:325 with_auth → with_config 全参
    - migrate keys.rs:240 connect_migrate_client(spec) → +tls 参（调用方 slots.rs:115/
      keys.rs:678 经 session.cluster_provider 取）
    - failover primary/replica 两文件 6 处 with_auth/with_endpoint 收敛基座 helper 后统一
      带 tls（C# FailoverSession 全参构造同口径）
12. ignore 不新增豁免：GarnetTlsOptions.cs 文件级 ignore（server.yml:519-521 rustls 原生
    承接口径）覆盖客户端面，落地不改动。

### 验收（本分支内）
- cargo check 两形态（default 明文零变化 + --features tls）过。
- 未启用 TLS 的构造路径全部 None 透传，行为不变。
