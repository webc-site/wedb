归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 130ad53（P2），收口形态：出站弃装配期快照，ServerTlsConfig::cert_source() 暴露 Arc<ArcSwap<CertifiedKey>> 单源句柄，client.rs 以 from_shared_source＋SharedClientCertResolver 握手期 load_full 现取（对位 C# GarnetTlsOptions.cs:185-189 闭包动态读 selector），boot.rs 改自 provider.tls_config() 派生，旧 new 构造器零残留；热换装双向一次生效。机制口径 src 净 +38（映射注释 22＋trait 体 16，删物化臂 −12 入抵），测试两集成锁另册。续排注：票面 boot.rs 锚 :202-213 现码 :207-223 行号漂移已按现码执行。

甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P2
核验记录：rust 亲验——boot.rs:202-213 cluster_tls_client 装配期单次构造 ClientTlsConfig::new（证书构造期物化进 TlsConnector）；client.rs grep from_shared_source/with_client_cert_resolver 零命中、无握手期现取钩；config_commands.rs:342 cert-file-name 臂仅调 tls.update_cert_file（入站 ServerTlsConfig），wtls/server.rs reload ArcSwap 仅入站——出站零刷新通道现码原样，无修复合入。C# 亲验——GarnetTlsOptions.cs:183-189 LocalCertificateSelectionCallback 闭包动态读 serverCertificateSelector.GetSslServerCertificate() 现读亲见，UpdateCertFile 换 selector 即传播出站属实。查重：§124 五域判净留痕（SNI/ALPN/恢复/链深 EKU/套件）不含证书热换传播面，§36/§55-57/§71/§102 各管一面零撞面；四池零同轴。架构：ArcSwap<CertifiedKey> 与入站同一单源、严禁第二份证书状态符合单机制纪律，None 回落 with_no_client_auth 禁拒启、RwLock 下发链零改，审核措辞订正（弃 custom key provider 表述、危害真拒面收窄为旧证过期或换 CA）已承接；集成测试双节点轮换断言闭环。格式：纯文本、双侧齐全。定级 P2：mTLS 集群旧证过期后新出站握手全断无自愈（gossip/复制/迁移断链、重启方恢复），可用性/资源面缺陷非协议错，循 r16-certswap 先例 P2。

审核结论：通过（修复级；boot.rs:202-213 唯一装配点、client.rs:60-103 证书烘进 TlsConnector 无现取钩、热换装与 reload 仅入站、C# 闭包动态读 selector 触出站、mTLS 钉根臂实校验有效期故旧证过期出站全断无自愈、GarnetClient 构造期捕获 Arc 使「重建下发仅覆盖新建连接」论证成立——全部亲验属实；deviations §36/§55-§57/§71/§102/§124 无本面、四池零同轴票、ing wtls-cert-hotswap 系入站锁面不同轴）。审核措辞订正：危害真拒面为「旧证过期或换 CA」，未过期旧证在 §56 钉根臂（无主体名钉）仍被受；入站校验器 issuer 根同为装配期快照（assemble:169）勿扩入本案；方案不用「custom key provider」措辞。

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. wtls/server.rs 把共享证书源暴露为 Arc<ArcSwap<CertifiedKey>>（或 DynamicCertResolver 双 trait 化），出站与入站同一单源，零新证书状态、严禁第二份证书态。
2. client.rs 增 from_shared_source 形：rustls 0.23 原生 with_client_cert_resolver 握手期 load_full 现取；保留既有 fail-fast 门；provider.tls_config() 为 None（仅 issuer/target-host 而 has_tls 真）回落 with_no_client_auth，禁拒启。
3. boot.rs 装配点改自 provider.tls_config() 派生，删除独立装载；RwLock 下发链与 gossip/复制/迁移/failover 消费点零改。
4. 测试验证点：wtls 侧捕获型 ClientCertVerifier 断言握手呈出的 end_entity 随 update_cert_file 现取而变；集成以 wnode_tls_test 族做服务端钉 CA-B、客户端装 CA-A、CONFIG SET cert-file-name 轮换后断言新出站握手由拒转受（对端视角新 DN/序列号），并覆盖长驻连接重连呈新证；对照入站轮换用例 server_cert_reload.rs 族与 client.rs 既有两单测不回归。

集群出站 TLS 客户端证书为装配期不可变快照，热换证与周期刷新均不触达，出站 mTLS 握手持续呈旧证书

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# TlsClientOptions（集群 gossip/复制/迁移/failover 五类出站连接单源配置）在 GarnetTlsOptions 构造期建立（libs/server/TLS/GarnetTlsOptions.cs:95-96），其 LocalCertificateSelectionCallback 闭包动态读实例字段 serverCertificateSelector（:185-189）。UpdateCertFile（:100-117）换装新 selector 后，新出站握手即呈新证书；周期刷新定时器原位替换 selector 当前证书（ServerCertificateSelector.cs:100-133 GetSslServerCertificate 返回 sslServerCertificate 当前值），到期轮换同样传播到出站方向。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 集群出站配置 cluster_tls_client 仅在装配期构造一次（wedb/wedb/src/server/boot.rs:202-213，从 node.tls_cert/tls_key 读文件入 ClientTlsConfig），ClientTlsConfig::new 构造期经 load_certs/load_private_key 把证书物化进 TlsConnector（wedb/wtls/src/client.rs:60-103），此后不可变。CONFIG SET cert-file-name 热换装只调 host.tls_config.update_cert_file（wedb/wnode/src/resp/config_commands.rs:336-348，仅入站 ServerTlsConfig）；wtls 刷新循环 Inner::reload 的 ArcSwap store 同样仅入站。出站方向零刷新通道。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
mTLS 集群（对端 tls_server_cert_required + issuer 钉根）证书到期轮换或紧急轮换后：本节点入站已用新证书收新连接，但新发起的 gossip/复制/迁移出站握手仍呈装配期旧证书，被对端校验拒 → gossip 断、复制断流、迁移失败；旧证书过期后出站面全断且无自愈通道（重启才恢复）。C# 出站随 selector 动态取新证无此窗口，属身份面单侧换证断链的真实缺陷。注意 cluster_username/password 热更已接线（config_commands.rs:327-330 update_cluster_auth），证书面缺对等通道使缺口更孤立可辨。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/boot.rs:run_async（cluster_tls_client 装配期单次构造 :202-213）
wedb/wtls/src/client.rs:ClientTlsConfig::new（证书构造期物化进 TlsConnector 不可变 :60-103）
wedb/wnode/src/resp/config_commands.rs:network_config_set（cert-file-name 臂仅入站 :336-348）
wedb/wtls/src/server.rs:Inner::reload / update_cert_file（ArcSwap store 仅入站 ServerTlsConfig）

对应 c# 文件与函数：
libs/server/TLS/GarnetTlsOptions.cs:GarnetTlsOptions 构造（:95-96）/GetSslClientAuthenticationOptions（:172-190 闭包动态读 selector）/UpdateCertFile（:100-117）
libs/server/TLS/ServerCertificateSelector.cs:GetSslServerCertificate / GetServerCertificate（:100-133 定时器原位换证）

精炼执行方案：
1. 出站证书解耦快照：ClientTlsConfig 握手期从共享证书源现取 certified_key（与入站 ServerTlsConfig 同一 ArcSwap 单源，rustls ClientConfig 以 custom key provider 或重装配 connector 形态承接），热换装与刷新循环对两方向一次生效；严禁另建第二份证书状态（单机制纪律）
2. 若采重建下发形（update_cert_file 成功点连带 set_cluster_tls_client），须知长驻 GarnetClient 持旧 Arc 仅覆盖新建连接，与 C#「既有连接维持已建会话、新握手取新证」语义有差距，故共享单源为正解；boot.rs 装配点从共享源派生，删除独立快照构造
3. 测试验证点：集成测试起双节点 mTLS 集群，CONFIG SET cert-file-name 轮换后断开既有 gossip 连接触发重连，断言新出站握手被对端接受（对端视角客户端证书为新 DN/序列号）；对照入站轮换用例（server_cert_reload.rs 族）不回归
