终态：已合入 dev（merge fx0N 系，2026-09-27）。dcbb96e CertState 存档 issuer/client_cert_required,acceptor ArcSwap 换装,update_cert_file 整体重建含 issuer;四案锁测 rcgen 真签发链

甄别结论：通过 | 定级 P2 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：CertState 存档+ArcSwap 换装+四案锁测；issuer 每连接重读对位

审核结论：通过（P2）

独立审核席亲验记录（2026-09-27）：
1 真实性坐实：rust 侧 assemble（wedb/wtls/src/server.rs:160-189）为全仓唯一 ServerConfig/TlsAcceptor 生产装配点（:169 server_config 唯一调用、:171 TlsAcceptor::from 唯一构造），client_verifier/ca_roots 构造期一次性读盘冻结；update_cert_file（:220-259）仅 :245 resolver store+路径/代际，CertState（:70-82）无 issuer 字段——issuer 路径装配后即失传，连重读能力都不存在。C# 侧 GarnetTlsOptions.cs:118 整体重建、:162 回调重挂、:242 GetCertificateIssuer 每次重建重读盘、GarnetServerTcp.cs:290 确在每连接 accept 回调内读属性当前值，四点全亲验属实。
2 反证排查（票不翻案）：a) acceptor 无任何其他重建路径——全仓 grep 仅 wtls/tests/outbound_cert_hotswap.rs:156 测试自建 acceptor，非生产路径；b) refresh loop（try_start_refresh_loop→Inner::reload :335-349）只重读 cert/key 换 resolver，从不触碰 issuer——刷新轮询不覆盖此缺口；c) issuer 文件无独立探测/重载通道（无 watcher、CertState 无字段）。C# 定时刷新同样只重读服务端证书（ServerCertificateSelector 域），issuer 刷新本就只挂 UpdateCertFile——票面 scope 与 C# 精确对位，无需收窄。
3 查重净：deviations §36（校验五分面）、§55（PEM-only）、§56（subject-name 删员）、§57（fail-fast 双向）、§102（握手超时）、§124（票据/链深/EKU/吊销旋钮）均不含 issuer 热重载面；issue/todo/ing/done/reject 五池与 fix.md 零命中。
4 架构合规：换装即重建 acceptor 系 rustls 结构必然（client_verifier 冻结于 ServerConfig），非双机制；ArcSwap 换装与 resolver 同谱（控制面换、数据面每连接一次 load），零开销纪律不破；失败保旧承 §57。
5 既有并发锁测不受破坏：server.rs 测试模块四例全用 client_cert_required=false+issuer=None，重建臂零 IO 无失败态，epoch 语义（换代恰自增一次）不变。

定级理由：mTLS CA 轮换场景整机拒入+陈旧信任锚残留+certificate_authorities 提示失真，控制面契约分叉实害成立；但触发需 mTLS 部署+CA 轮换运维动作叠加，非常态数据路径，非 P0/P1，P2 恰当。

问题分析：
1 Garnet 契约对齐：C# UpdateCertFile 不是只换证书——GarnetTlsOptions.cs:118 TlsServerOptions = GetSslServerAuthenticationOptions() 整体重建认证选项，:162 RemoteCertificateValidationCallback = ValidateClientCertificateCallback(IssuerCertificatePath)，回调构造期 :242 GetCertificateIssuer(issuerCertificatePath) 每次重建从 issuer 文件重新读盘装载（:253-276）；消费点 GarnetServerTcp.cs:290 handler.Start(tlsOptions?.TlsServerOptions) 位于每连接 accept 回调读属性当前值——C# 换证后新连接的客户端证书校验钉根同步换新。
2 工程现状确证：rust acceptor 的 ServerConfig 仅 assemble 构造一次（wedb/wtls/src/server.rs:169-171，server_config 全仓唯一调用点），客户端校验器该时点冻结——:390-391 with_client_cert_verifier(client_verifier(issuer)?)、:408 WebPkiClientVerifier::builder(Arc::new(ca_roots(src)?))、:426 CaSource::Pem(path) 启动期一次性读盘。而 update_cert_file（server.rs:220-259）仅 :245 resolver.0.store(Arc::new(ck)) 换服务端证书与路径/代际，从不触碰 acceptor 与 client_verifier——issuer CA 快照永久停留启动时刻；:239-241 注释自陈「对位 C# :118 整体重建 TlsServerOptions 的换装即终态」但实际只转写了证书重建一半，issuer 重建半未转写且无在册裁决。
3 逻辑危害确证：mTLS CA 轮换场景——运维以新 CA 签发客户端证书、更新 tls-issuer-cert 文件并 CONFIG SET cert-file-name 换服务端证：C# 新连接按新 CA 验签通过；rust 服务端证书换了但客户端校验仍钉死旧 CA 快照，新 CA 客户证书全部 rustls UnknownIssuer 拒绝（mTLS 面整机拒入），旧 CA 反继续被信任（陈旧信任锚残留），WebPkiClientVerifier 回给客户端的 certificate_authorities 提示亦为陈旧集。控制面重载不完整导致的运行期 mTLS 中断与信任面漂移。

涉及代码：
rust 文件与函数：
wedb/wtls/src/server.rs:ServerTlsConfig::update_cert_file（:220-259）、assemble（:160-189，:169 唯一 server_config 调用点）、server_config（:384-396）、client_verifier（:406-418）、ca_roots（:423-441）

对应 c# 文件与函数：
garnet/libs/server/TLS/GarnetTlsOptions.cs:UpdateCertFile（:100-120，:118 整体重建）、GetSslServerAuthenticationOptions（:122-168，:162 回调重挂）、ValidateClientCertificateCallback（:234-247，:242 issuer 重读）；garnet/libs/server/Servers/GarnetServerTcp.cs:290（每连接读 TlsServerOptions 属性）

精炼执行方案：
1 update_cert_file 成功臂内以现存 issuer CaSource 参数重建 ServerConfig/TlsAcceptor（client_cert_required 与 issuer 路径构造态存档于 CertState），Arc 换入 Inner.acceptor（acceptor 已 Arc 包装，accept 侧每连接现取 clone，换装对后续连接自然生效）；换装序保持锁外装载成功后才临界区替换，失败保留旧 acceptor（§57 fail-fast 形延续）
2 测试验证点：wtls 集成锁——CA1 签发 client 证书启动 → CA2 重签 client 证书 + update_cert_file → 新握手应过；未换 issuer 文件时应仍拒（防反向放宽）

审核裁定执行方案（审核席整理，供 task/fix.md 消费）：

1 CertState 增两字段：client_cert_required: bool 与 issuer 有owning 形态（CaSource<'a> 借用形不可跨期存档——Pem 存 PathBuf、Der 存 Vec<CertificateDer>，或直接 Option<PathBuf> 对位 Pem 主形态）；assemble 装配时同步落档。
2 Inner.acceptor 字段转可换装形态：ArcSwap<TlsAcceptor>（或同形 ArcSwap<ServerConfig>+每连接 TlsAcceptor::from）；现字段为 &Arc<Inner> 后不可变裸值，票面「已 Arc 包装自然生效」措辞不精确——TlsAcceptor 内部 Arc 不可穿透替换，必须改字段形态方可换装。acceptor() 改经 load() 取用，每连接多一次原子 load，与 resolver 同谱合规。
3 update_cert_file 扩序：既有 cert/key 锁外装载后，同锁外按存档 client_cert_required+issuer 重建 ServerConfig（必须 Arc::clone 现存 resolver 传入 server_config——否则刷新循环与 reload 的证书换装脱锚于新 acceptor，既有四枚并发锁测与刷新单源语义即破）→ TlsAcceptor 包装；任一装载失败（含 ca_roots 空集/坏 PEM）即 Err 返回保留旧 acceptor 与旧证（§57 fail-fast 延续，禁半态）。临界区内 store resolver + 换 acceptor + 路径落位 + 换代同守卫原子落位；换装期间新 acceptor+旧证（或反向）瞬态组合无害（服务端证书与客户端钉根互不相干）。
4 client_cert_required=false / issuer=None 臂重建退化为 with_no_client_auth，零 IO 无失败态——既有测试与单向 TLS 形态零行为变化。
5 测试验证点（承票面并落实）：wtls tests 增集成锁（rcgen 0.14.10 已在测试依赖、outbound_cert_hotswap.rs 已有进程内真握手先例）——a) CA1 签发 client 证书 + issuer=CA1 启动 + mTLS 握手应过；b) 文件换 CA2 重签 client 证书 + update_cert_file → 新握手应过（钉根随换）；c) 只换 client 证书到 CA2 不动 issuer 文件 + update_cert_file → 新握手应拒（防反向放宽）；d) issuer 文件指坏路径 update_cert_file → Err 且旧钉根仍生效（fail-fast 保旧锁）。
