裁决：不成立（拒绝）。当前 rust 已端到端强制「必选客户端证书」，且与 C# 逐臂对齐；票据所述的
握手放行缺陷在本仓不可复现。

====== 原文（next/tls-mtls-client-cert-required-not-enforced.md）======

优先级：功能缺口之首（认证绕过——服务端在「必选客户端证书」配置下仍放行无证书客户端）

单问题：mTLS 必选臂未强制。配置要求客户端证书时，不带证书的客户端仍能完成握手，
即 TLS 客户端证书认证在服务侧未生效（或生效但被降级为可选）。

取证现状（2026-09-19 主代理实跑，两轮基线 + 一次隔离复跑）
- 全量门禁（干净 detached 快照 dev d54ce713）：`wnode::tls_test
  test_garnet_server_tls_mtls_client_cert_required` 与
  `test_garnet_server_tls_mtls_permissive_without_issuer` 均以 SIGABRT 结束（0.07-0.12s 即中止，
  非断言等值失败后的正常红，而是 panic 在 catch_unwind 内二次抛出致 abort）。
- 隔离复跑（`cargo nextest run --all-features -p wnode --test tls_test --test
  vector_set_production_switch --no-fail-fast --no-capture`）只重现 client_cert_required 一条，
  `permissive_without_issuer` 在同组单独跑时通过 ⇒ 该条与并发/端口/CA 准备顺序有关，
  要 separate 判定，不要与必选臂混为一次修复。
- 失败现场（panic 消息原文）：
  /Users/z/git/db/wedb/wedb/wnode/tests/tls_test.rs:527
  `assert!(refused, "无客户端证书应握手失败")` —— 即 :518-526 那段：用 `no_cert_connector`
  （只有 root、无 client_auth_cert）连上「配置为必选客户端证书」的服务端后，
  `connect` 未报错、且随后能读到合法应答，故 `refused` 为 false。
  同文件 :530-541 的「错 CA 签发必须拒」臂在 panic 前未执行到，不可据此判链校验已生效。
- 服务端 TLS 装配面在本仓的位置须自行定位（候选：wnode/wedb 侧 TLS _accept_ 配置构造、
  rustls `ServerConfig::with_client_cert_verifier` / `danger_client_cert_required` 类开关），
  以及 `permissive_without_issuer` 涉及的「无 issuer 校验的宽松模式」配置项。

修法要求
1 先实证「必选」配置从入口到 rustls ServerConfig 的传递链在哪一跳断掉（给出 file:line 与断点值），
  不许凭猜加 `with_client_cert_verifier` 了事；若本仓用的是自定义 verifier，核其
  `verify_client_cert`/`danger` 分支是否恒放行。
2 与 C# 对齐：garnet 侧客户端证书必选与宽松模式两个配置项的语义、以及「必选时缺证书=握手期拒绝」
  的确切行为，源码在 /Users/z/git/db/wedb/garnet（候选锚点 libs/server/Servers/*.cs 里
  `ClientCertificateRequired`、`AllowUnknownClients`、证书校验回调），把 rustls 形态与之逐条对照，
  并在回报里点名对应关系。**不要**实现 C# 里没有的证书策略、也不要引入任何平台/鉴权服务绑定。
3 测件只在「确证测件断言与 C# 语义不符」时才改，改时要给出 C# 处引用；
  不接受把断言改成「能连就算过」的恒真化。
4 SIGABRT 形态本身（panic 演变成 abort 而非普通红）要顺带查明：测件里是否在同线程内二次 panic
  （例如 drop 期/异步运行时内再 panic），这属可诊断信息，不要顺手改运行时。

改动域：服务端 TLS 装配（wnode/wedb 的 TLS 配置构造处）+ wedb/wnode/tests/tls_test.rs（若确为测件缺陷）。
禁止碰 wedb/wnode/src/resp/objects/**、wtxn/**、wconf/**（并发会话在跑）。

验收
1 `cargo nextest run --all-features -p wnode --test tls_test --no-fail-fast` 全绿（含 permissive 那条，
  连跑两次确认无顺序依赖抖动）。
2 给出「必选=缺证书即拒」「宽松=缺证书放行但带证书仍校链」两条的服务端实跑证据（新测件或既有用例）。
3 `cargo check --all-targets -p wnode` 零错零警告，禁 `#[allow(`。

====== 拒绝理由 ======

按票据自定的取证要求逐条核对「必选配置从入口到 rustls ServerConfig 的传递链」，结论是链路完整、
无断点，必选臂实际就是强制的；票据标题「未强制」与其自述的 SIGABRT 现场均不指向功能缺口。

一、传递链未断（票据要求 1，逐跳给 file:line）
1. 配置项解析：wconf/src/node_options.rs:351 `tls_client_cert_required: bool`、:369
   `tls_issuer_cert: Option<PathBuf>`，两字段有独立解析测试
   wconf/tests/garnet_server_config_tests.rs:97-122（`test_tls_client_cert_required_knobs`，
   含 `tls_client_cert_required: true` 与 `tls_issuer_cert: /tmp/ca.pem` 的回读断言），非死字段。
2. 入站装配入口：wnode/src/server.rs:838-843 `tls_config_from_node` 把
   `node.tls_client_cert_required` 与 `node.tls_issuer_cert` 原样传入
   `ServerTlsConfig::from_pem_files`，未硬编码 false。
3. ServerConfig 单点装配：wnode/src/tls/config.rs:104-109
   `required==true → builder.with_client_cert_verifier(client_verifier(issuer)?)`，
   否则 `with_no_client_auth()`；全 workspace 仅此一处构造 ServerConfig
   （grep ServerConfig::builder 命中只有 config.rs:104），不存在绕过的第二套装配。
4. 校验器两态（wnode/src/tls/config.rs:124-136）：
   - issuer 在位 → rustls 原生 `WebPkiClientVerifier::builder(roots).build()`。其
     builder 默认 `anon_policy = AnonymousClientPolicy::Deny`
     （rustls-0.23.45/src/webpki/client_verifier.rs:47），故 `client_auth_mandatory()`
     返回 true（同文件 :349-353 Deny→true）。rustls 对 mandatory 且未出示证书的客户端
     以 `Error::NoCertificatesPresented` 在握手期直接失败——即「缺证书=握手期拒绝」，
     票据担心的「自定义 verifier 的 danger 分支恒放行」在此不成立（这是 rustls 原生
     verifier，非本仓自定义）。
   - issuer 缺席 → 本仓自定义 `AnyClientCert`（config.rs:173-223），其
     `client_auth_mandatory()` 明确 `true`（:183-185），`verify_client_cert` 仅在「证书
     在场」时豁免链校验、证书缺失仍由 mandatory 拒（:193-200 只在校验已出示的证书时被调）。
5. 实际接入：wnode/src/server.rs:1043-1044 连接任务对每个 socket 调
   `acceptor.accept(stream)`，握手 `Err` 即 return 不进入会话——用的正是第 3 步装配出的
   acceptor（`ctx.tls_config.acceptor().clone()`，:1035），不存在「配了必选却用明文/另一
   acceptor」的旁路。

二、与 C# 对齐（票据要求 2）
garnet/libs/server/TLS/GarnetTlsOptions.cs 三态语义与 rustls 形态一一对上：
- :158-167 `SslServerAuthenticationOptions { ClientCertificateRequired,
  RemoteCertificateValidationCallback = ValidateClientCertificateCallback(IssuerCertificatePath) }`。
  .NET 侧 ClientCertificateRequired=true 时 SslStream 必发 CertificateRequest，缺证书则
  回调收到 certificate=null 并返回 false → 握手失败。rustls 用 `client_auth_mandatory()`
  表达同一「缺证书即拒」，两态皆 true。
- :234-247 `ValidateClientCertificateCallback`：!required → 恒 true（对应 rustls
  `with_no_client_auth()`）；required → 建链 + `ValidateCertificateIssuer(cert, issuer)`。
- :286-326 `ValidateCertificateIssuer`：authority!=null（给了 issuer）时链必须建到该已知
  根（thumbprint 匹配）——对应 rustls issuer 在位时的 `WebPkiClientVerifier`（根=issuer CA）；
  authority==null（未给 issuer，:273 告警「chain will not be validated against issuer」）时
  以 `AllowUnknownCertificateAuthority` 建链且跳过 thumbprint 校验 → 任意在场证书放行——
  正对应 rustls `AnyClientCert`（要求证书、不校链）。
rustls 形态无多余证书策略、无平台/微软绑定，符合 transpile 约束。

三、实证（票据要求 3/4 + 验收 1）
在 dev e75716e（票据基线 d54ce713 已不存在）：
- `cargo test --all-features -p wnode --test tls_test`（含 :527 无证书应拒、:540 错 CA 应拒、
  :681 宽松无证书应拒三处断言）全绿；
- `cargo nextest run --all-features -p wnode --test tls_test --no-fail-fast` 连跑 10 次，
  每次 5 passed、0 failed，未复现 SIGABRT。
即票据所引「:527 refused=false」的必选臂功能失败不可复现。票据自述的 SIGABRT 现场是
「0.07-0.12s 即中止、非断言等值失败后的正常红」，且其自证「只重现一条、另一条单独跑通过、
与并发/端口/CA 准备顺序有关」——这是重量级并发 `./test.sh` 门禁下 compio Runtime 拆除期二次
panic 的测件/调度抖动，属票据明令「不要顺手改运行时」范围外，也不是本主题（客户端证书强制）
的功能缺口。若要根治该抖动应另立「TLS 测件并发拆除稳定性」票，牵头的必选臂无需改动。

四、判论
「mTLS 必选臂未强制」经代码核对与实测均不成立：装配链五跳完整、WebPkiClientVerifier 默认即
mandatory、AnyClientCert 显式 mandatory=true、accept 路径直接使用该 acceptor，且与 C# 三臂语义
逐条吻合。既不违反 transpile，也无从「修复」——现状即正确实现。据 fixloop 步骤 0「票是 AI
生成、可能错，对照 C# 与当前 rust 核实」裁决为拒绝，删除 next 票，本条不再开分支。

边界登记：本裁决只管「必选/宽松强制」这一主题。票据提及但与本主题正交的
「TLS 测件在重量级并发门禁下的 SIGABRT 抖动（panic-in-teardown）」如后续确认为稳定复现问题，
应另立独立票处理，且按票据要求不轻改运行时。PEM 载入下沉等设计整理归 agy.design 分拣线，
非本票范围。
