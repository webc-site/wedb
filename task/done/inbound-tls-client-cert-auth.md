入站 TLS 客户端认证（mTLS）整体缺位：无 client-certificate-required / issuer-certificate-path 旋钮，握手固定 with_no_client_auth

来源：glm.net 第 10 条（分拣判定成立且待做）。取证基线：主仓 HEAD 50d1cb5f，行号为当下实况。

现状
- 装配硬编码：/Users/z/git/db/wedb/wedb/wnode/src/tls/config.rs:32 from_pem_files 内
  :40 与 :56 两处 `.with_no_client_auth()`——rustls ServerConfig 永远不请求/校验客户端证书。
- 配置面无旋钮：/Users/z/git/db/wedb/wedb/wconf/src/node_options.rs 仅 :259 tls_cert、:263 tls_key
  两字段（默认值 :524-525，TOML 映射 :698-699）；全仓 wconf/wnode/wedb grep
  `client_cert|issuer_cert|client-certificate|issuer-certificate` 零命中。
- 启动装配同形：/Users/z/git/db/wedb/wedb/wnode/src/server.rs:786-799 只按
  (tls_cert, tls_key) 二元组建 ServerTlsConfig，无客户端认证分支。
- 既成后果：mTLS 双向认证部署形态在 rust 侧不可表达；与集群出站 TLS 缺席
  （task/ing/cluster-outbound-tls-client.md）叠加后，「TLS 集群」在 rust 仅能做到单向服务端认证。
- 现有 ignore 登记不构成豁免：/Users/z/git/db/wedb/js/check/ignore/server.yml:512-516
  对 CertificateUtils.cs / GarnetTlsOptions.cs / ServerCertificateSelector.cs 的理由是
  「C# .NET SslStream/X509 证书体系，Rust 侧采用 rustls 原生安全传输层」——只声明实现载体替换，
  未声明裁掉客户端认证功能，且该登记只覆盖 TLS 配置类文件，不含 host 层两个选项。
- rustls 生态有现成对位（rustls::server::WebPkiClientVerifier + rustls-pemfile 读 CA +
  可选 CRL revocation），无需新造验证器。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/server/TLS/GarnetTlsOptions.cs:39 ClientCertificateRequired 字段、
  :49 CertificateRevocationCheckMode、:59 IssuerCertificatePath、:81-88 构造期赋值；
  :152-166 GetSslServerAuthenticationOptions 内 :160 `ClientCertificateRequired = ClientCertificateRequired`、
  :161 revocation mode、:162 `RemoteCertificateValidationCallback = ValidateClientCertificateCallback(IssuerCertificatePath)`；
  回调实现 :234-273（未开启时 :238 直接告警放行；开启但未给 issuer 时 :273 告警不校验链）。
- 选项入口：/Users/z/git/db/wedb/garnet/libs/host/Configuration/Options.cs:333
  `[Option("client-certificate-required")]`、:339 `[Option("issuer-certificate-path")]`。
- 消费点：/Users/z/git/db/wedb/garnet/libs/server/Servers/GarnetServerTcp.cs:290
  `handler.Start(tlsOptions?.TlsServerOptions)`（入站握手携带上述 options）。

修法
1. wconf 增两字段（默认关闭，与 C# 一致）：`tls_client_cert_required: bool`（默认 false）、
   `tls_issuer_cert: Option<PathBuf>`，进 NodeArgs 与 TOML 映射表，禁从环境变量另立第二条通路。
2. ServerTlsConfig::from_pem_files 增参数（或改 try_from_node_args 单点，禁两套构造入口并存）：
   required=true 时以 issuer PEM 构建 root store 并 `rustls::server::WebPkiClientVerifier::builder(...).build()`
   换掉 with_no_client_auth；required=true 而 issuer 缺失时按 C# :273 语义「要求证书但不做颁发者链校验」
   落地（AnyClientVerifier 形态）并 warn 一次，禁静默降级为不请求证书。
3. 装配点唯一化：/Users/z/git/db/wedb/wedb/wnode/src/server.rs:786-799 的 TLS 匹配臂把两新旋钮
   一并传入，且该臂须与 boot 侧同一入口（与 boot-assembly 票的「TLS 段收进统一入口」同向，不冲突）。
4. C# 另有 --certificate-revocation-check-mode（:49/:161）：rustls 需显式 CRL 输入，
   本票按「登记缺席」处理——在配置注释点名 C# 锚点与缺席原因，禁自造第二套吊销检查实现，
   也不随本票落地（如需实现另立单）。

优先级
功能缺口（既有安全能力面缺失，非风格问题；不开启即行为不变，开启后可表达 mTLS）。

协调
- 与 task/ing/cluster-outbound-tls-client.md 互补不重复：那条是节点间出站加密（wconn），
  本条是入站客户端认证（wnode/tls + wconf）。两票共用 rustls/compio-tls 依赖面，
  `cargo add` 须一次谈齐避免各加一次；两票可并档交付但不合并（消费面完全不同）。
- 不动 js/check/ignore/server.yml:512-516 的既有理由（该登记仍成立，载体替换事实不变）；
  落地后 host 层两选项自然转为「有对位」，无需 ignore 变更。

验收
- 默认（旋钮不开）行为零变化：现有 TLS 用例与握手全通过。
- 新增用例：客户端证书由 issuer 签发 → 握手成功并被识别；客户端证书非该 issuer → 握手失败
  且服务端错误可辨；required=true 且客户端不带证书 → 握手失败。
- 两新旋钮进 CONFIG/命令行装配链的现有配置面测试（wconf 侧参数表断言）。
- 验证纪律：仅 cargo check --workspace --all-targets（私有 target 目录）零 error 零 warning；
  test.sh/clippy 由中央整合轮执行。

细化方案（2026-09-19，实现代理追加；基线为认领时 HEAD，票据原修法第 1 条按当前实况收敛）
- 勘误：tls_issuer_cert 已随 cluster-outbound-tls-client 落地（wconf node_options.rs，
  出站语义）。C# IssuerCertificatePath 本就是单字段双用（入站 ValidateClientCertificateCallback
  :162 与出站 ValidateServerCertificateCallback :182 共用 :59 字段），故本票不新增第二个
  issuer 字段，直接复用 tls_issuer_cert，仅把注释补成双用语义——「一处定义」。
- wconf：NodeArgs 增 tls_client_cert_required: bool（--tls-client-cert-required，默认 false，
  serde default，进 over![] TOML 表）；注释点名 Options.cs:333 锚点与 Options.cs:336
  certificate-revocation-check-mode 缺席原因（rustls 需显式 CRL 输入面，不随本票落地）。
- wnode/src/tls/config.rs：from_pem_files 增 (client_cert_required, issuer_path) 两参；
  from_der 增 (client_cert_required, issuer_ca: Option<Vec<CertificateDer>>) 两参（内存 DER
  形态，测试/自签装配同入口）；两者收敛到私有单点 client_verifier：
  required=false → with_no_client_auth（行为零变化）；
  required=true + issuer → rustls-pemfile 载 CA 入 RootCertStore（空即 NotFound 快速失败）
    → WebPkiClientVerifier（rustls 原生验证器，不自造）；
  required=true + 无 issuer → AnyClientCert 宽松校验器（client_auth_mandatory=true、
    verify_client_cert 恒过、握手指名校验委托 rustls::crypto::verify_tls12/13_signature，
    形态与 wconn/tls.rs NoVerify 同源）+ 构造期 log::warn 一次（C# :273 语义）。
- wnode/src/server.rs run_node TLS 匹配臂单点传参 conf.tls_client_cert_required +
  conf.tls_issuer_cert.as_deref()，无第二装配入口。
- 测试：tls_test.rs 既有三处 from_der 补 (false, None)；新增 issuer 钉根用例（合法证书
  握手成功 / 无证书失败 / 错 CA 失败）与宽松模式用例（任意自签证书成功 / 无证书失败），
  rcgen 0.13 KeyPair+CertificateParams+signed_by 签发链；wconf 侧默认断言与
  --tls-client-cert-required 解析断言进 garnet_server_config_tests.rs。
- rustls 0.23.45 无 AnyClientVerifier 类型，宽松臂自实现 danger::ClientCertVerifier（签名
  面全委托 provider，无自造密码学）。
