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
