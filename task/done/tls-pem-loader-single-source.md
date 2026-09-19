优先级：高（重复/多套架构，仅次于死代码；两处逐行同形的 PEM 载入器 + 同形的签名算法表）

一句话：load_certs / load_private_key 在 wnode 与 wconn 各写一份、逻辑与依赖完全雷同，SIGNATURE_ALGS 与其 verifier 方法体也各写一份，违反「一处定义」；下沉为 wbase 的 tls 特性模块单点，两侧只留薄封装。

来源：next/agy.net.md 条 4（同一问题在 next/agy.design.md 条 4、next/muse.design.md 条 4、next/muse.net.md 条内亦有登记，本件为载体，禁两票各搬一半）。

现状（主仓 dev 现刻按符号取证，行号随并发漂移）
- /Users/z/git/db/wedb/wedb/wnode/src/tls/config.rs:226 load_certs、:242 load_private_key（入站侧，`#[cfg(feature = "tls")]` 门内）
- /Users/z/git/db/wedb/wedb/wconn/src/tls.rs:190 load_certs、:208 load_private_key（出站侧），其文档注释自陈「与 wnode/src/tls/config.rs:load_certs 同源实现（crate 平级不互依）」——即已知重复、无处置
- 两份函数体除 `BufReader` 取 `std::io::BufReader` 与裸 `BufReader` 之外逐行同形：rustls_pemfile::certs 收集 → 空集判 NotFound → private_key 的 map_err/ok_or_else 文案一致
- 同族第二处重复：/Users/z/git/db/wedb/wedb/wnode/src/tls/config.rs:177 static SIGNATURE_ALGS（ring default_provider 的 signature_verification_algorithms，LazyLock）与 /Users/z/git/db/wedb/wedb/wconn/src/tls.rs:149 同名同式；两侧的 verify_tls12_signature / verify_tls13_signature 三行体（crypto::verify_tls12_signature(..., &SIGNATURE_ALGS) 与 supported_verify_schemes）亦同形（config.rs:202-222 vs tls.rs:164-184）。本票射程含签名算法表单点，verifier 结构体本体（AnyClientCert / NoVerify 分别实现 ClientCertVerifier / ServerCertVerifier）职责不同，不并
- 依赖面：wnode 与 wconn 均已引 rustls-pemfile + rustls-pki-types（/Users/z/git/db/wedb/wedb/wnode/Cargo.toml:22 tls 特性与 :37/:38 依赖；/Users/z/git/db/wedb/wedb/wconn/Cargo.toml:33），两 crate 均已依赖 wbase（wnode/Cargo.toml:45、wconn/Cargo.toml:35），故下沉无需新建 crate
- pki 类型同源：wnode 经 compio_tls::rustls::pki_types 再导出取 CertificateDer（wnode/Cargo.toml:11-12 注释），wbase 直接引 rustls-pki-types（workspace 同版 1.15.1）即同一类型，无须转换

对应 C#
- garnet/libs/server/TLS/CertificateUtils.cs:GetMachineCertificateByFile（+ 私有段 GetCertificateFromPemFile / IsPemFile）：C# 侧证书与私钥载入是一个静态工具类的单点，仅被 ServerCertificateSelector.cs:112/:118 一处消费，不存在两份实现
- garnet/libs/server/TLS/CertificateUtils.cs:GetMachineCertificateBySubjectName 走 X509Store（机器证书存储），平台绑定，不转写；本票不得借下沉之名引入任何证书存储/微软认证面
- 现有锚点分布须一并核对：全仓 grep CertificateUtils 零命中（rust 侧无该文件的映射登记），下沉单点正名时把 C# 锚点写在承接函数上

修法
1. wbase 新增 tls.rs 模块（照 wbase 既有按特性切口的形态，如 pool/、future.rs）：pub fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> 与 pub fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>>，函数体取现 wnode 版单点（含「未在证书/私钥文件 {} 中找到有效…」文案），另收 `pub static SIGNATURE_ALGS`（或等价 fn signature_algs() -> &'static WebPkiSupportedAlgorithms，若 ring 类型不便静态持有则维持 LazyLock）
2. 依赖只走 cargo add：在 wedb/wbase 目录 `cargo add rustls-pemfile --optional` + `cargo add rustls-pki-types --optional`，并在 wbase 的 Cargo.toml features 里加 `tls = ["dep:rustls-pemfile", "dep:rustls-pki-types"]`；禁手写版本、禁改已有依赖行（版本一律 workspace 继承）
3. wnode/src/tls/config.rs 与 wconn/src/tls.rs 删除各自两份实现，改 `use wbase::tls::{load_certs, load_private_key}`；两侧调用点（wnode from_pem_files 路径 config.rs:40 起、wconn ClientTlsConfig::new 路径 tls.rs:52 起）签名不变
4. 两 crate 的 tls 特性须把 wbase 的 tls 口点亮（wnode features.tls 追加 "wbase/tls"，wconn 同），并保持既有的 rustls-pemfile 可选依赖在删码后归零引用——若归零则一并由 cargo 侧清理（feature check 会报未用依赖，按 /Users/z/git/db/wedb/feature.check.sh 的 ignored 白名单口径处理，禁为跑通而放宽）
5. SIGNATURE_ALGS 单点后，两侧 verifier 的 verify_tls12_signature / verify_tls13_signature / supported_verify_schemes 三处改为消费 wbase 单点，静态表只留一份
6. 文档注释按 SKILL 格式写 C# 映射（`/// 在 garnet 中的相对路径: libs/server/TLS/CertificateUtils.cs:GetMachineCertificateByFile`），两侧薄封装不再重复挂同一锚点，避免把「重复定义」从代码面搬到注释面

验收判据
- 全仓 grep `fn load_certs` 与 `fn load_private_key` 各自仅命中 1 处定义（限定符号 wbase::tls::load_certs / wbase::tls::load_private_key），wnode/wconn 侧只余 use 与调用
- 全仓 grep `static SIGNATURE_ALGS` 仅命中 1 处（含 wbase 内的最终命名）
- cargo check --workspace --all-features 与 cargo check --workspace（关 tls）双态零错误零警告，禁 #[allow]
- 既有 TLS 用例不降级：cargo nextest run --all-features -p wnode --test tls_test --no-fail-fast 的 cert/key 载入相关臂通过；wconn 出站 TLS 单测（tls.rs:224 server_name_fallback 等）保持绿
- mTLS 必选臂不得降级：该议题已由并发会话裁定为不成立并归档 /Users/z/git/db/wedb/task/reject/tls-mtls-client-cert-required-not-enforced.md（现文核实「必选客户端证书」端到端强制在位、与 C# 三臂对齐）。本票下沉 PEM 装载只动证书/私钥读盘面，`client_auth_mandatory` 与 verify_now 的裁决逻辑一律不动，改前改后该臂行为必须逐字一致
- js/check.js 的重复定义段不因本改动新增条目（在 worktree 内跑，禁在主仓跑以免回写 ignore 语料）

互斥与边界
- 本票文件域：wbase/src/tls.rs（新）、wbase/Cargo.toml（cargo add 生成）、wnode/src/tls/config.rs、wconn/src/tls.rs；与 wtxn/wkv/wnode/src/resp/objects 的 rmw 路径零交叠
- 在途实测更正（2026-09-19 现刻，本条作废原「无任何在途修复分支可作互斥依据」的说法）：`git worktree list` 实得 11 个 fork（docs-data-comment-batch、docs-readme-crate-map、fix-custom-obj-multikey、fix-lua-pending-handoff、fix-pending-lat-mget、fix-reviv-crtt-gate、fix-rmw-atomic-window、fix-vector-registry-two、split-cluster-provider、windex-2pl-removal，另 /tmp/fork/dev-2026-09-19 为 garnet C# 快照）。逐个 `git diff --name-only dev...<br>` 比对：无 fork 触及 wnode/src/tls/config.rs、wconn/src/tls.rs、wbase/Cargo.toml，本票可即时开工；但与 task/ing/net-checkjs-dup-anchor-trio.md 条二同撞 wnode/src/tls/config.rs（from_der 与 server_config 的文档块），两票必须串行——先本票下沉 PEM 装载、后本域锚点收口票，勿并行
- 不做向下兼容：两份旧实现直接删，不留薄壳转发
