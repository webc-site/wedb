优先级：中（重复实现清理，一处定义规范；行为零变化纯去重）
分拣来源：next/agy.net.md 条 4 + next/muse.net.md 条 8（两档同题，本票为唯一载体，源档已分拣清空删除）

问题
load_certs 与 load_private_key 在 wnode 与 wconn 各实现一份，逐行同形（rustls-pemfile::certs 收集 + 空集判 NotFound；rustls_pemfile::private_key + None 判 NotFound，错误文案两份一字不差），违反一处定义与去重规范。wconn 侧注释自述「与 wnode/src/tls/config.rs:load_certs 同源实现（crate 平级不互依）」——同源而两写即待收敛状态。

取证（rust，2026-09-19 主仓 dev 按符号重取）
- wedb/wnode/src/tls/config.rs:226-239 fn load_certs、:241-253 fn load_private_key（tls feature 门内）
- wedb/wconn/src/tls.rs:190-203 fn load_certs、:208-219 fn load_private_key（无 feature 门，模块整体随 tls feature 编入）
- 两份实现均为 File::open + BufReader + rustls-pemfile 解析 + 同款错误文案（「未在证书文件 {} 中找到有效证书」「未在私钥文件 {} 中找到有效私钥」）
- 签名面完全一致：fn load_certs(&Path) -> io::Result<Vec<CertificateDer<'static>>>、fn load_private_key(&Path) -> io::Result<PrivateKeyDer<'static>>
- 消费点：wnode 侧 from_pem_files（config.rs:46-47）与 ca_roots（:145）；wconn 侧 ClientTlsConfig::new（wconn/src/tls.rs:71-72）与 root_store（:114）

C# 对标
garnet/libs/server/TLS/GarnetTlsOptions.cs:GetCertificateIssuer（证书/签发者载入段，C# 单点实现全服务端与客户端共用，无第二份拷贝）

修法建议
- 下沉到 wbase 新增 tls pem 装载模块：feature 门内（如 tls-pem）引 rustls-pemfile 与 rustls-pki-types（cargo add 添加，禁手改 Cargo.toml），暴露 load_certs / load_private_key 单点；类型经 rustls-pki-types 直引（CertificateDer/PrivateKeyDer 与 compio_tls::rustls::pki_types 同源，wnode/Cargo.toml:11-12 注释已有先例说明）
- wnode、wconn 两侧删除本地实现改为转发或直用；错误文案单点保持现口径不改
- 纯去重零行为变化：不改解析栈（仍 rustls-pemfile 单一解析栈）、不改错误种类与文案、不动 AnyClientCert/NoVerify 校验器面
- 验收：bun js/check.js 无新增缺失；两侧 tls 既有测试全绿；grep 全仓 load_private_key 实现体仅 wbase 一处

边界
- 出站 NoVerify 与入站 AnyClientCert 校验器不在本票射程（两侧语义不同形，非重复）
- 与 next/tls-mtls-client-cert-required-not-enforced.md 无交叠（那单管必选客户端证书未强制的行为缺陷，本单纯注释外函数去重）
