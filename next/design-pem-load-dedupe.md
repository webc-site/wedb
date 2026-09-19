优先级：中
来源：next/agy.design.md 条 1 与 next/muse.design.md 条 1（两轮同题合并，口径一致）。
取证基线：主仓 dev 当下代码。

问题
PEM 证书链与私钥解析函数在入站（服务端）与出站（客户端）两侧各写一份，逐行同形真重复；
wconn 侧注释自认「与 wnode/src/tls/config.rs:load_certs 同源实现（crate 平级不互依）」，即
已知重复仍维持两份。

取证
- wedb/wnode/src/tls/config.rs:226 fn load_certs、:242 fn load_private_key（均 #[cfg(feature = "tls")]）
- wedb/wconn/src/tls.rs:190 fn load_certs、:208 fn load_private_key
- 两份函数体逐行同形：File::open → BufReader → rustls_pemfile::certs/private_key →
  collect/map_err → 空结果 NotFound 错误（中文文案也逐字相同）
- C# 对标：garnet/libs/server/TLS/GarnetTlsOptions.cs 的证书载入（GetCertificateIssuer 相关段）
  只在服务端选项一处定义，客户端侧（SslClientAuthenticationOptions 装配）复用同一载入逻辑，
  无第二份手抄

修法建议
两函数下沉一处共享定义：放 wbase（tls feature 门控）或独立薄口，wnode/tls/config.rs 与
wconn/tls.rs 改薄包装调用；错误文案随函数一处定义。C# 锚点注释保留在下沉后的单点
（garnet/libs/server/TLS/GarnetTlsOptions.cs:载入段），两包装处不再各自挂锚。
