终态：已合入 dev（2026-09-27）。9230844 tls_config_from_node 入口前置组合门:unixsocket+证书对齐即 InvalidArgument 拒启,文案对齐 guard_no_tls 红线;三臂差分锁测 rcgen 真签

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：裁启动门拒启，拒补臂防过度设计

审核结论：通过（P3）

审核席亲验留痕（2026-09-27，独立复核非背书票面）：
1 rust 侧全点位核实为真：run_uds_accept_loop 注释自陈（server.rs:1451/:1511）与直连臂 ConnectionStream::Unix（:1522）；TCP TLS 臂 acceptor+timeout+kill_token 三件套（:1357-1395）对照成立；guard_no_tls 先例（:874-883，cfg(not(tls)) 侧）；wconn stream.rs:84-93 出站 Unix 臂显式拒组合、注释自陈绝不静默降级明文。出入站两臂形态矛盾属实。
2 装配链反证排查：wconf node_options.rs validate()（:1299-1419）与 endpoints()（:1229-1255）均无 tls+unixsocket 互斥门，全仓无第二校验点；service.rs:1693 tls_config_from_node Some 臂注入后仅 TCP 循环消费 ctx.tls_config，UDS 循环不触达。组合配置真实可达，静默明文成立。
3 C# 侧反证排查无翻案：GarnetServer.cs:287-294 UDS 端点与 TCP 同构造器携 opts.TlsOptions（:294）；HandleAccept 对 UDS 仅跳 NoDelay（GarnetServerTcp.cs:248），:290 handler.Start(tlsOptions?.TlsServerOptions) 照常执行；NetworkHandler.cs:110-129 useTLS 即建 SslStream、:148-157 Start 阻塞走 AuthenticateAsServerAsync，ClientCertificateRequired 门同覆盖 UDS。上游全仓无 TlsOptions 与 UnixSocket 互斥校验。票面「C# 同配置受 TLS 门」主张成立。
4 查重：deviations §54（UDS 父目录自创建）/§56（cert-subject-name 删员互拒门）/§124（TLS 残面四形）均无本形态在册条，无重复提报。
5 危害评估：unixsocketperm None 沿用 umask（server.rs:381 自陈），默认权限面未收紧属主，强制 mTLS 部署组合配置下 UDS 面绕开身份门与握手治理面，且违 guard_no_tls「严禁静默降级为明文」自设红线；组合面窄、无数据损坏与崩溃面，P3 定级恰当。

三向裁决：裁方案 1(a) 拒启门。guard_no_tls 同文件同族先例、控制面启动期单点收口、最小改动。方案 1(b) 补臂判过度设计：UDS 本地信任边界下 TLS 增益近零，新增 ConnectionStream 变体与 UDS-TLS 握手机制属双重机制面。方案 2 纯登记留静默不满足红线，仅作未来裁「UDS 本地信任既定改良」时的回退位。

审核裁定执行方案（供 task/fix.md 直接消费）：
1 收口单点：server.rs tls_config_from_node（:858-872，cfg(feature="tls") 侧 TLS 配置投影唯一入口）Some 臂前置判 node.unixsocket.is_some()，命中即 Err(Error::InvalidArgument)，文案对齐 guard_no_tls 风格点名 --tls-cert/--tls-key 与 --unixsocket 组合拒启防静默明文；与 cfg(not(tls)) 侧 guard_no_tls 构成对称双门，仿 validate() 内 latency_monitor 全仓唯一校验点纪律不在 wconf 二设（wconf 不知特性开关）
2 不动 UDS 循环与 wconn 出站臂；run_async（:182-183）cfg(not) 门保持原样
3 测试：组合拒启锁测三臂，tls_cert+tls_key+unixsocket 即 Err、cert+key 仅 TCP 即 Ok、unixsocket 无证书即 Ok（对齐 guard_no_tls_tests :1666-1684 形制）

TLS 启用时 UDS 监听臂无握手位、无组合拒启门——静默明文端点：C# UDS 端点同携 TlsOptions 每连接握手，rust UDS 直连明文且与出站臂显式拒组合自相矛盾

问题分析：
1 Garnet 契约对齐：C# UDS 端点走同一 GarnetServerTcp accept 路径（garnet/libs/host/GarnetServer.cs:287-294 对 UnixDomainSocketEndPoint 同样以 opts.TlsOptions 构造），每连接 :290 handler.Start(tlsOptions?.TlsServerOptions) → NetworkHandler.cs:148-157 AuthenticateAsServerAsync → :185 SslStream 握手，UDS 连接同样执行 TLS 握手与 ClientCertificateRequired 门。
2 工程现状确证：rust UDS 接入环（wedb/wnode/src/server.rs:1453-1527）自陈「UDS 无 TLS 握手位」（:1451、:1511），连接直接 ConnectionStream::Unix(stream)（:1522）不接 acceptor；wconf 装配链无「tls_cert + unixsocket 组合」拒启门（全仓 grep 仅注释命中）。同仓出站侧显式拒组合——wedb/wconn/src/network/stream.rs:84-93 Unix 臂返回 Unsupported「Unix domain socket over TLS not supported」（注释自陈绝不静默降级为明文）——出入站两臂形态自相矛盾，入站臂恰是 TLS 开着却静默明文形态。
3 逻辑危害确证：tls-client-cert-required=true 部署同时配 UDS 端点时，UDS 面完全绕开 mTLS 身份门与握手超时/kill_token 治理面（C# 同配置受 TLS 门约束）；互操作面 TLS 强制客户端指向 UDS 端点在 C# 可握手成功，对 rust 则 ClientHello 字节灌入明文 RESP 解析器回协议错误帧。方向为 rust 较 C# 宽（静默明文），与 guard_no_tls（server.rs:874-883 拒绝启动防静默降级明文）自设红线同族冲突。§54 仅登记父目录自创建，本形态无在册条。

涉及代码：
rust 文件与函数：
wedb/wnode/src/server.rs:run_uds_accept_loop（:1453-1527，对照 run_tcp_accept_loop TLS 臂 :1357-1395）、guard_no_tls 先例（:874-883）
wedb/wconn/src/network/stream.rs:OutStream::with_tls 出站拒组合（:76-97）

对应 c# 文件与函数：
garnet/libs/server/Servers/GarnetServerTcp.cs:HandleAccept（:240-300，:290 Start 携 TLS）
garnet/libs/common/Networking/NetworkHandler.cs:Start/AuthenticateAsServerAsync（:148-157/:180-190）
garnet/libs/host/GarnetServer.cs:287-294（UDS 端点同携 TlsOptions）

精炼执行方案：
1 最小收口二择一：(a) 启动门——tls_cert 在位且端点集含 UDS 时显式拒启（对齐 guard_no_tls 文案风格，消灭静默明文）；(b) 补臂——UDS 循环接入同一 acceptor+timeout+kill_token 形（与 TCP 臂同构）
2 若裁 UDS 本地信任为既定改良，则 deviations 补登记条并回写 wconn with_tls 出站臂注释互指，消双侧形态矛盾
3 测试验证点：组合配置拒启锁测或 UDS-TLS 握手往返锁测
