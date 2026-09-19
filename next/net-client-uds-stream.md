优先级：中（客户端功能缺口：C# 客户端具备的 UDS 建连形态在 rust 客户端缺失，服务端 UDS 监听已就位）
分拣来源：next/agy.net.md 条 6（单源，无同题；源档已分拣清空删除）

问题
GarnetClient 客户端仅支持 TCP 建连：connect_async 硬编码 TcpStream::connect，OutStream 及其读写半句柄只有 Tcp / Tls(Tcp) 两臂。服务端已支持 Unix Domain Socket 监听（ServerEndpoint::Unix），但客户端无 UDS 路径，本机高性能进程间通信（嵌入式直连、本机副本/工具链）在客户端侧断链——这是 C# 客户端已有能力的转写缺失，不是自造扩展。

取证（rust，2026-09-19 主仓 dev 按符号重取）
- wedb/wconn/src/client.rs:133-145 fn GarnetClient::connect_async：TcpStream::connect(&self.end_point) 硬编码；:140 OutStream::Tls（TLS over Tcp）、:144 OutStream::Tcp 明文两臂
- wedb/wconn/src/network/stream.rs:29-35 enum OutStream { Tcp(TcpStream), Tls(Box<TlsStream<TcpStream>>) }；split() :40 起的 ReadHalf / WriteHalf 同样仅两臂
- TLS 路径硬绑 TCP：wedb/wconn/src/tls.rs:98-105 fn ClientTlsConfig::connect(stream: TcpStream, ...) 参数即 TcpStream
- 服务端 UDS 在位：wedb/wnode/src/endpoint.rs:18-19 ServerEndpoint::Unix(PathBuf)；wedb/wnode/src/server.rs:596-680 UDS 监听 Worker 经 ConnectionStream::Unix 交 process_stream（:680）
- 客户端端点形态：end_point 为 String（host:port），无路径形态判别

C# 对标
garnet/libs/client/GarnetClient.cs:ConnectSendSocket（:319-350）与 ConnectSendSocketAsync（:351 起）：EndPoint 非 DnsEndPoint 臂 new Socket(EndPoint.AddressFamily, Stream, Unspecified)，:345 `if (EndPoint is not UnixDomainSocketEndPoint) socket.NoDelay = true`，TryConnectSocket 对任意 EndPoint（含 UnixDomainSocketEndPoint）直连。即 C# 客户端构造入参收任意 EndPoint，UDS 一等公民。

修法建议
- 端点判别：end_point 以 '/' 开头（绝对路径）判 UDS，compio::net::UnixStream::connect 建连；其余走现 TCP 路径
- OutStream / ReadHalf / WriteHalf 增 Unix 变体（UnixStream + into_split 对齐 Tcp 臂形态）
- TLS over UDS：ClientTlsConfig::connect 的流参数从 TcpStream 泛化（或增枚举入参），对齐 C# SslStream 可套任意流的形态；若最小面先落明文 UDS，TLS-over-UDS 留待后续并注明
- server_name 的 host 段剥取逻辑（tls.rs:126-138）复核 UDS 路径形态（无冒号，rsplit_once 不命中即整串，需短路）
- 测试：wconn 内 loopback UDS pair（bind 临时路径 + connect + PING/应答往返），模式参照现有 loopback 用例

边界
- 不动服务端 UDS 监听面（已在位）；不改 TCP 既有路径行为
- gossip/迁移等上层消费是否切 UDS 不在本票射程，本票只补客户端建连能力
