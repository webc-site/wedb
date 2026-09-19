优先级：功能缺口（服务端已支持 UDS 监听、客户端不支持，本机进程间通信链路断一半）

一句话：wconn 的 GarnetClient 与 GarnetClientSession 都把 TcpStream::connect 写死，出站无 Unix 域套接字臂；C# 两个客户端类都按 EndPoint 类型分派 TCP/UDS，属未转写缺口。

来源：next/agy.net.md 条 6。

现状（主仓 dev 现刻按符号取证）
- /Users/z/git/db/wedb/wedb/wconn/src/client.rs:133-144 GarnetClient::connect_async：`let sock = TcpStream::connect(&self.end_point).await?;` 后仅 OutStream::Tcp / OutStream::Tls 两臂；set_nodelay 无条件施加
- /Users/z/git/db/wedb/wedb/wconn/src/session.rs:84-85 GarnetClientSession::connect_async 同型写死（同一缺陷的第二处）
- /Users/z/git/db/wedb/wedb/wconn/src/network/stream.rs:29 enum OutStream（Tcp / Tls）、:56 enum ReadHalf、:83 enum WriteHalf 三处均无 Unix 变体；split / read_owned / write 各 match 需补臂
- 入站侧对照（证明本仓已具备 UDS 形态，缺的只有出站）：/Users/z/git/db/wedb/wedb/wnode/src/net/stream.rs:29-32 ConnectionStream::Unix(UnixStream)（#[cfg(unix)]）、/Users/z/git/db/wedb/wedb/wnode/src/net/uds.rs 监听与治理、/Users/z/git/db/wedb/wedb/wnode/src/server.rs:442 ServerEndpoint::Unix 分派臂
- 端点识别口径现成一处：/Users/z/git/db/wedb/wedb/wnode/src/endpoint.rs:15-40 enum ServerEndpoint 与 parse（`unix:` 前缀、`/`、`./` 前缀、`.sock` 后缀判 UDS，其余按 TCP 补齐 `:port`）；wnode 与 wconn 是平级 crate（wconn/Cargo.toml 无 wnode 依赖、wnode/Cargo.toml 无 wconn 依赖），故该口径不可被 wconn 直接 use
- 门面透传口：/Users/z/git/db/wedb/wedb/wedb/src/client.rs:116-131 起（GarnetClient::connect_async 建 ConnClient 时透传 endpoint 字符串、超时与 TLS），本票不改其语义；集群 gossip 侧端点恒为 host:port（/Users/z/git/db/wedb/wedb/wedb/src/server/gossip/node_connection.rs:50 `format!("{}:{}", address, port)`），与 C# 一致，不在本票射程

对应 C#
- garnet/libs/client/GarnetClient.cs:262 ConnectAsync → :355-373 ConnectSendSocketAsync：`if (EndPoint is DnsEndPoint …)` 走 DNS 枚举 + Tcp；`else` 分支 `new Socket(EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified)` 且 `if (EndPoint is not UnixDomainSocketEndPoint) socket.NoDelay = true;` —— 即 UDS 端点直接按 AddressFamily 建流并跳过 NoDelay
- garnet/libs/client/ClientSession/GarnetClientSession.cs:278、:311 同一判定（会话层亦支持 UDS，NoDelay 仅对非 UDS 施加）
- garnet/libs/client/GarnetClient.cs:341-347（同步版 ConnectSendSocket）同口径，rust 无需另建同步口

修法
1. 端点识别单源：把 ServerEndpoint 的 UDS/TCP 判定规则抽到 wbase 新模块（`wbase::endpoint`，照 wbase 既有按特性切口形态，`endpoint = []` 空依赖特性即可，禁引入网络依赖），签名 `pub fn parse_socket_endpoint(s: &str) -> Option<PathBuf>`（Some = UDS 路径，None = TCP 地址串）或等价的 `enum SocketEndpoint`；wnode/src/endpoint.rs:27 parse 改为其上的薄封装（保持 ServerEndpoint 对外形态与 :72 既有断言不变），wconn 复用同一条规则——严禁在 wconn 内再写第二套前缀/后缀判定
2. wconn 出站分派：client.rs:133 connect_async 与 session.rs:84 connect_async 首步改为按 1 的判定分派——UDS 走 `compio::net::UnixStream::connect(path)`（整体 `#[cfg(unix)]` 门；非 unix 平台该分支编译期不存在，命中 UDS 端点即回 Error 明确文案），TCP 走现路径并保留 set_nodelay；UDS 分支不设 NoDelay（C# `is not UnixDomainSocketEndPoint` 同语义）
3. OutStream / ReadHalf / WriteHalf 各补 `#[cfg(unix)] Unix(UnixStream)` 臂（stream.rs:29、:56、:83），split 用 UnixStream 的 into_split 或 BiLock 对（按其实际 API 取零开销形态），read_owned / write / shutdown 三处 match 补臂；TLS 臂的底层流泛型若需扩为 `TlsStream<UnixStream>` 则再开一 UnixTls 变体，不强求（本仓集群内 TLS + UDS 组合在 C# 亦为可选路径，最低要求是明文 UDS 可用）
4. wconn 的 Cargo.toml 不改（compio 已带 net 特性，UnixStream 属同 crate）；如需新依赖一律 cargo add
5. 端点字符串来源不改协议面：客户端与会话层的 end_point 字段仍是 String（与 C# 的 EndPoint 形参同位），仅解析口径共享

验收判据
- 限定符号 wconn::client::GarnetClient::connect_async 与 wconn::session::GarnetClientSession::connect_async 体内不再出现无条件 `TcpStream::connect`（grep 该两函数体，UDS 分支必存）
- 限定符号 wconn::network::stream::OutStream 含 Unix 变体，ReadHalf / WriteHalf 同步（`grep -n "Unix(" wedb/wconn/src/network/stream.rs` 三处齐）
- 全仓 UDS 端点判定只有一处规则实现：限定符号 wbase::endpoint 的解析函数为唯一读者，wnode::ServerEndpoint::parse 与 wconn 两处客户端均经它；`grep -rn 'starts_with("/")\|strip_prefix("unix:")' wedb/` 命中数不高于改动前（不得新增第二套）
- 新增集成测试进 crate 的 tests 目录（不在 src 内写#[test]）：wconn 起一枚 UnixListener 假服务（tempdir 路径），经 GarnetClient 走 UDS 完成 AUTH/回环应答断言 + 经 GarnetClientSession 同测一条；对端为 UDS 时断言未施加 set_nodelay 的等价路径可达（按 API 可观测面判，不为断言而加生产接口）
- cargo check --workspace --all-features 与 cargo check --workspace 双态零警告；Windows/非 unix 目标不要求可编（本仓无该平台门禁），但 cfg 门须与 wnode/src/net/stream.rs 的既有 `#[cfg(unix)]` 写法同形

互斥与边界
- 本票文件域：wconn/src/client.rs、wconn/src/session.rs、wconn/src/network/stream.rs、wnode/src/endpoint.rs、wbase/src/（新 endpoint 模块）+ wbase/src/lib.rs 的 mod 与 feature 登记；不碰 wtxn/wkv/wnode/src/resp/objects 的 rmw 路径，亦不碰 wnode/src/tls/config.rs 与 wconn/src/tls.rs（那是 task/ing/tls-pem-loader-single-source.md 的域）
- 经查 /tmp/fork 仅 dev-2026-09-19（garnet C# 快照）、`git worktree list` 仅主仓 [dev]、`git branch --list` 仅 dev/main：无任何在途修复分支，「勿与在跑 rmw 票冲突」属僵尸声称，照常开工，仅需避开上面点名的同域文件
- 不做向下兼容与不做优化：不引入 UDS 自动重连、不做 DNS 多地址轮询之外的额外策略（C# ConnectSendSocketAsync 的 DNS 枚举臂若现缺，另计功能项，不在本票偷偷扩面）
