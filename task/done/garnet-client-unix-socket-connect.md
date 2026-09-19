优先级：功能缺口（服务端已支持 UDS 监听、客户端不支持，本机进程间通信链路断一半）

一句话：wconn 的 GarnetClient 与 GarnetClientSession 都把 TcpStream::connect 写死，出站无 Unix 域套接字臂；C# 两个客户端类都按 EndPoint 类型分派 TCP/UDS，属未转写缺口。

复核追加（2026-09-19 HEAD be2fca2）：并发会话另立同题票 /Users/z/git/db/wedb/next/net-client-uds-stream.md（其取证亦含 C# GarnetClient.cs:345 的 `EndPoint is not UnixDomainSocketEndPoint` NoDelay 门与 wconn/src/tls.rs:98 ClientTlsConfig::connect 的 TcpStream 硬绑，与本票同向）。同一批文件两份票，主代理须二选一收编、勿双跑；本票多出的两点是端点形态判定单源化（`wbase::endpoint` 单规则，wnode::ServerEndpoint::parse 改薄封装，禁 wconn 内第二套前缀/后缀判定）与跨票文件互斥边界——若收编 next 票，这两点须并入其修法。

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
- 在途实测更正（2026-09-19 现刻，本条作废原「无任何在途修复分支、属僵尸声称」的说法）：`git worktree list` 实得 11 个 fork（docs-data-comment-batch、docs-readme-crate-map、fix-custom-obj-multikey、fix-lua-pending-handoff、fix-pending-lat-mget、fix-reviv-crtt-gate、fix-rmw-atomic-window、fix-vector-registry-two、split-cluster-provider、windex-2pl-removal，另 /tmp/fork/dev-2026-09-19 为 garnet C# 快照）。逐个 `git diff --name-only dev...<br>` 比对：无 fork 触及本票文件域（wconn/src/client.rs、wconn/src/session.rs、wconn/src/network/stream.rs、wnode/src/endpoint.rs、wbase/src/ 新模块）——注意 fix-lua-pending-handoff 动的是 wnode 侧 net/handler/drive.rs 与 resp_*，与本票的 wconn 出站面不同 crate，不交叠；fix-rmw-atomic-window 在 wtxn/wkv/wnode/src/resp/objects 的 rmw 路径上跑，本票零接触
- 不做向下兼容与不做优化：不引入 UDS 自动重连、不做 DNS 多地址轮询之外的额外策略（C# ConnectSendSocketAsync 的 DNS 枚举臂若现缺，另计功能项，不在本票偷偷扩面）

判词（2026-09-19 回合，裁决：成立，非拒）
- C# 行实（现刻 /Users/z/git/db/wedb/garnet 快照逐行复核）：
  - garnet/libs/client/GarnetClient.cs:359 `if (EndPoint is DnsEndPoint dnsEndpoint)` → :378-379 `if (EndPoint is not UnixDomainSocketEndPoint) socket.NoDelay = true;`（异步 ConnectSendSocketAsync）；同步版同口 :326 / :345-346
  - garnet/libs/client/ClientSession/GarnetClientSession.cs:292 / :311-312 与 :259 / :278-279 同型两态
  - 即：两个客户端类的两个连接入口全部按 EndPoint 形态分派，UDS 端点连流且不施 NoDelay——rust 侧写死 TCP 属未转写缺口，非微软平台绑定（Socket.AddressFamily/DnsEndPoint 的枚举臂是 .NET BCL 形态，但「UDS 直连」本身是纯 OS 能力，compio::net::UnixStream 对位在案）
- 本仓入站 UDS 已在位（wnode/src/net/stream.rs 的 ConnectionStream::Unix + net/uds.rs + server.rs 的 ServerEndpoint::Unix 臂），出站缺臂，链路确断一半，判据成立
- 票面四处更正（落地时实测）：
  1. 「复核追加」条所指并发同题票 /Users/z/git/db/wedb/next/net-client-uds-stream.md 现已不存在（next/ 无该文件），二选一收编的前提消失，按本票单跑；本票多出的两点（端点判定单源化 + 跨票文件互斥边界）已随本票一并落地
  2. 「在途实测更正」条枚举的 11 个 fork 已过期：现刻 `git worktree list` 另得一批（见 /tmp/fork/ 清单），逐个与本票文件域比对后仍零交叠，尤其 wconn/、wbase/src/、wnode/src/endpoint.rs、js/check/ 全域；tls-pem-loader 票域（wconn/src/tls.rs、wnode/src/tls/config.rs）未触碰，故在途清单须由主代理每次重取，勿引用本票快照
  3. 修法 1 建议签名 `parse_socket_endpoint(s: &str) -> Option<PathBuf>`；落地签名为借用形态 `wbase::endpoint::uds_path(s: &str) -> Option<&Path>`（Some = UDS 路径、None = TCP 地址串，语义等价，热路径零分配；调用方需要拥有权时自行 to_path_buf）。同步改名，不再保留建议名
  4. C# ConnectSendSocket(Async) 的 `DnsEndPoint` 多地址枚举臂（GarnetClient.cs:359-373）本仓现无对应实现，按票面约定另计功能项，本票不扩面

落地注记（分支 client-unix-socket，主仓 dev 已 FF 合入）
- 提交面：功能提交 8a58fe1（11 files, +318/-33），其后三轮「merge 最新 dev 以保持可 FF」为 e656588 / caca22f / 3f6b8ae；合入后 `git rev-list --count dev..client-unix-socket` = 0
- 改动清单（绝对路径，主仓）：
  - 新建 /Users/z/git/db/wedb/wedb/wbase/src/endpoint.rs —— UDS/TCP 形态判定唯一实现 `uds_path`（`unix:` 前缀、`/`、`./` 前缀、`.sock` 后缀，含空白归一）；/Users/z/git/db/wedb/wedb/wbase/src/lib.rs 加 `#[cfg(feature = "endpoint")] pub mod endpoint;`，/Users/z/git/db/wedb/wedb/wbase/Cargo.toml 加 `endpoint = []`（零依赖空特性，未引入网络依赖，合票面要求）
  - /Users/z/git/db/wedb/wedb/wconn/src/network/stream.rs —— OutStream / ReadHalf / WriteHalf 三枚举各加 `#[cfg(unix)] Unix(UnixStream)`（该文件 `Unix(` 命中 10 处 = 三变体 + split/read_owned/write_all/shutdown 各臂）；新增 `connect(endpoint)` 统一分派（UDS → `UnixStream::connect`，TCP → `TcpStream::connect` + set_nodelay，UDS 臂不施 NoDelay，与 C# `is not UnixDomainSocketEndPoint` 同语义）；`with_tls` 收拢为方法（把 client/session 各 4 行 cfg 双态收敛成 1 行调用），命中 TLS×UDS 明确回 `ErrorKind::Unsupported`，不静默降级明文
  - /Users/z/git/db/wedb/wedb/wconn/src/client.rs:136、/Users/z/git/db/wedb/wedb/wconn/src/session.rs:87 —— 首步改 `OutStream::connect(&self.end_point)`，删 `net::TcpStream` 直连与导入；end_point 仍 String，协议面/形参位不变
  - /Users/z/git/db/wedb/wedb/wnode/src/endpoint.rs —— `ServerEndpoint::parse` 的 UDS 判定改薄封装经 `wbase::endpoint::uds_path`，对外形态与 :72 既有断言不动；wnode/Cargo.toml 的 wbase features 加 `"endpoint"`
  - 测试：新建 /Users/z/git/db/wedb/wedb/wconn/tests/client_uds.rs（133 行 3 枚，compio UnixListener + tempfile 假服务：`unix:` 前缀端点走 GarnetClient 完成 AUTH 握手 + PING 回环；裸 `.sock` 路径走 GarnetClientSession 回环；缺失套接字文件断言 `Error::Io` + `ErrorKind::NotFound`，以两臂互斥证 NoDelay 只可能落 TCP 臂——未为断言新增生产接口）；/Users/z/git/db/wedb/wedb/wbase/tests/main.rs 追加 `test_endpoint_uds_path_rule` 覆盖判定全分支
- 判据实测（现刻 HEAD grep）：
  - `git grep -n "OutStream::connect|TcpStream::connect" HEAD -- wedb/wconn/src/{client,session}.rs` → 仅两处 `OutStream::connect`，无裸 TCP
  - `git grep -c "Unix(" HEAD -- wedb/wconn/src/network/stream.rs` → 10
  - `git grep -n 'starts_with("/")|strip_prefix("unix:")' HEAD -- wedb/ | wc -l` → 0（改动前为 2，判定规则已收敛为 wbase 单源）
- 门禁（私有 CARGO_TARGET_DIR=/tmp/ct-cus，未跑主仓 test.sh / clippy.sh）：`cargo check --workspace --all-targets` 与 `--all-features` 双态 0 warning / 0 error；`cargo test -p wconn --features tls` → 21+2+3+2 全绿，无 tls 态 → 20+2+3+2 全绿；`cargo test -p wbase --features endpoint` → 1 passed；`cargo test -p wnode --features tls --lib endpoint` → 3 passed；`cargo test -p wnode --test node_test uds` → 2 passed；`cargo fmt -p wbase -p wconn -p wnode --check` 无差异；`bun js/check.js` 于 worktree 与 dev 基点对照树（/tmp/gate-cus-base）前后对跑，stdout 与 ignore 语料回写逐字节相同（锚点不增不减——新码一律散文指认 C# 臂、不落 `X.cs:Symbol` 锚；基线段重复定义 36 行、miss 产物 14 枚），跑后 `git checkout -- js/check/ignore/` 还原
- 需知会两处工序偏差：① tempfile 按同侪 crate 写法手写 `tempfile.workspace = true`（本机 nightly cargo 1.100 的 `cargo add` 不认 `workspace` 版本形态），非新增外部依赖，workspace 已有该成员；② `cargo fmt --all` 会波及他人未归一文件（wkv/wlua/wnode 数处），故本票只在自家 3 crate 范围内跑 fmt

遗留知会
- TLS over UDS 未接线（`ClientTlsConfig::connect` 仍具体绑 TcpStream）：属 task/ing/tls-pem-loader-single-source.md 域，本票以明确 `ErrorKind::Unsupported` 报错而非降级，票面「最低要求是明文 UDS 可用」已满足；两票若合并推进，该臂可直接扩
- 非 unix 目标：UDS 臂 `#[cfg(not(unix))]` 回 `ErrorKind::Unsupported` 明确文案（写法与 wnode/src/net/stream.rs 的 `#[cfg(unix)]` 同形），本仓无 Windows 门禁，未实编验证
- C# `DnsEndPoint` 多地址枚举臂（GarnetClient.cs:359-373）本仓仍无对应实现，另计功能项
