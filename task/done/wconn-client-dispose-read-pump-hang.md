# wconn-client-dispose-read-pump-hang（P1，源自 task/issue/wconn-client-dispose-read-pump-hang-silent-peer-leak.md）

## 甄别结论：通过（2026-09-28 主控现码复验）
C# 契约亲验：GarnetClient.cs:521-532 Dispose(bool) 无条件拆连——
timeoutCheckerCts?.Cancel() + socket?.Dispose()（双向关闭，挂起 receive 即完成），
与对端是否配合无关；三条回收链（GarnetServerNode.cs:116-134 gc?.Dispose()、
Gossip.cs:223-228 catch 中 Dispose、GarnetClusterConnectionStore.cs:214 起摘除即
Dispose）全落该契约。
rust 现状亲验：facade wedb/wedb/src/client.rs:589 dispose 仅 inner 置 None（:125），
wconn 侧 GarnetClient（wedb/wconn/src/client.rs）无 Drop 实现、无显式拆连面；
network/stream.rs 仅有写半 Shutdown::Write；pump.rs 读泵竞速仅
progress.is_some_and(is_timed_out) 唤醒源（:274/:329），progress 只在
timeout_millis>0 创建（client.rs:148-157 形）。timeout 旋钮关闭态
（cluster-node-timeout 0，node_connection.rs/flags.rs/runtime_server_config.rs
链都在位）下静默对端使被 dispose 连接的读泵 task、读半 fd、池借出缓冲永久滞留，
gossip 主循环每轮净漏一套直至 EMFILE。

## 方案
对齐 C#「dispose 即拆 fd」契约，单套机制：wconn GarnetClient 增 dispose 面
（AtomicBool 幂等；双向 shutdown 拆 fd 令常驻 read 以 EOF/err 落定唤醒读泵自然
收场；pending 完成侧按既有断连错误形态收场），facade dispose 与最终 Drop 均经
该面；timeout 旋钮回归纯「在途命令超时判成」语义，不再充当读泵唯一唤醒源。
禁新增通道/任务仲裁等第二套机制，优先 compio shutdown 天然语义，避免 abort。

## 验证
wconn 集成测试（tests/ 下）：对端 accept 后静默不发的半开连接，timeout 关闭态下
调用 dispose 后读泵任务即时收场（可观测：任务退出/计数回落）、池缓冲归还、fd 释放；
正对照正常连接 dispose 零回归；gossip 链复用既有 node_test 夹具加一条 dispose 回收
断言。子代理只跑 cargo check 与相关专测，门禁归主控。

## 收口记录（2026-09-28 主控）
- 席：`wconn-dispose-pump`（起点 51c9c9ae，tip 007eea65）→ dev 合并 **779343c1**（`--no-ff`）。
- 收口形态：`OutStream::connect` 同点（TLS 包裹前）返回 `(Self, DisposeHandle)`，
  `DisposeHandle` 为底层 socket fd 的 **dup 自持副本**（unix `OwnedFd`/windows `OwnedSocket`，
  `try_clone_to_owned`，零 unsafe）——compio 流本体经 `SharedFd`（Rc）非 Send/Sync，
  持流克隆会令门面 `Arc<GarnetClient>` 失格（席实测 E0277 级联），故取 dup 面；
  `shutdown_both()` 经 socket2 `SockRef` 同步下发 `shutdown(2, SHUT_RDWR)`
  （compio-net 0.12.5 异步 shutdown 恒 `Shutdown::Write` 单向，无 Both 形制，已核实）；
  `disposed: Arc<AtomicBool>` 幂等位同时承 `is_connected` 翻假、`reconnect_async`
  先拆旧连、`network_loop` 收场分类（仅分类读侧，不作唤醒源，progress 语义保持纯超时判成）；
  `Drop for GarnetClient` 转调同一 `dispose()` 兜底；门面
  `wedb/src/client.rs:dispose` 由「只摘引用」改「摘链 + 锁外转调拆连面」。
- 依赖：`socket2 = "0.6.5"` 经 `cargo add` 入 wconn（与 wnode 既有同版本直依同形，非工作区重复版本）。
- 测试：新增 `wedb/wconn/tests/dispose_reclaim.rs` 五例（半开 dispose 收场 /
  最后 Arc 落下 / 在途拆连结算两态分类锁 / 正常往返零回归），判据取
  池借出计数 borrowed→0 与 free==1（泄漏窗直测），非墙钟断言。
  主控另笔去假 RESP 端点臂内空转语句（779343c1 后补）。
- 主控独立复核：`cargo check -q --workspace --all-targets` EXIT 0、
  `cargo nextest run -p wconn` **40/40 PASS**、`cargo clippy -q -p wconn --all-targets` EXIT 0
  （均在席暖 target 上重算，非采信自报）。
