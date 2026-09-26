登记档（主代理席，2026-09-26 09:5x，opt-r201-server-core 复核席裁定「可入 dev 附留档」，合入 8ad04328）。本档为已接受项登记，非待执行任务。

## 事项一：TLS 早退臂「容量守卫归零」与「半开 socket 关闭」相对次序翻转

- 实位：`wedb/wnode/src/server.rs:1309`、`:1316`、`:1320`（`serve_connection` 内三处显式 `drop(guard)`）。
- base 形：TLS 握手失败三早退臂的释放序为 `shaken/handshake`（持半开 socket）→ 容量守卫 → handler。
- 现状形：显式 `drop(guard)` 把在途连接容量回落提到局部 socket drop 之前，即 guard 与 fd 关闭相对次序翻转；「守卫先于 handler 释放」这一原注释约束未破。
- 定裁：接受。理由——守卫只涉及 `conn_limit` 计数的提前一格归还（方向是缩短占用，不是延长），窗口为若干条指令，不存在丢更新或死锁面；`handshake_timeout` 专测（正压 TLS 早退臂配额释放）1/1 通过，`node_test` 的 connection_limit 通过。
- 后席注意：**不得**为「复原 base 次序」而回退这三处 `drop(guard)`；若未来把容量门改为跨域计数器，先复核本档。

## 事项二：`start()` 失败臂 `worker_threads` 锁域收窄

- 实位：`wedb/wnode/src/server.rs:541` `reclaim_workers`（5 个失败臂调用点 591/622/645/654/681）。
- 变化：base 持 `worker_threads` 锁跨 `tcp_addrs.clear()`；现形 join 后即释放锁再做 clear。锁域只减不增，无新死锁面，join 全为正序（与 base `for h in handles` 一致）。
- 定裁：接受，无需动作。`server_lifecycle` 4/4、`node_test` uds_lifecycle/stop_drains 通过。

## 本票其余收口结构（供后续席参考，勿重复劳动）

- 三径启动骨架（TCP 首发核 / TCP 其余核 / UDS）→ `spawn_accept_worker`（`server.rs:1111`，前奏与主体闭包注入）；UDS 循环外提为 `run_uds_accept_loop`（`:1384`，~115 行搬迁非复制），`start_unix_worker` 由 ~160 行降至 23 行。
- `ready_rx.recv()` 断链兜底 3 处 → `recv_ready`；三处兜底文案（`Worker-0 初始化失败` / `Worker 初始化失败` / `UDS Worker 初始化失败`）逐字保留，入参传递，禁改写。
- `TcpAcceptContext` 已随 UDS 复用 `capture()` 更名 `AcceptContext`（`:1158`）；代码内引用零悬空，唯一残名在历史评审档案 `task/review_history/zcode-r14-conn.md:14`（纪实体，不改）。
- 前席已判定不硬凑、本席复述以免重复立案：TCP/UDS accept 循环头尾再并需自定义 listener trait 且两处 `Cancelled` 文案不同形；Phase 2 排空 4 行两循环尾部同形（合并约 ±0）；`run_async` 的 `ensure_dir` 三处同形（抽函数仅净 −1）；5 个 `with_*` setter 同形需宏才划算。
