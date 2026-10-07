# wepoch : LightEpoch 纪元保护

## 项目介绍

wepoch 提供 Garnet Tsavorite 风格的无锁纪元保护：管理并发参与者的纪元生命周期，判定"何时可以安全回收内存"（SMR），并提供结构化的挂起/排空协调原语。

参与者的 Entry 与管理器本体均为 64 字节缓存行对齐，杜绝伪共享；布局由编译期 const assert 钉死。

## 模块组成

- `entry`：`EpochEntry`，64B 缓存行对齐的纪元条目（8B epoch + 8B thread_id + 4B reentrant + 1B reserved + 3B 填充 = 24B 载荷，独占 64B 缓存行）
- `epoch`：`LightEpoch` 管理器、`Participant`、RAII 守卫、TLS 本线程状态
- `error`：错误类型

## 核心 API

- `LightEpoch`：`register`（→ `Result<Participant>`）/ `resume` / `suspend` / `bump_current_epoch` / `bump_current_epoch_action[_relaxed]` / `compute_safe_to_reclaim_epoch` / `is_safe_to_reclaim` / `drain` / `bump_and_wait` / `wait_condition_sync/async` / `thread_protected`（另有 `try_suspend` / `protect_and_drain` / `safe_to_reclaim_epoch` / `entry_count` 等 debug 门控诊断面）
- `Participant`：`enter` / `refresh` / `exit`
- `EpochSuspendGuard`：RAII 挂起深度守卫（`wait_condition_*` 配套），释放按原深度恢复
- `EpochGuard`：RAII 进入保护，Drop 自动 exit
- `ProtectedScope`：RAII 挂起保护（`!Send + !Sync`，严禁跨线程转移）
- `EpochEntry`：参与者条目（独占 64B 缓存行）
- `DRAIN_LIST_SIZE = 16`：drain 动作列表容量

## 设计要点

- 伪共享消除：`EpochEntry` 独占 64B 缓存行；`try_reserve` / `try_claim` 跨 reserved / epoch 两变量 Dekker 式互斥，必须 SeqCst
- 退避策略：自旋 32 轮 → yield 1024 轮 → 50μs 睡眠
- TLS 管理：内联 4 槽 + overflow 16 上限；线程退出 Drop 兜底释放槽位并补 SeqCst fence
- TLS 作用域槽位与线程绑定，跨线程转移受类型系统拒绝（`ProtectedScope` 为 `!Send + !Sync`）；`Participant` 句柄可跨线程转移，enter 时绑定当时线程

## 测试覆盖

覆盖：缓存行对齐、参与者容量上限与槽位复用、refresh 机制、长期守卫下 drain list 满载不活锁、线程退出 TLS 兜底回收、瞬态实例零泄漏；protection（suspend / resume / 嵌套作用域 / safe epoch 单调）、drain（动作恰一次、按纪元序、级联触发）、并发竞争与多实例隔离。
