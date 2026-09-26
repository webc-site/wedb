审核结论：拒绝（审核席 zcode-r22-review-cargo，2026-09-26）

拒绝理由：核心断言「windows 构建必炸」经实证为虚假缺陷，连带「平台分派臂全部死码」「signal feature 零使用」两条论据均不成立，触红线第 1 条（凭空臆测与虚假缺陷）。反证如下：

1. nix 0.31.3 在 windows target 编译为空 crate，构建不炸。nix 源码 src/lib.rs:45 有 crate 级门 #![cfg(unix)]，非 unix 平台整个 crate 编译为空；其 [dependencies] 仅 bitflags/cfg-if/libc/memoffset/pin-utils，libc 在 windows 上可正常编译。实证：临时最小 crate 声明 nix = { version = "=0.31.3", features = ["signal", "fs"] } 后 cargo check --target x86_64-pc-windows-msvc 直接通过（Finished dev profile）。cargo tree --target x86_64-pc-windows-msvc 图中 nix 在列仅说明会被编译为空 crate（附带 libc 传递编译的构建时间浪费），非构建失败。

2. CI 全矩阵含 windows 且跑同一 workspace。.github/workflows/rust-test.yml:33 矩阵含 windows-latest，wedb.test.yml 以 dir: wedb 调用该可复用工作流，在 windows-latest 上执行 cargo nextest run --all-features（working-directory: wedb）。若 nix 真致构建必炸，CI 自始必红，与「源码平台分派臂完备且仓库活跃开发」的事实矛盾。

3. 平台分派臂非死码，windows 上正常可达。wnode/src/datadir_lock.rs:36-37/#:41-44 的 use nix::fcntl::Flock 与 type HeldLock 全部裹 #[cfg(unix)]，:135-142 的 #[cfg(not(unix))] 告警降级臂与 wnode/src/signal.rs:56-62 的 #[cfg(windows)] ctrl_c 臂在 windows 上正常编译执行，分派闭环完好。

4. 「signal feature 零使用」为假，按票面方案执行反而会炸。wnode/tests/signal_default_disposition_restore.rs:30-33 直接消费 use nix::{sys::signal::{SigHandler, Signal, kill, signal}, unistd::Pid}（signal feature 含 process，unistd::Pid 由其开启）；wedb_standalone/tests/shutdown_second_signal.rs:36-38 同形消费。票面方案第 1 步「删除零使用的 signal feature」一旦执行，上述两个集成测试当场编译失败。

5. 残余事实仅观感级：无 target 门使 windows 构建图携带空 nix crate 与 libc 传递编译，属构建时间噪音，零功能影响，不构成缺陷，不达立案门槛。

附注：票面 Cargo.toml 行号引用漂移（wnode 实为 :71 非票面 :1226，wedb_standalone 实为 :32 非票面 :658），事实定位虽可对上，但反映起草未亲验行号。

wnode 与 wedb_standalone 的 nix 依赖缺 cfg(unix) target 门，windows 构建必炸且源码平台分派臂全部沦为死码

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# garnet 系 .NET 跨平台工程（linux/windows 同源构建运行），仓根单一 MIT LICENSE 无平台绑定；关停信号注册走跨平台 API（Console.CancelKeyPress，garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/EntryPoint.cs）。多实例互斥职责在 rust 侧由存储目录排他锁 flock 承担，系 deviations.md §77 已登记的法定机制。deviations.md §56 确立跨平台纯净性规范：清退 Windows 证书存储专有集成的方向是删专有绑定，而非转投 unix 专有绑定。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wnode 源码平台分派完备：signal.rs 存 #[cfg(windows)] ctrl_c 臂（wnode/src/signal.rs:56）；datadir_lock.rs 存 #[cfg(not(unix))] 无 flock 降级告警臂（wnode/src/datadir_lock.rs:43 与 :135-143，注释明示「非 unix 平台无 flock 能力、仅持 fd 留痕」）；nix 全部使用点（use nix::fcntl::Flock，datadir_lock.rs:37 与 :117）均在 #[cfg(unix)] 门内。但依赖声明 nix = { version = "0.31.3", features = ["signal", "fs"] }（wedb/wnode/Cargo.toml:1226）落在无 target 门的 [dependencies]；wedb_standalone 的 dev 依赖 nix = { version = "0.31.3", features = ["signal"] }（wedb/wedb_standalone/Cargo.toml:658）同样无门。实测 cargo tree --target x86_64-pc-windows-msvc -p wnode 图中 nix v0.31.3 仍在。nix crate 仅支持 unix，windows 构建在编译 nix 本身即失败，signal.rs 的 windows 臂与 datadir_lock.rs 的 not(unix) 臂永不可编译。附带：wnode 侧 nix 的 "signal" feature 零使用（信号走 compio::signal::unix::signal，wnode/src/signal.rs:9），仅 "fs" 面（Flock）真实消费。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
源码明写非 unix 降级为「仅告警不锁、多实例同目录并发双写不设防」（datadir_lock.rs:135-143），但该臂因 Cargo 依赖门缺位在 windows 上永不可达，平台分派成纸面死码；违反 review.md 板块1 编译隔离（crate 编译边界完整）与零死代码清退（专有平台绑定清退），与 deviations §56 跨平台纯净规范直接冲突。信号面同理：signal.rs windows 臂承诺 ctrl_c 承接 SIGINT，实际不可编译，平滑关停面在 windows 全断。

涉及代码：
rust 文件与函数：
wedb/wnode/Cargo.toml:1226（[dependencies] nix，无 target 门，含零使用的 "signal" feature）
wedb/wedb_standalone/Cargo.toml:658（[dev-dependencies] nix，无 target 门）
wedb/wnode/src/datadir_lock.rs:lock_exclusive（:37 use nix::fcntl::Flock；:43 not(unix) HeldLock=File；:117-129 Flock::lock；:135-143 告警降级臂）
wedb/wnode/src/signal.rs:wait_shutdown_signal（:8 cfg(unix) unix 信号臂；:56 cfg(windows) ctrl_c 臂）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/EntryPoint.cs:Main（Console.CancelKeyPress 跨平台关停信号注册，.NET 同源跨平台形态参照）

精炼执行方案：
1. wedb/wnode/Cargo.toml 将 nix 自 [dependencies] 移入 [target.'cfg(unix)'.dependencies]，并删除零使用的 "signal" feature（仅留 "fs"）
2. wedb/wedb_standalone/Cargo.toml 将 dev 依赖 nix 移入 [target.'cfg(unix)'.dev-dependencies]（tests/shutdown_second_signal.rs 本系 unix 信号测试，随门收编）
3. 测试验证点：cargo tree --target x86_64-pc-windows-msvc -p wnode 图中 nix 消失；常规门禁（nextest --all-features）不回归；wnode/src/datadir_lock.rs 与 signal.rs 零源码改动
