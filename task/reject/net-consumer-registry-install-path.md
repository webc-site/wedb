裁决：不成立（假设路径不存在：生产启动唯一经 bootstrap assemble → StorageSessionProvider 构造口全家汇 from_parts 统一 install_global；「直接构造 GarnetServer」仅测试 NullProvider 形态）
来源：next/agy.net.md 条 18 + next/muse.net.md 条 10（两档同题）。核销 2026-09-19。

一句话结论：install_global 的唯一生产调用在 StorageSessionProvider::from_parts（service.rs:1021），
而该 struct 对外全部构造口（open_with_config / open_with_config_and_aof / open_recovered_with_config_and_aof /
open_from_args）无一绕过 from_parts；GarnetServer::new 的直装调用只有 cfg(test) 里两处 NullProvider
（server.rs:1135/:1154，位于 :1096 #[cfg(test)] 域内），不消费 CLIENT 命令面。

逐条核销
1. 生产启动链实测：wedb/wnode/src/server.rs:229 `assemble(args, cluster)` → :236 GarnetServer::new——
   assemble 由生产入口注入，最终走 StorageSessionProvider 构造口；构造口全家汇
   wedb/wnode/src/service.rs:1013 from_parts，:1020-1021 `let registry = Arc::new(ConsumerRegistry::new());
   registry.install_global();`。from_parts 三个调用点 :1001/:1213/:1271 均为 open_* 系列公共口。
2. 绕过路径封死：StorageSessionProvider 含私有字段（lock_table: TxnLockTable 等，service.rs:923-927），
   外部 crate 无法字面量构造绕过 from_parts；仓内亦无第二 install_global 调用
   （server.rs:872 是 monitor.install_global，非 registry）。
3. 消费面安全：wedb/wnode/src/resp/client_commands.rs:48/:139/:463 的 ConsumerRegistry::global() 只在
   真实 provider 会话可达；NullProvider 测试形态（server.rs:1100 域内）无 CLIENT 命令面。
4. 与 C# 差异声明：C# GarnetServerBase.activeHandlers 是实例直持不靠进程级单例；rust 采用 registry +
   进程级安装是本仓已声明的装配形态，两档意见也仅要求「收敛到装配单点」，而该单点已存在。
