wnode::service::open_node 零生产消费者缺省装配口：删，收敛为 StorageSessionProvider::open 单入口

来源：next/wnode-service-split.md（该「纯移动分文件」票判否，见 task/reject/wnode-service-split.md）
分拣复核取证时暴露的真实死面，独立立项。
取证基线：主仓 /Users/z/git/db/wedb，dev HEAD 90201c15。

现状
- 定义：/Users/z/git/db/wedb/wedb/wnode/src/service.rs:749-755，:749-752 文档注释，
  :753 `pub fn open_node(data_path: impl AsRef<Path>) -> crate::Result<DefaultNodeHandles>`，
  :754 函数体一行 `open_node_with_config(store_config(), data_path)`。
- 零消费者，三种口径实测全为 0：全仓 `use` 引入本口名（非 with_config 变体）0 命中；
  限定调用 `service::open_node` / `wnode::open_node` 0 命中；裸调用 `open_node(` 的命中
  全部是 tests/ 内各自本地同名 helper 的定义与调用
  （/Users/z/git/db/wedb/wedb/wedb/tests/diskless_sync_anchor_window.rs:133、
  wedb/tests/checkpoint_import.rs:63、wedb/tests/diskless_loop_convergence.rs:52、
  wedb/tests/diskless_sync_ri_vector.rs:69、wnode/tests/service.rs:33、
  wnode/tests/tiered_promote_aof_replay.rs:38、wnode/tests/aof_domain.rs:40），
  其签名 `fn open_node(tag: &str)` / `(name, ring)` 与本口单参 `impl AsRef<Path>` 不同名同实。
- 未被 crate 顶层再导出：/Users/z/git/db/wedb/wedb/wnode/src/lib.rs:60 只
  `pub use service::open_node_with_config;`，本口连再导出都没有。
- 在册死面普查未含本口：task/ing/zero-consumer-surfaces-batch-two.md、
  task/ing/zero-consumer-dead-surfaces-batch-five.md 与 next 侧批六台账均无 service.rs 命中。
  普查口径把 tests/ 计入消费侧索引，同名本地 helper 把本口掩盖了（批六自陈盲区即「同名」），
  故本票补登记，后续批次据此排除，勿再判为「新出」。
- 重复入口事实：本口与同文件 :966-968 `StorageSessionProvider::open`
  （体为 `Self::open_with_config(store_config(), data_path, decorate)`）
  是同一「按缺省配置打开单文件节点」的两份声明，前者吐裸三件套 DefaultNodeHandles
  （:640 类型别名），后者吐可挂服务的 provider 基座。生产与嵌入式装配入口实为
  ServerBootstrap（/Users/z/git/db/wedb/wedb/wnode/src/server.rs:15 模块头声明为服务端唯一入口）
  加 StorageSessionProvider::open*，wedb/src/server/boot.rs 与 wedb_standalone/src/main.rs
  均不经本口。

C# 对位
- /Users/z/git/db/wedb/garnet/libs/host/GarnetServer.cs 的宿主装配入口只有两枚构造器：
  :81 `GarnetServer(string[] commandLineArgs, ...)`、:149 `GarnetServer(GarnetServerOptions opts, ...)`，
  分别在 :139 / :160 调私有 :170 `InitializeServer`，启动面 :527 `Start`。
  C# 没有任何「自由函数式节点三件套打开口」，本口是 rust 侧自造的便利面。
- 该两枚 C# 成员在 rust 的真挂载位已在位且不依赖本口：
  /Users/z/git/db/wedb/wedb/wnode/src/server.rs:474-476 的
  `libs/host/GarnetServer.cs:Start` 与 `:InitializeServer`（挂在 ServerBootstrap::start）。
  service.rs:752 的「对标 C# GarnetServer InitializeServer」是散文提及、非路径锚点位，
  删本口不丢 check.js 对位面。

目标形态
- 整删 service.rs:749-755（含其文档注释）。
- 不新增替代口，不改 open_node_with_config（:758）的签名与可见性，不改 store_config（:650）。
- 「缺省配置装配」此后只剩一处声明：StorageSessionProvider::open（:966-968）；
  小预算配置注入走 open_node_with_config / open_with_config。

处置取向（不采「接线」）
- 为死口反向改装配链（让 ServerBootstrap / wedb_standalone 改走 open_node）会新加一层与
  provider 重复的三件套出口，与「一处定义」相反，也非 C# 形态，故本票只裁删。

门禁
- 无先行依赖：task/ing/boot-assembly-projection-single-source.md 射程是 boot.rs / main.rs /
  server.rs run_node 的三字段投影，不经本口；原 next/wnode-service-split.md 纯搬票已判否，
  不存在与本票的同文件逐块移动冲突。
- 同文件在途票（task/ing/session-metrics-option-dead-track.md、
  task/ing/reviv-knobs-zero-production-wiring.md、
  task/ing/primary-checkpoint-cluster-callback.md）均不落 :749-755；
  开工前按符号名重取行号，不按本档行号（service.rs 现 1816 行，已多次漂移）。

验收
- 仅 `cargo check --workspace --all-targets`（私有 target 目录）零 error 零 warning；
  不得以 #[allow] 或降级为 pub(crate) 留悬口。
- `grep -rn "\bopen_node\b" wedb --include=*.rs` 命中只剩 open_node_with_config
  （定义 :758、lib.rs:60 再导出、各消费点）与 tests/ 本地 helper；
  wnode/src/service.rs 内不再存在该自由函数。
- 两条陈旧措辞注释随删口校正：wnode/tests/recover_test.rs:481 与
  wnode/tests/node_test.rs:328 的「生产缺省 open_node 走 StoreConfig::auto」改指
  store_config() / StorageSessionProvider::open，不留悬空引用
  （只改散文说明，不涉任何 `路径.cs:符号` 锚点，禁借机动 check.js 判读口径）。
- ./js/check.js 缺失与重复组数不增；test.sh 与 ./sh/clippy.sh 由中央整合轮执行。

坑与边界
- DefaultNodeHandles（:640）仍作 open_node_with_config（:758-:766）的返回类型，勿连带删；
  node_components（:723）另在 :1179 与 :1216 消费，store_config（:650）另在 :710、:967、
  :1164 与 cfg(test) 段消费，均不随本口消失。
- 本票只裁这一口。service.rs 其余 pub 面（open_node_with_config、spawn_* 四口、
  StoreSwapSlot、StorageSessionProvider、assemble_lua_timeout）实测各有消费者，
  分属在册票射程，勿顺手收口或改名。
- tests/ 内的同名本地 helper 与本口无关，勿改勿并（它们是各测试自备的建库夹具）。
- 若整合轮复核后判定该口须作对外文档 API 保留：全仓 md/yml 除 next/ 与 task/ 之外
  `grep -rn open_node` 实测 0 引用，保留结论需给出新证据，否则按删口落地，不留中间态。
