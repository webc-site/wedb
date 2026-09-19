启动装配参数投影单点化：NodeArgs 三字段样板在 boot.rs 与 main.rs 双抄

来源：qcode 第 8 轮 design 条 3（MED，台账 next/qcode.rounds.md 本轮清账删除）。按主仓 HEAD 复核：
原报「三份手抄」已消一份，残两处样板 + 一处异形态，判定成立且待做（范围收窄）。
取证基线：主仓 /Users/z/git/db/wedb，dev HEAD 0671aca5。

现状
- wedb/wedb/src/server/boot.rs:29-42：先取 node.metrics_sampling_frequency_secs / latency_monitor /
  commandstats_monitor 三局部量，再 ServerBootstrap::new(args).with_cluster_provider(..)
  .metrics_sampling_frequency(..).latency_monitor(..).commandstats_monitor(..).banner(..)，
  随后 :43-45 手接 with_shutdown_coordinator。
- wedb/wedb_standalone/src/main.rs:51-63：同一段三字段取值 + 三连 setter 逐字重抄（仅 banner 文本与
  无集群切面之差），另 :56-58 的 LoggingBuilder::from_node 已是单点（同类投影的正确形态先例）。
- 第三形态 wedb/wnode/src/server.rs:784-800 run_node：只 .banner(..) 后自展 TLS 匹配臂，未走同一投影，
  故装配面实为「两套半」。
- 统一流水线本体已在位：wedb/wnode/src/server.rs:15 模块头声明 ServerBootstrap 为服务端唯一入口，
  :71 结构、:89/:108 两段 impl（with_cluster_provider / with_shutdown_coordinator 等），缺的只是
  「args → 采样与监视器旋钮」这一层投影。

C# 参考
- C# 侧监视器与采样频率不经调用方逐字段手抄：装配入参整体为 GarnetServerOptions
  （garnet/libs/server/Servers/GarnetServerOptions.cs:297 MetricsSamplingFrequency 字段，
  :839-840 构造期校验 LatencyMonitor 必须伴 MetricsSamplingFrequency），消费方直接读 options。
- rust 对位即 ServerBootstrap 吃 args 自取投影，调用点零样板。

方案
- ServerBootstrap 增 with_node_args（或 new 内自取）承接三字段投影，boot.rs / main.rs / run_node 三处
  删局部量与逐条 setter；TLS 段一并收进同一入口，杜绝 run_node 独走一形态。
- 顺带把 C# :839-840 的 LatencyMonitor 需伴 MetricsSamplingFrequency 校验落在投影单点一处（禁三处各校验）。

优先级
重复/多套架构（同一装配事实多声明点，新增启动旋钮时必然漏抄）。

交叉引用
- 同文件域的服务面分文件见 task/ing/wnode-service-split.md；本条只收启动投影，勿在其纯移动拆分里改装配语义。
- 在途 network_connection_limit 接线亦经 boot.rs（task/ing/network-connection-limit-accept-guard.md），
  两单同文件不同事实，开工顺序上本单先行可少一次冲突。

落地记录（fixloop 收尸一棒，dev 6aea5715）
- 载荷取前手分支 boot-assembly 三提交：ServerBootstrap::run_async 一处从 NodeArgs 投影
  metrics_sampling_frequency_secs / latency_monitor / commandstats_monitor，新增
  wnode/src/server.rs:836 tls_config_from_node 作 TLS 唯一入口；boot.rs 与 wedb_standalone/main.rs
  删三局部量与三连 setter；删 run_node（lib.rs 同步摘导出）、ServerBootstrap::run /
  run_with_provider / with_tls_config 与三监视器字段、service.rs 零调用薄壳 open_node /
  StorageSessionProvider::open / open_recovered。
- C# GarnetServerOptions.cs:839-840「LatencyMonitor requires MetricsSamplingFrequency to be set」
  校验收进 wconf NodeArgs::validate 一处（NodeOptionsError::LatencyMonitorWithoutMetrics），
  装配侧零判定，用例 node_options.rs:test_latency_monitor_requires_metrics_sampling_frequency。
- 冲突处置（一律以 dev 现状为基，不造第二套并行机制）：
  1) server.rs 第三形态：dev 已给 run_node 的 TLS 臂加 mTLS 两旋钮（tls_client_cert_required +
     tls_issuer_cert，见 task/done/inbound-tls-client-cert-auth.md），收口时把 4 参
     ServerTlsConfig::from_pem_files 一并移进 tls_config_from_node，mTLS 语义无损且仍只一处。
  2) boot.rs/main.rs：dev 后落的 network_connection_limit 局部量与 setter 属另一事实
     （network-connection-limit-accept-guard 已在 dev），按票面范围原样保留，未并入本投影。
  3) node_options.rs：与 dev 的 unixsocketperm 定界校验并列，两段各留。
  4) 在途 txn-aof-marker-session-wiring（未进 dev）与本载荷无交集：未读其树、未扫其成果，
     合流基线取 dev。
- 验收：cargo check --workspace --all-targets exit 0 零警告；
  cargo check -p wnode -p wedb -p wedb_standalone --all-targets --features tls exit 0；
  bun js/check.js exit 0（其重复定义段不含本票文件，缺失实现段为本票无关存量台账）。
- 台账 next/qcode.rounds.md 的本条清账由主代理集中处理，本棒未触碰 next/。
