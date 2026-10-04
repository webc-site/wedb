## 审核结论

判定：两票均通过（2026-09-28 独立审核席 + 主控采纳），主控并族为单一执行票，定级 P3（特定配置可达之条件形）维持。

并族裁决（主控，覆盖两票各自「精炼执行方案」）：
1. 两票同机理底座（compio 定时器 `Instant::now() + d` checked 加法溢出 panic）、同闸点文件（wedb/wedb/src/server/boot.rs 既有拒启闸名单 :121-129）、同判据（boot 值域收到 C# IntRangeValidation 秒/毫秒契约上界），分票各开闸会在同一函数上产生两席触线并留下「只修两点留族二重口径」风险，故合为一票一席、单一闸名单改动、一旋钮一闸。
2. NodeArgs 域另二旋钮（index_resize_frequency_secs、metrics_sampling_frequency_secs）闸点在 wedb/wconf/src/node_options.rs:NodeArgs::validate，与本票闸点不同文件，不归本票——并入 task/todo/wconf-repl-attach-timeout-boot-no-upper-gate-compio-overflow-panic.md（该票已在同函数落 lua 式手闸先例），见该票「主控增补」段。
3. 文字订正（审核席指出的票面瑕疵，fix 席按本段表述）：
   a. CLI 长名实为 `--cluster-node-timeout-ms`（args.rs:72 随字段名推导）与 `--gossip-delay-secs`（args.rs:76），票面写作 `--cluster-node-timeout` / `--gossip-delay` 系简写，测试断言按真名。
   b. cluster-node-timeout 缺省为 60000 毫秒（args.rs:17 DEFAULT_CLUSTER_NODE_TIMEOUT_MS = DEFAULT_CLUSTER_TIMEOUT*1000，C# 60 秒口径），票面「缺省 5000」作废。
   c. 「约 1.8e10 秒」上界系常量误读：std sys/pal/unix/time.rs 的 TIMESPEC_MAX tv_sec = time_t::MAX = i64::MAX，1.8e10 那个是 TIMESPEC_MAX_CAPPED，注释明载仅 nto/qnx 的 pthread_cond_timedwait 使用，与 Instant 加法界无关。真实必炸域为 i64::MAX 近端（票一 u64::MAX 毫秒、票二 1e16 毫秒档在任何 64 位平台必炸，机理与极端档必炸坐实，不为具体阈值普适性背书）。测试断言理由须写「契约带上界拒收」，不得写「溢出 panic 断言」。
   d. 票一「全仓超时旋钮唯一未折哨兵大正值直通道」表述偏强（index_resize / metrics 两处 u64 直通道同在），本票落地 + NodeArgs 族票落地后方全闭。
   e. 票二「gossip 环死、故障检测/拓扑收敛整面停摆」宜收窄为「panic → supervise Err → dispose（gossip_manager.rs:477-478 复位 is_running）→ 事件重拉 → 再 panic 的自毁循环」：gossip_manager.rs:98-106 注释自陈重拉面存在，但配置不修则重拉必再炸，实效等同停摆，危害内核成立。票一出站炸点在派发任务内被任务级 catch_unwind 静默隔离（compio-executor-0.1.4/src/task/mod.rs:106），表现为连接尝试反复夭折而非进程崩溃。
4. 形态裁决：两票均取 a（boot 闸名单补上限拒启）。反证 b（消费侧钳）：CONFIG SET 面已被 META 秒域闭（wconf/runtime_server_config.rs:139-147），boot 一闸即令槽位全域有界，消费侧再钳即第二套运行期钳轨，与亚秒 done 票「值域在入口收干净」先例相悖。反证 c（折 None 哨兵）：cluster 侧 0 => None 无限哨兵已存在，把越界笔误并入故意无限系语义合并，正是 subsecond done 票拒绝过的形态；gossip_delay 为周期任务，折 None 等于永睡或空转，更糟。反证 d（判拒）：机理、可达、危害三面全实证，无从判拒。
5. 族口径：不与 repl-sync 的 INFINITE_SYNC_TIMEOUT_SECS 折 None（cluster_provider/flags.rs:58-66）冲突——彼为对位 C#「非正即无限」的语义折算，本闸为对位 C# IntRangeValidation 入口闸，同属「boot 值域按 C# 契约收口」单机制的两极；cluster_manager_slot_gate.rs:382 saturating_add 与 wepoch/src/wait.rs:82 elapsed 纯比较系消费侧安全形，与入口闸不冲突。
6. 查重：票二与 done 票 wedb-boot-gossip-delay-secs-ms-bare-mul-overflow.md 正面交叠但非重开——前票收口「裸乘溢出/release 环绕」，其方案 1 前提「饱和到 u64::MAX 毫秒即事实永不到期」恰被本票打破（定时器在到期之前就炸），本票属续票纠偏；前票执行方案本就并列过「args 层上界校验」。本闸落地后 boot.rs:342 saturating_mul 变冗余保护层但不碍事，禁删（删它即改前票收口面）。subsecond done 票只闭低边（boot.rs:38-44），高边确不在其面。doc/zh/deviations.md 无相关条目。

执行方案（fix 直接消费）：
1. 改动点唯一：wedb/wedb/src/server/boot.rs:run_cluster_server 的既有拒启闸名单（现位 :121-129，:122 gossip 抽样百分比门、:128 亚秒门）同段追加两闸，仿既有 `NodeError::InvalidArgument(ERR_*.into())` 单源文案形（错误常量与 :32/:36 同名单处新增）：
   - `args.cluster_node_timeout_ms != 0 && args.cluster_node_timeout_ms > (i32::MAX as u64) * 1000` 即拒（C# 秒域 IntRangeValidation(0, int.MaxValue) 上界原样搬到毫秒域；0 维持既有无限哨兵臂、亚秒档由 :128 既有闸拒，两闸不重叠）。
   - `args.gossip_delay_secs > i32::MAX as u64` 即拒。
   不动 flags.rs、不动 boot.rs:317/:342 播种臂、不动 args.rs 字段类型与 clap 值解析器（窄化 value_parser 会改 TOML 面缺省合并语义，闸名单是既有单机制）。
2. 测试：两档拒启断言挂既有 boot 拒启册（亚秒门/抽样门测试同处，承其形制）——`--cluster-node-timeout-ms 10000000000000000`（1e16）拒、`--gossip-delay-secs 99999999999`（1e11）与 `18446744073709551615` 拒；放行四臂——cluster 缺省 60000 / i32::MAX*1000 / 0 / 亚秒既有拒、gossip 缺省 5 / i32::MAX。合法面回归挂既有 gossip supervise 与 CONFIG GET 回显册（i32::MAX*1000 档回显走 boot.rs:51-58 既有 i32::MAX 饱和，只护回显不护计时器，本闸落地后高边全闭）。
3. 禁触线：禁在 flags.rs / gossip_delay() / node_connection.rs 消费侧另加第二钳；禁新建饱和工具函数；禁改 saturating_mul 与秒槽播种臂；禁 #[allow]；禁触在途他席域——本票只可改 boot.rs + args.rs 测试册，wconf/node_options.rs 属姊妹票域。

---

# 票面一（原文，cluster-node-timeout）

cluster-node-timeout 毫秒启动旋钮无上限闸与 C# 运行期钳制，越界大值折 Some 直送 gossip 建连与 MEET 限时器溢出 panic（C# 侧 CLI 闸加 TimeSpan 饱和双保险永不炸）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 双保险：
- 入口闸：garnet/libs/host/Configuration/Options.cs:298-300 `[IntRangeValidation(0, int.MaxValue)] public int ClusterTimeout`（秒整型），越界启动期拒。
- 运行期显式钳制：garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs:69-70 `GetClientTimeoutMilliseconds(int clusterTimeoutSeconds) => clusterTimeoutSeconds <= 0 ? 0 : (int)Math.Min((long)clusterTimeoutSeconds * 1000, int.MaxValue)`——即便契约带内也恒钳到 24.8 天毫秒上界，出站客户端每帧 WaitAsync 有界，无一档可炸。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧该旋钮以毫秒承载且下限门、上限两缺中只补了下限：
- wedb/wedb/src/args.rs:70-73 `cluster_node_timeout_ms: u64`，无上限校验；boot 仅有亚秒拒启下限门 wedb/wedb/src/server/boot.rs:38-44,127-129（done 票 wedb-cluster-node-timeout-subsecond-config-get-inversion.md 收口的正是低边回显反转，高边不在其面）。
- wedb/wedb/src/server/boot.rs:317 `set_cluster_node_timeout_ms(args.cluster_node_timeout_ms)` 原值入 provider 原子槽（CONFIG GET 播种侧 :322 已 i32::MAX 饱和，但那只护回显不护计时器）。
- wedb/wedb/src/server/cluster_provider/flags.rs:115-120 cluster_node_timeout() 只折 `0 => None`，任意大正值一律 `Some(Duration::from_millis(ms))`，无 C# Math.Min 对位钳制；对照 flags.rs:112-113 注释自陈「Duration::MAX 不可用作哨兵——compio 定时器 Instant::now() + Duration::MAX 溢出 panic，故无限分支不挂计时器」——约定写在注释上，值源高边无闸承接。
- 危险消费链（sink 全为 compio 定时器，std Instant checked 加法溢出即 panic、debug/release 同炸）：
  (a) wedb/wedb/src/server/gossip/node_connection.rs:61-65 `d.as_millis() as u64` 转回毫秒喂 new_outbound_client，wedb/wedb/src/client.rs:162-165 `timeout(Duration::from_millis(self.timeout_ms), connect_async)` 建连即炸，每条被选 gossip 连接反复触发；
  (b) wedb/wedb/src/server/gossip/gossip_manager.rs:169-173（原票 168-173，漂 1 行）`wait_async(cluster_node_timeout(), try_meet_async)` MEET 限时直送（wedb/wedb/src/server/mod.rs:29-37 Some 臂挂 compio timeout）。
- 安全消费面（甄别后不入本票）：wedb/wedb/src/server/cluster_manager_slot_gate.rs:382 i64 毫秒域 saturating_add 永不到点（:37 在册，对标 C# 形态）；wedb/wedb/src/server/cluster_provider/checkpoint.rs:54-63 经 wedb/wepoch/src/wait.rs:77-85 `start.elapsed() >= limit` 纯比较形态（现位 :82），无加法，不炸。CONFIG SET 面 META 上限 i32::MAX 秒（wedb/wconf/src/runtime_server_config.rs:139-147）已闭，缺口仅 boot/TOML 值源臂。

3. 逻辑危害确证（条件形，限定窗诚实自陈）
boot 配置 `--cluster-node-timeout-ms` 取极大毫秒档直至 u64::MAX（典型即「拿 MAX 当永等」的哨兵混写，恰属本议题点名的 u64::MAX 与超时字段混用形态；该面在 sync/attach 收口后仍为未折哨兵的大正值直通道之一），节点照常启动，gossip 出站建连与 MEET 在 compio 定时器溢出 panic，故障检测与拓扑收敛面反复夭折；C# 同输入启动期即拒。契约带内（<= i32::MAX 秒）rust 与 C# 均为事实不燃尽大界等待，无实质差，故为特定配置方可达之条件形，降档登记。

涉及代码：
rust 文件与函数：
wedb/wedb/src/args.rs:ClusterArgs.cluster_node_timeout_ms（:72-73，TOML 投影 :33/:138-139）
wedb/wedb/src/server/boot.rs:is_subsecond_cluster_node_timeout（下限门现状 :42-44）与 cluster 装配段 :317
wedb/wedb/src/server/cluster_provider/flags.rs:ClusterProvider::cluster_node_timeout
wedb/wedb/src/server/gossip/node_connection.rs:NodeConnection::new
wedb/wedb/src/client.rs:GarnetClient::connect_async（:138，:162-165 限时臂）
wedb/wedb/src/server/gossip/gossip_manager.rs:try_meet_async 限时段（:169-173）

对应 c# 文件与函数：
garnet/libs/host/Configuration/Options.cs:298-300
garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs:GetClientTimeoutMilliseconds（:69-70）与 GarnetServerNode 构造臂（:89）

---

# 票面二（原文，gossip-delay）

gossip-delay 秒启动旋钮无上限闸，大值经 saturating_mul 折 u64::MAX 毫秒送 gossip 主环 compio sleep 溢出 panic 致故障检测面停摆

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
- 入口闸：garnet/libs/host/Configuration/Options.cs:294-296 `[IntRangeValidation(0, int.MaxValue)] public int GossipDelay`（秒整型），越界启动期拒。
- 消费：garnet/libs/cluster/Server/ClusterManager.cs:112 `gossipDelay = TimeSpan.FromSeconds(serverOptions.GossipDelay)`；garnet/libs/cluster/Server/Gossip/Gossip.cs:354 `await Task.Delay(gossipDelay, ctsGossip.Token)`——契约带 <= i32::MAX 秒恒为有限大界或 .NET 饱和形态，主环不炸；GarnetServerNode.cs:113,191 的 reconnect/GossipAsync 亦 WaitAsync(gossipDelay) 同族有界。C# 全链无崩溃档。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
- wedb/wedb/src/args.rs:76-77 `gossip_delay_secs: u64`（TOML 同域 :34,:141-142），validate 无上限项（boot.rs:121-129 只闸 gossip 抽样百分比与亚秒节点超时，缺本项闸）。
- wedb/wedb/src/server/boot.rs:342 `cluster.set_gossip_delay_ms(args.gossip_delay_secs.saturating_mul(1000))`——饱和乘把一切超界秒数收拢成 u64::MAX 毫秒档原样入槽（wedb/wedb/src/server/cluster_provider/flags.rs:124-131），无任何折算闸把该档折回 None 或钳回契约上界。
- 危险消费链（sink 全为 compio 定时器；std Instant checked 加法溢出即 panic，debug/release 同炸）：
  (a) wedb/wedb/src/server/gossip/gossip_manager.rs:56 gossip_delay()=Duration::from_millis(ms)，:108-111 主环 `sleep(this.gossip_delay()).await`——首轮即 `Instant::now() + d` 溢出 panic；supervise 承接实读 wbase/src/supervise.rs:122-130（catch_unwind → Err 臂），gossip_manager.rs:107-117 Err → dispose（:477-478 复位 is_running）；
  (b) :413-421 `timeout(delay, conn.try_gossip_async(&config_bytes, delay))` 派发限时与 node_connection.rs:129-140 建连限时同源同值同炸（在 detached spawn 内，经任务级 catch_unwind 隔离）。
- 对照已闭面（不入本票）：gossip 消息扇出配额/重传/MEET 收敛侧系在册扫净缝；repl-sync-timeout、client connect timeout、boot TLS 三票面已收口；本票只圈 gossip-delay 值源高边单点。

3. 逻辑危害确证（条件形，限定窗诚实自陈）
配置 `--gossip-delay-secs`（或 TOML gossip_delay_secs）取极大秒档（含字面 MAX 哨兵档；saturating_mul 反把「超大即饱和」变成「超大必炸」的独木桥），节点启动成功后 gossip 主环首轮 sleep 即 panic 并连坐 dispose 全环，进「panic-监督-dispose-事件重拉-再 panic」自毁循环（审核结论 3e）——集群失去节点失联判定，分区永久 PFAIL 不裁决。契约带（<= i32::MAX 秒）内 rust sleep 68 年与 C# 大界不燃尽同形无实差，故为特定配置方可达之条件形，降档登记。缺省 5 秒不可达。

涉及代码：
rust 文件与函数：
wedb/wedb/src/args.rs:ClusterArgs.gossip_delay_secs（:76-77，含 TOML 投影 :34/:141-142）
wedb/wedb/src/server/boot.rs:集群装配段 :342
wedb/wedb/src/server/cluster_provider/flags.rs:ClusterProvider::set_gossip_delay_ms / gossip_delay_ms
wedb/wedb/src/server/gossip/gossip_manager.rs:ClusterGossipManager::gossip_delay / 主环 :108-111 / gossip_step 派发段 :413-421

对应 c# 文件与函数：
garnet/libs/host/Configuration/Options.cs:294-296
garnet/libs/cluster/Server/ClusterManager.cs:112
garnet/libs/cluster/Server/Gossip/Gossip.cs:GossipMainAsync（:354）
garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs:ReconnectAsync/GossipAsync 限时段（:113,:191）

---

## 终态注记（2026-09-29 boot 时间旋钮上限闸门执行席；票面判定：成立，已落地）

前一席「票面前提证否」经主控复核判废，本席对主控锚清单全部一手复核为真后动手。复核确证：值源 wedb/wedb/src/args.rs:72-73（cluster_node_timeout_ms: u64）、:76-77（gossip_delay_secs: u64）声明处仅 `#[arg(long, default_value_t = ...)]` 无 value_parser，u64 全域解析通过进槽；两字段属 ClusterArgs 域，args.rs:185 `merged.node.validate()?` 只辖 NodeArgs，对其零触碰；boot 闸名单为唯一承接位。C# 对位实读坐实：garnet/libs/host/Configuration/Options.cs:294-296 GossipDelay、:298-300 ClusterTimeout 均 `[IntRangeValidation(0, int.MaxValue)]`，契约带上界即 i32::MAX 秒。sink 面（flags.rs:115-120 只折 0=>None、:124-131 gossip 裸存裸取；node_connection.rs:61-65 → client.rs:162-165 建连限时；gossip_manager.rs:169-173 MEET 限时、:56+:108-111 主环 sleep）行号如本注记所列一手复核在位。

### 改动对位（落地后树内行号）
1. wedb/wedb/src/server/boot.rs（唯一生产改动文件）：
   - :41 `ERR_CLUSTER_NODE_TIMEOUT_TOO_LARGE`、:46 `ERR_GOSSIP_DELAY_TOO_LARGE`——与既有 :32 `ERR_GOSSIP_SAMPLE_RANGE`、:36 `ERR_CLUSTER_NODE_TIMEOUT_SUBSECOND` 同常量名单区。
   - :62-64 `is_oversized_cluster_node_timeout(ms) = ms != 0 && ms > (i32::MAX as u64) * 1000`、:71-73 `is_oversized_gossip_delay_secs(secs) = secs > i32::MAX as u64`——承 :52-54 `is_subsecond_cluster_node_timeout` 既有 pub const fn 纯判据闸形制（单机制先例；非饱和工具函数，零新折算机制）。
   - 闸名单判据位 :162-164（cluster 高边闸）、:168-170（gossip 高边闸），紧跟抽样门 :150-152、亚秒门 :156-158 之后，错误形制 `NodeError::InvalidArgument(ERR_*.into()).into()` 与名单同款；0 无限哨兵豁免臂与亚秒门共用、两闸域不重叠（亚秒拒 <1000，上限拒 >i32::MAX*1000）。
   - 播种臂一字未动（本票禁触面）：`cluster.set_cluster_node_timeout_ms(args.cluster_node_timeout_ms)` 现 :358、`cluster.set_gossip_delay_ms(args.gossip_delay_secs.saturating_mul(1000))` 现 :383——票面 :317/:342 系插闸前行号，内容逐字保留，saturating_mul 作前张 done 票收口面留冗余保护层。
2. 测试册（两文件，无新建文件）：
   - wedb/wedb/tests/cluster_node_timeout_subsecond_gate.rs（同旋钮册扩臂，册名头同步改为「亚秒拒启、上限拒启与秒槽播种一致性」）：新臂 `oversized_cluster_node_timeout_rejected_before_boot`（:96）拒档 `--cluster-node-timeout-ms 10000000000000000` 与 `18446744073709551615`（from_args_iter 不 Err 即值源可达实证；断言 InvalidArgument 指明 cluster-node-timeout-ms，理由=契约带上界拒收，非溢出 panic 断言）；`oversized_cluster_node_timeout_gate_boundary`（:115）放行档 0 豁免 / 缺省 `DEFAULT_CLUSTER_NODE_TIMEOUT_MS`（=60000，导入单源）/ 1000 / i32::MAX*1000 本身，拒档 i32::MAX*1000+1 / u64::MAX。既有亚秒拒启 :30、亚秒边界 :48、播种回显 :62 三臂零改动回归不破（:48 对 is_subsecond(u64::MAX)==false 的断言系低边纯判据面，仍真；boot 级 u64::MAX 拒由高边新闸承接，不冲突）。
   - wedb/wedb/tests/boot_gossip_delay_saturate.rs（gossip 档归属择此同旋钮册，不开新文件——理由与授权见下「范围注记」）：`boot_in_contract_gossip_delay_secs_boots`（:77）放行烟测 缺省 5s 与 i32::MAX s（原 `boot_boundary_gossip_delay_secs_no_overflow_panic` 三极值存活臂 18446744073709551/…552/u64::MAX 已被本闸改为拒启，臂义失效即此纠偏本体，按新语义改写；票面审核结论 6 自陈「前票方案 1 前提『饱和到 u64::MAX 即事实永不到期』恰被本票打破」）；`boot_oversized_gossip_delay_secs_rejected_before_boot`（:88）拒档 `--gossip-delay-secs 99999999999` 与 `18446744073709551615`（同上形制，指明 gossip-delay-secs）；`gossip_delay_upper_gate_boundary`（:116）判据边界 5 / i32::MAX 放行、i32::MAX+1 / 1e11 / u64::MAX 拒。册头注记补本票纠偏对位；provider 槽面 `gossip_delay_slot_chain_keeps_upper_bound`（:137）与 LAST_SAFE/FIRST_OVERFLOW 常量逐字未动（直驱 ClusterProvider 纯册面不经 boot 闸，仍绿）。

### 范围注记（改动文件清单对票面的自辩）
票面执行方案 3「本票只可改 boot.rs + 测试册」。boot_gossip_delay_saturate.rs 系测试册而非生产面，且其 boot 烟测臂硬编码本票明令推翻的旧语义（极值饱和放行）——不写红它即本闸无法落地，写了不改它即门禁必红，属票面落地的必然连带，非顺手越界；flags.rs / gossip/** / client.rs / args.rs 字段与 clap 属性 / wconf/** 一律未触。

### 锚漂移订正
1. 主控清单「亚秒门与抽样门测试同处 cluster_node_timeout_subsecond_gate.rs」不确：抽样门 boot 拒启臂实挂 boot_error_propagation.rs `test_boot_error_propagation_invalid_gossip_fraction`（:36-62，InvalidArgument 断言形制与本票一致，已承）；亚秒册为同族形制先例。本席按「同旋钮同册」放置臂位。
2. boot.rs 播种臂票面 :317/:342 → 落地后 :358/:383（插闸行号平移，内容未动）；闸名单票面 :121-129 → 落地后 :150-158（同因平移）。
3. args.rs 票面 :70-73/:76-77 复核在位如锚；wconf/runtime_server_config.rs:139-147 META 秒域、wedb/wconf/src/node_options.rs 姊妹票面本席未触。

### 未尽面 / 合规自陈
1. cluster 面 boot 级字面 i32::MAX*1000+1 拒档未单列（边界由 :115 判据册锁 i32::MAX*1000±1；boot 级拒档案值按票面 1e16，另附 u64::MAX 毫秒档——票面一「MAX 当永等哨兵混写」点名案值，主控清单第 4 点亦实证其解析通过，纳入拒臂属票面域内）。
2. 合法面重活（gossip supervise 册、CONFIG GET 回显册）按票面「挂既有」处理：本票生产改动为纯入口拒启，既有两册未受影响臂；i32::MAX*1000 回显仍走 boot.rs:80-87 既有 seed_secs i32::MAX 饱和形制，本闸落地后其饱和臂在 boot 可达域内变恒等直通（冗余保留）。
3. 合规：无 #[allow]、无假桩、饱和工具函数零新建、消费侧零第二钳、播种臂零触碰。自查命令仅 `cargo check --offline -p wedb --tests`（绿）；门禁全归主控。
4. 风险自陈：i32::MAX 秒烟测臂主环 sleep≈68 年不燃尽系 C# TimeSpan(int.MaxValue 秒) 同形（带内档非炸域），烟测承本册 150ms 存活+优雅停机形制、无计时依赖。前一席遗留的「boot.rs 死码」误判若曾入库他册注记，请主控合入轮顺检（本树 HEAD 764970d 面未见其改动残留）。

分支：boot-time-knobs-upper-gate；commit：本分支唯一 commit（注记无法自含自身哈希——自指不可行，取 `git log -1 --format=%H` 即本注记所在笔提交，主控合入时对账；哈希已随席位交报正文呈报）。

## 主控验票注记（2026-09-28 收票）

1. 全量审 commit `82bce03`（分支 boot-time-knobs-upper-gate）：merge 净面 `git diff 02e82be^1 02e82be --stat` = 4 文件 198+/14-，与席报逐项一致，零越线。生产改动仅 wedb/wedb/src/server/boot.rs 一处：两枚 `pub const fn` 判据（is_oversized_cluster_node_timeout / is_oversized_gossip_delay_secs）+ 两枚 ERR_* 单源文案 + 闸名单两臂，严格承亚秒门同形制；播种臂 :317/:342 与 saturating_mul 一字未动（前张 done 票收口面保留为冗余保护层，与本票审核结论 6 口径一致）。
2. 前一席判废存档：该席交报「票面前提证否、空手、零改动」，经主控一手复核其全部关键锚为伪造——`wedb/wnode/src/args.rs` 在本仓不存在、boot.rs 实有 cluster_node_timeout_ms 四处命中、args.rs:185 `merged.node.validate()?` 只辖 NodeArgs 域（两枚 ClusterArgs 旋钮不经它）、两字段声明处无 value_parser。重派后本席以一手锚复核推翻该判废，票面成立。教训入册：席报「证否」必逐锚亲验文件存在性与符号在位，不得凭报告结论销票。
3. 测试面复核：subsecond 册扩两臂（拒启 1e16/u64::MAX + 判据边界含 0 豁免与 ±1 界），gossip 前票册按纠偏新语义改写（原「极值饱和放行」烟测臂在本闸下必红，改写合理）；LAST_SAFE_SECS / FIRST_OVERFLOW_SECS 两常量仍被槽链册使用（:139/:155/:156），无死码残留；`WnodeError` 导入在册（:24）非凭空。
4. 断言口径复核：全部写「契约带上界拒收」，无「溢出 panic」伪断言；0 无限哨兵豁免臂与亚秒门域不重叠已测。
5. merge：02e82be（--no-ff，dev 逐个并）；fmt 尾随 8c69f19 收敛。门禁（./test.sh --no-fail-fast、./sh/clippy.sh、bun js/check.js）归主控本轮统一跑。
