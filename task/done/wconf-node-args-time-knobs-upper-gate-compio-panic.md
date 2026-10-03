repl-attach-timeout 启动旋钮无上限折算门，超大正值经 get_time_span 直送 compio 限时器溢出 panic（C# 侧 CLI 闸加 TimeSpan 饱和双保险永不炸）

（本票已由主控扩为 NodeArgs 时间旋钮族票：同名旋钮之外并收 index_resize_frequency_secs、metrics_sampling_frequency_secs 二旋钮，见文末「主控增补」段与其「执行口径合并」——fix 席以增补段的三旋钮口径为准。）

## 审核结论

审核结论：通过但收窄（2026-09-28 独立审核席 + 主控采纳，定级 P3 条件形维持）

收窄一（危害定形，落地注释按此表述）：溢出 panic 的真实形域不是「约 1.8e10 秒起直至 i64::MAX」，
  而是仅字面 MAX 近端档（raw 落在 i64::MAX 减去运行 uptime 以上）必炸；
  (i32::MAX, 该窗) 区间为「不炸但永不燃尽的限时器」（副本 attach 事实永停），属契约带外偏差而非崩溃。
  票据所引 std sys/pal/unix/time.rs:21 的 1.8e10 上界系常量误读：TIMESPEC_MAX tv_sec = time_t::MAX = i64::MAX
  （现位 :10-13），1.8e10 那个是 :19-23 的 TIMESPEC_MAX_CAPPED，注释明载仅 nto/qnx 的
  pthread_cond_timedwait 使用，与 Instant 加法界无关。
  测试点 --repl-attach-timeout 99999999999（1e11）的断言理由须写「契约带上界」，不得写「溢出 panic」。
收窄二（机理本身属实，席一手）：compio-runtime-0.12.6（Cargo.lock :557-560）src/time/mod.rs:7 导入
  std time::{Duration, Instant}，:52-53 sleep(d) = sleep_until(Instant::now() + d)，:85-89 timeout 走 sleep；
  std time.rs:427-428 的 Add for Instant 为 checked_add(...).expect("overflow when adding duration to instant")，
  非 overflow-checks 门控，debug/release 两档皆 panic；sleep_until 侧（time/runtime.rs:80）为
  saturating_duration_since，不加法不炸。与 task/done/repl-sync-timeout-infinite-sentinel-timer-overflow.md
  :43-45 的主控实测确证一致。

缺口实证（供 fix 直接引用，不必重跑）：
1 wedb/wconf/src/node_options.rs:NodeArgs::validate（现位 :1335-1462 全函数亲读）零行触碰
  replica_attach_timeout_secs；字段 :623 i64 无值域标注；投影 :1498 直通；播种
  wedb/wconf/src/runtime_server_config.rs:545-546 经 seconds_from_time_span（:941-943 只折非正为 0，
  正值原样进 i64 槽）；get_time_span :662-676 正值臂 Some(Duration::from_secs(raw as u64)) 无饱和无上限。
2 单机制先例即在同函数：lua_script_timeout_ms 手闸 :1432-1435（0 放行、其余须落 [10, i32::MAX]）、
  expired-key-deletion-scan-freq check_range :1397-1402（界即 i32::MAX）、
  network-connection-limit :1389-1394。注意 check_range（node_options.rs:1095）签名为 i32-only，
  本 i64 字段只能走 lua 式手闸 + 新错误变体（参照 LuaScriptTimeoutOutOfRange），不算新机制。
3 C# 一手（席实读 garnet/）：garnet/libs/host/Configuration/Options.cs:464-466
  [IntRangeValidation(0, int.MaxValue)] public int ReplicaAttachTimeout；折算 :996
  ReplicaAttachTimeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(...)；缺省
  garnet/libs/server/Servers/GarnetServerOptions.cs:425 = 60 秒。
4 值域可达：CLI 面 node_options.rs:617-621 仅 allow_negative_numbers、无 value_parser 窄化，
  clap 缺省 FromStr 收满 i64；TOML 面同字段直落；CONFIG SET 面确已被拦
  （runtime_server_config.rs try_set :715-728 先 parse::<i32> 再 META 槽 13 界 [0, i32::MAX]，现位 :207-218）。
  缺口仅在 boot/TOML 值源臂。
5 消费面三处属实：server/replication/assembly.rs:251-262（diskbased attach 应答限时）、
  server/replication/replica_diskless_sync.rs:155-159、
  server/replication/diskless_replication/replica_sync_session.rs:354-358，
  均经 cluster_provider/flags.rs:94-100 repl_attach_timeout()（runtime_config 不在位时 Some(60s) 硬回落，
  到不了危险值）；启动装配链 wnode/src/service.rs:1635/:1656/:1673 与 wedb/src/server/boot.rs:177 路通。

形态裁决：采 a（validate 补上限闸拒启），非正臂维持「<=0 折 0 即永等」现口径不动。
  反证 b（get_time_span 侧饱和折 None）：该法是槽表通用访问器，把越界正值折 None 等于「正 = 永等」，
    与 :657-658 在册「非正即无限」约定相悖，语义欺骗且不可观测。
  反证 c（折 MAX 哨兵）：sync 面 INFINITE_SYNC_TIMEOUT_SECS 解决的是 <=0 折永等臂，
    attach 的永等臂已由 seconds_from_time_span 折 0 存在；再立「大正值折永等」哨兵与 b 同欺骗，
    且违 done 票 :100-101「禁巨大时限作无限」纪律（task/reject/wnode-replay-align-barrier-...
    :15-19「saturating 案不予采纳」口径同向）。本闸补的是入口闸层，与 sync 折叠层不在同一机制位，
    不构成双机制。
  反证 d（判拒）：缺口属实可达，不判拒。

查重与分票：doc/zh/deviations.md grep「溢出/饱和/哨兵/无限」仅 §1（浮点格式化）、§4（EXPIREAT 钳制）、
  §165（/proc 探针）命中，均与本旋钮无关；五池 grep replica_attach_timeout / repl-attach-timeout
  仅命中本 issue。与同族姊妹票 task/issue/wedb-cluster-node-timeout-boot-upper-no-gate-compio-panic.md
  必须分票（文件 wedb/src/args.rs + boot.rs 对 wconf/node_options.rs；u64 毫秒 provider 槽
  对 i64 秒 RuntimeServerConfig 槽），该票自陈本 attach 闸落地后 cluster-node-timeout 即全仓
  唯一大正值直通道。

执行方案（fix 直接消费）：
1 改动点唯一：wedb/wconf/src/node_options.rs:NodeArgs::validate（现位 :1335），
  仿 :1432-1435 lua 手闸形制加「replica_attach_timeout_secs > i32::MAX as i64 即 Err」+ 新错误变体
  （参照 LuaScriptTimeoutOutOfRange 命名与文案口径）；承判位即启动漏斗末端
  （wedb/wedb/src/args.rs:185 merged.node.validate()?）。
  非正臂、投影 :1498、seconds_from_time_span 一律不动。
2 顺手订锚漂：node_options.rs:614 doc 注释自陈「对标 Options.cs:462」改为 :464-466
  （:462 实为 ReplicaDisklessSyncDelay）。
3 测试：wconf/tests/node_options.rs 既有拒启册扩臂——i64::MAX 拒、1e11 按契约带上界拒、
  缺省 60 / i32::MAX / 0 / 负值四臂放行；合法面回归挂 wconf/tests/default_single_source.rs；
  运行面永等臂复用 wedb/tests/replica_wire_integration.rs。
4 禁触线：禁改 get_time_span 与 seconds_from_time_span 语义、禁动 META 表值域、
  禁新建饱和工具函数或第二套入口闸、禁 #[allow]；禁触在途他席域
  （wedb/wedb/src/server/cluster_session/**、windex、wtls 现为他席脏文件）。

未尽面（席判无残余，登记供后续）：boot.rs:322 CONFIG GET 播种侧 i32::MAX 饱和只护回显不护计时器
  （姊妹票 :13 已点名），本闸落地后槽面高边即全闭。


问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 该旋钮三层全部落安全档：
- 入口闸：garnet/libs/host/Configuration/Options.cs:464-466 `[IntRangeValidation(0, int.MaxValue)] public int ReplicaAttachTimeout`，越界值启动期即拒。
- 折算：garnet/libs/host/Configuration/Options.cs:996 `ReplicaAttachTimeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(ReplicaAttachTimeout)`（本票议题点名的 995 行同族旋钮，995 的 ReplicaSyncTimeout 面已由 task/done/repl-sync-timeout-infinite-sentinel-timer-overflow.md 收口，996 无对位收口票）。
- 消费：garnet/libs/server/Config/RuntimeServerConfig.cs:314-326 GetTimeSpan 非正归 InfiniteTimeSpan（永等）；.NET Core 3.0 起 TimeSpan.FromSeconds 对大值饱和不抛，WaitAsync 收到超大 TimeSpan 经 TimeoutHelper 折 -1 即不挂计时器——garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:171-174、ReplicaDiskbasedSync.cs:180-186 全部有界或永等，无崩溃形态。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧同旋钮值域与闸均宽于 C#，且无限分支折叠只覆盖非正臂、不覆盖极大正值臂：
- wedb/wconf/src/node_options.rs:614-623 `NodeArgs.replica_attach_timeout_secs: i64`，doc 注释自陈「对标 C# Options.cs:462 IntRangeValidation(0, int.MaxValue)」，但 wedb/wconf/src/node_options.rs:1335 validate() 对本公司声明闸零实施（对照同函数 :1432-1434 lua_script_timeout_ms 闸、:1396-1402 expired-key-deletion-scan-freq check_range 先例）。
- wedb/wconf/src/node_options.rs:1498 直通投影入 RuntimeServerOptions；wedb/wconf/src/runtime_server_config.rs:941-943 seconds_from_time_span 正值直通、:545-546 播种槽位；:662-676 get_time_span 对 `raw > 0` 一律 `Some(Duration::from_secs(raw as u64))`，无饱和、无哨兵折 None。
- 消费落点 wedb/wedb/src/server/mod.rs:29-37 wait_async：`Some(d) => compio::time::timeout(d, fut)`，None 臂（永等、不挂计时器）形制正确；但 Some(极大 d) 即炸。compio-runtime-0.12.6 time/mod.rs:53 `sleep(d) = sleep_until(Instant::now() + duration)`，std Instant 加法为 checked 显式 panic（library/std/src/time.rs:428，debug/release 两档皆 panic，非 overflow-checks 门控），内部上界约 1.8e10 秒（library/std/src/sys/pal/unix/time.rs:21 TIMESPEC_MAX tv_sec = u64::MAX / NSEC_PER_SEC）。
- 三处实际消费：wedb/wedb/src/server/replication/assembly.rs:246-263（disk-based attach 应答限时）、wedb/wedb/src/server/replication/replica_diskless_sync.rs:156、wedb/wedb/src/server/replication/diskless_replication/replica_sync_session.rs:351-355。
- 对照面全部已闭环、非本票：repl-sync-timeout（i32 值域加 INFINITE_SYNC_TIMEOUT_SECS 折 None，node_options.rs:1493-1496、flags.rs:64-69）、CONFIG SET 面 repl-attach-timeout 本身有 META 上限 i32::MAX（runtime_server_config.rs:209-219）、failover 会话走 saturating_add（failover_session.rs:99-113）。缺口仅在 boot/TOML 值源臂。

3. 逻辑危害确证（条件形，限定窗诚实自陈）
配置 `--repl-attach-timeout` 取约 1.8e10 秒以上直至 i64::MAX 的量级（典型误用即「想要永等而字面写 MAX 哨兵」，恰为本仓禁止的 from_secs(u64::MAX) 混用形态；本旋钮无 sync 旋钮的对等哨兵折叠臂），节点照常启动，首个副本 attach（diskless/diskbased 同步握手停等帧）在 compio 定时器 `Instant::now() + d` 溢出 panic，副本 attach 链路中断；C# 同输入为启动期拒收、根不会走到运行期。缺省 60 与 C# 契约带（<= i32::MAX 秒）两档均安全（2.1e9 秒远低于 1.8e10 秒上界），故为特定配置方可达之条件形，降档登记。

涉及代码：
rust 文件与函数：
wedb/wconf/src/node_options.rs:NodeArgs.replica_attach_timeout_secs / NodeArgs::validate
wedb/wconf/src/runtime_server_config.rs:RuntimeServerConfig::get_time_span / seconds_from_time_span
wedb/wedb/src/server/mod.rs:wait_async
wedb/wedb/src/server/replication/assembly.rs:recover_replication（:246-263 attach 应答限时段）
wedb/wedb/src/server/replication/replica_diskless_sync.rs:replica_diskless_attach（:156）
wedb/wedb/src/server/replication/diskless_replication/replica_sync_session.rs:attach_replica_wire（:351-355）

对应 c# 文件与函数：
garnet/libs/host/Configuration/Options.cs:464-466,996
garnet/libs/server/Config/RuntimeServerConfig.cs:GetTimeSpan（:314-326）
garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:TryReplicateDisklessSyncAsync（:171-174）
garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:TryReplicateDiskbasedSyncAsync（:180-186）

（本段原案已被顶部审核结论的「执行方案」段收窄覆盖，fix 席以审核结论为准，禁直读本段。）
精炼执行方案：
1. NodeArgs::validate 补本项上限闸：`replica_attach_timeout_secs > i32::MAX as i64` 即拒启（对标 C# IntRangeValidation(0, int.MaxValue) 契约带上界，与同函数 :1432-1434 lua 闸、:1396-1402 check_range 同先例同名单机制；非正臂维持「<=0 折 0 即永等」现口径不动，避免与 done 票 sync 折叠面双轨）。
2. 测试验证点：`--repl-attach-timeout 9223372036854775807` 与 `--repl-attach-timeout 99999999999`（1e11）启动期拒收断言；缺省 60、i32::MAX、0 与负值档照常启动，replica_wire_integration 既有永等臂回归不破。

---

## 主控增补（r436 boot 席合族裁决，闸点同函数扩至三旋钮）

task/todo/wedb-boot-cluster-time-knobs-upper-gate-compio-panic.md 审核席扫出 NodeArgs 域另有二枚 u64 时间旋钮同炸且同缺上限闸。闸点与本票一致（wedb/wconf/src/node_options.rs:NodeArgs::validate，现位 :1335-1462），故并入本票一席收全，禁另开第二票在同一函数落两轨。

### 增补一 index_resize_frequency_secs（必炸，sink 主控亲验）
- 值源：node_options.rs:1000 `pub index_resize_frequency_secs: u64`，CLI 长名 `--index-resize-frequency`（:996-1000），缺省 DEFAULT_INDEX_RESIZE_FREQUENCY_SECS = 60（:302-303）；validate 现对该字段零行触碰（全函数 grep 仅 :1210 缺省与 :1687 投影命中）。
- sink：wedb/wnode/src/service.rs:583 `let interval = Duration::from_secs(frequency_secs.max(1))` → :591 `sleep(interval).await`，而 service.rs:23 实绑 `use compio::{runtime::spawn, time::sleep}` —— compio 定时器，`Instant::now() + d` checked 加法溢出 panic，debug/release 两档皆炸（机理同本票主段）。supervise_resumable 隔离故进程不崩，表现为自动扩容任务反复夭折。
- C# 一手（主控实读 garnet/）：garnet/libs/host/Configuration/Options.cs:616-618 `[IntRangeValidation(1, int.MaxValue, isRequired: false)] public int IndexResizeFrequencySecs`，garnet/libs/server/Servers/GarnetServerOptions.cs:167 `public int IndexResizeFrequencySecs = 60`——int 秒域天然有界，越界启动期拒。
- 闸形：`index_resize_frequency_secs > i32::MAX as u64` 即拒启，与本票 repl-attach 闸同名单同形制。
- 未尽面（本票不裁，禁顺手改）：低边 0 档 rust 经 `.max(1)` 折 1 秒（service.rs:583），C# 显式 0 属 IntRangeValidation(1,..) 拒收档——语义分歧属实，但改它即动既有放行档，须另立小票，本票只补高边。

### 增补二 metrics_sampling_frequency_secs（必炸，审核席标「不可判」项由主控追实 sink 闭环）
- 值源：node_options.rs:872 `pub metrics_sampling_frequency_secs: u64`，CLI 长名 `--metrics-sampling-freq`（:867-872），缺省 DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS = 0（:96）；validate 现仅有 :1426「latency_monitor 须配非零采样」一条关联闸，无值域闸。
- sink（注入实链已追到底）：wedb/wnode/src/server.rs:987 `if frequency_secs > 0` 拉起监视任务 → :999 `monitor.main_monitor_task_async(time::sleep, …)`，其中 `time` 即 server.rs:40-44 `use compio::{ … time }`（compio 域，非 std）；wedb/wmetric/src/garnet_server_monitor.rs:123 `Duration::from_secs(metrics_sampling_frequency_secs)` → :469-470 泛型 `S: FnMut(Duration) -> Fut` 注入点 → :473-474 主环 `sleep(self.monitor_sampling_frequency).await`。泛型闭包实参即 compio sleep，同炸机理确认。
- C# 一手：garnet/libs/host/Configuration/Options.cs:358-360 `[IntRangeValidation(0, int.MaxValue)] public int MetricsSamplingFrequency`（注释明载「0 disables metrics monitor task」，与 rust :987 `> 0` 拉起门同口径，非正臂不动），garnet/libs/server/Servers/GarnetServerOptions.cs:297 `public int MetricsSamplingFrequency = 0`，:839-840 LatencyMonitor 关联闸（rust :1426 对位）。
- 闸形：`metrics_sampling_frequency_secs > i32::MAX as u64` 即拒启；0 档既有「禁用监视器」语义维持不动。

### 增补段的执行口径合并
1. 本票改动点由一处变三处、同函数同形制：NodeArgs::validate（现位 :1335-1462）仿 lua_script_timeout_ms 手闸（:1432-1435）加三条上限拒启——repl_attach_timeout_secs（i64，负值与 0 维持「<=0 折 0 即永等」现口径）、index_resize_frequency_secs（u64，仅高边）、metrics_sampling_frequency_secs（u64，仅高边）；界一律 i32::MAX 秒（对位 C# IntRangeValidation 上界）。
2. 错误变体：check_range（node_options.rs:1095）签名 i32-only，本域 i64/u64 字段走手闸 + 新错误变体（参照 LuaScriptTimeoutOutOfRange 命名与文案口径），三旋钮可共用一条「值域越 C# 契约带上界」形制文案，禁造第二套闸机制。
3. 测试：wconf/tests/node_options.rs 既有拒启册扩臂——i64::MAX/u64::MAX 与 1e11 三档按契约带上界拒（断言理由禁写「溢出 panic」，见上方收窄一 c）；放行臂补 index_resize 缺省 60 / i32::MAX / 0（0 维持现放行折 1s）、metrics 缺省 0 / i32::MAX；repl-attach 四臂照旧。运行面回归挂 wconf/tests/default_single_source.rs 与 wedb/tests/replica_wire_integration.rs。
4. 禁触线追加：本席只改 wconf/node_options.rs 与其测试册；gossip/** 与 boot.rs 属姊妹票域（现他席在途），wedb/wedb/src/client.rs 现为他席脏文件（MM），禁触。

## 终态注记（node-args-time-knobs-upper-gate 席，2026-09-28）

结论：票面缺口属实可达，按主控增补段三旋钮口径落地；改动仅 wedb/wconf/src/node_options.rs 与其测试册两处文件，投影/消费面零改动。主控亲验锚清单本席逐条一手复核属实（rust 侧 sed 实读 + C# 侧 garnet/ 实读），无伪造锚。

### 改动对位（均为改后新行号）
1. 新错误变体 `NodeOptionsError::TimeKnobAboveContractUpper(&'static str, u64)`：node_options.rs:426-435。文案 `"{0} expected to be at most 2147483647 seconds (C# IntRangeValidation upper bound). Actual value: {1}"`——循 LuaScriptTimeoutOutOfRange（:410-415）的 C# 校验消息同形句式；三旋钮共用此一条变体一份文案（增补段口径 2），非正/低边语义不经此变体。次参 u64 承载实际值：拒收臂恒为正（闸条件保证），对 i64/u64 两域无损，变体 doc 已注明。
2. 三闸入 NodeArgs::validate()（函数现位 :1349-1504，原 :1335-1462 因本席 enum/doc 改动行漂），置于同族关联闸 LatencyMonitorWithoutMetrics（:1440-1442）之后、lua 手闸先例（:1472-1475）之前的同段落：
   - :1453-1458 `replica_attach_timeout_secs > i32::MAX as i64` 即 Err（"repl-attach-timeout"）；非正臂「<=0 经 seconds_from_time_span 折 0 即永等」不动；
   - :1459-1464 `index_resize_frequency_secs > i32::MAX as u64` 即 Err（"index-resize-frequency"）；仅高边；
   - :1465-1470 `metrics_sampling_frequency_secs > i32::MAX as u64` 即 Err（"metrics-sampling-freq"）；0 档「禁用监视器」不动。
   闸形制 = lua_script_timeout_ms 手闸先例；check_range（:1108）i32-only 签名对本域 i64/u64 不适用，未造第二套闸/饱和工具。
3. 锚漂订正与字段 doc：:624-627 attach 注释「对标 C# Options.cs:462」订正为 Options.cs:464-466（本席实读 garnet Options.cs:462 确为 ReplicaDisklessSyncDelay，:464-466 为 ReplicaAttachTimeout，订正属实）；:876-878 metrics 注释补 :358-360 区间与「0 disables metrics monitor task」口径（原引 :359 为同项 Option 行，非漂）；:1005-1008 index_resize 注释补 IntRangeValidation(1, int.MaxValue, isRequired:false) 全带与低边折 1s 说明。

### 测试臂清单（tests/node_options.rs:382-435 `test_time_knobs_contract_upper_bound`，走 from_args_iter 全漏斗含 validate）
- 拒档六臂（断言变体+点名 CLI 长名+原值字符串三方对位；口径一律「契约带上界拒收」，未写溢出 panic，收窄一 c 照办）：attach `9223372036854775807` / `99999999999`；index_resize `18446744073709551615` / `99999999999`；metrics `18446744073709551615` / `99999999999`。
- 放行档七臂：三枚缺省一次验（60/60/0）、attach/index/metrics 各 i32::MAX 界上档、attach `0` 与 `-1`（非正臂现口径）、index_resize `0`（经 service.rs:583 `.max(1)` 折 1 秒现放行，本票不动低边）；另两臂断言放行档字段原值不被闸扭曲。
- 合法面回归挂载面（default_single_source.rs / replica_sync_timeout_sentinel_projection.rs 同族、replica_wire_integration.rs 运行面永等臂）本席未追加改动——放行语义已由上列放行臂在闸函数本体锁死，既有册照常跑门禁即可。

### 一手复核记录
- C#：garnet/libs/host/Configuration/Options.cs:464-466（IntRangeValidation(0,int.MaxValue) ReplicaAttachTimeout，折算 :996 `<=0 ? InfiniteTimeSpan`）、:358-360（MetricsSamplingFrequency，HelpText 明载 0 disables）、:616-618（IndexResizeFrequencySecs IntRangeValidation(1,int.MaxValue,isRequired:false)）；GarnetServerOptions.cs:425 = TimeSpan.FromSeconds(60)、:297 = 0、:167 = 60。
- rust 缺口链：get_time_span :662-676（raw>0 一律 Some(from_secs)）、seconds_from_time_span :941-943、播种 :545-546、投影 node_options.rs:1540 attach 直通（原锚 :1498，本席改动致行漂，符号定位订正）；sink 抽读 service.rs:583/:591（compio sleep）、server.rs:987/:999 与 garnet_server_monitor.rs:123/:469-474 属实。CONFIG SET 面 META 秒域 [0,i32::MAX]（runtime_server_config.rs:139-147 区段亲验）与 try_set parse::<i32>（:715-728）已闭，本席未重复造闸；缺口仅 boot/TOML 值源臂，闸点 validate 为唯一承判位（args.rs:185 在册）。

### 未尽面（登记，本票禁顺手改）
1. index_resize 低边分歧：rust 显式 0 经 `.max(1)` 折 1 秒放行，C# IntRangeValidation(1,..) 显式 0 即拒——须另立小票。
2. 契约带内 (i32::MAX, 定时器可燃尽窗) 的不炸永限形两仓同出（C# int.MaxValue 秒同样不燃尽），非本闸语义面。
3. boot.rs:322 CONFIG GET 播种侧属姊妹票域，未触。

### 自查风险
1. attach 闸错误携值 `as u64` 转换：仅在已判 >i32::MAX（恒正）分支执行，无损；若后续把该变体复用于可含负值的闸须改携值类型。
2. 三闸置于 latency 关联闸之后：metrics=0+latency_monitor 拒启臂先行、高边闸后行，两条件互斥无遮蔽；同函数各闸独立 return，无顺序耦合。
3. 新变体加于 enum 末位，既有 match 臂零改动；`cargo check --offline -p wconf --tests` 本席通过（唯一被许可编译命令），其余门禁归主控。

分支：node-args-time-knobs-upper-gate；commit：`9bc3695`（该分支唯一 tip commit，含 node_options.rs 三闸/变体/锚订正与测试册扩臂两文件；本注记留工作区归主控 docs 归档，未入我 commit）

## 主控验票注记（2026-09-28 收票）

1. 全量审 commit `9bc3695`（分支 node-args-time-knobs-upper-gate）：merge 净面 `git diff 2439ab4^1 2439ab4 --stat` = 2 文件 103+/6-（node_options.rs + tests/node_options.rs），与票面禁触线完全一致，零越线。三闸落 NodeArgs::validate 同段（attach `> i32::MAX as i64`、index_resize/metrics `> i32::MAX as u64` 仅高边），循 lua_script_timeout_ms 手闸先例，共用一条新变体 TimeKnobAboveContractUpper；get_time_span / seconds_from_time_span / META 表 / 投影消费侧零改动（复核 diff 无该行段）。
2. 语义保持复核：attach 非正臂「<=0 折 0 即永等」与 metrics 0 档「禁用监视器」未动；index_resize 低边 0 经 `.max(1)` 折 1 秒现放行未动（其低边分歧已由席登记为未尽面 1，须另立小票，本票禁顺手改，照办）。
3. 锚漂订正核实：:624-627 attach doc「对标 Options.cs:462」→ :464-466，主控独立实读 garnet Options.cs:462 确为 ReplicaDisklessSyncDelay，订正属实。
4. 测试面复核：六拒臂（三旋钮 × i64/u64::MAX + 1e11）断言变体+CLI 长名+原值三方对位，口径「契约带上界拒收」；七放臂含三枚缺省、各 i32::MAX 界上档与两枚特殊语义档，另两臂锁放行档字段原值不被闸扭曲——真断言，非形似。
5. 连带收口：本票落地后 boot 时间旋钮族高边全闭（姊妹票 task/done/wedb-boot-cluster-time-knobs-upper-gate-compio-panic.md 收 ClusterArgs 域）。check.js 遗留缺锚面中 RecordInfo.cs:TryResetModifiedAtomic 一项经独立查证席定性为死标记（task/issue/wrecord-modified-bit-dead-marker-retire.md），其 js/check 收口步骤归该票承接，不在本票面。
6. merge：2439ab4（--no-ff）；fmt 尾随 8c69f19。门禁归主控统一跑。
