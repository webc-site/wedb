终态:闭环(2026-09-29)。甄别 adcd275 → 沙箱 d589e8a → 并 dev 2262c67;fmt 合行残留 d1a56ba。12 枚闸(11+commit_ms 随收)全走 check_range/ValueOutOfRange 单点,落位与禁触线经审查席七轴全过;审查席 P3(直填态第四元弱断言)修于 916f0e1;门禁红 vector_quantization_task_count_assembly 两测系票面漏盘的第三把旧「负值折叠」锁,随票回改启动期拒启断言于 da26847。终验 test.sh 5205/5205+clippy 0。
甄别结论:通过(P1 级,2026-09-29 主控席)。亲验:check_range 单点 :1109-1115、validate :1349-1504 现有闸仅五枚+高边三闸+lua 手闸,11 枚下界零覆盖;11 枚字段行逐行命中均 i32;aof_commit_ms :593 无区间闸仅组合闸,遵审核席点 1 随票收口。

审核结论：通过（2026-09-29 独立审核席，P1 级）。11 枚 validate 零闸逐枚亲验、C# 校验属性 11 处全亲读、
甲档三枚危害链全验（replica_replay_driver.rs:192-212 永久挂起、scan_input.rs:80/:291 无界外发、
metrics_commands.rs:103/:112 每命令入慢日志）、clap 4.6.7 等号形放行源码级确证、11 枚缺省值全落区间内
无条件 check_range 放行成立。查重与先例票正交，方案单机制合规。
审核席整理两点（执行席遵照）：
1. aof-commit-ms（node_options.rs:593 Option<i32>，C# :247 (-1,max) 下界缺）建议随本票同段收口：
   validate 内补 if let Some(ms) = self.aof_commit_ms { check_range("aof-commit-ms", ms, -1, i32::MAX)? }
   （四行内，与 11 枚同面同法）；若执行席裁不随收，须在归档时显式登记 follow-up 票防遗忘。
2. d) 新增下界臂 check_range 点名与既有正臂（:1357）同用 CLI 名 "slow-log-threshold"，避免同字段
   两臂命名分叉。

原票面：
启动配置装配臂十一枚数值旋钮缺 C# IntRangeValidation 定界闸，负值经 CLI 等号形与 TOML 文件臂直通运行期消费者，形态自 CONFIG SET 臂已拦、启动臂双层失守

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）。C# 全部配置源（结构缺省、defaults.conf 基线、--config 文件、CLI 显式）
经 ServerSettingsManager 两遍解析汇成单一 Options 实例后，恒过 IsValid 漏斗（Options.cs:738 定义；
ServerSettingsManager.cs:149 判定，失败即拒启、零兜底）。IntRangeValidationAttribute
（OptionsValidators.cs:499-501，基类 RangeValidationAttribute :430/:445 的 isRequired 缺省 true）恒校验，
isRequired:false 跳检臂（TryInitialValidation，OptionsValidators.cs:82-99）仅在值==类型缺省时放行。
本票 11 枚旋钮的 C# 校验属性锚（均为区间属性行）：
  a) --aof-tail-witness-freq：Options.cs:243 IntRangeValidation(0, int.MaxValue)（属性 :245）
  b) --expired-object-collection-freq：Options.cs:267 (0, max)（属性 :269）
  c) --compaction-max-segments：Options.cs:278 (0, max)（属性 :280）
  d) --slowlog-log-slower-than：Options.cs:350 (0, max)（另有正臂 100µs 二次闸 :860-863）
  e) --slowlog-max-len：Options.cs:354 (0, max)
  f) --replica-sync-delay：Options.cs:433 (0, max)（属性 :435）
  g) --aof-replay-max-lag-bytes：Options.cs:437 (-1, max)（属性 :439）
  h) --object-scan-count-limit：Options.cs:589 (0, max)
  i) --cluster-replication-reestablishment-timeout：Options.cs:695 (0, max, isRequired:false)
     ——缺省 0 在区间内自然放行，负值非缺省必检必拒
  j) --repl-diskless-sync-delay：Options.cs:460 (0, max)（属性 :462）
  k) --vector-set-quantization-task-count：Options.cs:716 (0, max, isRequired:false)
     ——同 i：缺省 0 放行（defaults.conf:542 = 0），负值拒启
即 C# 形态下上述 11 枚的任何负值（g 为 ≤ -2）在 CLI 与文件两臂均拒绝启动，永远到不了运行期消费者。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）。rust 装配单点 NodeArgs::from_layered_matches
（node_options.rs:1820-1841）：Self::default → --config TOML（from_file）→ CLI 显式
（override_explicit :1662-1762）→ validate()?（:1833）→ 投影。validate()（:1349-1504）为对标 C# IsValid
漏斗末端的唯一启动数值定界单点，其现有数值臂仅五枚 check_range（:1357 慢日志正臂 100µs、:1396 max-databases、
:1403 network-connection-limit、:1411 expired-key-deletion-scan-freq、:1433 unixsocketperm）加尺寸文法、
AOF 提交组合、时间旋钮高边三闸（:1453-1470）与 lua 手闸——本票 11 枚旋钮的下界/区间闸零枚在册。
第二层失守：RuntimeServerConfig::init 播种直 store 裸值零校验（runtime_server_config.rs:518-592，
:589-591 逐槽 store），META 槽位 min/max（槽 8 min 0 :155、槽 9 min -1 :166、槽 15 min 0 :235、
槽 17 min 0 :257、槽 18 min 0 :267、槽 22 min 0 :309）仅护 try_set 的 CONFIG SET 臂（:703-728）——
同一负值 CONFIG SET 被拒、启动臂放行，优先级链的启动臂与热更臂校验分叉。
负值可达性：11 枚全为 i32 字段（声明锚：a :660、b :863、c :706、d :787、e :793、f :641、g :728、
h :851、i :671、j :739、k :686）；clap 对未带 allow_negative_numbers 的选项仅拦空格分隔形
（-5 被当旗标），--opt=-5 等号形直入值解析；TOML 文件臂经 serde 直落负值零文法闸。
3. 逻辑危害确证（逐枚消费者链，分三档）。
  甲档（真实行为危害）：
  g) aof-replay-max-lag-bytes ≤ -2：boot.rs:347 直读注入 ClusterProvider（checkpoint.rs:237
     set_aof_replay_max_lag_bytes）→ 副本侧每帧 cluster_replication_session.rs:375
     driver.throttle_wait(… ) → replica_replay_driver.rs:192-212：仅 == -1 免节流，否则
     throttle_released = current_lag <= max_lag（:209-211），lag 恒 ≥ 0 而 max_lag ≤ -2 恒不成立 →
     返回永久挂起体，副本背景回放推流停摆直至处置（dispose）。C# 该值拒启。
  h) object-scan-count-limit 负：会话现取槽位（shared_object_commands.rs:311/:355、
     garnet_api/mod.rs:816）传入 read_scan_input → scan_input.rs:80
     `result.count = i64::from(c).min(i64::from(limit_count_in_output))`，负预算使 COUNT 用户参数
     钳出负值；scan_kernel :291 `emitted == count_limit` 相等截断对负值恒不成立 → *SCAN 族单帧
     无界整对象外发（内存态与分层态同核），应答体积失控。与已收口 wcol 负 count 案不同缝（那是
     命令参数域，此为配置值域，见查重）。
  d) slow-log-threshold 负：metrics_commands.rs:101-112 handle_slow_log 仅 `== 0` 短路禁用，负值绕开；
     :112 `elapsed > self.slow_log_threshold` 恒真 → 每命令入慢日志且 :113-118 逐命令参数快照分配，
     诊断面被淹没、热路径掺入分配开销。C# 的 (0,max) 恒检闸未转写（rust 只转写了正臂 100µs）。
  乙档（静默钳位/折叠成禁用档，C# 拒启 vs rust 放行且语义等同文档化禁用值）：
  e) slowlog-max-len 负：SlowLogContainer::new（slow_log_container.rs:31-32，经
     metrics_commands.rs:138-140 构造）`size.max(0)` → 容量 0 静默禁用慢日志；与 d) 同配即
     「每命令记而全存不下」的组合失形。
  f) replica-sync-delay 负：replica_replay_task.rs:75-84 current_sync_delay `.max(0)` → 折叠成
     0 档（文档化「关闭节流」），负值配置者意图不可观测。
  b) expired-object-collection-freq 负：primary_tasks.rs:330-344 `(freq > 0).then_some` → None →
     任务 break 自退；且该任务是分层键后台降阶评估轮唯一宿主（:364-370 tiered_demote_round，
     deviations.md §120 在册寄生关系），一负值连降阶评估一并静默停。
  c) compaction-max-segments 负：service.rs:2050-2054 启动投影 `.max(0) as usize` → 0；
     wkv/gc/compact.rs:108-112 `max == 0` 视作紧缩关闭（wkv/config.rs:140 注释「0 = 永不紧缩」）→
     日志段无界积压。
  k) vector-set-quantization-task-count 负：service.rs:1534/:1648/:1678 `.max(0) as usize` → 折叠成
     0 档 = ProcessorCount（vector_manager.rs:236 C# 对位注释）。
  丙档（节流折叠，负值等同 0/禁用档、引发高频空转）：
  a) aof-tail-witness-freq 负：aof_sync_task.rs:366-370 `now - last < get_milliseconds(槽 11)` 恒假 →
     每轮发 CLUSTER ADVANCE_TIME 脉冲帧（与 0 档同形，C# 拒负）。
  i) cluster-replication-reestablishment-timeout 负：replication.rs:161-170 仅 `== 0` 判禁用，负值入
     replication_manager.rs:1059-1069 `interval_ms > 0` 恒假 → 永不节流，每个 APPENDLOG 初始化帧
     握手都重试 ensure_replication（重连风暴面）。
  j) repl-diskless-sync-delay 负：槽 12 播种后主端攒批开窗等待 `> 0` 门（replication_sync_manager.rs
     对位臂）不成立 → 折叠成 0 档立即开窗。

涉及代码：
rust 文件与函数：
wedb/wconf/src/node_options.rs:NodeArgs::validate（1349-1504，唯一改动落点）
wedb/wconf/src/node_options.rs:check_range（1109-1115，i32 域现成单点，复用零新增错误变体）
wedb/wconf/src/node_options.rs:字段声明（641、660、671、706、728、739、787、793、851、863、686）
wedb/wconf/src/node_options.rs:from_layered_matches（1820-1841，装配序证据）、override_explicit（1662-1762）
wedb/wconf/src/runtime_server_config.rs:init 裸播种（518-592）、try_set（703-728）、META min/max（149-309）
wedb/wedb/src/server/boot.rs:347、cluster_provider/checkpoint.rs:237-251、
  cluster_provider/replication.rs:159-170、cluster_replication_session.rs:375
wedb/wedb/src/server/replication/replica_replay_driver.rs:throttle_wait/throttle_released（192-212）、
  replica_replay_task.rs:current_sync_delay（75-84）、aof_sync_task.rs:send_advance_time_pulse（359-372）、
  replication_manager.rs:ensure_replication_due（1059-1069）
wedb/wcol/src/types/scan_input.rs:read_scan_input（80）、scan_kernel（291）
wedb/wnode/src/resp/metrics_commands.rs:handle_slow_log（101-122）、new_slow_log_container（138-140）
wedb/wnode/src/resp/objects/shared_object_commands.rs:311/:355、garnet_api/mod.rs:816
wedb/wmetric/src/slowlog/slow_log_container.rs:new（31-32）
wedb/wnode/src/primary_tasks.rs:object_collect_loop（324-370）、wnode/src/service.rs:2050-2054、
  service.rs:1534/:1648/:1678、wkv/src/gc/compact.rs:108-112、wkv/src/config.rs:140

对应 c# 文件与函数：
garnet/libs/host/ServerSettingsManager.cs:149（IsValid 拒启）
garnet/libs/host/Configuration/Options.cs:738（IsValid）、校验属性行 243/267/278/350/354/433/437/460/589/
  695/716、二次闸 860-863
garnet/libs/host/Configuration/OptionsValidators.cs:49（isRequired 缺省 true）、82-99（跳检臂）、
  430/445/499-501（RangeValidation/IntRangeValidation）
garnet/libs/host/defaults.conf:179/542（缺省档 10/0 均在区间内）

精炼执行方案：
1. 单点补闸：validate()（node_options.rs:1349-1504）内对 11 枚各加一条 check_range 无条件调用
   （全部 i32 字段、check_range 签名现成，缺省值全落区间内自然放行，无需分支）：
   g) lo = -1（-1 无限滞后档保留）；d) lo = 0 且置于现有 `> 0` 100µs 臂之前（两臂分别对位 C# :350
   与 :860-863，不合并语义）；其余 a/b/c/e/f/h/i/j/k lo = 0，hi = i32::MAX。
   每臂行上注释钉对应 C# 属性行锚。i/k 两枚的 C# isRequired:false 语义因缺省 0 在区间内，
   无条件 check_range 与之等价（非零负值两态同拒）。
2. 禁触线：
   a) 不改 META 槽位 min/max、不改 try_set、不在 init 播种处加校验——启动门唯 validate 单点
      （先例票 time-knobs-upper-gate 同款口径，禁第二套装配路径）；
   b) 不用 clap range_only/自定义 value_parser 设闸（第二机制且不覆盖 TOML 臂）；
   c) 不拆消费者侧 .max(0)/==0 兜底钳位（防御纵深保留，收口在启动臂不在消费臂）；
   d) 不改任何缺省值常量；不新增错误变体（复用 NodeOptionsError::ValueOutOfRange）；
   e) 先例票裁「保留」的臂一字不动：expired-key-deletion-scan-freq 的 -1 档（:1411）、
      network-connection-limit 的 -1 档（:1403）、attach/index-resize/metrics 三旋钮的非正档与高边闸
      （:1453-1470）、slow-log 正臂 100µs（:1356-1363）；
   f) deviations.md 不登记（对齐修复非偏差）；§120 的 0 档寄生语义不动（本票只补负值拒启）。
3. 测试验证点（纯静态起草，交执行席落地）：
   a) wconf 单元测试逐臂：--opt=-1 等号形 CLI（g 用 --aof-replay-max-lag-bytes=-2）、TOML 负值文件形、
      构造体直填三态均 Err(ValueOutOfRange)，name/lo/hi 逐值断言；合法边界放行集：g=-1、d=0、
      d=100、b=0、k=0、i=0（缺省档回归）；d 的 50（正臂 100µs 既有拒形回归）；
   b) 装配臂集成锁测：含负值的 --config 文件启动拒码非零、零服务监听；同值 CONFIG SET 既有
      try_set 拒形测试保持（两臂终态一致）；
   c) 回归：time-knobs-upper-gate 族测、aof-size-limit 空串哨兵测、size/bool 文法族测、§120 的
      0 档停轮测、wcol 负 count 参数族钳制测全绿零改动。

查重结论：
task/done/wconf-node-args-time-knobs-upper-gate-compio-panic.md 只收三时间旋钮高边（且明文保留非正档），
本票为其正交的 0/-1 下界族，无重叠；wconf-aof-size-limit-empty-string-boot-guard-sentinel-divergence.md、
wconf-size-flag-grammar-*、wconf-bool-flag-grammar-* 裁文法/哨兵面不涉数值区间；
boot-tls-gate-* 与 repl-sync-timeout-* 两票各收 TLS 形态与 :468 旋钮，不在本 11 枚；
wcol-random-family-negative-count-reply-unbounded-process-oom.md 收 HRANDFIELD/SRANDMEMBER/ZRANDMEMBER
命令参数负 count（parse_random_member_args 入口），本票 h) 为配置值域预算（scan_input.rs:80 的
limit_count_in_output 参数侧），缝不同源不重复；
deviations.md 已核：§69/§111（RC 页绑主存页）、§93（空缺号禁引）、§120（本票只动负值臂）、§158、§162
均不覆盖本 11 枚下界闸。全仓 grep「IntRangeValidation 补闸」「启动定界臂」议题零命中。

未尽面：
1. C# IntRangeValidation 全表共 46 处（Options.cs grep 确数），本票只立「rust 在场 i32 字段且闸缺失」的
   11 枚确证项；同属性族另三枚待裁：aof_commit_ms（:593 Option<i32>，C# :247 (-1,max) 下界缺，现仅
   组合闸 :1419-1428）、aof_size_limit_enforce_frequency_secs（:996 u64 下界型域等形、上界 C# i32::MAX
   缺）、tls_cert_refresh_freq（:554 u64 同上界形）——建议随本票族一并裁或另席盘点。
2. metrics-sampling-freq（C# :358 (0,max)）rust 高边闸在、负臂缺（i64 域，先例票只裁 0 档保留未涉负值）。
3. 域外或删员旋钮（pagecount/readcache-pagecount §69/§111 在册、pmt、threads 族、device 族、reviv 族、
   aof-physical-sublog-count、aof-replay-task/drift/barrier 族（drift 双旋钮 §158 恒默认在册）、
   logger-freq、checkpoint-throttle/fast-commit/network-send、vector-set-replay-task-count 自述不落地）
   的逐枚对位盘点未在本票展开。
4. cluster-timeout/gossip-delay/cluster-config-flush-frequency（C# :298/:294/:302）投影在集群参数域
   （非 NodeArgs 本表），其闸缺失与否未盘，另案。
5. lua_allowed_functions 合并语义分叉：C# UnionWith 并集 setter（Options.cs:642-677）加
   ServerSettingsManager.cs:128-146 列表恢复循环（文件∪CLI），rust override_explicit 整体替换
   （node_options.rs:1662-1762）——列表型旋钮合并语义族待另票裁。
