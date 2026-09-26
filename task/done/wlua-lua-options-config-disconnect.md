甄别结论：通过（甄别席 zc-fix-r16-luaopts，2026-09-26）定级 P2
核验记录（双侧锚逐条现码复跑）：
1. C# 契约面成立：Options.cs:642/:647/:655/:664 四旋钮逐字在册（:645 ForbiddenWithOption Native 互斥、:649 IntRangeValidation(10,int.MaxValue) isRequired:false 均在位）；:1029 五参数整体装配 new LuaOptions 且 timeout==0 映射 InfiniteTimeSpan 坐实；defaults.conf 五键实为 :500-:512（票面 500-510 有 2 行末位漂移，LuaAllowedFunctions 在 :512，不构成反证）；SessionScriptCache.cs:66-70 逐字段下传 LuaRunner 成立；LuaOptions.cs:47 GetMemoryLimitBytes 的 Native 忽略+warn 与 [1K,2GB] 值域闸逐字核实。
2. Rust 断链面成立：wconf node_options.rs:871-881 lua 段确仅 enable_lua+lua_script_timeout_ms 两参数；attach.rs:93-96 为生产唯一 LuaOptions 字面构造点（全仓 grep 仅此一处非 wlua 测试面构造，:67 为 Default 测试面），四字段恒 default 坐实；wlua options.rs:44 <=0 首闸恒 None→runner/mod.rs:129 Native 矛盾臂与 :136-143 Tracked/Managed 分配器臂、functions/redis.rs:109-116 Disable/Silent 两态、loader.rs:390 allowed_functions 裁剪均生产不可达，commands.rs:95 runner_options 逐字段下传在位——配置端补字段即全链激活论断成立。
3. 非重复成立：deviations §97/:111/:112/:139/:149 逐条对读均非本面（§111 五组收形不含 lua 族）；hosting.yml:113-119 仅 LuaTransactionMode 删员；task 四池与 review_history r15-contract「33 槽位判净」（其自陈仅覆盖已有旋钮假接线）无同轴票。
4. 超时防线论断成立：wlua/src/timeout.rs 头注自证 safepoint 原子哨兵读形态（仅拦每指令点），分配型脚本防线确在 allocator 层，配置断链即防线失效。
5. 方案合规：wconf→attach 单点装配→wlua 消费单向分层；值域闸复用 wlua options.rs 单点不复刻、无第二装配点、默认真源唯一（LuaOptions::default 已载 Native/0/Enable/空）；max(0) 钳制随启动校验前置转死可删；四旋钮平铺 clap+toml 派生沿既有机制，strum 枚举拒启形有 connection_protection_option 先例，无过度设计无假桩。
6. 微瑕不阻：wconf 现无 value_delimiter 逗号 Vec 先例（clap 原生能力，落码即用）；C# LuaOptions.cs:21 库级构造默认 Silent 与 rust 默认 Enable 之差异已系 zcode-r30-defaults 在册裁决（options.rs:10-11 注释自陈），非本票新分叉。

审核结论：通过（审核席 zcode-r17-review-luaopt，2026-09-26）

审核亲验记录（双侧锚点逐一复跑，全部属实）：
a) C# 契约面：Options.cs:642/:647/:655/:664 四旋钮在册，:651 timeout 带 IntRangeValidation(10, int.MaxValue) 且 isRequired: false（0 特例 disabled 由装配链 :1029 `== 0 ? InfiniteTimeSpan` 承接），:1029 五参数整体装配 new LuaOptions(...)；defaults.conf:500-510 五键全在册（LuaLoggingMode 默认 "Enable"）；SessionScriptCache.cs:66-70 逐字段下传 LuaRunner；LuaOptions.cs:47 GetMemoryLimitBytes 的 Native 忽略 + [1K, 2GB] 双闸逐字核实。
b) Rust 断链面：wconf node_options.rs lua 段仅 enable_lua + lua_script_timeout_ms 两参数；全仓 grep 四旋钮词（lua-memory/lua_logging/lua_allowed）唯命中 wlua 消费端实现与测试；attach.rs:93-96 为生产唯一 LuaOptions 构造点（attach.rs:67 default 为测试面），四字段恒 default（memory_mode=Native/limit=0/log_mode=Enable/allowed_functions 空）。
c) 生产不可达链坐实：limit 恒 0 → options.rs:44 get_memory_limit_bytes 首闸 <= 0 恒 None → runner/mod.rs:129 Native+limit 矛盾臂永不可达、:136-143 Tracked/Managed 分配器分支永不可达；log_mode 恒 Enable → functions/redis.rs:109-119 Silent/Disable 两态死代码；allowed_functions 恒空 → loader.rs:390 恒走默认集。commands.rs:95 runner_options 已从 lua_options 逐字段下传——配置端补字段即全链激活，无需动消费端。
d) 查重：deviations.md lua 相关条目 §97（SET/GET 错误折叠）/§112（number 转串）/§139（EXEC 重入锁）/§149（keynum 槽位门）均非本面；§111 五组收形（尺寸/reviv/缓冲池/pagecount/tree_cache）不含 lua 族且其划界声明明示补旋钮归独立代码票；hosting.yml:113-119 仅 LuaTransactionMode 删员登记（彼系事务上下文内核不存在整链不转写，与本票消费端已备仅断配置端性质不同）；task 四池与 review_history r15-contract 33 槽位判净（只覆盖 rust 已有旋钮）均无同面登记。本票为 r17-lua 审查席立案原件。
e) 维度命中：板块 5.2「配置全链路真接线，严禁只读不用的假旋钮」+ 板块 1「零死代码与假桩清退」（本票为该维度反向形：消费端全链已备、配置端零暴露，LuaTrackedAllocator/LuaLimitedManagedAllocator/Silent/Disable/loader 裁剪全成生产死代码面）。超时管理器只能拦 CPU 死循环（safepoint 轮询）、拦不住超时前快速分配型脚本，内存配额防线失效的判断成立。

整理优化执行方案（供 task/fix.md 直接消费）：
1. wconf node_options.rs 补四参数（均平铺 clap #[arg] + #[toml(default)]，嵌套配置随派生自动纳入 TOML 导入导出面，无第二套机制）：
   lua_memory_management_mode：strum 枚举 LuaMemoryManagementMode（wlua 侧类型或 wconf 本地镜像再投影，CLI 形 lua-memory-management-mode，默认 Native）
   lua_script_memory_limit：Option<String> 尺寸串（CLI 形 lua-script-memory-limit，默认 None；wconf 复用 try_parse_size 解析为字节）
   lua_logging_mode：LuaLoggingMode 枚举（CLI 形 lua-logging-mode，默认 Enable——defaults.conf:509 在册生效默认，与 LuaOptions::default 同值同源）
   lua_allowed_functions：Vec<String>（CLI 形 lua-allowed-functions，value_delimiter = ','，默认空）
2. 启动校验单点（wconf 既有校验核形态）：lua_script_memory_limit 与 lua_memory_management_mode = Native 同时设置拒启（对齐 C# ForbiddenWithOption）；lua_script_timeout_ms 负值或 0 < 值 < 10 拒启（对齐 IntRangeValidation(10, int.MaxValue)，0 = disabled 保留现状映射无限）。
3. attach.rs 装配臂（:93-96 单点，模块头自陈「新增旋钮只此一处」）四字段投影进 LuaOptions：memory_mode/log_mode 直投影；尺寸串已由 wconf 解析为 i64 字节量投影 lua_memory_limit_bytes，值域闸（[1K, 2GB] 越界忽略 + warn）复用 wlua options.rs get_memory_limit_bytes 现有单点，不在装配点复刻第二道闸；allowed_functions 直投影。attach.rs 的 max(0) 钳制随校验前置后可删（负值已在启动校验拒启，钳制成死逻辑）。
4. 测试验证点：wconf 单测锁四旋钮 CLI/toml 双形态解析、Native+limit 互斥拒启、timeout 值域拒启；wnode 端到端（对齐 wlua lib.rs:319 allocator_quota_stops_runaway_script 语义）以 Tracked + limit 配置跑 EVAL 大分配脚本断言配额错误帧；LuaLoggingMode::Disable 形态 redis.log 回 ERR_LOGGING_DISABLED 错误帧、Silent 形态静默零日志断言。
5. 禁改红线：wlua 消费端（options.rs/runner/loader/functions）零改动——实现已全链齐备；不建第二装配点；默认值真源唯一（LuaOptions::default 已载 Native/0/Enable/空，配置端缺省形态不得另立常量）。

原始票面（未改动，下承）：

Lua 脚本选项配置断链：C# 四个 lua 旋钮（内存模式/内存限额/日志模式/允许函数集）rust 配置端零暴露，LuaOptions 恒 default，内存配额与沙箱收窄防线生产不可达

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   garnet/libs/host/Configuration/Options.cs:642 定义 `--lua-memory-management-mode`（LuaMemoryManagementMode，OptionValidation 枚举），:647 定义 `--lua-script-memory-limit`（MemorySizeValidation 尺寸串 + ForbiddenWithOption 与 Native 互斥），:655 定义 `--lua-logging-mode`（LuaLoggingMode 枚举），:664 定义 `--lua-allowed-functions`（逗号分隔函数集）。:1029 装配链把五参数（含 :651 LuaScriptTimeoutMs）整体传入 `new LuaOptions(...)`，成为 storeWrapper.serverOptions.LuaOptions；SessionScriptCache.cs:67-70 逐字段下传进每个 LuaRunner（构造选 LuaStateWrapper 的 Tracked/Managed 分配器、redis.log 行为、沙箱导出函数集）。即 C# 契约：运维可按部署形态配置脚本内存配额（防失控脚本吃尽进程内存）、redis.log 管控三态、沙箱函数白名单。另 :651 `--lua-script-timeout` 带 IntRangeValidation(10, int.MaxValue)，负值/过小值拒启（0 特例 = disabled）。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   rust 配置端 wedb/wconf/src/node_options.rs:871-881 仅两参数：enable_lua 与 lua_script_timeout_ms；同文件其余冷门旋钮（enable-vector-set-preview:889、aof-page-size:908 等）均全量转写，唯 lua 族缺此四项。装配端 wedb/wnode/src/resp/resp_server_session/attach.rs:93-96 `LuaOptions { timeout_millis: node.lua_script_timeout_ms.max(0), ..LuaOptions::default() }`，生产唯一构造点（attach.rs:67 default 为另一构造点，测试面），memory_mode 恒 Native、lua_memory_limit_bytes 恒 0、log_mode 恒 Enable、allowed_functions 恒空。消费端实现已全链齐备：wedb/wlua/src/options.rs:43 get_memory_limit_bytes（Native 忽略/[1K,2GB] 值域双闸，逐字对齐 C# LuaOptions.cs:GetMemoryLimitBytes）、wedb/wlua/src/runner/mod.rs:127-143 LuaRunner::new 的 Tracked/Managed 分配器分支、wedb/wlua/src/loader.rs:390 prepare_loader_block_bytes 的 allowed_functions 沙箱裁剪、wedb/wlua/src/functions/redis.rs:109 redis.log 三态门——全部仅测试可达，生产配置永不可达。js/check/ignore/hosting.yml:113-119 仅登记 LuaTransactionMode 一项删员依据（task/done/lua-txn-mode-drop-placeholder.md），四旋钮零登记；doc/zh/deviations.md 零登记；task/review_history r15-contract 的「33 槽位消费判净」只覆盖 rust 已有旋钮的假接线面，不覆盖 C# 有 rust 无的漏项面。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   内存配额防线失效：Tracked/Managed 模式 + limit 是 C# 防失控脚本（死循环构造大表/大串）耗尽进程内存的运维防线，rust 侧 LuaTimeoutManager 只能中断 CPU 死循环，无法拦截快速分配型脚本（短时间海量分配未触发超时 safepoint 前即 OOM），Native 模式下 LuaRunnerInitError 矛盾臂（runner/mod.rs:129）永不可达。零死代码纪律违例：LuaTrackedAllocator/LuaLimitedManagedAllocator 两文件、LuaOptions::get_memory_limit_bytes、loader allowed_functions 过滤、LuaLoggingMode::Silent/Disable 两态成生产死代码面。redis.log 管控缺失：多租户生产环境脚本可经 redis.log 向服务端日志注入任意文本（C# 可配 Silent/Disable 关闭），日志污染与磁盘占用面失控。附带：lua_script_timeout_ms 负值 rust 静默折无限（max(0) + >0 门），C# IntRangeValidation 拒启，非法配置静默吞。

涉及代码：
rust 文件与函数：
wedb/wconf/src/node_options.rs:NodeArgs（enable_lua/lua_script_timeout_ms 字段段，缺四旋钮）
wedb/wnode/src/resp/resp_server_session/attach.rs:From<&NodeArgs> for RespServerSessionOptions（lua_options 装配臂）
wedb/wlua/src/options.rs:LuaOptions（消费端已备）

对应 c# 文件与函数：
garnet/libs/host/Configuration/Options.cs:Options（LuaMemoryManagementMode/LuaScriptMemoryLimit/LuaLoggingMode/LuaAllowedFunctions 属性与 GetServerOptions 装配）
garnet/libs/server/Lua/LuaOptions.cs:LuaOptions（构造与 GetMemoryLimitBytes）

精炼执行方案：
1. wconf node_options.rs 补四参数：lua_memory_management_mode（strum 枚举 LuaMemoryManagementMode，CLI 形 lua-memory-management-mode）、lua_script_memory_limit（尺寸串 Option<String>，与 memory-mode=Native 互斥校验对齐 C# ForbiddenWithOption）、lua_logging_mode（LuaLoggingMode 枚举）、lua_allowed_functions（逗号分隔 Vec<String>）；toml 派生随字段自动纳入导入导出面；lua_script_timeout_ms 补启动校验（负值或 0<值<10 拒启，对齐 C# IntRangeValidation(10, int.MaxValue)+0 特例）。
2. attach.rs 装配臂四字段投影进 LuaOptions（尺寸串解析为字节，解析单点收 wlua options.rs 现有 [1K,2GB] 值域闸，非法值启动拒）。
3. 测试验证点：wconf 单测锁四旋钮 CLI/toml 双形态解析与互斥校验；wnode 测试以 Tracked+limit 配置跑 EVAL 大分配脚本断言配额错误帧（对齐 wlua lib.rs allocator_quota_stops_runaway_script 语义的生产链端到端）；LuaLoggingMode::Disable 形态 redis.log 回错误帧断言。

合入哈希：24709e3
收口形态：wconf 补 Lua 四旋钮（lua-memory-management-mode / lua-script-memory-limit / lua-logging-mode / lua-allowed-functions，平铺 clap + toml 派生自动纳入导入导出面）与 lua-script-timeout 值域、限额- Native 互斥两道启动校验拒启，attach.rs 单点把五参数逐字段投影进 LuaOptions（删 max(0) 死钳制、限额 [1K,2GB] 值域闸复用 wlua get_memory_limit_bytes 单点），恒 default 假旋钮形态消灭，LuaTracked/LimitedManaged 分配器、redis.log Silent/Disable、loader 沙箱裁剪全链生产可达。
