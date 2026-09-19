只读投影字段断供修复：runtime_server_options() 补齐 dir/wal_dir/unixsocket/aof/aof_size_limit 五项投影，INFO appendonly 改走只读回落

来源：next/runtime-options-read-only-path-fields-unprojected.md（qcode10.design 条 6）。
取证基线：dev7/ro-path-project 分支（fork 自 dev HEAD 9cd09e14），行号按符号在当下代码复核。

结论

判定成立。C# 的 CONFIG GET 只读投影（DIR/LOGDIR/UNIXSOCKET/APPENDONLY/AOF_SIZE_LIMIT）直读
启动装配产物 GarnetServerOptions；rust 的 RuntimeServerOptions 同名五字段在启动投影函数
NodeArgs::runtime_server_options 中漏投，全程停在 Default 值，故 CONFIG GET dir/logdir/
unixsocket/appendonly/aof-size-limit 恒吐空串或 "no"，与实态相反。附带一处同源断链：
INFO 的 appendonly 经 get_bool 读只读项的空槽位（只读项无槽位、播种恒 0），即使补齐投影
也读不到，C# 该处是直读 serverOptions.EnableAOF，故改为经 resp_format 走只读格式化回落，
与 CONFIG GET 同一机制、同一真源。

实现

1. 投影单点：wedb/wconf/src/node_options.rs:runtime_server_options（对应 C#
   libs/host/Configuration/Options.cs:GetServerOptions 装配段）补五项——
   checkpoint_base_directory ← dir（C# Options.cs:910 CheckpointDir →
   GarnetServerOptions.cs:625 CheckpointBaseDirectory 回落根，rust 单根无二级回落）；
   log_dir ← wal_dir() 口径单点（C# Options.cs:909 LogDir 设备根）；
   unix_socket_path ← unixsocket（C# Options.cs:1030，绑定侧 endpoints() 同读该字段）；
   enable_aof ← aof（C# Options.cs:921）；aof_size_limit ← aof_size_limit 原样字符串
   （C# Options.cs:935，行为侧仍走 aof_size_limit_bytes() 单点解析）。
   路径落 String 统一 .display().to_string()（沿用 wedb/src/args.rs:cluster_config_path
   现役写法），仅此一处定口径。
2. INFO 读侧：wedb/wnode/src/resp/info_provider.rs:SessionInfoSource::facts 的 enable_aof
   由 get_bool(AppendOnly)（读永不播种的槽位）改 resp_format(AppendOnly)（只读格式化直读
   启动选项，对应 C# libs/server/Metrics/Info/GarnetInfoMetrics.cs:84 直读
   storeWrapper.serverOptions.EnableAOF）。未新增 pub 面。
3. 测试：wedb/wconf/src/node_options.rs:test_runtime_server_options_projection 补五字段
   注入断言与缺省断言（缺省 dir=./data 恒非空，unixsocket/aof_size_limit 未配置为
   None/空，格式器吐 ""/no，与 C# 空串/false 口径一致）；test_config_export_round_trip
   补 --config-file 形态经投影等于入参；wedb/wnode/tests/runtime_options_entry.rs 补
   CONFIG GET 端到端断言（provider.runtime_config.resp_format 五名），钉死配置进→展示出。

不裁定（留单原范围外）：enable_cluster 与 aof 物理布局诸旋钮 rust 无 CLI 对位，不投影；
aof_memory_size/aof_page_size/aof_segment_size 归 next/aof-size-knobs-read-side-wiring.md。

验证：cargo check --workspace --all-targets 零 error 零 warning。
