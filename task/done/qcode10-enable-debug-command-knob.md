enable-debug-command 三面缺位：DEBUG 门恒 No 使 local 档语义分叉，拒答文案指引一个本构建不存在的选项

来源：qcode 第 10 轮 net 条 4（next/qcode10.net.md:46-48）。取证基线 dev。

问题
- 会话侧三档保护门（Yes/No/Local）、本地连接判定、拒答文案、两处命令臂门槛全部已实装并被读：
  wnode/src/resp/resp_server_session.rs:117-127 ConnectionProtectionOption、:137 选项字段、
  :169 缺省 No、:503 入会话、:948-954 can_run_debug（:949 读门 + :966-970 is_local_connection）、
  :3077-3082 can_run_with_protection；消费点 wnode/src/resp/admin_commands.rs:338、
  wnode/src/resp/txn_resp_commands.rs:304；文案 wresp/src/cmd_strings.rs:304-305
  RESP_ERR_DEBUG_DISALLOWED。
- 唯一缺的是配置到会话的投影：From<&NodeArgs>（resp_server_session.rs:187-215）用
  `..Self::default()` 兜住该字段，wconf 全目录既无 enable-debug-command 旋钮也无对应
  ServerConfigType 槽位（命令行/配置文件/CONFIG SET 三面皆无），
  于是 options.enable_debug_command 恒 No、connection_protection_debug 恒 No。
- 后果一：本机 unix/127.0.0.1 会话执行 DEBUG 一律被拒，与 C# 的 local 档放行语义分叉；
  后果二：拒答文案原样承诺「If the enable-debug-command option is set to "local" …
  otherwise you need to set this option in the configuration file」，而该选项在本构建
  三面均不存在 —— 面向用户的假修复指引。

C# 参考
- libs/host/Configuration/Options.cs:594-595 [Option("enable-debug-command")]
  ConnectionProtectionOption EnableDebugCommand、:1018 投影进 serverOptions
- libs/host/Configuration/Redis/RedisOptions.cs:33-34 redis.conf 同名词
- libs/server/Servers/GarnetServerOptions.cs:561 字段
- libs/server/Resp/RespServerSession.cs:453 会话侧取值
- libs/server/Resp/AdminCommands.cs:737 与 libs/server/Transaction/TxnRespCommands.cs:184 同文案拒答

修法
- 单源三处齐全：wconf 旋钮声明（CLI + redis.conf 别名 enable-debug-command，值域
  yes/no/local，缺省与 C# 一致）→ NodeArgs 解析 → From<&NodeArgs> 投影进
  RespServerSessionOptions；并按 C# :1018 补 CONFIG GET/SET 槽位（若该字段属只读投影，
  按本仓既有只读投影口径写明并注明 C# 锚点，不得静默回落 default）。
- 补线面测试：local 档下本机回环会话 DEBUG 放行、远程会话拒；yes 档全放行；no 档全拒；
  三档文案与 RESP_ERR_DEBUG_DISALLOWED 一致。
- 不做向上兼容：不保留「字段恒 No」的隐式缺省路径，default 只允许经显式配置产生。

甄别细化（fixloop 代理，基线 dev 2026-09-19，票面两处订正后落地）
- CONFIG 面订正：C# RuntimeServerConfig（libs/server/Config/RuntimeServerConfig.cs 全文）
  并未注册 enable-debug-command —— C# 实际只有两面：命令行（Options.cs:594-595
  [Option("enable-debug-command")] ConnectionProtectionOption）与 redis.conf
  （RedisOptions.cs:33-34 RedisConnectionProtectionOption）。Options.cs:1018 是
  GetServerOptions 的 serverOptions 投影而非 CONFIG 槽位。故 rust 不加
  ServerConfigType 槽位（对标 C#，不虚设 CONFIG 面）；本仓无 redis.conf 兼容层
  （transpile 明列 nested_text 唯一配置格式），配置文件面即 nested_text。
- 枚举单源迁移：ConnectionProtectionOption 自 wnode/src/resp/resp_server_session.rs:118
  迁至 wconf（新文件 wconf/src/connection_protection_option.rs，模式对标
  log_compaction_type.rs：#[repr(u8)] 判别值对齐 C# No=0/Local=1/Yes=2、Default No、
  MEMBERS/from_raw/as_name/try_parse 忽略大小写 + 十进制数字；补 Display 小写名
  对标 TypeConverters.cs RedisConnectionProtectionOptionConverter ConvertTo
  ToLowerInvariant、FromStr 供 clap、手写 Serialize/Deserialize 供 nested_text，
  反序列化走 try_parse 与 C# Enum.Parse(ignoreCase) 同口径）。wnode 侧 pub use
  承接，既有引用路径（wnode::resp::* / tests）零改动。redis.conf 专属别名 all
  （RedisTypes.cs:19 All=2=Yes）不设：本仓无 redis.conf 层。
- NodeArgs 旋钮：字段 enable_debug_command（#[arg(long = "enable-debug-command",
  default_value_t = No)] + #[serde(default)]），Default 分支补 No，override_explicit
  over![] 登记（unixsocketperm/network_connection_limit 同模式）。
- 投影：From<&NodeArgs> 显式 enable_debug_command: node.enable_debug_command，
  脱离 ..default() 兜底。
- 测试：三档门行为与文案线面已有（wnode/tests/resp_admin.rs:451-561 Yes/Local×3、
  resp_server_session_tests.rs:516/:529 Local/No），不重复造；补
  node_args_projects_session_options 的投影断言（缺省 No、透传 Local/Yes）。
  本轮 fixloop 流程仅 cargo check，测试编译不执行。

验收
- cargo test -p wconf、cargo test -p wnode --lib、相关线面测试全绿；
  bun js/check.js exit 0；无 #[allow(、无 #[ignore]、无恒真断言。
- 撞车口径：wconf/src/runtime_server_options.rs 与 resp_server_session.rs 并发会话在改，
  冲突即 `git merge dev` 解，解不动提交已完成阶段回报，不要磨。
