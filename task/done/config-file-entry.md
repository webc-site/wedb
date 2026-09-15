# 配置文件入口与配置面加载接线 (config-file-entry)

对应待办：next/design.md 条 13、next/glm.md 条 12

## 甄别结论

### 问题一：配置文件入口断头（成立，接线而非删除）

C# 语义对标 libs/host/ServerSettingsManager.cs:TryParseCommandLineArguments：

默认值（defaults.conf 嵌入资源）→ ConfigImportPath 文件覆盖 → 命令行显式项覆盖。

三层合并，命令行解析两遍，第二遍以文件合并后的对象为工厂，仅覆盖显式给出的项。

Rust 侧 SKILL 钦定 nested_text 单格式（C# GarnetConf/RedisConf 双格式与
ConfigFileType 枚举不转写，hosting.yml 已登记理由）。

三个函数现状签名甄别：

- from_args：改语义为三层合并解析，返回 Result（原直接 Self::parse()，全仓零调用，无破坏面）
- from_nested_text_str：保留，文件解析单点
- from_file：保留，内部转 from_nested_text_str

实现：

- NodeArgs 增 config / config_export_path 两个 clap 字段（serde skip，
  对标 C# Options.cs:483-506 的 JsonIgnore 语义；Azure 存储导入导出不转写）
- 新增 ConfigFileArgs trait：from_args_iter（默认实现：command 解析 matches
  → from_layered_matches）+ from_args（进程参数入口）；NodeArgs /
  ClusterArgs / StandaloneArgs 各自实现
- 合并核心 NodeArgs::override_explicit：以 ArgMatches::value_source 判定
  显式项；clap_derive 的 arg id 为字段名原样（item.rs:Item::id →
  Name::ToTokens 原样输出 snake_case，long 才是 kebab），故以
  stringify!(字段名) 作查询键，kebab 字段（--wal-dir 等）测试覆盖
- 两 main 由 Struct::parse() 改为 ConfigFileArgs::from_args_iter(env)
- 导出：--config-export-path 指定后将合并后的生效配置写 nested_text
  （C# 仅导出非默认项，Rust 导出全量字段，差异登记）

### 问题二：配置面加载接线（部分成立，只补有运行时消费的项）

补齐项（每项核实过运行时消费点）：

- slowlog-log-slower-than（Options.cs:351，默认 0）→
  RuntimeServerOptions.slow_log_threshold → RuntimeServerConfig 槽位 →
  handle_slow_log 已读（metrics_commands.rs:97）
- slowlog-max-len（Options.cs:355，默认 128 = GarnetServerOptions.cs:292）→
  SlowLogContainer 装配。C# StoreWrapper.cs:243 无条件创建；Rust 生产从未
  接线（set_slow_log_container 零调用），本次打通：provider 无条件构造 +
  会话 attach（RespSessionConsumer::set_slow_log_container）
- max-databases（Options.cs:688，默认 16）→ 单机会话
  RespServerSessionOptions.max_databases；集群维持 CLUSTER_MAX_DATABASES=2
  （C# AllowMultiDb = !EnableCluster，GarnetServerOptions.cs:620）
- protected-mode（Options.cs:602，默认 yes）→ bind 未显式时的回退域：
  保护回环 / 非保护 0.0.0.0（Format.cs:TryParseAddressList 对标；C#
  defaultBindAny 为双栈 any，Rust 收敛 IPv4 any，差异登记）。bind 字段
  String → Option<String>（DEFAULT_BIND 常量保留为回环回退值）
- object-scan-count-limit（Options.cs:590，默认 1000）→
  RuntimeServerOptions → SCAN 计数上限（shared_object_commands.rs:222 已消费）
- metrics-sampling-freq（Options.cs:359，默认 0 禁用）→ ServerBootstrap
  metrics_sampling_frequency 构建器（已存在，main 此前未接线）

运行时单一真源投影：NodeArgs::runtime_server_options()（对标
Options.cs:GetServerOptions 服务选项装配段）。with_node_args 与新增
StorageSessionProvider::with_runtime_server_options 共用，杜绝双份映射。

拒绝补齐项（无运行时消费或架构差异，详见 js/check/ignore/hosting.yml 理由栏）：

- reviv 系 8 项：wreviv FreeRecordPool 常开无开关（wkv store 恒建）
- index-resize-freq/threshold：Rust 无 Tsavorite 哈希索引（BfTree 拓扑）
- max-inline-key/value-size、initial-io-record-size：无 hybrid log inline 概念
- aof-memory/page/segment-size：waof WalLog 自有分段，RuntimeServerOptions
  同名项仅 CONFIG GET 只读展示
- aof-physical-sublog-count / aof-replay-task-count 等多日志拓扑项：
  生产装配恒单物理日志（glm.md 条 26 另案）
- aof-size-limit / aof-size-limit-enforce-frequency：槽位已声明但无自动
  检查点消费者
- latency-monitor：set_global_latency_metrics 生产零调用
- minthreads/maxthreads/miniothreads/maxiothreads：.NET 线程池专属，
  compio 一线程一核
- use-azure-storage 系、aad 系：红线禁用项

### 配置文件承载范围

nested_text 配置文件键 = NodeArgs 字段（serde 命名，Option 字段空串即
None、bool 支持 yes/no）。ClusterArgs 集群扩展参数（cluster-config-file /
cluster-node-timeout-ms / gossip-delay-secs / gossip-sample-percent）仅
命令行面，配置文件不支持（nested_text 无 flatten 保证，NodeArgs 域已全量
可文件化）。

## 改动范围

- wedb/wconf/src/node_options.rs：config / config_export_path 字段、
  ConfigFileArgs trait、override_explicit 合并、runtime_server_options
  投影、六项新活选项、bind Option 化、NodeOptionsError::Cli
- wedb/wconf/src/runtime_server_options.rs：slow_log_max_entries 字段
  （默认 128，单一常量源 DEFAULT_SLOW_LOG_MAX_ENTRIES）
- wedb/wconf/src/lib.rs：ConfigFileArgs 等导出
- wedb/wnode/src/service.rs：StorageSessionProvider::slow_log_container
  字段 + with_runtime_server_options + 会话 attach；with_node_args 复用投影
- wedb/wnode/src/resp/resp_session_consumer.rs：set_slow_log_container 委托
- wedb/wedb/src/main.rs、args.rs：from_args_iter 接线、metrics 采样接线、
  ClusterArgs 实现 ConfigFileArgs
- wedb/wedb_standalone/src/main.rs：from_args_iter 接线、max_databases
  注入会话选项、metrics 采样接线、StandaloneArgs 实现 ConfigFileArgs
- 测试：wconf node_options 单测 5 项新增（三层合并、kebab 字段覆盖、
  protected-mode 回退、导出往返、投影）；wnode/tests/runtime_options_entry.rs
  装配测试 2 项；standalone/cluster 端到端测试 2 项
- js/check/ignore/hosting.yml：三层合并对标落点 + 冷门项不转写清单

## 验证结果

1. ./clippy.sh：0 警告（fixrs + nightly fmt + clippy -D warnings，
   禁 allow）
2. ./test.sh：全量通过——wedb 2027 tests + 1 skipped（既有忽略），regress
   2 tests 全过
3. bun ./js/check.js：0 缺失 0 重复
4. 端到端链路验证：nested_text 文件加载 → CLI 显式覆盖 →
   runtime_server_options 投影 → RuntimeServerConfig 槽位生效 +
   慢日志容器容量生效（runtime_options_entry.rs 断言）
5. 合并 dev（错误枚举收敛、with_node_args 签名 crate::Result 冲突已解），
   合并后全量测试复跑通过；已合入主目录 dev，worktree 与分支已清理

## 备注

- NodeArgs.compaction_freq_secs 死旋钮属 next/glm.md 条 27（另案），本轮未动
- 全局 cargo 配置 target-dir=/tmp/rust 被多 worktree 共享，验证改用私有
  target 目录规避脏编译
