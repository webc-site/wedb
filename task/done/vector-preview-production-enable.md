向量域生产开关装配：EnableVectorSetPreview 配置面与条件装配缺失

来源：第 7 轮 design 补跑 HIGH 条（台账自述「VectorManager 构造硬编码 is_enabled=false」）。
按主仓 dev HEAD 复核：硬编码已改为选项字段，但生产效果未变——整栈仍不可达。

现状
- wedb/wnode/src/resp/vector/vector_manager.rs:148-153 VectorManagerOptions 字段 is_enabled（derive Default → false）。
- wedb/wnode/src/service.rs:716-717 生产唯一构造点取 VectorManagerOptions::default()，全仓生产代码无任何
  is_enabled: true 写点（grep `is_enabled` 命中仅剩 wkv 读缓存与测试构造体 wnode/tests/resp_vector_set.rs 等）。
- 消费面因此恒假：wedb/wnode/src/resp/vector/resp_server_session_vectors.rs:317/:679/:933/:993/:1007/:1022/
  :1043/:1105/:1127/:1154/:1197/:1226 十二处命令臂一律回 abort_disabled（:293 统一文案 ERR_VECTOR_SET_DISABLED，
  定义于 wedb/wnode/src/resp/vector/mod.rs:4-5）；
  wedb/wnode/src/resp/vector/vector_manager.rs:925/:1078、vector_manager_migration.rs:106/:183、
  vector_manager_context_metadata.rs:369 同门。
- 配置面零承接：wedb/wconf/src/node_options.rs:218 NodeArgs 无向量开关字段（grep vector 零命中于该文件），
  nested_text 导出面亦无该项。

C# 参考
- garnet/libs/host/Configuration/Options.cs:704 EnableVectorSetPreview、:1036 投影进 GarnetServerOptions；
  garnet/libs/host/defaults.conf:533 默认 false。
- garnet/libs/server/Resp/Vector/VectorManager.cs:182 `IsEnabled = serverOptions.EnableVectorSetPreview`。
- garnet/libs/server/StoreWrapper.cs:1054 `if (serverOptions.EnableVectorSetPreview)` 条件装配。

修法
一、wconf NodeArgs 增向量预览开关（默认口径照 C# defaults.conf 取 false 或按本仓「预览即默认开」的既定决策定，
    决策写进字段文档注释），经 ServerArgs 访问器（node_options.rs:807 impl 段）与 nested_text 导出。
二、service.rs:716 构造点改读该配置投影 is_enabled，与 C# StoreWrapper 条件装配同位（开关为假时不建清理/量化
    后台链，为真时全链拉起）。
三、开关为真时的端到端集成测试补一条（走生产构造路径，不再用测试内 is_enabled: true 直构，
    wnode/tests/resp_vector_set.rs 族）。

关联：task/ing/vector-manager-cleanup-launch-dispose.md（清理协程拉起）、
task/ing/vector-registry-nsdb-isolation.md（登记表 (ns,db) 域隔离）——本单只做开关与装配，不重复其内容。

优先级：功能缺口（命令整栈生产不可达），排在死代码/多套架构清理之后、其余打磨之前。

验收
- 生产构造路径下 VADD/VSIM/VMGET 可端到端应答；开关关闭时回 ERR_VECTOR_SET_DISABLED 且无后台链。
- CONFIG/nested_text 导出含该开关；grep 无第二处 is_enabled 写点。

细化方案（f17-vec-preview，已核实 dev HEAD 配置流后定稿）

生产入口核实：boot.rs:66 与 wedb_standalone 均走 StorageSessionProvider::open_from_args
→ open_from_args_with_config 四臂 → provider builder 链（with_requirepass 等）。
provider 四臂 pub 方法外部调用约 140 处（tests），不可改签名 → 开关经装配链注入而非构造参数下传。

1. wconf/src/node_options.rs
   - NodeArgs 增 enable_vector_set_preview: bool（#[arg(long, default_value_t = false)] +
     #[serde(default)]，serde derive 自动纳入 nested_text 导入/导出面）。
     默认口径取 false（照 C# defaults.conf:533 EnableVectorSetPreview: false，
     预览特性未稳定；决策写入字段文档注释，标 Options.cs:704 映射）。
   - ServerArgs trait 增默认访问器 enable_vector_set_preview()（对齐既有 aof() 模式）。
   - test_node_args_defaults 补默认断言；nested_text 往返断言补该项。
2. wnode/src/resp/vector/vector_manager.rs
   - VectorManager.is_enabled: bool → AtomicBool（装配期单次写、运行期只读，
     语义对位 C# VectorManager.cs:182 构造器注入 IsEnabled；不改构造签名，
     VectorManagerOptions.is_enabled 保留为构造初值源，测试直构不受影响）。
   - 增读方法 is_enabled()；替换全部读点：resp_server_session_vectors.rs 12 处、
     vector_manager.rs 925/1078/1090、vector_manager_migration.rs 106/183、
     vector_manager_context_metadata.rs 369。
3. wnode/src/service.rs
   - provider 增 with_vector_set_preview(mut self, enabled: bool) -> Self
     （builder 链式，对位 C# 构造注入位；一处 store 写点）。
   - open_from_args_with_config 链尾注入 .with_vector_set_preview(node.enable_vector_set_preview())。
   - get_session 量化（start_quantization_tasks）与清理（ensure_cleanup_tasks_started）
     协程拉起加 is_enabled() 门控：对位 C# VectorManager.Initialize() 的
     !IsEnabled 早退（量化）与 StoreWrapper.cs:1054 StartReplicaTasks 条件；
     C# 构造器无条件 fire 的三个空转 cleanup 协程在 rust 侧为惰性拉起，
     关闭时不拉起即「无后台链」，语义等价省协程。
4. wnode/tests/ 增端到端测试（生产构造路径，对标 aof_commit_wait_e2e.rs 骨架：
   wnode_test::{start_server, send_cmd, read_line_reply}）
   - 开：NodeArgs { enable_vector_set_preview: true } → open_from_args →
     VADD/VSIM/VMGET 应答非 disabled。
   - 关（默认）：同路径 → VADD 回 ERR_VECTOR_SET_DISABLED。
