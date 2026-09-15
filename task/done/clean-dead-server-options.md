# 清理死启动配置面 (clean-dead-server-options)

对应待办：next/glm.md 待办 30

## 任务背景
历史实现中 wedb/wconf 存在三套配置面定义：
1. server_options.rs (ServerOptions)
2. garnet_options.rs (GarnetServerOptions)
3. node_options.rs (NodeArgs) 与 runtime_server_options.rs (RuntimeServerOptions) / runtime_server_config.rs (RuntimeServerConfig)

其中 ServerOptions 与 GarnetServerOptions 全仓生产引用为 0，生产完全走 NodeArgs 与 RuntimeServerConfig / RuntimeServerOptions。
两套死配置面引入了死字段、未用参数（如 _prop_name）及向下兼容逻辑，违背无向下兼容与代码精简原则。

## 改动内容

1. 彻底删除死模块与死结构体：
彻底删除 wedb/wconf/src/server_options.rs 与 wedb/wconf/src/garnet_options.rs。
从 wedb/wconf/src/lib.rs 中移除上述两个模块的声明与全部二次导出（严禁二次导出死结构体）。
彻底删除 AofLogSettings、StoreSettings、OptionsError、ServerOptions、GarnetServerOptions 等废弃类型。

2. 换算工具与常量收敛：
将容量换算与位运算工具（parse_size, pretty_size, previous_power_of_2, next_power_of_2, log2_exact, try_parse_size 等）统一收敛在 wedb/wconf/src/size.rs (wconf::size) 模块中。
将 MIN_PAGE_SIZE_BYTES 定义在 wconf::size 模块。
将 DEFAULT_RESP_VERSION 收敛定义在 wedb/wconf/src/node_options.rs 活配置面，作为全系统单一真源。
修改 wedb/wnode/src/resp/config_commands.rs 引用为 wconf::size。

3. 路径推导方法迁移：
将原 GarnetServerOptions 上的检查点与 AOF 目录推导方法迁移并优化至 RuntimeServerOptions：
- store_checkpoint_base_directory
- get_checkpoint_directory_name
- get_store_checkpoint_directory
- get_append_only_file_directory_name
- get_append_only_file_directory
结合 itoa::Buffer 与 with_capacity 实现零堆分配和精准预分配。

4. 单测重构与忽略列表适配：
重构 wedb/wconf/tests/garnet_server_config_tests.rs，删除针对 ServerOptions / GarnetServerOptions 的无效测试，改为直接覆盖活配置面：
- RuntimeServerOptions 默认字段覆盖测试
- NodeArgs 默认值测试
- RuntimeServerConfig 初始化与槽位播种测试
- RuntimeServerOptions 目录路径推导测试
- wconf::size 尺寸解析与 2 的幂工具测试
在 js/check/ignore/server.yml 中整文件登记淘汰 ServerOptions.cs 与 GarnetServerOptions.cs 的理由。
在 js/check/ignore/test.yml 中登记淘汰关联测试方法的理由。

## 验证结果
1. bun ./js/check.js：0 缺失 0 重复
2. ./clippy.sh：0 警告
3. ./test.sh：全量测试通过
