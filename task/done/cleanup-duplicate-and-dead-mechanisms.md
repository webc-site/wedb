# 清理重复实现、发明别名 API 与死模块归档

任务目标
清理多处重复实现、发明别名 API 与死模块，确保全仓单一机制，对标 C#，消除向下兼容与遗留负担。

已完成改动

1. 清理 wnode/src/inputs.rs 死模块
彻底删除 wedb/wnode/src/inputs.rs（包含 StringInput、UnifiedInput、CustomProcedureInput）。
从 wedb/wnode/src/lib.rs 中删除 mod inputs 以及 StringInput、UnifiedInput、CustomProcedureInput 的重新导出。
全仓输入统一收敛至 ReplayInput 与 SessionParseState。
在 js/check/ignore/server.yml 中将 libs/server/InputHeader.cs 登记为整文件忽略，说明淘汰遗留结构体。

2. 清理 wpubsub 发明别名 API
在 wedb/wpubsub/src/subscribe_broker.rs 中，删除 publish_to_channel、publish_fast、publish_to_pattern 三个同构套壳别名函数。
对标 C# SubscribeBroker.cs 仅保留 Publish / PublishNow。
subscribe_broker.rs 内部单测统一改用标准 publish。

3. 收敛 wkv 集合版本字典为 wbase 单一真源
在 wedb/wkv/Cargo.toml 中为 wbase 依赖开启 map feature。
在 wedb/wkv/src/store/mod.rs 中删除重复自建的 new_key_id_versions_map 函数与 GxBuildHasher 手写构建。
KeyIdVersionsMap 改为 wbase::map::ConcurrentMap<u64, (u64, bool)> 类型别名。
版本字典实例统一采用 wbase::map::new_concurrent_map() 构造。

4. 错误文案单点收敛
在 wedb/wresp/src/cmd_strings.rs 中新增权威常量 RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS。
在 wedb/wlua/src/strings.rs 中，ERR_WRONG_NUMBER_OF_ARGS 统一引用 wresp::cmd_strings::RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS。
在 wedb/wnode/src/resp/bitmap/bitmap_commands.rs 中，删除本地私有 RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS 常量定义，改用 wresp::cmd_strings::RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS。

5. 任务清单闭环
从 next/glm.md 中删除对应已闭合的四.4、四.7、四.10、四.11 对应条目并重排序号。

验证指标

1. bun ./js/check.js
0 缺失，0 重复，检查全部通过。

2. ./clippy.sh
0 警告，静态代码检查通过。

3. ./test.sh
2098 passed, 0 failed，全部单元测试与集成测试通过。

子代理审查
调用子代理按 ./.agents/skills/rust_review/SKILL.md 规范进行深度代码审查。
结论为通过，无潜在 bug，无冗余代码与别名，符合零拷贝与现代 Rust 规范。
