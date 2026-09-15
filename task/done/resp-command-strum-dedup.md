# resp-command-strum-dedup

来源：next/clude.md 条目 3（命令名 ↔ RespCommand 双向映射收敛）。

## 背景与痛点

wedb/wnode/src/resp/resp_commands_info_data.rs 原有 764 行代码，其中包含两段庞大的手写 match：
1. resp_command_from_cs_name：366 个分支手写字符串到枚举映射，且每次执行 name.to_ascii_uppercase() 产生堆内存分配。
2. resp_command_to_cs_name：366 个分支手写枚举到静态字符串映射。

两份双向映射冗余重复，维护成本高，新增枚举变体容易遗漏或拼写漂移。

## 重构方案

1. 依赖管理：
在 wedb/wresp/Cargo.toml 中通过 cargo add 引入 workspace 统一声明的 strum，启用 derive 特性。

2. 枚举派生：
在 wedb/wresp/src/command.rs 中，为 RespCommand 派生：
- strum::EnumString
- strum::Display
- strum::IntoStaticStr
- strum::AsRefStr

并配置属性：
#[strum(ascii_case_insensitive, serialize_all = "SCREAMING_SNAKE_CASE")]
自动支持对 C# 枚举名（如 ACL_CAT, APPEND, BITOP_AND, CLUSTER_SEND_CKPT_FILE_SEGMENT, GEORADIUSBYMEMBER_RO 等）的直接双向转换。

3. 函数收敛：
在 wedb/wnode/src/resp/resp_commands_info_data.rs 中：
- 删除 740 行手写 match 分支。
- resp_command_from_cs_name 改为直接调用 RespCommand::from_str(name).ok()，实现原地切片大小写不敏感匹配，消除堆内存分配。
- resp_command_to_cs_name 改为直接调用 cmd.into()，返回 &'static str 静态切片，零拷贝。
- 对外部调用方（JSON 导入导出、命令分派、ACL 解析等）保持无损平滑兼容。

4. 验证测试：
在 wedb/wresp/src/command.rs 与 wedb/wnode/src/resp/resp_commands_info_data.rs 中补充往返转换与大小写容错测试用例。

## 验证结果

- 检查脚本：bun ./js/check.js 输出 0 缺失 0 重复。
- 静态检查：./clippy.sh 保持 0 警告。
- 自动化测试：./test.sh 全量 2019 项测试全部通过。
- 规范审查：子代理按 rust_review/SKILL.md 完成审查，确认无堆分配、符合零成本抽象规范。
