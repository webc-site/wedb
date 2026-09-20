# resp_commands_info_data.rs 简化并内联至 RespCommand

来源：next/zcode.design.md 问题 7

## 问题

wnode/src/resp/resp_commands_info_data.rs 仅包含两个极薄的包裹函数：
resp_command_from_cs_name 与 resp_command_to_cs_name。
独立建文件增加了不必要的模块层级。

## 涉及路径

- wedb/wnode/src/resp/resp_commands_info_data.rs
- wedb/wresp/src/command/mod.rs

## 解决建议

1. 将两个函数内联为 RespCommand 的关联方法，或者在相关调用点直接使用 RespCommand::from_str / as_ref。
2. 删除孤立文件 resp_commands_info_data.rs。
