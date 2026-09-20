# Vector Set 量化与重放任务数配置项支持

来源：next/zcode-r6-cli.md 问题 10

## 问题

Vector Set 的 quantization_task_count 硬编码为 4，replay_task_count 缺少配置面。
C# 支持配置且 0 表示按 CPU 核心数自动对齐。

## 涉及路径

- wedb/wnode/src/resp/vector/vector_manager.rs
- wedb/wconf/src/node_options.rs

## 解决建议

1. 在 NodeArgs 中支持 vector-set-quantization-task-count 与 vector-set-replay-task-count 参数。
2. 0 或缺省时对齐系统物理 CPU 核心数。
