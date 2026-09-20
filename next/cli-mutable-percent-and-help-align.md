# mutable-percent 默认值确认与 CLI 帮助文档单位说明对齐

来源：next/zcode-r6-cli.md 问题 4 与问题 13

## 问题

1. mutable-percent 在 C# defaults.conf 中生效值为 90，Rust 当前基线为 0.5，且注释中存在口径不一致。
2. 部分 CLI 参数键名单位存在变体（如 cluster-node-timeout-ms），help 文档中未明确标明单位。

## 涉及路径

- wedb/whlog/src/config.rs
- wedb/wconf/src/node_options.rs
- wedb/wedb/src/args.rs

## 解决建议

1. 确认 mutable-percent 基线值（推荐对齐 0.9 并更新注释）。
2. 在 NodeArgs 与 ClusterArgs 的 help 描述中明确注明毫秒/字节等物理单位。
