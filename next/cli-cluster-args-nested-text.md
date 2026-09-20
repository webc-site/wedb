# ClusterArgs 扩展参数支持 nested_text 配置文件解析

来源：next/zcode-r6-cli.md 问题 7

## 问题

ClusterArgs 扩展字段（announce ip/port、gossip 参数、node-timeout 等）仅支持命令行输入，
配置文件中指定同名键时被 serde 忽略，无法持久化至配置文件。

## 涉及路径

- wedb/wedb/src/args.rs
- wedb/wconf/src/node_options.rs

## 解决建议

1. 为 ClusterArgs 派生 Deserialize。
2. 在三层合并流程中将配置文件中的集群配置与命令行参数进行合并覆盖。
