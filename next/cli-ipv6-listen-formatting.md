# IPv6 监听地址方括号格式化支持

来源：next/zcode-r6-cli.md 问题 8

## 问题

NodeArgs::endpoints 在组装监听地址时使用 format!("{addr}:{port}") 拼接，
对于 IPv6 地址字面量缺少方括号包裹，导致生成的 SocketAddr 格式非法。

## 涉及路径

- wedb/wconf/src/node_options.rs
- wedb/wnode/src/endpoint.rs

## 解决建议

1. 拼接前先解析 addr 是否为 IPv6 地址，若是则格式化为 "[{addr}]:{port}"。
2. 验证 IPv6 单栈与双栈监听解析正确性。
