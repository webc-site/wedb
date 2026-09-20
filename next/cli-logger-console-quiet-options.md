# 日志输出配置支持 --quiet 与 --disable-console-logger 及错误留痕

来源：next/zcode-r6-cli.md 问题 11 与问题 12

## 问题

1. file-logger 在目标文件创建失败时静默丢弃，未留痕报错。
2. 控制台输出管理缺少 --quiet（静默横幅）与 --disable-console-logger（关闭控制台日志输出）参数。

## 涉及路径

- wedb/wnode/src/logging.rs
- wedb/wnode/src/server.rs
- wedb/wconf/src/node_options.rs

## 解决建议

1. 文件日志初始化失败时使用 error 级别日志报警并留痕。
2. 在 NodeArgs 中增加 --quiet 与 --disable-console-logger 参数，接通控制台与横幅打印开关。
