unixsocketperm 配置旋钮落地

C# 事实（已核实，票据成立）：garnet/libs/host/Configuration/Options.cs:683-685 定义 unixsocketperm 选项，IntRangeValidation(0, 777) 加 SupportedOSValidation（Unix only）；Options.cs:816-817 把十进制写法按八进制数字转 UnixFileMode；Options.cs:1031 投影进 GarnetServerOptions；libs/host/GarnetServer.cs:294 把 UnixSocketPath 与 UnixSocketPermission 传入 GarnetServerTcp 构造；libs/server/Servers/GarnetServerTcp.cs:147-151 Start 中 Bind 之后、非默认值（0 即跳过）且非 Windows 时 File.SetUnixFileMode 收紧套接字文件权限。

Rust 实现：
- wconf/src/node_options.rs 的 NodeArgs 增 unixsocket_perm 字段（Option<i32>，对标 C# int 与八进制数字字面量口径），CLI 长名 unixsocketperm（Redis 兼容名），serde 派生自动纳入 nested_text 单一配置机制，不新增第二格式。Default 置 None（不设置即现行为，对标 C# 默认 0 跳过臂）。validate 单点校验：0-777 界复用 ValueOutOfRange（对标 IntRangeValidation），八进制数字位（任一位大于 7 拒启，对标 Options.cs:817 Convert.ToInt32 转换失败面）。新增 unix_socket_mode 访问器把八进制数字折算为真实模式位，0 或 None 返回 None（对标 unixSocketPermission != default 跳过臂）。override_explicit 列表补该字段。
- wnode/src/net/uds.rs 的 UdsGuard::bind 增 perm 形参（Option<u32>），bind 成功后 Some 即 std::fs::set_permissions（PermissionsExt::from_mode），失败点名路径返回 Err 不静默（对标 GarnetServerTcp.cs:151，bind 后置动作时序一致）。
- wnode/src/server.rs 的 GarnetServer 增 unix_socket_perm 字段与 with_unix_socket_perm 注入器（同 with_tls_config 单点注入形态，值不进端点字符串，无双通道）；run_async 从 node_args.unix_socket_mode 单点取值注入，start_unix_worker 透传给 UdsGuard::bind。
- 权限值不进 endpoints() 的 unix: 端点字符串，与 endpoint-parse-fail-fast-multi-bind 域形态保持一致。

测试：wconf 参数表测试补默认 None、CLI 与 nested_text 口径、非法界与非法八进制位在 validate 期报错、折算位值断言；wnode tests/node_test.rs 断言 perm 0o600 绑定后 metadata mode 恰为 0o600，未指定不设断言（避免 umask 依赖）。
