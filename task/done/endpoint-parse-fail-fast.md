监听端点解析失败静默回退 127.0.0.1:0 修复：拒启上抛 + bind 多地址拆分落 wconf 单点

来源：next/endpoint-parse-fail-fast-multi-bind.md（glm.net 第 9 条）。取证基线：主仓 dev，
C# 相对路径行号按当下代码复核。

结论

票面成立，全臂核实。C# 确实对端点解析失败拒启且原生支持 bind 多地址：
garnet/libs/common/Format.cs:52-83 TryParseAddressList 对地址列表按逗号与空格切分
（:64 TrimEntries | RemoveEmptyEntries），任一地址建端点失败即 endpoints=null 返回 false
并回填 errorHostnameOrAddress；garnet/libs/host/Configuration/Options.cs:795-797
调用处以 `|| endpoints.Length == 0` 追加空列表判定，任一不满足抛
GarnetException("Invalid endpoint format ...")，进程无回退直接拒启。rust 侧
wnode/src/server.rs 旧 GarnetServer::new 以 unwrap_or(DEFAULT_FALLBACK_ENDPOINT)
丢弃 AddrParse 错误静默落 127.0.0.1:0（打印启动横幅却监听随机端口，配置面静默劣化），
且 wconf/src/node_options.rs endpoints() 把 bind 整串拼一个端点、无多地址切分，
两处均为对 C# 的偏离，按票修复。

落地

1. wconf/src/node_options.rs NodeArgs::endpoints()：bind 先 trim，全空白视同未指定
   走 protected-mode 回退臂（对标 C# IsNullOrWhiteSpace）；否则按逗号与空格切分
   （split + trim + 滤空，等价 TrimEntries|RemoveEmptyEntries），逐地址与 port 组合成
   多端点；unixsocket 尾部追加不变（对标 Options.cs:813-814）；切分仅此一处，
   消费侧禁再 split。全分隔符输入产出空列表，对应 C# endpoints.Length == 0 拒启臂，
   由下条消费侧承接。
2. wnode/src/server.rs GarnetServer::new 改返回 crate::Result<Self>：逐端点
   ServerEndpoint::parse 以 `?` 上抛 Error::AddrParse（错误信息已含原字符串），
   空列表报 AddrParse("监听端点列表为空（bind 无有效地址）")；
   删除 DEFAULT_FALLBACK_ENDPOINT 常量，不留兜底端点概念。不新增 try_new，一套签名。
3. run_async 装配段（server.rs:262 域）对 new 以 `?` 承接，启动流水线随 Err 终止、
   进程退出码非 0（block_on 闭包本即返回 crate::Result）。
4. 调用面同步：wnode_test/src/lib.rs start_server、wedb_test/src/node.rs、
   wnode/tests/{node_test,tls_test,tls_push_tests,pubsub_assembly_tests,
   net_pump_consume_tests}.rs、wedb/tests/{replication_end_to_end,checkpoint_import,
   diskless_sync_ri_vector,diskless_loop_convergence,diskless_sync_anchor_window,
   appendlog_reject_disconnect,cluster_flushall_broadcast}.rs 全部传合法端点，
   以 ? 或 unwrap 承接 Result，未放宽签名。
5. 新增断言：wconf test_multi_bind_split（多地址切两条以上、全分隔符产出空列表、
   纯空白回落保护模式臂）；wnode server.rs tests 模块
   new_rejects_invalid_endpoint（端口越界端点返回 Err 且信息含原字符串）与
   new_rejects_empty_endpoints（空列表 Err）。

验证

cargo check 私有 target 目录零 error 零 warning（含 --all-targets）。
grep 归零：DEFAULT_FALLBACK_ENDPOINT 定义与 unwrap_or 兜底均不存在。
