resp_server_session.rs 3172 行按 C# 分片文件边界纯搬运拆分

来源：next/agy.design.md 条 17（同题另见 next/muse.design.md 条 18「巨文件待拆分」，以本单为载体）。
取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev 当下工作树，行号按符号重取。

现状
- /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs 实测 3172 行
  （`wc -l`），一个文件同时承载五类 C# 分片文件职责：
  1 依赖注入与装配面：:548 set_garnet_api、:561 set_item_broker、:567 set_runtime_config、
    :578 set_primary_tasks、:596 attach_transaction_components、:609 attach_pubsub、
    :623 attach_acl、:639 inject_dependencies、:765 attach_cluster_session、:773 attach_cluster_provider；
  2 指标与延迟：:873 attach_monitor、:895 attach_session_metrics、:900 get_latency_metrics、
    :912 reset_all_latency_metrics、:1098 latency_batch_start、:1118 latency_batch_stop；
  3 消费主循环与解析：:998 write_protocol_error、:1030 try_consume_messages、
    :1042 try_consume_messages_body、:1149 process_messages、:1332 make_upper_case、
    :2070 is_command_arity_valid、:2127 get_command_range、:2230 get_upper_case_command_range；
  4 事务面（含网络命令体）：:1310 enter_and_get_response_object、:1320 set_transaction_mode、
    :1448 process_transactional_command、:1492-:1529 network_exec/discard/watch/watch_ms/watch_os/
    unwatch/runtxp、:1536 with_txn_manager、:1552 network_skip、:1560 txn_queued_command_info；
  5 鉴权与自定义命令/Lua 入口：:921 set_user_handle、:927 update_resp_protocol_version、
    :937 authenticate_user、:967 can_run_debug、:1858 apply_authenticated_handle、
    :1881 network_auth_session、:1949 network_custom_obj_cmd、:2041 run_custom_command。
- 分派器本体（:862 dispatch_via_garnet_api、:1360 process_basic_commands、
  :1424 process_array_commands、:1597 process_other_commands、:1844 network_command_root、
  :2059 process）留在核心，与 C# 一致。
- 兄弟面已在目录化：同目录已有 basic_commands/、objects/list_commands/、
  objects/sorted_set_commands/ 目录模块先例，本文件是 resp/ 下最大的单文件。

C# 参考（RespServerSession 是 partial class，按职责分片成独立文件）
- 核心与会话态：/Users/z/git/db/wedb/garnet/libs/server/Resp/RespServerSession.cs（1737 行，
  持 TryConsumeMessages 主循环与分派）
- 鉴权：/Users/z/git/db/wedb/garnet/libs/server/Resp/ACLCommands.cs（NetworkAUTH）
- 事务：/Users/z/git/db/wedb/garnet/libs/server/Transaction/TxnRespCommands.cs
  （NetworkEXEC/WATCH/RUNTXP）
- 自定义命令与 Lua：/Users/z/git/db/wedb/garnet/libs/server/Custom/CustomRespCommands.cs、
  /Users/z/git/db/wedb/garnet/libs/server/Lua/LuaCommands.cs
- 指标/延迟：/Users/z/git/db/wedb/garnet/libs/server/Metrics/Info/InfoCommand.cs、
  /Users/z/git/db/wedb/garnet/libs/server/Metrics/Latency/RespLatencyCommands.cs
- 输出面已对位：/Users/z/git/db/wedb/garnet/libs/server/Resp/RespServerSessionOutput.cs ↔
  /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session_output.rs（本单不动它）

修法（纯搬运，禁夹带语义改动）
1. resp_server_session.rs 改为目录模块 resp/resp_server_session/，mod.rs 只做 `mod` 声明 +
   `pub use` 重导出，保持 `wnode::resp::resp_server_session::RespServerSession` 对外路径不变，
   消费方 use 面零改动（先例：resp/basic_commands、objects/list_commands）。
2. 子文件切法贴 C# 分片：core.rs（现状 3 + 分派器 + 结构体字段与 new/dispose）、
   attach.rs（现状 1 装配面，对标 API/SessionApi.cs 与 ctor 注入段）、
   auth.rs（现状 5 的鉴权臂，对标 Resp/ACLCommands.cs）、
   custom.rs（:1949 :2041 自定义对象命令与自定义命令入口，对标 Custom/CustomRespCommands.cs）、
   txn.rs（现状 4，对标 Transaction/TxnRespCommands.cs）、
   metrics.rs（现状 2，对标 Metrics/ 两文件）。
3. 可见性只调必要项：跨子模块私有项提 `pub(super)`，对外保持现 `pub`/`pub(crate)` 面，
   禁为搬运新增 `pub`（本仓禁以扩面换编译）。
4. doc 注释与 C# 锚点随函数体搬位、一锚一位点，禁重复挂载
   （锚点口径同 next/object-slow-dispatch-arg-reparse-single-source.md 与 check.js 记账）。
5. 目标规模：除 core.rs 外各子文件不逾 ~600 行，core.rs 不逾 ~1200 行（对位 C# 1737 行含
   主循环与缓冲管理，本仓缓冲管理已在别处）。

验收判据
- `wc -l wedb/wnode/src/resp/resp_server_session/*.rs` 无单文件逾 1200 行。
- 符号定义点唯一：`grep -rn "fn network_auth_session\|fn txn_queued_command_info\|fn latency_batch_start\|fn inject_dependencies" wedb/wnode/src`
  各自只命中一处（定义），其余命中均为调用面且路径不变。
- 消费侧零改动：`grep -rn "resp_server_session::" wedb/wnode/src wedb/wedb/src wedb/*/tests`
  的命中集合与拆分前逐条同名（除 mod.rs 声明行）。
- diff 只含 use 行、可见性标记、文件切分；无新增 allow、无签名变化、无协议字节变化。
- 联动的双轨清理票 next/zero-consumer-surfaces-batch-two.md 第一条（PING/ASKING/ECHO 臂转调）
  与本单同文件：先落该票或本单二选一顺序执行，禁同文件并行改（否则逐块冲突）。

优先级
打磨（拓扑对标 C# 分片、消巨峰文件），不改行为；排在死代码与去重票之后开工。
