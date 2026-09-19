优先级：高（dev 测试基线红，计数轮前置）

单题：RESP 应答形态（null 的 RESP2/RESP3 双形、错误串文案、pubsub 上下文命令门）在当前 dev 上与
C# 对位偏离。

红测试与实测证据（2026-09-19 dev 6e008a33 复跑，确定性失败）：
1/2 wnode::resp3_null_parity::resp3_null_forms_match_csharp 与 resp2_null_forms_unchanged —
  wnode/tests/resp3_null_parity.rs:96，两条同一处断言：
  BITFIELD OOB 场景 left: "*1\r\n:0\r\n"，resp3 期望 "_"、resp2 期望 "$-1\r\n"
  即数组元素位的 null 形态两条都不对，落到了整数 0——怀疑 null 写口被某个整数默认值吃掉。
3 wnode::resp_vector_set::result_writers_honor_bitmap_and_count —
  wnode/tests/resp_vector_set.rs:931 left: "%1\r\n$1\r\ne\r\n*2\r\n,0.5\r\n_\r\n"
  right: "%1\r\n$1\r\ne\r\n*2\r\n,0.5\r\n$-1\r\n"
  与 1/2 同族：map 值内数组元素位的 null 形态（此处用例期望 $-1，须按 C# 在该上下文（RESP3 map
  内的数组元素）究竟写 `_` 还是 `$-1` 判谁对，不许照抄用例）。
4 wnode::resp_sorted_set::geo_dist_unit_validation — wnode/tests/resp_sorted_set.rs:900
  两侧都是 "-ERR wrong number of arguments for '..." 但文案不等（GEODIST 参数数错误串），
  须与 C# 的错误串逐字对齐。
5 wnode::resp_server_session_tests::client_type_gate_pubsub_without_gossip_link —
  wnode/tests/resp_server_session_tests.rs:228
  unexpected CLIENT INFO: -ERR Can't execute 'CLIENT_INFO': only (P|S)SUBSCRIBE /
  (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context
  即 pubsub 上下文命令白名单与 C# 对位（C# 侧该上下文允许哪些命令要按源码核实）。
6 wnode::transaction_session_test::runtxp_resolves_registered_proc_end_to_end —
  wnode/tests/transaction_session_test.rs:291 left: "-ERR command requires asynchrony..."
  right: "+OK"

避让与边界：另有会话正在主索引暂存 wnode/src/resp/resp_server_session.rs、
wnode/src/resp/garnet_api/mod.rs、wnode/src/storage/session/storage_session.rs（pending-lat 计时
面），改动这三处前先 git diff --cached 看清在途内容，只做本票最小改，勿顺手重排；
分层写臂（tiered_*）与向量登记键域（vector_key_domain_ops / vector_set_rename）属另票，勿碰。
阻塞命令 blpop 的冷键属另票，勿动。

改动域：wresp 写帧/null 形态口、wnode/src/resp 下的 sorted_set 错误串与会话命令门、事务 runtxp
异步判定，以及上述测试文件。禁止触碰 wkv、waof、wnode/src/storage/**。

判重（主代理）：与 next/qw13-red-resp-null-and-command-gate.md 同题双花，后者覆盖面为前者超集（多 pubsub 白名单与异步判定两条），只保留后者。该域在 resp_server_session.rs / cmd_strings.rs 链 A 上，链上有在途代理，延后放行。
