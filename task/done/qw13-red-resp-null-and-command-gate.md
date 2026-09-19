优先级：高（dev 测试基线红，计数轮前置）

单题：RESP 应答形态与命令门（null 的 RESP2/RESP3 双形、错误串文案、pubsub 白名单、异步判定）
在当前 dev 上偏离 C# 对位。

红测试与实测证据（2026-09-19 dev 6e008a33 复跑，确定性失败）
1/2 wnode::resp3_null_parity::resp3_null_forms_match_csharp 与 resp2_null_forms_unchanged —
  wnode/tests/resp3_null_parity.rs:96，两条同一处断言：BITFIELD OOB 场景
  left: "*1\r\n:0\r\n"（字节 [42,49,13,10,58,48,13,10]），resp3 期望 "_"、resp2 期望 "$-1\r\n"
  即数组元素位的 null 被整数 0 吃掉，两条同时红说明该形态写口有单一真源被改坏。
3 wnode::resp_vector_set::result_writers_honor_bitmap_and_count —
  wnode/tests/resp_vector_set.rs:931 left: "%1\r\n$1\r\ne\r\n*2\r\n,0.5\r\n_\r\n"
  right: "%1\r\n$1\r\ne\r\n*2\r\n,0.5\r\n$-1\r\n"
  同族问题（map 值内数组元素位的 null 形态）。须按 C# 在该上下文（RESP3 map 内嵌数组）究竟写
  `_` 还是 `$-1` 判谁对，不许照抄用例期望。
4 wnode::resp_sorted_set::geo_dist_unit_validation — wnode/tests/resp_sorted_set.rs:900
  两侧同为 "-ERR wrong number of arguments for '..." 但文案不等（GEODIST 参数数错误串），
  与 C# 错误串逐字对齐。
5 wnode::resp_server_session_tests::client_type_gate_pubsub_without_gossip_link —
  wnode/tests/resp_server_session_tests.rs:228
  unexpected CLIENT INFO: -ERR Can't execute 'CLIENT_INFO': only (P|S)SUBSCRIBE /
  (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context
  pubsub 上下文白名单按 C# 的 resp-server-session 门实现核实（含 CLIENT 类命令是否放行）。
6 wnode::transaction_session_test::runtxp_resolves_registered_proc_end_to_end —
  wnode/tests/transaction_session_test.rs:291 left: "-ERR command requires asynchrony..."
  right: "+OK"（已注册 proc 走 runtxp 被判需异步）
7 wnode::resp_info::info_store_snapshot_channel_populates_segments —
  wnode/tests/resp_info.rs:492 assertion failed:
  info.contains("total_main_store_size:70024")（INFO 存储快照通道未填该段）

判读方向（须自行核实）
1/2/3 大概率同源于 null 写口（单一 sink），先定位该 sink 最近的改动（f49-resp3-frame 一带），
修一处而非在各调用点补 if；4/5/6/7 各自独立定性，禁止以「用例过期」名义改断言，除非给出
garnet 源码锚点。

避让
wnode/src/resp/resp_server_session.rs、wnode/src/resp/garnet_api/mod.rs、
wnode/src/storage/session/storage_session.rs 三处主索引有他人暂存在途（pending-lat 计时面），
动之前先 git diff --cached 核对，只做最小改动；向量登记键 \0\0 前缀（另一票）、分层写臂
（另一票）、wkv dbmeta（另一票）勿越界。

改动域
wresp 写帧与 null 形态口、wnode/src/resp 下的 sorted_set 错误串与会话命令门与 INFO 段、事务
runtxp 异步判定，以及上述测试文件。禁止触碰 wkv、waof、wnode/src/storage/**（分层）。
