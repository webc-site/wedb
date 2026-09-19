拒绝 resp-pubsub-acl-e2e-test-parity（next 票据为 AI 生成的测试覆盖注水，两点前提均不成立）

取证基线：主仓 /Users/z/git/db/wedb，dev HEAD 306d32da。子代理仅 Read/Grep 核验，未 fork、未改主树。

## 第一部分 RESP PubSub 大载荷测试：测的是 rust 里根本不存在的特性

C# LargeSUBSCRIBE（garnet/test/standalone/Garnet.test/RespPubSubTests.cs:59）之所以存在，
是因为 C# SubscribeBroker 用 TsavoriteLog 承载待投递负载，有 PubSubPageSize 旋钮
（garnet/libs/host/GarnetServer.cs:277，默认 4k，测试设 256k），载荷按页切分，需重组路径。

rust 转写已彻底消除该设计：
- wedb/wpubsub/src/subscribe_broker.rs:1-11 头注：C# 以 TsavoriteLog 专日志承载待投递负载，
  rust 托管面以待发队列承接同一管线且不再落日志。
- pending_queue: List<PendingEntry> 以 (Box<[u8]>, Box<[u8]>) 原子存放，无页边界分支。
- PubSubMailbox 按消息条数（1024）限容，不按字节。
- wedb/wnode/src/resp/resp_server_session.rs:2275-2280 write_direct_large 注：rust 托管缓冲天然可扩容，
  等价一次追加。
- 帧化经 wresp/src/ext.rs:89 write_resp_bulk_string，无按尺寸分支。

rust 的「大载荷」用例只会把一个更大的 Box<[u8]> 走过与 pub_sub_self_publish_resp3_no_lock_error
已覆盖的完全相同链路（publish_now→broadcast→try_publish→drain_into→write_resp_bulk_string→Vec<u8>）。
纯注水，不暴露任何行为缺口。

## 第二部分 ACL SETUSER 矩阵测试：事实前提错误，且超出 C# 断言范围

票据称 wnode/tests/acl_tests.rs 无「规则→认证→命令授权判定」生效链用例。实为已有：
- acl_tests.rs:473-526 session_level_acl_gating_end_to_end 正是该链：未认证 PING→NOAUTH，
  AUTH 错密码→WRONGPASS，AUTH 对→+OK/+PONG 放行，ACL SETUSER default -ping 改规则，
  同会话 PING→NOPERM（授权判定）。
- 规则矩阵解析已由 wacl/src/acl_parser.rs:346-635 八个测试覆盖：password_ops
  （>pw/</#hash/!hash/nopass/resetpass）、flag_ops（on/off/reset/~*/+cmd）、malformed、category_lookup。
- SETUSER→ACL LIST/GETUSER/WHOAMI/CAT 反射已由 basic_list_test、get_user_test、basic_whoami_test、
  acl_cat_via_session_dispatch_for_default_user、acl_list_and_users_stream_consistent_frames 覆盖。
- 命令放行/拒绝由 can_access_command 测试 + 上面的端到端 NOPERM 覆盖。

票据自认「底层实现均已活，缺口只在测试覆盖层」，即按定义无行为缺口。C# SetUserTests 主要断言
ACL LIST 文本与 AUTH 结果，而非命令 allow/deny；票据提议的 acl_permits/acl_allows_command 矩阵
超出 C# 断言范围，违反「测试对标 c#，清理 c#没有的测试」（.agents/skills/transpile/SKILL.md）。

## 结论

两部分均为 AI 生成的覆盖注水，且建立在关于现有 e2e ACL 覆盖的过时/错误前提上。拒绝实现，
不新增测试。next/resp-pubsub-acl-e2e-test-parity.md 已删除。
