# consume-single-form

来源：next/ds.net.md 条 16 与原 next/net.md 条 12 合并（主代理预清理后移交）。

问题：resp_server_session.rs 的 try_consume_messages（批次拷贝）与 try_consume_pending（scratch 持久游标）两套并存；resp_session_consumer.rs 生产泵走 scratch，net/handler.rs 拷贝形态服务回退与测试。对标 RespServerSession.cs:474 TryConsumeMessages 单一形态。改法：长期收敛到 scratch 单形态，拷贝形态删除，测试改走 scratch 入口。

## 甄别结论（对照 garnet C# 生产链）

C# 形态唯一：libs/common/Networking/IMessageConsumer.cs:11 唯一方法 `unsafe int TryConsumeMessages(byte* reqBuffer, int bytesRead)`；NetworkHandler.cs:504 TryProcessRequest 把网络缓冲指针 + 游标喂会话，返回消费量后网络层平移（ShiftNetworkReceiveBuffer，半包驻留头部）。无拷贝形态、无回退分支。

rust 两形态消费方实测：

1. 生产泵（wnode/src/net/handler.rs drive_loop）：握手点探测 take_recv_scratch，RespSessionConsumer 恒返回 Some（mem::take 会话缓冲），生产连接恒走 scratch 形态（try_consume_scratch_into → try_consume_pending）。拷贝形态生产零命中。
2. 回退路径真实消费方只有一个：ClusterReplicationSession（wedb/src/server/replication/cluster_replication_session.rs:356）只实现拷贝面 try_consume_messages_into，副本端 TCP 泵收 APPENDLOG 帧时走泵拷贝分支。
3. 脚本重入（resp_server_session.rs:2518 RespScriptingApi.dispatch_resp）：走会话拷贝形态重入。C# 对标 LuaRunner.Functions.cs:ProcessCommandFromScripting 尾部 `respServerSession.TryConsumeMessages(request.ptr, request.length)`——单形态重入（recvBufferPtr 切到脚本格式化缓冲 + 入口 if (!txnSkip) readHead = 0）。
4. 测试：会话层直调 try_consume_messages（resp_server_session.rs 内测约 40 处、session_metrics_slowlog_tests 25 处、txn_resp_commands.rs 2 处、resp_command_parse.rs 5 处、lua_script_tests / replication_stream_e2e / cluster_replication_session 等）；consumer 层经 helper 收口（requirepass / transaction_session / resp_pubsub / acl_tests / resp_dispatch 等，roundtrip 族函数调 trait 拷贝面）；泵双形态桩（net_pump_consume_tests 的 LineConsumer 拷贝桩 + ScratchLineConsumer scratch 桩，node_test / tls_test 的 EchoConsumer 拷贝桩）。

回退路径语义去留结论：拷贝形态是 rust 自创兼容面，C# 无此物。生产 RespSessionConsumer 恒走 scratch，拷贝面生产零命中；事务跨批次排队语义拷贝形态做不到（会话注释自认「拷贝形态做不到」，靠泵 pooled 跨批持久兜住，游标驻泵、偏移驻会话两态分裂）。删除无生产语义损失；「回退」随 scratch 面必选化消除——对标 C# IMessageConsumer 单方法必选。

上轮改动语义保持清单：

- epoch 快照批首尾包裹（acquire/release_current_epoch）保留在唯一入口
- write_protocol_error → None 通道（协议违规发尽应答后断连）保留
- fatal_disconnect → None 通道（致命断流）保留
- 收尾「整段消费完 + 无在途事务才清缓冲复位」保留（事务排队字节驻留缓冲供 EXEC 回退重解析）
- 泵 take_dispose_request（QUIT 哨兵）写出段后检查保留
- 泵订阅推送双路等待（读挂起中邮箱唤醒直写）保留为唯一读路径

与并发代理的边界：process_messages 体（计数段 / no_script 门）不动；只动两形态入口函数、泵、消费者与测试。冲突以先合并者为准。

## rust 侧改动点

1. wnode/src/resp/resp_server_session.rs
   - 删拷贝形态 try_consume_messages(&[u8]) + try_consume_messages_body
   - try_consume_pending → 改名 try_consume_messages（签名 () -> Option<usize>），try_consume_pending_body → try_consume_messages_body，映射注释 RespServerSession.cs:TryConsumeMessages 单形态
   - RespScriptingApi.dispatch_resp 改：recv_buffer 替换为脚本格式化缓冲 + read_head/end_read_head 归零（对标 C# recvBufferPtr 切换 + readHead = 0）+ 调唯一入口；外层批 EVAL 后剩余命令随缓冲替换失效，与 C# 行为一致
   - 内部测试改 scratch 填充 + 唯一入口两行式
2. wnode/src/traits.rs：MessageConsumerFace 收敛单一形态——删拷贝必选面 try_consume_messages_into(req,resp) 与桥接面 try_consume_messages(req)；try_consume_scratch_into 改名 try_consume_messages_into(resp_buf) 必选（映射 IMessageConsumer.cs:TryConsumeMessages）；take_recv_scratch / return_recv_scratch 删默认实现改必选
3. wnode/src/resp/resp_session_consumer.rs：删拷贝实现；try_consume_scratch_into → try_consume_messages_into
4. wnode/src/net/handler.rs：drive_loop 删双形态——握手段前置（pooled 收首批 → 识别 WireFormat → 建会话 → 未消费字节迁入会话缓冲），稳态 loop 纯 scratch；删回退读取分支、回退消费分支、收尾段复位、scratch_mode 标志
5. wedb/src/server/replication/cluster_replication_session.rs：加 recv_buffer + read_head 字段，删拷贝实现改三件套（帧解析自 recv_buffer[read_head..] 切片，完整帧消费推进游标，半包回残余，整段完复位）
6. wedb/src/server/replication/replica_wire.rs：FrameSink::Session 直调改生产等价序（recv_buffer 填充 + 唯一入口 + fatal 哨兵）
7. 测试面：net_pump_consume_tests 删 LineConsumer 拷贝桩，全用例集归一 scratch 桩驱动；node_test / tls_test EchoConsumer 改三件套；consumer 层 helper（roundtrip 族）函数体改三件套序，调用点不动；会话直调测试改两行式
8. js/check/ignore：TryConsumeMessages 映射保留在唯一入口，预计零新增；以 bun ./js/check.js 实际输出为准

## 验收口径

- ./clippy.sh 零警告；./test.sh 全过；bun ./js/check.js 无新增缺失/重复
- 泵唯一消费形态：drive_loop 无拷贝分支、无 scratch_mode 标志
- QUIT 断连、协议违规断连、致命断流、事务跨批次（MULTI..EXEC）、阻塞/慢路径挂起、订阅推送双路等待行为不回归
- 脚本重入（redis.call）行为不回归（lua_script_tests）
- CLUSTER APPENDLOG 副本接收链路不回归（cluster_replication_session / replication_stream_e2e）

## 验证结果

- 分支：w5-consume（已合 dev 基线 2bba4c3，worktree 与分支合并后清理）
- 实际改动：35 文件 +1012/-951
- 会话层（resp_server_session.rs）：删拷贝形态 try_consume_messages(&[u8]) + body；try_consume_pending 改名 try_consume_messages 为唯一入口（签名 () -> Option<usize>，scratch 持久游标）；脚本重入 dispatch_resp 改「缓冲替换 + 游标归零 + 唯一入口」（对标 C# recvBufferPtr 切换 + if (!txnSkip) readHead = 0，LuaRunner.Functions.cs:ProcessCommandFromScripting 尾部）
- trait 层（traits.rs）：MessageConsumerFace 单形态——删拷贝必选面 try_consume_messages_into(req,resp) 与桥接面 try_consume_messages(req)；try_consume_scratch_into 改名 try_consume_messages_into(resp_buf) 必选（映射 IMessageConsumer.cs:TryConsumeMessages）；take_recv_scratch/return_recv_scratch 必选化（Vec<u8> 直取，不再 Option）
- 泵（net/handler.rs）：删 scratch_mode 双形态，握手段前置（pooled 收首批 → 建会话 → 字节迁移），稳态循环序「消费 → 镜像 → 写出 → 哨兵 → 读取」（C# Read → Process 循环序等价重排——握手段迁移字节首轮即被消费，修复重排前的首批判滞留挂点）；删回退读取/消费分支与收尾复位段；订阅推送双路等待、QUIT/违规/致命断流哨兵保持
- ClusterReplicationSession：加 recv_buffer/read_head 字段，帧解析改从会话缓冲切片循环消费（一次吃全全部完整帧），畸形帧/APPENDLOG 拒收统一走 None 通道断连；replica_wire FrameSink::Session 直调改生产等价序
- 测试面：泵桩归一（删 LineConsumer 拷贝桩，全用例集 scratch 桩驱动，补批内流水线/三批交错用例）；EchoConsumer×3 改 scratch 桩；consumer 层 helper（roundtrip/pump/feed 族）改三件套序，会话直调测试经 pump_feed/feed_consume 生产等价辅助
- 语义修正 2 处（拷贝形态时代的错误基线）：
  - multi_exec_roundtrip_without_writes 期待 *1\r\n → *1\r\n+PONG\r\n（拷贝形态 clear 丢排队 PING 字节致重放走空；scratch 持久游标下 PING 真执行，与 C# IsSkippingOperations 禁平移语义一致）
  - cluster_slot_verify_wait 重放断言：重评消费不再重喂帧，驻留字节原位续解析（Some(0) 完整消费）
- 回退路径语义结论：C# 单形态无回退物；rust 拷贝面生产零命中（RespSessionConsumer 恒走 scratch），删除无生产语义损失；「回退」随 scratch 面必选化消除
- 静态检查：./clippy.sh 0 警告（禁 allow）
- 自动化测试：./test.sh 全过（wedb 2056 项 + regress 2 项）
- 检查脚本：bun ./js/check.js 零输出（无新增缺失/重复）；删除面无 C# 映射丢失（TryConsumeMessages 映射保留于唯一入口），无需 ignore 登记
