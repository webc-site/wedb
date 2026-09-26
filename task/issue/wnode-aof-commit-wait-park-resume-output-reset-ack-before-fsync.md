审核结论：通过（r26 审核席，定级 P2 维持发现席自定级；档位承诺失守而非默认面丢数——--aof-commit-wait 系 opt-in，node_options.rs:577 默认 false，默认档零影响）。逐锚实测属实：resp_command.rs:480-489 复位判据 pending_output_len()==0 属实，唯一调用点 :274-275（aof_commit_mode_gate，attach.rs:132-134）；pump.rs:172-174/:198-206（output.clear()）/:130-141/:156-168、drive.rs:266/:278/:315/:346 停泊出口、:371 标记单点读、:395 write_all、core.rs:1125-1140 停车中断、txn.rs:31 批首 clear 全部对位。关键必判：出网臂确有闸（:371），但闸读的是会话可变字段——resolve 后内层循环重入消费（drive.rs:211-356）在写出段之前重解析流水线后续命令，此时 output 已被 take_output_into 清空，PING 复位标记，写出段读到 false，resp_pooled 中积存的 BLPOP/SET 应答未经 wait 直发，闸形同虚设，缺陷成立；正常单轮轮内 output 非空故不复位，与 C# 同构，分叉仅在停泊-续跑轮。C# 对照属实：Send :1440-1465（票面 1451-1463 略偏不影响判定）唯一致命出口受闸，:1475 系 DebugSend debug-only 非反证；RespCommand.cs:1207、HandleAofCommitMode :1267-1283、StoreWrapper :504-509、ListCommands :284 全实；C# dcurr==head 判据含「未出网字节」而 rust 只看 output，语义弱化即根因，「恒受闸」不失实。查重：deviations 全册与五池 grep 零实质命中（reject 的 mailbox-stall、ing 的 objrmw/pump 扫描两票均他轴）。审核实测补出票面漏项：drain_pubsub_into（drive.rs:208）系第六处冲应答出口，推送帧随轮逃闸，须一并计入。

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. drive_loop 轮首引入局部 bool armed（随 :205 resp_pooled.clear 一并复位），六处冲出口（票面五处 + 实测新增 :208 drain_pubsub_into）后就地置位（判据：resp_pooled 非空且 wait_for_aof_blocking），收为一处 closure 单点，不新建第二套等待调用；:371 出网条件改为 armed || 会话字段，成功 wait 后清零。
2. 票面「推送臂 :527 并入闩」一款取消：该臂 flush 与读闸之间无解析窗，维持现状。
3. 解析期字段与 take_output_into 的 1:1 映射不动；订正 drive.rs:360-368 注释，并订正 task/review_history/zcode-r24-x-session.md 第三节第 3 线「跨批无残留」判净语（该句只核字段本身，未对账泵侧出口时序，确属误判）。
4. 测试：net_pump_consume_tests.rs 的 AofWaitProvider 增「停泊 + 流水线后续 PING」案（断言 waits==1 且 wait 先于 recv）、fail_wait 零字节案、纯 PING 回归案；aof_commit_wait_e2e.rs 全绿不回退。
5. 遗留（不入本票）：水位多轮让渡写点紧随 flush 无解析窗不受影响；集群槽位 Wait/EXEC 重放已由 resolve_slow_wait_into 覆盖在方案 1 内；MONITOR 面出网闸属他域另席。

停泊-续跑轮 take_output_into 先冲应答出会话致 handle_aof_commit_mode 按 pending_output_len()==0 复位 wait_for_aof_blocking，出网臂漏等提交落盘，commit-wait 档应答先于 fsync（C# Send 前置闸分叉）

问题分析：
1. Garnet 契约对齐：C# WaitForCommit 档（EnableAOF 且 aof-commit-wait）的对客持久性承诺是「应答出网前阻塞等待 AOF 提交落盘」。该承诺由两条不变量合成：其一，应答字节离开会话响应缓冲的唯一出口是 Send（RespServerSession.cs:1451-1463），Send 内 `if (waitForAofBlocking)` 即调 StoreWrapper.WaitForCommitAsync（StoreWrapper.cs:504-509）BlockingWait 后才 SendResponse，提交失败异常上抛、应答不发、连接收场；其二，复位判据 HandleAofCommitMode（RespCommand.cs:1267-1283）为 `dcurr == networkSender.GetResponseObjectHead()`，即「响应缓冲内还有未发出字节」时绝不复位——阻塞/慢命令在网络线程内同步执行（ListCommands.cs:284 AsyncUtils.BlockingWait），其应答写入响应缓冲后，同批后续命令解析时 dcurr>head，标记保持；本轮全部应答经 Send 受闸出网后 dcurr 才回到 head。故 C# 结构上不存在「AOF 相关命令应答未受闸出网」的窗口。
2. 工程现状确证：rust 会话不持网络发送器，应答先经 take_output_into（pump.rs:198-206，output.clear() 即视为「已无未发送数据」）冲入泵侧 resp_pooled，真正出网在出网臂 write_all（drive.rs:395），wait_for_aof_blocking 只在出网臂 drive.rs:371 读一次——字节出会话冲入 resp_pooled 的时刻完全不受闸。正常单轮批次与此前 C# 同构（同轮内后续命令解析时 pending_output_len()>0，不复位，不误放行）。分叉在停泊-续跑多轮形态：阻塞族挂起（core.rs:1125-1140 停车即中断本轮消费）、慢路径/冷上下文挂起、AUTH/ACL 停车臂、脚本挂起、集群槽位 Wait 停车，这些轮在 try_consume_messages_into 收尾（resp_session_consumer.rs:158-172）与 resolve_blocked_wait_into / resolve_slow_wait_into / flush_output_into（pump.rs:130-141、156-168；drive.rs:266）处把此前 AOF 相关命令的应答冲入 resp_pooled，泵随即驱完成体、应答直写 resp_pooled，再重入消费轮解析流水线后续命令——此时 session.output 恒为空（连批首 enter_and_get_response_object 也 clear，txn.rs:30-32），handle_aof_commit_mode（resp_command.rs:480-489）据 pending_output_len()==0 复位 wait_for_aof_blocking，后续命令属 AOF 无关集（PING/ECHO/INFO/TIME/CLIENT 族等）则保持 false；出网臂 drive.rs:371 读到 false 即跳过 wait_for_commit_async，resp_pooled 中已积的 SET/BLPOP/EVAL 应答未经 fsync 直发客户端。协议违规批的收尾冲出（resp_session_consumer.rs:168）同属该未受闸出口。
3. 逻辑危害确证：开 --aof-commit-wait 的连接以「BLPOP key 0 后流水线 PING」（客户端心跳与阻塞读并用的常见形）触发：BLPOP 弹出已入 AOF 的 DEL/LPOP 条目、应答已在 resp_pooled，PING 解析即清标记，应答先出网、提交后落盘——断电窗口内客户端已见弹出的元素在重启后复活（双重消费），且 fsync 失败时 C# 的「不发应答即断连」防线同步失效，脏写静默确认为假。分层冷读停泊后的 GET、含写 EVAL 脚本停泊后随发 PING、EXEC 锁停泊后的 MULTI 批同属此族：该档的对客语义由「应答即持久」退化为「尽力等一轮」，档位承诺主从两侧不可对拍。C# 同输入必等待（应答字节驻响应缓冲直至受闸 Send），此为契约分叉而非既定改良；review_history r24-x-session 「跨批无残留」判净语只核解析字段本身，未对账泵侧 take_output_into 出口时序，属误判需订正。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/parser/resp_command.rs:handle_aof_commit_mode（:480-489 复位判据 pending_output_len()==0）
wedb/wnode/src/resp/resp_server_session/pump.rs:pending_output_len（:172-174）、take_output_into（:198-206）、resolve_blocked_wait_into（:130-141）、resolve_slow_wait_into（:156-168）
wedb/wnode/src/resp/resp_session_consumer.rs:try_consume_messages_into（:158-172 轮尾冲出与违规批冲出）、flush_output_into（:296-298）
wedb/wnode/src/net/handler/drive.rs:drive_loop 出网臂（:359-395，:371 标记单点读、:395 write_all）、停车臂 flush_output_into（:266）、推送臂同款闸（:527）
wedb/wnode/src/resp/resp_server_session/core.rs:process_messages 停车中断（:1125-1140）、enter_and_get_response_object 批首清缓冲（:839，实现 txn.rs:30-32）

对应 c# 文件与函数：
garnet/libs/server/Resp/RespServerSession.cs:Send（:1451-1463，waitForAofBlocking 闸先于 SendResponse，字节唯一致命出口受闸）
garnet/libs/server/Resp/Parser/RespCommand.cs:HandleAofCommitMode（:1267-1283，复位判据 dcurr==GetResponseObjectHead 与「未受闸积压字节」严格互斥）
garnet/libs/server/StoreWrapper.cs:WaitForCommitAsync（:504-509）
garnet/libs/server/Resp/Objects/ListCommands.cs:284（阻塞命令网络线程内同步收割，无提前冲出点）

精炼执行方案：
1. 单机制收口为「字节出会话即受闸」的泵侧闩：drive_loop 内层循环维护一枚 bool（如 armed），在内层各冲出口（try_consume_messages_into 返回后、resolve_blocked_wait_into / resolve_slow_wait_into / flush_output_into / resume_suspended_script_fut 完成后）当 resp_pooled 非空且 session.wait_for_aof_blocking() 置位时置 true；出网臂等待条件改为 armed 或 session.wait_for_aof_blocking()，成功等待后清零 armed；失败仍按现臂断连不发应答。推送臂（drive.rs:527）并入同一闩判据。
2. 解析期字段语义不动：handle_aof_commit_mode、pending_output_len、take_output_into 与 C# 的 1:1 映射保持不变，闩仅是连接级出网判定叠加，不引入第二份等待调用（仍单点 wait_for_commit_async）；同步订正 r24-x-session 判净语与 drive.rs:360-368 注释（补「停泊续跑轮须以出会话时点标记为准」一句）。
3. 测试验证点：net_pump_consume_tests.rs 宿主（AofWaitProvider）增三锁——慢臂停泊桩本轮产应答后续批流水线 PING_FRAME，断言 waits==1 且时间线 wait 先于 recv；fail_wait 注入下同形态断言应答零字节出网即断连；纯 PING 单轮批回归 waits==0。aof_commit_wait_e2e.rs 既有两档回归全绿。

查重自证（新模板入库，零命中）：
grep -rilE 'wait_for_aof_blocking|pending_output_len|take_output_into|aof.commit.wait|handle_aof_commit_mode' task/issue task/todo task/ing task/reject task/done task/review_history doc/zh/deviations.md
grep -rilE '出网前置|提交落盘等待|ack.?before|先应后刷|fsync.*应答|应答.*fsync|waitForAofBlocking' 同上路径
命中甄别：deviations 全册零在册；reject/wnode-blocked-wait-pubsub-mailbox-stall 系阻塞期邮箱停摆他轴（其案文仅在修订建议中复述既有写出规则，未涉本根因）；review_history 中 r24-x-session（:24 判净语错，本票第 3 条已订正）、r16-server/r17-pubsub/r24-x-network/r17-net 为符号在场清单非同因结论、r19-serverops 结论为双侧无 WAIT 命令（与本轴无关）；ing 池 aof 相关两票（objrmw 入队吞错、pump 扫描错误臂驱逐）与本票出网闸根因不重叠。
