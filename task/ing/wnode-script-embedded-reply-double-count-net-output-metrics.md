甄别结论：通过（fix 席 r27，2026-09-27，定级 P3 维持审核席自定级；纯指标观测面系统性虚增，无内存安全与数据面影响）。逐锚现码双侧复跑属实：rust 侧 pump.rs:188-192 account_output、:206-214 take_output_into（:213 无条件入账）、lua.rs:509 水位让渡臂与 :527 dispatch 收尾臂两路内嵌冲出、:309/:314 script_out 回并、lua.rs:152/:162 脚本内挂起 resolve_*（入账落 pump.rs:148/:175）、lua.rs:206 余量绕过入账、wmetric garnet_server_monitor.rs:221-237 dispose 归并，全部命中；C# 一手锚 RespServerSession.cs:1462 Send 唯一外计入账、SessionScriptCache.cs:24-26/:64 内嵌独立 processor、:77 processor.Dispose 与订正锚 :264/:405 属实，无上游误读。因果链现码可达：采样开启下 SET k v; EVAL "redis.call('GET','k');return 1" 0 —— 窗内内嵌应答按 pump.rs:213 入外层活跃会话指标，批尾最终应答再入账，活跃面得「内嵌和 + 最终」而契约只该有最终应答；挂起族 :152/:162→:148/:175 同形，:206 余量为反向漏计。审核亲验订正的两条口径（活跃面成立、history 面 C# 经 processor.Dispose 亦并入，修复后 rust 少报属自然投影须登记一句；虚增本质是内嵌字节进活跃面而非「双过同一字节」）复核认可。与近期两笔同轴合入正交、未被顺带收口：b9ff014 仅改出网等待闸（drive.rs 全文件无 account_output），9d2b06c 仅短接 handle_aof_commit_mode 标记维护（现 :491），均不触 take_output_into 入账路径，现码 :213 仍无条件入账；done 池那两票亦自证异轴不并案。查重：deviations.md 无在册，全池 grep 仅命中本票与其 issue 原件。架构合规：定案 1 保持唯一冲出实现 + account_output 唯一入账点、否决 dispatch_resp 直拷变体（那会造第二份「拷贝加清空」冲出实现），符合单套机制与无过度设计；定案 3 把「记账点在冲出非实写」的次生面剔除出本票的理由（C# KILL Dispose 同样先计后弃）成立。格式纯粹、双侧路径齐全。随票移交 fix 席四处现码勘误：其一，票面 drive.rs:315/:346 现漂至 :332/:366（外层泵同名调用保持入账，按现号定位）；其二，票面「pump.rs:205」系 take_output_into 定义行（:206）而非调用点，勿误改；其三，resp_session_consumer.rs:337 drain_pubsub_into 亦为入账调用点，票面「其余保持入账」清单漏列，须补入不变面；其四，修复落地后按审核订正 1 在 doc/zh/deviations.md 登记 history 面少报一句。派沙箱席 b02d。

审核结论：通过（本轮审核席，定级 P3；裁定理由：内嵌应答字节经 lua.rs:509/:527 两臂入外层活跃会话指标、C# 活跃面只对最终 EVAL 应答计一次，虚增逐锚亲验成立，纯指标观测面的系统性偏差故 P3）

脚本 redis.call 内嵌应答经 take_output_into 双过 account_output 入账点，total_net_output_bytes 按「内嵌应答 + 最终应答」双份累计虚增（C# 内嵌 processor 计数随窗口丢弃不入账）

问题分析：
1 Garnet 契约对齐：C# 会话出向字节唯一记账点是 Send 内的会话指标累计（garnet/libs/server/Resp/RespServerSession.cs:1451-1463 内 netOutput 字节统计随 SendResponse 落账）；脚本内 redis.call 经内嵌独立 processor + ScratchBufferNetworkSender 承接（garnet/libs/server/Lua/SessionScriptCache.cs:24-26/:60-64），内嵌管道的应答字节只进脚本缓冲、其独立计数随窗口丢弃，外层连接只对最终 EVAL 应答入账一次。
2 工程现状确证：rust 会话出向入账单点为 account_output（wedb/wnode/src/resp/resp_server_session/pump.rs:188-192），由 take_output_into（pump.rs:206-214，出账 self.account_output(len as u64)）无条件驱动。脚本窗内每条 redis.call 的合成应答经 session.take_output_into(response) 冲入脚本缓冲（lua.rs:509 水位让渡臂、lua.rs:527 dispatch 收尾臂），此时 account_output 已按内嵌应答字节入账一次；窗口关闭 script_out 并回会话 output（lua.rs:309-314），批尾冲出经 take_output_into 再入账一次——内嵌应答字节双计。rust 记账点在「冲出会话 output」而非「实写网络」，pump.rs:184-187 注释自陈对位 C# Send 唯一出向记账点，与 C# 口径存在两处弱化：内嵌管道字节混入外层会话句柄 + 泵 break 'drive 弃写路径（drive.rs 写失败/KILL/终止胜出臂）已入账未出网字节滞留累计。
3 逻辑危害确证：EVAL/EVALSHA 重度负载下 INFO total_net_output_bytes 与 instantaneous_net_output_tpt 按内嵌应答字节虚高（每条 redis.call 应答记两遍），会话 dispose 经 add_metrics_history_session_dispose（wedb/wmetric/src/garnet_server_monitor.rs:221-237）把虚增值永久并入 history；基于该指标的容量规划与吞吐对账失真。纯指标观测面虚增，无内存安全与数据面影响。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/resp_server_session/pump.rs:account_output（:188-192）、take_output_into（:206-214）
wedb/wnode/src/resp/resp_server_session/lua.rs:redis.call 消费环内嵌冲出（:509 水位臂、:527 收尾臂）、窗口关闭 script_out 回并（:309-314）
wedb/wmetric/src/garnet_server_monitor.rs:add_metrics_history_session_dispose（:221-237）

对应 c# 文件与函数：
garnet/libs/server/Resp/RespServerSession.cs:Send 出向记账点（:1451-1463）
garnet/libs/server/Lua/SessionScriptCache.cs:内嵌 processor 与 ScratchBufferNetworkSender（:24-26、:60-64，计数随窗口丢弃）

精炼执行方案：
1 内嵌冲出与外层入账分流：脚本窗内 redis.call 的 take_output_into(response) 改走不入账的内部冲出变体（take_output_into 增是否入账参，或 dispatch_resp 内直拷 output 后清空绕开 account_output 单点），最终 EVAL 应答经既有批尾冲出一次入账——C# 内嵌计数随窗口丢弃的对位形，不引入第二套记账点
2 附带低置信注记由评审裁 scope：account_output 在冲出点而非实写点入账，泵弃写路径已计未发字节滞留（C# 计在 Send 实写点）；若一并收口需在写出段按实写字节记账、冲出段不再入账，涉及面较大可另案
3 测试验证点：wnode 测增 EVAL 内含 N 条 redis.call 案，断言会话 total_net_output_bytes 增量等于最终 EVAL 应答字节（不含内嵌应答）；非脚本路径 SET/INFO 记账回归不回退

审核裁定执行方案

审核亲验订正（不改变判定，执行时按此口径）：
1 C# 口径精确化：内嵌 processor（SessionScriptCache.cs:64 构造）在 RespServerSession.cs:264 采样开启时自带独立 sessionMetrics，其 Send 逐 redis.call 入自己的私有计数；RespServerSession.cs:405 的 Dispose 归并对任何实例生效，SessionScriptCache.cs:77 processor.Dispose() 使内嵌计数在会话释放时并入 history。故 C# 契约精确形为：活跃面（ActiveConsumers 采样，INFO total_net_output_bytes 与瞬时吞吐的源）只含外层会话的最终 EVAL 应答；内嵌字节仅 dispose 后经 history 体现。票面「计数随窗口丢弃不入账」在活跃面成立、history 面不精确；本票收口的正是活跃面系统性虚增（瞬时吞吐虚高在 C# 无任何对应形态）。修复后 history 面 rust 不含内嵌字节而 C# 含，方向为少报，系 rust 无内嵌 processor 身份的自然投影，随修复在 deviations 登记一句即可，不属回改目标。
2 字节流归属订正：内嵌应答字节本身只过一次入账点（窗内 take_output_into），批尾再入账的字节是 script_out 最终应答；虚增本质是内嵌应答字节计入了不该进的活跃面（指标 = 内嵌和 + 最终应答，契约 = 最终应答），执行与锁测按此口径表述。

执行方案定形：
1 take_output_into 增是否入账参，保持唯一冲出实现与 account_output 唯一入账点（否决 dispatch_resp 内直拷变体——那会造第二份「拷贝加清空」冲出实现，违单机制）。lua.rs:509 水位让渡臂与 :527 收尾臂两处同传不入账（两臂同在 dispatch_resp 体内，只改 :527 会漏水位臂、大应答脚本命令仍虚增）；其余调用点（pump.rs:144/:165/:205、resp_session_consumer.rs:161/:168/:297）保持入账。
2 同族收口（本票范围内一并落）：resume_suspended_script 内 resolve_blocked_wait_into/resolve_slow_wait_into（lua.rs:152/:162，account_output 在 pump.rs:148/:175）把脚本内挂起体应答字节记进活跃面，同为内嵌字节入活跃面，一并按不入账收口（外层泵路径 drive.rs:315/:346 同名调用保持入账，入账判别收敛单一）；lua.rs:206 script_out 余量直入驱动方 resp_buf 绕过入账，属最终应答漏计，在窗口关闭后补一次 account_output。
3 次生注记裁定：剔除出本票。记账点在冲出非实写的发散（throttle 失败、AOF 提交失败、killable 取消的已计未发滞留）为单批界、断连偶发，且 C# KILL Dispose 弃发同样先计后弃（SendResponse 排队点即计），两端同形非契约分歧，不另案登记。
4 测试验证点（可测性已核，有既有先例）：照 wnode/tests/resp_server_session_tests.rs:736 直读会话 metrics total_net_output_bytes、wnode/tests/consumer_registry_counters.rs:120 经 monitor_sample 断言的形态——EVAL 内含 N 条 redis.call 案断言增量等于最终 EVAL 应答字节；补脚本内 BLPOP 挂起案断言挂起体应答不入账且最终应答入账；非脚本 SET/INFO 记账回归不回退。
