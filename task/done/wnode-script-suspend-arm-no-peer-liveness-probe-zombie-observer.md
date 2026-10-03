终态：已合入 dev（2026-09-27）。b02e15b 脚本续跑臂并入 probe_race 三路竞速,Disposed 胜出 break;probe_race 去泛型本地累积 RaceEnd 携带返回;字节并入后移+入向记账就地拆分保 r18 契约;三案真实经纪测试

甄别结论：通过 | 定级 P2 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：并入 probe_race 单一样板；与 ing wnode-probe 票互指，严禁两套计数两套门

审核结论：通过（本轮审核席独立复验，定级 P2 维持；rust 侧缺陷逐锚亲验成立——脚本续跑臂 drive.rs:290-302 确为终止广播 × resume_fut 两路 select 无探测读，挂起窗内 blocked.resolve→observer.wait_result 纯事件等待、slow.resolve 纯存储 future，全链零套接字读，BlockedWait 注销通道盘点（Drop、dispose core.rs:751/:762、显式 abort 四处）对对端 FIN 全不可达，僵尸窗真实且 timeout=0 无界。C# 对照柱一处误读已订正但不翻案：TcpNetworkHandlerBase.cs:214 内核守护仅在 ReceiveAsync 未决期有效，而 do/while 循环在 OnNetworkReceiveWithoutTLS（ListCommands.cs:283 自注 Must block as we're on the network thread，AsyncUtils.BlockingWait 即裸 GetResult）执行期间不持有未决接收——C# 内联阻塞窗同样存在 FIN 盲窗，元素误弹与 timeout=0 悬挂在 C# 同形。缺陷正立柱改为 rust 自身同构性：blocked/slow 臂样板自陈判据「等待期间不读套接字即断连盲区」直接覆盖脚本臂形态，脚本臂系 ACL 两臂模板误载（ACL 臂短时操作盲窗无害，脚本臂承载无界阻塞等待），属同构分支漏配钩子而非 C# 差异承接，修复即补齐单机制而非偏离原型。方案两处硬伤已修订（resume_fut 全程持 &mut session 与 preserve_probe 双借用、探测字节直写换出壳），修订后可落，见文末审核裁定执行方案。查重：deviations.md 与五池零在册（reject 池空，票面所引 wnode-blocked-wait-pubsub-mailbox-stall 全树不存在；done 池两脚本窗票系 AOF 出网闸/输出换出异轴，ing 指标票与 todo 事务闩票均异轴）。

脚本挂起续跑臂缺对端活性探测：脚本内 BLPOP 挂起窗客户端断连不可观测，经纪等待队列滞留僵尸观察者、元素误弹出后 BrokenPipe 丢弃、同键真实等待者被挤占（同泵阻塞/慢臂 probe_race 三路样板漏配）

问题分析：
1 Garnet 契约对齐：C# 脚本内 redis.call('BLPOP',k,0) 在网络线程内联收割（garnet/libs/server/Resp/Objects/ListCommands.cs:284 AsyncUtils.BlockingWait(itemBroker.GetCollectionItemAsync(...))）；内联阻塞期间套接字异步接收仍由内核事件守护（garnet/libs/common/Networking/TcpNetworkHandlerBase.cs:214 BytesTransferred == 0 || SocketError != Success 即 Dispose，TLS 臂 :236 同形），Dispose 尾 garnet/libs/server/Resp/RespServerSession.cs:408 itemBroker?.HandleSessionDisposed(this) 注销观察者——C# 在脚本内联阻塞窗可观测对端断连并即时注销收场。
2 工程现状确证：rust 泵对挂起族的标准竞速样板是三路 probe_race（终止广播 × 执行体 × 对端活性探测读，wedb/wnode/src/net/handler/drive.rs:318-339 阻塞臂、:341 起慢臂，probe_race/preserve_probe :725-790 样板单点，其注释自陈判据「等待期间不读套接字即客户端断连盲区——FIN/RST 无人在场，观察者滞留经纪等待队列成僵尸，新写入元素被误弹出后写回 BrokenPipe 丢弃」）。脚本续跑臂（drive.rs:290-302）漏配：仅 wait_terminate × resume_suspended_script_fut 两路 select，无探测读；挂起体驱动点 blocked.resolve().await（lua.rs:151）与 slow.resolve().await（:161）全在该两路 select 内。脚本内 BLPOP 挂起后客户端 FIN/RST 不可达，BlockedWait（wedb/wcol/src/itembroker/item_broker_face.rs:239-245 Drop→abort）因未 drop 不触发注销，观察者无限期滞留。
3 逻辑危害确证：僵尸观察者滞留期间他端 LPUSH k v 经经纪指派给僵尸——元素已从列表弹出、结果回流死连接会话，脚本照常完成、应答写死套接字 BrokenPipe 丢弃（元素丢失，C# 同输入在 FIN 即刻 Dispose 收场不丢），同键真实等待者被挤占饥饿；连接泵滞留 resume await 直至 KILL/停机广播。慢路径脚本挂起（脚本内冷键读）同盲区但时长有限危害较轻。修复窗口事实：挂起期会话 recv_buffer 是换出后的空壳（lua.rs:121 outer_recv 在 resume 局部），preserve_probe 的 take_recv_scratch 直写该壳会被窗口关闭 self.recv_buffer = outer_recv（lua.rs:194）覆没——探测到站的活连接流水线字节须保全落 outer_recv 或延后并入，防凭空丢失。

涉及代码：
rust 文件与函数：
wedb/wnode/src/net/handler/drive.rs:drive_loop 脚本续跑臂（:290-302 两路 select）、probe_race/preserve_probe 三路样板（:318-339 阻塞臂、:341 起慢臂、:725-790 样板本体）
wedb/wnode/src/resp/resp_server_session/lua.rs:resume_suspended_script 挂起体驱动点（:151/:161）、窗口缓冲换出（:121/:194 outer_recv）
wedb/wcol/src/itembroker/item_broker_face.rs:BlockedWait Drop abort（:239-245）

对应 c# 文件与函数：
garnet/libs/common/Networking/TcpNetworkHandlerBase.cs:HandleReceiveWithoutTLS（:214，TLS 臂 :236）
garnet/libs/server/Resp/RespServerSession.cs:Dispose（:408 HandleSessionDisposed）
garnet/libs/server/Resp/Objects/ListCommands.cs:284（BlockingWait 内联收割）

精炼执行方案：
1 脚本续跑臂并入 probe_race 三路样板：resume_fut 作执行体与终止广播、对端探测读同竞，Disposed 胜出即 break 'drive（future drop → BlockedWait Drop abort / SlowWait drop 取消，与 dispose 同取消口径）；探测字节保全按窗口事实落 outer_recv 通道，不得直写换出壳
2 测试验证点：脚本内 BLPOP 0 挂起 + 对端 FIN，断言观察者即时注销（broker 队列空）且泵退出、元素不被误弹出；活连接挂起窗到站流水线字节续跑后照常消费；既有 lua_script_tests 回归全绿

审核裁定执行方案（审核席修订，覆盖票面方案 1 的两处硬伤）：

1 硬伤一（借用结构，票面直插不可编译）：resume_suspended_script_fut 签名为 `&'a mut self -> Pin<Box<dyn Future + 'a>>`（traits.rs:199-205），协程执行体全程持会话可变借用；probe_race 现签名另收 `session: &mut C` 供 preserve_probe 的 take_recv_scratch 直填——两落在同一调用点构成 E0499 双重可变借用。阻塞臂无此问题系因 BlockedWait 先经 take_blocked_wait 移出会话、resolve 与会话解耦；脚本协程 VM 绑定会话，此解耦形态不可复用。
2 硬伤二（换出壳，票面已识别但「落 outer_recv 通道」修法过重）：挂起窗内 session.recv_buffer 为 lua.rs:121 mem::take 换出的空壳，preserve_probe 直写该壳的字节被 lua.rs:194 窗口关闭覆没，且窗内每次 redis.call 重入 dispatch_resp（lua.rs:483-484）clear+覆写同一壳，窗中段并入亦不可活。修订：不动 lua.rs 换出机制，探测字节改在竞速收场后并入——竞速 Resolved 时 resume future 必已完成，lua.rs:194 已把真身还原进 recv_buffer，彼时 preserve_probe 的 take_recv_scratch 直写即真身，下一消费轮照常吃进，字节零丢失。
3 收口形态（保持样板单点，禁第二套探测机制）：probe_race 内部把「探测到站即保全」改为「竞速期本地累积、收场时一次保全」——本地 probe_buf 累积活连接到站字节，Resolved 胜出连同挤干臂字节一并返回或落会话，Disposed 弃收（连接将死，与现形等价）。对阻塞/慢臂行为中性：竞速期泵本不消费，保全时点后移至收场前无观测差，add_net_bytes 入账仍先于下一读取段基线（drive.rs:722-724 的净增口径不变式保持）。落地形态二选一：probe_race 收 session 参数改由调用方收场后调 preserve_probe（单机制、三调用点收口），或 probe_race 内部以收场回调/返回字节承载——严禁另写第二份竞速循环。
4 脚本臂收场口径：Disposed 胜出 break 'drive，resume future drop → 挂起体局部 BlockedWait Drop abort（lua.rs:146 take 出的局部随 future 丢弃）、慢执行体内观察者随 ObserverDropGuard 注销，与现终止胜出路径同一取消口径；对端探测读（EOF/is_dead_conn）胜出与终止广播同形。
5 测试验证点（维持票面，落点明确）：wedb/wnode/tests/net_pump_consume_tests.rs 既有桩消费者 + 真实 TcpStream 泵夹具形制现成（ScratchLineConsumer/QuitConsumer 同款），加脚本挂起桩（has_script_suspend 置真 + resume_suspended_script_fut 覆写为长挂执行体）三案——a) 挂起窗对端 close 即 FIN，断言泵退出且观察者注销（broker 队列空）；b) 挂起窗活连接到站流水线字节，断言续跑后照常消费应答；c) 无断连回归（挂起体正常 resolve 全链绿）。撤销探测臂反证自检案 a 即红。
6 票面 C# 对照段按审核结论订正归档（:214 守护仅未决期有效、C# 盲窗同形），后续对拍席遇此轴直引本裁定，勿按原票面 C# 即时注销叙事复报。
