任务名称: r4client-kill-blocked-session

来源 next/zcode-r4-client.md 问题 2（轮4 客户端会话生命周期专项）。

问题一句话
CLIENT KILL 与停机排空对「阻塞命令挂起中」的会话无即时效力：kill 位只打断挂起读，泵停在
blocked.resolve() 上时没有任何路径观察终止事件，被杀连接不关、注册表条目不摘、被杀端收不到 FIN。

rust 现状
- wedb/wnode/src/net/handler/kill.rs:18 spawn_kill_watcher 是全仓唯一的终止观察点，收场只做
  kill_token.cancel()（:31），该令牌只挂在网络读取段的 read future 上
  （wedb/wnode/src/net/handler/drive.rs:296-358 的三种读形态）。
- wedb/wnode/src/net/handler/drive.rs:179 `let (cmd, result) = blocked.resolve().await;`
  在消费段，不持取消令牌，也不复查条目 is_terminating()，终止广播对这一段 await 完全不可见。
- wedb/wcol/src/itembroker/item_broker_face.rs:161 BlockedWait::resolve 只等
  observer.wait_result()（:162-164 无限档，timeout_secs <= 0.0 即 BLPOP 0 一类）或
  timeout 包裹的同一等待（:166-171），然后 broker.finish_wait；不存在第三条唤醒通道。
- wedb/wnode/src/resp/resp_server_session/pump.rs:93 take_blocked_wait 已把挂起体从会话
  pending_block 取走，故 wedb/wnode/src/resp/resp_server_session/core.rs:403 dispose 的
  `self.pending_block.take()` 恒为 None，block.abort()（item_broker_face.rs:151 →
  broker.handle_session_disposed）落空，观察者残留在经纪的每键队列里。
- 后果面：阻塞族（BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX/BZMPOP）timeout 0 形态被 KILL 后
  连接全开、CLIENT LIST 持续可见、被杀端无任何断连感知，直至真有元素到达；同一根因令
  wedb/wnode/src/servers/consumer_registry.rs:575 dispose_active_handlers 对每条该形态会话
  必等满 DRAIN_TIMEOUT_MS（:44 = 5000ms）后 warn 强弃并留痕（:585-589），关停被拖满 5 秒。
- 现存补救仅 CLIENT UNBLOCK（经经纪强解 observer.wait_result 可达），KILL 自身语义残缺。

C# 证据（逐字核对 /Users/z/git/db/wedb/garnet）
- libs/server/Resp/RespServerSession.cs:1319 `public bool TryKill() => networkSender.TryClose();`
  即关套接字，FIN 即刻出网，与该会话此刻是空闲、读挂起还是阻塞等待中无关。
- libs/server/Resp/ClientCommands.cs:205 NetworkCLIENTKILL 在 :394 `if (session.TryKill())`
  对目标会话调用，杀掉即计数。
- C# 的阻塞等待发生在网络线程本身（AsyncUtils.BlockingWait 形态），TryClose 后该线程续跑时
  写失败收场，客户端侧立见断连；两侧可观察差就是「连接有没有当场关」。

修法
1. 让挂起体的等待与条目终止竞选：drive.rs 消费段取到 blocked 后，把 blocked.resolve() 与
   `entry.listen_terminate()`（consumer_registry.rs:156，与 kill.rs 哨兵同一广播、同一真值源
   is_terminating()）做双路 await，命中终止即 break 'drive，复用
   wedb/wnode/src/net/handler/drive.rs:42-50 既有的 stream.shutdown → dispose 关闭序，
   不新写收场路径。
2. 终止路径必须先摘观察者再退出：把已取走的挂起体由泵局部持有，命中终止时对其调
   block.abort()（经 handle_session_disposed(self.id) 置 SessionDisposed 并唤醒），杜绝幽灵
   观察者滞留每键队列；条目注销仍由 dispose 后的 unregister 承接（现序不变）。
3. 慢路径挂起 slow.resolve()（drive.rs:185）执行体有界、必自收场，不在本票范围，
   不得为它另设第二套终止广播。
4. kill.rs 的读令牌路径不动（读挂起形态已等价于 C# 直关），全仓终止真值源仍是
   ConsumerEntry 的 kill_flag/removed 两位。

红判据
在 wedb/wnode/tests/client_commands_tests.rs（已有真 socket 双连接 harness：start_server /
send_cmd / read_reply）加一用例：会话 A 发 `BLPOP <不存在的键> 0` 挂起，会话 B 发
`CLIENT KILL ID <A 的 id>` 断言回 :1，再对 A 的 socket 限时读：断言远小于 1 秒内读到连接关闭
（read 返回 0 字节），且随后 CLIENT LIST 不再含该 id。修复前该用例必红（A 侧永无应答、socket
不断、LIST 仍可见），修复后转绿。第二判据：consumer_registry.rs 的停机排空单元用例中，注册一条
挂起态条目后 dispose_active_handlers 应在 kill 广播命中即返回（不断言等满 5 秒留痕路径）。

为什么这不是自造优化
C# 的 TryKill 无任何「按会话等待形态分流」的分支，语义就是「令该连接即刻关闭」。rust 把等待拆成
读挂起 / 阻塞挂起 / 慢路径挂起三态后，只给读挂起一态接了终止（kill.rs 注释自陈「等价物为注册条目
kill 位 + CancelToken」），本票补的是同一等价物缺的另一半，不新增能力、不新增超时旋钮、不新增
字段与机制，全部构件（终止广播事件、listen_terminate、abort、dispose 关闭尾）均已存在。

同文件在途纪律
与 task/ing/r4client-registry-live-view.md 同域同文件（wnode/src/servers/consumer_registry.rs、
wnode/src/resp/client_commands.rs，且 CLIENT KILL 集成测试文件亦重叠），须待其落地后开工。
