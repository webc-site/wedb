SubscribeBroker::dispose 全仓零生产调用：停机链缺宿主层 broker 收口步骤

来源：glm.net 第 11 条（分拣判定成立且待做）。取证基线：主仓 HEAD 50d1cb5f，行号为当下实况。

现状
- 带锚点的死面：/Users/z/git/db/wedb/wedb/wpubsub/src/subscribe_broker.rs:487-501
  `pub fn dispose(&self)`（:488 置 disposed、:490 pending_event.notify 唤醒消费任务、
  :491 clear 待发队列、:492-499 清三张订阅表），文档注释 :486 明挂
  `libs/server/PubSub/SubscribeBroker.cs:Dispose`；全仓调用只有本文件测试体
  （:681 `f.broker.dispose();`），src 面零调用。
- 被收口的后台任务确在跑：/Users/z/git/db/wedb/wedb/wnode/src/service.rs:581-588
  spawn_pubsub_consume_task（:583 `while broker.wait_pending().await`，其退出依赖
  subscribe_broker.rs:462/:469 的 disposed 判定）由 :1502 在 broker 装配时启动；
  broker 本体为 /Users/z/git/db/wedb/wedb/wnode/src/service.rs:886
  `pub pubsub: Option<Arc<SubscribeBroker>>`，构造点 :991。
- 两条停机链都不触及该步：
  单机/集群统一尾部 /Users/z/git/db/wedb/wedb/wnode/src/server.rs:301-308
  （仅 cluster_provider.dispose() :305 + flush_config() :306）；
  宿主关机 /Users/z/git/db/wedb/wedb/wnode/src/server.rs:701-716 GarnetServer::stop
  （shutdown_coordinator :702 → AOF 背压放行 :706-710 → join worker :711-714 → buffer_pool.purge :715）。
- 后果：worker 运行时析构时消费任务无显式收口点，pending 队列在途消息与正在分发的批次随
  线程退出被截断（无「等当前批次完成再清表」的语义），且 dispose 已写体长期零读者，
  与 C# 停机时序不对位。属关机接线缺口，不是可删死代码（有明确 C# 消费点对位）。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/host/GarnetServer.cs:566-584 InternalDispose 分相：
  Phase1 servers[i].Close()（:568-569）→ Phase2 servers[i].Dispose() 排空在途处理器（:572-573）→
  Phase3 Provider?.Dispose()（:576）→ :578 `subscribeBroker?.Dispose();`
  即 broker 收口排在连接排空与 provider 关机之后。
- /Users/z/git/db/wedb/garnet/libs/server/PubSub/SubscribeBroker.cs:392-401 Dispose：
  :394 disposed=true → :395 cts.Cancel() → :396 done.WaitOne()（同步等后台消费循环跑完当前批次）→
  :397-398 清 subscriptions / patternSubscriptions → :399-400 释放 aof 与 device。
- broker 构造点（对照 rust service.rs:991）：GarnetServer.cs:277 `new SubscribeBroker(...)`。
- 同类先例：本仓 item broker 的同一缺口已立项为
  /Users/z/git/db/wedb/task/ing/itembroker-shutdown-dispose.md（CollectionItemBroker::dispose 零调用），
  本票是该族第二张，二者消费点不同（pubsub 中枢 vs 集合项经纪），不互为覆盖。

修法
1. 在 GarnetServer::stop（/Users/z/git/db/wedb/wedb/wnode/src/server.rs:701-716）的连接排空之后、
   join worker 之前（:710 与 :711 之间）补 `if let Some(broker) = &self.session_provider.pubsub { broker.dispose(); }`，
   时序对标 C# Phase2 → Provider.Dispose → broker.Dispose 的相对位置；
   若判定该位置与 provider 关机次序须重排，须在注释点名与 C# 的差异及原因。
2. 收口语义须真「等在途批次」：C# 用 done.WaitOne() 同步等循环退出。rust 侧当前是 detached
   任务 + disposed 标志，dispose 仅唤醒不等待。落地二选一——
   a) 消费任务退出时经既有关闭协调器/CompletionChannel 回报（对位 C# `done`），stop 侧有界等待；
   b) 明确登记差异：说明 compio 下 join worker（:711-714）已构成「等任务退出」的等价屏障，
      dispose 只需在其之前置位唤醒。禁既不等又不声明。
3. pubsub 取口经会话提供者门面单点（与 aof()/backpressure() 同族的既有访问形态），
   禁在停机链里从别处再取一份 broker 克隆。
4. 不得把本票误解为删 dispose：删函数与该函数注释里的 C# 锚点会造 check.js 缺失锚点，
   且 C# 确有该停机步骤。

优先级
功能缺口（停机序列缺失既有 C# 对位；在途消息截断与死面对偶风险，但非数据面正确性缺陷）。

协调
- 与 task/ing/itembroker-shutdown-dispose.md 同一 stop 函数：两票若并行落地须一次排好
  停机步序（连接排空 → item broker → pubsub broker → aof），禁两次插入互相覆盖。
- next/task-manager-single-lifecycle-track.md（若仍在册）管后台任务生命周期登记轨道；
  本票只补 broker 收口调用，不改任务登记机制，两票若同时命中 stop 函数须错开。

验收
- 停机路径单测/集成用例：预置若干 pending 消息与订阅者，触发 stop，断言 dispose 被调用
  （disposed=true、三表清空、消费任务退出），且不再有「写了 dispose 无人调」的 grep 结果。
- grep 验收：`rg "\.dispose\(\)" wedb/wpubsub/src` 之外，src 面至少一处生产调用。
- 现有 pubsub 测试与停机相关测试期望复核，禁仅改期望值放行。
- 验证纪律：仅 cargo check --workspace --all-targets（私有 target 目录）零 error 零 warning。

## 细化方案（f22-broker-dispose，2026-09-19）

甄别：成立。行号按当前 HEAD 05dc7ec9 校正（dispose 现位于 subscribe_broker.rs:512-526，
stop 位于 server.rs:709-731，spawn_pubsub_consume_task 位于 service.rs:583-590，
消费任务拉起位于 service.rs:1517-1521）。itembroker-shutdown-dispose.md 已不在册
（cb6dd74e 走 wcol 形态收口，无停机接线），协调条款无并行冲突。

等待语义选 a（真等在途批次，对位 C# done.WaitOne()），与 vector dispose_cleanup/
wait_stopped 先例同构（CleanupRuntime：状态位 + Event + 有界等待）。

C# 语义源（SubscribeBroker.cs）：
- :28 `ManualResetEvent done = new(true)`（状态位，初始无任务在跑）
- :173 Initialize 内 `done.Reset()` 后 Task.Run(StartAsync)
- :134 StartAsync finally `done.Set()`
- :396 Dispose 内 `done.WaitOne()`（无超时，清表之前）

### 改动面

1. wedb/wpubsub/src/subscribe_broker.rs
   - 新字段：consumer_live: AtomicBool（done.Reset/Set 的状态面）+ consumer_done: Event（唤醒面）
   - 新方法 consumer_start()（对位 Initialize 的 done.Reset，宿主消费任务体首行调）、
     consumer_finish()（对位 StartAsync finally 的 done.Set，退出循环后调）
   - dispose() 在 notify 与 clear 之间插 wait_consumer_exit()（对位 done.WaitOne()）：
     仅 consumer_live 为真时等；loop「查 live → listen → 再查 live → wait_timeout」
     防 listen/notify 窗口错过；超时 warn 放行（C# 无界，rust 有界防停机挂死，
     常量 10s）。清表语义保持 C# 顺序（等退出后才清）
2. wedb/wnode/src/service.rs
   - spawn_pubsub_consume_task 任务体首尾接 consumer_start/consumer_finish
   - StorageSessionProvider 实现 SessionProviderFace::dispose_pubsub（pubsub Some 才调，
     门面单点，禁停机链另取 broker 克隆）
3. wedb/wnode/src/traits.rs
   - SessionProviderFace 新默认方法 dispose_pubsub()（默认空 = 未装配 pubsub 宿主；
     NullProvider/测试 provider 零改动）
4. wedb/wnode/src/server.rs stop()
   - AOF 背压放行之后、join worker 之前调 session_provider.dispose_pubsub()
   - 注释点名与 C# 次序差异：C# broker.Dispose 在 Provider.Dispose 之后；rust 的 AOF
     完整收口（dispose_async）须 join 后主运行时直驱（async 设备 IO 进不了同步 stop），
     而消费任务跑在 worker 运行时——dispose 等待需运行时驱动，必须先于 join。
     结构性重排：join = worker 运行时析构屏障，broker 收口只能前置

### 验证

- wpubsub 单测（thread + block_on 基建现成）：
  dispose_waits_for_consumer_exit（后台线程跑消费循环 + 预置订阅与 pending，
  dispose 返回后断言 live 清零、三表清空、pending 排空）；
  dispose_without_consumer_no_wait（无消费任务直接返回）
- wnode 侧仅接线，不另起重型停机集成测试；验证纪律 cargo check
- grep 验收：wnode/src 面经 dispose_pubsub 门面出现 broker.dispose 生产调用

## 落地状态（2026-09-19 f22-broker-dispose）

实现已完成于分支 f22-broker-dispose（worktree /tmp/fork/f22-broker-dispose，
提交 104d96b3，已两次 merge dev 至 de9f7d3a 之后的最新 dev）：

- wpubsub/wnode 四文件改动如上细化方案，cargo check -p wpubsub -p wnode
  --all-targets 零 error 零 warning
- 全仓 cargo check --workspace --all-targets 当前红 48 处，全部位于
  wext_json/tests/json_commands_test.rs（25）与 wext_roaring/src/
  roaring_bitmap_commands.rs（21+2），系 dev 基线损坏（de9f7d3a 提交信息
  自述「dev 侧 wext_json/wext_roaring arity 红待对账」），与本票改动无交集

未合入主仓（按流水线第 10 步：等 60 秒重 merge dev 后仍坏）。待 dev 基线
修复后：worktree 内 cargo check 全绿 → 主仓 merge --no-ff f22-broker-dispose
-m "merge: subscribe-broker-shutdown-dispose (fixloop)" → 本票归档
task/done/ → 清理 worktree 与分支。
