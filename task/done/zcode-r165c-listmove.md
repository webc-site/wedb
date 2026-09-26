甄别结论：通过（甄别席 zc-fix-r16-listmove，2026-09-26）定级 P1
案一成立：collection_item_source.rs 现码亲验——:118 装载 src、:209 异键臂同键时二次独立装载 dst、:247 try_move_next_list_item（wcol collection_item_broker.rs:844 弹 src 推 dst 确认）、:252 同键双写回后写覆盖前写致元素蒸发、:255 moved(curr_count-1) 计数失真均属实；同键捷径 :233 仅罩同端/单元素。C# 锚 ListOps.cs:212-301（:229-231 双键排他锁、:253 sameKey 窥视、:296 Commit）、CollectionItemBroker.cs TryGetNextListResult:394-471（同键捷径 :428 附近，弹推经同记录 RMW 计数不变）、TryGetResult:566 锁段 585-598、GetCollectionItemAsync:127-163、ListCommands.cs:317-376 逐条对 garnet 现树核验成立。rust 快臂 write.rs list_move_core 与慢臂 slow.rs move_core_cold 均具 same_key 就地旋转单写回分支，票面「双臂对照」成立。
案二成立：zset_outcome 双保护锚 :286 try_rmw_window、:319/:336 obj_writeback_recheck_sync 精确命中；list_outcome 全臂无窗，:155/:186/:252 裸调 save_list；:13-16 头注自述「List 出件臂同形裸态属父票 list 侧留尾」。父票 load-type-rmw-window 于 todo/ing/reject/issue/deviations 全池 grep 零命中，留尾未在册、现状缺陷仍存在，补齐与 zset 臂同套机制，非重复非扩面。
案三推翻：C# ListBlockingMove（ListCommands.cs:354-376）无任何会话内非空预探，无条件 MoveCollectionItemAsync 入经纪（码注 "On the networking thread, no choice but to block"），立即试取由经纪主循环 InitializeObserver 即时 TryGetResult 承担（CollectionItemBroker.cs:253/:269）；deviations.md 第 24 条明文「无条件 BlockingWait…恒阻塞不可选」且生产恒注入经纪。票面「Garnet 契约须会话上下文立即出件」系对原型误读，park 前预探将另立第二套立即取件机制违单套基准。执行席仅落案一案二，案三及 blocking.rs 死分支注随案二头注清理一并处置，不按其方案改调度链。
deviations 第 100 条仅在册异键「先目标后源+重放补偿」写回序，不豁免同键双写毁正；本票改动与五臂同款先例自洽。

审核结论：通过，定级 P1。
确证 BLMOVE/BRPOPLPUSH 同键旋转时在 CollectionItemSource 中先写 dst 后写 src 造成双写覆盖与元素永久丢失、计数失真；确证 List 族经纪出件缺失 RMW 锁窗保护；确证激活经纪时非空源未短路立即可取分支。执行方案清晰完备，供 task/fix.md 直接消费。
合入哈希：d2d4eea 收口形态：list_outcome 同键旋转改在已装载 src 上就地弹推、单写回、found(curr_count) 不派冗余唤醒（案一），BLPOP/BRPOP/BLMPOP 取 try_rmw_window 单键窗、BLMOVE 取 try_sync_rmw_window_pair 桶升序双窗且各待写回键落笔前 obj_writeback_recheck_sync 复验、失闩报 contended 交经纪让核重投、头注留尾声明清理（案二），tests/resp_blocking_commands 增同键 BLMOVE/BRPOPLPUSH 旋转回归两例（旧码必红）（案三驳回不落地，依 §24）。

List 族双端移动与阻塞边界原子写及应答契约审查报告

一、审查视角与背景说明
审查视角：List 族双端移动与阻塞边界 (LMOVE/BLMOVE/RPOPLPUSH/BRPOPLPUSH 在源与目标同键/异键、空列表删空自愈、阻塞挂起取消时的原子写与应答契约)
核查目标与范围：
1. 核查 wedb/wnode/src/resp/objects/list_commands/ 与 collection_item_source.rs 中 LMOVE, BLMOVE, RPOPLPUSH, BRPOPLPUSH 命令实现。
2. 核查源键与目标键相同时的循环旋转原子性、异键移动时的跨键锁序与防死锁策略。
3. 核查源列表弹空后的删空自愈（是否原子删除键、清除元数据与 TTL、避免悬挂空列表）。
4. 核查阻塞移动超时或客户端断连/取消时的状态清理，对标 garnet/libs/server/Storage/Session/ObjectStore/ListOps.cs、garnet/libs/server/Resp/Objects/ListCommands.cs 与 garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs。
5. 核验 doc/zh/deviations.md 既有在册条款，严禁将既定架构改良报为缺陷。

二、原型行为与对标核查（C# Garnet 事实确证）
1. C# 官方契约与源码现状
对应 c# 文件与函数：
garnet/libs/server/Storage/Session/ObjectStore/ListOps.cs:StorageSession.ListMove
garnet/libs/server/Resp/Objects/ListCommands.cs:RespServerSession.ListMove
garnet/libs/server/Resp/Objects/ListCommands.cs:RespServerSession.ListRightPopLeftPush
garnet/libs/server/Resp/Objects/ListCommands.cs:RespServerSession.ListBlockingMove
garnet/libs/server/Resp/Objects/ListCommands.cs:RespServerSession.ListBlockingPopPush
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CollectionItemBroker.MoveCollectionItemAsync
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CollectionItemBroker.GetCollectionItemAsync
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CollectionItemBroker.TryGetResult
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CollectionItemBroker.TryGetNextListResult
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CollectionItemBroker.InitializeObserver
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CollectionItemBroker.TryAssignItemFromKey

核查确证事实：
1) 单事务多键加锁与原子提交：
在 ListOps.cs:212-301 中，ListMove 显式校验源与目标是否为同一键 sameKey。若非处于事务中，通过 txnManager 登记 sourceKey 与 destinationKey 的排他锁（LockType.Exclusive），并通过 txnManager.Run(true) 按照确定的键桶顺序统筹加锁防死锁。其内部对源键执行 GET 校验类型与非空；若是异键，再对目标键进行 GET 预检 WRONGTYPE 防误弹源元素。随后通过 ListPop 与 ListPush 依次 RMW 操作，并在 finally 块统一 Commit 提交事务。提交后仅针对目标键调用 HandleCollectionUpdate 唤醒等待者。
2) 同键同向与单元素旋转窥视捷径：
当 sameKey 为 true 时，若 sourceDirection == destinationDirection 或列表元素数量为 1，ListMove 认定此时弹推为 no-op，直接窥视端点元素返回，严禁执行 pop 与 push，防止清空列表破坏现有 TTL。
3) 阻塞移动与经纪出件原子性：
在 ListCommands.cs:317-376 中，BLMOVE 与 BRPOPLPUSH 经 PinnedSpanByte 组装目标键与方向参数，调用 MoveCollectionItemAsync 进入经纪等待。在 CollectionItemBroker.cs:TryGetResult（第 586-598 行）中，试取同样受事务排他锁保护（加锁 sourceKey 与 dstKey）。进入 TryGetNextListResult（第 418-471 行）后：
同键同端或单元素直接作为 no-op 返回原值；
异端旋转或异键移动时，依次调用 ListPop 弹出元素与 ListPush 推入目标键；
由于底层 Tsavorite 事务保证，同键旋转在提交前为一个原子的 pop+push 整体，不会产生中间态撕裂；出件成功后在 finally 块中把 dstKey 放入 broker 事件队列唤醒目标观察者。
4) 超时与取消状态清理：
在 GetCollectionItemAsync（第 139-163 行）中，等待 ResultFoundSemaphore 并在超时或取消后将 observer 状态置为 Empty。主循环与队列清理逻辑 CleanKeysToObservers 会及时剔除已完成或已销毁的观察者，不会发生死锁或悬挂泄漏。

三、工程现状确证（Rust wedb 实现核查）
1. 模块结构与实现路径
rust 文件与函数：
wedb/wnode/src/resp/objects/list_commands/write.rs:RespServerSession::list_move
wedb/wnode/src/resp/objects/list_commands/write.rs:RespServerSession::list_right_pop_left_push
wedb/wnode/src/resp/objects/list_commands/write.rs:RespServerSession::list_move_core
wedb/wnode/src/resp/objects/list_commands/blocking.rs:RespServerSession::list_blocking_move
wedb/wnode/src/resp/objects/list_commands/blocking.rs:RespServerSession::list_blocking_pop_push
wedb/wnode/src/resp/objects/list_commands/blocking.rs:write_collection_item_result
wedb/wnode/src/resp/objects/list_commands/slow.rs:move_core_cold
wedb/wnode/src/resp/objects/list_commands/slow.rs:wait_src_or_null
wedb/wnode/src/resp/objects/list_commands/mod.rs:list_save_or_gc
wedb/wnode/src/resp/objects/collection_item_source.rs:CollectionItemSource::list_outcome
wedb/wcol/src/itembroker/collection_item_broker.rs:try_move_next_list_item
wedb/wcol/src/itembroker/item_broker_face.rs:BlockedWait::resolve
wedb/wcol/src/itembroker/item_broker_face.rs:BlockedWait::abort
wedb/wnode/src/resp/resp_server_session/pump.rs:RespServerSession::park_broker_wait
wedb/wnode/src/net/handler/drive.rs:ConnectionHandler::drive

核查确证事实：
1) 快慢路径双键锁序与倒序重放收敛：
在 write.rs 的 list_move_core（第 374 行）与 slow.rs 的 move_core_cold（第 809 行）中，均通过 try_sync_rmw_window_pair 与 rmw_window_pair_async 按照哈希桶升序（rmw_window_sorted）单机制获取双键 RMW 排他锁窗，彻底杜绝跨键加锁逆序死锁。
按照 doc/zh/deviations.md 第 100 条在册裁决，wedb 无事务打包机制，写回序采用先目标后源。目标写回失败时源零变异，重放补偿收敛。
2) 删空自愈机制闭环：
无论同步段 list_save_or_gc 还是慢路径 save_or_gc，列表为空时均传 is_empty = true 触发 obj_save_or_gc_sync -> try_delete_sync，级联清理用户记录、信封墓碑、TTL 与 ETag，不存在悬挂空列表。
3) 超时与取消清理收口：
网络泵 drive.rs 采用三路竞速（probe_race），套接字断连或取消时触发 BlockedWait::abort，同步调用 handle_session_disposed 清退观察者；慢路径 BlockWaitFace::wait 内嵌 ObserverDropGuard 守卫，异常中止时同点注销，防僵尸等待者滞留。

四、缺陷与增量隐患确证（问题清单与逻辑危害）

案一：BLMOVE/BRPOPLPUSH 在经纪取件源（CollectionItemSource）中同键旋转双写覆盖致元素丢失与计数失真
问题分析：
1. 原型契约对齐：
在 Garnet 中，BLMOVE/BRPOPLPUSH 在源键与目标键相同时属于循环旋转（rotation）命令。单元素或同端位移直接作为窥视（peek）返回，多元素且异端旋转（如 BLMOVE k k LEFT RIGHT 或 BRPOPLPUSH k k）在存储中作为一个原子上下文，弹出端点元素并将其推入对端，列表元素总量保持不变，旋转后的列表持久保留。
2. 工程现状确证：
在 wedb/wnode/src/resp/objects/collection_item_source.rs 的 list_outcome 中（第 194-260 行）：
当 key == dst_key 且 curr_count > 1、src_dir != dst_dir 时，未能命中第 233 行的窥视捷径，落入通用搬移分支：
首先，在第 118 行已从 key 装载了内存对象 src（包含当前所有元素）；
随后，在第 209 行又从 dst_key（即同一个 key）通过 obj_load_typed_sync 独立装载出第二份全新的 ListObject 实例 dst；
紧接着，在第 247 行调用 try_move_next_list_item(&mut src, &mut dst, src_dir, dst_dir)，从 src 弹出元素 A（src 变短为 N-1 个元素），并将 A 推入 dst（dst 变长为 N+1 个元素）；
随后在第 252 行顺序执行双写回：
save_list(&batch, dst_key, &dst); // 先将含有 N+1 元素的 dst 写入 key
save_list(&batch, key, &src);     // 紧接着将仅含 N-1 元素的 src 再次写入 key！
后一次写入彻底覆盖了前一次写入，存储中最终保留的是被弹空了元素 A 的 src。被弹出的元素 A 在推入 dst 后被第 252 行的二次写入当场抹杀丢弃！
此外，第 255 行返回 TryGetOutcome::moved(curr_count - 1, CollectionItemResult::single(key.to_vec(), moved_item), dst_key.clone())：
同键旋转本应保持元素数不变，此处却将 curr_count 错误扣减 1；并且向 broker 投递了 dst_key 的冗余唤醒事件，破坏了集合长度记账与事件循环状态机。
对比快路径 list_move_core（write.rs:428-456）与慢路径 move_core_cold（slow.rs:871-879），双臂均具备明确的 same_key 分支，仅在同一个 src 对象上就地旋转并仅写回一次。collection_item_source.rs 的实现脱节破坏了多路径行为同构。
3. 逻辑危害：
生产环境下，客户端使用 BLMOVE k k LEFT RIGHT 或 BRPOPLPUSH k k 调度常驻任务队列时，只要触发经纪取件源执行，该次旋转操作会导致队首任务被取出后，其在队列中的流转完全丢失，队列长度永久递减，最终将整个队列抽空报废，引发严重的数据丢失与业务中断。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/collection_item_source.rs:CollectionItemSource::list_outcome
wedb/wcol/src/itembroker/collection_item_broker.rs:try_move_next_list_item
对应 c# 文件与函数：
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CollectionItemBroker.TryGetNextListResult
garnet/libs/server/Storage/Session/ObjectStore/ListOps.cs:StorageSession.ListMove

精炼执行方案：
1. 在 collection_item_source.rs 的 list_outcome 中，重构 BLMOVE 分支对 key == dst_key 的处理：
若 key == dst_key.as_slice()：
同端搬移或 curr_count <= 1 维持现有窥视逻辑不变；
异端旋转时，严禁重复装载 dst。直接在已装载的 src 上执行端点弹出并推入对端；仅调用一次 save_list(&batch, key, &src)；返回 TryGetOutcome::found(curr_count, CollectionItemResult::single(key.to_vec(), moved_item))，不扣减元素数，不向 dst_key 派发冗余通知。
2. 在 wedb/wnode/tests/ 增加同键 BLMOVE 与 BRPOPLPUSH 旋转测试用例，断言多元素同键旋转后元素完整保留、顺序正确倒转且长度不减。


案二：List 族经纪出件未接线 RMW 互斥窗与落笔前域归属复验（遗留技术债务致并发写丢与撕裂）
问题分析：
1. 原型契约对齐：
在 Garnet 中，CollectionItemBroker.TryGetResult（第 586-598 行）在执行任何集合出件前，必须通过 txnManager 登记待操作键（若为 BLMOVE 则同时登记 asKey 与 dstKey）的排他锁，并在 txnManager.Run(true) 事务保护下串行执行。取件与落盘全程杜绝与会话线程的写操作发生并发撕裂。
2. 工程现状确证：
在 wedb/wnode/src/resp/objects/collection_item_source.rs 中：
同文件的 ZSet 取件臂 zset_outcome（第 286 行、319 行、336 行）已严格遵循双保护规范：装载前通过 batch.try_rmw_window(key) 获取键桶排他锁窗，遇争用返回 TryGetOutcome::contended() 告知经纪主循环让核重投；写回前调用 obj_writeback_recheck_sync(&batch, key, true) 严格复验域归属。
然而在 list_outcome（第 110-263 行）中，整个 List 取件臂（BLPOP/BRPOP/BLMPOP/BLMOVE）既未获取任何 RMW 锁窗（无论是单键 try_rmw_window 还是双键 try_sync_rmw_window_pair），也未在落笔前调用 obj_writeback_recheck_sync（第 155、186、252 行均裸调 save_list）。
在文件头部（第 13-16 行）注释明文自述：ZSet 出件臂与命令层装载型写臂同规格取 rmw 窗并做落笔前域归属复验（票 load-type-rmw-window zset 留尾），不可取即整体拒写重试；List 出件臂同形裸态属父票 list 侧留尾，本票不扩面。
该留尾导致 List 经纪出件处于完全裸态。
3. 逻辑危害：
经纪主循环在后台独立任务中执行，与各工作线程处理客户端请求（如 LPUSH/RPUSH/DEL/SET 等）处于并发竞争中。
当某一连接正在向列表推入新元素，或另一客户端执行 DEL/SET 覆写键时，由于 list_outcome 未持桶排他锁且未复验域归属，经纪取件时读到的内存镜像在出件后通过 save_list 盲目整包回写，将并发线程刚刚 ACK 的写入直接抹杀，造成隐蔽而严重的数据写丢失与幻键复活。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/collection_item_source.rs:CollectionItemSource::list_outcome
wedb/wnode/src/resp/objects/collection_item_source.rs:save_list
对应 c# 文件与函数：
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CollectionItemBroker.TryGetResult

精炼执行方案：
1. 对标 zset_outcome 与 write.rs 双保护规范，补齐 list_outcome 锁窗机制：
对于 BLPOP/BRPOP/BLMPOP，装载前获取 batch.try_rmw_window(key)，失锁返回 TryGetOutcome::contended()；
对于 BLMOVE，装载前通过 try_sync_rmw_window_pair(&batch, key, dst_key) 获取双键桶升序排他窗，失锁返回 TryGetOutcome::contended()；
2. 在 save_list 之前，对所有待写回键（源键及 BLMOVE 目标键）调用 obj_writeback_recheck_sync 进行域归属复验，复验失败直接返回不可取，严禁盲目裸写覆盖。
3. 清理 collection_item_source.rs 文件头部的历史留尾声明注释。


案三：BLMOVE/BRPOPLPUSH 激活经纪时非空源未短路立即可取分支强制跨线程排队
问题分析：
1. 原型契约对齐：
在 Redis 标准与 Garnet 契约中，阻塞移动命令（BLMOVE/BRPOPLPUSH）与阻塞弹出命令类似，属于阻塞备选（blocking optional）语义：当源键在执行当时已存在且非空时，必须立即在当前会话的执行上下文中完成出件并即时应答，绝不允许无故产生不必要的跨线程异步挂起。
2. 工程现状确证：
在 wedb/wnode/src/resp/objects/list_commands/blocking.rs 中：
list_blocking_move（第 190-200 行）与 list_blocking_pop_push（第 228-251 行）在完成语法解析和超时解析后：
在 any_sync_degrade 判定通过后，无条件直接调用 self.park_broker_wait(RespCommand::Blmove, ...)；
在服务端运行环境下，self.item_broker 恒为 Some，因此 park_broker_wait 必定挂起会话并返回 true；
紧随其后的第 199 行与第 243 行 self.list_move_core 退化成了仅在无经纪注入（如裸单元测试）下才可达的死分支。
这意味着：即便源列表在内存中早已存在且有大量元素，客户端发送 BLMOVE 或 BRPOPLPUSH 时，依然会被强制放入 pending_block，由网络泵将其挂起，将 NewObserver 事件丢入 broker 事件队列，经历跨任务排队、主循环调度与经纪会话取件流程。
3. 逻辑危害：
对非空存量列表的双端移动命令造成严重的性能劣化与延迟抖动。每次操作增加不必要的协程让渡、跨线程通道收发与锁竞争开销，违背 review.md 板块 3.1 零拷贝与单次迭代、控制面不污染数据面的效能纪律。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/list_commands/blocking.rs:RespServerSession::list_blocking_move
wedb/wnode/src/resp/objects/list_commands/blocking.rs:RespServerSession::list_blocking_pop_push
对应 c# 文件与函数：
garnet/libs/server/Resp/Objects/ListCommands.cs:RespServerSession.ListBlockingMove
garnet/libs/server/Resp/Objects/ListCommands.cs:RespServerSession.ListBlockingPopPush

精炼执行方案：
1. 重构 list_blocking_move 与 list_blocking_pop_push 的执行前置逻辑：
在 park_broker_wait 之前，先以非阻塞方式在当前会话 store 上试探执行 list_move_core；
若源列表存在且成功移动元素，直接在当前连接写回应答并返回 Ok(true)；
若 list_move_core 检测到源列表缺失或为空集合，再调用 park_broker_wait 挂起经纪等待。
2. 增加针对预置数据列表的 BLMOVE/BRPOPLPUSH 延迟与立即出件断言测试，保证立即可取分支不发生挂起调度。

七、核验与裁决结论
经过对 List 族双端移动命令（LMOVE/BLMOVE/RPOPLPUSH/BRPOPLPUSH）在快路径、慢路径、阻塞挂起及经纪出件全链路的深度代码比对：
1. 快慢路径（write.rs 与 slow.rs）上的双键排他锁序、删空自愈以及超时/取消清理机制完备可靠，符合 deviations.md 第 100 条裁决；
2. 但在集合项经纪取件源（collection_item_source.rs）中，BLMOVE/BRPOPLPUSH 的同键多元素旋转存在破坏性的双写覆盖缺陷，会导致元素被无端永久删除，且整个 List 经纪出件臂存在未接线 RMW 锁窗和落笔复验的历史留尾漏洞；
3. blocking.rs 中 BLMOVE/BRPOPLPUSH 存在非空源未短路立即可取分支强制进经纪排队的调度缺陷。

视角结论:有增量
