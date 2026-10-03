甄别结论：通过（甄别席 J7，2026-09-27，定级 P2 勘误——原审 P1 偏高，半移交臂触发窗窄，危害为活性悬挂非数据面）。:319-325 四段单短路+裸回 with_count、:326-330 moved 携 dst_key、消费两 site none 分支 :569-592/:647-667 丢 notify_key、:680 仅 Some 分支读，亲验全实；C# :468 notifyKey 置位、finally :679-680 无条件入队亲验；§100 写回序条不含唤醒贴发面，list-blocking-exec-txn 票零重叠；复用 enqueue_event mpsc 单机制合规。派沙箱席 c01o。

审核结论：通过（P1 真案。链形属实：collection_item_source.rs:319-325 四段并入单短路、:324 裸回 with_count 双无、:326-330 唯全成功出口 moved 携 dst_key；同键 src==dst 已 :243-284 特判折叠；dst 持久后可达失败臂唯 :321 复验（recheck unwrap_or(false)）与 :322 save_list（obj_save_or_gc 门），「src 读空」在 dst 持久前不达；消费端 notify_key 仅 result-Some 分支读，none 分支丢弃；C# 验真：notifyKey :468 置、finally :669-681 恒发，且 C# 先弹 src 后推 dst 同事务内无半持久形。方案裁决：补 notify 方向正确——回滚 dst 需补偿删即第二套判据，违 §100 与 r145c 案一既裁；步 1/2 复用 enqueue_event mpsc 单机制、守 :511-517/:545 锁序注）

整理执行方案（审核席订正版，供 fix 消费）：
1 半提交裸回臂（:324）改携 notify_key（dst 键）回，经纪 none 分支在 :568-591/:646-666 补发 enqueue_event 唤醒 dst 观察者；复用 mpsc 单机制勿新建
2 票面订正：write.rs 唤醒锚 :490-497（notify 在 :491）、src 臂 :499-509；C# finally :669-681；头注点明「None 携 notify_key 系本仓无事务投影新增形态（C# 恒 result 并存）」防回改
3 危害面限 dst 观察者悬挂，发起者自身复挂依 §100 下一 src 事件收敛，勿扩案；锁测：dst 持久之 src 复验失败臂注入 + timeout=0 阻塞者唤醒断言

BLMOVE 经纪出件臂半移交残态（dst 已持久、src 写回失败）不派发目标键唤醒，dst 队列 timeout=0 观察者悬挂至 dst 下一写

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 经纪出件 TryGetResult（garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:603-681）内核置 notifyKey（:468）后，finally 块在事务提交之后无条件把 notifyKey 入队 CollectionUpdated（:674-681，注释自陈「Wake observers blocked on the destination key…enqueue directly…callers hold keysToObserversLock」）。「dst 已持久 ⇒ 目标唤醒必发」在 C# 为结构不变式（事务原子提交或整体回滚，无半持久态）；本仓 deviations §100 已裁无事务打包机制，以「写回序先目标后源」五臂补偿纪律替代，并容忍「元素双份」残态——但该纪律的唤醒贴发面（dst 提交即发、与 src 成败解耦）才是「已提交不漏发」的另一半。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
命令层两臂已收口贴发面：快臂 list_commands/write.rs:list_move_core（:488-496 dst save Ok(true) 即 notify_collection_update(dst_key)，其后 :498-510 src 复验/写回失败臂均在唤醒之后）；慢臂 list_commands/slow.rs:move_core_cold（:871-888，:879-885 注释明言「notify 严禁漂回下方 src save ? 之后——src fail-closed 会把已持久 dst 元素落成已提交漏发永悬态」，票 zcode-r145c-lblpop2 案一）。唯独经纪出件臂 wnode/src/resp/objects/collection_item_source.rs:list_outcome 的 Blmove 异键分支（:317-330）把「dst 复验、dst save、src 复验、src save」四段并入单一短路条件链，dst 已成功持久而后两段落空时裸回 TryGetOutcome::with_count（result=None、notify_key=None），仅全成功出口 :326-330 经 moved 携 notify_key。消费侧 wcol/src/itembroker/collection_item_broker.rs 亦只在 result-Some 分支看 notify_key（try_assign_item_from_key:679、initialize_observer:606），result-none 分支（:646-666 break、:568-591 挂队 continue）直接丢弃。臂不携、经纪不收，两端共同断掉半移交态的唤醒链。
3. 逻辑危害确证
他会话 BLPOP/BLMPOP dst 已挂 dst 观察队列时，BLMOVE 半移交即致 dst 上已持久元素零唤醒：timeout=0 客户端永悬至 dst 下一次写入才被顺带服务，违反经纪自证不变式「外层唯一写点已过、后续无写入时 timeout=0 永久悬挂」（同 TryGetOutcome::is_contended 注释 :121-128 与让核重投机制 :778-798 的立案机理）；有限超时白等整段。元素本体不丢（留 src 且 dst 有副本，双份形 §100 在册容忍），危害面为唤醒悬挂与多路径非同构——五臂同款纪律在唤醒贴发面独漏经纪臂，非 §100/§33/§63/§24 与 todo 事务闩票、脚本僵尸观察者票任何在册面。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/collection_item_source.rs:list_outcome Blmove 异键分支（:317-330）
wedb/wcol/src/itembroker/collection_item_broker.rs:TryGetOutcome（:106-184）、try_assign_item_from_key result-none 分支（:646-666）、initialize_observer result-none 分支（:568-591）

对应 c# 文件与函数：
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetResult finally 块 notifyKey 无条件入队（:674-681）、TryGetNextListResult notifyKey 置位（:468）

精炼执行方案：
1 list_outcome Blmove 分支拆四段链短路：dst 复验+save 成功（元素已持久）后，src 复验或 save 失败时回 outcome 携 notify_key=dst_key（形如 with_count 另置 notify_key；result=None 与 notify_key=Some 并存专指半持久态，TryGetOutcome 头注补该形态与 is_degrade/contended 恒不携 notify_key 的定序注）。严禁在臂内直调 handle_collection_update——调用时刻主循环持 src 键队列锁，取 dst 键队列锁即跨键锁序逆环（头注锁纪律明令禁止）
2 消费两site 对称收口：try_assign_item_from_key 与 initialize_observer 在 result-none 分支 break/continue 之前，若 outcome.notify_key 为 Some 则 enqueue_event(CollectionUpdated(prefix.isolate(notify_key)))——enqueue_event 无队列锁（mpsc 推入），与两site 成功分支锁内直入队先例（:679-683、:606-610）同一机制，不新建第二通道
3 测试验证点：wnode 侧锁测——A 挂 BLPOP dst timeout=0，B 走经纪出件 BLMOVE src dst，夹具令 dst save 成功后 src 复验/写回必失败（沿 lmove_dst_migration_busy 族 try_swap_in_window 封窗注塞先例改塞 src 位）；断言半移交后 A 即被唤醒取走 dst 元素而非悬挂；revert-proof 撤步 1 或步 2 转红；既有 §100 锁测 lmove_dst_* 族、collection_item_broker_tests、list_blocking_cold_wait 全绿不回退
