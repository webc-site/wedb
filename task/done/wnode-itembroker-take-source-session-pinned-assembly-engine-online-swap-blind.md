锁定注记（2026-10-01 r8 波主控补锚，基线 f89369e；票面行号有漂，以本注记为准）：
- service.rs 现位：node_components :746（broker_session = store.new_session() :751、
  CollectionItemSource::new :753），三处调用点 :850 / :1476 / :1537（原 :847/:1470/:1531）。
- 未漂：collection_item_source.rs:74 结构体、:435 try_get_result、:446 set_context 域切换单点；
  checkpoint.rs:145 swap_online_store（清扫臂 :183-196 仅射 ConsumerType::Client）；
  server.rs 停机链 dispose_item_broker 与 primary_tasks.rs:191 bind_object_collect_env（原 :189，+2 系 e7efacb 模块头注释）。
- 红线（照甄别席原口径）：本票取件源须落「引擎槽动态解析」或「置换漏斗重建会话」两形之一；
  若实现须给 wedb/wnode/src/aof/** 或 assembly.rs 新增观测口，立即灭票回炉，不得以扩口换实现。
- 旧引擎 Arc 钉死解除须同时订正 checkpoint.rs:144 注释的失真前提（票面方案 1 末句），禁只改码不改注。
- 禁触域：wedb/wnode/src/resp/vector/vector_store_callbacks.rs、wedb/wedb/src/server/replication/**、
  wedb/wnode/src/storage/session/common/ttl_sync.rs（同侪在途）。

审核结论：通过（2026-09-30 甲轮48 审核席；P1。C# 引注已按审核席勘误改 Reset→ResetDatabase 链、补域切换单点 :446、方案 1 补 unsafe Sync 论证段扩写与旧引擎 Drop 收口）

集合项经纪取件源会话钉死装配期引擎，副本检查点在线置换后取件域永久错位旧引擎

问题分析：
1. Garnet 契约对齐：C# 原位恢复不换引擎实例，全体会话经 StoreWrapper.cs:41 的 Store 单计算属性恒见同引擎（StoreWrapper.Reset :555 委托 DatabaseManagerBase.ResetDatabase :265-271 对同一实例 db.Store.Reset() 原位重置，恢复全程不换引擎实例）。rust 引擎置换形态（实例换指）下，置换漏斗 swap_online_store 逐件重挂宿主钩子束（EngineHookSlots 全举三件：WATCH 版本推进/AOF per-op 镜像/缺席删除登记）并清扫存量客户端会话，纪律自陈「断开存量会话保锁面==写面不变量」（checkpoint.rs:164-169）与「向量钩子一次注入跨置换存活，禁钉死旧引擎映射」（storage_session.rs:1132-1133）；doc/zh/collection.md §5 要求取件判定落在当前在线引擎。
2. 工程现状：经纪取件源 CollectionItemSource 在 node_components（service.rs:743-754）装配期经 store.new_session() 一次性派生，StoreSession 持具体引擎 Arc（wkv/session/mod.rs:224 pub store: Arc<WedbStore>），此后 try_get_result 仅 set_context 换租户库（域切换单点 collection_item_source.rs:446）、引擎永不换（collection_item_source.rs:74-99）。node_components 仅冷启动/恢复装配三处调用（service.rs:847/:1470/:1531），swap_online_store（checkpoint.rs:145-197）不重跑；置换清扫仅射 ConsumerType::Client 注册表条目（:183-196），经纪主循环 compio 任务不在消费者注册表不被清扫不重建；broker 收场仅停机链 dispose_item_broker（server.rs:778）可达。同域对照组 CollectTaskEnv 有 bind_object_collect_env 置换刷新口（primary_tasks.rs:189-210），经纪取件会话恰是该枚举漏掉的绑定面（甲轮8 留观「引擎置换后集合项经纪取件会话钉装配期引擎」本轮升级为可达缺陷）。
3. 逻辑危害确证：副本 diskbased 全量同步→检查点导入 swap_online_store 后，存量客户端被清扫重连落新引擎，经纪主循环与其旧引擎会话存活不动（旧引擎被 StoreSession Arc 钉住永不 Drop）。此后新引擎 LPUSH/ZADD 写成功→事件入共享经纪队列→try_get_result 落旧引擎。臂一：旧引擎键缺失→TryGetOutcome::none→观察者挂队，BLPOP timeout=0 永久饥饿、有限超时空回。臂二：旧引擎存置换前同名旧数据→从幻影引擎弹出旧元素 ACK 客户端，弹出写仅落旧引擎，当前引擎元素仍在（后续重复出件），且旧引擎事件 sink 仍指共享 AOF 门面照常入账——AOF 出现当前状态不含的弹出道目，主从发散。臂三：分层判定 is_live_tiered_collection 同读旧引擎 meta，degrade 送客判定失真。升主（REPLICAOF NO ONE）后常驻可达直至进程重启。

涉及代码：
rust 文件与函数：
wedb/wnode/src/service.rs:743 node_components（broker_session 装配期一次性派生）
wedb/wnode/src/resp/objects/collection_item_source.rs:74 CollectionItemSource（session 字段/try_get_result 引擎域固定）
wedb/wedb/src/server/cluster_provider/checkpoint.rs:145 swap_online_store（置换漏斗：钩子束+清谱写面）
wedb/wnode/src/primary_tasks.rs:189 bind_object_collect_env（同域对照组置换刷新口）

对应 c# 文件与函数：
libs/server/StoreWrapper.cs:41 Store 计算属性与 Reset（原位恢复不换实例，无对位缺陷，置换语义对账锚）
libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetResult（取件执行体）
doc/zh/collection.md §5（StorageSession 统一检测键类型与形态——取件判定须落当前在线引擎）

精炼执行方案：
1. 取件源改持引擎槽动态解析（StoreSwapSlot 形）或将经纪纳入置换漏斗绑定面枚举：engine_swap 钩子束内同步重建 broker 会话（CollectionItemSource 会话替换或整体 dispose+重装），与 CollectTaskEnv::bind 同批同口径；改持引擎槽时 collection_item_source.rs:79-93 的 unsafe Sync 单写者论证段须同步扩写「槽内会话置换亦仅发生于经纪主循环事件段（try_get_result 入口比对槽引擎与 session.store 指针，异指即原地换代）」，防注释与本体纪律脱同步；置换后旧会话弃置即解除旧引擎 Arc 钉死，旧引擎自此可真实 Drop（顺带修正 checkpoint.rs:144 注释在本缺陷场景下的失真前提）
2. 保持经纪主循环单写者纪律不破：会话替换点选在主循环事件段内（try_get_result 入口现取当前引擎派生短会话亦为可行形，纪元进出仍在属主线程同步段配对，勿引入跨线程触达）
3. 测试：wedb/wedb/tests/engine_swap_hook_bundle.rs 补第五绑定面锁测——构造 swap 后新引擎 LPUSH+BLPOP，断言出件自新引擎生效、旧引擎无弹出写与 AOF 镜像道目

终态注记：
- 收口形态：CollectionItemSource 改持 StoreSwapSlot，并在 try_get_result 入口动态感知当前在线引擎比对指针，发生置换时原地换代重建会话并弃置旧会话，解除旧引擎 Arc 钉死；在服务装配期完成第五绑定面挂接；补齐第五绑定面跨置换出件与无残留写锁测。
- 合入哈希：6c2c97b
- 状态：已收口归档。

主控收票审计（2026-10-01 r9 波，沙箱 dev 尖 4016d62 亲跑，席上自证缺席故全量亲验）：
- 红线遵守：改动面落 wcol itembroker 两 getter、collection_item_source.rs、service.rs、checkpoint.rs 注释、
  一处锁测。未给 wedb/wnode/src/aof/** 或 assembly.rs 新增任何观测口，红线不破。
- 方案 1 两形择一落实为「引擎槽动态解析」：try_get_result 入口比对槽引擎与 session.store 指针、
  异指原地换代重建会话，unsafe Sync 论证段按票面要求同步扩写（槽内置换仅发生于主循环事件段），
  checkpoint.rs 失真前提注记一并订正——票面末句双要求齐达。
- 单写者纪律：换代点选在 set_context 之前、任何 enter_batch 之前，纪元进出仍在属主线程同步段配对，
  store_swap 读 guard 在换代块语句末即释放，不跨取件持有；未引入跨线程触达。
- 新增导出均有消费者非孤儿（item_source/current_store 由锁测射，attach_store_swap_slot 由装配点射，
  CollectionItemBroker::store 由 item_broker_face 转发）；StoreSwapSlot 泛型化为取件源持槽的必要前提，非过度设计。
- 臂二/臂三闭环亲证：is_live_tiered_collection 取 &batch 系换代后会话 enter_batch 产物，分层判定随会话落新引擎。
- 反向验证（本席核心补证，席上未申报）：沙箱内把换代判据短路为恒假后重跑，锁测转红且失败报文精确复现
  票面臂二——BLPOP 自幻影旧引擎弹出 `old_engine_val`（left 值即旧引擎元素），坐实三件事：
  ① 该锁测真走经纪取件路径而非客户端快径（否则 BLPOP 会直接命中新引擎而恒绿）；
  ② 换代臂是出件正确的充要条件；③ 票面危害论证（臂二幻影出件）在真机上可复现，非静态推演。
  短路已复原，沙箱 git status 干净。
- 门禁缺口：本票锁测由主控沙箱复跑 2/2 绿（engine_swap_hook_bundle 全册），AOF 镜像道目断言以
  「旧引擎 LLEN 恒 1 + 旧元素原样在场」传递证成（无弹出写即无弹出道目），未单开 AOF 道目级断言，
  记为可接受收口形。

