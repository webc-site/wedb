# 阻塞族 CollectionItemBroker 跨租户命名空间隔离

来源：next/zcode-r7-redteam.md 问题 P0-1

裁决：成立，接受。 wedb 多租户维度是转写新增，但 C# 取件域锚定观察者会话的机制在转写中被换成经纪单例会话后丢失，构成转写引入的结构性旁路，票面 P0 定性准确。

## 双侧证据

rust 侧（问题真实存在）：

1. 观察者注册用裸用户键：
   - wedb/wnode/src/resp/objects/list_commands/blocking.rs:100-104（BLPOP/BRPOP）、:192-193（BLMOVE）、:230-237（BRPOPLPUSH）、:286-287（BLMPOP）
   - wedb/wnode/src/resp/objects/sorted_set_commands/blocking.rs:205-211（BZPOPMIN/BZPOPMAX）、:280-286（BZMPOP）
   - wedb/wnode/src/resp/resp_server_session/pump.rs:71-89 park_broker_wait 原样转交 start_wait
2. 经纪取件会话为进程单例，默认 ns0/db0（wedb/wkv/src/session/mod.rs:184-185 原子量初值 0），装配后全程无 set_context：wedb/wnode/src/service.rs:802-805
3. 取件源直用裸键装载，物理键 = 会话前缀 + 裸键（wedb/wkv/src/session/raw/read.rs:917-924 session_tag_key），故经纪取件恒落在 ns0/db0 物理域：wedb/wnode/src/resp/objects/collection_item_source.rs:74、:160、:221
4. 观察表为进程级单 map，键为裸键：wedb/wcol/src/itembroker/collection_item_broker.rs:41、:187
5. 唤醒通知同样裸键：wedb/wnode/src/resp/resp_server_session/pump.rs:24-28、wedb/wnode/src/resp/garnet_api/mod.rs:405-411（调用点 list_commands/write.rs、sorted_set_commands/write.rs、garnet_api/slow.rs:832、:884）

后果链：ns5 会话 BLPOP q → 观察者挂裸键 q → 经纪在 ns0/db0 物理域试取并弹出 → ns0 同名键数据被越权消费并回给 ns5 客户端；反向任意租户 LPUSH 唤醒他租户观察者；不同租户同名键观察者共用同一 FIFO 队列互相串扰。

c# 侧（机制对照）：

1. 经纪随 StoreWrapper 单例：garnet/libs/server/StoreWrapper.cs:160、:246
2. keysToObservers 裸键 map：garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:38 —— C# 无 namespace 维度，裸键在单租户内无歧义，票面所诉「C# 用 ns 隔离」不存在，但 C# 有自己的域锚定：
3. 取件经观察者自身会话执行域：garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:269、:337 `TryGetResult(key, observer.Session.storageSession, ...)` —— 弹出/搬移走发起阻塞命令那个会话的 storageSession，域（库上下文）天然跟随观察者；这是 C# 的隔离机制，rust 转写以经纪单例会话替换该执行域时丢失了域绑定（collection_item_source.rs 模块文档自述的刻意差异），叠加 wedb 新增多租户维度即成旁路

## 方案

单一机制：观察者域锚定。注册键折叠 + 唤醒键折叠 + 取件域切换，三面同源于观察者所属 (ns, db)，对齐 C# 「取件随观察者会话」语义，键折叠复用 pubsub ChannelNsPrefix 既有先例（wedb/wpubsub/src/channel_ns.rs，模块文档自述 wedb 自有架构 C# 无对位）。

1. 前缀编解码收敛 wbase 单源：wpubsub/src/channel_ns.rs 的 ChannelNsPrefix 移入 wbase 新模块 ns_prefix，更名 NsPrefix，wpubsub 原路径 `pub use` 转发保持调用面不变；新增 `join(n: u64)` 段扩展（缓冲 21B→42B），wpubsub 调用点零改动。隔离键形态 `[ns 十进制 ':'][db 十进制 ':'][裸键]`；十进制 + 单定界符规范无歧义（无前导零），跨 (ns, db) 折叠键互不前导碰撞
2. wcol 观察者域锚定：CollectionItemObserver 增 prefix: NsPrefix 与 domain: (u64, u64)；start_wait 增 domain 入参，折叠在 wcol 经纪内单点完成；handle_collection_update 增 domain 入参同径折叠；initialize_observer / try_assign_item_from_key 取件前经 observer.prefix.strip 还原裸键，BLMOVE 目标唤醒键 outcome.notify_key（裸键，cmd_args 未折叠）经 observer.prefix.isolate 折叠后入队。折叠/剥离全部收敛在 wcol itembroker 一个模块
3. 取件域切换：CollectionItemStore::try_get_result 增 (ns, db) 首参；CollectionItemSource 入口 `session.set_context(ns, db)` 后走现行装载路径（wedb/wkv/src/session/mod.rs:387 set_context 非严格会话纯内存物化）。经纪主循环为单消费者（initialize_observer 与 try_assign_item_from_key 均在 handle_broker_event 串行臂内），单例会话串行换域无竞态，复杂度对标 C# 每观察者自带会话
4. wnode 接线：pump.rs park_broker_wait 传 (self.namespace, self.active_db_id)；pump.rs:24 notify_collection_update 传同域；garnet_api/mod.rs:406 传 (self.session.namespace(), self.session.active_db())；service.rs:1789 CollectionNotify 闭包签名随动
5. 边界确认：
   - 经纪会话非严格态，观察者存活期间其会话 bind_route 钉住租户快照，set_context 无冷租户盲分配面
   - ns0/db0 同样折叠（"0:0:" 前缀），进程单表键全域唯一
   - 应答键名取剥离后裸键，客户端视角不变
   - CLIENT UNBLOCK 走 session_id 映射，与域无关，不动
6. 测试：
   - wbase：NsPrefix 单元测试（含 join 两段折叠/剥离对称、跨域不前导碰撞）
   - wcol tests/collection_item_broker_tests.rs：既有用例随签名更新；新增双域同名键互不指派用例
   - wnode tests 新增 resp_blocking_ns_isolation.rs 集成测试：StorageSessionProvider + ACL 建 1#bob / 2#charlie（样板 acl_namespace_admin_tests.rs:56-103），消费者注入共享经纪，A(ns1) BLPOP 挂起 → B(ns2) LPUSH 同名键不得唤醒 A 且数据留 B 域 → A 域推入才唤醒；同 ns 跨 db（SELECT）同名队列互不串扰
7. 文档注释补 c# 映射（在 garnet 中的相对路径:函数名），经纪刻意差异注释随实现更新
