裁决：不成立（与 C# 拓扑相悖：ItemBroker 就在 Objects/ 下，rust wcol 是其对位；C# 同样在库内用异步设施）
来源：next/agy.design.md 条 22。核销 2026-09-19。

一句话结论：C# 的 CollectionItemBroker 位于 garnet/libs/server/Objects/ItemBroker/（server 的
Objects 子目录，rust wcol 的对位层），且其内部同样使用 Task.Run 与 ConcurrentQueue；
rust 把 itembroker 放 wcol/src/itembroker/（目录结构 1:1 同构，含 event/observer 配套文件）
并使用 compio/crossfire 是 SKILL.md:57-58 钦定运行时的忠实转写，移到 wnode 反而偏离 C#。

逐条核销
1. C# 拓扑实测：garnet/libs/server/Objects/ItemBroker/ 下 CollectionItemBroker.cs、
   CollectionItemBrokerEvent.cs、CollectionItemObserver.cs、CollectionItemResult.cs 四件；
   CollectionItemBroker.cs:123 mainLoopTask = Task.Run(StartAsync)、:38/:179/:259/:287
   ConcurrentQueue——C# 在 server 的 Objects 层内持有异步运行时设施。
2. rust 对位实测：wedb/wcol/src/itembroker/ 下 collection_item_broker.rs、
   collection_item_broker_event.rs、collection_item_observer.rs、item_broker_face.rs、mod.rs，
   文件组织与 C# 同构；:23 use compio::runtime::spawn、:24 crossfire 对标 C# Task.Run /
   ConcurrentQueue（文件内注释 :12「C# 的 Task.Run 以 compio 宿主注入的 TaskSpawner 等价实现」
   :177「crossfire mpsc::List 为其零锁等价物」）。
3. 「wcol 是底层纯对象层」前提不成立：wcol 对标 C# Garnet.server 的 Objects/ 域（非独立底层库），
   该域在 C# 中本就含 ItemBroker 这类带异步设施的组件；「剥离异步调度」无 C# 对标依据。
4. 消费面（wnode/src/service.rs、wnode/src/resp/objects/collection_item_source.rs）跨 crate
   引用是正常服务装配，与 C# Resp 会话引用 ItemBroker 同构。
