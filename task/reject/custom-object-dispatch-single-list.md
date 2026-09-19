custom-object-dispatch-single-list 部分拒录：验收首条「wext_* 直呼点全仓归零」不含堆内存估算面

来源：/Users/z/git/db/wedb/task/ing/custom-object-dispatch-single-list.md 验收第 1 条
（原文来自 next/custom-object-dispatch-single-list.md，观点为 AI 生成）。本条经对照 HEAD
代码与 garnet C# 后按事实收窄，票内其余各条（三轨收敛、扩展内双表归一、objects/mod.rs
注释）成立并已照做，故本票整体不拒，仅拒此一条的字面验收口径。


被拒原文（逐字转录）
- grep 全仓 `wext_roaring::` / `wext_json::` 直呼点除扩展 crate 自身与清单定义处
 （custom_objects.rs:19-25）外归零。


拒绝原因

1. 与该清单自身的既有裁定冲突。/Users/z/git/db/wedb/wedb/wnode/src/resp/custom_objects.rs
   模块头（收敛后 :15-17）明文「装箱能力面（堆内存估算）不入清单：C# 侧仅部分扩展对象维护
   `IHeapObject.HeapMemorySize`，各扩展 crate 的对象级记账入口自持，MEMORY USAGE 消费点
   按现状直呼，不另立第二口径」；/Users/z/git/db/wedb/wedb/wcustom/src/object_desc.rs:36-40
   对 `CustomObjectEntry` 同口径复述（「堆内存估算不入描述」）。即「清单外的直呼」是显式
   设计裁定，不是本票要拆的第三轨。

2. C# 事实支持该裁定：RoaringBitmap 记账在
   /Users/z/git/db/wedb/garnet/modules/RoaringBitmap/RoaringBitmapObject.cs:33、:41、:80
   维护 HeapMemorySize，而 /Users/z/git/db/wedb/garnet/modules/GarnetJSON/GarnetJsonObject.cs:350
   的 remarks 明确「This currently does not update IHeapObject.HeapMemorySize」；
   消费侧 /Users/z/git/db/wedb/garnet/libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:117
   取 `srcLogRecord.ValueObject.HeapMemorySize` 走 IHeapObject 多态，Garnet.server 工程
   不引用 modules（C# 侧根本不存在「集中清单承接记账」这一层）。rust 对象以
   `[1B 标签][载荷]` 信封落库，标签分派必须由持有扩展 crate 编译期接线的 server 层承担，
   故 /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs:553-565
   `envelope_heap_estimate` 的 roaring 臂直呼属设计内例外，与命令分发无关。

3. 把它并入清单需给 `wcustom::CustomObjectEntry` 新增堆估算函数字段，与本票修法第 1 条
   「清单项已具备承接全部三轨所需信息，无需新增字段、无需第二张表」的收敛前提相悖；且
   JSON 侧无该能力（C# 同款 TODO），加字段即为对侧造空实现臂，属扩范围而非收敛，
   亦违反「不实现虚设面」的仓库裁定。

4. 越界面：本票只管分发/校验面的名单轨。`wext_roaring::heap_estimate` 与
   `RoaringCommand::OBJECT_TAG`（记账臂的标签比对）留在原位，不并入清单、不改语义。


收窄后的验收（本票实际执行口径）
分发与校验面（快路径解析、慢路径重放、ACL 按名门、清单项标签取用）的 `wext_roaring::` /
`wext_json::` 直呼点，除扩展 crate 自身与清单定义处（custom_objects.rs 的
`CUSTOM_OBJECT_ENTRIES`）外归零；`object_store_utils.rs:553-565` 的堆内存估算臂按既有裁定
保留直呼，为唯一例外（既有例外，非本票新增）。
