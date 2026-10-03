r7 裁撤波遗留 15 个方法面 check.js 实现缺失未收口（三族应改挂存活锚、四族应走 ignore 登记，缺一次逐消费点对位裁决）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
本仓以 js/check.js 做 C# 与 rust 的对账门禁：每个被裁认可达的 C# 方法面，必须在 rust 侧留一处
公开承锚（文档面 `路径.cs:方法名`），或以 js/check/ignore/<域>.yml 逐方法登记「刻意不镜像」并附对位理由
（先例：task/done/checkjs-miss-respreadutils-elementdata-anchor-closure.md，RespReadUtils 两方法经
逐消费点对位属 C# 自有重复码，转 ignore 不实现；ElementData.ConvertU8ToF32 属 rust 单实现已承接、
仅锚形制不合，改写注释收口）。二者皆无即落 `# 实现缺失`，并在 js/check/miss/ 生成产物。

2. 工程现状确证（Rust 现有实现路径与代码现状）
r7 重构波（1ae6dd1 r7-C、7cb63cf r7-D、c184916 r7-G、ae7ffcd r7-B）删除死面时把承载锚一并带走，
现树 `bun js/check.js` 恒剩 7 类 15 方法面缺失，js/check/miss/ 产物逐条对位：
a wedb/wconn 客户端便捷封装族（r7-C 把 api.rs 缩至生产存活面，删
   GarnetClientBasicRespCommands.cs 的 QuitAsync/PingAsync/StringGetAsync/StringSetAsync/
   KeyDeleteAsync/StringIncrement/StringDecrement、GarnetClientListCommands.cs 的
   ListLeftPushAsync/ListRightPushAsync/ListRangeAsync/ListLengthAsync、
   GarnetClientSortedSetCommands.cs 的 SortedSetAddAsync/SortedSetRemove/SortedSetRemoveAsync/
   SortedSetLengthAsync、GarnetClientAdminCommands.cs 的 Save）——
   存活单机制与裁决理由已在册：wedb/wconn/src/api.rs:1-8 册头自陈「22 便捷封装 + 
   SortedSetPairCollection 本仓零生产消费、唯一调用方是本 crate 自测，语义由
   GarnetClient::execute_for_string_result_async 底层执行口直接表达」，存留仅
   info / replica_of 两方法（api.rs:22、:35，各自带一手锚），
   且同四文件在 js/check/ignore/client.yml :62/:70/:73/:207 已有逐方法 ignore 先例条目，属同表同口径残余。
b RecordInfo.cs:TryResetModifiedAtomic（r7-D 裁撤 RecordMut 委托族时删除）——
   rust 存活对应面为 wedb/wrecord/src/header.rs:181 set_modified 与
   wedb/wrecord/src/record_mut.rs:353 set_modified（MODIFIED 位经 info_set_bit 原子 RMW），
   属「实现存活、锚随委托层灭失」形，应改挂存活公开位而非 ignore。
c ArrayKeyIterationFunctions.cs:DeleteIfExpiredInMemory（r7-G 删无前缀重复版）——
   本类在 wedb/wkv/src/gc/ttl_sweep.rs:47-55 与 wedb/wkv/src/store/keyspace.rs:554 仍有多方法在册，
   唯此一名断锚；存活过期删链为 ttl_sweep.rs:66 collect_expired 与 :133 sweep_expired，
   须先判「无前缀版删除后是否仍有独立语义」再分流（有则改挂，无则 ignore 附对位）。
d IHeapObject.cs:DoSerialize（r7-B 删 wcol trait 死臂）——
   rust 无同名/同义实现，对象序列化经 wcol/wval 编译期定长与分派链承接
   （见 wedb/wnode/src/resp/objects/object_store_utils.rs:991-1000 叙述与 wcol/src/object_payload.rs:175），
   DoSerialize 属 C# 多态序列钩子、rust 形制不镜像，应走 ignore。
现存门禁噪声：`# 仅词元提及` 同时挂 GarnetClientAdminCommands.cs:Save、
GarnetClientSortedSetCommands.cs:SortedSetRemove、IHeapObject.cs:DoSerialize、
RecordInfo.cs:TryResetModifiedAtomic 四条，与本缺失族同源。

3. 逻辑危害确证（实际危害）
非运行时缺陷，属对账基座失真：缺失名单长期挂红后，后续每一轮审查与每一张裁撤票都无法用
`# 实现缺失` 段判别「新删真功能」与「历史已裁决不镜像」，对账门禁退化为噪声源；
同时 r7 波自身的收票复验（task/fix.md 要求并后即跑 check.js）失去差分基准，
下一次真漏锚会被这 15 条存量淹没。

涉及代码：
rust 文件与函数：
wedb/wconn/src/api.rs（r7c 裁决后库门面册头自陈 + 存留 info / replica_of）
wedb/wconn/src/client.rs:execute_for_string_result_async / execute_for_bytes_result_async /
execute_for_string_array_result_async（底层执行口单机制）
wedb/wrecord/src/header.rs:set_modified、wedb/wrecord/src/record_mut.rs:set_modified
wedb/wkv/src/gc/ttl_sweep.rs:collect_expired / sweep_expired（本类在册锚同文件）
wedb/wcol/src/object_payload.rs、wedb/wnode/src/resp/objects/object_store_utils.rs（堆对象序列化承接面）
js/check/ignore/client.yml、js/check/ignore/storage.yml、js/check/ignore/common.yml（登记载体）

对应 c# 文件与函数：
garnet/libs/client/GarnetClientAPI/GarnetClientBasicRespCommands.cs:QuitAsync/PingAsync/StringGetAsync/StringSetAsync/KeyDeleteAsync/StringIncrement/StringDecrement
garnet/libs/client/GarnetClientAPI/GarnetClientListCommands.cs:ListLeftPushAsync/ListRightPushAsync/ListRangeAsync/ListLengthAsync
garnet/libs/client/GarnetClientAPI/GarnetClientSortedSetCommands.cs:SortedSetAddAsync/SortedSetRemove/SortedSetRemoveAsync/SortedSetLengthAsync
garnet/libs/client/GarnetClientAPI/GarnetClientAdminCommands.cs:Save
garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:TryResetModifiedAtomic
garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DeleteIfExpiredInMemory
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/IHeapObject.cs:DoSerialize

精炼执行方案：
1. 逐方法双面定档（禁止批量塞 ignore）：每个缺失名先答「rust 是否有存活等价实现」，
   有则把 C# 一手锚改挂到该存活公开位（注释形态，零行为改动），无则入对应域 ignore 登记，
   条目必须附逐消费点对位与一句裁决理由（照 checkjs-miss-respreadutils 先例的写法与粒度）。
2. b 面（TryResetModifiedAtomic）与 c 面（DeleteIfExpiredInMemory）先做真伪复核：
   若存活面语义确有分叉（C# 为 CAS 条件复位、rust 为无条件位写；或前缀版与无前缀版行为不同），
   须另立功能票而非在本票内改码，本票只登记差集与判据。
3. 测试验证点：改后主树跑 `bun js/check.js`，断言 `# 实现缺失` 段与 `# 仅词元提及` 中本族 15+4 条归零、
   js/check/miss/ 目录清空；`# 重复定义` 段不得因此新增（同锚多点即回红，须单点承锚）。
4. 纪律：纯注释与登记票，禁改任何函数体；禁触在途席域（tls 席的 wedb/Cargo.toml、wnode/Cargo.toml、
   wedb/wnode_tls_test/** 与本票零交叠，但 wconn 与 wnode/tests 若被并发 r7 后续席改写须先让路）；
   严禁为凑绿把真缺失塞 ignore，也严禁用 #[allow] 或改判定脚本绕过。

## 主控终态注记（2026-09-28 归档，主控亲办登记批）

四面收口对位：
- a 面（12 条 client 便捷封装：GarnetClientBasicRespCommands / ListCommands / SortedSetCommands / AdminCommands）：走 ignore 登记，落 js/check/ignore/client.yml（新增 GarnetClientAdminCommands.cs:[Save]，BasicResp/List/SortedSet 三组补名），理由句沿 r7c 裁决清退的零生产消费面口径、语义由 wedb/wconn/src/client.rs:execute_for_string_result_async 底层执行口单点承接。随 735ad44 与本票 a3d27cc 侧一并入库。
- c 面（ArrayKeyIterationFunctions.cs:DeleteIfExpiredInMemory）：走改挂存活锚，落 wedb/wkv/src/ttl.rs check_expired 文档注释（与 UnifiedStoreOps.cs:DELIFEXPIM 逐名两行，禁合写锚），说明 rust 把 C# Reader→DeleteIfExpiredInMemory→DELIFEXPIM 两级折为会话级判定单点。随 92a8261 入库。
- d 面（IHeapObject.cs:DoSerialize）：走改挂存活锚，落 wedb/wcol/src/types/garnet_object.rs 册头（接口级序列化契约在 rust 无 trait 方法承接，由四对象 inherent serialize_to_vec 单点各持本类型锚，SetObject.cs/ListObject.cs/HashObject.cs/SortedSetObject.cs 四锚已在位）。随 92a8261 入库。
- b 面（RecordInfo.cs:TryResetModifiedAtomic）：本票原预断「属实现存活、应改挂存活公开位」**作废**——独立查证席与审核席定性为死标记（全仓零生产读方、WATCH 不变量由 wkv bump_watch_version 无条件推进单源承接），裁决与源码清退归 task/todo/wrecord-modified-bit-dead-marker-retire.md 先落，其 js/check 收口面须同时覆盖 SetModified（RecordInfo.cs:234，承锚在 header.rs:179，删访问器即灭锚）。本票不再认领 b 面，避免双轨。

门禁实测（主控主树跑，bun js/check.js）：`# 实现缺失` 由 15 项降至 1 项（仅剩 RecordInfo，依上条归 wrecord 票）；`# 仅词元提及` 由 4 项降至 1 项（TryResetModifiedAtomic，同归）；`# 重复定义` 段零出现（承锚单点化未回红）。js/check/miss/ 目录残留 RecordInfo.yml 一条，随 wrecord 票收口清空。
