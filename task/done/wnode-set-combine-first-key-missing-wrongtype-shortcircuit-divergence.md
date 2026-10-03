终态:闭环(2026-09-29)。甄别 5a4623a,审核席裁修复型登记方向,主控直改零代码:deviations.md 新开 §184(五命令族首源缺失臂 rust 全键判型锁形,读臂回 -WRONGTYPE/STORE 面 dst 不删回 -WRONGTYPE,判据合引 §113 并归拢其 SINTERCARD 应答面,§113 加回指),C# 短路收空锚 SetOps.cs:442/:879/:381/:822 与 rust load_many 单点符号锚全钉。纯文档零测试面。
甄别结论:通过(P2 级,2026-09-29 主控席)。亲验:load_many write.rs:291 单点、deviations.md 在册(567 行)。审核席裁修复型登记方向遵照:纯文档零代码改动,登记条目覆盖五形,STORE 面锁形写死,dst 不删回 -WRONGTYPE,合引 §113。主控直改不派席。

审核结论：通过（2026-09-29 甲轮34-A，P2 级）。裁量方向：裁修复型登记（保留 rust 全键判型，落 deviations.md 登记锁形），不采 C# 短路收口——rust 现行即 Redis 上游语义；C# 短路面具破坏性（STORE 判空臂 EXPIRE(dst,TimeSpan.Zero) 静默删目标键）；§114/§150/§151 先例充分，采短路反需形态参数违单机制。执行席遵照：登记条目显式覆盖五形（SINTER/SDIFF/SINTERCARD+SINTERSTORE/SDIFFSTORE），STORE 面锁形写死「dst 不删、回 -WRONGTYPE」，合引 §113 防台账碎片化；文档面修复零代码改动。

原票面：
SINTER/SDIFF/STORE 族首源缺失臂 C# 短路收空语义与 rust 全键判型分叉，STORE 面附带目标键存活处置分叉

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# libs/server/Storage/Session/ObjectStore/SetOps.cs 私有 SetIntersect（442 行）与私有 SetDiff（879 行）在 GET keys[0] 返回 NOTFOUND 时立即 return GarnetStatus.OK 且结果为空集，对后续键不再 GET、不再判型——上游键数组中排在缺失键之后的 WRONGTYPE 键永远不被探及。命令层 SetCommands.cs:SetIntersect（66 行）据此回空集合帧，SetDiff 据此回空集合帧，SetOps.cs:SetIntersectLength（938 行，SINTERCARD）据此回 :0；SetIntersectStore（381 行）/SetDiffStore（822 行）据此走 members.Count == 0 臂 EXPIRE(key, TimeSpan.Zero) 删除目标键并回 :0。仅 SUNION/SetUnion（612 行）无此短路：逐键判型，任一键 WRONGTYPE 即上抛。即 SINTER missing strkey、SDIFF missing strkey、SINTERSTORE dst missing strkey、SDIFFSTORE dst missing strkey 四形在 C# 均为「OK 收空」应答（STORE 形附带删 dst），而非 -WRONGTYPE。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧四命令共用 load_many 单点装载：读臂 SINTER/SDIFF（wedb/wnode/src/resp/objects/set_commands/read.rs set_combine）、SINTERCARD（read.rs set_intersect_length）、STORE 臂（write.rs set_combine_store）、慢路径（slow.rs load_many_async 与 Sinter/Sdiff/Sintercard/Sinterstore/Sdiffstore 各臂）全部经 load_many/write.rs load_many_async 逐键 set_load_sync 装载，任一键 WrongType 即整体 Err 上抛 WRONGTYPE 错误帧，不存在「首源缺失即收空短路、后续键免判型」分支。同输入下 SINTER missing strkey：C# 回空集、rust 回 -WRONGTYPE；SINTERCARD 同形：C# 回 :0、rust 回 -WRONGTYPE；SINTERSTORE/SDIFFSTORE 同形：C# 判空收臂删除目标键回 :0、rust 回 -WRONGTYPE 且目标键原样保留。SUNION/SUNIONSTORE 两侧一致（全键判型），不在本票面。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
应答契约分叉：同输入同态回帧不同（空集帧 vs 错误帧、:0 vs 错误帧），违背「数据结构响应逐字节全等」契约维度；STORE 面叠加状态分叉：C# 删目标键、rust 保目标键，主从/迁移拓扑若两端实现异构将状态发散。无崩溃面无数据损坏，属真实契约分叉。查重：deviations.md §150（判死异构键折叠 NotFound）只覆盖 string 影子/RI 到期判死域，活 string 键判型臂不在其内；§113（SINTERCARD 应答面分叉）判据不可回收且仅挂 SINTERCARD 名，SINTER/SDIFF 与两 STORE 面不在其名下；五池（done/reject/issue/todo/ing）无同面票。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/set_commands/write.rs:load_many
wedb/wnode/src/resp/objects/set_commands/write.rs:set_combine_store
wedb/wnode/src/resp/objects/set_commands/read.rs:set_combine
wedb/wnode/src/resp/objects/set_commands/read.rs:set_intersect_length
wedb/wnode/src/resp/objects/set_commands/slow.rs:load_many_async
wedb/wnode/src/resp/objects/set_commands/slow.rs:set（Sinter/Sdiff/Sintercard/Sinterstore/Sdiffstore 各臂）

对应 c# 文件与函数：
garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersect（私有，442 行起）
garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:SetDiff（私有，879 行起）
garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersectStore（381 行起）
garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:SetDiffStore（822 行起）
garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersectLength（938 行起）
garnet/libs/server/Resp/Objects/SetCommands.cs:SetIntersect（66 行起）

精炼执行方案：
1. 先裁定归齐方向：按 C# 短路语义收口（首源缺失即收空：读臂回空集/回 0，STORE 臂走判空收臂删 dst 回 :0，后续键不再判型），或裁修复型分叉（rust 现行全键判型更贴 Redis 上游语义、对 WRONGTYPE 源不静默删键）并落 deviations.md 登记锁形——两向择一，禁双态各表。
2. 若采 C# 短路：load_many 增加「首缺短路」形态参数（intersect/diff 臂 GET 首键 Missing 即回空、免装载余键；union 臂维持全键判型），load_many_async 同点同改，保持单机制单装载漏斗，严禁另起第二套装载判据。
3. 测试验证点：SINTER/SDIFF/SINTERCARD 的 missing+wrongtype 快慢双路径应答帧断言；SINTERSTORE/SDIFFSTORE 同场景目标键存活态与 :0/错误帧断言；SUNION/SUNIONSTORE 全键判型回归不回摆。