甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P4
核验记录（现码复跑，非票面背书）：
1 失真句现读在位：doc/zh/db.md:24（1.1 物理键结构段）仍写「子标签由 `wnode` 侧强类型枚举 `resp::vector::vector_registry_recovery::VectorRegistrySubTag` 单点定义与编解码」。
2 定义单点现码亲验：grep "enum VectorRegistrySubTag" wedb/ 全仓仅 wval/src/tag.rs:146 一处；wval/src/lib.rs:23 导出；wnode 侧 vector_registry_recovery.rs 模块头（:19-20）自述「定义于 wval tag.rs…共用同一真值源」——文档与现码相反属实，缺陷未灭失。
3 C# 锚抽验：db.md 同句所引 VectorManager.RecordType/MetadataNamespace 系语义对位说明，本票不触该机制，无对账分叉。
4 查重：deviations.md 零 VectorRegistrySubTag 命中；task/{done,ing,issue,reject} 无同轴票。
5 格式与可执行度：单句订正最小改动、验证以 grep 归零闭环、纯文档不触 .rs；双侧路径齐全。定级 P4：文档卫生。

审核结论：通过（r23-review-misc，2026-09-26）

亲验摘要：
- doc/zh/db.md:24（1.1 物理键结构段）失真句在位：「子标签由 wnode 侧强类型枚举
  resp::vector::vector_registry_recovery::VectorRegistrySubTag 单点定义与编解码」。
- 现码定义单点：wedb/wval/src/tag.rs:146 起 pub enum VectorRegistrySubTag
  （Index=0x01/Metadata=0x02），经 wval/src/lib.rs pub use 导出。
- wnode/src/resp/vector/vector_registry_recovery.rs 模块头（:19-20）自述
  「VectorRegistrySubTag 定义于 wval tag.rs，与本文件写透/回建编码和 wkv 紧缩
  豁免臂共用同一真值源」——码内两处自述一致，仅 db.md 落后。
- 真实分工：枚举定义在 wval 基座（单一真源），编码单点在 wnode；恰合
  review.md 板块 1「概念抽象单一真源：统一在基座定义」。
- 查重：deviations.md 零 VectorRegistrySubTag 命中；task/todo、task/reject
  无同类票；r18-wprefix:18 在案观测佐证长期滞后。

整理执行方案（供 task/fix.md 直接消费）：
1. doc/zh/db.md:24 该句订正为「子标签由基座 wval::tag::VectorRegistrySubTag
   单点定义（wnode resp::vector::vector_registry_recovery 消费其编解码做
   写透与回建）」，Index/Metadata 两值与既有布局描述不动。
2. 顺带核对该段其余锚（VectorManager.RecordType / MetadataNamespace）保持
   原样，无失真。
3. 验证：grep "wnode 侧强类型枚举" doc/zh/db.md 零命中；grep -rn
   "enum VectorRegistrySubTag" wedb/ 仅 wval/src/tag.rs 一处定义、wnode 侧
   仅消费；纯文档改动不触 .rs。

db.md 1.1 宣称 VectorRegistrySubTag 由 wnode 侧枚举单点定义，现码已下沉 wval/src/tag.rs，台账归属点失真

问题分析：
1. Garnet 契约对齐：C# 侧向量登记记录类型对位为 VectorManager.RecordType 索引记录与 MetadataNamespace 元数据记录（驻 Tsavorite 主存随检查点持久）；rust 侧以 KeyTag::VectorRegistry (0x0F) 旁路标签 + 1B 子标签承载同一语义，属既定改良（单物理存储写透持久），本票不触该机制本身。
2. 工程现状确证：doc/zh/db.md:24（1.1 物理键结构段）写「子标签由 wnode 侧强类型枚举 resp::vector::vector_registry_recovery::VectorRegistrySubTag 单点定义与编解码」。现码定义点在 wedb/wval/src/tag.rs:146（pub enum VectorRegistrySubTag，Index=0x01/Metadata=0x02），经 wedb/wval/src/lib.rs:23 导出；wnode 侧 wedb/wnode/src/resp/vector/vector_registry_recovery.rs:19 模块头自述「VectorRegistrySubTag 定义于 wval tag.rs，与本文件写透/回建编码和…」——码内文档与 db.md 相反。r18-wprefix 档 :18 早有在案观测「wval/src/tag.rs 全文：…VectorRegistrySubTag 强类型子标签」，db.md 该句自彼时起即落后于代码。
3. 逻辑危害确证：治理面危害——后续审查席按 db.md 指引去 wnode 找「单点定义」将扑空，轻则误判 wnode 枚举缺失，重则误判存在 wval/wnode 两套定义而按「双机制」立案（实际单一真源在 wval 基座，wnode 仅消费，恰好符合「概念抽象单一真源：统一在基座定义」纪律，文档却把定义点写成上层）；与 review.md 板块 1「概念抽象单一真源：全局常量、枚举统一在基座定义」的台账示范作用相悖。无运行期危害。

涉及代码：
rust 文件与函数：
wedb/wval/src/tag.rs:VectorRegistrySubTag（定义单点，:146 起）
wedb/wval/src/lib.rs（:23 导出）
wedb/wnode/src/resp/vector/vector_registry_recovery.rs（消费面，模块头 :19 自述定义在 wval）

文档锚：
doc/zh/db.md:24（§1.1「子标签由 wnode 侧强类型枚举 … 单点定义与编解码」失真句）
task/review_history/zcode-r18-wprefix.md:18（在案观测，佐证长期滞后）

对应 c# 文件与函数：
garnet/libs/server/Resp/Vector/VectorManager.cs:RecordType（登记记录类型语义对位，db.md 同句已引）

精炼执行方案：
1. doc/zh/db.md:24 该句订正为「子标签由基座 wval::tag::VectorRegistrySubTag 单点定义（wnode resp::vector::vector_registry_recovery 消费其编解码做写透与回建）」，Index/Metadata 两值与既有布局描述不动。
2. 顺带核对该段其余锚（VectorManager.RecordType/MetadataNamespace 引用保持原样，无失真）。
3. 测试验证点：grep "wnode 侧强类型枚举" doc/zh/db.md 零命中；grep "VectorRegistrySubTag" wedb/wval/src/tag.rs 命中定义、wedb/wnode 侧仅 import/消费无 enum 定义；纯文档改动不触 .rs。

合入哈希：8511a74 收口形态：db.md:24 子标签定义点单源订正归 wval::tag，纯文档零触码，独立引用 grep 归零
