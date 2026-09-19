裁决：不成立（garnet_api 三切面即建议的组织本身；slow.rs/ns_codec.rs 体量在 C# 单文件先例量级内，职责单一不拆）
来源：next/muse.design.md 条 20（garnet_api 四文件边界）+ 条 18 余量（garnet_api/slow.rs、
wval/ns_codec.rs 两文件；条 18 其余构成 resp_server_session / set_commands / vdb / tiered
四文件已分别立票或归并，见 next/design-resp-server-session-file-split.md、
next/design-set-hash-commands-dir-split.md、next/design-vdb-file-split.md、
next/tiered-collection-ops-file-split.md）。核销 2026-09-19。

条 20（按 raw/slow/objects 三执行域各自收敛 trait）核销
- 现状即三执行域切分：wedb/wnode/src/resp/garnet_api/mod.rs:15-17 mod objects / raw / slow
  三子模块在场；mod.rs（575 行）承载 GarnetApiFace trait（:72 起，exec / exec_slow /
  set_context / 快照等约 26 个 fn 位）+ StoreGarnetApi 装配 Builder + 两投影函数。
- muse 自述「快慢与对象三切面正交」——切面正确即组织正确；其修法「按 raw slow objects 三
  执行域各自收敛 trait」与现状同构，唯一差异是把大 trait 拆散进三域，属无 C# 对标依据的
  品味重组（C# 对位是 IGarnetApi 单接口 + RespServerSession 部分类实现，rust 的单一
  GarnetApiFace 挂 IGarnetApi.cs:IGarnetApi 锚点反而更贴 C#）。
- raw/slow 切面本身源自 SKILL.md:28-31 集合分层存储自定义架构（内存快路径 / 磁盘慢路径），
  维持现状即可，重组违反 SKILL.md:10（不实现自己的优化）。

条 18 余量（slow.rs 1101 行、ns_codec.rs 779 行）核销
- garnet_api/slow.rs 1101 行：单执行域文件（慢路径闭环），C# 单文件先例
  garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs 1798 行、SetOps.cs 977 行
  同量级；且与 raw.rs（540）/objects.rs（271）按域分治已符合「一域一文件」。
- wval/ns_codec.rs 779 行：单一职责的物理键编解码器（会话前缀 varint 编解码 + 保序比较 +
  栈上键缓冲），56 个 fn 中仅 13 个 pub，模块头 :3-6 有明确的手写布局论证（字节稳定、前缀
  封闭、保序，bitcode 不适用）；职责无多域混杂，拆分破坏内聚。其体量与 C# 对标
  （skiplist/序列化类单文件）相当。
