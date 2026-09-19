check.js A 层硬门红：分层写臂注释把 C# 锚点写成不存在的 HashObjectImpl.cs:Set

来源：qcode 第 10 轮主代理实测（bun js/check.js 于纯 dev 快照 exit=1，
「虚构锚点（符号存在性断言失败，libs 族 A 层）」唯一命中）。取证基线 dev c681c025 之后。

现状
- wedb/wnode/src/resp/objects/tiered_collection_ops.rs:91 文档注释锚点
  `libs/server/Objects/Hash/HashObjectImpl.cs:Set`。
- 该 C# 文件不存在名为 Set 的成员：garnet/libs/server/Objects/Hash/HashObjectImpl.cs
  哈希写入实现是 :185 `private void HashSet(ref ObjectInput input, ref ObjectOutput output)`，
  类内其余为 HashGet/HashGetAll/HashDelete/HashLength/HashCollect 等（:26-:503）。
  check.js 符号存在性断言据此判虚构锚点，A 层 symbol_fail → 整门 exit 1。
- 语义本身成立（注释说的是「C# 内存对象插入是无失败纯字典写」），只是锚点写错，
  属登记面缺陷，不是实现缺陷。

修法
- 把 :91 的 `HashObjectImpl.cs:Set` 改为 `HashObjectImpl.cs:HashSet`，其余文字不动。
- 改完 `bun js/check.js` 须 exit 0 且「虚构锚点」一节为空。
- 禁止用删注释、改 ignore 语料的方式消红。

验收
- bun js/check.js exit 0。
- 无源码逻辑改动（纯注释 diff，不跑全量 test.sh，跑 cargo check -p wnode 即可）。

拒绝原因（fixloop 核实 2026-09-19）
- 重复 + 已失效：所述 :91 挂 HashObjectImpl.cs:Set 在当前 dev HEAD（0cd7d5d5）已不存在。
- 修复由 dev 已含提交 3d6f4fea「refactor(anchor): CS 锚点重复挂载收敛为单套机制」完成：
  该行现写「HashObjectImpl.cs 内 HashSet 一族，该符号锚点 1:1 挂在
  wcol::hash::hash_object_impl::HashObject::hash_set，本判据位不复挂」，
  比本票修法（Set→HashSet）更彻底；grep 全仓无 HashObjectImpl.cs:Set 残留。
- 同面票据 task/ing/gate-anchor-drift-reclean.md 修法第 1 条亦覆盖此锚点（在途，worktree 存在），
  票据自身已注明「命中他人改动即让位」。
