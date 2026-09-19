tiered_collection_ops.rs 巨峰文件按 C# ObjectStore 会话分界纯搬运拆分

来源：next/qcode10.design.md 条 9（切片四「模块拓扑与体量」，MED）分拣立项。
取证基线：主仓 /Users/z/git/db/wedb 当下代码，行号为当下实测（原快照 /tmp/rev10 旧行号作废）。

现状
- 单文件 /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/tiered_collection_ops.rs 共 2167 行，
  模块声明 /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/mod.rs:13 `pub mod tiered_collection_ops;`。
  一个文件同时承载四类 C# ObjectStore 会话文件 + 公共底座 + 成员 TTL 面。
- 位点分区（实测）：
  底座与上下文：TieredCtx :51、tree_put :75、tree_put_ok :96、tiered_precheck :111、
    tree_put_rejected :131、tree_del :140、tree_member_state :148、tree_payload :163
  成员 TTL 面：member_expire_arm :181、member_ttl_probe :223、expiry_tick_to_reply :242、
    member_persist_arm :258、collect_expired_members :292
  收尾单点：finish_tiered_arm :350、drain_or_save :370
  四类型执行体：exec_tiered_hash :392-:907（516）、exec_tiered_set :908-:1159（252）、
    exec_tiered_zset :1160-:1614（455）、list_head_seq :1615 + exec_tiered_list :1627-:1893（267）
  跨类型面：tiered_materialize_blob :1894-:2006、exec_tiered_collect :2007-:2044、
    exec_tiered_scan :2045-:2167
- 消费面：全仓 10 个文件按 `tiered_collection_ops::` 路径调用（
  /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/{hash_commands.rs,set_commands.rs,
  shared_object_commands.rs,object_store_utils.rs,tiered_demote.rs,
  list_commands/slow.rs,sorted_set_commands/slow.rs} 与
  /Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/{objects.rs,slow.rs}）。
  路径末段名即模块名，拆目录后可保持不变。

C# 参考（按类型四分，公共底座独立）
- /Users/z/git/db/wedb/garnet/libs/server/Storage/Session/ObjectStore/HashOps.cs（610 行）
- /Users/z/git/db/wedb/garnet/libs/server/Storage/Session/ObjectStore/ListOps.cs（470）
- /Users/z/git/db/wedb/garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs（977）
- /Users/z/git/db/wedb/garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs（1798）
- /Users/z/git/db/wedb/garnet/libs/server/Storage/Session/ObjectStore/Common.cs（837，公共底座）
  另见 AdvancedOps.cs（53）、CompletePending.cs（29）、SortedSetGeoOps.cs（233）——
  C# 从不把四类会话操作并进一个文件，rust 并了。

修法（纯搬运，禁夹带语义改动）
1. 把该文件改为目录模块 tiered_collection_ops/，mod.rs 只做 `mod` 声明与 `pub use` 重导出，
   保持 `wnode::resp::objects::tiered_collection_ops::<同名>` 的对外路径不变，消费侧 10 文件零改动
   （若 mod.rs 形态与仓内惯例相左，按 /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/
   list_commands、sorted_set_commands 既有目录模块先例同型处理）。
2. 文件切法贴 C# 分界：common.rs（第 1 项「底座与上下文」+「收尾单点」+ TieredCtx，
   对标 ObjectStore/Common.cs）、hash.rs、set.rs、zset.rs、list.rs（各承接对应 exec_tiered_*，
   对标四类 *Ops.cs）、scan.rs（tiered_materialize_blob + exec_tiered_collect + exec_tiered_scan，
   跨类型扫描/物化面，对标 C# 扫描与 CompletePending 侧的公共位）。
   成员 TTL 五口随底座进 common.rs（C# 侧 HashObjectImpl.cs/SetOps 等各自内调同一算式，
   rust 已是单点，禁在四类型文件里各抄一份）。
3. 可见性只调必要的：跨子模块使用的私有项由 `fn` 提 `pub(super)`，对外四入口保持
   `pub(crate)`；禁为搬运新增 `pub` 面（本仓禁以扩面换编译）。
4. use 树按新文件各自精简（现文件 :6-:49 的集中 use 逐子文件重列，删不带入的子项），
   doc 注释与 C# 锚点随函数体搬位、一锚一位点，禁重复挂载
   （锚点重复口径见 task/ing/cs-anchor-dup-single-mount.md 的判据基准）。
5. 单文件目标规模：除 zset.rs 外各文件不逾 ~900 行（对位 C# 单文件量级）；
   若 zset.rs 仍最大，按 C# SortedSetOps.cs 与 SortedSetGeoOps.cs 的自然分界再切 geo 面，
   不新造第三分法。

优先级
打磨（拓扑对标 C#、消巨峰文件），不改行为；排在死代码与去重票之后开工。

协调
- task/ing/object-store-utils-file-split.md：同目录兄弟文件 object_store_utils.rs 的拆分票，
  两票只搬各自文件，禁在同一次提交里混搬两族。
- task/ing/zero-consumer-dead-surfaces-batch-five.md 第 5 项删 object_store_utils.rs 的
  write_n2_array 零消费口：与本票不同文件，但同 crate 同目录，落地顺序错开即可。
- task/ing/tiered-write-arm-concurrency.md、task/ing/tiered-promote-demote-key-ttl.md、
  task/ing/ttl-purge-watch-version-bump.md 均改本文件语义：本票是纯搬运，须赶在其行为票之前
  或之后整体合入，禁与行为票同文件并行（否则产生逐块冲突）。
- task/ing/resp-null-protocol-single-source.md 射程含本文件的协议写面：同样按「先搬后改」错开。

验收
- 行数：tiered_collection_ops/ 下无单文件逾 1000 行；主目录文件树与 C# ObjectStore 四分同构。
- 对外符号路径与签名零变化：`grep -rn "tiered_collection_ops::" wedb/wnode/src` 的 10 个消费文件
  diff 为空（mod 声明处除外）。
- 语义零漂移：函数体逐字搬运，diff 只含 use 行、可见性标记、文件切分；无新增/删除 pub 面、无新增 allow。
- ./js/check.js 缺失与重复锚点组数不增（锚点随函数搬家不重复挂）。
- 仅 `cargo check --workspace --all-targets`（私有 target 目录）零 error 零 warning；
  test.sh 与 clippy 由中央整合轮执行。

盘点补记（qw13.invA tiered-collection-ops-file-split）：dev e75716e 复核：文件 2237 行（票载 2167 后继续上涨），wnode/src/resp/objects/ 仍无 tiered_* 子模块目录承接，四执行体分界仍在单文件。政策提示维持：与本仓 resp-server-session-file-split 拒判不同型（确有执行体分界），可派纯搬运。
