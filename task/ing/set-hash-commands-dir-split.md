set_commands.rs 与 hash_commands.rs 目录化：对齐 list/zset 既有分片先例

来源：next/agy.design.md 条 19。
取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev 当下工作树，行号按符号重取。

现状
- 两个单文件仍是平铺文件，而同目录兄弟族早已目录化：
  /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/set_commands.rs 实测 1431 行、
  /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/hash_commands.rs 实测 1116 行；
  对照 /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/list_commands/
  （mod.rs 148、read.rs 163、write.rs 506、blocking.rs 517、slow.rs 738）与
  sorted_set_commands/（mod.rs 261、read.rs 262、write.rs 1071、blocking.rs 421、slow.rs 883）。
- 内部分层已经存在但被压在同一文件里：
  set_commands.rs —— 底座 :39 run_operate、:54 set_load_sync、:70 set_save_or_gc、
    :91 should_write_back、:102 is_read_only、:117 load_many（多键漏斗）、
    :770 intersect_sets、:788 union_sets、:799 diff_sets、:817 combine_store、
    :845 write_set_members、:1397 cfg(test) mod；慢路径整段是内联嵌套模块
    :859 `pub(crate) mod slow {` 起至 :1396（约 538 行内联 mod）。
  hash_commands.rs —— 底座 :34 run_operate、:52 hash_load_sync、:75 should_write_back、
    :94 is_read_only；慢路径 :766 `pub(crate) mod slow {` 起至文件末（约 350 行内联 mod）；
    同文件另有 :751 write_null_array（HMGET 逐元素 nil 占位）属命令族局部写出面。
- 内联 `pub(crate) mod slow {}` 形态与本仓目录分片先例不一致：慢段与快段同文件，
  滚动审阅时两段的参数推导对（见 next/object-slow-dispatch-arg-reparse-single-source.md 现状 1、2）
  分散在同一文件两端，改一处极易漏另一处。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/server/Resp/Objects/SetCommands.cs 实测 778 行、
  /Users/z/git/db/wedb/garnet/libs/server/Resp/Objects/HashCommands.cs 实测 812 行，
  单命令单方法平铺（rust 因快慢双路 + tiered 门臂膨胀到 1.4~1.8 倍）。
  C# 的 pending 重放在同一方法内（CompletePending 后 goto 重跑），
  rust 已把慢路独立成 mod，切文件天然贴合。

修法（纯搬运，禁夹带语义改动）
1. set_commands.rs → 目录模块 set_commands/：
   mod.rs（结构体/枚举、`pub use` 重导出、跨子模块共用的 use 头）、
   common.rs 或直接在 mod.rs 承接底座（run_operate、set_load_sync、set_save_or_gc、
   should_write_back、is_read_only、write_set_members），
   read.rs（SMEMBERS/SSCAN/SISMEMBER/SRANDMEM 类只读命令）、
   write.rs（SADD/SREM/SPOP/SMOVE/SDIFFSTORE&SINTERSTORE&SUNIONSTORE 组合面
   intersect_sets/union_sets/diff_sets/combine_store/load_many），
   slow.rs（现 :859 内联 `pub(crate) mod slow` 整体提为文件，`pub(crate)` 路径保持不变）。
2. hash_commands.rs → 目录模块 hash_commands/：mod.rs + read.rs + write.rs + slow.rs
   （现 :766 内联 mod 提出），底座 run_operate/hash_load_sync/should_write_back/is_read_only
   留 mod.rs，:751 write_null_array 随 HMGET 所在文件走（禁挪进 wresp，它非版本分派复抄，
   已确认版本分派单源在 wresp::ext::RespVecExt::write_resp_null_ver）。
3. 快慢命令归类以现有 `is_read_only` 名单为准（set :102、hash :94），禁另立第二套读写判据。
4. 分片尺寸向 list/zset 先例看齐：除 slow.rs 外单文件不逾 ~700 行；
   set 的 slow.rs 若超 ~900 行，按 read/write 再切一层，不新造第三种分法。
5. 可见性只调必要项（跨子模块私有项提 pub(super)），禁新增 pub 面；
   doc 注释与 C# 锚点随函数搬位、一锚一位点。

验收判据
- `grep -rn "pub(crate) mod slow {" wedb/wnode/src/resp/objects/*.rs` 零命中
  （两族的慢段均已成文件；sorted_set/list 先例本即文件）。
- 对外路径不变：`grep -rn "objects::set_commands::\|objects::hash_commands::" wedb/wnode/src wedb/*/tests`
  的命中符号集合与拆分前一致（除 mod 声明行）。
- 定义点唯一：`grep -rn "fn set_load_sync\|fn hash_load_sync\|fn write_set_members" wedb/wnode/src`
  各命中 1 处定义。
- diff 只含 use 行、可见性标记、文件切分；无签名变化、无新增 allow、无协议字节变化。

优先级
打磨（拓扑对齐同目录先例、消巨型文件与内联 mod），不改行为；
排在 set/hash 族的语义票（next/object-slow-dispatch-arg-reparse-single-source.md、
next/tiered-zset-demote-bf-tree-recursion-stack-overflow.md 相关面）之后或之前整体合入，
禁与语义票同文件并行。
