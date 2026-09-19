wkv/src/vdb.rs 1495 行按三机制切目录：路由表 / DbMeta 记录 / GC 死号队列 / 管理器

来源：next/agy.design.md 条 20。
取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev 当下工作树，行号按符号重取。

现状
- /Users/z/git/db/wedb/wedb/wkv/src/vdb.rs 实测 1495 行，模块声明
  /Users/z/git/db/wedb/wedb/wkv/src/lib.rs:14 `pub mod vdb;`。一个文件里压了四块互不相干的职责：
  1 并发路由表：:37 DbRoutingTable（:50 get、:64 cell_or_insert、:71 set、:79 swap_out、
    :88 snapshot），papaya + ArcSwap 零锁查询面；
  2 DbMeta 持久记录与手写编解码：:118 enum DbMetaRecord、:161 DbMetaKeyBuf、:180 DbMetaValueBuf、
    :223 key、:286 value、:323 key_ns_map、:329 key_db_map、:340 dead_vid_of、:354 decode、
    :414 decode_value，加同段私有字节原语 :423 put_be64、:428 put_be_i64、:434 get_be64、
    :441 get_be_i64（KeyTag::DbMeta 0x0E 落盘面）；
  3 回收队列与引用面：:447 GcDeadEntry、:460 TenantRouting（:472 new、:481 refs）、
    :494 GcDeadLog（:510 insert、:520 remove、:532 get、:544 clear、:560 pop_reclaimable）；
  4 虚拟号管理器：:613 VirtualDbManager 至 :1208，含映射装配（:697 insert_ns_mapping、
    :747 get_or_create_ns、:773 get_or_create_db、:899 insert_db_mapping）、
    空闲换出（:910 bind_route、:941 unbind_route、:958 pop_idle_candidates、:983 evict_idle_route）、
    秒级清库（:1005 flush_db、:1046 flush_db_virtual、:1083 flush_ns、:1124 flush_ns_virtual）、
    安全纪元判定（:1157 is_virtual_id_dead_and_expired、:1181 is_dead_domain、:1190 is_dead_ns、
    :1196 is_active_vns）；:1210 起是本文件 cfg(test)。
- 消费面（跨 crate 路径按 `vdb::` 末段，切分后可保持不变）：
  /Users/z/git/db/wedb/wedb/wkv/src/store/vdb_load.rs（两处）、store/mod.rs、store/keyspace.rs、
  session/swap.rs、session/mod.rs、gc.rs，测试 wkv/tests/gc.rs、wkv/tests/store/cold_tenant_lazy_load.rs。

C# 参考
- 票面写的「garnet/libs/server/Storage/Session/MainStore/」不成立：C# 无虚拟库号映射、
  无 DbMeta 记录、无偏序 GC 队列（本项目自定义架构，
  见 .agents/skills/transpile/SKILL.md「数据库隔离、Namespace 与 ACL 存储架构」段与
  doc/zh/db.md）。故本单切分边界取「SKILL 自述的三机制 + 管理器」，不强行贴 C# 文件；
  这也正是它和 tiered_collection_ops、resp_server_session 两单的区别（那两单有 C# 分片文件可贴）。

修法（纯搬运，禁夹带语义改动）
1. vdb.rs → 目录模块 vdb/，mod.rs 只做 `mod` 声明与 `pub use` 重导出，
   保持 `wkv::vdb::<同名>` 对外路径不变，第 2 项消费文件零改动。
2. 切法：routing.rs（第 1 块 DbRoutingTable）、meta_record.rs（第 2 块 DbMetaRecord + 两 Buf +
   四个 be 原语）、gc.rs（第 3 块 GcDeadEntry + GcDeadLog，含 TenantRouting 引用面；
   与既有 /Users/z/git/db/wedb/wedb/wkv/src/gc.rs 的关系只在调用，禁混搬两文件）、
   manager.rs（第 4 块 VirtualDbManager）；
   cfg(test) 测试模块随所属子文件下沉为各文件内 cfg(test)（或按仓内惯例移 wkv/tests，
   与现状一致优先，禁顺手改断言）。
3. 可见性只调必要项：跨子模块私有项提 `pub(super)`，现 `pub` 面一律不扩、不缩；
   doc 注释与 garnet 锚点随函数搬位、一锚一位点。
4. 单文件目标规模：manager.rs 若仍逾 ~600 行，按 C# 无对位的自有分界再切 flush 族
   （flush_db/flush_db_virtual/flush_ns/flush_ns_virtual 四口 → vdb/flush.rs），
   不新造第三分法。
5. 本单不动编解码形态：第 2 块的手写 be64/be_i64 与 SKILL「编码优先 bitcode」取向的差异，
   若需处理另立票（现无在册票），搬运期禁改字节格式与记录语义。

验收判据
- `wc -l wedb/wkv/src/vdb/*.rs` 无单文件逾 700 行。
- 定义点唯一：`grep -rn "struct DbRoutingTable\|enum DbMetaRecord\|struct GcDeadLog\|struct VirtualDbManager" wedb/wkv/src`
  各命中 1 处。
- 消费面零改动：`grep -rn "vdb::" wedb/wkv/src wedb/wkv/tests` 的命中集合与拆分前逐条同名
  （lib.rs 的 mod 声明除外）。
- 编解码回归不破：DbMetaRecord 的 key/value 字节序用例（现 :1210 后 cfg(test) 段内的
  编码往返断言）全部通过，期望字节未改。
- diff 只含 use 行、可见性标记、文件切分；无新增 allow、无签名变化。

优先级
打磨（拓扑与体量，不改行为）；死代码与去重票之后开工。
