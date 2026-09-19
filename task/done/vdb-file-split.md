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

结案注记（判成立；载体分支 vdb-split：代码 9b654a3 + 文档指针随迁 2237568，主仓 dev 纯 FF 42652b2）

步骤 0 交叠自检（实测）
- reviv 二棒：/tmp/fork/fix-reviv-crtt-gate 的 status --short 与 diff --name-only 皆空，
  dev..fix-reviv-crtt-gate 零提交（该棒载荷已入 dev 1905e8a），射程不含本票对象，无交叠。
- git log --oneline -- wedb/wkv/src/vdb.rs 仅 6dc1cb6 与 init 两枚，无他人已拆。
- 并发禁碰面零触碰：wkv/src/lib.rs 未动一行（`pub mod vdb;` 仍在 :14，本票无需改它），
  session/** 未动；wkv/src/gc.rs 在合并窗内已由 wkv-gc-file-split 棒落成
  wkv/src/gc/{mod,compact,reclaim,ttl_sweep,vdb}.rs，本棒只与之并存、未并其一字节
  （票面「禁混搬两文件」守死——新件 wkv/src/vdb/gc.rs 与其 wkv/src/gc/vdb.rs 同名交叉，
  见文末撞面提示）。

票面复核（现刻 HEAD e86da1b，行号按符号重取）
- 成立。1495 → 实测 1431 行，系 6dc1cb6（−86/+22）删本地取号回放死面所致；票面点名的
  四块职责与全部符号逐枚命中，票面行号整体前移约 10 行（如 decode 票面 :354 实测 :364）。
- 唯一实质漂移：票面第 4 块的 flush_db_virtual(:1046) 与 flush_ns_virtual(:1124) 两口已被
  6dc1cb6 连同 flush_virtual_database/flush_virtual_namespace 一并删除，清库族现存
  flush_db(:1025)/flush_ns(:1053)。修法条 4 的「四口」按现存两口执行，不新造分法。
- 消费面清单实测一致（store/vdb_load.rs 两处、store/mod.rs、store/keyspace.rs、
  session/swap.rs、session/mod.rs、gc.rs，测试 tests/gc.rs、tests/store/cold_tenant_lazy_load.rs）；
  票面未列的 lib.rs:39 `pub use vdb::DbMetaRecord;`（crate 级透出，wnode 回放面用）一并保住。
- 开工前置「死代码与去重票之后」已满足（dead-batch-six 已于 e86da1b 归档）。

落位与行数对照（旧件 1431 行 → 六件，最大 630 行）
- vdb/mod.rs 10：五枚 mod 声明 + 四枚 pub use 透出旧件全部十枚公开名（ROOT_VIRTUAL_ID、
  ROOT_DBMETA_PREFIX、DbRoutingTable、DbMetaRecord、DbMetaKeyBuf、DbMetaValueBuf、
  GcDeadEntry、TenantRouting、GcDeadLog、VirtualDbManager）；旧件无 //! 件首注释，
  故不新造（修法 1「mod.rs 只做 mod 声明与 pub use 重导出」从严执行）。
- vdb/routing.rs 78：DbRoutingTable + impl + Default（旧 29-101）。
- vdb/meta_record.rs 482：ROOT_DBMETA_PREFIX（24-27）+ DbMetaRecord 六变体、DbMetaKeyBuf、
  DbMetaValueBuf、编解码 impl 与四枚 be 原语（103-453）+ 编码往返测试（1313-1430）。
- vdb/gc.rs 204：GcDeadEntry、TenantRouting（引用面）、GcDeadLog（455-610）+ 到期堆测试
  （1284-1311）——本件纯死亡账本，与 wkv/src/gc/ 驱动面无一行交叉。
- vdb/manager.rs 630：ROOT_VIRTUAL_ID（21-22）+ VirtualDbManager 结构与 Default、new/reset、
  映射装配、空闲换出、安全纪元判定（612-1017、1086-1144）+ 四条管理器测试（1150-1282）。
- vdb/flush.rs 71：清库换号两口（1018-1051、1053-1084），独立 impl VirtualDbManager 块。
- 条 4 的触发与止境：manager.rs 切片后实测 698 行（逾 ~600）→ 按票面自有分界迁出 flush 族
  得 630 行；两口已全迁，再切即须新造第三分法（票面禁区），故止于 630，验收条 1
  「无单文件逾 700 行」满足（余件 482/204/78/71/10）。
- cfg(test) 六用例随所属子件下沉（4→manager、1→gc、1→meta_record），未移 wkv/tests、
  断言与期望字节零改动（票面「与现状一致优先」）。
- 可见性只提跨件必要四枚为 pub(super)：TenantRouting 的 refs 与 authoritative 字段、
  TenantRouting::refs() 访问器、GcDeadLog::new；现 pub 面不扩不缩，无 shim、无门面转发、
  无新增 allow、无签名变化。

搬家中性取证
- 旧件 1315 条非空行中 1310 条逐字节在册（脚本切片 + 盘上多重集复验）；余 5 条 =
  4 条 pub(super) 提级行 + 1 条 use 头重组行（`AtomicBool, AtomicU64,` 一行按件拆两半，
  AtomicBool 只归 vdb/gc.rs）。新件独有 35 种行全部为件首 use、mod 声明与 pub use 透出、
  三处 cfg(test) 骨架、flush.rs 的 impl 包裹两行，零逻辑行。
- 旧件 git rm，全仓（src/tests/配置/ignore 语料）grep "vdb.rs" 零命中。
- 锚点：域内唯一 garnet 锚 `garnet/libs/server/StoreWrapper.cs:FlushDatabase` 随 DbMetaRecord
  块走位于 vdb/meta_record.rs；复刻 rustScan.js 的 CS_REF_REGEX 全树对跑，拆前/拆后同为
  提及 4641 处、去重 4263 枚，逐枚相同，逐文件分布唯一差异即 wkv/src/vdb.rs(1) →
  wkv/src/vdb/meta_record.rs(1)。
- bun js/check.js 同基（e86da1b）前后对跑：输出逐字节相同（含 C# 语料降级段），exit 0，
  ignore 语料零回写；并入最新 dev 后复跑仍 exit 0、零回写。
- 验收条 2：`grep -rn "struct DbRoutingTable\|enum DbMetaRecord\|struct GcDeadLog\|struct VirtualDbManager" wedb/wkv/src`
  四式各命中 1 处（依次落 routing/meta_record/gc/manager 四件）。
- 验收条 3：`grep -rn "vdb::" wedb/wkv/src wedb/wkv/tests` 拆前拆后 diff 空（八个消费文件
  与 lib.rs 逐字节未改）。
- 已知副作用（不属本仓门禁）：三枚 intra-doc 链跨件后 cargo doc 不再解析——vdb/gc.rs 的
  [`VirtualDbManager::bind_route`]、vdb/meta_record.rs 的 [`VirtualDbManager`]、vdb/manager.rs 的
  [`GcDeadEntry::vns`]（后者因 flush 族迁出后 manager 不再在代码里构造 GcDeadEntry，
  若保留导入即成 unused import，故按「无新增 allow」取舍弃导入）。与本件拆前既有形态同口径：
  拆前 [`WedbStore::finish_vdb_rebuild`] 已是跨模块裸链不解析，本仓门禁无 cargo doc 步骤，
  cargo check 面零告警。修法 3「doc 注释随函数搬位」优先，未改写任何 doc 文本。

门禁实测（私有 CARGO_TARGET_DIR=/tmp/ct-vdb，未跑主仓 ./test.sh 与 ./sh/clippy.sh）
- cargo check --workspace --all-targets：exit 0，0 error 0 warning（并入最新 dev 后复验同）。
- cargo nextest run -p wkv --no-fail-fast：222/222 全绿，含下沉后的
  vdb::manager::tests 四条、vdb::gc::tests::test_gc_dead_expiry_heap_prefix、
  vdb::meta_record::tests::test_dbmeta_record_roundtrip（验收条 4 期望字节未改）。
  他棒在途件未致红，故未使用 --test 过滤、亦未改他人测试。
- cargo fmt --all -- --check：exit 0（本棒六件零 diff；合并窗内他人三件 fmt 漂移已由
  96638dc 归一，本棒不携）。

C# 对位面注记（vdb 相关件，本棒实测补票）
- 票面「不成立」判断复验：garnet 全库 grep `DbMeta|VirtualDb` 零命中，C# 确无虚拟号映射、
  DbMeta 记录、偏序回收队列三机制。
- 但 C# 侧「库管理器族」另有实物，本棒未贴亦不拟贴：garnet/libs/server/StoreWrapper.cs:613
  FlushDatabase、:567 GetDatabasesSnapshot，以及 garnet/libs/server/Databases/ 五件
  （IDatabaseManager.cs、DatabaseManagerBase.cs、MultiDatabaseManager.cs、
  SingleDatabaseManager.cs、DatabaseManagerFactory.cs）——其分界维度是「单库/多库管理器形态」，
  每库一 Tsavorite 实例、清库即物理截断（DatabaseManagerBase.cs:301），与本票「三机制 +
  管理器」无一对位。域内唯一在册锚仍为 StoreWrapper.cs:FlushDatabase，现挂 meta_record.rs
  （即 DbMetaRecord 件首自述 C# 清库形态处，旧件 :116 原行 → 新件 :21）；manager.rs:311 的
  `GetDatabasesSnapshot` 系裸名提及（无 libs/ 前缀），按 check-ignore-gate 既有取证不登记。
- 本票四（实为五）件边界即 SKILL「数据库隔离、Namespace 与 ACL 存储架构」自述形态，
  doc/zh/db.md 1.3/1.4 为唯一权威描述面（其代码路径指针已随迁，见下）。

撞面提示与后续
- doc/zh/db.md 三处旧路径指针随迁（2237568）：1.3 FLUSHDB 行改指 vdb/flush.rs 与
  vdb/routing.rs、FLUSHALL 行改指 vdb/flush.rs、1.4「代码路径」行改列目录模块五件职责；
  wkv/src/gc.rs 一枚指针不动（现件已由 gc 棒落成 wkv/src/gc/ 目录，归其票面处置）。
- 在途票的 vdb.rs 行号指针自本票落地起失效，须按符号重取：
  task/ing/my-dbmeta-lock-yield-spin.md（DbMeta 换号锁改异步等待 → vdb/manager.rs 与
  vdb/meta_record.rs 面）、task/ing/my-vector-replay-slot-fallback.md:15
  （logic_domain_of → vdb/manager.rs:292 起）。
- 命名交叉警示：本票新件 wkv/src/vdb/gc.rs（死亡号账本）与 gc 棒新件 wkv/src/gc/vdb.rs
  （GC 侧 vdb 编排）同名互易，后续引用与看票务必带全路径。
