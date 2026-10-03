审核结论：通过（2026-09-28 r437 阶段三独立审核席 + 主控复核；定级：可观测显示面，缺省配置即达不降档）

裁定：走 a 路（堆行恒 0 + 投影注释自陈 + deviations 册尾顺编登记）。
一、硬依据（审核席一手实读，主控抽检复验吻合）
1. C# 的 HeapSizeBytes 值域只有两态：tracker 在场时真实对象堆驻留，或 tracker 为 null 时 0
   （LogAccessor.cs:102）。且接入是有条件的：garnet/libs/host/GarnetServer.cs:496 仅在
   kvSettings.LogMemorySize > 0 || ReadCacheMemorySize > 0 才 Initialize 并挂 SetLogSizeTracker，
   而 LogMemorySize 只在配置非空串时才写入（GarnetServerOptions.cs:768-769）
   ⇒ 缺省部署（未配目标内存）下 C# 生产本身即报 0，「恒 0」是本仓对位 C# tracker-null 的缺省生产同形，非虚标。
2. 恒 0 显示行有在册先例：deviations §165（含 §165a）已裁 gc_* 四行恒 0 系纯台账零行为合法形态，同族先例覆盖本案。
3. 排除 b（建/接真源）：本仓无「页环外稳态对象堆」维度——集合信封序列化入环（whlog 头注）、
   页环外稳态驻留另有 TreeCache.ReservedBytes/BudgetBytes 独立行（§166c 在册）、
   集合读内存只随结果窗口增长不随键基数物化整表（doc/zh/collection.md:104/:115/:135）。
   为观测面新建第二套计账机制违单机制规矩。审核席另独立证 wkv/src/store/stats.rs:StoreSnapshot
   （本投影的唯一值源）通体无堆字节字段 ⇒ 不存在「真源在侧却接错」，属域缺失 + 接线别名双重事实。
4. 排除 c（认「环即堆」）：MemorySizeBytes 的 C# 文档注释原文即「not including heap objects」
   （LogAccessor.cs:88-92），两行恒等直接违背该字段群量纲定义，且「环即堆」并非本仓既定改良口径（台账零命中）。
二、票面订正（审核席一手纠正，主控复验）
1. 原「测试夹具即按不同值填写」失实：resp_info.rs:578-580 与 :607-608 实为
   log_memory_size_bytes 65536 / log_heap_size_bytes 65536 同值、rc memory_size_bytes 200 / heap_size_bytes 200 同值，
   夹具复刻了别名恒等形，只证明字段结构上可独立承载，不构成投影接错的反证素材。
2. 原「ObjectAllocatorImpl.cs:1382-:1393 系写入侧递增」有误：该段实为 TrackRecoveredObjectRecord（检查点恢复侧补账，
   :1382 判 null 短路）；常规写入侧递增经 logSizeTracker.UpdateSize 与页闭合路径（AllocatorBase.cs:1439-1441 注释）。不改结论。
3. CacheSizeTracker 接入位行号微漂（:84-86 ⇒ Initialize 于 :77 起，mainLogTracker :85-86，SetLogSizeTracker :87，
   读缓存臂 :90-95），一律以符号名为准。
三、收窄后执行方案（供施工席直接消费）
1. wedb/wnode/src/resp/garnet_api/mod.rs:project_db_snapshot 两处（log_heap_size_bytes、读缓存 heap_size_bytes）
   改 0i64 常量形；不改 DbSnapshot/ReadCacheSnapshot 结构、不新增字段、不动 max/current 两行；
   投影注释追加堆行口径一句（logSizeTracker 机制缺席 ⇒ 对位 C# tracker-null 恒 0；禁钉行号，给符号锚）。
2. doc/zh/deviations.md 册尾顺编新条目：判据 + 符号锚（log_heap_size_bytes、heap_size_bytes、
   LogAccessor.HeapSizeBytes、project_db_snapshot）+ 来源指针本票路径；占号按「先入库者得号、撞号让位」，禁钉行号。
3. wedb/wnode/tests/resp_info.rs：夹具堆字段改 0（与投影真形一致），新增断言 INFO STORE 段
   Log.CurrentHeapSizeBytes 与 ReadCache.CurrentHeapSizeBytes 出值为 0 且与同行 CurrentMemorySizeBytes 不再恒等；
   既有行名单源/段序回归臂不得弱化；求和面（:403/:410/:434）零 diff 即为「不污染记账」的回归自证。
四、禁触线
不得触碰 wnode/src/resp/objects/ 与 tiered_collection_ops 域（同批 wnode-tiered-zscan 案射程）；
不得触碰 waof/aof 投影邻码；不得改写 wmetric MEM_SOURCE_PAIRS/gc_* 段（§165 在册域，只可回指）。
五、未尽面（防扩面）
max/current 两行「上限==当前==整环常驻」口径已有 :343-349 在册自陈，本票不动；
target_size 系 None 形态族（store_heap_memory_target_size 链）属既有自陈覆盖；
wcol 对象内 heap_memory_size 账与 MEMORY USAGE 按需估算链是完整在册机制，严禁接线借用；
未来若真引入目标内存强制，tracker 与 heap 行一并复活属独立大票。

---

INFO STORE 段 Log.CurrentHeapSizeBytes 与 ReadCache.CurrentHeapSizeBytes 被投影别名成整环常驻字节，与同段 CurrentMemorySizeBytes 恒等（C# 系日志外对象堆驻留维度，rust 无 SizeTracker 机制，值源接错而非缺域）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 侧两行是不同量纲，且各自有唯一值源：
   - garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:313 Log.MaxMemorySizeBytes ← db.Store.Log.MaxMemorySizeBytes
   - 同文件 :314 Log.CurrentMemorySizeBytes ← db.Store.Log.MemorySizeBytes
   - 同文件 :315 Log.CurrentHeapSizeBytes ← db.Store.Log.HeapSizeBytes
   - 同文件 :324-:326 ReadCache 三行同形（CurrentHeapSizeBytes ← ReadCache?.HeapSizeBytes）
   值源定义在 garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs：
   - :88-92 MemorySizeBytes 文档注释原文「Actual memory used by log (not including heap objects)」，实现 (long)AllocatedPageCount << LogPageSizeBits（页环字节）
   - :100-102 HeapSizeBytes 文档注释原文「Heap memory used」，实现 allocatorBase.logSizeTracker is null ? 0 : logSizeTracker.LogHeapSizeBytes
   - :82-85 LogSizeTracker 属性注释原文「The log size tracker (currently used only by test)」
   LogHeapSizeBytes 本体在 garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/LogSizeTracker.cs:95 heapSize.Total，
   递减点在 garnet/libs/storage/Tsavorite/cs/src/core/Allocator/ObjectAllocatorImpl.cs:360/:374/:453（驱逐/回收侧），
   递增经 logSizeTracker.UpdateSize 与页闭合路径（AllocatorBase.cs:1439-1441 注释），
   另 ObjectAllocatorImpl.cs:1382-:1393 系 TrackRecoveredObjectRecord 检查点恢复侧补账（:1382 判 null 短路）而非写入侧，
   即堆维度只计「不在页环内的对象堆负载」。
   生产接入点：garnet/libs/server/Storage/SizeTracker/CacheSizeTracker.cs:Initialize（:77 起）内
   :85-:86 建 mainLogTracker 并 :87 store.Log.SetLogSizeTracker，:90-:95 读缓存同形；未配目标内存时 tracker 为 null，HeapSizeBytes 如实报 0。
   即 C# 的取值域只有两种：真实对象堆驻留字节，或 0；任何情况下都不等于页环常驻字节。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   - wedb/wnode/src/resp/garnet_api/mod.rs:352 计算 log_memory = (s.log_num_pages * s.log_page_size_bytes) as i64（页环字节，对位 C# MemorySizeBytes 公式）
   - 同文件 :371 log_max_memory_size_bytes: log_memory、:372 log_memory_size_bytes: log_memory、:373 log_heap_size_bytes: log_memory
     三行同源同一个 log_memory，堆行被别名成页环字节。
   - 同文件 :385 读缓存臂 heap_size_bytes: rc.memory_size_bytes 同为别名（读缓存域亦无独立堆量源）。
   - 出行为 wedb/wmetric/src/info/garnet_info_metrics.rs:595 MetricsItem::new("Log.CurrentHeapSizeBytes", n(db.log_heap_size_bytes))，
     与 :594 Log.CurrentMemorySizeBytes 相邻出行；读缓存侧 wedb/wmetric/src/info/garnet_info_metrics.rs:1126 READ_CACHE_ROWS 内
     ("ReadCache.CurrentHeapSizeBytes", |c| c.heap_size_bytes)。
   - 快照字段本身在结构上独立可承载不同值：wedb/wmetric/src/info/garnet_info_metrics.rs:90 pub log_heap_size_bytes: i64
     与 :88 log_memory_size_bytes 分列两字段（读缓存侧 ReadCacheSnapshot.heap_size_bytes 同形）；
     注意夹具现形复刻了别名恒等（:578-:580 与 :607-:608 两两同值），故字段可分列只证「恒等非结构要求」，
     夹具本身不构成投影接错的反证素材（见上「二、票面订正」第 1 条）。
   - rust 侧确无 C# LogSizeTracker 的等价常驻计账机制（全仓检索 pattern「fn .*heap.*\(」「object_heap」「heap_size」仅命中：
     按需估算路径 wedb/wcol/src/object_payload.rs:178 object_heap_estimate、wedb/wcol/src/types/garnet_object.rs:51/:106 heap_memory_size
     （单对象字段，随 account_entry 增减，见 wedb/wcol/src/hash/hash_object.rs:302、wedb/wcol/src/zset/sorted_set_object.rs:728）、
     wedb/wext_roaring/src/roaring_bitmap_object.rs:117 heap_estimate、消费点
     wedb/wnode/src/resp/objects/object_store_utils.rs:1004 envelope_heap_estimate（MEMORY USAGE 单键按需估算）；
     无任何 store 级堆字节累加器）。
   - 该分叉的既有自陈只覆盖「目标内存」未覆盖「堆驻留」：garnet_api/mod.rs:343-349 投影注释宣示「whlog 常驻整页分配模型下已分配页 ==
     上限 == 整环页数，内存上限 == 当前 == 常驻整环字节」与「mainlog/readcache 目标内存（C# SizeTracker）rust 无对应机制，
     None 经 wmetric 出直加当前内存分支」，通篇未提 heap 行取值口径。
   - 不在册：doc/zh/deviations.md 全文 grep「HeapSizeBytes」「heap_size」零命中；票据池 grep「CurrentHeapSizeBytes」「log_heap_size_bytes」零命中。

3. 逻辑危害确证
   - 可观测契约分叉：INFO STORE 段（读缓存九行同段，garnet_info_metrics.rs:601-:609 单次装配）两行恒等，运维侧据 C# 语义读出的「日志外堆驻留」维度在 rust 侧被虚标为整个环大小，
     同段自相矛盾（页环 + 堆 = 两倍环容量），任何按 C# 字段语义做的容量核算/告警阈值都会失真。
   - 非记账污染：该值不参与求和，store_mainlog_memory_size 只用 log_memory_size_bytes
     （wedb/wmetric/src/info/garnet_info_metrics.rs:403，求和行 :410/:434），故 INFO MEMORY 段与 total_main_store_size 面不受影响，
     危害限于 STORE 段逐行可观测面。
   - 缺省配置即达：INFO 全段为默认输出，无需任何旋钮，故定级不因「需特殊配置」而降。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/garnet_api/mod.rs:project_db_snapshot（:352 log_memory 计算、:371-:373 三行同源、:385 读缓存别名）
wedb/wmetric/src/info/garnet_info_metrics.rs:DbSnapshot.log_heap_size_bytes（:90）、get_database_store_stats（:594-:595）、READ_CACHE_ROWS（:1120-:1130）
wedb/wnode/tests/resp_info.rs（:580/:608 夹具按不同值填写，现无该两行的值源断言）

对应 c# 文件与函数：
garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetDatabaseStoreStats（:313-:315、:324-:326）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:MemorySizeBytes（:88-:92）、HeapSizeBytes（:100-:102）、LogSizeTracker（:82-:85）、SetLogSizeTracker（:205-:206）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/LogSizeTracker.cs:LogHeapSizeBytes（:95）
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/ObjectAllocatorImpl.cs:堆增减三点（:360/:374/:453/:1382-:1393）
garnet/libs/server/Storage/SizeTracker/CacheSizeTracker.cs:Initialize（:77 起；mainLogTracker :85-:86、SetLogSizeTracker :87、读缓存臂 :90-:95）
garnet/libs/host/GarnetServer.cs（:488 裸构造 CacheSizeTracker；:496-:499 仅 LogMemorySize>0 || ReadCacheMemorySize>0 才接入并挂 tracker）
garnet/libs/server/Servers/GarnetServerOptions.cs:LogMemorySize（:768-:769 仅在配置非空串时写入 kvSettings）

精炼执行方案：
1. 先由审核席裁口径（三选一，必须给一手依据，严禁落第三套）：
   a. 恒 0 + 投影注释自陈：rust 无 store 级堆驻留计账机制，且本仓集合态为「不随键基数物化整表、内存只随结果窗口增长」
      （doc/zh/collection.md:104/:115/:135 口径），页环外无稳态对象堆维度 ⇒ 如实报 0，语义即 C# logSizeTracker 为 null 形态；
      :373/:385 两处改 0，注释按此自陈，并在 deviations 册尾顺编登记该分叉。
   b. 接真源：若审核席认定本仓确有稳态页环外驻留维度，则须先指明该维度的唯一累加器符号（现检索零命中，需先建后再消费，
      不得为观测面新增第二套计账机制），否则本项不成立。
   c. 认定「rust 环即堆」为既定改良口径：则两行恒等需在该行出处与投影注释双处宣示，并登记 deviations；
      审核席须同时裁「为何同一量出两行」不违可观测语义。
2. 主方案（a）落地面最小：project_db_snapshot 两字段改 0 常量形（不得新增字段、不得改 DbSnapshot 结构），
   投影注释 :343-:349 追加堆行口径一句（禁钉行号），deviations 条目带符号锚 log_heap_size_bytes、heap_size_bytes、HeapSizeBytes。
3. 测试验证点：
   - wedb/wnode/tests/resp_info.rs 新增断言：INFO STORE 段 Log.CurrentHeapSizeBytes 与 ReadCache.CurrentHeapSizeBytes
     按裁定口径出值（恒 0 或登记口径），并断言其与 Log.CurrentMemorySizeBytes 不再恒等（或恒等理由在册）；
     夹具 :580/:608 既有的不同值填写须随之改为与真源一致的形（不得留「夹具能填不同值、投影却恒等」的自相矛盾态）。
   - 既有 INFO 行名单源/段序回归臂不得弱化。

---

## 终态注记（2026-09-28 施工席 r437，分支 fix-wmetric-info-heap-row-zero，基 e3b1838）

### 一、改动面清单与每处理由
1. wedb/wnode/src/resp/garnet_api/mod.rs::project_db_snapshot（审头方案 1）
   - log_heap_size_bytes 由别名 log_memory 改 0i64 常量形；
   - 读缓存臂 heap_size_bytes 由别名 rc.memory_size_bytes 改 0i64；
   - 函数头注（原「max/current 口径 + target None 分支」自陈段末）追加堆行口径：sizeTracker
     机制缺席⇒恒折 0 对位 C# LogAccessor.HeapSizeBytes tracker-null 出 0；严禁为观测面另建
     第二套堆计账；登记锚 deviations.md §183。未钉行号，全用符号锚。
   - DbSnapshot/ReadCacheSnapshot 结构未改、无新增字段；max/current 两行不动（仍 log_memory /
     rc.memory_size_bytes 同形）；wmetric 行出面（get_database_store_stats、READ_CACHE_ROWS）零改动。
2. doc/zh/deviations.md 册尾顺编 §183（审头方案 2）：判据 + 符号锚（project_db_snapshot、
   DbSnapshot.log_heap_size_bytes、ReadCacheSnapshot.heap_size_bytes、LogAccessor.HeapSizeBytes、
   CacheSizeTracker.Initialize）+ 来源指针 task/done/wmetric-info-log-heap-row-aliased-to-ring-bytes.md；无钉行号，未动任何既有条目。
3. wedb/wnode/tests/resp_info.rs::info_store_snapshot_channel_populates_segments（审头方案 3）
   - 夹具 log_heap_size_bytes 65536→0、读缓存 heap_size_bytes 200→0（与投影真形一致，消除夹具
     复刻别名恒等形）；
   - STORE 段新增：堆两行出值 0 正断言、同行内存行出值断言（65536/200）、负断言锁堆行≠内存行值
     （别名恒等形回摆即红）。

### 二、锚复核（施工席一手实读，含订正回报）
- 审头判据全部复验吻合：C# LogAccessor.cs HeapSizeBytes 实现即 logSizeTracker is null ? 0 :
  LogHeapSizeBytes；MemorySizeBytes 注释「not including heap objects」；GarnetServer.cs 仅
  kvSettings.LogMemorySize > 0 || ReadCacheMemorySize > 0 才 cacheSizeTracker.Initialize；
  GarnetServerOptions.cs 仅 !string.IsNullOrEmpty(LogMemorySize) 才写 kvSettings.LogMemorySize；
  GarnetInfoMetrics.cs Log.CurrentHeapSizeBytes ← db.Store.Log.HeapSizeBytes；
  wedb/wkv/src/store/stats.rs 全文 grep「heap」零命中（StoreSnapshot 无堆字节字段，复核「非真源在侧接错」）。
- 审头「二.3」微续漂移：CacheSizeTracker.cs mainLogTracker 构造实 :84-86（审头写 :85-86）、
  store.Log.SetLogSizeTracker 实 :86（审头写 :87），符号锚为准，不影响结论。
- rust 侧票面/审头行号（project_db_snapshot 内 log_memory 计算与三行/读缓存别名、
  resp_info 夹具 :578-580/:607-608、wmetric :594-595/:1126/:403/:410/:434、投影注释 :343-349）
  与现码全部吻合，本轮无漂移。
- 全仓 grep CurrentHeapSizeBytes / log_heap_size_bytes / heap_size_bytes 消费面仅
  garnet_api 投影、garnet_info_metrics 出行与结构字段、resp_info 夹具三文件，无其它测试或
  工具面引用恒等形，改后无波纹触面。

### 三、台账取号与撞号经过
- 册尾 grep 正文实测最大号 §182（# 一、在册条目末），取 §183；读号与写入在同一条命令内完成，
  未发生撞号、未让号；引用的先例号 §165（含 a/c 分目）与 §166 均先 grep 正文确认在册后才引；
  空缺位 §173/§177/§179/§180 未复用。

### 四、测试自证
- 断言内容：INFO STORE 段 Log.CurrentHeapSizeBytes:0、ReadCache.CurrentHeapSizeBytes:0、
  Log.CurrentMemorySizeBytes:65536、ReadCache.CurrentMemorySizeBytes:200，另二负断言锁
  「堆行≠同行内存行值」。
- 未弱化：STORE/MEMORY/PERSISTENCE/STOREHASHTABLE/STOREREVIV 既有行值与段序断言、
  N/A 形态臂、aof 推导臂逐条未动。
- 求和面零 diff 自证：wmetric garnet_info_metrics.rs 本次零改动（git diff 文件面不含 wmetric），
  store_mainlog_memory_size/store_readcache_memory_size/total_main_store_size 只取
  log_memory_size_bytes 与 rc.memory_size_bytes，夹具两字段值未动 ⇒ MEMORY 段断言
  （65536/200/70024）原样成立，不污染记账。
- 运行口径：本席仅 CARGO_TARGET_DIR=/tmp/_rs/fix-wmetric-info-heap-row-zero cargo check
  --offline -p wnode --tests -j 3，两次 Finished 干净、warning 计数 0（wmetric 作为依赖被
  同级 check 覆盖）；test.sh/clippy/check.js 归主控门禁，resp_info 新断言待门禁实证落绿。

### 五、未尽面与自查风险
- resp_info 为 DbSnapshot 直装夹具形态，不经 project_db_snapshot 装配缝；本票按审头方案只改
  夹具形 + 出值断言，未加「真走投影」的端到端装配缝堆行出 0 断言（现测试面无该缝既有臂，新扩
  越审头方案一界，挂待后票）。投影面真源由代码常量形 + §183 台账口径承接。
- max/current「上限==当前==整环常驻」口径、target_size None 形态族、wcol 对象内 heap_memory_size
  账与 MEMORY USAGE 链均未碰（审头§五防扩面）；未来目标内存强制落地时 tracker 与堆行一并复活。
- 残余风险：负断言为 contains 形，若未来出行改多段/复合格式可能弱化锁力（现单值出形无此风险）。

## 主控验票注记（2026-09-28 r437 收口席）

一、收口经过与归属
- 合入 2d78491（merge: ...heap-row-aliased-to-ring-bytes），归档 5fc114b（纯改名，零内容改动）。
  本波合入与改名由并发席按其门禁窗顺次落笔（他席 16:44 起在主树跑 cargo fix --all-targets
  --all-features，索引与构建锁为其持有，我方全程只读等待）。
- 归属确认：本票为我方 r437 阶段一立案、独立审核席审结（裁 a 路）、施工席执行的票，
  终态注记系施工席自写，本节为收口复核。

二、主控独立双验（不采信席报）
1. 净面等值：对席枝尖 e1dfa1c 与 dev 现树逐档双参比对，garnet_api/mod.rs、resp_info.rs、
   deviations.md 三面零差（既无 hook 补 fmt 漂，也无他档夹带）；merge stat 4 文件 +97/-5
   与审结方案文件面全等。
2. 消费面独立复扫：全仓 grep CurrentHeapSizeBytes / log_heap_size_bytes / heap_size_bytes，
   除 garnet_api 投影两行、garnet_info_metrics 结构字段与 :595/:1126 两出行、resp_info 夹具
   外零命中 ⇒ 「不入 store_* 求和、wmetric 零 diff 即记账无污染」的自证成立。
3. 台账面：§183 全册唯一命中；所引先例 §165 正文确在册（标题形为
   「### [§165（含 a/b/c 分目）]」，裸号 grep 方可见，勿以带尾注形检索）；
   空缺位 §173/§177/§179/§180 未被复用；来源指针指向本 done 路径，无死指针。
4. 禁触线复核：未碰 waof/aof 投影邻码、未碰 wmetric gc_*/MEM_SOURCE_PAIRS 注释、
   未碰 tiered 域（同批姊妹票射程）。

三、审头订正入册（本案两处失实锚，均已由审核席一手纠正并落票）
- 「夹具即按不同值填写」失实：resp_info 原夹具两堆字段与内存行同值，复刻的正是别名恒等形；
  施工按真形改 0 并加负断言，方向已由本票落地。
- 「ObjectAllocatorImpl.cs:1382-1393 系写入侧递增」失实：该段为检查点恢复侧补账
  （TrackRecoveredObjectRecord），常规递增走 logSizeTracker.UpdateSize/页闭合路径。
  两订正均不改裁 a 结论。

四、门禁
- 本波两票合入后单轮主控门禁已跑毕（dev 尖 eeda83e）：./test.sh --no-fail-fast
  5198 tests run / 5198 passed / 1 skipped / EXIT=0（Summary 148.295s），
  ./sh/clippy.sh 三组 EXIT=0，bun js/check.js EXIT=0；本票新断言（堆两行出 0 且与同行
  内存行非恒等）实证落绿。全量数字与残余非阻断警告归属见
  task/done/r437-r438-gate-record-20260928.md。
