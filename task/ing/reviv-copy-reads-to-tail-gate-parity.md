优先级：中高（同族三处门控/覆盖缺口：三旋钮已能拧动，但语义未对齐 c#，其中两处全仓无票登记）

4 单问题：reviv 三旋钮（--reviv / --reviv-fraction / --copy-reads-to-tail）的配置投影链已对齐 c#
（wconf hlog 段 → wnode service.rs::apply_hlog_overrides → wkv StoreConfig），但三处落点语义与 c#
不一致：链内原地复活不认双门（暂停门 + 复活下限窗口）、冷读晋升缺「内存不可变区命中」臂、
删除脱钩仍被 rust 自有 record_elision 开关挡死。第三处已由 ing/reviv-knobs-zero-production-wiring.md
承接，前两处无票。

取证现状（2026-09-19 主代理在 HEAD=23e14541 上逐行比对 ./.garnet 得出；行号若位移，按符号重定位）

一 链内原地复活不认双门（IsEnabled + 复活下限）
- rust 唯一链内复活点 wkv/src/session/raw/write/inplace.rs:220-227：墓碑探针命中后只判
  `self.store.config.enable_revivification`，随即转 hlog.try_revivify_in_chain；本体
  whlog/src/hlog/inplace.rs:73-90 无任何比例/暂停谓词（只判墓碑 + val 容量 + MAX_FILLER_BYTES）。
- 池侧门已存在但只覆盖池取：wreviv/src/pool.rs:205-207 is_enabled（reviv_suspend_count == 0），
  消费点仅 pool.rs:267-272 take 入口；暂停的唯一生产触发是
  wedb/wedb/src/server/migration/migrate_driver/slots.rs:88-100 RevivPauseGuard（:93 pause、:100 resume）。
- 复活下限公式在 rust 只写了一份，且只在池取路径：wkv/src/session/raw/mod.rs:116-132
  （tail - (tail - read_only) × fraction，f64 舍入钳制到 [0, window]）。
- c# 双门：InternalRMW.cs:125-126 与 InternalUpsert.cs:125-126 都是
  `RevivificationManager.IsEnabled && stackCtx.recSrc.LogicalAddress >= GetMinRevivifiableAddress()`；
  IsEnabled = RevivificationManager.cs:18（revivSuspendCount == 0；EnableRevivification 为假时构造
  函数在 RevivificationManager.cs:40-43 之前 return，计数恒 -1 故恒假）；下限单点 Helpers.cs:106-107；
  暂停调用点 cluster/Server/Migration/MigrationDriver.cs:139 / :225。
- 后果两条：a) --reviv-fraction 对链内复活零效力（只有池取受限），c# 是同一谓词管两路；
  b) 迁移窗口（RevivPauseGuard 生效期间）新到的写操作仍可在链中部原地复活墓碑并挂链，
  c# 该期间落 CreateNewRecord 尾部追加。
- 与既有暂停票的分工（勿混）：reject/reviv-pause-epoch-drain-dup.md 讲的是「暂停后在途 take 写者
  未排空」的窄竞态（纪元 drain 半协议），本处是谓词根本没接线；本棒不主张排空、不碰
  Tsavorite.PauseRevivification 门面（该门面已裁定不投影）。

二 copy_reads_to_tail 缺「内存不可变区命中」臂
- rust 冷读晋升只有两条臂：磁盘冷读 read.rs:693-724（read_cache 启用则挂 RC，否则若
  copy_reads_to_tail 则 append_record_compacted 回 Tail）；内存不可变区命中 read.rs:453-455 只调
  promote_immutable_to_read_cache，而该函数 read.rs:622-625 在 read_cache 未启用时直接 return。
- 会话位只是装配期取 store 真源初值（session/mod.rs:154-161），真源在 StoreConfig
  （wkv/src/config.rs:232-239），投影单点 wnode/src/service.rs:709-721 —— 这一段已对齐，缺的是读者臂。
- c# 语义：GarnetServerOptions.cs:899-900 置 `kvSettings.ReadCopyOptions =
  new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog)`；AllImmutable 明确定义为 Device 或 immutable
  region（Index/Common/OperationOptions.cs:22-26）；InternalRead.cs:135-137 不可变区命中且 CopyFrom
  非 None 时走 CopyFromImmutable，:167-184 按 CopyTo 分派 MainLog（ConditionalCopyToTail wantIO:false）
  或 ReadCache。
- 后果：--copy-reads-to-tail 开、read-cache 关（该组合是 Garnet 主用法）时，内存不可变区
  [head, safe_read_only) 的命中不回 Tail，c# 会回；当前只有磁盘冷读那一段对齐。

三 删除脱钩仍被 rust 自有 record_elision 挡死（差异 #2，已有票）
- inplace.rs:474-491 以 `self.record_elision() && cur == addr && cur >= read_only_addr &&
  (prev == 0 || prev < begin_addr)` 门控；该位生产初值恒 false（session/mod.rs:162），setter 仅测试
  调用（tests/store/reviv.rs:91/:178/:291/:340）。
- c# Helpers.cs:254-267 无配置谓词（判据 CanElide 只有链首/前驱/IsFrozen，Helpers.cs:83-89）。
- 已登记：task/ing/reviv-knobs-zero-production-wiring.md:32（撤门）与 :41（执行序排在 RMW 原子票之后）。
  本票不双写，只把同批残留两项一并挂在此（见修法棒 3）。

修法（一棒一问题，可拆三个 ing 并发；文件域不重叠。三棒均不写兼容分支、不留占位臂、不另立第二套机制）

第 1 棒 链内复活接双门（改动最小、风险最低，先做）
- 把复活下限推导收成单点（建议 wkv store 侧一个方法，如 min_revivifiable_address()，对标
  Helpers.cs:106-107 的单点形态），raw/mod.rs:116-132 的池取路径改为直调；inplace.rs:222 的链内
  分支改为 `reviv_pool.is_enabled() && cur >= min_revivifiable_address()`，删掉原来的
  `config.enable_revivification` 直判（暂停计数为 0 时该谓词本身已蕴含启用语义，勿留两道并列）。
- 禁止在 whlog/wrecord 层新增谓词，也禁止在 whlog 侧再推一遍公式——min 地址推导需要 tail/
  read_only 水位，属 wkv store 面（与 wreviv/src/lib.rs:25-29 的既有裁决一致）。
- 验收：迁移暂停期间对「索引指向墓碑、且墓碑落在可变区」的键做 upsert，地址必须推进 Tail
  （不出现链内原地复活）；fraction 收窄后落在窗口外的墓碑同样不得原地复活；resume 后两条臂恢复。
  用例照 tests/store/reviv.rs:275-360 的现有风格写，但不得再靠测试专用旁路开关开启功能
  （存量 set_record_elision 类旁路随棒 3 一并清）。

第 2 棒 冷读晋升补齐不可变区臂（唯一目的地语义）
- 内存不可变区命中（read.rs:453）在 read_cache 未启用、且 store 级 copy_reads_to_tail 为真时，
  走与磁盘臂同一内核（append_record_compacted 免 AOF 镜像旁路 + update_address CAS，
  copy_to_tail.rs 现有骨架），不得在读路径写第二份追加+挂链样板；read_cache 启用时维持现状
  （其优先级与 c# CopyTo 单目的地语义一致：RC 与 MainLog 二者择一，禁两臂同时动作）。
- 顺手核一遍 promote_immutable_to_read_cache（read.rs:622-639）与磁盘臂 read.rs:701-708 是否可收
  成一个单点（同形「命中即尝试晋升」，目的地由配置裁决），若收益不足则保留两处但注释互指，
  不许出现第三处。
- 验收：copy_reads_to_tail 开、read-cache 关：读一条已滑入不可变区的记录后，Tail 推进且索引
  指向新地址（再次读取命中 Tail）；关时两条臂都不推进 Tail。磁盘臂两条方向同样断言，防回归。

第 3 棒 撤 record_elision 门 + 同批残留（随 ing/reviv-knobs-zero-production-wiring.md 与 RMW 原子票落）
- 按既有票撤 inplace.rs:477 的门控谓词与 session/mod.rs:132 字段/:162 初值/:427 setter/:433 getter
  开关位，脱钩按 c# CanElide 无条件执行；inplace.rs:483 的入池仍按 enable_revivification 分流（与
  c# Helpers.cs:254-278 的两件事同口径）。
- 残留 a（入池门槛）：c# TryAddToBin 用 minRevivifiableAddress 作门槛
  （FreeRecordPool.cs:520-522、:535-536），rust 各入池点一律传 read_only_address
  （inplace.rs:38/:298/:484、read.rs:722、copy_to_tail.rs:142、compact.rs:171）。随棒 1 的单点
  一并改口径，池内存量因此变紧，需在验收里断言「窗口外槽位不再入池」。
- 残留 b（满桶回链）：c# RestoreDeletedRecordsIfBinIsFull 默认 true
  （RevivificationSettings.cs:56），elide 成功但未入池时把墓碑 CAS 放回链（Helpers.cs:289-300）；
  rust pool.put 满返回 false（pool.rs:238-249）而调用点 inplace.rs:482-487 忽略返回值，槽位成孤儿。
  要求二选一并写明理由：接回链臂（对标 c#）或明确声明为不转写项并在 ignore 登记，
  不允许「调用了但返回值无人看」的现状并存。
- 注意：wreviv 的 put 满桶向上溢桶（pool.rs:238）与 take 全桶扫描（pool.rs:252-256）是已声明的
  刻意差异，不在本棒范围内，勿顺手改回 c# 的 NumberOfBinsToSearch 默认 0 语义。

验收判据（三棒共同）
- wconf/wnode 侧不再新增第二套 reviv 投影面；复活语义真源仍只有 StoreConfig 一处
  （config.rs:209-239）+ 池暂停计数一处。
- 默认装配（不开 reviv）行为零变化：不出现新谓词带来的可见行为差异。
- ./js/check.js 无新增缺失项；js/check/ignore 中 reviv 相关措辞与代码事实一致
  （本票落地后 hosting.yml:58-60 的「脱钩无条件化随 RMW 原子票同批落地」一句须去掉，改为已落地）。
- 新增用例断言不得恒真：暂停臂、fraction 臂、不可变区臂、入池门槛臂各至少一条正反双断言。

改动域：wedb/wkv/src/session/raw/**（inplace.rs、read.rs、write/copy_to_tail.rs）、
wedb/wkv/src/session/mod.rs（撤位）、wedb/wkv/src/config.rs 与 store 侧（单点方法、put 门槛口径）、
whlog 仅在被本票证明需要时触碰、wedb/wkv/tests/store/reviv.rs、js/check/ignore 相关登记。

避让：wkv/src/vdb.rs、store 层 dbmeta、wnode/src/storage/**、waof、wresp、wcol/wbftree 分层写臂
各有修红代理在跑；本票不碰 variant、不碰 ReadCache 环形本体（read_cache/ 目录）与 cleanse 协议；
in-place 写臂与 wtxn 锁表的窗口收敛另有两票
（ing/string-rmw-key-bucket-lock.md、ing/rmw-atomic-read-modify-write-window.md），棒 3 与之同向
但不得改其判据；ing/reviv-knobs-zero-production-wiring.md:38 把 with_revivifiable_fraction 子项挂给
zero-consumer-dead-surfaces-batch-six，该文件现已不在 task/ 下（路径引用失效），落地时先核实再决定
是否双写，禁止凭该行断言「已被别人接走」。