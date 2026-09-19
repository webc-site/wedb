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

核销（二棒收口，2026-09-19）
落地载体：dev 1905e8a（棒 1 + 棒 2 全部载荷）与 dev b003b3b（跨包夹具连带与 rustfmt）。

一 覆盖度（对照本票修法清单）
- 棒 1 链内复活双门：已落。下限推导收成 store 单点 `WedbStore::min_revivifiable_address()`
  （wkv/src/store/addr.rs，对标 Helpers.cs:106 `GetMinRevivifiableAddress`），raw/mod.rs 池取
  与 inplace.rs:225 链内臂一律直调；inplace.rs 原 `config.enable_revivification` 直判已删，
  改判 `reviv_pool.is_enabled() && cur >= 下限`，两道门并列的形态已消除。whlog/wrecord 零新增
  谓词、whlog 侧未复抄公式（一棒未触 whlog）。
  为让 `is_enabled()` 真正等价 C# `IsEnabled`，未启用位折进池挂起计数初值
  （wreviv/src/pool.rs `SUSPEND_DISABLED = -1`，`FreeRecordPool::new` 收启用位）——此即 C#
  RevivificationManager.cs:24 初值与 :40-43 提前 return 的形态；不改此处则链内臂换判
  `is_enabled()` 后会出现「--reviv 关时反而放开原地复活」的语义倒挂，属必要前置而非扩面。
- 棒 2 冷读晋升不可变区臂：已落。`promote_immutable_to_read_cache` 改 `promote_immutable_read_hit`，
  目的地由配置单点裁决（RC 与 MainLog 二者择一，禁两臂并行），对标 InternalRead.cs:CopyFromImmutable
  的 CopyTo 分派；尾臂取 ConditionalCopyToTail(wantIO:false) 形态（直连 hlog.append，需发 I/O 即
  放弃本次），追加值出页读锁后落笔，杜绝读页读锁内嵌套尾页写锁。
  「命中即尝试晋升 → CAS 挂载 → 败帧入池」两步收成 `cas_mount_copied_frame` 单点
  （write/copy_to_tail.rs），磁盘回填臂、不可变区臂、copy_record_to_tail 内核三处共用，全库无第二份
  追加+挂链样板（本票未新增第三处）。
- 棒 3 撤 record_elision 门 + 残留 a（入池门槛口径）/ 残留 b（满桶回链）：本票未动，按票面
  「随 ing/reviv-knobs-zero-production-wiring.md 与 RMW 原子票落」执行；各入池点仍传
  read_only_address（inplace.rs:488、compact.rs:171 等），`cas_mount_copied_frame` 的败帧回收亦
  沿用该口径，与残留 a 同批改，不拆一半。
- 真源唯一性：wconf/wnode 零改动，复活语义真源仍 StoreConfig 一处 + 池暂停计数一处。

二 门禁实测
- `CARGO_TARGET_DIR=/tmp/target-fix-reviv-crtt cargo check --tests -p wreviv -p wkv -p wedb`：
  退出 0、零告警；`cargo check --workspace --all-targets` 亦干净（`FreeRecordPool::new` 签名变更的
  全仓消费点经 grep 取证仅 wkv store/mod.rs 与 wreviv 自身测试，无他 crate 构造）。
- nextest：wreviv + wkv 246/246 绿；并入 wedb 后 558/558 绿（合入前回合 dev 三次前进，每次都重跑）。
- `./js/check.js`：退出 0，输出仅存量提示段（B 层词法 129 处、重复定义族），无新增缺失项。
- 严禁清单遵守：未跑 ./test.sh、./sh/clippy.sh。

三 断言非恒真的变异校验（票面验收判据第四条）
逐项破坏生产门，确认对应用例转红且红在该门上：删链内 `cur >= min_revivifiable_address` 门 →
比例臂红；链内臂回退为 `config.enable_revivification` 单门 → 暂停臂红；不可变区臂回退为
read_cache-only（修复前形态）→ 不可变区臂红而磁盘臂仍绿；磁盘臂恒走 copy-to-tail → 磁盘臂关态
断言红。四轮验后生产件 `git checkout` 复位、取证不留痕。

四 二棒补的改动
1. 441fe54（一棒未提交的 tests/store/reviv.rs 现场，属票面射程，予以提交）：磁盘臂改
   on/off 两态对照并弃用测试旁路口 `set_copy_reads_to_tail`、改由 store 级
   `with_copy_reads_to_tail` 真源驱动；新增不可变区臂用例（Tail 推进 + 索引改指新地址 +
   二次读命中可变区且不重复晋升 / 关时零推进）。与票面棒 2 验收逐条对齐，无扩面。
2. dbe4752（修一棒 18733bd 已提交用例的一处错误前提）：`test_revivification_in_chain_dual_gate`
   暂停臂正断言恒红。实测取证 `cands=[112,160]`：pause 阶段落回尾部追加已把键 k 的链首槽位
   改写为 Active 记录，其后 append_record + index.insert 悬置的墓碑只是桶内第二候选，探针自
   链首即命中等长 Active → 走「原位更新」臂返回 112，到不了复活臂。若顺手把断言改成
   `after2 == 112` 则测的是更新臂、反成假绿，故 resume 正断言改用独立桶位干净键（链首恒为
   墓碑，与本票 275-360 既有夹具及比例臂同法），并给暂停臂/比例臂各补一条
   `min_revivifiable_address` 前置自查，坐实挡路的是被测那道门。
3. 314cf78（修本票生产改动打红的跨包连带）：`wedb/tests/cluster_migration.rs` 四条断言
   `reviv_pool.is_enabled()` 的用例转红（slots_migration_reviv_pause_guard_raii /
   slots_migration_pauses_and_resumes_reviv_pool / slots_migration_task_full_flow_success /
   slots_migration_task_batch_reject_recovers）。归因：改前挂起计数无条件以 0 起算，
   `is_enabled()` 根本不看启用位，四条用例遂以 `test_store_config()`
   （enable_revivification 默认 false，config.rs:359）建店并断言「初始即启用」为真。
   取证明知非 R4 门禁红七枚之列（那批属 aof 回放系/garnet_log/ttl_purge）。
   处置：按 C# 口径 `IsEnabled` 在 --reviv 关时恒假，暂停/恢复只在启用态才有区分度，
   故为断言暂停臂的四条补 `migrate_store_reviv` 入口，建店本体收成 `open_migrate_store`
   单点、reviv 位由调用方裁决；其余 17 处 `migrate_store` 调用点维持关态不动。
   生产语义零回退：改前 take 路径外层已判 `config.enable_revivification`，reviv 关时 take
   从不触达，与改后一致（票面「默认装配行为零变化」成立；差异只在直接探
   `is_enabled()` 的测试可见面）。
4. 96638dc：`cargo fmt` 归一本票四件（一棒三提交未过 rustfmt），纯换行重排、token 序列一致。

五 文档落位与本票刻意未改处
- hosting.yml:58-60 未改。票面第 100 行要求本票落地后把「脱钩无条件化随 RMW 原子票同批落地」
  改为已落地，但该句主语是棒 3 的脱钩无条件化，棒 3 随 ing/reviv-knobs-zero-production-wiring.md
  与 RMW 原子票落、本票未落（见上「一 覆盖度」棒 3 段）。此时改写即为假账，故原句保持，
  待棒 3 落地那一棒一并核销。
- js/check/ignore/storage.yml 未改（本票禁改清单内）。取证：本票新增锚点
  `Helpers.cs:GetMinRevivifiableAddress` 已把该 C# 符号登记为已映射，实跑 check.js 会自动摘除
  storage.yml:1643 的同名 ignore 条目，但同一次运行连带改写了 :1724 附近
  `UnlockExclusive`/`UnlockShared` 两枚他域条目（陈旧基线上的溢出桶锁表漂移，非本票射程），
  故已整份 `git checkout` 回退。请主代理在全新 dev 基线上重跑 check.js，把 GetMinRevivifiableAddress
  那条 ignore 摘除归位。
- 相邻票事实更正：`with_revivifiable_fraction` 在本票落地后已是双侧生效谓词（链内原地复活与
  池取同一门，raw/mod.rs 与 inplace.rs 两臂直调 store 单点），不再只是池取口径；
  zero-consumer-dead-surfaces-batch-six.md 现已回到 task/ing/，其 with_revivifiable_fraction
  子项与本票无交叉，本票未撤任何投影面，故不双写、不代改他票正文。