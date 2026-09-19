reviv / 冷读晋升族旋钮生产零装配：wreviv 复活池与记录脱钩整面不可达，ignore 声明与代码相反

来源：qw.db.md 第 11 轮条 1（HIGH），本票为该条的细化件（原票文曾拆为
next/reviv-knobs-zero-production-wiring.md，已由分拣波 git mv 并入本路径，票文由下列取证内容取代）。
验尸：git branch --list 与 ls /tmp/fork 无 reviv 相关分支/工作树，非在途。

现状（rust）
- wedb/wkv/src/config.rs:210 字段 enable_revivification；:349 默认 false；:470 with_revivification 为唯一置真入口。全仓生产命中仅 wedb/wkv/src/store/cpr_host.rs:410 WedbStore::open_recovered（读回上次会话 StoreMeta），新建库无任何 CLI/CONFIG/嵌入式入口置真，故 wedb/wreviv 复活池在进程内恒关。
- wedb/wkv/src/config.rs:219 revivifiable_fraction（:350 取 DEFAULT_REVIVIFIABLE_FRACTION，:420-427 validate 区间校验）唯一写侧 :477 with_revivifiable_fraction，调用点全在 wedb/wkv/tests/store/reviv.rs:336；读者 wedb/wkv/src/session/raw/mod.rs:116-127 RawSession 复活下限窗口恒用默认值。
- copy_reads_to_tail 只存在于会话位：wedb/wkv/src/session/mod.rs:134 字段、:160 new_session 硬编码 AtomicBool::new(false)、:410 set_copy_reads_to_tail、:416 getter；生产零调用（仅 wkv/tests/store/reviv.rs:49/:59），唯一读者 wedb/wkv/src/session/raw/read.rs:709 冷读晋升臂恒假。
- record_elision 同类：wedb/wkv/src/session/mod.rs:135/:161/:422/:428，其 doc 自陈「rust 自有开关；C# RevivificationSettings 无对应剔除标志」；唯一读者 wedb/wkv/src/session/raw/write/inplace.rs:477 脱钩臂（:470-491），生产恒假 ⇒ 链首死记录清链与槽位入池（:483-485 又叠一道 enable_revivification）在生产形态双重不可达。
- 连带：wedb/wkv/src/session/mod.rs:438 ephemeral_lock_enabled = enable_revivification || record_elision，两恒假 ⇒ traceback 前 ephemeral 桶锁（C# BasicSessionLocker 无条件）在生产永不开启。
- 装配单点缺口：wconf 全域 grep reviv / copy_reads 零命中；投影链是 wedb/wconf/src/node_options.rs:133 HlogProjection + :155 HlogOptions::validated → wedb/wnode/src/service.rs:709 store_config_from_node → :676 apply_hlog_overrides（现仅 page_size / memory_size / mutable_fraction / read_cache / read_cache_memory_size）。
- ignore 面失真：js/check/ignore/hosting.yml:58-59 写「reviv 系 8 项（wreviv 自由记录池常开无开关）」，与代码两处相反（非常开：默认 false 且无开启路径；非无开关：config.rs:210/:219 就是两个开关，只是无人投影）。js/check/ignore/storage.yml:1770-1774 称复活配置参数经 StoreConfig 承接，实际承接面没有上游读者。

C# 对位
- garnet/libs/host/Configuration/Options.cs:128 CopyReadsToTail；:545 reviv-bin-record-sizes、:550 reviv-bin-record-counts、:559 reviv-fraction、:564-567 reviv（EnableRevivification）、:570 reviv-search-next-higher-bins、:576 reviv-bin-best-fit-scan-limit、:584 reviv-in-chain-only。
- garnet/libs/server/Servers/GarnetServerOptions.cs:530 RevivifiableFraction；:899-900 CopyReadsToTail 置 kvSettings.ReadCopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog)；:912/:924 投影 RevivifiableFraction 进 kvSettings。
- 脱钩无门控：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:83-89 CanElide（判据只有链头/前驱/IsFrozen，无配置谓词）、Helpers.cs:254 HandleRecordElision；调用点 InternalDelete.cs:133-134、InternalRMW.cs:175-176/:435、InternalUpsert.cs:315。

需求依据（.agents/skills/transpile/SKILL.md）
- 「尽量 1:1 对标 c#，不要实现自己的优化（如果有，也撤销）」：record_elision 这道 rust 自有门须撤，脱钩回到 C# 无条件语义。
- 「缺失的函数，不单单是要实现函数本身，更要打通上下游的调用链路，杜绝写死函数」：引擎面已转写但写侧缺装配，属断链而非可选优化。
- 「严禁在代码中写占位函数或虚设实现」与「ignore 须写清为何无需实现」：整块不可达引擎面 + 与代码相反的 ignore 措辞二者都违此条。
- 不做向下兼容：enable_revivification 的落盘/恢复读回面（cpr_host.rs:410）保留为恢复自洽，不为其另造兼容分支。

修法
- wedb/wconf/src/node_options.rs HlogOptions/HlogProjection 增 reviv: bool、reviv_fraction: Option<f64>、copy_reads_to_tail: bool，校验口径对标 C#（reviv-fraction ∈ (0,1]，与 config.rs:420-427 的 mutable_fraction 区间校验串联，勿在下游重校）。
- wedb/wkv/src/config.rs 增 store 级 copy_reads_to_tail（对标 C# kvSettings.ReadCopyOptions 是 store 配置而非会话私有），with_copy_reads_to_tail 与既有 with_read_cache 同族；:435-437 文档注明门控真源在 StoreConfig 一处。
- wedb/wnode/src/service.rs:676 apply_hlog_overrides 单点投影三旋钮（reviv → with_revivification，reviv_fraction → with_revivifiable_fraction 走 validate，copy_reads_to_tail → 新 store 字段）；不在 service 之外开第二处投影。
- wedb/wkv/src/session/mod.rs:160 new_session 的 copy_reads_to_tail 初值改读 store.config（消除恒 false），:410/:416 会话 setter 若仅测试域用则收进 cfg(test) 或删，避免与 store 真源并列两套。
- 撤除 record_elision 开关（session/mod.rs:135/:161/:422/:428）与 inplace.rs:477 的门控谓词，脱钩按 C# CanElide 无条件执行；inplace.rs:483 的 reviv_pool.put 仍按 enable_revivification 分流（C# 同口径：清链 tidy 与入池回收是两件事，见 HandleRecordElision 内部分支）。
- session/mod.rs:438 ephemeral_lock_enabled 随之收敛为单一谓词（复活开启即需桶锁），并在注释写明与 C# 无条件加锁的等效裁剪边界。
- 订正 js/check/ignore/hosting.yml:58-59：已投影项删出清单，未转写的分桶细化项（reviv-bin-record-sizes/counts、search-next-higher-bins、best-fit-scan-limit、in-chain-only）按实际理由逐条写名，禁止以「常开无开关」这类与代码相反的措辞充数；storage.yml:1770-1774 理由同步。
- 若落地评估认为 wedb/wreviv 分桶形态与 C# 差异过大而只接 reviv + reviv-fraction + copy-reads-to-tail 三旋钮，须在 ignore 措辞里体现，不得留第四态（开关在场但无人写）。

联动
- next/zero-consumer-dead-surfaces-batch-six.md 的 with_revivifiable_fraction 子项由本票承接，该批落地时跳过勿双写。
- ing/wreviv-test-record-alignment-second-source.md、ing/info-resetstat-gossip-reviv-reset-arms.md 是另两面（测试夹具口径、INFO 复位臂），不重叠。
- reject/reviv-pause-epoch-drain-dup.md 已裁定 Tsavorite.cs 的 Pause/ResumeRevivification 门面不投影，本票不得借道复活该门面。
- 脱钩无条件化会牵动 ephemeral 桶锁与 RMW 同键互斥窗口，执行序排在 ing/string-rmw-key-bucket-lock.md 与 ing/rmw-atomic-read-modify-write-window.md 之后，两票同向落。

验收判据
- grep with_revivification / with_revivifiable_fraction / copy_reads_to_tail 在 wedb/wnode/src/service.rs 与 wedb/wconf/src 有生产命中，wkv/tests 之外不再出现唯一写侧。
- 新建库经 NodeArgs hlog 段开 reviv 后，复活池 take/put 路径实际可达（wkv 集成测试断言槽位复用，而非靠 set_* 手开）。
- 默认装配（不开 reviv）下 DEL/RMW 链首死记录仍执行清链脱钩（C# 无条件语义），原 record_elision 测试改由无条件路径覆盖。
- copy_reads_to_tail 打开时冷读回填晋升 Tail、关闭时不晋升，两条臂均由 store 配置驱动。
- ./js/check.js 无新增缺失项，hosting.yml/storage.yml 措辞与代码事实一致。
- 无第二套真源：会话内不再有独立于 StoreConfig 的晋升/脱钩开关位。

优先级：高（整块引擎面写侧零装配的死码 + ignore 与代码相反的污染扩散，功能缺口居次）。
