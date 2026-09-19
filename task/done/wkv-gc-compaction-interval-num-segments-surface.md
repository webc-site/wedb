GcConfig 紧缩节奏与回退段数两面失真：零生产写侧字段 + 三处互斥注释 + 三个仅测试自环常量

来源：qcode10.db 条 1、条 2 合并立项（同一结构体、同一文件、同一修复动作，拆两单必互踩）。
取证基线：主仓 HEAD f974dd1f，行号为当下实况（原快照基线 fd17e895 的旧行号已全部重取）。

现状一：compaction_interval_ms 零生产写侧，0 值语义三处注释两种互斥口径
- 唯一读点：/Users/z/git/db/wedb/wedb/wkv/src/gc.rs:563
  `if !boosting && last != 0 && now.saturating_sub(last) < cfg.compaction_interval_ms`——
  值 0 时判据恒假，实义为「不节流、GC 每轮即判紧缩」。
- 字段声明与注释：/Users/z/git/db/wedb/wedb/wkv/src/config.rs:122-123
  「日志紧缩判定间隔毫秒（默认 0：禁用后台定时判定）」，与上面实效相反；
  /Users/z/git/db/wedb/wedb/wkv/src/config.rs:104-107 结构体级 doc 又称本字段
  「统一承担」Garnet CompactionTask 频率（引擎仅此一条紧缩调度链）；
  /Users/z/git/db/wedb/wedb/wkv/src/config.rs:160-167 高水位 doc 反口
  「旁路 `compaction_interval_ms` 节流（每轮即判）」——同一个 0 值两种互斥语义，代码取后者。
  模块注释第三处：/Users/z/git/db/wedb/wedb/wkv/src/gc.rs:22。
- 默认值：/Users/z/git/db/wedb/wedb/wkv/src/config.rs:187 恒 0。
- 生产写侧为零：全仓 update_gc_config（定义
  /Users/z/git/db/wedb/wedb/wkv/src/store/gc.rs:103）仅三处调用，两处投影面都只写
  compaction_max_segments 与 compaction_type：
  /Users/z/git/db/wedb/wedb/wnode/src/service.rs:1520-1527（启动投影）、
  /Users/z/git/db/wedb/wedb/wnode/src/config_owner.rs:83-94（CONFIG SET 两条臂）。
  故本字段恒为 default 0，即「承接口径」名存实亡。
- 承诺面已被外溢：/Users/z/git/db/wedb/doc/zh/db.md:169 把
  「紧缩判定另有 compaction_interval_ms 节流」写进库级回收设计承诺。

现状二：compaction_num_segments 是给 C# 字面量造的伪旋钮，注释出处系虚构
- 常量与虚构出处：/Users/z/git/db/wedb/wedb/wkv/src/config.rs:88-89
  「对标 C# Garnet CompactionNumSegmentsToCompact 默认值」——该符号在 C# 全仓不存在
  （grep `NumSegmentsToCompact` 于 /Users/z/git/db/wedb/garnet 零命中）。
- 字段：/Users/z/git/db/wedb/wedb/wkv/src/config.rs:127-129；唯一赋值
  /Users/z/git/db/wedb/wedb/wkv/src/config.rs:189（取该常量）；唯一读点
  /Users/z/git/db/wedb/wedb/wkv/src/gc.rs:582-586（常规档回退段数，熔断档改用 max）。
- 生产写侧同样为零（投影面同现状一）。
- 自相矛盾：本仓 /Users/z/git/db/wedb/wedb/wkv/src/gc.rs:539 自己写着
  「C# numSegmentsToCompact 恒为 1 的同款保守」，与「可配置字段」并存。

现状三：三个 DEFAULT_GC_* 间隔/段数常量仅测试自环
- /Users/z/git/db/wedb/wedb/wkv/src/config.rs:74-75 DEFAULT_GC_SCAN_INTERVAL_MS = 5_000、
  :77-82 DEFAULT_GC_COMPACTION_INTERVAL_MS = 60_000、:88-89 DEFAULT_GC_NUM_SEGMENTS = 1，
  doc 均自称「内置 GC 默认…间隔/阈值」。
- 对外导出：/Users/z/git/db/wedb/wedb/wkv/src/lib.rs:17-18。
- 生产零消费：task/done/expdelscan-bg-scan-mutex.md 已按 C# 默认 -1 关掉装配期硬开，
  删掉 wnode 侧对两 INTERVAL 常量的引用；此后唯一断言方是
  /Users/z/git/db/wedb/wedb/wkv/tests/config_defaults.rs:22-24、:46-54（自环，
  且把常量称作「生产推荐值」，与 GcConfig::default() 的 0 两两不等）。
  同族 DEFAULT_GC_MAX_SEGMENTS（config.rs:84-86）不在本单射程：它有 wconf 槽位与
  两处投影，是活件（见 task/done/wkv-compaction-runtime-config-wiring-and-shift-tier.md）。

C# 参考
- 紧缩周期唯一节奏源是独立注册任务：/Users/z/git/db/wedb/garnet/libs/server/StoreWrapper.cs:967-969
  `if (serverOptions.CompactionFrequencySecs > 0 && serverOptions.CompactionType != LogCompactionType.None)`
  → RegisterAndRun(CompactionTask, CompactionTaskAsync(serverOptions.CompactionFrequencySecs))；
  周期实现 /Users/z/git/db/wedb/garnet/libs/server/StoreWrapper.cs:697。
- 该旋钮有真实配置源：/Users/z/git/db/wedb/garnet/libs/host/Configuration/Options.cs:265
  与 /Users/z/git/db/wedb/garnet/libs/host/defaults.conf:194（默认 0）。
- 回退段数在 C# 是调用点字面量而非配置项：
  /Users/z/git/db/wedb/garnet/libs/server/Databases/DatabaseManagerBase.cs:379
  `DoCompactionAsync(db, runtimeConfig.GetInt(COMPACTION_MAX_SEGMENTS), 1, ...)`
  （同句只给 maxSegments 与 compactionType 配了源，段数刻意传常量 1）；形参声明 :425；
  回退量推导 :436 `(mainStoreMaxSegments - numSegmentsToCompact) × 段长`。

修法（每面二选一，不留第三态）
1. compaction_interval_ms：
   a) 首选按 C# 口径收口：删该字段与三处「频率由本字段统一承担」的声明
      （config.rs:104-107、config.rs:79、gc.rs:22 三处声明），try_compact 去节流（与现网 0 值等效），
      紧缩节奏单点交回 GC 轮次本身；doc/zh/db.md:169 的节流措辞同步改。
   b) 若保留承接口径：wconf 补对标 CompactionFrequencySecs 的槽位 + ConfigReconcile 变体 +
      service.rs:1520 / config_owner.rs:83 两处投影，并把 0 的语义在代码与注释里取一钉死。
2. compaction_num_segments：删字段与 DEFAULT_GC_NUM_SEGMENTS，gc.rs:582-586 常规档按 C#
   以字面量 1 参与 `max - n` 推导（config.rs:127-128 与 gc.rs:532-534 的回退量 doc 随之单点化）；
   确需可调则删虚构符号名、改引 DatabaseManagerBase.cs:379 的字面量 1，并补 wconf 槽位与投影臂。
3. DEFAULT_GC_SCAN_INTERVAL_MS / DEFAULT_GC_COMPACTION_INTERVAL_MS 随 1 的取舍一并处置
   （走 a) 即删常量 + 删 lib.rs:17-18 导出 + 改判 config_defaults.rs 断言为按 C# 默认 0/-1 口径）；
   走 b) 则常量成为 wconf 槽位默认的唯一出处，测试改为断言 default == 常量 == 槽位默认。

优先级
死代码（两字段与三常量均无生产写侧/无生产读者）+ 污染扩散（三处互斥注释与一个虚构 C# 符号名
会把后续转写会话引向不存在的旋钮）。

交叉引用
- 零消费面普查批（函数/方法口）见 task/ing/zero-consumer-dead-surfaces-batch-five.md；
  本单只管 GcConfig 结构体字段与 DEFAULT_GC_* 常量，两面不重复立项。
- 既有接线事实以 task/done/wkv-compaction-runtime-config-wiring-and-shift-tier.md 与
  task/done/expdelscan-bg-scan-mutex.md 为准，本单不重开 compaction_max_segments /
  compaction_type / scan_interval_ms 三条活链。

验收
- 同一「紧缩判定节奏」事实全仓只剩一个声明点，且代码实效与该点一致（grep
  compaction_interval_ms 的注释命中数按所选分支归零或归一）。
- C# 不存在的符号名（CompactionNumSegmentsToCompact）在本仓注释里零命中。
- 走 a) 时：gc.rs 无节流分支、config.rs 无两 INTERVAL 常量、lib.rs 导出同步收缩；
  走 b) 时：CONFIG GET/SET 该旋钮与引擎实效一致，并补一条「SET 改值 → 判定节奏变化」用例。
- wkv/tests/config_defaults.rs 不再自环断言已删除的常量。

落地记录（dev 基线 a0f9dda，行号按符号重定位后实测复核；修 face 见分支提交 fc77e35）

复核结论：三条现状全部成立，无一剪除，故无 reject 追加。逐条复验证据：
- 现状一：唯一读点原样在 /Users/z/git/db/wedb/wedb/wkv/src/gc.rs:609（dev 上
  reclaim-decouple 落地方向下行号漂移，判据文本未变）；字段声明
  config.rs:123「默认 0：禁用后台定时判定」与结构体 doc config.rs:104-107「统一承担」、
  高水位 doc config.rs:160-167「旁路节流（每轮即判）」三口径互斥，代码取后者；
  模块注释第三处在 gc.rs:22；默认值 config.rs:187 恒 0。生产写侧复核为零：
  update_gc_config 定义 store/gc.rs:103，wnode 两投影点 service.rs:1529-1537（启动投影）
  与 config_owner.rs:83-93（CONFIG SET 两臂）只写 compaction_max_segments /
  compaction_type。外溢承诺面在 /Users/z/git/db/wedb/doc/zh/db.md:172（票面写 :169，行漂移）。
- 现状二：`CompactionNumSegmentsToCompact` 于 /Users/z/git/db/wedb/garnet 全仓零命中已复核，
  C# 真实形态是形参 `numSegmentsToCompact`（DatabaseManagerBase.cs:425），调用点
  DatabaseManagerBase.cs:379 恒传字面量 1（同句只给 COMPACTION_MAX_SEGMENTS 与
  COMPACTION_TYPE 配 runtimeConfig 源），回退量推导 :436；本仓矛盾点 gc.rs:539（现 :583）
  「C# numSegmentsToCompact 恒为 1 的同款保守」与可配置字段并存。
- 现状三：三常量除 config.rs 定义 + lib.rs:17-18 导出外，唯一消费者为
  wkv/tests/config_defaults.rs:22-24、:46-54 自环；DEFAULT_GC_MAX_SEGMENTS 确有 wconf
  槽位（runtime_server_options.rs:105 默认 32）与两处投影，按票面排除在本单射程外。
- 票面漏记面（同族，一并修）：README 示例四处
  wedb/README.md:154、:561 与 wedb/readme/zh.md:126、wedb/readme/en.md:126 的
  `gc.compaction_interval_ms = 300_000;`，及 wkv/tests/gc.rs 四处结构体字面量用法。
- 票面交叉引用失效：task/done/wkv-compaction-runtime-config-wiring-and-shift-tier.md 与
  task/done/expdelscan-bg-scan-mutex.md 在当前仓与 git 历史均不存在（档案被并发清理），
  其结论已按代码实测复现，不影响立项。

取舍：走 1.a)（首选，按 C# 口径收口）+ 2 的删字段支 + 3 随 1 处置。理由：C# 侧
`CompactionFrequencySecs` 只用于注册周期任务（StoreWrapper.cs:967-969），
`DoCompactionAsync` 内无第二层节流；本引擎紧缩判定已挂在物理回收轮次上（扫描循环
`tick` 与常驻 `spawn_bftree_reclaimer` 两驱动，见 gc.rs:46-50），再补 wconf 槽位即造
第二套节奏源；回退段数在 C# 本就是调用点字面量，非旋钮。

落地清单：
- 删字段 `compaction_interval_ms` / `compaction_num_segments` 及默认值两行（config.rs）
- 删 `DEFAULT_GC_SCAN_INTERVAL_MS` / `DEFAULT_GC_COMPACTION_INTERVAL_MS` /
  `DEFAULT_GC_NUM_SEGMENTS` 与 lib.rs 三项导出
- gc.rs：删 `last_compact_ms` 状态与 try_compact 节流分支（现网 0 值等效）；常规档
  回退段数取字面量 1（gc.rs:620-622），熔断档取 max 不变
- 口径单点：节奏事实正点在 gc.rs:594 try_compact doc，config.rs:92 与 gc.rs:22 改为指针；
  高水位 doc（config.rs:142-146）、GcStatsSnapshot doc（gc.rs:207-208）、
  store/gc.rs:98 热更新 doc 的「节流/间隔」措辞随之改
- doc/zh/db.md:172 承诺措辞改「与之同轮评估、不设独立周期旋钮」；README 四处示例改引
  活件 `compaction_max_segments`
- 测试：config_defaults.rs 改按 C# 默认口径断言（紧缩无周期字段）；tests/gc.rs 熔断用例
  去掉对节流的两处依赖（态 1 改判「常规单步回退不贴只读线」、态 2/3 计数改相对增量与
  熔断位判定），不再断言已删字段

验收核对（全仓 grep，task|next 历史档案除外）：`compaction_interval_ms`、
`compaction_num_segments`、`DEFAULT_GC_SCAN_INTERVAL_MS`、
`DEFAULT_GC_COMPACTION_INTERVAL_MS`、`DEFAULT_GC_NUM_SEGMENTS`、
`CompactionNumSegmentsToCompact` 六符号零命中；`cargo check --workspace --all-targets`
绿（日志 /tmp/fork/wkv-gc-surface-check3.log）。

