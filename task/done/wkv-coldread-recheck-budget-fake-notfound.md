甄别结论：通过（甄别席 zc-fix-r16-coldread，2026-09-26）定级 P2
核验记录（逐锚现码复跑，非票面背书）：
1 rust 走尽臂成立：read.rs:898 MAX_DISK_RECHECKS=16；:1007-1015 Retry 预算尽分支穿透至 :1019 return Ok(None) 丢弃 new_cands；命中臂 :946-956 预算尽回落实读磁盘记录（:958-1002）；Retry 判据 :853-873（:863-867 链头不在已检候选集）逐行亲验属实。
2 c# 侧成立：ContinuePending.cs while(true) :46、重发臂 :83-123（do/while HandleImmediateRetryStatus :103-108 无预算上限，:78-82 注释自陈以新链头为 minAddress 重发收敛）、NOTFOUND 出口仅 :37/:41/:68/:135-142/:205-206，确无「候选仍在而报不存在」产出形态。
3 先例成立（一处措辞出入）：read.rs:299-300、rmw_window.rs:479-497/:549-558、write/inplace.rs:23-24 预算尽上抛 Error::Index(WindexError::LockTimeout) 亲验；ttl.rs:234 实为 try_lock 单次失败即 LockTimeout（非循环预算尽形），作为 LockTimeout 透明通道先例仍成立，不动摇论断；走尽臂零缺席证据断言不存在确为全仓孤例吞错。
4 非重复非灭失成立：deviations.md 全册 grep 冷读/复检/ContinuePendingRead/RECHECK 仅命中 PFADD/HINCRBYFLOAT 无关「复检」字样；task/ing 空、reject/issue 无同轴票（r16-wkv 审查史即本源票、todo 内 flushall 票仅引 read_from_disk 纪元挂起面非本行为面）；缺陷现码仍在（:1019 原样）。
5 batch 复用成立：batch.rs:196-208 磁盘收割臂经 ? 复用 read_from_disk，注释明载「任一磁盘读失败即刻上抛中止交付」既有契约；错误面 -ERR slow path storage error 有 wnode/tests/tiered_cmds_align.rs:2132 等回归锁定；set_store_cold_window_ttl_selfheal.rs 存在。
6 架构与格式合规：复用既有 LockTimeout 通道零新增变体、命中臂与常态路径零改动、单机制无过度设计无假桩；订正 1（线性化定性收窄为契约分叉+吞错纪律）与订正 2（碰撞键推进 16 轮的测试构桩形）合理必要；纯文本、双侧路径齐全。

审核结论：通过，定级 P2（审核席 zcode-r16-review-coldread 独立双侧亲验）。

事实核验（全部现码对位确认）：
1 rust 侧 read.rs:898 MAX_DISK_RECHECKS=16、走尽臂 :1006-1019 第 16 次 Retry 落入 return Ok(None) 丢弃 new_cands、命中臂 :946-956 预算尽回落返回已读磁盘记录（有实读证据、线性化合法）、Retry 判据 :853-873（槽链头滑入磁盘区且不在已检候选集）——票面引用行号全部属实。
2 c# 侧 ContinuePending.cs 亲读：while(true) :46、重发臂 :83-123 do/while HandleImmediateRetryStatus 无预算上限；NOTFOUND 出口仅 :37/:41（真实走尽低于 BeginAddress/minAddress）、:68/:135-142（内存或磁盘墓碑）、:205-206——确无等价复检预算，确不产出「候选仍在而报不存在」。
3 仓内先例对照：同文件 drive_mem_read:299-301（INNER_LATCH_RETRY_BUDGET 尽）、rmw_window.rs:496/:557（RMW_LATCH_YIELD_BUDGET 尽）、raw/write/inplace.rs:24、ttl.rs:234 五处预算尽一律上抛 Error::Index(WindexError::LockTimeout)，客户端面回 -ERR slow path storage error（可重试语义有 wnode/tests/set_store_cold_window_ttl_selfheal.rs 回归锁定）——走尽臂是全仓唯一预算尽静默吞错的孤例。
4 deviations.md 全册无此取舍登记（grep 冷读/复检/ContinuePendingRead/RECHECK 仅命中无关工单名）——非已裁决偏差。
5 batch.rs:196-208 复用 read_from_disk 确证；错误上抛中止本批交付系方法注释明载既有契约，read_tag_with 一致读同链确证。

两处订正（供 fix.md 消费，防执行与测试走偏）：
1 线性化定性修正：可达形态中被丢的活记录均系读窗口期内新写（T0 时键缺席，初始候选扫描必含 T0 已在盘记录、首轮即命中臂），故 Ok(None) 严格说可线性化于 T0，票面「既非任何合法线性化」过强；缺陷正身是走尽臂预算尽分支在零缺席证据下断言不存在（最后一次观测恰是未读新候选）、与 C# minAddress 单调收敛终答分叉、静默吞复检结果违板块 4.1、与仓内五处预算尽先例不一致。定级维持 P2，修复依据为契约分叉与吞错纪律而非实时序违例。
2 触发机理修正：该键自身的写入一旦在复检前滑入磁盘区，下一轮走链必命中返回 Done，故 16 轮循环由同槽 tag 碰撞键的持续写推进（每个磁盘 I/O 窗口内槽头前移到新磁盘地址），该键自身写入只在末轮窗口落地被吞。单测桩须按此形态构造（每轮推进 head 制造碰撞重试，末轮窗口才落该键记录），勿按票面「每轮写该键」原描述驱动（该形态下循环会提前收敛，测不出预算尽路径）。

整理执行方案（最小单机制，命中臂与常态路径零变化）：
1 走尽臂 MemRecheck::Retry 预算尽分支改 return Err(Error::Index(WindexError::LockTimeout))，注释锚 ContinuePending.cs:83-123 重发收敛（C# 无预算因 minAddress 收敛保证终局，rust 复检无收窄机制故需预算，预算尽不得断言缺席）与同文件 :299-301 先例；命中臂不动（回落值有实读证据，注释锚定该合法线性化依据防后人误改）。
2 复用既有 LockTimeout 错误通道，不新增错误变体（五处先例同源，客户端既有 -ERR slow path storage error 可重试应答零新增面）。
3 测试：受控 Device 桩按订正 2 形态驱动 16 轮碰撞 Retry + 末轮落该键记录，断言得 LockTimeout 而非 Ok(None)；回归 wkv/tests 冷读晋升臂确认常态路径零变化；batch 收割臂错误中止契约回归一次。

磁盘冷读走尽臂复检预算耗尽后静默返回假 NOTFOUND（活键误报不存在），应上抛可重试错误对齐仓内预算耗尽先例

问题分析：
1 Garnet 契约对齐。C# 磁盘冷读完成后的并发写复检在 ContinuePendingRead 的 while(true) 收敛循环内：内存未找到记录而链头地址高于 initialLatestLogicalAddress（即等待磁盘 I/O 期间该键有新写入且已滑入磁盘区）时，重发 InternalRead 并推进 minAddress 单调收敛（ContinuePending.cs:83-123，do/while HandleImmediateRetryStatus 无预算上限），NOTFOUND 只在真实走尽（request.logicalAddress < BeginAddress/minAddress，:37/:41）或命中墓碑（:68/:135-142）时返回——C# 任何复检轮次都不产出「候选仍在而报不存在」的假 NOTFOUND。
2 工程现状确证。rust 侧 wedb/wkv/src/session/raw/read.rs:read_from_disk 以 recheck_memory_concurrent_write 复检，命中臂（:946-956）预算耗尽后回落返回磁盘值（线性化合法、无害）；但走尽臂（:1006-1019）在 rechecks 达到 MAX_DISK_RECHECKS=16（:898）后落入 return Ok(None)，而此刻 MemRecheck::Retry 携带的 new_cands 正是「复检中新发现的该键磁盘候选」——键实际存活于新候选链上，却对客户端回 NOTFOUND。该臂静默吞掉复检结果：既非任何合法线性化（键在存储中存在，任何线性化点都不能报告不存在），也违背仓内预算耗尽先例（同文件 drive_mem_read 的 INNER_LATCH_RETRY_BUDGET、rmw_window.rs 的 RMW_LATCH_YIELD_BUDGET 耗尽均上抛 Error::Index(WindexError::LockTimeout) 可重试错误），deviations.md 全册无此取舍登记。
3 逻辑危害确证。触发形态：持续高写入压使环形缓冲快速翻转（MIN 配置 16 页 x 64KB 即 1MB 环），读冷键期间每轮复检窗口内该键恰好又被并发写入并被驱逐出内存（新记录地址入磁盘区且不在原候选集），连续 16 轮后 GET/MGET 返回 nil 而键实际存在；批量读 read_batch_raw_with 复用本内核同染。属瞬态假缺失（下一次读收敛），但对副本一致读（ConsistentReadContext::read 经 read_tag_with 同链）与依赖存在性的上层逻辑构成静默数据不可见面，且吞错形态与板块 4.1「底层存储错误透明转发，禁止静默吞错」直接冲突。

涉及代码：
rust 文件与函数：
wedb/wkv/src/session/raw/read.rs:StoreSession::read_from_disk（MAX_DISK_RECHECKS :898；走尽臂预算尽 return Ok(None) :1007-1019；对照命中臂 :946-956）
wedb/wkv/src/session/raw/read.rs:StoreSession::recheck_memory_concurrent_write（Retry 判据 :853-873）
wedb/wkv/src/session/raw/batch.rs:StoreSession::read_batch_raw_with（磁盘收割臂复用 read_from_disk 同染 :196-208）

对应 c# 文件与函数：
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ContinuePending.cs:ContinuePendingRead（收敛循环 :46/:83-123；NotFound 真实出口 :37/:41/:68/:135-142/:205-206）

精炼执行方案：
1 走尽臂 MemRecheck::Retry 预算耗尽分支改上抛 Error::Index(WindexError::LockTimeout)（与命中臂保留「返回磁盘旧值」的合法线性化区分：命中臂不动，只改走尽臂），注释锚 ContinuePending.cs 重发收敛循环与仓内 INNER_LATCH_RETRY_BUDGET 先例
2 同步审读命中臂预算尽回落路径，确认回落值在磁盘命中帧非墓碑时闭环（现码已正确，仅注释锚定该合法线性化依据，防后人误改）
3 测试验证点：单测驱动 read_from_disk 注入 16 轮假 Retry（以受控 Device 桩在每轮磁盘读后推进 head 越过新写入记录），断言预算尽得 LockTimeout 错误而非 Ok(None)；回归既有冷读测试（wkv/tests 冷读晋升臂）确认常态路径零变化
合入哈希：d33336d 收口形态：走尽臂 Retry 预算尽改上抛既有 LockTimeout 可重试通道，命中臂与真实走尽出口零改动，双定向测试锁死
