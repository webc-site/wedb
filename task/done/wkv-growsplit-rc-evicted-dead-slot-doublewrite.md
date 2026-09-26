甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P1
核验记录：C# 亲验——IndexResizeSMTask.cs :52 先翻 resizeInfo.version、:75-76 后 SplitAllBuckets（时序亲见）；HashBucketEntry.cs:43-46 Address 回 word & kAddressBitMask（含 RC 指示位→SplitIndex 滑出判定恒真、上游疏漏论证成立）；ReadCache 清洗活跃表单面契约锚在案。rust 亲验——resize.rs:276-278 skip_read_cache(addr).unwrap_or(0) 折 0 与「全量迁移兜底」注释自相矛盾现读亲见；split.rs:122-126 else 臂死地址原样双写左右子桶；append.rs pump_close_barrier 以 `index: &Arc<HashIndex>` 参数捕获注册时表、close_pending_page 单表清洗；cleanse.rs:77-80 evict_chain find_tag 落空即 return；全 wkv read_cache 面 grep old_index 零使用——恢复面单表缺口现码仍在，无修复合入；审核订正版（取 resize.old_index 而非执行时活跃表，原方案 1 证伪）与 PrepareGrow bump_and_wait 收割时序相符。查重：deviations 仅尺寸族三面不同轴；四池同轴零命中（todo 另票 growwindow 系紧缩探查轴、reject/done 无撞面）。架构：双表清洗幂等（同表判等+evict_chain 收敛）、非扩容期恒 None 零开销、控制面动作不渗数据面，单机制合规。格式：纯文本、双侧齐全。定级 P1：驱逐×迁移竞态致被逐页内键持续 LockTimeout 读不可用（显式写可自愈、无数据丢失），危害窗为常态交叠非边角。

审核结论：通过（审核席 zcode-r20-review-rcdouble，2026-09-26）

双侧源码亲验属实：resize.rs:277-278 unwrap_or(0) 折 0 后 0 < head_addr 恒返 None、split.rs:122-126 else 分支死 RC 地址原样双写左右子桶、append.rs:221-242 清洗闭包仅持注册时 load_full 快照表、cleanse.rs:77-80 find_tag 落空即 return、read.rs:295-301 Retry 预算尽上抛 LockTimeout、find.rs:199-202 死槽惰性清退显式豁免 RC 条目（确认无读路径自愈，唯一自愈为显式写覆盖槽位）。C# 对位亲验：IndexResizeSMTask.cs IN_PROGRESS_GROW 先切 resizeInfo.version 后 SplitAllBuckets、TsavoriteBase.cs:226-229 FindTag 用活跃表、ReadCache.cs:224 FindTag 落空 goto NextRecord 同构、HashBucketEntry.cs:45-46 Address 返回 word & kAddressBitMask 含第 47 位与 readcacheBase.HeadAddress 纯地址比较恒真（kAbsoluteAddressBitMask 存在于 LogAddress.cs:19 佐证本意为绝对地址比较）。查重干净：deviations 全册仅尺寸族三面、issue/todo/ing/reject 各池无同轴。

危害窗口精确条件（审核补充论证）：清洗延迟动作执行时刻早于死 X 所在分块迁移完成即成害；清洗由纪元排空收割（毫秒级）而迁移秒级，成害为常态路径非边角。反向窗口（迁移先于清洗完成）存在自愈：闭包持 B、死 X 已双写入 B，cleanse_page(P, B) 遍历被驱逐页 P 记录时 find_tag(B) 命中死 X 且 X 地址落本页区间（entry_abs >= page_start_addr），槽位 CAS 恢复为主日志地址。P 清零后信息全失，修复点必须在清洗执行体，别处无解。

执行方案修正（原方案 1「执行体入口再取 store 当前活跃表」无效）：切表前武装的清洗已被 grow_index 步骤 1b bump_and_wait 强制收割，PrepareGrow 拦阻期无新武装，「注册时旧表、执行时新表」错位窗不存在；危害场景恒为「闭包持新表 B、死槽位在旧表 A」，执行体再取当前活跃表仍得 B。恢复面必须取 resize.old_index（迁移源表）方能覆盖 A。

精炼执行方案（修正版）：
1 pump_close_barrier 增参 old_index: Option<Arc<HashIndex>>，两个泵调用点（read.rs:promote_immutable_read_hit 与 read_from_disk 回填臂）从 store.resize.old_index.load_full() 取注册时快照随闭包捕获；close_pending_page 对捕获双表各跑一遍 cleanse_page（同表判等跳过，evict_chain 幂等收敛）。非扩容期恒 None 零开销，扩容期清洗多一遍旧表恢复为控制面动作不渗数据面。迁移窗口内旧表槽位恢复为主日志地址后，迁移读到恒为活地址或主日志地址，死条目源头消除。
2 订正 resize.rs get_record_hash_and_prev 注释「全量迁移兜底」的失实表述（双写并非兜底而是死条目落库），并在方案 1 落地后确认该臂对滑出 RC 条目不再可达（旧表槽位已被恢复，skip 恒成功）；保留 unwrap_or(0) 作为防御残臂并注明可达性论证。
3 测试验证点：新增扩容×驱逐竞态用例（RC 启用 + 2 页小环形 + 大索引触发 grow + 迁移期间并发读晋升灌满环形触发回绕），断言迁移完成后新表全部槽位经 skip_read_cache 可解析（无死 RC 地址）、被驱逐页内键读返回正常值非 LockTimeout；回归既有 rc_tag/rc_eviction_ckpt/roundtrip 与 resize 套件。

grow 扩容迁移遇已驱逐滑出的 ReadCache 条目折 0 双写死 RC 地址入新表，该键后续读永久 Retry 耗尽预算报 LockTimeout

问题分析：
1. Garnet 契约对齐。C# 驱逐清洗契约（ReadCache.cs:ReadCacheEvict/ReadCacheEvictChain）要求被驱逐段全部哈希链引用经 FindTag 命中后恢复（槽位 CAS 或高位缝合），而 FindTag 只查当前活跃表（TsavoriteBase.cs:226 用 resizeInfo.version）；读侧契约（ReadCache.cs:ReadCacheNeedToWaitForEviction + ReadCache.cs:73-80 走查不变式注释）明定滑出 RC 条目必须等待清洗恢复后回链头重探，索引槽位绝不应残留指向已清零页的死 RC 地址。C# 扩容时序（IndexResizeSMTask.cs:52 先切 resizeInfo.version、:76 后 SplitAllBuckets）决定迁移源是旧表、清洗恢复面是活跃新表。C# SplitIndex.cs:128-131 对 RC 条目的滑出判定 entry.Address >= readcacheBase.HeadAddress 因 entry.Address 含第 47 位 RC 指示位（HashBucketEntry.cs:43-50 返回 word & kAddressBitMask）恒为真，滑出 RC 条目照样 CreateLogRecord 读已清零页——此为上游实现疏漏（本意应为 AbsoluteAddress 比较），产出错桶死条目，非 rust 应镜像的正确契约。
2. 工程现状确证。rust 迁移定位器 wedb/wkv/src/store/resize.rs:split_single_chunk::get_record_hash_and_prev 以 skip_read_cache(addr).unwrap_or(0) 处理 RC 条目：记录滑出环形窗口（abs < rc.head）时 skip_read_cache 返 None 折 0，0 < 主日志 head_addr 恒成立故 record_locator 返 None，落入 wedb/windex/src/split.rs:split_single_bucket 的 else 分支把原样含 RC 位的死地址双写进新表左右子桶（注释宣称「迁移分块保守跳过，全量迁移兜底」，但全量迁移就是这些分块本身，None 条目双写后无任何后续修复通道，兜底注释与实际语义不符）。而清洗执行体 wedb/wkv/src/read_cache/append.rs:close_pending_page 只对泵注册时闭包捕获的活跃表跑 cleanse_page/evict_chain（pump 调用点 read.rs:promote_immutable_read_hit 与 read_from_disk 回填臂各传 load_full 快照）：grow_index 在 PrepareGrow 相位 bump_and_wait（resize.rs 步骤 1b）已把切表前武装的清洗全部收割（用旧表恢复，正确），但切表后迁移期间新武装的清洗闭包持新表 B，evict_chain 的 find_tag_entry_by_hash_with_min_addr 在 B 中查找——被驱逐记录 X 所在分块尚未迁移时 B 无此键，查找落空即 return（cleanse.rs:77-80），旧表 A 槽位无人恢复；迁移随后读到 A 槽位的死 X，双写进 B。危害链：读者 find_tag(B) 命中死 X → find_in_read_cache 首步 need_to_wait_for_eviction(X) 因 abs < rc.head 自旋（closed_until 已发布即快速解除）返 Retry，或 with_record(X) 判 Gone 返 Retry，drive_mem_read 重试环预算尽上抛 Error::Index(LockTimeout)（read.rs:299-301），该键每次读都报错，直至用户显式写入（写路径链头 CAS 覆盖槽位）才自愈。
3. 逻辑危害确证。触发时序：ReadCache 启用 + 索引扩容迁移进行中（秒级窗口）+ 读晋升持续推满环形触发回绕驱逐 + 被驱逐页记录的迁移分块晚于清洗执行。RC 默认 64 页环形在读密集负载下回绕频繁，大索引迁移耗时秒级，交错概率非边角；每次命中影响被驱逐页内多条键的读可用性（持续错误应答）。C# 同位上游缺陷形态为假 NOTFOUND 或错桶（恒真比较掩盖滑出判定），rust 形态为持续 LockTimeout，两侧皆违背「槽位不残留死 RC 地址」的自有契约（window.rs RcVisit 注释、wcompact/host.rs:110-112 端口文档均明定滑出条目必须等待清洗、绝不断链降级）。

涉及代码：
rust 文件与函数：
wedb/wkv/src/store/resize.rs:WedbStore::split_single_chunk::get_record_hash_and_prev
wedb/windex/src/split.rs:split_single_bucket（else 双写分支）
wedb/wkv/src/read_cache/append.rs:ReadCache::close_pending_page（清洗表句柄仅闭包捕获注册时活跃表）
wedb/wkv/src/read_cache/cleanse.rs:ReadCache::evict_chain（find_tag 落空即 return，恢复面单表）
wedb/wkv/src/session/raw/read.rs:StoreSession::find_in_read_cache 与 drive_mem_read（Retry 预算尽 LockTimeout）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexResizeSMTask.cs（切表先于 SplitAllBuckets 的时序）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs:SplitSingleBucket（entry.Address 含 RC 位与 HeadAddress 比较恒真，上游疏漏对照）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:ReadCacheEvict 与 ReadCacheEvictChain（清洗恢复活跃表单面契约）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:ReadCacheNeedToWaitForEviction（滑出等待恢复契约）

（原精炼执行方案已被顶部修正版取代删除：原方案 1「执行体入口再取 store 当前活跃表」经审核证伪——危害场景下当前活跃表与闭包捕获表同为新表 B，无法覆盖旧表 A 槽位，正确取表来源为 resize.old_index。）
合入哈希：31ca6ee 收口形态：pump_close_barrier 注册期并捕 resize.old_index 迁移源快照，close_pending_page 双表幂等并洗（同表判等跳过），迁移分块 locator 恒取活/主日志地址杜绝死 RC 双写；确定性回绕恢复主证＋单表残死对照＋真 grow×并发读晋升压级回绕三测落地，resize/rc/read_cache/ckpt 回归与 wkv 全包全绿。