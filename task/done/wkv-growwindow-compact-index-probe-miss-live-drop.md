甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P0
核验记录：C# 亲验——TsavoriteCompaction.cs:45-55 紧缩逐活记录 CompactionCopyToTail（NOTFOUND 保守补拷臂，无「探针未命中即弃迁」形态）现树亲见，ConditionalCopyToTail/FindRecord 补拷链锚对位属实。rust 亲验——probe.rs:44-46 find_latest_address 直取 store.index().lookup_candidates（wcompact 全 crate grep ensure_split 零命中）；run.rs:210-213 None 臂即 tally.superseded += 1 弃迁现读亲见；compact.rs:301/:319 TTL/Etag 宿主探查直读 session.store.index.load().find_tag 未过协同门；gc/compact.rs try_compact 无 grow 相位门；对照读面 read.rs 已防（test_read_gap_hook 在位）——紧缩面为唯一漏防消费面属实，现码无修复合入。查重：deviations §9/§48 不同轴（票面 §48a 系误标已由审核订正）；四池零撞面。架构：CompactSession 端口补 ensure_split 转调 StoreSession::ensure_split_by_hash 既定协同单机制、与读面/扫描面同构，明确拒绝第二套补拷语义与相位互斥（grow 可穿插紧缩、入口门不足收口，论证成立），改动最小、测试注入钩仿既有先例闭环。格式：纯文本、双侧齐全。定级 P0：grow 迁移秒级窗内用户键成批静默丢失＋begin 截断后永久不可见（紧缩区间恰为最老数据、成批非孤例），且 TTL 静默消失，属数据丢失。

审核结论：通过（修复级，数据丢失主张全文成立；probe.rs:46-49 直查活跃表、run.rs None→superseded 三臂、mod.rs 无相位门、gc try_compact 无 grow 互斥（reclaim_inflight 仅串轮不隔 grow，无更高互斥可推翻）、is_deleted TTL/Etag 宿主探查未过门、resize begin 门危害链、读面已防对照、C# NOTFOUND 保守补拷臂全部现码亲验属实；紧缩面确为唯一漏防消费面（SCAN/KEYS 走协同、写面入口门控、hlog_scan 系诊断面）；查重零同轴属实——票面"§48a"系误标应为 §48，§9/§48 均不同轴）

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. CompactSession/CompactStore 端口新增 ensure_split(key)（wcompact 侧经 trait 暴露），wkv 实现转调 StoreSession::ensure_split_by_hash 单点（非扩容期仅一次 old_index 判空原子 load）；probe.rs find_latest_address 在 lookup_candidates 前必过，CAS 期望槽位恒来自协同后探查天然收口；不新增第二套补拷语义、不新增加锁或相位互斥。
2. wkv/compact.rs is_deleted 的 TTL(:282)/Etag(:300) 宿主探查由 index.load().find_tag 改走 session.find_tag_cooperative，同单机制零新抽象。
3. 测试验证点：仿 test_read_gap_hook 在 wcompact 探针新增确定性扩容窗注入钩（非直接复用读面钩子），锁"探针 growing 期不得直采空候选判 superseded"；补 Lookup/Scan 两档"迁移期并发紧缩活键保全 + TTL/ETag 旁路不误判孤儿"回归。

grow 扩容迁移窗内紧缩索引探查未过协同门，未迁分块活键被判 superseded 随截断静默丢失；TTL/Etag 旁路记录被误判孤儿丢弃

问题分析：
1 Garnet 契约对齐（C# 原型行为与协议约定）
C# 索引扩容 IN_PROGRESS_GROW 期活跃表已切新表，未迁分块条目仅存旧表，FindTag 只查 state[resizeInfo.version] 新表无旧表回退（TsavoriteBase.cs:226-231）。该窗口内一切索引消费面必须先过 SplitBuckets 协同（SplitIndex.cs:37-68，迁移并自旋等待本分块完成），会话读写面铁律在 InternalRead.cs:71、InternalRMW.cs:68、InternalUpsert.cs:65、InternalDelete.cs:58 入口先行协同。紧缩面的同等保障由 NOTFOUND 后的保守补拷承担：CompactLookup 逐活记录调 storebContext.CompactionCopyToTail（TsavoriteCompaction.cs:51，BasicContext.cs:491-505），CompactionConditionalCopyToTail 内 TryFindRecordInMainLogForConditionalOperation 找到链上新版才免拷（ConditionalCopyToTail.cs:105-107 返 CreateFound），FindTag 未命中（含 grow 未迁分块窗）走 NOTFOUND（FindRecord.cs:40-43）后无条件 ConditionalCopyToTail 补拷插回（ConditionalCopyToTail.cs:111-113）——上游任何索引未命中形态下紧缩都以拷贝保守保数据，绝不因探针未命中而弃迁记录。
2 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 读面对该窗口已有完整防线：read_probe / try_read_mem 入口先 ensure_split_by_hash、growing 期陈旧 None 一律重探（session/raw/read.rs:319-334、:617-625），扫描面 find_tag_cooperative 单点承接（session/mod.rs:722-726），并有 test_read_gap_hook 回归锁定「growing 期采得的陈旧 None 不得直采进内核」。但紧缩面为唯一漏防消费面：(a) wcompact probe.rs find_latest_address 于 :46-49 直调 store.index().lookup_candidates(key)，CompactStore::index 为 active_index()（wkv/compact.rs:139-141），未经协同门，未迁分块键探得候选为空返 None；(b) run.rs compact_lookup :210-213 与 compact_scan :329-331 将 None 直判「非最新版本：并发新版本已生效，安全弃迁」计 superseded，不拷贝不保留；conditional_copy_to_tail 复核臂 :487-489 同形返 Superseded；(c) compact_with_filter 全程无 is_growing 门（wcompact/compactor/mod.rs:169-241），收尾 :219 shift_begin_address 越过该记录；(d) wkv 侧 gc/compact.rs try_compact :92-156 亦无 grow 相位门，GC 常驻回收轮与 grow_index 并发无互斥；(e) WedbCompactionFunctions::is_deleted 的 TTL 宿主探查（wkv/compact.rs:281-292）与 Etag 宿主探查（:299-315）直读 session.store.index.load().find_tag，同样未过协同门。
3 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
其一，用户键批量静默丢失：grow_index InProgressGrow 迁移期大索引可达秒级（resize.rs:426-535），期间并发紧缩对未迁分块键探针 None 判 superseded 弃迁，shift_begin 推 begin 越过原记录后，旧表条目或被分裂期 begin 门过滤不迁（resize.rs split_single_chunk :270-274 现读 begin_address，紧缩推进后 begin > 记录地址即滤除）、或迁入新表成低于 begin 的悬垂槽位（读路径判缺失），键永久不可见；紧缩区间恰为最老数据、其条目恰居旧表，成批丢失非孤例。其二，TTL/Etag 旁路记录误判孤儿：is_deleted 宿主探查直查新表，宿主条目未迁即判「无主孤儿」判死丢弃，TTL 静默消失（键永不过期）、ETag 对偶校验记录丢失，均无告警。对照读面同窗已防（test_read_gap_hook 回归在案）与 C# NOTFOUND 补拷保守臂，此为移植漏防而非上游共有边界。查重：deviations.md 全册（§9 分裂 begin 过滤、§48a DBSIZE 口径均不同轴）、r16-wkv（resize 相位机单审）、r20-compaction（紧缩单审，其判净线索 7 仅裁紧缩对紧缩）、r20-readcache（grow 与 ReadCache，todo 票 wkv-growsplit-rc-evicted-dead-slot-doublewrite 域不同）、r17-wcpr（检查点对 grow，ing 票 wcpr-checkpoint-exit-double-reset-grow-cas 域不同）、task/ 各池零同轴登记。

涉及代码：
rust 文件与函数：
wedb/wcompact/src/compactor/probe.rs:LogCompactor::find_latest_address（:46-49 直查活跃表无协同）
wedb/wcompact/src/compactor/run.rs:LogCompactor::compact_lookup（:210-213 None 判 superseded）/ compact_scan（:329-331）/ conditional_copy_to_tail（:487-489）
wedb/wcompact/src/compactor/mod.rs:LogCompactor::compact_with_filter（:169-241 无 grow 门；:219 shift_begin_address）
wedb/wkv/src/compact.rs:CompactStore::index（:139-141）/ WedbCompactionFunctions::is_deleted（:281-292 TTL 宿主探查、:299-315 Etag 宿主探查）
wedb/wkv/src/gc/compact.rs:GcManager::try_compact（:92-156 无相位门）
wedb/wkv/src/store/resize.rs:WedbStore::grow_index（:426-535）/ split_single_chunk（:270-274 begin 门）
wedb/wkv/src/session/mod.rs:StoreSession::ensure_split_by_hash（:698）/ find_tag_cooperative（:722，既定协同单机制）
wedb/wkv/src/session/raw/read.rs:read_probe / try_read_mem（:319-334、:617-625，读面已防对照锚）

对应 c# 文件与函数：
libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs:CompactLookup（:45-65 逐活记录 CompactionCopyToTail）
libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:CompactionCopyToTail（:491-505）
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ConditionalCopyToTail.cs:CompactionConditionalCopyToTail（:95-114，NOTFOUND 落 ConditionalCopyToTail 补拷插回）
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/FindRecord.cs:TryFindRecordInMainLogForConditionalOperation（:38-45）
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindTag（:226-231 仅查新表）
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs:SplitBuckets（:37-68）
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:InternalRead（:71）/ InternalRMW.cs（:68）/ InternalUpsert.cs（:65）/ InternalDelete.cs（:58）

精炼执行方案：
1 wcompact 探针协同：CompactStore 端口族新增 ensure_split(key)（wkv 实现转调 StoreSession::ensure_split_by_hash 单点，非扩容期仅一次 old_index 判空原子 load），find_latest_address 的 lookup_candidates 前必过；紧缩写侧 CAS update_address 消费的期望槽位恒来自协同后的探查，天然收口。
2 wkv/compact.rs is_deleted 的 TTL 与 Etag 宿主存在性探查由 index.load().find_tag 改走 session.find_tag_cooperative（同单机制，零新抽象）。
3 不引入 miss 即补拷的第二套弃迁语义（C# 补拷臂为上游等价物，仓内既定单机制为协同门，与读面/扫描面同构收敛）；不新增加锁或相位互斥（grow 可在紧缩中途发起，入口门不足以收口）。
4 测试验证点：仿 test_read_gap_hook 形态在 wcompact 探针处注入扩容窗确定性用例，锁定「紧缩探针 growing 期不得直采空候选判 superseded」；补 grow 迁移期并发紧缩活键保全回归（Lookup 与 Scan 两档）。

合入哈希：104e3fc0d2ff5a35200fbdb9179af587816be70e 收口形态：紧缩探针入口补 CompactSession::ensure_split 协同门（wkv 转调 ensure_split_by_hash 单点）＋is_deleted TTL/ETag 宿主探查经 host_exists_cooperative 改走 find_tag_cooperative＋wcompact 探针扩容窗注入钩（仿 test_read_gap_hook）＋wcompact 三档/wkv 四档 grow 窗回归（Lookup/Scan 活键保全＋旁路不误孤儿＋非扩容窗 superseded 对照负形）全绿
