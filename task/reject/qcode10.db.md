qcode10.db 台账分拣归档（第 10 轮 db 视角：底层存储引擎）

本档不含本代理裁决拒绝的观点条目。台账 next/qcode10.db.md 的 5 条主张经逐条对现仓代码
与对 /Users/z/git/db/wedb/garnet 的 C# 源复核后全部成立，均属清理/收口类待做，已立案
（详见下方「成立条去向」），台账文件已剪空并删除。本档的主要用途是转录台账作者自己
撤销的 10 条初判及其理由，供下一轮 db 视角审查去重，避免同一疑问反复上报；其中第七条
的理由经复核不成立，已单独标注，不得当作核销依据。

成立条去向（同主题只此一案，本代理已独立重取全部证据；四张工单按文件名引用，
它们在 /Users/z/git/db/wedb/next/ 与 /Users/z/git/db/wedb/task/ing/ 之间随开发波认领迁移，
本波实测 windex-batch-lock-std-dedup-single-source.md 已被认领进 task/ing/）

条 1 与条 2（GcConfig::compaction_interval_ms 与 GcConfig::compaction_num_segments 零生产
写侧、同一个 0 值三处注释两种互斥语义、DEFAULT_GC_NUM_SEGMENTS 引用 C# 不存在的符号
CompactionNumSegmentsToCompact、三个 DEFAULT_GC_* 常量仅测试自环）合并为一案：
/Users/z/git/db/wedb/next/wkv-gc-compaction-interval-num-segments-surface.md。
现仓复核：唯一读点在 /Users/z/git/db/wedb/wedb/wkv/src/gc.rs:563（节流判据）与 :585
（回退段数），字段声明 /Users/z/git/db/wedb/wedb/wkv/src/config.rs:123 与 :129，默认值
:187 与 :189；生产写侧仅两条臂 /Users/z/git/db/wedb/wedb/wnode/src/service.rs:1529-1535
与 /Users/z/git/db/wedb/wedb/wnode/src/config_owner.rs:84 与 :93，均只写
compaction_max_segments 与 compaction_type；wconf 侧确无紧缩频率槽位（
/Users/z/git/db/wedb/wedb/wconf/src/runtime_server_config.rs:227 与 :237 只有这两项）。
C# 侧：/Users/z/git/db/wedb/garnet/libs/server/StoreWrapper.cs:967-969 的
CompactionFrequencySecs 门与 :697 CompactionTaskAsync 是活链，该旋钮在
/Users/z/git/db/wedb/garnet/libs/host/Configuration/Options.cs:265、
/Users/z/git/db/wedb/garnet/libs/host/defaults.conf:194、
/Users/z/git/db/wedb/garnet/libs/server/Servers/GarnetServerOptions.cs:211 均有源；
回退段数在 C# 是调用点字面量（/Users/z/git/db/wedb/garnet/libs/server/Databases/
DatabaseManagerBase.cs:379 传 1，形参 :425，推导 :436），全仓 grep 无
CompactionNumSegmentsToCompact。补强一条：doc 承诺面的失真措辞在
/Users/z/git/db/wedb/doc/zh/db.md:172（台账原记 :169）。

条 3（windex 批量桶锁自造 in_place_dedup_by 内核，立论注释「slice 无 dedup_by——该方法为
Vec 专属」为事实错误）：工单文件 windex-batch-lock-std-dedup-single-source.md（本波实测
已开发波认领，现居 /Users/z/git/db/wedb/task/ing/）。
现仓复核：内核 /Users/z/git/db/wedb/wedb/windex/src/table.rs:634，错误注释 :682，
排序 :684，唯一调用 :685，去重长度直入下游 :687；本仓另有 9 处切片 .dedup() 全部经
Deref 走同一个 std 切片方法，可就地反证（wcpr/src/manager/mod.rs:427、
wcustom/src/txn_proc.rs:157、wnode/src/resp/resp_server_session.rs:2661、
wlua/src/loader.rs:461、wext_json/src/json_path/path.rs:139、
wedb/src/server/replication/aof_sync_driver.rs:548、
wedb/src/server/migration/migrate_driver/keys.rs:635、wepoch/tests/epoch/concurrency.rs:465、
wnode/tests/resp_set.rs:594）。C# 侧相邻去重确为加锁循环自身的判等分支：
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/ClientSession/
TransactionalContext.cs:63-102（:68-70 注释与 :80 的 currBucketIndex != prevBucketIndex）
与 :104 起的 DoTransactionalTryLock，无独立预压缩内核。

条 4（whlog 尾部地址恒等别名挂错位 C# 出处，另有两条生产零消费的接口）：
/Users/z/git/db/wedb/next/whlog-safe-tail-identity-alias.md。现仓复核：
/Users/z/git/db/wedb/wedb/whlog/src/address.rs:126 的 safe_tail 体即 :119 的 tail，
/Users/z/git/db/wedb/wedb/whlog/src/hlog/shift.rs:272 再转发一层，生产零读者，
仅 /Users/z/git/db/wedb/wedb/whlog/tests/hlog/append_scan.rs:64-65 与
/Users/z/git/db/wedb/wedb/wkv/tests/store/flush_evict.rs:401-402 断言它等于 tail_address；
C# 的 SafeTailAddress 属追加日志侧缓存值（/Users/z/git/db/wedb/garnet/libs/storage/
Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:104，doc 以 cref 指向
RefreshSafeTailAddress），混合日志/分配器侧无该符号（AllocatorBase.cs 内 grep 零命中），
本仓真正的对位面已在 /Users/z/git/db/wedb/wedb/waof/src/wal/log.rs:79 且被多处消费，
故 whlog 这一套是名同实异的第三份口径。事实修正一条：台账原文称
shift_addresses_with_wait（whlog/src/hlog/shift.rs:334）与 set_page_id
（whlog/src/buffer.rs:207）「全仓含测试零引用」在现仓不成立，实况是生产零引用、
仅测试在用（前者 /Users/z/git/db/wedb/wedb/whlog/tests/hlog/flush_and_shift.rs:213、:339，
后者 /Users/z/git/db/wedb/wedb/whlog/tests/hlog/append_scan.rs:455）；结论不变，
因为 C# 侧该组合口确有活链（AllocatorBase.cs:1212 → Index/Tsavorite/LogAccessor.cs:171 →
Index/Common/LogSizeTracker.cs:442），而 wedb 的驱逐编排走自有两件
（/Users/z/git/db/wedb/wedb/wkv/src/session/raw/mod.rs:269 与 :273 直接用
shift_head_address + wait_safe_head_drained），组合口属重复面。

条 5（跨 crate 注释锚定不存在的 wkv/src/checkpoint.rs 与 read_cache.rs）：
/Users/z/git/db/wedb/next/wkv-stale-checkpoint-read-cache-anchor.md。现仓复核：六处命中
为 /Users/z/git/db/wedb/wedb/wkv/src/config.rs:244、:246、:247，
wkv/src/range_index/mod.rs:205，wcpr/src/error.rs:36，wbftree/src/manager/replication.rs:134；
两文件确不存在，真身为 wkv/src/store/cpr_host.rs:28 的 cpr_err、:392 的 from_recovered、
wkv/src/read_cache/append.rs:39 的 append（行号相对台账已位移）。同句 :247 提到的
raw/read.rs 不是失效锚，它指向 /Users/z/git/db/wedb/wedb/wkv/src/session/raw/read.rs，
勿一并改动。

台账作者自撤的 10 条初判（转录保留理由，防下轮重报）

一、update_gc_config 零生产调用方。撤销理由：起因是对 wnode/src/service.rs 截断误读，
实有启动投影与 CONFIG SET 调停两条臂在调；结论收窄为仅 compaction_interval_ms 与
compaction_num_segments 两字段无写侧（即已立案的条 1、条 2）。本代理复核成立。

二、whlog flush_page 为死 API。撤销理由：非零消费。本代理复核：grep flush_page 现仓
36 处命中，成立。

三、wbase/src/pool/work_set.rs 的 SPIN_LIMIT 与 wbase/src/backoff.rs 同值重复声明。
撤销理由：backoff 与 pool 在 /Users/z/git/db/wedb/wedb/wbase/Cargo.toml 的 features 里
互不依赖（pool 不含 backoff），合并会引入特性耦合，且 work_set.rs:7-9 注释已明示刻意对齐。
本代理复核成立（pool 特性臂不含 backoff，两处常量分属两条独立编译面）。

四、whlog/src/hlog/mod.rs:36 的 DISK_READ_PROBE_LEN 与 wbase/src/align.rs 的
DEFAULT_SECTOR_SIZE 跨 crate 同值。撤销理由：前者私有且只有一个读点（探针长度，
与扇区对齐单位不同源）。本代理复核成立：现仓仅 whlog/src/hlog/io.rs:153 一处读。

五、whlog 原位更新路径的区域谓词两处判定。撤销理由：与在册 next/ 同族条目重复，按避开
清单不重报。本代理未复核（属去重判断，不涉事实）。

六、wkv PendingFlushList 的 completed 集合与 C# PageStatusIndicator[] 语义分叉。
撤销理由：读完 PendingFlushList.cs 全文与 AllocatorBase.cs:228-236 后确认是同一
「乱序完成、按连续前缀推进 flushed_until」内核的等价表示。本代理未复核该对读。

七、mutable_fraction 与 ro_lag_num 双份只读区比例声明。撤销理由原文写作「与
TsavoriteBase.cs:215-400 的 MutableFraction/ReadOnlyLagNum 同位，属 C# 同位双件」。
本代理复核：该理由不可信——C# 全仓不存在 ReadOnlyLagNum 符号，且 TsavoriteBase.cs:215-400
实为哈希表 FindTag 区段，不含这两个名字；C# 只有 logMutableFraction 一个量
（/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:96，
读点 :1596 由它就地换算只读线）。rust 侧是把浮点比例预转成定点滞后分子
（whlog/src/config.rs:42 ro_lag_num_from_fraction）后与 mutable_fraction 并存，属派生缓存
还是重复声明需重判。本条不作为核销依据，下一轮可重开（重开前先定死：C# 无 ReadOnlyLagNum）。

八、windex CandidateAddresses 迭代体与主扫表体重合。撤销理由：与 C# Revivification 的
CandidateAddresses 同类，同构内联。本代理未复核该对读。

九、whlog/src/buffer.rs 的 unsafe impl Send/Sync。撤销理由：命中在册
next/unsafe-safety-comments-backfill 覆盖面。属去重判断，不涉事实。

十、windex 批量查找的预取分块展开 batch_pipeline。撤销理由：与 C# 批量锁/批量查的分块
预取同构。本代理复核：该内核在 /Users/z/git/db/wedb/wedb/windex/src/table.rs:593，
两处消费者为同文件 :616 与 :626，是本仓私有批查驱动，与条 3 的桶锁去重不是一事。
