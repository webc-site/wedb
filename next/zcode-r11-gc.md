轮11 视角:GC 引擎深水(候选选择×水位×前端交互×物理回收序×换号×检查点×页回收)

方法:wkv/src/gc/ 六件(mod/compact/reclaim/task/ttl_sweep/vdb)全读,下沉 vdb/gc_dead、vdb/bftree_release、store/keyspace、store/compact、store/addr、wcompact 全套、wbftree/manager/lifecycle+checkpoint、wcpr/manager/create+recover、wdev 截断、whlog/shift+scan;C# 侧逐行回读 DatabaseManagerBase.cs DoCompactionAsync、StoreWrapper.cs 三任务注册与警示语、AllocatorBase.cs ShiftBeginAddress、ArrayKeyIterationFunctions.cs ExpiredKeysBase。不重复 r2-crash(树删除时机)、r5-timers(双驱动窗口)、r9-soak(单文件空操作)、r8-sample-b(shift 机制逐行已判一致)已立面。

一、真缺陷

1. P0 紧缩物理删段与检查点恢复重放窗之间无屏障:熔断紧缩(默认配置可达)可使最近检查点永久不可恢复

机制
rust 检查点非自含:wcpr 检查点只落索引快照+meta(hlog_meta.begin/head/flushed/tail),hlog 数据页依赖 flush_all 后驻留在主设备上;恢复时按 meta 重建 hlog 地址视图,再由 run_recovery_pass 对设备扫 [begin, tail) 重放(index 补账、DbMeta 映射重建、RI 桩重插全在这趟)。而紧缩/移位推进 begin 后经 truncate_begin_until 无条件物理删除段文件(wdev 分段设备 remove_file),截断链上无任何检查点感知(无「不得越过最近已发布检查点基线」的钳制,也无 C# 式 checkpoint 期互斥)。时序:拍检查点 T_c → 之后换号风暴使 gc_dead 积压 > 1024 → 熔断旁路 None 短路全速紧缩(until=safe_ro,begin 推到安全只读线)→ 段文件删除覆盖 [T_c 前后] 的检查点重放窗 → 崩溃 → recover_latest 选中 T_c 检查点 → run_recovery_pass 扫已删段 → SegmentNotFound/Device 错误(启动失败)或恢复半途(按 fail-on-recovery-error 配置);DbMeta 映射记录恰居重放窗内,即便容错续跑也是映射丢失→冷装载盲分配新号→旧数据整体不可达。恢复路径的 purge_unrecovered_checkpoints(保留仅恢复版)删掉了全部更早检查点,无回退余地。

证据
wedb/wkv/src/gc/compact.rs:try_compact(熔断旁路 L89,boost 档 n=max、until=safe_ro L110-113)
wedb/wkv/src/gc/reclaim.rs:spawn_bftree_reclaimer(200ms 常驻驱动,gc.enabled 关闭态也推进)
wedb/wkv/src/store/addr.rs:shift_begin_address/after_truncate(L174/L161,截断后无检查点联动)
wedb/whlog/src/hlog/shift.rs:shift_begin_address(L139→L186 truncate_begin_until,屏障仅纪元排空,无检查点参数)
wedb/wdev/src/segmented_device/truncate.rs(remove_file L62/L111,物理删段)
wedb/wcpr/src/manager/recover.rs(L121-124 取 meta 地址视图,L255 重建 hlog,数据页在主设备)
wedb/wkv/src/store/cpr_host.rs:run_recovery_pass(L188-198 扫 [begin_addr, tail_addr) 设备页)
wedb/wnode/src/database/database_manager_base.rs:purge_unrecovered_checkpoints(恢复后清未用,无回退)
wedb/wkv/src/config.rs GcConfig::compaction_type 文档自认「wedb 移位经设备截断无条件物理回收历史段」

C# 对位
libs/server/Databases/DatabaseManagerBase.cs:425 DoCompactionAsync(ShiftBeginAddress(until,true,…) 同款 truncateLog 物理删段,AllocatorBase.cs:1655);但 C# 双闸:CompactionTask 仅在 CompactionFrequencySecs>0 且 CompactionType!=None 时注册(StoreWrapper.cs:967-969,默认零紧缩),且 StoreWrapper.cs:702-705 明文警示 "Compaction will delete files, make sure checkpoint/recovery is not being used"。rust 熔断旁路为自引入机制(GcConfig 文档自认「Garnet 无对应物」),默认配置(gc.enabled=false、compaction_type=None)下经 spawn_bftree_reclaimer 自动可达,且无任何警示或检查点耦合闸。C# 另有检查点完成臂补跑紧缩(DatabaseManagerBase.cs:189 isFromCheckpoint),rust 无此耦合。

判定
真缺陷(恢复破洞)。建议方向:shift_begin_address/truncate 链对「最近已发布检查点的 hlog_meta.begin/tail 重放窗」钳制,越窗段延后到下一检查点发布后回收(即 C# 「拍检查点才真正删文件」的语义落地);或熔断旁路与常规紧缩统一受「距上一检查点」闸门。FLUSHALL 的 flush_all_databases→shift_begin_address(tail) 同穿此面(C# unsafeTruncateLog 语义同,属共享契约,但 rust 侧检查点保留策略未与 destructive 截断联动)。

二、活性/效率备注级(非正确性洞)

2. 冷区游标在持续热区过期风暴下永久饿死;删除失败键要等游标绕日志一整圈

机制
ttl_sweep.rs:sweep_expired 冷区段以 picked.len() < cap(默认 256)为让位条件:热区窗口每轮无界收集,只要每间隔热区新增过期键 ≥ 256,冷区段永不执行,cold_cursor 冻结——恰在过期风暴(最需要冷区消化欠账)时失效。另:冷区候选删除 Err 仅 warn,游标照样前移(cold_commit 无条件提交),失败键跌出窗口,物理删除要等游标越过 read_only 绕行一整圈后重扫。

证据
wedb/wkv/src/gc/ttl_sweep.rs:sweep_expired(L148-150 冷区让位门,L179-198 删除循环与游标提交)

C# 对位
libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:ExpiredKeysBase + DatabaseManagerBase.cs:583 StoreExpiredKeyDeletionScan——C# 只扫 [ReadOnlyAddress, TailAddress] 窗口且边扫边删,无冷区概念;ro 之下的过期键 C# 只能靠惰性读或紧缩回收。rust 冷区是增强,但饿死与绕圈条件削弱其在长尾场景的承诺。

判定
备注(活性降级面;读路径惰性过期与紧缩 TTL 判死仍兜底,无正确性洞)。

3. 换号批崩溃前缀的孤儿旧域:无账本条目被紧缩判活,每轮紧缩迁移放大

机制
commit_swap 批序 [新映射, 旧域墓碑, 水位],崩溃落在「新映射已落、GcDeadDb 未落」前缀时,旧 vid 无账本条目→is_virtual_id_dead_and_expired 恒假→紧缩谓词判活→旧域全部废弃记录以活记录身份被迁移到尾(每轮紧缩复制一遍,写放大+永久滞留)。文档自述「最坏旧域泄漏」,但泄漏不止滞留:紧缩存活迁移使其成为常驻迁移负载。

证据
wedb/wkv/src/store/keyspace.rs:commit_swap(L384-390)/flush_database;wedb/wkv/src/compact.rs:WedbCompactionFunctions::is_deleted(L217-223 死亡判据仅看账本)

C# 对位
无对位(C# FlushDatabase 整段截断,无账本与批前缀概念)。

判定
备注(概率极窄的批前缀窗;如要收敛,可在重建期对「路由表已不指向的 vid」补登记死亡)。

4. compaction_max_segments=0 连带关闭熔断安全网

机制
try_compact 早退门 seg==0 || max==0 在熔断旁路之后独立生效:max=0 时即便积压超水位,紧缩也零推进,「物理回收不受旋钮关闭」的承诺实际只覆盖 compaction_type,不覆盖 max_segments=0;死亡账本与旧域垃圾在纯小库(永不积到水位)之外的场景永久滞留。

证据
wedb/wkv/src/gc/compact.rs:try_compact(L105 max==0 早退);wedb/wkv/src/config.rs(compaction_max_segments「0 = 永不紧缩」vs gc/mod.rs「不受 compaction_type 旋钮关闭」承诺口径)

C# 对位
无对位(C# 无熔断;COMPACTION_MAX_SEGMENTS 无 0 值禁用语义,值 0 时阈值恒不触发,行为巧合等价)。

判定
备注(口径交叉的配置脚枪;至少文档需把「熔断不受关闭」改为「不受 compaction_type 关闭」的精确表述)。

5. 热区候选全量累积内存峰值:C# 边扫边删、rust 先集后删

机制
热区段 max_picks=usize::MAX,一轮把窗口内全部过期键装进 ExpiredKeySet(三元组含整键拷贝)后才进删除循环;千万级短 TTL 键同时到期时内存峰值与窗口过期键总量成正比。C# ExpiredKeysBase.Reader 在扫描回调内直接 DELIFEXPIM,零累积。

证据
wedb/wkv/src/gc/ttl_sweep.rs(L131-145 热区全量预算,L176-187 删除循环)

C# 对位
ArrayKeyIterationFunctions.cs:ExpiredKeysBase.Reader(inline delete)

判定
备注(与 C# 单轮总量等价,差在驻留形态;可给热区候选设上限分批删)。

三、无增量确认

- 水位算法:迟滞双水位状态机(死区维持、低水位倒挂钳制、hi=0 关闭)正确;常规档每轮回退 1 段使窗口自然回落至阈值下,无「刚紧缩又触发」振荡;熔断档受 safe_ro-begin>max*seg 阈值门约束,200ms 轮询不空转。C# 无迟滞与熔断,均为 rust 增强,方向安全。
- 阈值/回退上界单源 safe_ro(修正 C# DatabaseManagerBase.cs:444 用 ro、内核 TsavoriteCompaction.cs:35 用 safe_ro 的双界分叉)为已登记刻意差异,本轮复核成立。
- 候选选择:收集内核 collect_expired 单点双入口(EXPDELSCAN/后台),四级过滤(墓碑位→TTL 键反解→到期比较→probe_ttl 最新态双检)与「删除完成后才提交游标」的幂等纪律闭环;无小库饥饿面(单日志全库通扫 db_match=all_dbs,C# 逐库扫描等量)。
- 前端交互:扫描锁面安全——scan_iter 以 safe_read_only 快照分派无锁裸读/模糊区页读锁,零头自旋覆盖在途窗(r8-sample-b #23 已判一致,本轮未翻案);删除走与用户 DEL 全一致路径(逐键 check_expired 终审+set_virtual_context 物理域直设,换号窗口不跨域误删)。
- 物理回收序:「先摘后缩」偏序成立——换号刻路由换指+树摘注(逻辑不可达),DbMeta 墓碑注销待 begin 越过 tail_address(物理记录不可达)后才离册;离册过早会使紧缩谓词复活判活,现序正确。
- 恢复账本重放:地址序 insert/tombstone 对消(rebuild_vdb_visit 墓碑臂)、批前缀崩溃最坏旧域泄漏不复活不撞号(水位收尾)、注销落盘失败下轮重扫幂等(pop_reclaimable 双检)、已回收 vid 的陈旧堆索引防御性丢弃——逐项闭环。
- 死域判死与树删除同用 db_gc_reclaim_delay_secs 安全纪元(reclaim_expired_at 单点),紧缩判死与树 unlink 同窗到期,无错代。
- bftree 页回收:页缓存为每树独立引擎实例(无全局缓存钉死被删树页);dispose 经纪元动作排空在途读者,Arc 保活至最后读者退出;settle_detached_release 世代守卫在登记与删除两点各判(锁内复查 live_indexes,杜绝排空窗同名重建误删新世代文件),条带锁让位重投有界。Windows 先关句柄后 unlink 次序保持。
- 检查点×树文件:检查点树快照为 token 目录内独立副本(cpr_snapshot/fs::copy),树工作文件删除不损历史检查点自含性;keep=2 环形保留+恢复清未用对标 C# tokenHistory=2 环;clear_all 清的是临时 cpr_dir(生产 range_index_dir=None),不触主检查点树快照。
- 死域树文件的带地址刷盘件({prefix}.{addr}.flush.bftree)不随 settle_detached_release 删除,靠 on_truncate 按地址回收与重建期 remove_addr_flush_files 兜底,无幻影恢复面(前缀寻址恢复已被 create_bftree_internal 工件清理封堵)。

视角结论:有增量
