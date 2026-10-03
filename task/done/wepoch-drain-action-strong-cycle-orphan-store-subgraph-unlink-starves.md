归档注记：合入 8142b6bd，捕获换弱两处+LightEpoch::drop 无条件收割滞留动作，WedbStore drop 末位收口；测试装配 join 时序修正

甄别结论：通过（甄别席 J1，2026-09-27，定级 P1——从库反复全量同步按次累积纪元强环孤岛，unlink 永饿无自愈）。强环三边逐边现码落实——边一 epoch.rs:150 drain_list 强持、:99-102 UnsafeCell<Option<EpochAction>>；边二 lifecycle.rs:610 let this = Arc::clone(self) 强捕（票面 :615 漂 -5，订正）；边三 manager/mod.rs:218 store_epoch 强字段、:302 装配、store/mod.rs:441 注入本体；全仓无 take/置 None 路径。第二环 append.rs:243-252 强捕 ReadCache + read_cache/mod.rs:110 强持纪元成立。Drop epoch.rs:799-807 仅减 ACTIVE_INSTANCES 不收割亲验。可达性实锚：replica_diskbased_sync.rs:157-174 每次导入 WedbStore::recover 新建纪元后 swap_online_store、checkpoint.rs:136-148 注释自陈旧引擎随引用计数 Drop、replica_failover_session.rs:337-340 DEFAULT 降级臂（票面写 failover/ 相对名，实位 wedb/wedb/src/server/failover/ 下，订正）、wcpr/manager/recover.rs:86 每恢复新建。unlink 唯一落点 lifecycle.rs:533 亲验。reject 前案 wepoch-light-epoch-drop-discards 的拒绝判词「方案 1 结构性空转因强环」正是本票重立案依据，步骤 1/2 断环与步骤 3 收割互为必要逻辑闭环，风险移位已入步骤 6 书面裁定，非重复系合规重立案。派沙箱席 c01d。

审核结论：通过（三强边逐边亲验属实——epoch.rs:150/:99-102 强持闭包、lifecycle.rs:615 Arc::clone(self)、mod.rs:218/:302 强字段且全仓无 take/置 None 路径；二环 read_cache append.rs:244-252+mod.rs:110 属实，whlog 仅捕纯标量 AddressManager 维持无环；恶性可达确证：replica_diskbased_sync.rs:150-174 每次导入新建纪元+置换旧引擎、failover:338-341 汇入，按次累积未夸大。订正已并入：注入实位改 store/mod.rs:441-442；C# 论断改写为「原型同形强捕但追迹 GC 免疫」；补步骤 6 书面裁定 None-fallback 风险移位。四步联袂最小零新机制，与 wnode-purge 不并案成立）

纪元延迟动作强捕纪元宿主构成 epoch→动作→宿主→epoch 强环，从库换库等存活期实例消亡形态下实例子图永久泄漏且物理 unlink 永不到场

问题分析：
1 Garnet 契约对齐（C# 原型行为与协议约定）
C# 原型侧同形强捕但追迹 GC 使其无害：Index.cs:296 BumpCurrentEpoch 闭包经实例方法 DisposeAndDeleteFilesDeferred（:313 private void）隐式捕 this，纪元→动作→管理器回边在原型侧同样存在，唯 .NET 追迹回收不依赖计数，环不阻对象终结与 Finalizer 收割，故无泄漏形态；且 C# 无「纪元实例存活期死亡」形态——副本全量同步经 StoreWrapper.cs:41 单计算属性原位恢复，TsavoriteKV 与 storeEpoch 实例永不更换，已登记动作在后续任意 bump 必被收割；LightEpoch.cs:Dispose（:244-265）虽同样不收割 drainList，其以 cts.Cancel 收口等待者后实例即随进程终局，动作丢弃与进程退出同刻。rust 侧为自加的「同名重建世代守卫」（登记与删除两时点各判一次，lifecycle.rs:481-485 自陈对 C# 裸删形态的差额）要求动作执行时复查 manager 的条带锁与 live_indexes，被迫强捕 Arc<manager>，叠加置换形态（checkpoint.rs:136-144 注释自陈「C# 原位恢复不换实例，rust 实例置换形态」）制造出计数回收下致命的环与纪元死亡窗口（原型同形环经追迹 GC 免疫，rust 无等价物）——而 Arc 引用计数对强环无解，登记契约「动作必于纪元排空后执行」（epoch.rs:496-497 文档）在该形态下被结构性打破：不是 Drop 时丢弃（另案已判），是 Drop 根本永不到来。

2 工程现状确证（Rust 现有实现路径与代码缺陷）
强环三边逐边实读确证，环成立：
边一（epoch→动作）：wepoch/src/epoch.rs:150 drain_list: Box<[DrainEntry; 16]> 为 LightEpoch 直属字段，epoch.rs:99-102 DrainEntry.action 经 UnsafeCell<Option<EpochAction>> 强持有闭包本体（EpochAction::new epoch.rs:64-68 Box::into_raw 裸指针装箱）。
边二（动作→宿主）：wbftree/src/manager/lifecycle.rs:605-619 release_detached 于 :615 let this = Arc::clone(self) 强捕 Arc<RangeIndexManager>，:616 闭包 move 携带该强引用与本席 DetachedTree（内含待弃 Arc<BfTreeService> 与 data_path）。
边三（宿主→epoch）：wbftree/src/manager/mod.rs:218 store_epoch: Option<Arc<LightEpoch>> 强字段（:302 装配注入，无任何后置 take/置 None 路径），wkv/src/store/mod.rs:441-442 init_range_index 以 Some(Arc::clone(&epoch)) 注入的正是该存储实例纪元本体（store/mod.rs:518 每 store 一实例；wcpr/src/manager/recover.rs:86 每次恢复新建）。
第二同型环：wkv/src/read_cache/append.rs:238-252 pump_close_barrier 以 let rc = Arc::clone(self) 强捕 Arc<ReadCache>，而 read_cache/mod.rs:110 ReadCache.epoch: Arc<LightEpoch> 强持有同一存储纪元——epoch→关闭动作→ReadCache→epoch 同款闭环。whlog/src/hlog/shift.rs:30-33/:83-86/:298-305 三处动作仅捕 Arc<AddressManager>（address.rs:13-43 纯原子标量组、无纪元回指），无环、裸弃良性，维持既判。
恶性可达性（进程存活期实例消亡的生产形态）：
a) 从库盘基全量同步：wedb/src/server/replication/replica_diskbased_sync.rs:150-174 每次主端检查点导入走 WedbStore::recover（recover.rs:86 新建纪元）→ provider.swap_online_store(new_store)，旧引擎「随引用计数 Drop」（checkpoint.rs:143 自陈）；
b) failover 降级：failover/replica_failover_session.rs:338 DEFAULT 选项旧主降为副本，即落入 a) 的换库臂；
c) 临时库装载后拆除等一切以 WedbStore 整实例消亡为结局的路径。旧 store 消亡时刻只要 drain_list 尚有一枚未收割 unlink/close 动作（注册后负载静默无人 bump 即滞留；或换库恰好落在末批回放动作与纪元排空之间），三边共存的强环使 LightEpoch::drop（epoch.rs:800-807）、RangeIndexManager 与 ReadCache 的析构全部永不触发：store/mod.rs:797-812 WedbStore::drop 自家 PendingTreeRelease 队列有末轮收割先例（drain_bftree_release(usize::MAX)），纪元队列方向则 dispose()（manager/mod.rs:412-424）不触 drain_list、无人自救。泄漏体量：每次消亡事件永久滞留 LightEpoch（128+ 缓存行级条目槽）＋RangeIndexManager 全图（128 条带锁、migrating 注册表、release_retries 积压队列 manager/mod.rs:216、在途 BfTreeService 及其页环——树页环预算上限 256MiB 量级、动作携带的 DetachedTree 整树）＋ReadCache 整页环（num_pages×page_size）；物理面每枚滞留动作一个 {hashPrefix}.data.bftree 孤儿文件永驻——删除路径唯一 fs::remove_file 落点即在闭包内的 lifecycle.rs:533（settle_detached_release），树早已经 remove_and_take_tree 摘出 live_indexes，重启不复活引用，同名重建不再发生即永无自愈（与在办票 wnode-checkpoint-recovery-purge-meta-filtered-orphans-leak 的「meta 无存根残件启动对账不回收」同盲区）。从库反复全量同步按次累积，长周期进程多重置换即多重泄漏。

3 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
确定性资源泄漏且无任何自救通道：违反 §3.2 资源生命周期收口「状态随宿主析构安全释放，无孤儿资源泄漏」——本形态连析构本身都永不发生，比「析构丢弃动作」更重一档。次面：unlink 永不到场使从库数据目录随每次换库沉积孤儿树文件，蚕食磁盘直至 ENOSPC；泄漏的 BfTreeService 页环占用不受 cache_budget 闸约束（记账随 manager 一并成孤儿，全局 cache_reserved 若被后续实例重建则口径失真）；契约面无例外声明，后续消费方按「动作终会执行」写不变量即在换库形态静默破功。

涉及代码：
rust 文件与函数：
wedb/wepoch/src/epoch.rs:LightEpoch（drain_list 字段）/ bump_current_epoch_action / Drop for LightEpoch
wedb/wbftree/src/manager/lifecycle.rs:release_detached / settle_detached_release（环边二与受害 unlink 单点）
wedb/wbftree/src/manager/mod.rs:RangeIndexManager.store_epoch / dispose（环边三）
wedb/wkv/src/read_cache/append.rs:pump_close_barrier / read_cache/mod.rs:ReadCache.epoch（第二同型环）
wedb/wkv/src/store/mod.rs:WedbStore::drop（宿主消亡点与自家队列收口先例）
wedb/wedb/src/server/replication/replica_diskbased_sync.rs 换库段 / cluster_provider/checkpoint.rs:swap_online_store（恶性可达实锚）
wedb/wcpr/src/manager/recover.rs:recover_checkpoint_components（每恢复新建纪元）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:Dispose（环在 GC 下无害、原型纪元不存活期死亡的对照）
garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock（:240/:296，闭包仅捕树与前缀局部量不捕 this——rust 侧世代守卫加捕 manager 即分叉点）
garnet/libs/server/StoreWrapper.cs:41（Store 单计算属性原位恢复，纪元实例永不更换）

精炼执行方案：
1 断环于捕获点（lifecycle.rs:605-619）：release_detached 改 let this = Arc::downgrade(self)，闭包内 this.upgrade()：Some(m) 走 m.settle_detached_release(detached, true) 原内核分毫不动；None 即宿主已终——同名重建以 manager 为唯一锚点结构上不可能，世代复查无对象，退化为 tree.dispose() 后直接 fs::remove_file(data_path)（登记时点 :607-609 的在场 filter 已生效）。Weak 克隆/升级仅发生在释放冷路径，读写热路径零触改，数据面零开销不破。
2 同型断环（append.rs:238-252）：pump_close_barrier 改捕 Arc::downgrade(self)，upgrade None 即丢弃（水位推进面，维持既判良性）；杜绝 ReadCache 页环随纪元成孤儿。
3 环断后方令析构点生效（epoch.rs:800-807）：LightEpoch::drop 体内字段拆除前遍历 drain_list，对 epoch != DRAIN_ENTRY_FREE 槽 take() 出 EpochAction 并无条件 call()（不判 safe_to_reclaim——Drop 为 &mut 独占上下文，他线程处于本实例方法须持本 Arc 可活借用，结构上不可能并发；CLAIMING 瞬态同因不可达）。配套在 bump_current_epoch_action 文档（epoch.rs:496 附近）钉死契约：闭包不得强持有「经任何路径强引用本纪元」的对象，宿主析构为动作最后法定收割点。步骤 1/2 与 3 互为必要：只断环则动作仍随 Drop 静默弃、unlink 照丢；只补收割则 Drop 永不触发，一步不缺方闭环。
4 末位收口 release_retries 让位队列（lifecycle.rs:516-521、manager/mod.rs:216）：WedbStore::drop（store/mod.rs:797-812）range_index.dispose() 之后追加一轮 self.range_index.harvest_release_retries(usize::MAX)——重投动作经步骤 1 的 Weak 臂再入 drain_list，统一由 3 收口，不新建第二条删除路径、零新机制。无跨 await 持同步锁问题（全程同步析构上下文）。
5 测试验证点：wepoch/tests/epoch/drain.rs 新增——a) 注册多枚动作后静止不收割，释尽外部 Arc，侧信道 AtomicUsize 断言宿主终态动作全部执行（含 Weak 升级失败退化臂）；b) 负控还原强捕形态 a 转红（环在则 Drop 不到来，动作计数恒零）。wbftree 新增——c) 装配 store_epoch 形态 manager，release_detached 挂入 unlink 动作后释尽全部强引用，断言 data.bftree 文件已消、manager/epoch 析构标记点亮；d) read_cache 同款换弱后断言 pump_close_barrier 滞留动作不再钉住页环。回跑 wepoch 全量、wbftree lifecycle、wkv store Drop 与 rc close 用例不回归。
6 书面裁定（审核席要求入码注）：None-fallback 直 unlink 系「泄漏→低概率误删」风险移位——跨实例共 {ri}/rangeindex 目录（cpr_host.rs:574-575）时旧代动作迟到可与新代同名文件交错，some 臂既存收割点同样有此窗，非新增机制；执行时须在 release_detached 文档注记此裁定。

查重自证：五池零命中本面——todo 在办两枚 wepoch 票（claim-entry-help-drain、entry-table-capacity）均为条目表面不涉 drain_list 捕获形态；wkv-bftree-release-queue 票对象为应用层 PendingTreeRelease 到期长窗（另一套队列），其审核注记反证 unlink 内核同源而病灶不同；wkv-cold-bftree-observed、wnode-checkpoint-recovery-purge 为启动对账盲区面，本票是其上游「 unlink 永不到场」的病灶端；reject 旧票 wepoch-light-epoch-drop-discards 正是本票前案（其「方案 1 结构性空转」判词即本票环论断来源，本票按该判词根因重立案，方案 1 仅在断环后保留为步骤 3 的一半）；deviations.md 全库 grep epoch/析构处置无相关裁决登记。已知未翻面（entry.rs:132 重入不刷新、EpochSuspendGuard 语义）与本票正交。
