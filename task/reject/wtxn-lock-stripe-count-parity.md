裁决：不成立——主张的「现状」在当下 HEAD 已全部不存在（私有条带锁表已删、锁源已收敛到 windex 桶闩、
虚构注释已订正、排序面与锁面同源）。本票是已落地改动的过期快照，无代码可做，删票归档。

核销 2026-09-19。取证基线：主仓 /Users/z/git/db/wedb 分支 dev 现刻 HEAD，行号按符号重取。

拒绝理由（逐条对票面主张）

1. 「恒定 1024 条带 + StripedLatch 独立数组内存」不实：
   wedb/wtxn/src/txn_lock_table.rs 全文 135 行无 STRIPE_COUNT、无 StripedLatch、无独立闩数组。
   现结构持 `loader: Arc<dyn Fn() -> Arc<HashIndex> + Send + Sync>`（对标 C#
   OverflowBucketLockTable 持 store 引用），`pin()` 每笔事务现取当前索引版本，
   try_lock_shared/try_lock_exclusive/unlock_shared/unlock_exclusive 一律
   `self.pin().bucket(idx)` 转发到 windex `HashBucket` 内嵌闩——与 windex 同一份锁内存。
   文件头 1-22 行已明写该口径并点名 `wkv/src/ttl.rs` 经 `acquire_keys_lock_exclusive`
   持桶闩为「同一把锁、同一份内存」。
   票面引的 `:21-23 pub const STRIPE_COUNT: usize = 1 << 10`、`:26-29 Arc<StripedLatch<...>>`、
   `:65-68 stripe_index_for_hash((hash >> 20) & (STRIPE_COUNT-1))` 三处符号在仓库里已不存在。

2. 「1024」仅剩的含义与票面立论相反：txn_lock_table.rs:29-31 的
   `const DEFAULT_TXN_BUCKETS: usize = 1024` 只服务 `TxnLockTable::new()`（:71-78，
   无 store 的单元/测试场景自带一张默认规模 HashIndex），生产装配走 `from_loader`
   （:86-89），粒度随索引规模与 split 扩容联动，与该默认值无关。票面「键数超过 1024 后
   跨键假冲突率被钳在 1/1024 且永不收敛」的前提（锁内存与表规模解耦）已不成立。

3. 「排序按 A 粒度、加锁按 B 粒度」不实：wtxn/src/txn_key_entry.rs:143
   `lock_plan(index)` 取 `index.bucket_index_for_hash(entry.key_hash as u64)`，
   wtxn/src/txn_key_entry_comparison.rs:36-37 `compare(index, ..)` 同一表达式，
   两者共用本笔事务钉定的同一 `&HashIndex` 版本；该文件头 1-4 行即写明「排序键为 windex
   当前索引版本下的主桶下标，与 TxnLockTable 的桶定位同源，杜绝排序按 A 粒度、加锁按 B 粒度
   的分叉」。票面点名的 txn_key_entry_comparison.rs:32-38 已是订正后的形态。

4. 「虚构的 64K 桶内存适配注释」已不在册：txn_lock_table.rs 现注释不含
   「64K 桶」「条带内冲突由桶级并发语义承接」「内存适配」三语（全仓 grep 零命中）。

5. 「wtxn/Cargo.toml 与 windex 零耦合」不实：wtxn/Cargo.toml:21 已
   `windex = { version = "0.1.4", path = "../windex" }`，票面推荐的「分层障碍」已用直接
   依赖消解，无需再造锁面对象 trait。

6. 同题前案已判：task/reject/design-txn-locktable-anchor-remount.md（2026-09-19 核销）
   已裁定「wtxn 转发薄壳 + windex 一处真实现」正是 C# 两份 CAS 实现收敛为一的去重形态，
   锚点各自保留即对标完整。本票若按「主路径」再删 TxnLockTable，等于把该已核销结论推翻，
   而现场代码已是该结论的产物。

7. 唯一残留的同名符号与本票无关：`STRIPE_COUNT` 全仓仅
   wedb/wnode/src/resp/vector/vector_manager_locking.rs:31（`pub const STRIPE_COUNT: usize = 256`，
   向量管理器自身条带面，对标 C# VectorManager.Locking），不在事务锁面域内；
   该域已有在途票 task/ing/vector-registry-user-key-strip-single-point.md，勿在本票揉包。

顺带取证（本票拒绝时新查得、留给主代理立案的独立事实，本棒不动）：
wtxn/Cargo.toml:19 仍为 wbase 启用 `striped` feature，而 wtxn 内 `StripedLatch` 零命中，
该 feature 位在 wtxn 侧疑为零消费者；判死须先核 wbase `striped` feature 的门禁面与其余
crate 用量，非本票范围。

—— 以下为原票全文 ——

wtxn 事务键锁表：私有条带表与索引规模解耦属第二套锁架构，且注释三处虚构 C# 出处

来源：next/glm.db.md 条 2 立项（该文件本波剪空删除）。取证基线：主仓 /Users/z/git/db/wedb
分支 dev，行号按当下 HEAD 的符号重取。判定：成立且待做。

结论一句话
C# 的事务锁没有独立的锁表内存：锁位就嵌在哈希桶本体的第一个溢出桶 entry word 高位，锁粒度
即哈希表桶数（默认 128m 索引 → 2^21 桶），并随 split 扩容自动细化。rust 另造了一张恒定 1024
条带的 StripedLatch 表，与 windex 表规模完全解耦、无任何扩容跟随，键数超过 1024 后跨键假冲突
率被钳在 1/1024 且永不收敛；而该表的注释声称的对位依据（「C# Tsavorite 锁表默认 64K 桶的内存
适配，条带内冲突由桶级并发语义承接」）三处皆假。本仓 windex 已经实现了与 C# 同构的桶内嵌闩，
所以这是典型的多套架构并存 + 虚构出处注释。

现状（主仓 HEAD 实测）
1. 恒定条带与虚构注释：/Users/z/git/db/wedb/wedb/wtxn/src/txn_lock_table.rs:21-23
   （doc「条带数：1024 个闩字槽位（C# Tsavorite 锁表默认 64K 桶的内存适配，条带内冲突由桶级并发
   语义承接）」+ pub const STRIPE_COUNT: usize = 1 << 10）、:26-29 持
   Arc<StripedLatch<STRIPE_COUNT>>（独立数组内存，与桶毫无关系）、:65-68
   stripe_index_for_hash（(hash >> 20) & (STRIPE_COUNT-1)，与索引 size_mask 无关）。
2. 消费面：/Users/z/git/db/wedb/wedb/wtxn/src/transaction_manager.rs:382-392（new 收
   lock_table: TxnLockTable、:389 TxnKeyEntries::new(16, lock_table)）、
   /Users/z/git/db/wedb/wedb/wtxn/src/txn_key_entry_comparison.rs:32-38（排序键即条带下标，
   加锁顺序与去重都建立在 1024 上，对标 C# TxnKeyEntryComparison 按桶下标排序）。
3. 装配面全仓唯一构造路径无尺寸参数：/Users/z/git/db/wedb/wedb/wnode/src/service.rs:390、:977
   TxnLockTable::new()（字段声明 :91、:871）。wtxn/Cargo.toml 依赖仅 wbase + whasher 等，
   与 windex/wkv 零耦合，故条带数在类型面上也不可能跟随索引规模。
4. 本仓已有的正确件（对照之下才显出「第二套」）：
   /Users/z/git/db/wedb/wedb/windex/src/bucket.rs:68-116、:150-280 桶内嵌共享/独占闩
   （TryAcquireSharedLatch / TryAcquireExclusiveLatch / TryPromoteLatch / Release* 一一对位），
   /Users/z/git/db/wedb/wedb/windex/src/table.rs:692 acquire_keys_lock_exclusive（批量键 →
   排序去重 → 多桶守卫，锁粒度 = 当前表桶数，随 split_chunk 在线扩容细化），生产先例
   /Users/z/git/db/wedb/wedb/wkv/src/ttl.rs:454、:502。

C# 参考（含对注释的逐条反证）
其一，无独立锁表内存：/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:14-15
"We use the first overflow bucket for latching, reusing all bits after the address."，
同文件 :40-61、:80-116 TryAcquireSharedLatch/TryAcquireExclusiveLatch 直接 CAS 桶 entry word；
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:10-21
只是持 store 引用的 struct（NumBuckets => store.state[resizeInfo.version].size_mask + 1）。
其二，「默认 64K 桶」无出处：默认规模链是
/Users/z/git/db/wedb/garnet/libs/server/Servers/ServerOptions.cs:67 IndexMemorySize = "128m"
经 :204-212 IndexSizeCachelines（÷64B/桶）= 2^21 = 2,097,152 桶（注入链
/Users/z/git/db/wedb/garnet/libs/server/Servers/GarnetServerOptions.cs:750-755），rust 的 1024
比它小 2048 倍；C# 里唯一像「锁表大小」的数字是
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Constants.cs:31
kDefaultLockTableSize = 16 * 1024，且全仓零引用（历史 ArrayLockTable 遗物），既不是 64K 也不是
活旋钮。其三，锁粒度动态取：OverflowBucketLockTable.cs:26-34 GetBucketIndex/GetBucket 每次现取
当前版本 size_mask，事务锁消费点
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs:63-102
DoTransactionalLock 与 :104-155 DoTransactionalTryLock 逐键经 store.LockTable.GetBucketIndex
定位；锁表随 store 构造（/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:105、:228）。

修法
主路径（推荐，去第二套架构）：删掉 TxnLockTable/STRIPE_COUNT/wbase StripedLatch 这条私有序列，
wtxn 的键锁面改由 store 侧注入的桶锁承接——即 wkv/windex 的 acquire_keys_lock_exclusive 形态
（TxnKeyEntries 持桶守卫而非 u16 条带下标，对标 C# ActiveLocks 持 HashBucketRef）。分层障碍是
wtxn 不依赖 windex，解法是 wtxn 定义一个最小锁面对象（由 wnode 用 store 的索引闭包实现）或
把键锁登记下沉到 wkv，保持「一处锁源」。次优路径（若本波不做架构收敛）：至少让条带数随 windex
表规模联动（构造期注入当前桶数、split 时同步细化，或排序键直接取 hash & size_mask），使粒度
不再是常量 1024。两条路径都必须先订正 txn_lock_table.rs:21-23 的注释：删「内存适配」立论、
删「64K 桶」数字、删「条带内冲突由桶级并发语义承接」（该表与桶无语义关联），改为据实描述。
排序面（txn_key_entry_comparison.rs:32-38）与锁面同源改口径，勿出现排序按 A 粒度、加锁按 B
粒度的分叉。

边界
与 task/ing/windex-batch-lock-std-dedup-single-source.md 不同面：那张管批量桶锁的排序去重内核
本身（含其立论注释与签名矛盾），本张管事务锁的粒度规模与出处注释；若按主路径实施，本张会消费
那张的收口结果。
与 task/ing/string-rmw-key-bucket-lock.md（同波新立）共用以 windex 桶闩为唯一锁源的结论：那一条
把 RMW 读改写窗口挂到同一把桶锁上，两单合并实施即彻底消除第二套锁架构。
正确性无虞（同键恒同条带），属结构失真 + 注释虚构。

优先级
重复/多套架构（第二套锁表）高于功能缺口，中上档；其中的注释订正部分可独立先行。
