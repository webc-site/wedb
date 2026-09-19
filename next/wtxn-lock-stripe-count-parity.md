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
