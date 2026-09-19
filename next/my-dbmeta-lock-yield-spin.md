优先级：低

问题
换号元数据串行锁 lock_dbmeta 为 AtomicBool compare_exchange + yield_now().await 自旋。compio 线程钉核下，持锁方与等待方常在不同核：临界区含 DbMeta 原子批落盘（含 IO），等待核上的任务在 yield-poll 循环里持续空转烧 CPU 直到落盘完成；同核任务虽在让渡间隙可跑，但自旋任务持续占用调度槽。锁只在管理面（flush_database/flush_namespace/swap_databases），频率低，故列低。另：该锁不得改 parking_lot 同步锁（不可跨 await，误改即死锁），头注须写清这一例外依据，防后人按「锁用 parking_lot」规范误改。

取证（dev 当下代码重取）
wedb/wkv/src/store/mod.rs:198-207 lock_dbmeta（while compare_exchange 失败即 yield_now().await）；:193-197 头注自述「协作让渡自旋…绝不 park 线程」。持锁临界区见 wedb/wkv/src/session/swap.rs:47-91（锁内换格 + persist_dbmeta_batch）与 flush 换号事务同型。

C# 对标
garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase（C# lock/Monitor 阻塞挂起让出 CPU，无自旋面；rust 因 async 不可持同步锁跨 await 才自研，形态差异需异步等待原语补齐）。

修法建议
改跨 await 可等待原语：event-listener（workspace 已有依赖）构造的异步互斥或等待队列，无争用快路径保持一次 CAS；临界区落盘期间等待任务真挂起零 CPU。头注补「不用 parking_lot 的原因（跨 await）+ 替代原语」说明。来源 next/agy.my.md 条 13 与 next/muse.my.md 条 18（后者增量即此注释要求）合并处理。
