String 域 RMW 命令族的读-算-写窗口无同键互斥：并发丢更新，收口点应收敛到 windex 桶闩

来源：next/glm.db.md 条 6 立项（该文件本波剪空删除）。取证基线：主仓 /Users/z/git/db/wedb
分支 dev，行号按当下 HEAD 的符号重取。判定：成立且待做。

同主题登记（本波编排期实况）
本面在 glm.my 侧有一条同根超集主张，已落地为 task/ing/rmw-atomic-read-modify-write-window.md，
该单文首自述为收口单，并登记了同期三份异名薄壳（含本单与已剪除的 rmw-same-key-atomicity.md）。
本单不再自称唯一载体：两单裁决、锁源、修法一致，派发时择一实施即可，勿两处各改一次 RMW 内核。
本单独占、不可随择单丢失的增量为：其一，读侧取证
（user_read.rs:162-169 与 wkv/src/session/mod.rs:205 的普通命令一致性读不加桶锁）；其二，桶闩
对位证据 windex/src/bucket.rs:68-116、:150-280 与 C# HashBucket 的 TryAcquire/Release/Promote
一一映射，这是收口单未列的锁实现层依据；其三，文首「修法」段的禁令——收口不得新建私有锁表、
也不得与 wtxn 条带表并联，须与 task/ing/wtxn-lock-stripe-count-parity.md 共用同一把 windex 桶闩
锁源，收口单无此跨单约束。若编排方取收口单实施，请把上述三点转录过去再剪本单。

结论一句话
C# 这批命令走一条 RMW 状态机，入口即对键所在哈希桶加 ephemeral 独占锁，锁内完成
TraceBackForKeyMatch 读旧值 → 原位改或追加新值 → 解锁，同键并发被桶锁串行。rust 把它拆成命令层
的两次独立调用：无锁读旧值 → 本地算新值 → 纯 upsert 写回，读写之间任意交错即丢更新。wkv 侧
其实已有两个正确件（页写锁内原位读改写的 try_modify_raw_in_place_unprotected、windex 的批量键
桶锁 acquire_keys_lock_exclusive），但都没接到这批命令上。

现状（主仓 HEAD 实测）
1. 两步形态命令层：/Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/incr.rs:65-130
   network_increment（:96-114 read_user_sync 取旧值并解析 → :116-119 checked_add →
   :122-127 store.try_rmw_sync 盲写），:133 起 network_increment_by_float 同型；
   /Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/set.rs:233 network_set_range、
   :607 network_append；/Users/z/git/db/wedb/wedb/wnode/src/resp/bitmap/bitmap_commands.rs:57
   network_string_set_bit 与 BITFIELD 写臂回写；
   /Users/z/git/db/wedb/wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:82 store_hll
   （PFADD/PFMERGE）。
2. 读侧无锁：/Users/z/git/db/wedb/wedb/wnode/src/storage/session/common/user_read.rs:162-169
   read_user_sync 只经 with_session_consistent_read 包一层（
   /Users/z/git/db/wedb/wedb/wkv/src/session/mod.rs:205 起），普通命令一致性读臂对键不加任何
   桶锁。
3. 写侧非 RMW：/Users/z/git/db/wedb/wedb/wkv/src/session/raw/write/rmw.rs:34-40 try_rmw_sync、
   :55-80 try_rmw_sync_unprotected_with_prefix —— 主体是 TTL 过期门裁决 +
   :75 try_upsert_raw_sync_unprotected 纯写回 + :77 bump_watch_version，无前值校验、无 expected
   address CAS。慢路径 /Users/z/git/db/wedb/wedb/wnode/src/storage/session/storage_session.rs:347
   rmw_string → upsert_rmw 同为纯写回。WATCH 版本推进只服务事务乐观校验，非事务裸命令无保护。
4. 已有但未接线的原子面：/Users/z/git/db/wedb/wedb/wkv/src/session/raw/modify.rs:18
   try_modify_raw_in_place_unprotected（沿 prev 链回溯 + 页写锁内就地读改写，对标
   InPlaceUpdaterWorker），生产消费只有内部域：
   /Users/z/git/db/wedb/wedb/wkv/src/etag.rs:60、/Users/z/git/db/wedb/wedb/wkv/src/ttl.rs:291、
   /Users/z/git/db/wedb/wedb/wnode/src/storage/session/common/ttl_sync.rs:86、
   etag_sync.rs:51；闭包式 rmw_raw（modify.rs:93，read_raw_with → updater → upsert_raw，本身
   两段 await 分离亦非原子）全仓无 wnode 生产调用者，仅同文件 :133 自用。
5. 桶锁先例（同一把锁的正确用法已在位）：/Users/z/git/db/wedb/wedb/windex/src/table.rs:692
   acquire_keys_lock_exclusive，生产消费点 /Users/z/git/db/wedb/wedb/wkv/src/ttl.rs:454、:502
   （EXPIRE/PERSIST 读改写窗口持本键独占桶锁），桶闩本体
   /Users/z/git/db/wedb/wedb/windex/src/bucket.rs:68-116、:150-280（对位 HashBucket
   TryAcquire/Release/Promote Latch）。

C# 参考
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:70
FindOrCreateTagAndTryEphemeralXLock（全程持锁）与 :244 EphemeralXUnlock；
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionLocker.cs:31-44
BasicSessionLocker.TryLockEphemeralExclusive；
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:199
（"Ephemeral must lock the bucket before traceback"）；命令面
/Users/z/git/db/wedb/garnet/libs/server/Resp/BasicCommands.cs:852 NetworkIncrement、
:902 NetworkIncrementByFloat、:441 NetworkSetRange、:959 NetworkAppend；
/Users/z/git/db/wedb/garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:131 NetworkStringSetBit
（单次 storageApi.StringSetBit RMW）；
/Users/z/git/db/wedb/garnet/libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:26-36、:109；
引擎锁内算值 /Users/z/git/db/wedb/garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs
（InitialUpdater/InPlaceUpdaterWorker/CopyUpdater 全在记录锁内跑）。

修法
在 wkv 提供一个命令级原子 RMW 入口，读旧值→算新值→写回在同一次本键独占桶锁（或对等的条带写锁）
内闭环，直接复用 windex 的 acquire_keys_lock_exclusive 作为唯一锁源（与 EXPIRE 先例同锁，杜绝
第四把锁），闭包形态沿用 modify.rs 的 with_memory_record 回溯；原位不可行（尺寸变化、已滑出
可变区、命中墓碑）时在锁内降级走 copy-update 追加（对标 C# CopyUpdater，扩展现有 modify.rs
骨架覆盖该分支），失败即锁内重试而非解锁重来。命令层七族（INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT/
APPEND/SETRANGE/SETBIT/BITFIELD 写臂/PFADD/PFMERGE）统一改走该入口，删除各自的读+写两步形态。
同一入口的第二消费方是对象信封域的 run_sync_rmw（整值写回丢字段，同根问题）。
禁止的收口方式：再新建一张私有锁表或与 wtxn 条带表并联的第二套锁源（见
task/ing/wtxn-lock-stripe-count-parity.md，那一条的正解同样是收敛到 windex 桶闩）。
验证：需补并发同键用例（多任务并发 INCR 断言终值 = 次数、并发 APPEND 断言尾段全在、并发
HSET 不同字段断言字段不丢），当前 wedb/wnode/tests 无并发同键读改写用例。

边界
与 task/ing/tiered-write-arm-concurrency.md 不同域（那条管 wcol/wbftree 分层树内多步写臂竞态，
本条管 hlog String 域与内存信封的命令层两步窗口）。
与 incr 旧值解析口径（前导零 / i64 严格解析）类票据不同问题。
与 task/ing/wtxn-lock-stripe-count-parity.md 共用「windex 桶闩是唯一锁源」这一收口结论，两单
分别落在事务锁面与 RMW 读改写面，实施时同一把锁、勿各造一套。

优先级
功能缺口（正确性：并发丢更新），高档；实施上排在锁源收敛（wtxn 那条）之后或与之一批。
