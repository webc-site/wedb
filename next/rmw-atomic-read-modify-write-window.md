主存 String RMW 族与信封对象 RMW 内核的读-算-写窗口无同键互斥：并发同键丢失更新

来源：next/glm.my.md 第 7 轮条。本条是 next/glm.db.md「String 域 RMW 命令读-算-写窗口」条的超集
（该条只列 String 域，本条另含信封域整值覆盖），两单同根同修法，本档为收口单（并发双花见文末登记）：
glm.db.md 那条并入本单，不得两处各改一次内核。取证基线：主仓 /Users/z/git/db/wedb 分支 dev，
行号按符号在当下代码复核（初检 HEAD a7402c4，位点在后续 HEAD 复测未漂移）。
原报引用的 wkv/src/session/mod.rs:779「无条带锁可分组」注释
行号已漂移，注释本体在位（现 session/mod.rs:809，try_upsert_batch_sync 的文档注释内），
且其射程只声明 MSET 批量折叠一处，不构成 RMW 免锁依据，本单结论不变。

结论

C# 的 INCR/APPEND/SETBIT/BITFIELD/PFADD 与全部集合族 RMW 只把增量封进 input 交引擎，
旧值读取、计算、写回全程在 Tsavorite 的 hash 桶 ephemeral 独占锁与纪元内完成
（InternalRMW.cs:70 取锁、:244 释放，锁内 TraceBackForKeyMatch 读旧值后走
MainStore/RMWMethods.cs:437 InPlaceUpdaterWorker 或 CopyUpdater/InitialUpdater），
同键并发天然串行。rust 把同一条状态机拆成命令层两步：read_user_sync 裸读旧值 → 命令层算新值 →
try_rmw_sync 盲写绝对值。try_rmw_sync 的实现就是带 TTL 清退门的纯 upsert
（wkv/src/session/raw/write/rmw.rs:34 → :75 try_upsert_raw_sync_unprotected →
wkv/src/session/raw/write/inplace.rs:132），其中的 ephemeral 桶锁只覆盖单次 upsert 的
回溯与挂链，不覆盖命令层那次读。运行时是 thread-per-core 加 SO_REUSEPORT
（/Users/z/git/db/wedb/wedb/wnode/src/server.rs:417 起、:446 起多核 worker），
不同连接落在不同线程共享同一 store，故跨连接同键并发是实况而非理论：两核并发 INCR 同键
各读 5 各写 6（丢一次自增）、并发 APPEND 丢一段、并发 SETRANGE 以零填充互覆、
PFADD 丢成员致 PFCOUNT 虚低；集合域经 run_sync_rmw 反序列化整对象改后整值写回，
并发 HSET 同键不同字段时后写者把前写者的字段整体抹掉。

现状

1. String 域两步形态（同一模式五处）：
   /Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/incr.rs:65 network_increment
   （:96 read_user_sync → :116 checked_add → :122 store.try_rmw_sync 写绝对值）、
   :155 network_increment_by_float（:159 读 → :192 写）；
   /Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/set.rs:268/:279 network_set_range、
   :607 network_append（:624/:633 写回）；
   /Users/z/git/db/wedb/wedb/wnode/src/resp/bitmap/bitmap_commands.rs:108 SETBIT、
   :715 BITFIELD 写臂；/Users/z/git/db/wedb/wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:88
   PFADD/PFMERGE 回写。慢路径同型：
   /Users/z/git/db/wedb/wedb/wnode/src/storage/session/storage_session.rs:348 try_rmw_sync
   与 :354 upsert_rmw（两者都是纯写回，无 expected 前值）。
2. 信封域整值覆盖：/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs:1218
   run_sync_rmw（:1246 起装载 → :1271 run_op → obj_save_or_gc_raw（:645）整值写回），
   HSET/ZADD/SADD/LPUSH 等全部集合 RMW 共用该骨架；收尾状态机
   :1006 apply_rmw_post_operate 同样是「整对象序列化后落盘」。
3. 引擎侧无原子 RMW 出口：/Users/z/git/db/wedb/wedb/wkv/src/session/raw/modify.rs:93 rmw_raw
   是 read_raw_with → updater → upsert_raw 两段 await 分离（注释自称对照 C# InternalRMW
   状态机，实际丢了锁语义），且全仓无 wnode 生产调用者；真正原子的
   modify.rs:18 try_modify_raw_in_place_unprotected（页写锁内原位读改写，对标 InPlaceUpdaterWorker）
   只服务 etag/ttl/stub 内部域，且只覆盖内存可变区同尺寸情形，无 CopyUpdater 对等的锁内重试。
4. 同键互斥的先例已在仓内：/Users/z/git/db/wedb/wedb/wkv/src/ttl.rs:454 与 :502
   EXPIRE/PERSIST 持 index.acquire_keys_lock_exclusive(&[user_key]) 串行化整个读改写窗口
   （注释明言无锁时 ttl_of → del_ttl 间隙会产生用户可见异常），锁原语在
   /Users/z/git/db/wedb/wedb/windex/src/table.rs:692。即「本键桶独占锁覆盖读改写窗口」
   是既成范式，RMW 族是唯一没跟上的一族。
5. 测试面无兜底：/Users/z/git/db/wedb/wedb/wnode/tests 无并发同键读改写用例。

C# 参考

garnet/libs/server/Resp/BasicCommands.cs:852 NetworkIncrement、:902 NetworkIncrementByFloat、
:959 NetworkAppend、:441 NetworkSetRange（命令层一律不读旧值，只封 StringInput 交 storageApi）；
引擎锁内读改写
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:70
FindOrCreateTagAndTryEphemeralXLock 与 :244 EphemeralXUnlock、
Implementation/Helpers.cs:199（注释「Ephemeral must lock the bucket before traceback」）、
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:420/:437 InPlaceUpdaterWorker
与 :544-554 TryInPlaceUpdateNumber；对象域
garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs（记录锁内对 IGarnetObject 执行 op）。
规范源：SKILL.md:10「尽量 1:1 对标 C# 的代码实现」、SKILL.md:21-22「迭代序稳定由连接任务
线程钉定承接」（线程钉定只保证单会话内串行，跨连接同键无承接面，不能作为 RMW 原子性的替身）。

修法

一处收口，禁两套判据：

一、wkv 提供命令级原子 RMW 原语并让命令层只交「增量算子」而非「算好的绝对值」。
形态取最小改动：在既有本键桶锁范式上开一个
rmw_user_key_atomic(user_key, updater: FnOnce(Option<&[u8]>) -> RmWOutcome) 入口，
锁内完成「回溯读旧值（含 TTL 清退门与域归属判定）→ 调 updater → 原位写或追加写 → 推 WATCH 版本」，
覆盖 InPlaceUpdater/CopyUpdater/InitialUpdater 三态（原位失败的翻页降级走同锁内追加写），
使 try_modify_raw_in_place_unprotected 与 upsert 追加臂合并为一条锁内路径。
二、五处 String 臂与 run_sync_rmw 全部改走该原语：INCR/DECR/INCRBY/INCRBYFLOAT 交 delta、
APPEND/SETRANGE/SETBIT/BITFIELD 交片段算子、PFADD/PFMERGE 交 sketch 合并算子、
集合族交 ObjectOperation 算子（信封域锁内反序列化-改-序列化，等价 C# 记录锁内 op）。
命令层的「读旧值再算新值」代码随之删除，错误分支（not-integer / WRONGTYPE）由算子返回值承载，
保持现有 RESP 文案逐字不变。
三、try_rmw_sync 与 upsert_rmw 在迁移完成后删除或降级为内部原语（仅供新原子入口复用），
不留「盲写绝对值」的第二条公开路径——否则该缺陷会以新调用点复发。
四、锁粒度与开销按 C# 同口径：本键桶锁 ephemeral 独占，不引入条带分组；
批量路径（MSET 折叠、prefix hoisting 族）不受影响，其原子性诉求由批锁承接。

优先级

功能缺口，但性质是静默丢数据，列在本文件各票的实用性首位（仅次于
aof-replay-virtual-domain-context 的跨租户串数据）。它是「去条带锁」这一未列入 SKILL 例外的
自定义优化把 C# 语义一并丢掉的结果，属 SKILL.md:10 要撤销的那类优化偏差，应尽早收口。

边界

next/tiered-write-arm-concurrency.md 只管分层树内多步写臂（tree_del 结果丢弃、
meta.size 覆写丢更新），域不同、锁面不同（wbftree 树与 Meta 记录 vs hlog 记录），修完那条
本单的 hlog 与信封两步窗口仍在；两单共用「同键多步序列须整体持锁」的判据但不共用代码。
next/incr-oldvalue-strict-i64.md 管旧值解析口径、next/ttl-purge-watch-version-bump.md 管
WATCH 版本推进，均不覆盖本单。task/ing/msetnx-slow-path-meta-domain-probe.md 管 MSETNX
漏探 Meta 域写出双域键，是判定缺域不是并发缺锁，不同面。
分层升降阶换域窗口另见 task/ing/tiered-drain-envelope-tombstone.md 与
task/ing/tiered-reflush-atomic-swap.md（同轮别条已立项两票，勿并）。

并发双花登记

同一题面在编排期内被三份文档并行承接，除本节所列差异外三者裁决、修法、验收等价，
编排方择一实施、余下两份在合并时删除即可，勿三份各改一次写内核：
本单 task/ing/rmw-atomic-read-modify-write-window.md、
task/ing/rmw-same-key-atomicity.md（同题，来源标注为 glm.my 第 6 轮条，额外给出
C# ISessionLocker.cs:31-44 锁表与 rmw.rs:88-104 慢路径行号）、
task/ing/string-rmw-key-bucket-lock.md（来源 glm.db 条 6，自述以 String 域为实施主体、
并要求 glm.my 侧只补交叉引用不另立单）。本单独占的增量为：五处 String 臂的逐处行号
（incr.rs:65/:96/:116/:122/:155/:192、set.rs:268/:279/:607/:624/:633、
bitmap_commands.rs:108/:715、hyper_log_log_commands.rs:88）与慢路径
storage_session.rs:348/:354 的无 expected 前值取证，以及文首更正的 session/mod.rs:809
注释射程判定。择单时若取他二份，本单取证段可整体转录。

验收

1. 新增并发回归用例（wnode/tests 集成层）：多线程/多会话并发 INCR N 次终值为 N、
   并发 APPEND 不丢段、并发 HSET 同键不同字段全部字段存活、并发 PFADD 后 PFCOUNT 与
   串行参照一致。用例须能在改动前失败（可用最小复现断言）。
2. 全仓 grep 无「命令层 read_user_sync 后紧跟 try_rmw_sync」的两步形态残留。
3. 单键 INCR 快路径新增开销仅一次桶锁获取，与 C# 同量级；不新增跨线程全局锁。
4. cargo check --workspace --all-targets 零告警，禁写 allow。
