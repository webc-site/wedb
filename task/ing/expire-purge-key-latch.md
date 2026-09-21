# expire-purge-key-latch：过期清退与 SET 臂并发闭合合并裁决

来源：next/r6-del-expire-purge-key-latch.md 与 next/r6-del-set-arm-closure-adjudication.md
（两票已 git rm，本文件为合并裁决与实施票，认领 expire-purge-key-latch 域）

## 裁决

混合面，单一机制：键闩串行为闭合主机制（purge 链入闩 + SET String 臂入闩），
闩覆盖不到的残余交错面保留既有落笔前复验兜底。两票各自主张的「纯键闩串行」与
「纯复验」都不成立，改为明确的混合面并对齐 C# 记录锁覆盖面。

C# 证据（记录锁对四族入口全覆盖）：

1. libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:70、
   InternalUpsert.cs:67、InternalDelete.cs:60：RMW/Upsert/Delete 三条内部路径同取
   FindOrCreateTagAndTryEphemeralXLock / FindTagAndTryEphemeralXLock 一把记录 X 锁，
   try/finally 全程持有
2. 过期清退即 DELIFEXPIM：libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:217-253
   （ExpiredKeyDeletionScan.Reader 仅候选初筛，CheckExpiry 无锁）→
   libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:120-127（统一走 RMW 通道）→
   libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:181-187 记录锁内对当前记录
   重判过期（ExpireAndStop 墓碑）、:202-204 未过期即 no-op、:67 NeedCopyUpdate 同判
3. SET 打在过期键上：libs/server/Storage/Functions/MainStore/RMWMethods.cs:441-446
   InPlaceUpdaterWorker 锁内 CheckExpiry → ExpireAndResume 视同不存在后按新建写入
4. 结论：C# 闭环 = 一把记录锁 + 锁内条件重判（清退臂本身就是复验式条件删除），
   SET 与过期清退对同键不可能交叠；票二「记录锁覆盖面为准」的方向成立

rust 现状证据（历史重置后以当前代码为准）：

1. 清退链无闩，票一缺陷属实：wedb/wkv/src/ttl.rs:337 purge_expired 不取
   try_lock_key_exclusive，check_expired（ttl.rs:434）惰删入口与
   wkv/src/gc/ttl_sweep.rs:165-172（sweep 逐键 check_expired）同走此链，
   delete（wkv/src/session/collection.rs:54）首清 TTL 旁路后无条件墓碑数据；
   ttl_of 读到 del_ttl/delete 落地之间并发 SET 可整键误删已 ACK 新值
2. SET String 臂无闩，票一点 3 属实：wkv/src/session/raw/write/mod.rs:211 upsert_tag
   （:225 del_ttl → :229 信封清退 → :234 Meta 排空 → :238 数据写）；同步臂 :124-179 同构
   （:151-156 清 TTL/信封 → :174 写），全程只持记录桶 ephemeral 闩
3. expire_at/persist 已闩：wkv/src/ttl.rs:481、:533；wnode 同步 EXPIRE/PERSIST 臂已窗：
   wnode/src/resp/key_admin_commands/keys.rs:729、:778（expire_apply_sync/persist_apply_sync）
4. RMW 写回前复验已存在，票二「已合并」属实：wnode/src/resp/objects/object_store_utils.rs:1026
   obj_save_recheck_sync、:1041 obj_save_recheck_async（经 rmw_helpers.rs 同步/异步 RMW
   骨架调用），回归 wnode/tests/rmw_writeback_revalidate.rs 八用例
5. wkv/src/session/rmw_window.rs:37-43 既有论证：SET/DEL 纯写回族不取窗
   （信封写回嵌记录闩的两基重入面），其与窗口的交错由复验承接

裁决理由：

1. 复验闭合不了 purge-vs-SET：wedb 同键 TTL 旁路记录与数据记录是两个物理键两个桶，
   purge 的「判过期」与「落数据墓碑」之间无共同锁可依，SET 闩外仍可在重读之后落笔，
   数据丢失窗口只能收窄不能消除；唯一闭合物是 user_key 键闩（expire_at/persist 与
   wnode 同步 EXPIRE/PERSIST 臂已证明此形态，wkv/src/ttl.rs:526-528 注释同述）
2. SET String 臂必须与 purge 同闩：否则闩只挡 expire/persist，窗口收窄不闭合（票一点 3）；
   C# Upsert 在记录锁内（InternalUpsert.cs:67），键闩串行是对标正解；失闩沿既有降级
   （同步臂 Ok(Err(u64::MAX)) 转异步、异步臂让核预算耗尽回 LockTimeout），与 expire_at
   及 rmw_window 既有纪律同一套
3. 不做全闩化、不删复验：DEL 与裸记录写者不入闩（超两票射程且拆除已验证层）；
   C# 自身清退臂就是锁内条件重判，rust 的「闩（purge + SET String 臂）+ 闩外复验
   （DEL/裸写者 vs RMW 窗）」与之同构——全仓一套闭合机制，复验只兜闩覆盖不到的面，
   不再与闩争夺同一交错面的主防线

## 改动清单

1. wkv/src/ttl.rs：purge_expired 拆两层——外层取 try_lock_key_exclusive
   （失闩 Ok(()) 放弃本轮，候选留待下轮扫描/惰删重入）、闩内 ttl_of 重读、
   is_expired 复判后调内层 purge_expired_latched（现体迁入，契约改为假设闩已持）；
   expire_at/expire_at_apply/persist 改调内层（已在闩内、已重读，行为不变）；
   check_expired 改调外层，返回值语义不变（expired 即视同 NOTFOUND，物理清退可顺延，
   ttl.rs:344 既有声明）
2. wkv/src/session/raw/write/mod.rs：upsert_tag 与 try_upsert_tag_sync_unprotected_with_prefix
   的 String 臂头部经 try_rmw_window/rmw_window 取本键窗（事务态让闩，复用 rmw_window
   单点不自建锁语义），跨「claim 判点 → del_ttl → 信封清退 → Meta 排空 → 数据写 →
   watch 推进」全程 RAII 放闩；同步失闩沿既有 Ok(Err(u64::MAX)) 降级异步收口
3. wnode/tests/rmw_writeback_revalidate.rs：两条 SET 用例按票二预案把交叠构造改为
   闩外 actor（裸记录写者直写 String 物理键，只持记录桶闩），复验判点继续被真实行使、
   复验层不删不放宽；DEL 两用例与 uncontended 对照用例不动（DEL 仍闩外，本就成立）
4. 新增并发回归：purge-vs-SET（惰删 probe_alive 入口与 GC sweep 入口两路：
   SET 回 OK 后键值仍活且无 TTL；闩内重读对续期键不误删；失闩放行不阻塞写者）
5. rmw_window.rs 模块头与 object_store_utils.rs 复验注释更新：键闩覆盖面清单
   （对象 RMW 窗、EXPIRE/PERSIST 异步与同步臂、purge 链、SET String 写臂、wtxn）
   + 复验角色声明（票二验收 3）

## 验收

cargo check -p wkv --tests 与 cargo check -p wnode --tests 零 error 零 warning；
rmw_writeback_revalidate 全部用例 + 新增 purge-vs-SET 并发用例全绿，
无删除任何既有断言层、无第二套闭合机制
