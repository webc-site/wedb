# watch-version-map-db-hash

来源 next/zcode-r3-txn.md 问题 2（认领棒：zcode-r3-perf / zcode-r3-txn 甄别票；其中「死字段」半边已被并发棒落地的 59198c4 覆盖，本票只立语义面）。

问题一句话
WATCH 版本表的哈希输入只含用户键，节点级单表下跨库（或跨命名空间）同名键互相误脏化：db0 WATCH k 后 db1 写 k，db0 的 EXEC 误判 dirty 回 nil——与 C#「每库一张表、库间零串扰」存在真实语义差，而 wedb 多库自由切库是常态场景。

rust 现状
- wedb/wnode/src/service.rs:1067 节点级唯一 Arc<WatchVersionMap>，:1089 注入 store.set_watch_hook、:1093 注入 vector_manager.set_watch_bump，:1600 经 SessionDependencies 分发给全部会话事务管理器（attach.rs:164-:173 attach_transaction_components）。
- wedb/wnode/src/storage/session/storage_session.rs:664 version_map_watch_hook：increment_version(TxnKeyEntryComparison::key_hash(user_key))，输入纯用户键字节；钩子入参面即用户键（wedb/wkv/src/session/mod.rs:70 WatchFn 文注「收用户键」、:560 bump_watch_version 写面收口）。
- wedb/wtxn/src/txn_watched_keys_container.rs:51 add_watch 同以用户键计哈希并快照版本——两臂哈希面一致但均无库域。
- wedb/wtxn/src/watch_version_map.rs 文件头自述「多库共用同一张表」。
- 死字段半边（GarnetDatabase::version_map）已由 59198c4 删除归档（task/done/version-map-dead-field-cleanup.md），本票不重开；该棒同时在模块文档明示了「节点级单表」设计，但把哈希域缺库的语义差留在了原地。

C# 证据（逐字核对 /Users/z/git/db/wedb/garnet）
- libs/server/GarnetDatabase.cs:55/:156：每库构造独立 VersionMap = new WatchVersionMap(DefaultVersionMapSize)（const :20 = 1 << 16）。
- libs/server/Databases/IDatabaseManager.cs:46 VersionMap 属性；libs/server/Databases/DatabaseManagerBase.cs:129 单库形态 VersionMap => DefaultDatabase.VersionMap。
- libs/server/Databases/MultiDatabaseManager.cs:729 与 SingleDatabaseManager.cs:360：每库会话 FunctionsState 各挂本库 db.VersionMap——键哈希相同，物理上不同表，跨库同名键互不干扰。
- libs/server/Transaction/WatchVersionMap.cs:12 类注释仍为 "An instance per garnet server"；键哈希面消费：MainStore/UpsertMethods.cs:45 等全部写面 IncrementVersion(upsertInfo.KeyHash)，KeyHash 来自用户键。
- 结论：C# 语义 = 库间 WATCH 完全隔离；「每库一表」是其长在 per-db Tsavorite 实例上的形状。wedb 按 SKILL（doc/zh/db.md 单日志多库共享、物理键前缀刚性隔离、冷库零常驻）不存在 per-db 存储实例，逐库各配 2^16×8B 常驻表反而违背冷租户 0 内存架构——该形状不搬，语义必须搬。

裁决与修法（票面二选一，取哈希面扩库域，不取逐库建表）
1. 版本表哈希输入单点改为「会话前缀字节（ns varint + db varint）+ 用户键」，仍不带 tag 维（同键 String/信封共版本槽，与 C# 单 unified store 同口）。单点落位放 wtxn（如 WatchVersionMap 侧新增 version_hash(prefix, user_key) 或经 wval::NamespaceDbCodec 现成编码），禁止散落两处手拼。
2. wkv 写面钩子把前缀带到 bump：wedb/wkv/src/session/mod.rs:560 bump_watch_version 处（单次写、非循环，函数内 self.session_prefix() 现取即可）与 WatchFn 签名同步扩为 (prefix, user_key) 或在 wkv 侧拼定长缓冲交付——落点唯一，全仓 bump 臂（含 vector_manager set_watch_bump 消费面、StorageSession 降级臂显式推进面）共用同一次签名变更。
3. wtxn add_watch 侧同步：网络 WATCH 入口（txn_resp_commands.rs 消费会话处）持有 ns/db，改传前缀字节计算 hash；WatchedKeySlice 仍存用户键副本（save_keys_to_lock 与 EXEC 槽校验键面不变），仅 hash 字段换输入域。
4. TxnKeyEntryComparison::key_hash 作为锁表桶序面保持不变（锁表面跨库同键多争一次桶闩只是概率安全向，C# 逐 store 锁表无此面，另案不混入本票）。
5. 修正 wedb/wtxn/src/watch_version_map.rs 文件头与 storage_session.rs:659-:664 文注：单实例对标 C# 类注释，库间隔离由哈希域承担。

验收（修复前必须能红的判据）
- 新增集成测试（wnode 或 wtxn crate tests/）：会话 A 于 db0 WATCH k → 会话 B SELECT 1 并 SET k v → 会话 A MULTI/EXEC：修复前 EXEC 回 nil（红），修复后队列命令正常执行；跨 ns 同名键同款用例一条。
- 防退化：同库写 k（含信封域 RMW、RENAME、删空自愈、TTL、vector 臂 bump）仍判脏，既有 WATCH 栅栏回归全绿；EXEC 正常路径逐字节回归不变。
- ./test.sh 与 ./clippy.sh 全绿。

为什么这不是自造优化
误回 nil 是用户可见的行为差（安全向假阳性仍是语义偏离），本票方向是补齐与 C# 的语义一致性；机制上沿用 C# 自身类注释的单实例形态与 wedb 既定的前缀隔离编码单点，不新增表、不新增机制，只统一一处哈希输入域。
