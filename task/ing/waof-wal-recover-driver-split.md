优先级：低（超长恢复流驱动占据 log.rs 近半）
来源：next/agy.db.md 条 9（票内「recover 从 190 行写到 528 行」不实，实测该函数体为
190-318 共 128 行，528 之前是它的私有 helper 群；本票按现刻实测重述，主张仍成立）。
核销 2026-09-19，取证基线 = 主仓 dev 当下 HEAD。

结论一句话
waof/src/wal/log.rs 里一整条 WAL 恢复流驱动器（入口 + 7 个私有件，占 :190-:528 区段）
与日常读写/扫描门面混在同一 734 行文件；把恢复域整体搬到 wal/recover.rs，
log.rs 只留 WalLog::recover 门面转调。

现状（主仓 HEAD 实测，waof/src/wal/log.rs 共 734 行）
1. 恢复入口：:190 pub async fn recover(&self) -> Result<u64>，函数体至 :318。
   内含帧同步、滑动窗口批量读、伪头与 CRC 过滤、截断点推导、Commit 元数据恢复、环形缓冲预热。
2. 恢复域私有件紧随其后（全部只服务恢复）：:328 note_recover_truncation、:356 has_nonzero_after、
   :374 dropped_bytes_after、:397 fetch_tail、:426 frame_sync、:479 verify_candidate、
   :505 read_recover_payload。
3. 恢复结果门面（留在 log.rs）：:529 safe_initialize、:711 recovered_cookie、
   :717 recovered_committed_begin、:724 recover_truncation。
4. 同目录已是分件粒度：waof/src/wal/{mod, log, commit, config, disk_window, flush, header,
   iterator, pipeline, record, ring_buffer, sequence_number_generator}.rs —— 新增 recover.rs
   与该 crate 惯例同形。

C# 参考
1. 票内 cite libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLogRecovery.cs 不存在；
   真实对位是 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs 内的恢复件
   （:218 构造期 syncRecover 分支、:2880 LoadCommitMetadata、:3092 CompleteRestoreFromCommit、
   :3105 ResetRecoveryState）与同目录 TsavoriteLogRecoveryInfo.cs（恢复元数据载体）。
2. 即 C# 的恢复逻辑本就是「一段独立流程 + 独立元数据结构」，rust 搬进独立文件不违对标，
   且 rus可逐个函数把 /// 在 garnet 中的相对路径:函数名 锚点带过去。

修法
1. 新建 /Users/z/git/db/wedb/wedb/waof/src/wal/recover.rs：把 :190-:318 的 recover 主体与
   :328-:528 七个私有件整体迁入，以 impl<D: Device> WalLog<D> 分部实现承载；
   recover 入口在 log.rs 保留一行门面
   转调（或直接把 pub 入口定义在 recover.rs、log.rs 不再重复），禁出现两份实现。
2. 迁移保持顺序与文档注释：每个搬移函数的 C# 锚点逐条随行（check.js 按 File.cs:Fn 聚合，
   锚点丢失会被报缺失实现，禁改口径也禁顺手删注释）。
3. 若私有件依赖 log.rs 的私有字段，字段可见性走同 crate 私有访问（同模块树内无需 pub(crate)），
   禁为拆分扩大可见面。
4. 不借机改恢复算法（帧同步/CRC 过滤/截断点推导口径不动）。

验收判据
1. grep 对 WalLog::recover、WalLog::frame_sync、WalLog::verify_candidate、
   WalLog::read_recover_payload 的定义点各 1 处且同在恢复域载体内。
2. waof/src/wal/log.rs 行数 ≤450，恢复域文件 ≤350。
3. 恢复门面四件（WalLog::safe_initialize、recovered_cookie、recovered_committed_begin、
   recover_truncation）签名与语义不变，waof/wkv/wnode 调用点零改动。
4. cargo check 通过（禁在共享 target 跑 test.sh / clippy.sh）。
