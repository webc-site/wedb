check.js 门禁体系审查(r12,视角:检查器纪律与准确性)

方法:bun ./js/check.js 输出 /tmp/checkjs-r12.txt(exit 0);读 check.js/garnetScan.js/rustScan.js/symbolCheck.js;
ignore yml 逐文件抽查;A 类 miss 条目人工核 rust 对位;与 task/done/checkjs-dup-anchor-families.md、
checkjs-fake-anchors-cmdstrings.md 交叉。未改代码、未跑 cargo。


一、ignore 配置完备性

抽查 20+ 条(StartSizeTrackers 五处、ClusterSession.SetInternalWriteSession、ReplicaSyncSessionTaskStore、
HashObject Clone/Dispose、SortedSetObject Clone/Dispose、RespServerSession.SetTransactionMode、
RespReadUtils 11 符号条、modules.yml RoaringBitmap 族、RangeIndexManager UnregisterIndex/EnumerateFilesForReplication、
GarnetServerMonitor.GetAllLocksets、SessionScriptCache 两件、MetricsApi 整文件、IGarnetTlsOptions.UpdateCertFile、
TaskManager 整文件、NetworkBufferSettings、GarnetObjectSerializer、server.yml 整文件族)。

总体质量高:理由带 C# 行号、fixloop 票号交叉引用、归并去向明确;抽查理由全部核实成立
(CacheSizeTracker 确无对位、SetTransactionMode 唯一调用面确为 RunInTransaction、
UnregisterIndex 全仓确零调用、roaring crate 确承接容器族)。未发现「已有实现却留 ignore 遮蔽」的滥用。

问题 1(补 ignore,遮蔽缺口)
js/check/ignore/modules.yml 只登 NoOpModule 5 文件(DummyObject/NoOpCommandRMW/NoOpCommandRead/NoOpModule/NoOpProc),
garnet/modules/NoOpModule/ 实有 8 个 .cs,DummyObjectNoOpRead.cs、DummyObjectNoOpRMW.cs、NoOpTxn.cs 漏登,
按同一理由(示例模块)整面漏掩,当前以 miss 噪声形式挂着
(输出树 modules/NoOpModule/DummyObjectNoOpRMW、DummyObjectNoOpRead、NoOpTxn)。
建议:modules.yml 补该 3 文件整文件条目。

问题 2(ignore 理由与锚面失真)
js/check/ignore/garnet/libs/server/Objects/Types/GarnetObjectSerializer.yml 理由称
「Deserialize/DeserializeInternal/Serialize 锚点已由该单点承载」,实际
wedb/wcol/src/object_payload.rs 只锚 DeserializeInternal(:33,:43)与 Serialize(:48),
Deserialize 无任何锚,导致 check/miss/libs/server/Objects/Types/GarnetObjectSerializer.yml 恒挂 fn: Deserialize。
建议:object_payload.rs from_blob 补挂 GarnetObjectSerializer.cs:Deserialize,或修订 ignore 理由并补登该符号。

问题 3(行号漂移,弱)
common.yml RespReadUtils 条理由引用「wbase/src/num.rs:94 strict_i64」,实际已在 num.rs:112。
理由主体(Utf8Parser 白名单、strict 通道单点)核实成立,仅行号过期,顺手修。


二、miss 台账与词元提示段

台账同步:跑 bun ./js/check.js 后 git status 显示 js/check/miss 与 check/miss 双份台账均零 diff,
台账与当前输出一致,不过期。双台账(MISS_DIR_LI)为既有设计。

词元提示段:当前 891 个/306 文件(任务书所记 1251 已过期,提示面在收紧)。
构成实测(/tmp/token-census.mjs 重算):libs/storage/Tsavorite 616(69%)、libs/server/Storage 49、
libs/server/AOF 47、libs/server/Resp 40、libs/server/Databases 32、libs/server/Lua 27、
StoreWrapper.cs 16、cluster/Server 11、API 11。
判定:大头是 Tsavorite 内核替换面(whlog/wkv/windex 叙述性提名)与 wnode 归并层,属已知 B 类「不转写/归并」面,
test 族反而几乎不在(被 test.yml 2610 行覆盖)。构成健康,无需动作。

残留小项:modules/RoaringBitmap/RoaringBitmapCommands.cs 词元 4 个
(fn: Updater/Reader/NotFound/TryParseArgs,工厂三元组 + 参数解析),
wext_roaring/src/roaring_bitmap_commands.rs 已锚 TryParseUInt32 等 8 口,这 4 口未锚未 ignore。
建议:补锚或随 modules.yml 补登。


三、A 类「实现缺失」误报面(抽 20 条,17 条假缺失)

系统性根因 1:多对一归并不补侧锚。rust 把 C# 三层链(接口/包装/实现)收敛为单点后,
只有一枚 C# 符号能持锚,同链其余符号全部落 miss。同 rust fn 挂多枚 C# 锚不触发重复定义
(dupDefFind 按 C# 键聚合),即假缺失完全可修,存量未修。

系统性根因 2:锚挂「调用文件」不被拦截。symbolCheck 对符号做 \b 词法断言,匹配的是文件文本任意出现
(含调用点),不校验是否为定义处。锚挂错文件时 A 层假绿、真定义文件恒挂 miss。

20 条抽样与判定(rust 对位均存在,假缺失除非另注):

1 AdminCommands.cs:ProcessAdminCommands — 对位 wnode/src/resp/admin_commands.rs 分臂实现,
   admin_commands.rs:46 仅有叙述提名。建议:分派入口 fn 补锚。
2 AdminCommands.cs:CheckScriptPermissions — 对位 wnode/src/resp/resp_server_session/lua.rs:168,
   但锚误写 libs/server/Resp/RespServerSession.cs:CheckScriptPermissions(该处 :653 只是调用点),
   词法断言因调用点同名而放行 → 根因 2 实例。建议:lua.rs 锚改挂 AdminCommands.cs:95。
3 AdminCommands.cs:CommitAofAsync — 对位链 admin_commands.rs:147 NetworkCOMMITAOF →
   database_manager_base.rs:492 commit_aof。链上锚已挂,此辅助件未随挂。建议:同 fn 补多锚。
4 BasicCommands.cs:SetResult(:2064 static 输出助手)— rust MGET 输出组装重构无同名对位,轻度,建议 ignore 或随挂。
5 NumUtils.cs:TryParseWithInfinity — 假缺失。wbase/src/num.rs 严格浮点 + TryReadInfinity 白名单回落
   即其全文对位(:49-52 注释即写明),未挂锚。建议:num.rs 解析口补挂。
6 GarnetObjectSerializer.cs:Deserialize — 见问题 2。
7 Gossip.cs:Expired(:437 static 局部函数)— 惰性过滤谓词,rust 侧随宿主内联,建议 ignore(静态局部函数族)。
8 TxnKeyEntry.cs:GetKeyHash — SKILL 红线废除 Key Hash(集群以 ns->db 分片),合法不转写,建议补 ignore。
9 TransactionManager.cs:IsSkippingOperations(state==Started||Aborted 门)— rust wtxn 无同名门,需语义甄别票
   (真台账项,非门禁失真)。
10 TxnKeyManager.cs:WriteCachedSlotVerificationMessage — C# 纯转发(clusterSession 同名),
    rust 收敛进 cluster_session/slot_verify.rs:109。转发层假缺失,建议 ignore(转发层族)。
11 TxnKeyManager.cs:VerifyKeyOwnership — 同上,转发层假缺失。
12 IClusterSession.cs:NetworkIterativeSlotVerify — 假缺失。对位 slot_verify.rs:57,锚挂在具体实现文件
    RespClusterIterativeSlotVerify.cs 名下,接口符号未随挂。
13 IClusterSession.cs:ResetCachedSlotVerificationResult — 同上(slot_verify.rs:128)。
14 StoreWrapper.cs:TryGetOrAddDatabase — 假缺失,对位 single_database_manager.rs:137(锚挂 SingleDatabaseManager 路径)、
    database_manager_base.rs:102(锚挂 DatabaseManagerBase 路径)。
15 StoreWrapper.cs:TryPauseCheckpoints/ResumeCheckpoints — 假缺失,对位 single_database_manager.rs:158-167。
16 StoreWrapper.cs:ApplyAofSyncMaxLagBytes — wconf/src/config_meta.rs:35 有叙述提名无锚,补锚即可。
17 StoreWrapper.cs 其余 12 条(TakeCheckpointAsync/RecoverAOFAsync/ReplayAOF/CommitAOFAsync/Reset/
    FlushAllDatabases/TrySwapDatabases/CompactionTaskAsync/ExpiredKeyDeletionScan/Start 等)— 同族假缺失,
    对位散在 single_database_manager/database_manager_base/wkv gc,锚全挂侧链路径。StoreWrapper.cs 一文件 16 条全属根因 1。
18 DatabaseManagerBase.cs 20 条(CommitToAofAsync/RecoverCheckpointAsync/EnqueueCommit/GetKeyspaceStats 等)— 同族,
    单点 commit_aof 已锚 CompactionCommitAofAsync,其余侧链未随挂。
19 ServerOptions.cs:ParseSize — 假缺失。wconf/src/size.rs:115 try_parse_size_bytes 即其核(锚挂在包装口 TryParseSize),
    C# 里恰相反(ParseSize 是核、TryParseSize 是包装),补挂即可。PrettySize(:270)rust 无对位,需甄别(INFO memory 面)。
20 SingleDatabaseManager.cs:CommitToAofAsync — 假缺失,trait impl 在 single_database_manager.rs:530 无文档锚。

小结:A 类 miss 段大头是归并侧链假缺失,真缺口(IsSkippingOperations、PrettySize 等)被淹没其中。
建议:对 StoreWrapper/DatabaseManagerBase/IDatabaseManager/IClusterSession/TxnKeyManager 五个
归并侧文件批量「同 fn 多锚或 ignore 归并去向」清理一次,miss 段可缩一半以上。


四、重复定义段(当前 19 组,逐条甄别;与 done 票交叉:checkjs-dup-anchor-families.md 处理的是
GetKeysInSlot/GetArgSliceByRef/ConsumeDirect/SetMove 等旧 5 组,已换血,现行 19 组无旧票覆盖)

分类判定(全部为真双锚,无假警报):

A 共享内核收敛双锚(6 组):HashObject.cs{DeleteExpiredItemsWorker,expirationTimes,IsExpired,SetExpiration}
   与 HashObject.cs/SortedSetObject.cs:UpdateSize — expiry_ledger.rs(pop_expired/insert/is_expired_at/
   set_expiration/account_entry)与宿主包装(hash_object.rs/sorted_set_object.rs 同名方法)互挂同一 C# 锚。
   收敛本身正确(SortedSetObject.yml 已明示「双份收敛于此」),错在双层同挂。
   建议:内核持锚,包装层改叙述;UpdateSize 的 account_entry 助手同法。
B 包装分层双锚(2 组):GarnetClusterConnectionStore.cs:GetOrAddAsync(connection_store.rs:128 entry + :172 包装)、
   RMWMethods.cs:InPlaceUpdater(whlog inplace.rs:44 内核 + wkv modify.rs:58/106 会话层)。
   建议:每层挂自己真正对位的 C# 符号(InPlaceUpdaterWorker/InternalRMW 系),勿同挂一口。
C 片段锚(2 组):ClientCommands.cs:NetworkCLIENTSETINFO(wresp/cmd_strings.rs:490 abort_with_invalid_client_attr
   只实现其错误文案臂)、MainStoreOps.cs:GET(wnode user_read.rs:97 record_outcome 只实现其 metrics 入账臂)。
   建议:片段改叙述,整方法锚归主实现。
D 双消费双锚(2 组):Format.cs:TryParseAddressList(node_options.rs:810 endpoints + announce.rs:61 listen_endpoints)、
   FindRecord.cs:TraceBackForKeyMatch(wkv modify.rs:18 + read.rs:472)。建议:单点持锚,余者叙述。
E 测试挂生产锚(违反已立纪律,3 组 8 处):ReplicaSyncSession.cs:NeedToFullSync(tests/replication_manager.rs:461)、
   RMWMethods.cs:InPlaceUpdater(whlog tests x3 + wrecord tests x2)、MainStoreOps.cs:GET(tests/resp_tests.rs:2306)。
   task/done/checkjs-dup-anchor-families.md 第 4 组已立「测试不承载生产映射」,此 8 处为漏网/回潮。
   建议:撤测试侧生产锚(撤锚归位,非绕检)。
F C# 测试符号细分双锚(6 组):CandDoZIncrby(2)、RIDelFieldTest(2)、RISetAndGetBasicTest(3)、
   HelloAuthErrorTest(2)、SetExpiry(2)、SetExpiryHighPrecision(2)— 一枚 C# 测试被多枚 rust 细分测试共挂。
   README 允许测试挂映射,与 SKILL「check.js 无缺失输出」的收敛口径互斥,该 6 组结构上不可清零。
   建议:每族选一规范承接测试持锚,余者撤;或 README 改口径「测试锚不参与 dup 判定」并在 check.js 固化。


五、虚构锚与豁免

A 层:当前输出无该段,symbolCheck 实测 A 层 0,libs 族锚点全干净(16 枚 wresp 簇已由
checkjs-fake-anchors-cmdstrings.md 票修复)。

B 层 33 处甄别:
- 真虚构符号 3 处(全树不存在,须修):
  wedb/wnode/tests/resp_hash.rs:460 test/.../RespHashTests.cs:CanDoHincrbyErr — C# 实名为 CanDoHIncrBy/
    CanDoHIncrByWithExpire/CanDoHIncrByLTM(:425/:443/:459),无 Err 变体;
  wedb/wnode/tests/resp_list.rs:184 与 :207 RespListTests.cs:LMoveSameKeySingletonReturnsCorrectValue、
    LMoveDestinationWrongTypeDoesNotCorruptSource — C# LMove 实测为 CanUseLMoveGC 族(:780/:836/:869),
    两名皆虚构。建议:改挂实名或撤锚。
- 错挂文件 1 处:wedb/wext_json/src/error.rs:9 lib/server/Resp/CmdStrings.cs:RESP_NEW_OBJECT_AT_ROOT —
  符号真实但在 modules/GarnetJSON/JsonCmdStrings.cs:15,路径还少 s(lib/)。双重失真,建议改挂实名全路径。
- 截断路径(符号实存)约 29 处:缺 libs/ 前缀或半路径,如
  Storage/Session/StorageSession.cs:StorageSession(实存 libs/server/Storage/Session/StorageSession.cs)、
  Recovery/Recovery.cs:RecoverHybridLogAsync(wcpr 三处)、MainStore/RMWMethods.cs:InPlaceUpdater(wnode set.rs 两处,
  实为 C# CustomRawStringFunctions 的同形函数名,锚挂错族)、Databases/MultiDatabaseManager.cs:GetKeyspaceStats
  (wkv keyspace.rs:425,该符号真实落点在 StoreWrapper/IDatabaseManager/DatabaseManagerBase)。
  均为 B 层提示不拦截,属「另一族口径待收编」存量。建议:另票批量补 libs/ 前缀(纯注释修正,映射关系不变)。

symbolignore.yml 复核:
- 条 2(RespCommand.cs:OBJECT_)在用有效(wnode/src/resp/basic_commands/mod.rs:55 触发,豁免命中 1 即它)。
- 条 1(ReadCache.cs:DRAM)已成死信:全树无任何 ReadCache.cs:DRAM 形态锚(现 rust 侧 DRAM 均为纯叙述,
  wkv/src/session/raw/read.rs:717/:826、read_cache/mod.rs:64,不触发断言),理由所引 read.rs:366 行号亦漂移。
  无害但过期,建议:删条或更新叙述。


六、门禁机制本身

机制 1(symbolCheck 词法断言盲区,真缺口):断言只验「符号词形在文件文本中出现」,调用点/字段名/注释均可满足,
导致「锚挂调用文件」A 层假绿、真定义文件恒挂 miss(条三-2 实例)。
建议:断言改双通道——文件文本通过后再查 garnetScan 名录(fn_map/test_map)无定义时降为提示级
(调用点挂锚常见,不必硬失败,但要可见),防永久假缺失。

机制 2(dup 段口径互斥):README「测试可标注映射」+ SKILL:97「直到 check.js 没有缺失输出」+ dupDefFind
不区分测试,三者合取使重复段与词元提示段结构上永不清零,收敛计数口径失真。
建议:明确「测试不承载生产映射」入 README 并在 dupDefFind 固化(tests/ 目录 + 生产符号 → 提示级),
与 done 票既立纪律对齐。

机制 3(正面确认):语料降级汇报、兜底零名录大声报(SpanByteKey.cs)、YAML 解析失败硬失败、
miss 假缺失不落盘、ignore 自动淘汰带 diag,这一组纪律实测都在工作,未发现失真。


统计

ignore 抽查 20+ 条:理由全成立 0 滥用;问题 3 条(NoOpModule 漏登 3 文件、GarnetObjectSerializer 理由失真、
RespReadUtils 行号漂移)
miss 台账:与输出零偏差;词元 891 构成健康(69% Tsavorite 替换面)
A 类抽样 20 条:17 假缺失(归并侧链 13、锚挂错文件 1、可随挂 3),3 需甄别(IsSkippingOperations/PrettySize/SetResult)
重复定义 19 组:真双锚全数成立;测试挂生产锚回潮 8 处;细分族 6 组结构性不可清零
虚构锚:A 层 0;B 层 3 真虚构 + 1 错挂文件 + 约 29 截断路径
机制问题 2 条(词法断言盲区、dup 口径互斥)
任务书数字过期 2 处(词元 1251→891、README 补回 438→388)

视角结论:有增量
