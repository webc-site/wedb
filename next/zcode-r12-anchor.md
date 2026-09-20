文档映射锚全量质量收尾(r12)

统计
- 锚总量(全部注释行+块注释,CS_REF_REGEX 口径):4709 处出现 / 4336 个唯一「路径:符号」/ 分布于 672 个 rust 文件
- 形态分布:libs/ 全路径 3602;其它层级路径 751(test/standalone 569、modules/ 126、test/cluster 26、坏形约 30);裸文件名 356(159 个目标文件)
- 机器甄别(garnet 树方法级核对):实存且为成员 3290;词法命中但非方法 732(C# 属性/常量/嵌套类,合法成员锚族,如 GarnetServerOptions 属性族、CmdStrings 常量族、AofHeader 头结构族);符号在目标文件全无 4;路径按原文不存在 310(其中裸名可按文件名唯一解析约 280,层级截断 20 形 30 处,目标文件全树不存在 2)
- 重复定义组(doc 注释,dupDefFind 口径复算):19 组
- 生产 rust 挂 C# test/benchmark 符号:7 处;rust 测试挂 C# 生产 libs 全路径锚:8 文件 11 处

一 虚构锚(符号全树不存在,需修)
1. wedb/wnode/tests/resp_objects_dispatch.rs:67「RespSetTests.cs:CanAddListItems」——文件与符号双虚构。garnet 真实集合测试文件是 test/standalone/Garnet.test.collections/RespSetTest.cs(单数),其方法名为 CandDoSaddBasic/CanAddAndListMembers 等,无 CanAddListItems。建议:改锚 RespSetTest.cs 真实名或去形为散文
2. 同文件 :109「RespBitmapTests.cs:CanSetGetBit」——同病。garnet 无 RespBitmapTests.cs;位图测试在 test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs,近似真名 BitmapSetGetBitResponseTest(:193)。建议:改锚或去形
3. wedb/wnode/tests/resp_list.rs:184「RespListTests.cs:LMoveSameKeySingletonReturnsCorrectValue」与 :207「LMoveDestinationWrongTypeDoesNotCorruptSource」——两符号在 RespListTests.cs 不存在,系杜撰名。真实候选:CanDoBasicLMove(:1269)、CheckListOperationsOnWrongTypeObjectSE(:1579)/CanSendErrorInWrongTypeLC(:993)。建议:按测试本体语义改锚真名
4. wedb/wnode/tests/resp_hash.rs:460「RespHashTests.cs:CanDoHincrbyErr(非法整数)」——杜撰名(带中文注仍走 regex)。RespHashTests.cs 无此名,亦无任何断言 hash value is not an integer 的测试(C# 该文案仅存于 CmdStrings.cs:247 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER,rust 侧超前脸)。建议:去形散文或改锚 CmdStrings.cs 常量
5. wedb/wkv/tests/store/defense.rs:251-253「TsavoriteBase.cs:L226-265 / FindRecord.cs:L160-181 / InternalRead.cs:L73-125」——行号被 regex 捕为符号 L226/L160/L73,注册假符号。真实意图是 FindTag 与 TraceBackForKeyMatch。建议:改写为全路径函数锚(TsavoriteBase.cs:FindTag、FindRecord.cs:TraceBackForKeyMatch、InternalRead.cs:InternalRead),删行号形

二 锚语义错位(符号实存但挂错文件/层,词法断言测不出)
1. wedb/wkv/src/ttl.rs:7「PrivateMethods.cs:EvaluateExpire」——EvaluateExpire 定义在 libs/server/Storage/Functions/SessionFunctionsUtils.cs:32;三个 PrivateMethods.cs(MainStore/ObjectStore/UnifiedStore)仅调用它。且 PrivateMethods.cs 是三同名文件裸锚歧义族。建议:改锚 libs/server/Storage/Functions/SessionFunctionsUtils.cs:EvaluateExpire
2. wedb/wext_json/src/error.rs:9「lib/server/Resp/CmdStrings.cs:RESP_NEW_OBJECT_AT_ROOT」——三重错:lib/ 拼写(应 libs/)、文件错(该常量在 modules/GarnetJSON/JsonCmdStrings.cs:15)、前缀错(server 应 modules/GarnetJSON)。建议:改锚 modules/GarnetJSON/JsonCmdStrings.cs:RESP_NEW_OBJECT_AT_ROOT
3. wedb/wnode/src/resp/resp_server_session/lua.rs:162「libs/server/Resp/RespServerSession.cs:CheckScriptPermissions」——方法定义在 libs/server/Resp/AdminCommands.cs:95(internal bool CheckScriptPermissions),RespServerSession.cs:653 只是调用点。在途票 checkjs-anchor-multiline-fix 第 2 项「修正 CheckScriptPermissions 锚点路径」尚未落地,正确目标是 AdminCommands.cs。建议:按此改
4. wedb/wkv/src/session/raw/write/inplace.rs:380「...Implementation/InternalDelete.cs:InternalDelete.cs：TryFindRecordForUpdate」——文件名双写 + 全角冒号,regex 实际捕获符号为类名 InternalDelete 而非预期方法 TryFindRecordForUpdate,注册错符号。建议:整段改写为 InternalDelete.cs:TryFindRecordForUpdate(全角冒号改半角、去重复文件名)
5. whlog 层挂 server 层:whlog/src/hlog/inplace.rs:30 与 5 处测试(见四.1)把 Tsavorite 日志层「原位增长」锚到 libs/server/Storage/Functions/MainStore/RMWMethods.cs:InPlaceUpdater(server 语义层,:388)。Tsavorite 层对位是 InternalRMW.cs:144-196 的 InPlaceUpdater 分派臂。建议:whlog/wrecord 侧改锚 Tsavorite 层符号,wkv server 层保留现锚

三 裸文件名锚机器盲区(356 处,双门禁均不参与)
- 现状:symbolCheck 裸名跳过、isDocumentedAnchor 不登记,check.js 完全看不见;157/159 个目标文件可按名唯一解析,存量本身多数语义正确
- 目标文件全树不存在 2 处:即一.1/一.2(RespSetTests.cs、RespBitmapTests.cs)
- 同名多义目标 9 族:ReadMethods.cs/RMWMethods.cs/VarLenInputMethods.cs/PrivateMethods.cs/UpsertMethods.cs 各 3 变体(MainStore/ObjectStore/UnifiedStore)、CmdStrings.cs 2(server/cluster)、ReplicaSyncSession.cs 2(Diskbased/Diskless)、SessionParseStateExtensions.cs 2(cluster/server)、Options.cs 10(benchmark/playground)。风险实例即二.1:裸名歧义叠加挂错文件。建议:不追存量全量改写(成本高收益低),优先清二、一、五各点名处;新增锚一律全路径

四 测试挂生产锚与重复定义(19 组现状,与 r7-verify12 六残留交叉)
1. RMWMethods.cs:InPlaceUpdater 8 处挂:生产 wkv/src/session/raw/modify.rs:53/:94、whlog/src/hlog/inplace.rs:30(层错,见二.5)、wkv rmw.rs:82(裸形);测试 whlog/tests/hlog/inplace_lifecycle.rs:243/:352/:461 + wrecord/tests/record/lifecycle_and_chains.rs:449/:585 共 5 处(r7-verify12 时为 4,增长 1)。建议:测试去形(「对标 server 层 InPlaceUpdater 的 APPEND/SETRANGE 语义」散文),生产按二.5 分层归位
2. ReplicaSyncSession.cs:NeedToFullSync:wedb/wedb/tests/replication_manager.rs:458(测试)+ replication_manager.rs:896(生产,r8-sample-c 已核实三条件对位正确)。残留原样,仅行号漂移。建议:测试侧去形
3. GarnetClusterConnectionStore.cs:GetOrAddAsync:gossip/connection_store.rs:125(get_or_add_entry)+ :166(get_or_add)双生产挂。C# 单方法(:180),rust 拆两层。建议:本体(:125)持锚、包装(:166)去形
4. Format.cs:TryParseAddressList:wconf/node_options.rs:804 + wedb/src/server/announce.rs:59 双生产挂。C# 单方法。建议同上,announce 侧去形(node_options 为参数面正主)
5. ClientCommands.cs:NetworkCLIENTSETINFO:wresp/cmd_strings.rs:486(错误文案模板 abort_with_invalid_client_attr)+ wnode/client_commands.rs:339(命令实现)双挂。建议:cmd_strings 侧去形(模板承载者非命令本体)
6. hash_object 过期族宿主锚:r7-verify12 单点残留已扩为 6 组 dup——expiry_ledger.rs 分流引入后与 hash_object.rs 对同一批 HashObject.cs 符号双挂:expirationTimes(hash_object.rs:92/:311 vs expiry_ledger.rs:26/:52)、DeleteExpiredItemsWorker(:322 vs :122)、SetExpiration(:495 侧 vs :170)、IsExpired(:412 侧 vs :238)、UpdateSize(hash_object.rs:303 与 :578 account_entry 同文件内双挂)、SortedSetObject.cs:UpdateSize(zset 同构 :741/:751)。建议:按「数据结构本体(expiry_ledger)持锚、宿主(hash_object/zset)旁述去形」一刀切,消 6 组
7. 其余新面(此前无票):
   - FindRecord.cs:TraceBackForKeyMatch:wkv/src/session/raw/read.rs:472 + modify.rs:18 双生产挂(C# 单方法,read 侧为正主)
   - MainStoreOps.cs:GET(裸形):wnode/src/storage/session/common/user_read.rs:91(生产)+ wnode/tests/resp_tests.rs:2303(测试)双挂,建议测试侧去形
   - 测试×测试 6 组(C# 单测试对应多 rust 测试的自然分裂,低危但污染 dup 段):CandDoZIncrby(resp_sorted_set.rs:143/:1456)、RIDelFieldTest(service.rs:130 + range_index_tests.rs:406)、RISetAndGetBasicTest(service.rs:91/:152 + range_index_tests.rs:230)、HelloAuthErrorTest(resp_tests.rs:1128/:1139)、SetExpiry(:467/:498)、SetExpiryHighPrecision(:485/:608)。建议:每组首处持锚、余处去形;或接受为常驻噪声并在 checkjs 口径票裁决
8. 测试承载生产映射遮蔽面:C# 生产符号仅由 rust 测试文件注册的 2 例——SessionParseStateExtensions.cs:TryGetGeoSearchOptions(仅 wnode/tests/resp_sorted_set.rs:1484;生产面 sorted_set_geo_commands.rs:151 只有散文裸名)与 ConditionalCopyToTail.cs:ConditionalCopyToTail(仅 wkv/tests/compact/concurrency_and_collision.rs;生产 compact.rs 只锚 PostCopyToTail)。前者已整文件 ignore 兜底、后者被测试注册遮蔽,均不进 miss 台账。建议:生产面补全路径锚,ignore 可随之自动修剪
9. 测试不挂生产锚全景:8 文件 11 处(上述 inplace_lifecycle 3、lifecycle_and_chains 2、concurrency_and_collision 1、collision_chain 1、resp_sorted_set 1、resp_set 1(SetMove,done/checkjs-dup-anchor-families 第 4 组既定口径:存储侧单点锚落测试,不复议)、wdev/tests/device/parallel.rs:129(IDevice.cs:StartSegment 属性)、replication_manager 1)。建议:除既定口径两例外全部去形

五 生产 rust 挂 C# test/benchmark 符号(7 处,反向纪律面)
1. wepoch/src/epoch.rs:435、participant.rs:206 → Tsavorite cs/test/test.epoch/helpers/EpochProtection.cs:ProtectedScope/:Scope——C# 生产对位是 LightEpoch.cs(RAII Scope 由调用方手放),EpochProtection 是测试脚手架。建议:改锚 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs 相应符号或去形(注明 rust ProtectedScope 为自定义 RAII 化)
2. wbase/src/pool/mod.rs:677、inbox.rs:139 → cs/test/SectorAlignedBufferPoolTests.cs 两测试方法——生产行为注解挂测试名,生产对位是 SectorAlignedBufferPool.cs。建议:去形散文或改锚生产文件
3. whlog/src/hlog/mod.rs:312/:318、config.rs:168 → test.hlog/LogCommitFailureTests.cs、test.recovery/RecoveryTests.cs、test.hlog/ScanIteratorEpochFailureTests.cs——恢复契约以 C# 测试行为为规格书写,主锚(Recovery.cs:AsyncReadPagesForRecovery)已在位。建议:测试引用改为散文(去 .cs:符号形),避免把 C# 测试符号注册为已文档化、削弱 miss 台账对其 rust 测试的需求判定
4. 反例说明:wacl/src/acl_password.rs:93 挂 AclTest.cs:DummyPasswordHash 位于 #[cfg(test)] 模块,属测试对测试,合法,不在清理面

六 层级截断路径锚(20 形 30 处,symbolCheck B 层仅提示不拦截)
- 形态:少了 garnet 树前缀的「半截全路径」,路径按原文不存在。清单(文件:行):
  wedb/wkv/tests/checkpoint/recovery_single_pass.rs:3、wcpr/src/manager/recover.rs:250/:287、wcpr/tests/cpr/fuzzy_replay.rs:1(Recovery/Recovery.cs ×4);
  wkv/src/range_index/drain.rs:23(ObjectStore/RMWMethods.cs);wkv/src/ttl.rs:274(MainStore/RMWMethods.cs);wnode/src/resp/basic_commands/set.rs:243/:636(MainStore/RMWMethods.cs:InPlaceUpdater ×2——注意该符号在 MainStore RMWMethods 实存,仅路径截断);
  wkv/src/store/keyspace.rs:425/:426(Databases/ 两形);wcpr/src/manager/create.rs:179(Tsavorite/Implementation/ReadCache.cs);
  wnode/src/resp/key_admin_commands/keys.rs:719 + wnode/tests/expire_persist_latch_concurrency.rs:10(UnifiedStore/RMWMethods.cs ×2,符号 HandleExpireInPlaceUpdate 在该文件 :137/:195 实存,仅截断);
  wnode/tests/vector_key_domain_ops.rs:1(MainStore/ReadMethods.cs);wnode/tests/msetnx_atomic.rs:4(Resp/ArrayCommands.cs);
  wnode/tests/rmw_writeback_revalidate.rs:13 + wnode/src/resp/objects/object_store_utils.rs:1014(MainStore/UpsertMethods.cs:InPlaceWriter ×2,该文件 :68 实存 InPlaceWriter,仅截断;同处 MainStore/DeleteMethods.cs:InitialDeleter 在 MainStore 变体 :14 实存,仅截断);
  wnode/src/resp/resp_server_session/auth.rs:60(Resp/ACLCommands.cs);wnode/src/resp/garnet_api/mod.rs:303(Storage/Session/StorageSession.cs,ctor :86 实存);
  waof/tests/wal/commit_frame_skip.rs:2(TsavoriteLog/TsavoriteLog.cs);wbftree/src/manager/lifecycle.rs:159(ObjectStore/VarLenInputMethods.cs);
  whlog/tests/hlog/support.rs:221 + whlog/tests/hlog/commit_failure.rs:2 + whlog/src/hlog/mod.rs:318(test.recovery/RecoveryTests.cs ×3);whlog/src/hlog/mod.rs:312(test.hlog/LogCommitFailureTests.cs);
  wedb/tests/diskless_sync_write_window.rs:6(DisklessReplication/ReplicationSyncManager.cs);wedb/src/server/replication/assembly.rs:16(ReplicaOps/ReplicaDiskbasedSync.cs)
- 符号层面全部在各自真实文件实存(逐一核对),无一虚构;病 purely 路径截断。建议:机械补全 garnet 全前缀即可,30 处一次波可清

七 格式不可解析与去形散文(低危,与既定「旁述去形」口径区分)
- 坏形需修:仅二.4(inplace.rs:380 双写文件名)与一.5(行号形)
- 全角冒号形 7 处(session_parse_state_extensions.rs:3、txn.rs:1、basic_etag_commands.rs:6、garnet_api/slow.rs:177、failover 两文件 :21/:24、inplace.rs:380):regex 不吞,天然去形;其中 failover 两处、slow.rs 为刻意散文,保留;inplace.rs:380 因双写必须改写
- 断行截断 3 处散文续行(wkv/tests/store/tiered_drain_envelope.rs:12-13、wkv/src/session/raw/write/rmw.rs:40、wkv/src/session/raw/read.rs:325-326):语义均有他处正式锚或纯叙述,保留可;若顺手可并单行
- 在途票 checkjs-anchor-multiline-fix 第 1 项(SendCheckpointAsync 断行,wedb/src/server/replication/replica_sync_session.rs:114-115):现状未修,断行使全路径退化为裸名 ReplicaSyncSession.cs(机器可见性为零),票面修法(并单行)仍成立

八 漏锚密度(四核心模块,生产 src 面函数 doc 无任何 C# 锚)
- wkv/src/session/:173/197 无锚(87.8%);wnode/src/resp/:833/1256(66.3%);wedb/src/server/cluster_provider/:75/112(67.0%);wedb/src/server/replication/:304/456(66.7%)
- 判读:无锚面主体是 rust 自有基础设施(fmt/new/drop/getter/set、键编造、闩封装、flags 访问器),无 C# 对位,非缺口;抽样 meaningful 面(session/swap.rs:33 swap_databases、keys.rs:413 rename_sync、keys.rs:71 expire_at_ticks、replica_replay_task.rs:137 spawn)亦多为跨 C# 多符号的组合件或层内转发,正式锚常由被转发层持有。未发现成片真漏锚;真正的遮蔽型缺口是四.8 两例(test-only 注册),已在彼处给建议
- 机器可见性底数:356 处裸锚 + 30 处截断锚不参与映射登记与符号断言,合计约 8% 的锚在双门禁之外;这是后续任何「check.js 归零」目标的先决清理面

九 与既有票交叉确认
- task/reject/anchor-comment-cleanup.md(已拒,四子项并发落地):其验收面(slot_verify 四锚全形、GetKeysInSlot 三层分锚、复制链 receive_checkpoint_handler/fan_out_send、gossip GetMostRecentConfig)经本扫描复核无回潮,判定维持
- task/done/checkjs-dup-anchor-families.md 遗留 3 组(RecordInfo.cs:WriteInfo、TsavoriteBase.cs:FindOrCreateTag、Helpers.cs:FindTagAndTryEphemeralXLock)已不在当前 19 组内,确认已消
- r7-verify12 六残留全部原样在档(行号漂移),且 InPlaceUpdater 测试挂锚由 4 处增至 5 处、hash_object 宿主锚由 1 组扩为 6 组(expiry_ledger 分流副产),趋势在涨,建议优先派棒
- 在途票 checkjs-anchor-multiline-fix 两项均未落地,范围与本报告七.4/二.3 契合,勿重复立项,等该票或并入其范围

优先级建议(注释级,零行为变更)
P0 虚构与错挂:一.1-4、二.1-3(7 处,均有明确正确目标)
P1 涨势残留与遮蔽:四.1、四.6、四.8、五.3(生产语义被测试/测试脚手架锚位挤占)
P2 机械补全:六(30 处截断)、一.5、二.4、七.4
P3 dup 收口:四.3/4/5/7(去形即可归零)
P4 存量口径裁决:三(裸锚 356 处是否收编入 A 层断言)、四.7 测试×测试 6 组是否豁免

视角结论:有增量
