# checkjs-r1：miss 清单补锚与 ignore 登记（登记级·主控亲办）

## 背景
check.js r1（/tmp/_rs/checkjs-r1.txt，2026-09-28）输出「实现缺失」41 类约 66 项；双 Explore 席三态判定：**零真缺失**——20+ 项为功能已在、仅缺/偏 doc 锚点（A 类），其余属架构性不适用应登记 ignore（C 类）。重复定义簇逐验为合法分层/壳/委托（假阳，不立项）。

## 范围
### A 补锚（按判定席证据逐处落）
1. wmetric/garnet_session_metrics.rs：cs_getters!/cs_incr! 调用表 24 枚逐条挂 `libs/server/Metrics/GarnetSessionMetrics.cs:<名>` 锚
2. wmetric/info/garnet_info_metrics.rs get_metric：拆出独立行锚 `:GetMetricInternal`
3. wnode hash_commands/read.rs hash_length：补 `HashObjectImpl.cs:HashLength`
4. wcol hash_object_impl.rs：补 `HashObjectImpl.cs:GetByteSpanFromInput`
5. wnode bitmap/bitmap_commands.rs string_bit_field_action：补 `BitmapCommands.cs:HandleFirstSubCommand`
6. wbitmap bitfield/execute.rs write_bitfield：补 `BitmapManagerBitfield.cs:IncrementBitfield`
7. wbitmap bit_op.rs fold/vectorized_n：补 `BitmapManagerBitOp.cs:Vectorized512/256/128`
8. wresp catalog/data_provider.rs：补 `RespCommandDataProvider.cs:TryImportRespCommandsData`（现锚 RespCommandDataCommon.cs 异文件）
9. wresp catalog/commands_info.rs 子命令表：补 `RespCommandsInfo.cs:TryGetRespSubCommandsInfo`
10. wnode resp_server_session_output.rs write_ascii_bulk_string：补 `RespServerSessionOutput.cs:WriteUtf8BulkString`
11. wresp resp_memory_writer.rs：write_map_length→`:WriteEmptyMap`、write_ascii_bulk_string→`:WriteUtf8BulkString`
12. wconn parser.rs：补 `libs/common/RespReadUtils.cs:TryReadStringWithLengthHeader/TryReadStringArrayWithLengthHeader`（现锚 libs/client/RespReadResponseUtils.cs 路径漂移）
13. wnode logging.rs format_date：补 `LogFormatter.cs:FormatTime`
14. wedb cluster_session/migrate.rs:89：虚构锚改径 `libs/cluster/Session/…` → `libs/cluster/Server/Migration/MigrateSession.cs:GetLocalSession`
15. wedb replication/aof_sync_driver.rs：start_address 补 `AofSyncDriver.cs:GetStartAddress`
16. wedb replication/replication_history.rs：补 Copy/ToByteArray/FromByteArray 三锚
17. waof aof/address.rs derive(PartialEq)：上方注 `AofAddress.cs:Equals`
18. wnode aof/replaycoordinator：BarrierKey derive 注 `AofReplayCoordinator.cs:Equals`
19. wbftree service/ops.rs scan_with_count_callback/scan_with_end_key_callback：补 BfTreeService.cs 两锚
20. wkv store/flush.rs（或 addr.rs）：补 `AllocatorBase.cs:ShiftReadOnlyToTail`
21. wkv session/raw/write/rmw.rs try_rmw_sync/upsert_rmw：补 5 枚 ClientSession 文件 `:RMW` 臂锚
22. wkv read_cache/cleanse.rs evict_chain：`:53` 裸文件名改全路径 `ReadCache.cs:ReadCacheEvictChain`
23. waof wal/log.rs：unsafe_commit_metadata_only 跨行断径锚修复
24. wext_json set_get.rs json_set_need_initial_update：补 `JsonCommands.cs:NeedInitialUpdate`
25. wext_json json_path/filter.rs slice_indices：补 `ArraySliceFilter.cs:IsValid` + `ScanArraySliceFilter.cs:IsValid`
26. wext_json json_path/mod.rs select_nodes：补 `JsonExtensions.cs:TrySelectNode`
27. wext_json json_path/parser.rs parse_expression：补 `JsonPath.cs:TryParseExpression`；EnsureLength 锚挂边界守卫处
28. wnode tests/resp_* PingTest：补 `RespAdminCommandsTests.cs:PingTest` 锚
29. wedb tests/cluster_resp_session.rs delkeysinslot 测：补 `ClusterMigrateTests.cs:ClusterDelKeysInSlotRemovesStringAndObjectKeys` 锚
30. wedb/tests/migrate_source_vector_set_replica_converge.rs:6：虚构锚改径 `libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnDispose`

### C ignore 登记
- RespCommandDataProvider.yml：GetRespCommandsDataProvider（工厂动态装载，编译期单点替代）
- AttributeExtractor.yml：ConvertJsonToBinary、ExtractFieldsBinary（C# 全仓零消费死方法）
- CustomProcedureKeyHashCollection：AddHash、UpdateSequenceNumber（RUNTXP 存储过程面已删）
- CustomCommandManager 族（server.yml）：补漏 IsCustomCommandRegistered（静态枚举分发承接 custom_objects.rs:is_custom_object_command）
- DirectVirtualMemory.yml：munmap、VirtualFree、Clear（P/Invoke 私有臂由 RAII drop/demand-zero 承接）
- DeviceTests.yml 两例、LogFastCommitTests 一例（无被测对象：段尺寸强校验/无 omitSegmentId 形/无 commit-num 定向恢复面）
- RespAofTests：AofCustomTxnRecoverTestAsync（READWRITETX 动态注册整族弃案）
- RoaringBitmap：Enumerate（C# 零消费透传臂，rust 由 iter()/range() 直供）
- windex direct_vm.rs Free 锚已覆 munmap/VirtualFree 语义——若登记后仍报则保留登记口径

### 收尾
- `bun js/check.js --prune-ignore` 裁剪 10 份可淘汰 ignore
- `bun js/check.js` 复跑：实现缺失族清零（或仅余口径外项），记录前后对比
- cargo check 受影响 crate（宏属性改动面 wmetric 必查）；./sh/clippy.sh 与 ./test.sh --no-fail-fast 门禁
- 收票归档 task/done/ 记收口形态

## 边界
- 禁触他席在途脏文件：custom_object_commands.rs、resp_roaring_bitmap_tests.rs、task/ing/wnode-collect-fallback-blind-write-after-recheck.md 辖面
- 纯注释/配置面：不改任何函数行为

## 收口记录（2026-09-28 主控亲办）
- 合入哈希：f83bdbf1（dev 直入，登记级两提交 cb4432f7 立项 + f5254f63 立 r2 票 + f83bdbf1 落面）
- 形制：A 族 42 处补锚全部以注释行入库（git diff 复审：wedb 面增删零非注释行）；GarnetSessionMetrics 宏表 24 枚采 `//` 普通注释锚形（`///` 挂宏调用触发 unused_doc_comments，rustScan 对全文注释提取故采信等价）；C 族 8 面 ignore 登记（AttributeExtractor/CustomProcedureKeyHashCollection/DirectVirtualMemory 三份新建 yml + RespCommandDataProvider/test.yml×2/Tsavorite cs test.yml/server.yml 追补）；`--prune-ignore` 落盘 12 份
- 终态：`bun js/check.js` 实现缺失/虚构锚点/可淘汰 ignore 清零，miss 树 41→0；symbolCheck A 层 0、B 层 42 处移交 checkjs-r2 专席票
- 门禁：`./sh/clippy.sh` RC=0（含 --fix 零落笔，树余他席 4 件未触）
- 附带销号：红灯册唯一在案件 store_dest_cold_upgrade::sunionstore_cold_page_overflow 于本尖端实测绿（他席 collect-fallback 修 097677e7 合入所致，verify worktree 单测复核）
