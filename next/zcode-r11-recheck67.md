轮11 复核:轮6/轮7 意见现状重验(逐条以代码现状为唯一标准)

方法说明:轮6/轮7 报告原文件已不在盘上。r6-scan/r6-cli/r6-doc/r7-verify12/r7-verify56 取 git blob 原文(20161b44/9c876e96/a24ec6fb/63cd3ec6/18f1761e);r6-mem/r6-del/r7-ops/r7-redteam 以认领票回构(逐条对应);r7-verify34 全文不可恢复(git 无对象、task/ 无归档,仅计划表留判定摘要)。票态以 task/{ing,done,reject} 现存为准,所有行号为当前 HEAD 实读。

轮6-zcode-r6-mem.md(3 条,票 2 张)

1. 意见:zset/成员 TTL 账本多处独立复制成员字节,记账只计一份,MEMORY USAGE 低报 2-3 倍,升降阶内存门限同倍放宽。
   判定:仍在(在途)。
   证据:票 task/ing/r6-mem-zset-member-byte-dedup-accounting.md 未合并;wcol/src/zset/sorted_set_object.rs:154/:156 双容器仍各持独立 Vec<u8>,装载点 sorted_set_object_impl.rs:354/:357 与 :399/:402 仍双 member.clone();account_entry(sorted_set_object.rs:751-754)仍单份 round_up+SLOT*2;expiry_ledger.rs:57-65 insert 仍 clone 键且固定加 SLOT*4。

2. 意见:升阶树页环 16MiB 整块常驻,无全局总闸,聚合常驻=树数×16MiB,并发升阶峰值无闸。
   判定:仍在(在途)。
   证据:票 task/ing/r6-mem-tiered-tree-cache-global-budget.md 未合并;wbftree/src/types.rs:86 cache_size 默认 16MiB 原样;wkv/src/range_index/promote.rs 与 wbftree/src/manager 无任何预算/信号量(grep budget|semaphore|总闸 零命中);r9-soak:47 亦以此面为前提佐证仍未闭合。

3. 意见:MEMORY USAGE 升阶臂注释「树页冷热换入换出,非常驻,不误报」与页环整块分配事实相反。
   判定:仍在(在途,并入上票)。
   证据:wnode/src/resp/basic_commands/mod.rs:629 注释原文在位,未订正。

轮6-zcode-r6-del.md(5 条)

1. 意见(P0):SET 覆写升阶树键同步快路径裸删 Meta 且零 AOF 镜像,主从发散、副本残留幽灵树。
   判定:误报(后果链不成立),整票经端到端实测驳回;残余另有悬空(见增量1)。
   证据:task/reject/zcode-r6-rejects.md §二/§三(把修复回退旧码后 RangeIndexDrop 零触发、副本 TYPE/GET/SMEMBERS/树注册全清断言全绿)。本人独立重走链路复核:三条字面事实属实——wkv/src/session/raw/write/mod.rs:162-171 Meta 在场裸清退(try_delete_raw_sync+unregister_bftree_key+delete_index,不发 RangeIndexDrop)、wnode/src/service.rs:178 Write 臂 Meta 不在 String/Acl/DbMeta 放行集、异步单点 drain.rs:37-61 完整;但副本回放 aof_processor_store_ops.rs:77-80 store_upsert→upsert_string→upsert_tag(String) 走主端同一写共同体,SET 命令本身确定性重放即副本自清,RangeIndexDrop 在此路径冗余。「主从发散」不成立,P0 标签应撤销;残留真问题是「两套树清退机制并存」的一致性收敛题,非正确性缺陷。

2. 意见(P0):过期清退「判过期→落删除」两段间无键闩,并发 SET 已 ACK 的新值被整键墓碑。
   判定:仍在(在途),P0 判定成立。
   证据:票 task/ing/r6-del-expire-purge-key-latch.md 未合并;wkv/src/ttl.rs purge_expired(:337)与 check_expired(:434→:446)全程无 try_lock_key_exclusive(全仓持闩点仅 expire_at:481/persist:533 与 wnode 六处 try_rmw_window);gc/ttl_sweep.rs:85 probe_ttl/:181 check_expired 两入口原样;SET 同步快路径(write/mod.rs:157-160)清 TTL 臂亦无闩,窗口真实。修复须 purge 链闩内重判+SET 清退臂一并收口,票面方案仍适用。

3. 意见:对象 RMW 写回与 DEL/SET 单边无互斥,DEL 后信封盲插复活、SET 后双域并存。
   判定:已修复。
   证据:票 task/done/r6-del-rmw-writeback-revalidate.md;提交 9db2e25a「对象信封 RMW 落笔前终态复验」+ 2f422833 回归(四交叠+两正向);17ce5cd3 rmw_window 头注改指复验承接。

4. 意见:同步 EXPIRE/PERSIST 快路径绕键闩,双会话并发 EXPIRE 回 1 而 TTL 丢失。
   判定:已修复。
   证据:票 task/done/r6-del-sync-expire-persist-latch.md;wnode/src/resp/key_admin_commands/keys.rs:728-729/:778 expire_apply_sync/persist_apply_sync 入口取 try_rmw_window 闩内读改写,失闩沿 Ok(None) 降级;tests/expire_persist_latch_concurrency.rs 在位。

5. 意见:迁移 DELETING 收口删源吞错静默进 MIGRATED;并断言 C# 删源失败上抛中止迁移。
   判定:驳回(主诉),残端未立项(见增量2)。
   证据:task/reject/zcode-r6-rejects.md §一至§四:C# MigrateOperation.cs:250/:261 同为 `_ =` 丢弃 DELETE 结果、catch 后强制 return true,「上抛中止」系票面误读(r7-verify56 亦先行判「表述过强」,与终裁一致);wedb/src/server/migration/migrate_driver/keys.rs:913 `let _ = storage.delete_string(key).await` 现状未动。驳回书认定的极小 1:1 缺口(Err 时补 log::error 对标 MigrateSessionKeys.cs:214-217)至今无票。

轮6-zcode-r6-scan.md(现存 3 条 + 原问题1)

0. (原问题1,认领时移出文件)分层 SCAN 到期成员先耗游标配额致跨页重复。
   判定:已修复。
   证据:票 task/done/tiered-scan-expiry-skip-order.md;scan.rs:301 member_expired_at 判定已先于 skipped<start;r9-recent:60 佐证提交 4d1bca6。

2. 意见:主存储 SCAN 向量键仅并入 cursor==0 首页且可超 COUNT,C# 按扫描序分布各页。
   判定:仍在(在途)。
   证据:票 task/ing/scan-cursor-semantics-align.md(问题2/3/4 合票)未合并;garnet_api/slow.rs:389 仍 `filter.cursor == 0` 才并 for_each_domain_user_key,后续页恒无。

3. 意见:COUNT<=0 与 TYPE 空串口径偏离 C#,注释「同语义」与实况不符。
   判定:仍在(在途)。
   证据:array_commands.rs:106「C# acceptedCount >= count 同语义」注释原样;TYPE 空串臂未动。

4. 意见:跨升/降阶分页游标发生迭代序域切换,重复/漏项语义未登记。
   判定:仍在(在途)。
   证据:tiered_collection_ops/scan.rs 头注(:223-234)与 doc/zh/collection.md 均无「游标仅为提示」声明。

轮6-zcode-r6-cli.md(13 条,3 驳回 10 在途)

1. 意见:复制域六启动旋钮无用户配置通路,恒取 Default。
   判定:仍在(在途)。
   证据:票 task/ing/cli-repl-options-wiring.md;wconf/src/node_options.rs 六字段零接线(grep 零命中),boot.rs:160 消费 reestablishment 恒默认。

2. 意见:aof-size-limit/index-max-size 尺寸串非法静默降级跳过后台任务。
   判定:仍在(在途)。
   证据:票 task/ing/cli-size-and-slowlog-validation.md;validate()(:865 起)无 size 预解析臂;service.rs wire_background_tasks 仍 `if let Some` 静默跳过。

3. 意见:--fail-on-recovery-error 缺失且默认臂相反(rust 恒拒启)。
   判定:驳回(有意收紧,fail-fast 为第一原则)。
   证据:task/reject/zcode-r6-cli.md #1;现状无开关,aof/garnet_append_only_file.rs:388/:397 map_err 上抛维持;原意见「若收紧须在 node_options.rs 登记裁决」未做,裁决记录仅在 reject 票。

4. 意见:mutable-percent 生效默认 rust 0.5 vs C# defaults.conf 90,注释把 C# 默认写成 50。
   判定:仍在(在途)。
   证据:票 task/ing/cli-mutable-percent-and-help-align.md(问题4+13 合票);whlog/src/config.rs:20 DEFAULT_MUTABLE_FRACTION=0.5、node_options.rs:93 注释「MutablePercent = 50」均原样。

5. 意见:log_level 缺省应 Info→Warning 对齐。
   判定:驳回(默认 Info 保留,可观测性取舍)。
   证据:task/reject/zcode-r6-cli.md #2;logging.rs:421 minimum_level Info 原样。

6. 意见:tls_client_cert_required 默认应 false→true 对齐并订正注释。
   判定:驳回(默认 false 保留,单向 TLS 为行业默认)。
   证据:task/reject/zcode-r6-cli.md #3;node_options.rs:742 false 原样;注释口径错(「字段初值当生效默认」模式)亦未订正,与 verify56 增量 c 相关部分仍悬。

7. 意见:集群域参数无 nested_text 承载面,文件同名键被静默忽略。
   判定:仍在(在途)。
   证据:票 task/ing/cli-cluster-args-nested-text.md;wedb/src/args.rs ClusterArgs 仍未派生 Deserialize(grep 零命中),未知键仍无 warn。

8. 意见:IPv6 监听不可达,bind 裸 v6 拼接产坏串且回退单栈。
   判定:仍在(在途)。
   证据:票 task/ing/cli-ipv6-listen-formatting.md;endpoints() 仍 `format!("{a}:{}", self.port)` 裸拼,DEFAULT_BIND 单栈原样。

9. 意见:slow_log_threshold (0,100) 开区间拒启臂缺失。
   判定:仍在(在途,并入问题2 票)。
   证据:validate() 无此臂;default_slow_log_threshold(:677)仅缺省值。

10. 意见:Vector Set 两任务数旋钮缺+默认差(0→4 硬编码、replay 无对位)。
   判定:仍在(在途)。
   证据:票 task/ing/cli-vector-set-task-options.md;vector_manager.rs:263-264 `0 => 4` 原样;replay_task_count 配置面仍无。

11. 意见:file-logger 打开失败静默跳过无告警。
   判定:仍在(在途)。
   证据:票 task/ing/cli-logger-console-quiet-options.md;logging.rs:471-474 `.ok()` 丢弃原样,近旁「按 C# 异常装配面收敛为跳过」的误述注释(verify56 增量点)同样原样。

12. 意见:控制台输出管理面缺 -q/--disable-console-logger 开关。
   判定:仍在(在途,并入上票)。
   证据:logging.rs:453 disable_console() 装配器有口全仓零生产调用;node_options.rs 无 quiet/disable-console-logger 键。

13. 意见:键名/拼写/单位漂移清单,建议 help 标注。
   判定:仍在(在途,并入问题4 票)。
   证据:aof-commit-ms(:425)、index-resize-frequency(:635)、cluster-node-timeout-ms 等键名原样,help 未标单位。

轮6-zcode-r6-doc.md(5 条,合票在途)

1. 意见:README/readme 三层职责声明与拓扑倒置,wnode 被述为「零存储零 AOF 零集群」。
   判定:仍在(在途)。
   证据:票 task/ing/doc-readme-and-db-align.md(问题1-5 合票);README.md:98/:274、readme/{zh,en}.md:81 原句在位。

2. 意见:拓扑图 wedb_standalone 五条依赖边实为 dev-dependencies。
   判定:仍在(在途)。
   证据:README.md:69-74 五条实边原样。

3. 意见:对标矩阵给 wcol 加了不存在的「BfTree 范围索引算子层」。
   判定:仍在(在途)。
   证据:README.md:105/:281、readme/{zh,en}.md:88 原句在位。

4. 意见:doc/zh/db.md §4.5 虚构 ClusterMsgFlushAll 帧(类型/宽度/符号三错)。
   判定:仍在(在途)。
   证据:doc/zh/db.md:377 原句在位;真实面仍为 gossip 扇出 CLUSTER FLUSHALL_NS。

5. 意见:db.md 路径锚漂移三处(garnet_api.rs、vdb/gc.rs、wkv/src/gc.rs)。
   判定:仍在(在途)。
   证据:db.md:65/:92/:103 三处按锚索骥仍落空。

轮7-zcode-r7-verify12.md(68 条复核判定 + 4 组增量)

主体可靠性:抽核 6 处「已修复」判定(gossip_manager.rs:256 initialize_async、bftree_release.rs:51 expired_at 门控、wedb/Cargo.toml:238 unwind 维持、runtime_server_options.rs:106 witness 10ms、drive.rs:46 catch_unwind、storage.yml 3768 行在案)全部相符,判定可信。其 68 条意见本体属轮1/2,代码面归并行代理,不越界重验。

仍在 2 条:均仍成立——storage.yml 无 MainStore/ObjectStore/UnifiedStore 登记(grep 零命中);wkv→wconf 反向依赖原样(config.rs:5、gc/compact.rs:10)。
在途 6 条:对应票 test-cluster-divergent-and-restart/test-migration-tombstone/test-hyperloglog-payload-and-merge/test-wconn-client-features/test-wedb-integration-move/r4observe-stats-wiring 全部仍在 task/ing,判定不过期。
驳回 4 条:维持。
增量 4 组(锚形残留):全部原样且仍无票——whlog/tests inplace_lifecycle.rs:243/:352/:461 与 wrecord/tests lifecycle_and_chains.rs:449 测试挂生产全形锚 InPlaceUpdater、wedb/tests/replication_manager.rs:457 NeedToFullSync;gossip/connection_store.rs:125/:166 双 GetOrAddAsync;wconf/node_options.rs:804 与 wedb/src/server/announce.rs:59 双 TryParseAddressList;cmd_strings.rs:489 与 client_commands.rs:339 双 NetworkCLIENTSETINFO;hash_object.rs:322 过期族宿主锚。task/ing/checkjs-anchor-multiline-fix.md 范围不同(SendCheckpointAsync 断行),不覆盖。

轮7-zcode-r7-verify34.md

全文不可恢复(git 全对象扫描无 blob,task/ 无归档;6e86b64 归档时仅收 verify12/56)。计划表载:35 条,已修复 18/在途 10/驳回 3/误报 2,视角结论已穷尽。无法逐条重验,登记为档案缺口:其 18 条「已修复」判定与 10 条在途票的时效性均成不可审计黑箱,建议后续轮抽样重验或在计划表声明缺失。

轮7-zcode-r7-verify56.md(48 条判定)

r6 部分(33 条):逐条与我本次直接核码一致(mem3/del5/scan3/cli13/doc5 的「成立」判定全部复核属实;仅下述 1 处错判)。其两项增量(c 口径错位模式、r6-del5 C# 对位误读)经本轮独立核实成立。
错判 1 处:对 r6-del-1 判「成立(P0)」——复核只证到「同步臂不发事件」三层事实,未核副本经 StoreUpsert 重放走同一写共同体的自清臂;fixloop 实测驳回已纠正(见轮6-del-1 条)。P0 标签按驳回书撤销。
r5 部分(15 条):属并行代理射程,不越界深核;顺带观察两条已由在途转已修复——r5-restart1 恢复实例 reclaimer 漏挂(open_shared 现无条件挂 spawn_bftree_reclaimer,wkv/src/store/mod.rs:534,票 task/done/recover-reclaimer-mount.md)、r5-restart2 向量登记表恢复回建(提交 7d16779f,票 task/done/vector-registry-recovery.md);其余 r5 相关票(repl 背压两张、gc-compact-store-flight-gate、api 卫生四张等)仍在 task/ing,与判定一致。

轮7-zcode-r7-ops.md(2 条)

1. 意见(P0):单机/集群二进制共用 --dir 物理件互踩,无异构残留预检。
   判定:已修复。
   证据:票 task/done/r7-ops-mode-shared-dir-collision.md;合并 33deadc 取「统一数据文件名互认」方案:wconf/src/node_options.rs:47 DATA_FILE="wedb.db" 单一命名(:855 注释对标 C# GarnetServer.cs:479-484),boot.rs:65 与 wedb_standalone/main.rs:76 共用 node.data_path();tests/mode_shared_dir_collision.rs 161 行回归在位。

2. 意见:恢复趟缺数据设备长度预检,备份三件套语义未登记。
   判定:仍在(在途)。
   证据:票 task/ing/ops-cpr-device-len-preflight.md;wcpr/src/manager/recover.rs:121-150 仍只有地址自洽校验(tail<initial/begin>tail/head>tail),无设备物理长度 vs tail_address/flushed_until 预检;doc/zh/db.md 备份节仍未补。

轮7-zcode-r7-redteam.md(1 条)

1. 意见(P0):阻塞族经纪注册裸键+单例取件会话,跨租户 ns0 越权读写/串扰。
   判定:仍在(在途),P0 成立。
   证据:票 task/ing/broker-namespace-isolation.md。本人重走链路:注册键为裸用户键(list_commands/blocking.rs:95-99 parse_state 直 map to_vec,sorted_set_commands/blocking.rs 同);broker 单例 assembly(service.rs:802-805 CollectionItemSource::new(store.new_session()))——新会话默认 ns0/db0 域且无 set_context;collection_item_source.rs 全文件无 namespace/prefix 概念,obj_load_typed_sync 直用裸键;keysToObservers 为进程级单 map(collection_item_broker.rs:57)。任意非 0 ns 会话 BLPOP/BZPOPMIN 等可经经纪消费或注入 ns0 同名键数据。r9-client-compat:5 亦以此面为已立在途前提。修法按票面三建议(ns 前缀隔离注册键/按观察者域动态设前缀/跨 ns 回归测试)。

统计

轮6:30 条(含 scan 原问题1)= 已修复 3(del3、del4、scan1)+ 驳回 5(del1 误报、del5、cli3、cli5、cli6)+ 仍在/在途 22(mem3、del2、scan2/3/4、cli 十条、doc 五条)。
轮7:verify12 主体判定经抽核可信,其 2 仍在/6 在途/4 增量组时效无变化;verify34 不可核(档案缺失);verify56 r6 部分判定除 1 处错判(del1 P0)外全部成立,r5 部分 2 条已转已修复;ops 2 条=已修复 1+在途 1;redteam 1 条=在途成立。
高价值 P0 三条终判:redteam 经纪 ns0=成立未修;过期清退丢 ACK=成立未修;SET 覆写主从发散=误报(实测驳回,副本重放自清),残余为清退机制去重题。
本轮未修改任何代码、未运行 test.sh/clippy.sh/cargo。

视角结论:有增量

增量1:verify56 对 r6-del-1 的「P0 成立」系错判(漏核副本重放臂),已被实测驳回纠正;但驳回连带处置失序——7b7fe37 声明「残余转去重立项」,而 task/{ing,done,reject} 无任何清退去重票,「两套树清退机制并存」(同步臂 163-171 vs drain.rs 单点)现处无主状态,悬空。
增量2:r6-del-5 驳回书自认的极小 1:1 残端(keys.rs:913 删源 Err 补 log::error,对标 MigrateSessionKeys.cs:214-217)要求「另开专票」,至今无票,残端悬空。
增量3:r7-verify12 的 4 组锚形增量(测试挂生产锚、gossip/announce/CLIENTSETINFO 三处双锚、hash 过期族宿主锚)全部原样且无票认领,随 check.js 重复定义段噪音持续累积。
增量4:r7-verify34 报告全文不可恢复(git 无对象、盘上无归档),其 35 条判定(含 18 条「已修复」)不可审计,属复核体系档案缺口。
增量5:r6-cli 三条驳回(3/5/6)落票后,原意见附带的「若收紧须登记裁决」半臂未执行——node_options.rs 侧无 fail-fast/日志级/TLS 默认的裁决注释,裁决仅存 reject 票,代码读者无从知晓系有意差异。
