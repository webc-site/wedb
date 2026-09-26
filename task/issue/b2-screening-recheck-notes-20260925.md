登记档（fix.md 批次2 甄别复核席，2026-09-25 05:44-06:05 五席并行，20 票双侧锚验逐锚现码复跑）：本档为甄别订正注记非待执行任务。20 票全部判定通过（16 doc-deviations + js-checkjs + wacl + wconf + wconn），wacl 已由并行席落地归档（合入 4b42130）。本档供各执行席落笔前消费，锚订正与执行注意以本档为增量补丁，票面冲突处按本档实位为准。

全局订正三条：
1 deviations.md 节号基线已变：wacl 票 4b42130 已占 §98，现册尾 §98。全部拟号新票（票面自称 §85/§86/§88/§89/§98 者一律失效）按让位条款取落笔当日册尾实况顺延，禁用任何写死号；同批多张 doc 票同落 deviations.md 时串行占号、后到者落笔前重读册尾只做增量。
2 doc-deviations-metrics-rounding-and-info-alias 系代码票非纯文档票：宗一含 wedb/wmetric/src/garnet_server_monitor.rs 两处函数名级改动（update_instantaneous_metrics cmd 面 :278 裸 .round() 与 round2 :481 换 round_ties_even 对齐 estimate.rs :137-142 既有单机制先例）加中点锁测（1 秒窗 byteUnit=1024 下 delta≡128 (mod 256)）；既有 instantaneous_metrics_underflow.rs 断言全整值，对齐零破测。宗二 STATISTICS 别名仍纯登记。派单按代码票对待。
3 纯文档票的票面验证条款若含全量 test.sh 项（pfcount-union 票方案 3）一律删去，全量门禁归主代理（task/fix.md §5），文档票验证以双锚 rg 命中对读为准。

分票订正（余 19 票在 task/ing）：

doc-deviations-32b-forget-reset-expiry-list-supplement
- 与 debug-gc-forcegc 票撞 §32b 条目 22 同号位：先入库者占 22、后者顺编 23，或同批一次编辑落齐勿覆写。
- 「已迁 task/done/」措辞改「内容已落地台账 §94、票档不在池」；既有 21 项文本一字不动；负 expiry .max(0) 面引用即停、MEET strict_i32 勿并句。

doc-deviations-acl-pluscmd-whitespace-trim-registry
- IsValidParse 行号以 :324-327 落笔，勿从票面审核注记抄 :325-329（源档本对）。
- 差分例钉 ACL SETUSER u "+ get"（C# +OK 授 GET / rust -ERR Command ' get' does not exist），可选锁测放 acl_parser.rs:560 测试区旁；引用 getkeys-numalias 注明怪癖族第二形勿扩其范围。

doc-deviations-bitfield-unknown-subcmd-echo-registry
- 落号勿插 §87 后（§88-§98 已占），按当日册尾顺延。
- 慢臂锚订正 slow.rs:885-898（调用 :889）；slot_mgmt 备查条 crate 前缀订正为 wedb/（wedb/wedb/src/server/cluster_session/slot_mgmt.rs:425）；ParseUtils.cs 实路径 garnet/libs/server/Resp/Parser/ParseUtils.cs。
- 差分锁（garnet_bitmap.rs 二进制 token 双形，fn bitfield_ro_read_only_and_overflow_policies 实位 :660）与让位注记两态互斥保持。

doc-deviations-conn-limit-global-count-granularity
- node_options.rs 锚系统性漂移约 +100 行：DEFAULT_BIND :46、DEFAULT_BIND_ANY :48、DEFAULT_NETWORK_CONNECTION_LIMIT :95、default_value_t :507、protected_mode :763-764 与缺省 fn :985、protected 分流 :1231-1234、unixsocket :1250-1252、<-1 定界 :1372-1377、解析测试 :2423 区；按实位落笔或降级符号锚。
- §84 实位 :1090-1099（activeHandler :1094）；「received 计数前移归 wconf 票、本条不复述」划界保持。

doc-deviations-debug-gc-forcegc-purgebp-registry
- rust 锚按现码订正：FORCEGC 臂 :458-481（strict_i32 :467、"GC completed" :479）、PURGEBP 臂 :483-516、num.rs :106/:139；或按票面自带建议降符号锚。
- §32b 现收束 :408；§30 现 :348-360 仅 PANIC 一条；条目 22 号位与 32b 票协调（见全局订正 1 与 32b 票条）。

doc-deviations-getkeys-numalias-keynum-overflow-registry
- 拟 §86 已被占，按当日册尾顺延禁落 §86。
- DEL=8 锚订正 RespCommand.cs:39（票面 :29 系源档笔误）；文档锚按现册刷新（§72 :913 起、§73 :932 起、§82 模板 :1066-1076）；宗 b 差分例经现码复算成立照写；体例模板径引 §92 现文（bitcount 先例已入册）。

doc-deviations-intparse-consumption-list-anchors
- 条目 11 零改动：现文 :398 已含订正后形态（SetCommands.cs:647 + r118-triage-set1 订锚注 + HashCommands.cs:228），stale-registry 范围补充二已入库，票面方案第 1 步「条目 11 并案落笔」跳过。
- 条目 17 rust 锚勿照抄票面自订 :514-515（现树 :511 系 ProtocolNotInteger 发射行非空行）：HELLO 解析已迁 parse_hello_args（mod.rs :901），§32 注＋strict_i32 实位 :914-915，取符号锚或 :914-915。
- C# 路径补 Objects/ 段（SortedSetCommands.cs、ListCommands.cs 三处）；RangeIndex 细锚 RIRANGE :416 起、语法注释 :407。
- §32a D 格式句订正成立（拒 "007" 系 NumUtils.cs:224 前置自查、D 格式本身符号后跳零）。

doc-deviations-lua-number-param-text-registry
- fallback 消费点锚按现码 :568 起或函数名符号锚（票面 :557-571 微漂）；C# coerce 注释锚 :3039-3042。
- 成稿与 §97 加一句互引防后审混淆（§97 折叠面在快路径 Err 臂、本条在 number 形参成帧臂）；五边界例双侧文本经现码推演成立（2^63 恰触 :383 上界经 as 饱和、-0.0 落整值臂归 "0"）照写。

doc-deviations-memory-shape-five-groups-registry
- 第二组「初始读尺寸全仓无对物」表述不准：whlog/src/hlog/mod.rs:198 与 whlog/src/hlog/io.rs:177/:195 有固定 4KB 探针并明注对标 C# InitialIORecordSize，缺的是用户旋钮面非机制对物；条目须写「旋钮缺席、内部固定探针」形并锚 whlog 两处，否则条目自身与现码矛盾。
- defaults.conf 无尺寸族区段（仅 reviv 族注释 :442-457），登记时尺寸族以 Options.cs CLI 在册面为锚。
- rust 锚实位：node_options.rs 内联容纳注 :123-124、pagecount 折叠注 :129-131、tree_cache_budget :157-165、wkv/config.rs :316-317、wreviv/bin.rs TAKE_RETRY_ROUNDS :43-46；票引 §77 :944-954 实为 :998、MutablePercent 先例现 §93 :1180。

doc-deviations-pfcount-union-dense-source-trymerge-drop
- 方案 3 全量 test.sh 项删去（见全局订正 3）。
- C# 真实位路径全形：libs/server/Storage/Session/MainStore/ 与 libs/server/Resp/HyperLogLog/，条目引用勿简写；模块头互引锚 :25-27（票引 :26-28 微漂）；:954 稀疏+稀疏增长姊妹面并入矩阵照写。

doc-deviations-repl-same-history2-fix-fork
- 锁测行锚订正：字段锁 replication_manager.rs :196-198/:268-279、is_empty 断言 :273（票面甄别自订 :277 实为 recovered 恢复断言）。
- C# 锚全形路径 libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs；语义锁三态经 pub disk_resync_strategy(:903) 断言，构造面复用既有 try_update_for_failover 测试形态。

doc-deviations-restore-payload-degrade-length32-registry
- length.rs 锚订正：自陈注 :17-20、case-2 跳信号字节臂 :29-33、写侧 try_write_length :42-71（甄别结论已订读侧两处、写侧本席补订）。
- 宗 a 与已落地 wnode-restore-10byte 修复（d42563f）对齐：恰 10 字节形现落 ERR_DUMP_VERSION_CHECKSUM 同族错误帧且会话存活，宗 a 补注按此实际形态写，勿按票面旧预留措辞。

doc-deviations-set-arith-absence-shortcut-divergence
- 登记条锚用订正实位：load_many :330-345、STORE 装载点 :272/:294/:316；短路臂矩阵四臂含 :496-500 后续键清空臂。
- 族域裁五命令、SDIFFSTORE 仅首键缺席一形，夹具勿扩；「避让在途 §85/§86」已过时，落号按当日册尾。

doc-deviations-setuser-new-fail-residue-registry
- 机制侧回指 doc/zh/db.md:294「ACL 管理命令直通存储」，勿写「deviations §3.4」（该册无此节）。
- C# 锚订正 :166-179/:196-199/:228-235；acl_commands.rs :334-336 既有注释已自陈事务性写穿，无须新增代码注释。

doc-deviations-zset-aggregate-three-divergences
- write.rs :925/:934 两处假 §2 锚改「§2 输入侧不变式延伸」并回指新条目实落号，:956-961 头注补回指。
- ZDIFF 负 numkeys 只可入观察句不入偏差账（翻案成立），锁测期望按翻案形（wrong-number-of-args 与 syntax error 双形）或删例。

js-checkjs-bare-layer-five-fabricated-anchors
- 宗三按 r113 订正：resp_list.rs:207 只换锚符号为 ListMoveWrongTypeDestinationDoesNotLoseElement(:1941) 不删锚。
- 宗四改锚 CanDoHIncrBy(:425)，StringValue 字段 HashIncrement 判定实位 :429-430。
- 宗五取 EvaluateWildcardScan(:812)（$..* 递归面）；EvaluateWildcardArray(:771) 是 "[*]" 非递归面勿取。
- symbolignore.yml:4-6 DRAM 孤儿条目清退后复跑 bun ./js/check.js 确认零新增红。

wconf-net-tls-two-unregistered-deviations
- 两宗登记落号按当日册尾顺延（票面 §85/§86 已被真实条目占用，§98 亦已占）。
- 票内行锚订正：§84=:1090、§36=:475；注释订正位 server.rs:1354-1356 与 consumer_registry.rs:520-541 区补裁决归属一句；验证=四词 grep 双侧命中对账+两锁测（:721/:737）维持绿。
- 所引 stale-registry-six-anchors 已落地现册 §94，直接写 deviations.md 根正本单源。

wconn-failrepl-offset-ascii-binary-frame-mismatch
- to_aof_binary/from_aof_binary 与 C# 同形时长度字段写 1 字节（对标 AofAddress.cs length 系 byte）；from_aof_binary 对前缀越界/超 MAX_SUBLOG_COUNT 回错帧。
- 同批改写 address.rs 头注 :7-11「编码面」段撤「无消费面不落地」断言并与新函数互指。
- mock 三处（wtest_base/net.rs:291-295、failover_primary_probe.rs:264-268、cluster_failover.rs:377）改造与「解码失败即测试失败」口径按票执行，交点首次入测。
- offset_waiters 回归锁按有界残留口径：观测 waiters_count 于下一次位点推进归零，非锁「不残留」也非锁「永驻」；应答侧维持文本与 C# RespClusterFailoverCommands.cs:155 一致，修形不扩面。
