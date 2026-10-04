终态：已合入 dev（2026-09-27）。39844f9 §169(撞号让位重编) 登记:纯读真值形严禁促热回改;flushed 冷态真值锁测+metrics_field 单趟游标泛化

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：纯读真值形，勿按 C# 促热回改；窄锁 metrics_is_live 旁

审核结论：通过（登记+锁测级，方向=维持 rust 纯读形严禁按 C# 促热回改。C# 促热确证：Locking.cs:160-181 两臂必改态（PromoteToTail RMW/RestoreTree），成功恒 is_live=true/is_flushed=false；rust ops.rs:581-589 纯读注册表反极性属实。裁决：is_live/is_flushed 系生命周期观测位非数据集回显不违 5.1（§94/§150 先例同型）；促热回改违板块 3/4。锚订正：heal.rs:350 系 #[test] 非生产路径；生产置位在 wbftree/manager/flush.rs:37/63，is_live=false 由 live_indexes 摘除致 get_tree 回 None）

RI.METRICS 存活与刷盘字段双侧反极性（rust 只读注册表报真值，C# 探针经 ReadRangeIndex 恒先晋升加惰性恢复后报 is_live true 与 is_flushed false，数据帧对错误帧）

查重结论：doc/zh/deviations.md 册内 RI 面仅三条在账（§68 预览门恒开、§83 RI.CREATE CACHESIZE 守卫、§150 判死异构键吸收形），RI.METRICS 回包面零登记；五池无同名或同面票（task/todo/wkv-promote-ri-chain-mid-command-generation-tear.md 系 FLUSHDB 换代域撕裂轴，与本条回包形正交，不复报）。命令文法与参数面本轮逐宗亲验无分叉，不另立案，收口句见末段。

问题分析：
1 Garnet 契约对齐（C# 一手形态）
C# RI.METRICS 的存活与刷盘两字段系「探针自身促热后的存根态」，非索引真实驻留态。链路：NetworkRIMETRICS（garnet/libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:600）→ RangeIndexMetrics（garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs:652）→ ReadRangeIndex（garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:108）。ReadRangeIndex 内两臂必在报值前改态：stub.IsFlushed 臂（:160-165）放锁、PromoteToTail 发 RIPROMOTE RMW（CopyUpdater 清刷盘标记，见 RangeIndexOps.cs:136-141）后 goto Retry；stub.TreeHandle == 0 臂（:167-181）放锁、RestoreTree 开数据文件并分配环形缓冲后 goto Retry。故 C# 成功出帧时恒 is_live=true（:681 判据即 TreeHandle 非零）、恒 is_flushed=false；恢复失败则 status=NOTFOUND（Locking.cs:177）→ 网络层回错误帧 ERR range index not found（RespServerSessionRangeIndex.cs:620-624），不出数据帧。C# 自有锁测只覆盖热态（garnet/test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:1779 RIMetricsBasicTest 断言 is_live true 与 is_flushed false），冷态与刷盘态双侧从未对拍。

2 工程现状确证（rust 现有实现路径）
rust 的 RI.METRICS 是纯读探针：network_rimetrics（wedb/wnode/src/resp/range_index/resp_server_session_range_index.rs:576）→ range_index_metrics（wedb/wkv/src/range_index/ops.rs:571）仅经 load_range_index_stub（wedb/wkv/src/range_index/stub.rs:202）读元记录存根，随后直取注册表 get_tree(&id_key) 判 is_live 并回 tree_handle 或 0（:581-584），全程不调 acquire_tree_read（wedb/wkv/src/range_index/stub.rs:256 的晋升与惰性恢复臂均在 acquire 内），is_flushed 与 is_recovered 直读存根位（wedb/wbftree/src/stub.rs:101）。检查点刷盘把 flushed 位持久进元记录（wedb/wkv/src/range_index/heal.rs:350 set_flushed(true)）且注册表条目同期摘除，故刷盘后的冷 RI 键在本仓 RI.METRICS 回 is_live=false、tree_handle=0、is_flushed=true，与 C# 同输入形三字段全反，且帧型本身分叉（rust 数据帧 vs C# 恢复失败时的错误帧）。既有 ops.rs:577-579 注释只自陈 is_live 判据一条，未述 is_flushed 的 C# 结构性恒 false、未述 C# 探针的写放大与建树副作用、未述错误帧形分叉，册面无锚可引。

3 逻辑危害确证
其一治理面：对拍与回归夹具在冷态或刷盘态跑 RI.METRICS 恒红且无登记据可引（同 §150 与 §130 观测面登记先例的绑架效应）。其二语义面：跨侧迁移的监控脚本判据反向——本仓 is_live=false 意为树未驻留、下一次点读才触发惰性恢复，C# 同名探针因自身已建树恒报 true，运维据此误判分层健康。其三资源面：C# 形使只读诊断命令变写放大与预算抢占源（每次 RI.METRICS 可把冷树整批拉驻、命中页缓存预算硬顶），违 task/review.md 板块 3 数据面零开销与板块 4 预算兜底纪律，rust 零副作用形系必守方向，严禁按 C# 促热形回改。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/range_index/resp_server_session_range_index.rs:network_rimetrics、write_metrics_resp
wedb/wkv/src/range_index/ops.rs:range_index_metrics
对照锚：wedb/wkv/src/range_index/stub.rs:load_range_index_stub、acquire_tree_read；wedb/wkv/src/range_index/heal.rs:mark_flushed 侧；wedb/wbftree/src/stub.rs:RangeIndexStub::is_flushed

对应 c# 文件与函数：
garnet/libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRIMETRICS
garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexMetrics
garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:ReadRangeIndex（IsFlushed 晋升臂与 TreeHandle 零值恢复臂）

精炼执行方案：
1 doc/zh/deviations.md 册尾顺位取号新增一条（先入库者得号、撞号让位不覆写），三宗并记：is_live 与 tree_handle 判据源分叉（注册表实况对促热后存根句柄）、is_flushed 的 C# 结构性恒 false 对 rust 可报 true、恢复失败时 C# 错误帧对 rust 数据帧；裁维持 rust 只读真值形并注严禁按 C# 促热形回改（回改即复活诊断命令的 RMW 写放大与预算抢占）。
2 两处锚回指注随条入库：ops.rs:range_index_metrics 头注补「C# 同命令经 ReadRangeIndex 先晋升加恢复后据存根报 live，本臂纯读注册表，登记条回指」，resp_server_session_range_index.rs:write_metrics_resp 头注补一句 is_flushed 在 C# 形下恒 false 的对照，杜绝后续席误读为转写漏项。
3 测试验证点：wedb/wnode/tests/range_index_tests.rs 既有 metrics_is_live 探针（:1430）旁补一枚窄锁——构造刷盘冷态（经既有 delete_index 故障注入或检查点刷盘口）后 RI.METRICS 回 is_live=false、is_flushed=true、tree_handle=0，同窗前后 RI.COUNT 值全等且元记录零 RIPROMOTE 入账（促热零副作用钉死），对照注释锚记 C# 同输入形并直引新登记条；不新增长测。

本席其余核验锚（坐实无分叉，按「双侧同缺一句收口」不立案）：RI 族在册名实为 RI.CREATE/RI.SET/RI.GET/RI.DEL/RI.SCAN/RI.RANGE/RI.EXISTS/RI.CONFIG/RI.METRICS/RI.COUNT 加同枚举双名 RI.LEN，双侧无 RI.PUT 名（task/review.md 射程列举按在册形订正）；参数个数错误帧文案与 Arity 逐项全等（check_arg_count 与 unpack_args/unpack_args_rest 口径对 C# parseState.Count 判定，RI.RANGE 与 RI.SCAN 的 COUNT 与 FIELDS 位置形同 C#）；错误帧族 RI.SET 的 ERR no such range index 与其余的 ERR range index not found、MEMORY 模式按命令覆写文案均已对位；预览旗面维持 §68 恒开裁决，rust 无残留门文案，未启用形在 rust 不可达（UNK_CMD 形只属真未知命令，与 RI 无涉）；RI 树身份四面（注册表、条带锁、claim、换号回收）均取 session_meta_key 物理域键（wkv/src/range_index/mod.rs:tree_identity_key），跨虚库同名索引互踩不成立；回包帧形方面，SCAN 与 RANGE 的 member 回显经 write_resp_bulk_string 二进制安全、乐观帧头回填支（wresp/src/ext.rs:backfill_resp_frame_head）位宽双向可移，RI.CONFIG 与 RI.METRICS 整型经 itoa 单点，均无分叉；RI.LEN 别名在两份命令目录 JSON 无目（仅 RI.COUNT 有目）与 RI.DEL 无字段缺席时的 :0 形，皆与 C# 同缺同形，并入本条不另立。
