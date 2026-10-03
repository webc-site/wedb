终态注记（2026-09-30 收口）：已合入 dev，分支提交 3375013、合入哈希 daeddd8（--no-ff）。收口形态：纯文档订正——doc/zh/deviations.md 十节（§4/§12/§14/§20/§27/§35/§53/§58/§104/§109）符号锚按现码实名替换（§4 convert.rs 饱和算式族现名；§12 单数 aof_boot_gate_violation；§14 PROBE_PRESERVE_WATERMARK；§20 parse_scan_filter 且来源路径订正为 wedb/wnode/src/resp/array_commands.rs；§27 HybridLog::recover 类型名伴锚消歧裸名；§35 StoreSession::check_expired；§53 types::member_ttl 族加 SortedSetObject::sorted_set_expire；§58 删 parse_auth_args 注并入 parse_hello_args；§104 sorted_set_commands/write.rs；§109 RespCommand::from_cs_name）。判据文字与来源票指针不动，零行号锚、零代码改动。验证：十宗旧锚全仓 grep 零命中复证，新锚定义级唯一命中核验通过。

甄别结论:通过(P3,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-A，P3 级）。deviations 册十宗符号锚在现码不可解析（改名/目录重构/复数误写）事实确证，判据语义经核验均在码；纯文档与注释订正，零行为代码改动。执行席遵照：按问题分析 2-11 逐节替换符号名与路径锚，保持判据文字不动，严禁新增行号锚，验证全仓 grep 唯一命中。

原票面：
deviations 册符号锚灭失批量十宗：登记符号在现码零命中或登记路径不存在，按符号名锚恢复判据的通道断裂

问题分析：
1 册规（册头声明 4）：每节只登判据加符号名锚（函数/类型/常量/文件名），严禁钉行号——符号名是唯一恢复通道，登记符号现码零命中即锚灭失（§32 族锚漂曾消耗整张订正票 task/done/doc-deviations-sec32-list-anchors-drift.md，同谱）。全册锚面系统扫描后，下列十宗登记锚现码不可解析（判据语义面均经核验仍在码，仅名字/路径漂移，故批量一票订正）：
2 §4：锚 to_absolute_timestamp、to_ticks_duration 零命中。现码 wbase/src/convert.rs 同族饱和算式为 expire_after_to_ticks、expire_at_seconds_to_ticks、expire_at_milliseconds_to_ticks、compute_expiration_ticks（§4/§4a 注释锚在 convert.rs:155/217 完好）。
3 §12：锚 aof_boot_gate_violations（复数）零命中；现码 wedb/wedb/src/server/boot.rs 为单数 aof_boot_gate_violation，来源票 task/done/wnode-aof-multi-replay-single-physical-silent-zero-replay.md:27 亦作单数，系册面转写误。
4 §14：锚 PROBE_PRESERVE_MAX_BYTES 零命中；现码 wnode/src/resp/resp_server_session/core.rs:80 为 PROBE_PRESERVE_WATERMARK（PubSubMailbox、preserve_probe 两锚完好，语义完好见 wnode/src/net/handler/drive.rs:772）。
5 §20：锚 parse_type_filter 零命中；现码 wnode/src/resp/array_commands.rs:108 为 parse_scan_filter。且来源路径 wedb/wnode/src/resp/basic_commands/array_commands.rs 不存在，实际路径 wnode/src/resp/array_commands.rs（§20 b 注释锚在该文件 :68/:87 完好）。
6 §27：锚 recover_committed 零命中；现码 whlog/src/hlog/mod.rs:539 为 recover（[flushed_until, tail) 清零语义注释完好，flushed_until 锚完好）。
7 §35：锚 StorageSession::check_expired 类型名错指；check_expired 实在 wkv/src/ttl.rs:505 且属 impl StoreSession（wkv 域类型），StorageSession 系 wnode/storage/session 域另一类型。§35 注释锚在 wkv/src/session/mod.rs:728 完好。
8 §53：锚 SortedSetObject::pop_ttl_members 零命中；成员级 TTL 剔除面现收敛于 wcol 类型域 types::member_ttl（format_member_ttl、decode_member、member_expired_at）与 sorted_set_expire（sorted_set_object_impl.rs:1147），tiered_collection_ops/hash.rs 消费 member_expired_at。
9 §58：锚 parse_auth_args 零命中；AUTH 选项解析已并入 parse_hello_args（wnode/src/resp/basic_commands/mod.rs:920，auth 臂 :950），cmd_strings.rs 与 parse_hello_args 两锚完好。
10 §104：文件锚 zset_commands/write.rs 路径不存在（objects/ 下无 zset_commands 目录）；实际 wnode/src/resp/objects/sorted_set_commands/write.rs（§104 a/b 注释锚在该文件 :843/:865 完好），zset_aggregate_nan0_disjoint_locks.rs 锚完好。
11 §109：锚 try_get_by_cs_name 零命中；现码 wresp/src/command.rs:754 为 RespCommand::from_cs_name（C# Enum.TryParse 对标注释完好），lookup_command 锚完好。

逻辑危害确证：
后续票按册规以符号名 grep 回收判据时，十宗全部零命中，回收通道断裂；§12 一宗更造成「登记名与来源票名不一致」的双名混乱，易被误判为符号灭失型缺陷另立新票（重复劳动）；§35 类型错指会引导审查在 wnode StorageSession 域空耗排查。

涉及代码：
rust 文件与函数：
doc/zh/deviations.md:§4/§12/§14/§20/§27/§35/§53/§58/§104/§109 各节符号锚行
现码位置逐一见问题分析 2-11（均为现名实际所在）

对应 c# 文件与函数：
N.A.（纯册面符号锚订正，无 Garnet 契约对位）

精炼执行方案：
1 按问题分析 2-11 将十节符号锚逐节替换为现码名/现路径（§4 改 convert.rs 饱和算式族现名；§12 改单数；§14 改 PROBE_PRESERVE_WATERMARK；§20 改 parse_scan_filter 与 resp/array_commands.rs 路径；§27 改 recover；§35 改 StoreSession::check_expired；§53 改 types::member_ttl 族加 sorted_set_expire；§58 删 parse_auth_args 并注明并入 parse_hello_args；§104 改 sorted_set_commands/write.rs；§109 改 RespCommand::from_cs_name）。
2 保持各节判据文字与来源票指针不动（十节判据语义经核验与现码一致，零行为改动）。
3 验证点：订正后十节每个符号锚可经全仓 grep 唯一命中；不新增行号锚（守册规 4）。
