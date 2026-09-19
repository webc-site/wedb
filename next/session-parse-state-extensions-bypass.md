优先级：中
分拣注记（qw.design 第 11 轮条 1 拆出；浅核 2026-09-19：符号在 wnode/src/session_parse_state_extensions.rs 在场，13 下标形态口经限定路径 grep 抽验确为零生产消费者——wmetric/tiered_collection_ops/hash_commands 命中均为 wresp::options 或 wmetric 私有同名活函数；台账无同题票）

SessionParseStateExtensions 的 C# 对位层整体旁路：13 个 (parse_state, buf, idx) 形态口
零生产消费者，其中 2 口是 wbitmap 既有单点的逐行复抄（同一 C# 函数双挂锚点、两文件互指唯一映射点）
问题：wnode/src/session_parse_state_extensions.rs 模块头自称对标 libs/server/SessionParseStateExtensions.cs，
但其 13 个「按参数下标取 token 再解析」的入口（try_get_info_metrics_type :85、try_get_latency_metrics_type :96、
try_get_client_name :108、try_get_bit_field_overflow :156、try_get_bitfield_encoding :168、
try_get_bitfield_offset :196、try_get_manager_type :212、try_get_operation_direction :246、
try_get_sorted_set_add_option :266、try_get_expire_option :276、try_get_sorted_set_aggregate_type :286、
try_get_expiration_option :296、try_get_timeout :308）全仓生产视图零引用，读者只有
wnode/tests/session_parse_state_extensions.rs；生产实际解析走同文件另一套字节核
（try_get_client_name_bytes :127、manager_type_from_token :221、operation_direction_from_token :255、
try_get_timeout_bytes :330）与 wresp::options 自由函数，即 C# 的「会话态下标解析」单一入口在 rust
被旁路为「调用方自行索引 + 字节核」，锚点声明与实现面脱节。
其中两口不是薄转发而是复抄实现：try_get_bitfield_encoding :168 与 wbitmap/src/bitfield/parse.rs:51
parse_bitfield_encoding 同体（len<=1 拒、i/u 前缀、strict_i64、有符号<=64/无符号<64，仅返回型
(u8,bool)/(i64,bool) 之差），try_get_bitfield_offset :196 与 wbitmap parse.rs:73 parse_bitfield_offset
同体；两处各自挂着同一 C# 锚点（wnode 挂 SessionParseStateExtensions.cs:TryGetBitfieldEncoding、
wbitmap 挂 BitmapCommands.cs:TryGetBitfieldEncoding），而 wbitmap/src/bitfield/parse.rs:34 注释还写着
「SessionParseState 版薄适配见 session_parse_state_extensions 模块（唯一 C# 映射点）」（:34-:35）——被指为唯一映射点的
两口本身零消费者，真身在产的另一套又各挂一锚。连带第三处同名：wmetric/src/info/info_command.rs:106
私有 fn try_get_info_metrics_type 再转发 InfoMetricsType::from_name，与 wnode 同名死口、wresp 内核三处同名。
修法：删 13 个下标形态口（含两口复抄）并把用例改调字节核，或将生产解析整体改走该层使锚点为真（择一）；
bitfield encoding/offset 只留 wbitmap 一份并删 wnode 复抄，wmetric :106 私有壳改直调 from_name，
js/check/ignore 按最终归属登记 SessionParseStateExtensions.cs 各方法。
c#：garnet/libs/server/SessionParseStateExtensions.cs（TryGet* 全族）；生产消费对位
garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:450、:541、garnet/libs/server/Storage/Functions/MainStore/PrivateMethods.cs:862、
garnet/libs/server/Resp/Objects/ListCommands.cs:214、garnet/libs/server/Resp/Objects/SortedSetCommands.cs:1577、
garnet/libs/server/Metrics/Info/InfoCommand.cs:37、garnet/libs/server/Metrics/Latency/RespLatencyCommands.cs:50、
garnet/libs/server/Resp/PurgeBPCommand.cs:49、garnet/libs/server/Resp/BasicCommands.cs:1492、
garnet/libs/server/Resp/ClientCommands.cs:525、garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:45
