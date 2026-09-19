版本无关基础帧型仍存第三套手写：13 处生产位点绕开 wresp 单点裸拼帧字节，hyperloglog 同文件四
连抄与 vector 自建编码器为最重样本

来源：next/glm.design.md 第 13 轮条 1（票面行号已按当下代码重取，并补两处票面未列的扫点）。取证基
线：主仓绝对根 /Users/z/git/db/wedb，分支 dev。

结论
C# 的 RESP 帧写出全仓单点 RespWriteUtils；rust 的对位单点已在场（wresp 两套入口），但 wedb /
wnode / wmetric / wcol 四个 crate 共 13 处生产位点仍手推帧字节，其中同一文件内「一半走单点、一半
手写」的逆例就有三处。判定成立且待做。

单点在场
- /Users/z/git/db/wedb/wedb/wresp/src/ext.rs:46-51 `RespVecExt::{write_resp_int,
  write_resp_bulk_string, write_resp_array_len, write_resp_error,
  write_resp_simple_string}`（Vec<u8> 面，四 crate 均已依赖 wresp：
  /Users/z/git/db/wedb/wedb/wedb/Cargo.toml:86、wnode/Cargo.toml:51、wmetric/Cargo.toml:21、
  wcol/Cargo.toml:25）。
- /Users/z/git/db/wedb/wedb/wresp/src/resp_memory_writer.rs:498 write_bulk_string、:539
  write_int64、:577 write_integer_from_bytes（RespWriter 直写面）。

手写位点（逐处实测）
1. /Users/z/git/db/wedb/wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:268、:294、
   :428、:459 —— 四处完整 `Buffer::new() + output.push(b':') + format + \r\n` 样板连抄（同步段与
   慢路径的 PFCOUNT 各双写）。C# 对位
   /Users/z/git/db/wedb/garnet/libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:88 一处
   `TryWriteInt64(cardinality, ...)`。
2. /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:1585-1590 —— CLIENT ID 手写
   `:` 帧，:1584 行内注释自认「C# TryWriteInt64(Id)」，而对位
   /Users/z/git/db/wedb/garnet/libs/server/Resp/RespServerSession.cs:1147 正是
   `TryWriteInt64(Id, ...)`：注释承认对标、代码另写一套。
3. /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/basic.rs:250-254、:276-282（:282 是
   `"$0\r\n\r\n"` 整帧字面量）、:323-327 —— 三条 cluster 命令的 bulk 帧手推；同文件 :216、:221、
   :547、:555 已在用 `output.write_resp_bulk_string(...)`，一文件两风格。C# 对位
   /Users/z/git/db/wedb/garnet/libs/cluster/Session/RespClusterBasicCommands.cs 的
   TryWriteAsciiBulkString / TryWriteBulkString 形态。
4. /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/migrate.rs:526-530 —— `:` 帧手推；
   C# 对位 /Users/z/git/db/wedb/garnet/libs/cluster/Session/
   RespClusterMigrateCommands.cs 的 TryWriteInt32(mtasks)。
5. /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/slot_mgmt.rs:673-681 —— `+` simple
   行手推（含空格分段）；C# 对位 RespClusterBasicCommands.cs 的 TryWriteAsciiDirect 系
   （/Users/z/git/db/wedb/garnet/libs/common/RespWriteUtils.cs:297）。
6. /Users/z/git/db/wedb/wedb/wmetric/src/latency/resp_latency_commands.rs:45、:52、:72 ——
   `-ERR Invalid event `、`*0\r\n`、`-ERR Invalid type ` 三处整帧字面量，而同文件 :25、:27、:82 走
   单点（write_resp_array_len / write_resp_simple_string / write_resp_int）；C# 对位
   /Users/z/git/db/wedb/garnet/libs/server/Metrics/Latency/RespLatencyCommands.cs 走
   TryWriteArrayLength / TryWriteError（RespWriteUtils.cs:70、:228）。
7. /Users/z/git/db/wedb/wedb/wnode/src/resp/vector/resp_server_session_vectors.rs:124-229 ——
   VectorReply 自建整套编码器 encode_resp2（:124-181）与 encode_resp3（:184-224），Simple / Error
   / Integer / Bulk / Map / Double / Boolean / Array 全帧型逐字节手推（帧头 push 见 :127、:132、
   :138、:145、:156、:168、:178、:194、:202、:212，整帧字面量见 :175 的 `$1\r\n1\r\n` 与 :199 的
   `#t\r\n`）。其中 null 两臂（:151 `$-1\r\n`、:153
   `*-1\r\n`）已由 task/ing/resp-null-protocol-single-source.md 认领，本单只收其余臂。C# 对位
   /Users/z/git/db/wedb/garnet/libs/server/Resp/Vector/RespServerSessionVectors.cs 全走
   TryWriteInt32 / TryWriteBulkString / TryWriteTrue / TryWriteFalse /
   TryWriteDoubleBulkString / TryWriteArrayLength。
8. /Users/z/git/db/wedb/wedb/wcol/src/hash/hash_object_impl.rs:553-558 私有
   `write_integer_from_bytes(output: &mut ObjectOutput, value: &[u8])` 逐字节复抄
   /Users/z/git/db/wedb/wedb/wresp/src/resp_memory_writer.rs:577 同名 pub 单点，消费点 :365、:378；
   同文件其余写出（:75、:91、:95、:116、:117、:367 等，RespWriter 共 29 处引用）走
   `RespWriter::new_ref(&mut output.payload)`，即同 crate 同文件的正例已在场。C# 对位
   RespWriteUtils.cs:526 TryWriteIntegerFromBytes 一处定义。

补扫两处（票面未列，落地时一并甄别）
- /Users/z/git/db/wedb/wedb/wnode/src/resp/key_admin_commands/types.rs:150 DUMP 的 `$` 长度头手
  推：该处的 crc/整帧口径差异有刻意声明注释（DUMP 面在 C# 走 WriteDirectLarge 整帧直写，形态本已
  分叉），若判定属刻意分叉则登记说明、不并入本单，否则改 write_resp_bulk_string 的头段。
- /Users/z/git/db/wedb/wedb/wext_json/src/json_object.rs:226 与
  /Users/z/git/db/wedb/wedb/wlua/src/functions/cjson.rs:304 的 `push(b':')` 属 JSON 语法字节，不是
  RESP 帧，明确排除。

修法
1. 帧型映射固定：`:` → RespVecExt::write_resp_int 或 RespWriter::write_int64（已有整数字节文本时
   用 write_integer_from_bytes）；`$len` → write_resp_bulk_string；`+` → write_resp_simple_string；
   `-` → write_resp_error（注意现单点会前置 `ERR `，与 C# 原样写出面的差异由
   /Users/z/git/db/wedb/wedb/wresp/src/cmd_strings.rs:1-6 模块头声明的 write_error_raw 承接，替换
   时按现文案是否已含前缀择一，勿改线协议字节）。
2. cluster_session 三条命令（basic.rs 三处、migrate.rs、slot_mgmt.rs）直接改单点调用，`$0\r\n\r\n`
   字面量随之消失。
3. vector 编码器保留 VectorReply 枚举与 RESP2/RESP3 分派骨架，只把每个臂的字节手推替换为单点转
   调（Boolean 的 `$1\r\n1\r\n`、Double 的 bulk 形态映射到 write_resp_bulk_string +
   format_double 现成工具），使其成为「枚举 → 单点」的薄壳而非第二套编码器；null 两臂按
   task/ing/resp-null-protocol-single-source.md 的收口结果接同一入口，两单以该文件为交汇点，先落
   该单者在本单里只余验证。
4. 删 wcol/src/hash/hash_object_impl.rs:553 私有副本，:365、:378 改走
   `RespWriter::new_ref(&mut output.payload).write_integer_from_bytes(...)`。
5. 落地后以帧字节不变为准绳跑 resp 族既有测试（不得为过测试改期望字节）。

优先级
重复/多套架构（同一线协议三种实现并存：wresp 两套入口 + 13 处手写，其中三处是同文件内自我分
叉；帧型面一旦有协议改动需逐点同步）。

边界
task/ing/resp-null-protocol-single-source.md 管 null 族收口（本单不含任何 null 臂）；
resp3-command-layer-frame-parity（现仍在 next/ 分拣中）管 set/zset 命令层的版本感知帧型（`~` 头
与 `,num` 分值），本单只管版本无关基础帧型；task/ing/cmd-strings-input-token-single-source.md 管输
出/输入常量表的承接完整性，与本单的写出原语是两个维度；acl-getuser-resp3-frame-parity 与
object-output-payload-direct-write（均在 next/ 分拣中）各管其写出版本分派与直写负载面。

盘点补记（qw13.invB resp-frame-literal-single-source）：dev e75716e 复核，缩窄：票面样本位已清零（hyper_log_log_commands.rs 手写整型帧、cluster_session/basic.rs 的 $0\r\n\r\n 均已改走单口，wmetric/wlua 生产面 b"-ERR 裸拼亦零命中）；残余裸拼：resp_server_session.rs:1613-1620 CLIENTID 臂仍手写 push(b\x27:\x27)+format+\r\n（可用 write_resp_int 单口），wlua/functions/cjson.rs:304 属 cjson 编码器内部不算 RESP 帧面。重派前先按「残余是否仍达一棒」复核，可能已缩成 CLIENTID 单点微修。
