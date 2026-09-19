RESP 命令层 nil 应答收口到版本感知单点（HELLO 3 会话下 GET 族、SET 条件臂、JSON 全域仍吐 RESP2 帧）

取证基线：主仓 /Users/z/git/db/wedb，分支 dev，HEAD 415e0d0e，全部按当下代码 grep 取证。
本档由 next/resp-null-protocol-single-source.md（基线 1fbe181）分拣后认领：已落地段与
不成立段均已从档中删除，不成立段连同拒绝理由见 task/reject/resp-null-protocol-single-
source.md。原档的 109 处 write_resp_null 消费点、14 处裸字面量、六个自造实现等计数在下
列 HEAD 实测面前已失效，一切以本档「二、未落地」的行号为准。

一、已收口部分（勿重做，只作接线参考）

- 版本感知单点已在 wresp 落成：wedb/wresp/src/ext.rs:53/:57-62 声明
  RespVecExt::write_resp_null_ver 与 write_resp_null_array_ver，:104-118 实现（>= 3 走
  Resp3::write_null / Resp3::write_null_array，否则 Resp2 同名），:185-192 有帧断言单测
  （`$-1\r\n_\r\n*-1\r\n_\r\n`）。文档注释已按 SKILL 格式指向 C#
  libs/server/Resp/RespServerSessionOutput.cs:WriteNull。原档提案名 write_resp_null_p2
  与此同义，视为已落地。
- 会话输出层已转调单点：wedb/wnode/src/resp/resp_server_session_output.rs:151-166
  write_null / write_null_array 均取 self.resp_protocol_version 后调单点，自造分派已消失。
- 阻塞与慢路径对象族臂已接线：wedb/wnode/src/resp/objects/list_commands/blocking.rs:91、
  :181、:370、:466、:468；list_commands/slow.rs:509、:511、:539、:575、:589、:610；
  objects/sorted_set_commands/blocking.rs:197、:274、:374；sorted_set_commands/slow.rs:660、
  :685。原档点名的 blocking.rs:454 超时臂与 garnet_api/raw.rs:76 已不再是裸字面量。
- 原档第 3 节第 1 步的前置件（wresp/src/output.rs 的 RespOutput 静态门面整文件删除）已并入
  dev：wedb/wresp/src/output.rs 在 HEAD 不存在，见 task/done/resp-output-facade-triple.md。
- 原档第 3 节第 4 步的划界对手方已消失：task/ing/tiered-resp3-frame-parity.md 与
  next/resp-frames-cmdstrings-homing.md 均不在树中；其运行时版本分派口已落成
  wedb/wresp/src/cmd_strings.rs:458-505（write_map_len_resp2 / write_map_len / write_set_len /
  write_null / write_double_numeric）。map / set / push 头不在本档射程。

二、未落地（本档射程，四条）

1. 命令层就地 RESP2 字面量 12 处（HELLO 3 下客户端可见帧与 C# 分叉）
- wedb/wnode/src/resp/basic_commands/get.rs:58（GET 缺键主臂）、:103（SG 批量臂）、
  :378（GETEX 臂）。C# 对位 garnet/libs/server/Resp/BasicCommands.cs:89 的
  `case GarnetStatus.NOTFOUND: WriteNull()`。
- wedb/wnode/src/resp/basic_commands/set.rs:495、:519、:597（SET 对象键 XX 覆写回 nil /
  条件不满足回 nil / SET GET 旧值缺失）。
- wedb/wnode/src/storage/session/storage_session.rs:320（批量 GET 逐键 nil 发出，即 MGET
  形态）。
- wedb/wnode/src/resp/vector/resp_server_session_vectors.rs:151（Bulk None 臂 `$-1\r\n`）与
  :155（NullArray 臂 `*-1\r\n`，同函数同时绕过 write_resp_null_array_ver）。C# 对位
  garnet/libs/server/Resp/Vector/RespServerSessionVectors.cs 的 4 处 WriteNull()。
- wedb/wmetric/src/info/info_command.rs:85 与 wedb/wnode/src/resp/garnet_api/slow.rs:357
  （INFO 空段应答）。
- wedb/wext_roaring/src/roaring_bitmap_commands.rs:319（RMW NotFound 兜底，其自身注释即写
  「C# 基类缺省 WriteNull」，C# 侧为版本感知）。
  判据（逐位点，不接受「看起来是 nil」的推断）：C# 走
  RespServerSessionOutput.cs:193/:208 的 WriteNull / WriteNullArray 位点，一律改调
  wedb/wresp/src/ext.rs 的两个单点；版本取会话现成口（RespServerSession::resp_protocol_version
  或 StorageSession::resp_protocol_version，后者见 storage_session.rs:116），零签名改动优先。
  协议恒定的 `$-1\r\n` 只剩集群配置线格式
  wedb/wedb/src/server/cluster_config/serializer.rs:632/:637（对位
  garnet/libs/cluster/Server/ClusterConfig.cs:855-861 与 libs/cluster/CmdStrings.cs:89
  GenericNullValue），不属 RESP 命令面，本档不动。

2. 会话层 write_null_array 仍在就地二选一
- wedb/wnode/src/resp/resp_server_session.rs:2827-2833 直接 extend_from_slice
  `b"_\r\n"` / `b"*-1\r\n"`，未走 ext.rs:112 的 write_resp_null_array_ver；同文件 :2956-2958
  的 write_null 是转发件（口径正确）。此臂与 resp_server_session_output.rs:162-166 是同一语义
  的两份实现，属重复。

3. 并立的运行时版本分派口 6 处未并入单点（同一 if/else 各写一份）
- wedb/wresp/src/cmd_strings.rs:487-493 `write_null(output, resp_protocol_version)`：与
  ext.rs:104 同 crate、同语义、同入参形态的真重复，消费面为
  wedb/wnode/src/resp/objects/tiered_collection_ops.rs:464、:486、:966、:973、:996、:1013、
  :1198、:1248、:1272、:1584、:1636、:1652 与 wedb/wcol 全域（zset/sorted_set_object_impl.rs:255、
  :335、:356、:923、:1413，zset/geo_impl.rs:131、:166，hash/hash_object_impl.rs:76、:96、:230，
  list/list_object_impl.rs:284、:443，set/set_object_impl.rs:156、:207、:229）。
- wedb/wcol/src/resp/output.rs:51-59 与 :61-69（crate 内 write_null / write_null_array，
  消费方经 `use crate::resp::output::write_null`，如 zset/sorted_set_object_impl.rs:36）。
  对位 C# libs/common/RespMemoryWriter.cs:317/:341 的 resp3 字段分派（该字段见 :26/:32），
  函数面可保留，分派体须转调 wresp 单点。
- wedb/wnode/src/resp/bitmap/bitmap_commands.rs:41-47 write_bitfield_nil（对位 C#
  libs/server/Storage/Functions/FunctionsState.cs:40 的 nilResp，本身即版本感知）。
- wedb/wnode/src/resp/basic_etag_commands.rs:51-61 write_etag_nil(output, resp3)。
- wedb/wpubsub/src/session_commands.rs:144-152 trait 方法内 if/else。
- wedb/wnode/src/resp/resp_server_session.rs:2827-2833（同第 2 条）。
  目标态：null 一族「版本二选一」的 if/else 只存在于 wedb/wresp（ext.rs:104、:112 两处
  入口，底层由 Resp2/Resp3 的 write_null / write_null_array 实现，见
  wedb/wresp/src/resp_memory_writer.rs:150-162（Resp2 三臂）、:229-241（Resp3 三臂）、
  :618-644（RespWriter 门面与 write_resp2_null / write_resp3_null）），其余 crate 一律转调。
  wcol / wpubsub / wmetric / wext_roaring / wlua 均已引 wresp
  （wcol/Cargo.toml:25、wpubsub/Cargo.toml:17、wmetric/Cargo.toml:21、
  wext_roaring/Cargo.toml:23、wlua/Cargo.toml:27），无新增依赖边。

4. RESP2 绑定的 .write_resp_null() 生产调用点 87 处（wext_json 47 + wnode 40）
- 单点存在但默认口仍是 RESP2：wedb/wresp/src/ext.rs:52 声明、:100-102 实现
  `RespWriter::new_ref(self).write_null()`，P 锁死 Resp2
  （wedb/wresp/src/resp_memory_writer.rs:150-152）。原档提案的「保留为显式 RESP2 语义 + 注释
  钉死」不成立（理由见 reject 档第 2 条），本档目标是把 87 处全数改调单点后删除
  write_resp_null 本体（fixloop 口径：不需向下兼容）。
- 最大缺口是 JSON 域：wedb/wext_json/src/json_commands.rs 43 处与
  wedb/wext_json/src/json_object.rs:95、:99、:113、:148 共 47 处，且 wext_json 全域 grep 不到
  protocol_version——命令层根本没有版本入参。C# 对位是
  garnet/modules/GarnetJSON/JsonCommands.cs:74、:192 与 GarnetJsonObject.cs:184、:230 的
  `writer.WriteNull()`（writer 以 respProtocolVersion 构造，版本感知）。故 JSON 域须先把会话
  版本穿到命令层（源头 storage_session.rs:116），不在 wext_json 另存第二份版本状态。
- wnode 40 处抽样：key_admin_commands/keys.rs:155（GETDEL 缺键，C# 对位
  garnet/libs/server/Resp/KeyAdminCommands.cs:311 WriteNull）、
  basic_commands/mod.rs:617、:702、:717（MEMORY USAGE 缺键与 TTL 族，C# 对位
  BasicCommands.cs:1635/:1673）、objects/hash_commands.rs、objects/set_commands.rs、
  objects/list_commands/read.rs 与 write.rs、resp_server_session.rs、
  rangeindex/resp_server_session_range_index.rs、acl_commands.rs、client_commands.rs、
  garnet_api/slow.rs:384。
- 分批：先 GET/TTL/键管理族，再 list/hash/zset/set 对象族，最后 JSON 族 47 处（含版本入参
  穿线），每批只跑该族既有 RESP 测试确认 RESP2 帧逐字节不变。

三、落地次序（死代码 > 重复/多套架构 > 污染扩散 > 功能缺口）

1. 并分派口：cmd_strings.rs:487-493 与 ext.rs:104-118 二选一（倾向保 ext.rs 的 trait 形态，
   cmd_strings 侧只留 RESP_ERRNOTFOUND / RESP3_NULL_REPLY 常量与 RESP2/RESP3 writer 实现），
   再把 bitmap_commands.rs:41-47、basic_etag_commands.rs:51-61、wcol/src/resp/output.rs:51-69、
   wpubsub/src/session_commands.rs:144-152、resp_server_session.rs:2827-2833 的 if/else 换成
   转调。零新常量、零新 writer 类型。
2. 替 12 处就地字面量与 2 处 vector `*-1` / session `_\r\n` 臂。
3. 分批替 87 处 write_resp_null，收尾删除 ext.rs:52/:100 本体。
4. 打磨：在 ext.rs 头注明「会话层 null 只有 write_resp_null_ver 与 write_resp_null_array_ver
   两个入口，crate 内不得再展开版本 if/else；协议恒定 `$-1\r\n` 仅存在于集群配置线格式」，
   并把 C# 的两层拓扑写进注释：会话层版本裁决
   （RespServerSessionOutput.cs:193/:208）+ writer 层版本裁决
   （RespMemoryWriter.cs:26/:32/:317/:341），rust 因 RespWriter 以类型参数静态分派，二者合并
   为 wresp 单点一处。

四、验收

1. wedb/*/src 内「null 一族的版本 if/else」命中仅剩 wedb/wresp/src/ext.rs（两处入口）；
   write_bitfield_nil / write_etag_nil / wcol::resp::output::write_null /
   wpubsub write_null / cmd_strings::write_null / resp_server_session.rs:2827 的分派体不再存在。
2. 命令层就地写出 b"$-1\r\n" / b"*-1\r\n" / b"_\r\n" 为 0。合法持有者：
   wedb/wresp/src/resp_memory_writer.rs 的 Resp2/Resp3 实现、
   wedb/wresp/src/cmd_strings.rs:308/:310 两个常量、解析侧 wedb/wconn/src/parser.rs 与
   wedb/wresp/src/read.rs 及其测试、测试基件 wedb/wnode_test/src/lib.rs:248 与
   wedb/wtxn_test/src/lib.rs:114、集群配置线格式
   wedb/wedb/src/server/cluster_config/serializer.rs:632/:637。
3. `.write_resp_null()` 生产命中为 0（trait 方法本体一并删除）。
4. 新增 wedb/wnode/tests/resp3_null_parity.rs（HEAD 无此档，全仓亦无等价断言族）：HELLO 3
   会话下 GET 缺键、GETDEL 缺键、MEMORY USAGE 缺键、SET NX / SET XX 条件失败、MGET 缺键、
   JSON.GET 缺路径、vector Bulk None 首字节为 `_`；同断言在 RESP2 会话下为 `$`（null 数组臂为
   `*`）；逐条与 BITFIELD OOB（已是 `_`）对照，证同一会话内形态一致。
5. RESP2 不回退：resp_hash / resp_list / resp_tests / hash_ttl / etag 族既有帧断言逐字节不变；
   next/txn-aof-marker-session-wiring.md:219 的 `*1\r\n$-1\r\n` 断言属 RESP2 会话，口径不变。
6. ./sh/clippy.sh 零警告无 allow；./test.sh 全绿；bun js/check.js 无新增缺失或重复定义
   （按 fixloop 口径由主代理在合并后执行，子代理只跑 cargo check）。

五、现状与 worktree 遗留风险

/tmp/fork 下有两处与本主题同源的死亡 worktree：分支 resp-null-protocol-single-source（工作区
脏 42 个文件）与 wave1-a-resp-null（脏 10 个文件），二者 ahead=0，即没有任何提交进过 dev，
且长时间无活动。它们的未提交改动只存在于各自 worktree 目录中，主仓 `git diff --stat <branch>`
看不到，因而既不能当作已落地铁据，也不应 cherry-pick 或照抄其 diff。本档全部判据以主仓
HEAD 415e0d0e 的代码事实为准，开工前须重新按上文行号逐条 grep 校验，行号漂移以符号名为准；
若发现同主题半成品与主仓现状冲突，一律以主仓现状重写，不做合并式缝合。
（另有 wave4-b-resync-strategy 脏 7 文件，主题不同，与本档无关。）

并发已认领：task/ing 下 dbmeta-atomic-batch、migration-frame-import-core、
tiered-background-demote、vector-key-ttl-fourth-domain、info-store-snapshot-channel、
resync-strategy-store-version、aof-tail-witness-freq-config-wiring、
txn-aof-marker-session-wiring 均不含 RESP null 写出主题，与本档无交叠；
txn-aof-marker-session-wiring.md 仅在验收第 5 条以既有 RESP2 断言形式与本档相接。

六、落地结果（二棒 fix-respnull-b，基于合并前 dev 重做）

一棒死树 27 文件 +131/-145 的逐 hunk 验尸结论与三处本档观点勘误见
task/reject/resp-null-protocol-single-source.md（INFO 空段非 WriteNull 位点、
RESP_ERRNOTFOUND/RESP3_NULL_REPLY 收口后零消费者故删、vector 编码器臂转调
Resp2/Resp3 而非 _ver 入口）。四条射程落地实况（合并后 dev 上 grep 复测）：

1. null 一族的版本 if/else 只剩 wedb/wresp/src/ext.rs:115 / :123 两处入口
   （声明 :72 / :77）；cmd_strings::write_null、wcol::resp::output 两函数分派体、
   bitmap::write_bitfield_nil 分派体（现为一行转调，bitmap_commands.rs:43-45）、
   basic_etag_commands::write_etag_nil（本体已删）、wpubsub write_null
   （session_commands.rs:158）、resp_server_session.rs:2840 TxnSession 臂（现为
   self.write_null_array() 转发，与该文件 write_null/write_push_length 同口径）
   的分派体全部不再存在。
2. 命令层就地 b"$-1\r\n" / b"*-1\r\n" / b"_\r\n" 为 0（唯一余命中是
   custom_object_commands.rs:381 的测试断言字面量）；RESP_ERRNOTFOUND /
   RESP3_NULL_REPLY 两常量删除，INFO 空段改回新增的 RESP_EMPTY（cmd_strings.rs:19）。
3. .write_resp_null() 全仓命中 0，trait 方法本体（ext.rs 声明与实现）一并删除，
   消费点改调版本入口（含 wresp/tests/writer.rs:189 与 ext.rs::vec_ext_formatting）。
4. wnode 40 处与 wext_json 47 处全数转调：JSON/Roaring 执行体的会话版本经
   wcustom/src/custom_object_fns.rs:33-42 的 RespVersion 入参穿线（四执行体签名同改，
   对位 C# ref RespMemoryWriter 与 FunctionsState.cs:nilResp），wnode 分派链
   custom_object_commands.rs:68 / :90 起承接，异步臂与 network_riget 取
   StorageSession::resp_protocol_version 现成口，扩展 crate 内不自存版本状态。
   验收 4 新档 wedb/wnode/tests/resp3_null_parity.rs 覆盖
   GET/GETDEL/MEMORY USAGE/SET NX/SET XX/MGET/HMGET/BITFIELD OOB/
   JSON.GET 缺键/R.SETBIT 缺键兜底/vector Bulk None 与 NullArray，
   RESP2 与 RESP3 各一轮；SG 批量 GET 臂的 e2e 断言缺口见 reject 档第四节。

门禁：cargo check -p wnode -p wresp -p wcol -p wpubsub -p wmetric --all-targets
零错误零警告（wnode 默认 feature 含 json/roaring，故 wext_* 与 wcustom 一并过编）；
clippy.sh / test.sh / bun js/check.js 按 fixloop 口径由主代理合并后执行。
