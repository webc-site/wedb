# cs-corpus 遗留票源盘点（inv1）

基线：分支 dev，盘点时 HEAD = fbd285951530bdbf301e7454e9afbcddf3e8d981（盘点为只读，未改任何代码；所有
grep/判定均按该现刻 HEAD 的树内容）。产出本文件外无新增/修改文件。

三批对象与移交来源：

1. 10 份新 miss（31 名、byte* 形参族）——来源 `task/done/garnet-scan-cs-corpus-parse-gate.md:123-126`。
2. 109 条 ignore 暗条目逐条判（改注释/删/留）——来源同档「遗留复核」段 :112-121 与 `js/check/README.md`
   第 1 节末段（实测 109 条）。
3. 225 处裸文件名锚点按 crate 分批——来源 check.js B 层锚点口径（`js/check/symbolCheck.js`），
   前序判例 muse.design：CS_REF_REGEX 不认「路径.cs:行号」形态裸文件名锚点。

状态：盘点进行中（分节追加）。

## 第一节 10 份新 miss（31 名，byte* 形参族）

### 1.1 该 miss 清单的在册位置（结论：易失产物，未入库，快照已不可复得）

- 产出与登记位：check.js 的「# 实现缺失」段按 C# 文件逐个落 `js/check/miss/<cs 相对路径>.yml`
  与 `check/miss/` 两份镜像（`js/check.js:14-17` MISS_DIR_LI、`:52-78` missSync 先删后写），
  被 `js/check/.gitignore:1` 的 `/miss/` 整族排除，从不进版本库。
- 现刻复跑 `bun js/check.js`：exit 0、stdout 无「# 实现缺失」段、两镜像目录皆空 →
  「10 份 / 31 名」的在册态已被后续波次消化，名单本身不可从库里再取出。
- 可复得的历史登记两处（本单据以核销）：
  来源棒 `task/done/garnet-scan-cs-corpus-parse-gate.md:123-126`（点名 5 名）+
  `task/reject/inv-gate-anchor-primarysync.md:79-89`（该棒现刻重跑所得「7 族 17 名」）。
  二者并集 19 名，逐名核销见 1.2。
- 结构性替代口径（本节主口径，可复算）：以工具自身口径重取「仅词法兜底才可见」的名集 =
  194 个 hasError 文件中 `AST+兜底名集 − 仅 AST 名集` = 388 名 / 43 个文件有增量
  （本单实测，与 `js/check/README.md` 第 1 节所记 194/388 逐字一致）。按现刻 HEAD 三分：
  同路径全路径锚在位 167、ignore 覆盖 161（其中函数级条目 130，即第二节对象）、
  既无锚又无 ignore 的「门禁盲名」31（首算得 32，复核时 `SessionParseState.cs:SetArgument`
  实已登记在 `server.yml:230`，属本单脚本取数误差，已改正）。31 名即「10 份 31 名」的现刻等价像。
- 门禁为何不报这 31 名（新增发现，值得单列后续工具票）：`js/check.js:423-427` 的
  `isDocumented` 除 `doc_file_fn_map`（按归一路径为 key）外，还兜一层 `doc_set`
  ——`js/check/rustScan.js:47-50` 把每条注释的全部词元灌进 `doc_set`，于是任一 .rs 注释里
  出现过同名裸词（哪怕只是「C# TryGetDouble 默认 canBeInfinite: true」这类叙述）就算已文档化，
  永久不进 miss。这条是 miss 判定的结构性死角，与本单第二节「ignore 假路径零反应」同族。

### 1.2 历史 19 名逐名核销（实已接线 3 · 转入 ignore 登记 16）

核销（rust 侧已有规范全路径锚，映射在登记）：

- SplitIndex.cs:TraceBackForOtherChainStart → `wedb/wkv/src/store/resize.rs:389`
- RespAdminCommandsTests.cs:ConfigWrongNumberOfArguments → `wedb/wnode/tests/resp_admin.rs:165`
- RespAdminCommandsTests.cs:ConfigGetWrongNumberOfArguments → `wedb/wnode/tests/resp_admin.rs:177`

转入 ignore 登记（不再是 miss，但按第二节的判读口径属「撤换前须逐条复核」族）：

- AofProcessor.cs:BeginReplayOp → `js/check/ignore/server.yml:1006`
- RespServerSession.cs:DebugSend → `js/check/ignore/server.yml:240`
- HyperLogLog.cs:DenseCountNonZero → `js/check/ignore/server.yml:1022`
- VectorManager.Callbacks.cs:AdvanceTo / GetInput / GetKey / GetOutput / MakeVectorElementKey /
  SetOutput / SlowPath（7 名）→ `js/check/ignore/server.yml:1011-1017`
- core/Epochs/LightEpoch.cs:AllocateUserWord / GetMinUserWord / ReleaseUserWord /
  ThisThreadUserWord（4 名）→ `js/check/ignore/storage.yml:3714-3717`
- VectorSetRecallSmokeTests.cs:Jitter → `js/check/ignore/test.yml:2252`
- RespReadUtils.cs:GetSerializedRecordSpan → `js/check/ignore/common.yml:3`

### 1.3 现刻 31 名逐条判（实现在位缺锚 15 · 无具名对位口 16 · 完全无实现 0）

关键结论：本批 31 名按现刻 HEAD 逐条核到 C# 签名与 rust 消费点，无一属「rust 侧完全无实现」，
全部落在两族——实现在位但注释锚点形态不登记（该改注释）与「无独立具名对位口、语义在消费点就地
承接」（该补 ignore 登记，不该派实现票）。原票「下一步该按 miss 派实现票」的预设不成立，
按现刻读数应改为「派注释整改票 + 登记补全票」；本批零实现票（唯一可疑的两名见丙族说明，属第二节范围）。

甲族（实现在位、缺规范锚 → 改注释；格式须 `全路径.cs:符号`，一处一符号）：

1. VectorManager.Callbacks.cs:ReadCallbackUnmanaged — 落点 `wedb/wnode/src/resp/vector/vector_store_callbacks.rs:80`
   （read_multi；现 :21/:65 为「批量下发与完成收割 ReadCallbackUnmanaged（同文件 :356-362…）」叙述形态）
2. 同文件:WriteCallbackUnmanaged — 落点 `vector_store_callbacks.rs:165`（write，:164 doc 已点名 C# 与行区间）
3. 同文件:DeleteCallbackUnmanaged — 落点 `vector_store_callbacks.rs:175`（delete）
4. 同文件:ReadModifyWriteCallbackUnmanaged — 落点 `vector_store_callbacks.rs:181`（rmw）
5. 同文件:FilterCallbackUnmanaged — 落点 `vector_store_callbacks.rs:202`（filter；`wedb/wvector/src/store.rs:167`
   亦点名同名，改锚时只在一处挂全路径锚，另一处改散文，否则触 dupDefFind「重复定义」）
6. 同文件:ReadSizeUnknown — 落点 `vector_store_callbacks.rs:145`（read；`wvector/src/store.rs:140` 同族重复风险）
7. SessionParseState.cs:GetArgSliceByRef — 落点 `wedb/wresp/src/session_parse_state.rs:71`（get_arg_slice_by_ref
   已是实名实口，:76 doc 为反引号形态不登记）
8. RespReadResponseUtils.cs:TryReadIntWithLengthHeader — 落点 `wedb/wconn/src/parser.rs:74`（:67 doc 已点名该 C# 口）
9. TsavoriteBase.cs:FindTagOrFreeInternal — 落点 `wedb/windex/src/table.rs:313`（classify_slot；:300/:321 叙述在位）
10. TsavoriteBase.cs:FindOtherSlotForThisTagMaybeTentativeInternal — 落点 `wedb/windex/src/table.rs:264`
    段（find_or_create_tag_by_hash_with_min_addr 一系）
11. LogRecord.cs:TrySetPinnedValueSpan — 落点 `wedb/wrecord/src/record_mut.rs:193`（write_val_with_slack；
    :183 doc 与 `header.rs:347` 容量口径皆叙述形态）
12. RangeIndexOps.cs:RangeIndexScan — 落点 `wedb/wnode/src/resp/rangeindex/resp_server_session_range_index.rs:507`
13. RangeIndexOps.cs:RangeIndexRange — 落点 同文件 `:555`
14. RespServerSession.cs:NetworkCustomRawStringCmd — 落点 `wedb/wnode/src/resp/resp_server_session.rs:2012`
    （run_custom_command 为 C# 三 local function 共骨架，只挂一锚）
15. RespReadUtils.cs:TryReadInfinity — 落点 `wedb/wbase/src/num.rs:210`（infinity_sign；:206 doc 已点名白名单口径）

乙族（无独立具名对位口，语义在消费点就地承接 → 该补 ignore 条目，不派实现票；括号为可并入的既有块）：

- SessionParseState.cs:DeserializeFrom / TryGetLong / TryGetDouble / TryGetFloat /
  GetString / TryGetBool（6）— rust 侧无 parseState 级读口，整数/浮点走 `wbase/src/num.rs` strict 通道、
  布尔走 `wedb/src/server/cluster_session/replication.rs:751` 就地 T/F 判形（可并入 `js/check/ignore/server.yml:225`
  或 `:993` 既有 SessionParseState.cs 块；该块 :230 已登 SetArgument/SetArguments、
  :227 EnsureCapacity、:228-229 GetDouble/GetFloat、:231 GetSerializedLength、
  :232 Slice —— 其中 Slice 一名经核 rust 已有 1:1 实口 `wedb/wresp/src/session_parse_state.rs:55`，
  属「已实现却挂忽略」，判改不判留，详见第二节 2.4）
- PrivateMethods.cs:IsValidNumber — 就地闭包承接，证据 `wedb/wnode/src/resp/basic_commands/incr.rs:132`、
  `slow.rs:403`（并入 `server.yml:273` 既有 PrivateMethods.cs 块）
- NumUtils.cs:TryReadInt64 — 与已登记的 strict_i64 同源，证据 `wedb/wnode/src/resp/basic_commands/incr.rs:133`
  （并入 `common.yml:322` NumUtils.cs 块）
- InputHeader.cs:DeserializeFrom — 组合形态承接，证据 `wedb/wnode/src/aof/replay_input.rs:235`
  （并入 `server.yml:34` InputHeader.cs 块）
- RespCommand.cs:SimdFastParse — rust 无同名解析口，MRU 双槽晋升语义在
  `wedb/wnode/src/resp/parser/resp_command.rs:147`（该文件当前无 ignore 块，需新立）
- core/Utilities/Utility.cs:IsPowerOfTwo / GetLogBase2（2）— rust 走 std `is_power_of_two` / `ilog2`，
  证据 `wedb/wbase/src/align.rs:6`、`wedb/wdev/src/chunk.rs:62`（并入 `storage.yml:2114` Utility.cs 块）
- core/ClientSession/TransactionalConsistentReadContext.cs:RMW / Refresh（2）— 只读会话无写接口，
  与同文件已登记的 Upsert/Delete 同口径（并入 `storage.yml:861` 既有块，Refresh 另可锚 `wepoch`）
- VectorManager.Callbacks.cs:SetActiveReadGeometry — 冷读尺寸预算改由 wkv 冷读侧承接，
  证据 `wedb/wnode/src/resp/vector/vector_store_callbacks.rs:9`（并入 `server.yml:1022` 既有块）
- playground/Bitmap/BitCount.cs:__simd_popcX128 — ISA 降级臂不转写，证据 `wedb/wbitmap/src/bit_count.rs:8-11`
  已自述「js/check/ignore 登记 __simd_popcX128」，但该条目只记在
  `js/check/ignore/libs/server/Resp/Bitmap/BitmapManagerBitCount.yml`，playground 侧未记 → 补 `playground.yml:2`
  既有 BitCount.cs 块（此为「注释声称已登记、实际未登记」的失真例，须收）

丙族（真缺，须派实现票）：本批 31 名内 0 名。全盘点唯一两处「留着登记可能是藏真缺」的不在 31 名内，
而在第二节 `storage.yml:1926 / :1961`（TsavoriteLog.cs:GetChecksum / VerifyChecksum，
rust 侧 waof 无任何校验和函数口），故 1.4 票 6 指向该处而非本批。

### 1.4 第一节立即派单建议 Top 5（另附 2 条条件票，不先行派发）

票 1 slug: cs-anchor-vector-callbacks-unmanaged
射程文件: wedb/wnode/src/resp/vector/vector_store_callbacks.rs、wedb/wvector/src/store.rs
判据: 甲族 1-6 六名同 C# 文件同承接面，一次改注释即可把 6 枚映射登记入门禁，且须顺手消掉
wvector 侧同名重复挂载，避免触「重复定义」。工作量: 纯注释 6 处，无函数体改动。

票 2 slug: cs-anchor-parse-state-arg-readers
射程文件: wedb/wresp/src/session_parse_state.rs、js/check/ignore/server.yml（:225 与 :993 块、:232 条）
判据: SessionParseState.cs 8 名一棒判尽——GetArgSliceByRef 补规范锚（甲族 7）、
DeserializeFrom/TryGetLong/TryGetDouble/TryGetFloat/GetString/TryGetBool 6 名补 ignore 条目并写理由（乙族）、
Slice 一条反向处理（server.yml:232 已登记但 rust 有 1:1 实口 `session_parse_state.rs:55`，
撤登记 + 补锚，见 2.5 票 1，两票同棒做以免自相矛盾）。工作量: 2 处注释 + 6 条 yml 条目。

票 3 slug: cs-anchor-tsavorite-tag-slot-and-pinned-span
射程文件: wedb/windex/src/table.rs、wedb/windex/src/chain.rs、wedb/wrecord/src/record_mut.rs、
wedb/wrecord/src/header.rs、js/check/ignore/storage.yml（:2114 / :861 块）
判据: 甲族 9-11 + 乙族 Utility/TransactionalConsistentReadContext 共 7 名全在 tsavorite 系 crate，
一次跑完可让 windex/wrecord/wbase 的映射与登记同时收口。工作量: 3 处注释 + 4 条 yml。

票 4 slug: cs-ignore-backfill-gate-blind-spots
射程文件: js/check/ignore/server.yml、common.yml、storage.yml、playground.yml
判据: 乙族除票 2/票 3 已覆盖外的余 6 名补登记（IsValidNumber、TryReadInt64、InputHeader.DeserializeFrom、
SimdFastParse、SetActiveReadGeometry，以及 playground/Bitmap/BitCount.cs:__simd_popcX128 这条
「注释自称已登记、实际未登记」的假账），把 31 名清零成「要么有锚要么有登记」。
其中 RespCommand.cs 当前无 ignore 块需新立。工作量: 6 条条目 + 理由，无代码。

票 5 slug: checkjs-miss-token-coverage-hole
射程文件: js/check.js（isDocumented）、js/check/rustScan.js（doc_set）、js/check_selftest.js
判据: 本节 1.1 末段——doc_set 词元全集使这 31 名结构性不进 miss，属门禁漏报，须先立判据再改口径
（建议 miss 判定只认 path 精确映射，token 面降为提示）；工具票，不与实现票混派。
工作量: 判定口径一处改 + 断言。

（另：原票预设「31 名皆实现缺口」经逐名取证后不成立——本批零实现票，全盘点唯一
「留着登记可能藏真缺」的一族不在 31 名内，见票 6。）

票 6 slug: my-tsavorite-log-checksum-finalize
射程文件: js/check/ignore/storage.yml（:1926 / :1961，同属 :1907 `…/core/TsavoriteLog/TsavoriteLog.cs` 块）、wedb/waof/src/*
判据: C# `TsavoriteLog.cs:GetChecksum / VerifyChecksum`（garnet/libs/storage/Tsavorite/cs/src/core/
TsavoriteLog/TsavoriteLog.cs）现随块挂忽略，且 rust 侧 waof 无任何校验和函数口
（`grep 'fn .*checksum' wedb/waof/` 零命中），本单判「留（待复核定性）」；先定性「AOF/复制帧是否需要
校验和」——不需要则在块理由里补一句校验和专项依据，需要则另开实现票补 waof 侧校验口。
与 2.5 票 3 同域，两票合一派，不双立。
工作量: 一次定性 + 1 条理由（或 1 个函数 + 测试）。

票 7（可选备注，不单开实现票）: C# `SessionParseState.DeserializeFrom(byte*)` 有 serialize_to 对位
（`wresp/src/session_parse_state.rs:159`）而 deserialize 侧仅由 `wedb/wnode/src/aof/replay_input.rs:235`
组合形态承接；本单判「乙族·补登记」（票 2 射程）。仅当将来回放需原地重建 parseState 时，在票 2
落地同一棒内补对称实现，不另立票。

## 第二节 ignore 暗条目逐条判

### 2.1 现刻基线与口径

- `bun js/check.js`（HEAD fbd2859）：exit 0；stdout 仅「# 重复定义」段 15 组，无「# 实现缺失」段；
  stderr 语料降级汇报 194/1425、兜底补回 388 名、B 层锚点提示 129 处、零名录 1 例（SpanByteKey.cs）。
  跑前后对 `js/check/ignore` 全 80 份 yml 逐文件 shasum 比对：零改动零回写（pre/post 校验集在
  /tmp/inv1_ignore_{pre,post}.sha），本单未留任何语料脏。
- 暗条目口径按来源棒给的复算配方执行（`task/done/garnet-scan-cs-corpus-parse-gate.md:119-121`）：
  hasError 文件的「AST+兜底名集 − 仅 AST 名集」（388 名）与 `js/check/ignore` 函数级条目名求交，
  现刻得 130 条（分块：storage 74、server 32、common 14、playground 7、client 2、
  libs/server/Resp/Bitmap/BitmapManagerBitCount 1；130 条 = 130 个互异 path::name，无重复行）。
- 与档案读数 109 的差 21 条未能归一到单因，本单已排除一条常见猜测：
  `git diff 4c8184d..HEAD -- js/check/ignore` 新增的 46 个函数名与暗集交集为 0，
  即差值不是「后续波次新登记的条目」，只能是语料侧兜底面/AST 断裂位置随转写波次漂移所致
  （README 的 109 是该棒当时一次性的读数，无常驻产物可回溯）。逐条判读以现刻 130 为准。
- 判据三条（与 check.js 的自动淘汰语义严格对齐）：
  删 = rust 侧已对该 `路径.cs:符号` 挂规范全路径锚（此时 ignoreLoadAndPrune 本就会自动删除该条目，
  故「已实挂却仍留登记」的条目现刻实测为 0 条）；
  改 = rust 承接口在位且注释已点名该 C# 符号，但形态不登记（裸文件名 / `路径.cs:行号` / 反引号 /
  纯叙述），补规范锚后条目自动淘汰；
  留 = 确属死码、平台绑定、或「无独立具名对位口」的形态不承接。
- 本组取数结论：130 条中，有同路径锚 0、有裸文件名锚 0、他路径同名锚 5（经核 5 条均为
  同名不同 C# 文件的巧合，不构成改锚依据）、含 `.cs` 的叙述行 10（逐条读原文后仅 2 条构成改锚依据，
  其余 8 条为「同名他文件」或「他文件语境顺带提及」）、零痕迹 112。
  即按现刻证据，本节改判为「改」的仅 2 条，其余为「留」，无一条可「删」。
  这与来源棒抽查所得「TryInPlaceUpdateNumber 属已实现却挂忽略」的单条直觉不同：该名
  （`server.yml:284`）在 rust 侧的两处提及（`wnode/src/resp/basic_commands/incr.rs:182`、
  `wnode/tests/resp_tests.rs:428`）经核均非 C# 该口的对位实现，而是「非有限旗标」语义叙述，
  仍判留；真正「已实现却挂忽略」的是下表的 SessionParseState.cs:Slice 与
  GarnetClientProcessReplies.cs:ProcessReplyAsNumber 两条。

### 2.2 已完成块一：common.yml（14 条，全留）

js/check/ignore/common.yml:3 | libs/common/RespReadUtils.cs:GetSerializedRecordSpan | 留 | 理由逐条给出消费点对位（wbase/src/num.rs:94/:124 strict 通道），C# 口为 ref ptr/out bytesRead 形态无承接位
js/check/ignore/common.yml:5 | libs/common/RespReadUtils.cs:TryReadDoubleWithLengthHeader | 留 | 理由记「全仓零生产调用」，rust 侧 MurmurHash/strict 通道零痕迹，与 wbase/src/num.rs 现刻形态一致
js/check/ignore/common.yml:7 | libs/common/RespReadUtils.cs:TryReadPtrWithLengthHeader | 留 | 理由点名唯一 C# 消费点 ObjectStore/Common.cs:223,242 属副本回读文本帧重解析，rust 走结构化通道
js/check/ignore/common.yml:8 | libs/common/RespReadUtils.cs:TrySkipByteArrayWithLengthHeader | 留 | 理由记 C# 仅 RespReadUtilsTests.cs:419,425,431 测试引用，生产零调用
js/check/ignore/common.yml:13 | libs/common/RespReadUtils.cs:TryReadStringResponseWithLengthHeader | 留 | 理由点名唯一生产引用 LuaRunner.cs:1302，rust 由 wlua resp_convert 承接
（同块提醒：同文件 `RespReadUtils.cs:TryReadInfinity` 未在此块内，rust 承接口
`wedb/wbase/src/num.rs:210 infinity_sign` 在位却无锚 → 第一节乙/甲族已列，须与本块同棒复核以免两块口径分叉）

js/check/ignore/common.yml:323 | libs/common/NumUtils.cs:WriteInt32 | 留 | rust 现刻唯一 WriteInt32 锚是 `RespServerSessionOutput.cs:WriteInt32`（wnode/src/resp/resp_server_session_output.rs:93），非 NumUtils 同名口，理由「itoa/to_le_bytes 原生」成立
js/check/ignore/common.yml:328 | libs/common/NumUtils.cs:WriteDouble | 留 | rust 侧仅测试与 incr.rs 的裸词叙述（wnode/tests/resp_tests.rs:354），无独立写浮点口，理由「ryu/dtoa 原生」成立
js/check/ignore/common.yml:332 | libs/common/NumUtils.cs:ReadInt64 | 留 | 零痕迹；rust 走 i64::from_le_bytes / strict_i64 单点（wbase/src/num.rs:94）
js/check/ignore/common.yml:336 | libs/common/NumUtils.cs:TryReadDouble | 留 | 他路径锚为 `ParseUtils.cs:TryReadDouble`（wbase/src/num.rs:78），与本条同词不同 C# 口，不构成改判
js/check/ignore/common.yml:340 | libs/common/NumUtils.cs:CountCharsInDouble | 留 | 零痕迹，C# 侧零调用（同块理由）
js/check/ignore/common.yml:341 | libs/common/NumUtils.cs:CountDigits | 留 | 零痕迹；rust 由 u64::trailing_zeros/ilog2 就地算
js/check/ignore/common.yml:342 | libs/common/NumUtils.cs:GetNextOffset | 留 | 零痕迹，C# 序列化偏移辅助无 rust 对位
js/check/ignore/common.yml:363 | libs/common/HashUtils.cs:MurmurHash3x64 | 留 | `wedb/wbase/src/hash.rs` 全文件仅 murmur_hash2_x64_a 一口，理由「C# 生产仅用 MurmurHash2x64A」经核成立
js/check/ignore/common.yml:364 | libs/common/HashUtils.cs:MurmurHash3x64A | 留 | 同上，无第二哈希口

小计：14 条 → 留 14、改 0、删 0。

### 2.3 已完成块二：storage.yml（74 条，留 72 / 改 2 / 删 0）

LogRecord.cs 块（41 条，storage.yml:220-269）——判定全留；块级提醒见末行：

js/check/ignore/storage.yml:220 | …/Allocator/LogRecord.cs:AsReadOnlySpan | 留 | wrecord 全 crate 零 LogRecord.cs 锚，且无 as_read_only_span 口
js/check/ignore/storage.yml:221 | LogRecord.cs:CalculateHeapMemorySize | 留 | rust 无对象堆尺寸核算面（对象走 wcol 信封）
js/check/ignore/storage.yml:222 | LogRecord.cs:CanGrowPinnedValue | 留 | 零痕迹；原地增长语义在 wrecord record_mut.rs:193 write_val_with_slack 内联，无判定型副口
js/check/ignore/storage.yml:223 | LogRecord.cs:ClearHeapFields | 留 | 零痕迹
js/check/ignore/storage.yml:224 | LogRecord.cs:ClearOptionals | 留 | 零痕迹
js/check/ignore/storage.yml:225 | LogRecord.cs:ClearValueIfHeap | 留 | 零痕迹
js/check/ignore/storage.yml:226 | LogRecord.cs:CreateRemappedOverPinnedTransientMemory | 留 | 瞬态内存重映射属 .NET MemoryAllocator 面，rust 无对应机制
js/check/ignore/storage.yml:227 | LogRecord.cs:GetAllocatedSize | 留 | 零痕迹
js/check/ignore/storage.yml:231 | LogRecord.cs:GetInfo | 留 | 他路径锚属 `ClusterManager.cs:GetInfo`（wedb/src/server/cluster_manager.rs:464），同名不同口
js/check/ignore/storage.yml:232 | LogRecord.cs:GetInfoRef | 留 | 零痕迹
js/check/ignore/storage.yml:233 | LogRecord.cs:GetInlineKey | 留 | 零痕迹（rust 键内联读在 wrecord record_mut.rs:153 key()，理由未点名，留但不宜据此改锚）
js/check/ignore/storage.yml:235 | LogRecord.cs:GetObjectLogRecordStartPositionAndLengths | 留 | 对象日志物理层在 rust 不存在（同块 ObjectAllocatorImpl 理由）
js/check/ignore/storage.yml:236 | LogRecord.cs:GetOptionalFieldsSpan | 留 | 零痕迹
js/check/ignore/storage.yml:237 | LogRecord.cs:GetOptionalStartAddress | 留 | 零痕迹
js/check/ignore/storage.yml:239 | LogRecord.cs:GetSerializedSize | 留 | 零痕迹；序列化长度由 wrecord header.rs 位域就地算
js/check/ignore/storage.yml:240 | LogRecord.cs:GetValueHeapMemorySize | 留 | 同 221
js/check/ignore/storage.yml:242 | LogRecord.cs:InitializeHeadersForNewRecord | 留 | 零痕迹（rust 构头在 wrecord/src/header.rs，无同名具口）
js/check/ignore/storage.yml:244 | LogRecord.cs:OnDeserializationError | 留 | 零痕迹，.NET 反序列化回调面
js/check/ignore/storage.yml:245 | LogRecord.cs:OnObjectReadComplete | 留 | 零痕迹
js/check/ignore/storage.yml:246 | LogRecord.cs:PopulateRecordSizeInfoForIPU | 留 | 零痕迹；IPU 在 rust 为 update_value_with_slack（record_mut.rs:261），非同名口
js/check/ignore/storage.yml:247 | LogRecord.cs:PrepareForRevivification | 留 | rust 复活面为 record_mut.rs:267 revivify_with_slack，语义并入，无独立准备口
js/check/ignore/storage.yml:248 | LogRecord.cs:RemapOverPinnedTransientMemory | 留 | 同 226
js/check/ignore/storage.yml:249 | LogRecord.cs:RemoveETag | 留 | wrecord 无 etag 函数面（ETag 位在 header.rs 位域），理由与现刻一致
js/check/ignore/storage.yml:250 | LogRecord.cs:RemoveExpiration | 留 | 叙述行 ttl.rs:27 指的是 `RMWMethods.cs` 的 GETEX 分支，非本 C# 口
js/check/ignore/storage.yml:251 | LogRecord.cs:RepointObjectLogPosition | 留 | 零痕迹
js/check/ignore/storage.yml:252 | LogRecord.cs:SetDataHeader | 留 | 零痕迹
js/check/ignore/storage.yml:253 | LogRecord.cs:SetDeserializedValueObject | 留 | 零痕迹
js/check/ignore/storage.yml:255 | LogRecord.cs:SetObjectLogRecordStartPositionAndLength | 留 | 零痕迹
js/check/ignore/storage.yml:256 | LogRecord.cs:SetRecoveredObjectLogRecordStartPosition | 留 | 零痕迹
js/check/ignore/storage.yml:257 | LogRecord.cs:SetReuseObjectIdForSize | 留 | 零痕迹
js/check/ignore/storage.yml:259 | LogRecord.cs:ToString | 留 | 他路径锚属 `RecordInfo.cs:ToString`（wrecord/src/header.rs:590）与 `LightEpoch.cs:Entry.ToString`（wepoch/src/epoch.rs:817），均非同口
js/check/ignore/storage.yml:260 | LogRecord.cs:TryCopyFrom | 留 | 零痕迹（裸词只在无关注释出现）
js/check/ignore/storage.yml:261 | LogRecord.cs:TryReinitializeValueLength | 留 | 零痕迹
js/check/ignore/storage.yml:262 | LogRecord.cs:TrySetContentLengths | 留 | 零痕迹
js/check/ignore/storage.yml:263 | LogRecord.cs:TrySetContentLengthsAndPrepareOptionals | 留 | 零痕迹
js/check/ignore/storage.yml:264 | LogRecord.cs:TrySetETag | 留 | 同 249
js/check/ignore/storage.yml:265 | LogRecord.cs:TrySetExpiration | 留 | 他路径锚属 `MainStore/RMWMethods.cs:TrySetExpiration`（wkv/src/ttl.rs:273），C# 两处同名不同层
js/check/ignore/storage.yml:266 | LogRecord.cs:TrySetPinnedValueLength | 留 | 零痕迹（注意：同名近亲 `TrySetPinnedValueSpan` 未登记、rust 承接口在位 → 第一节甲族 11，须与本块同棒复核）
js/check/ignore/storage.yml:267 | LogRecord.cs:TrySetValueObject | 留 | 零痕迹
js/check/ignore/storage.yml:268 | LogRecord.cs:TrySetValueObjectAndPrepareOptionals | 留 | 叙述行 wkv/src/session/raw/write/mod.rs:57 指的是 `ObjectStore/RMWMethods.cs` 路径下的同名口
js/check/ignore/storage.yml:269 | LogRecord.cs:TrySetValueSpanAndPrepareOptionals | 留 | 零痕迹
块级提醒：本块理由自称「rust 对标单点在 wrecord record_mut.rs + header.rs」，而 wrecord 现刻对该 C# 文件
零锚点（该 crate 已锚的是 RecordInfo.cs / RecordDataHeader.cs 两族），理由的「单点」是机制面而非具名口——
属「理由表述过头」，不改判但应在下次触碰时改为「按位域/松弛写合并承接，无逐口对位」。

Utility.cs 块（13 条，storage.yml:2115-2131）——全留：

js/check/ignore/storage.yml:2115 | …/core/Utilities/Utility.cs:GetCallbackErrorMessage | 留 | .NET 异常反射取串，rust 走错误类型，零痕迹
js/check/ignore/storage.yml:2116 | Utility.cs:GetCallbackExceptionDetail | 留 | 同上
js/check/ignore/storage.yml:2117 | Utility.cs:GetCallerInfo | 留 | 本条为 GetCurrentMethodName：运行时栈取方法名，rust 无等价且不需要，零痕迹
js/check/ignore/storage.yml:2118 | Utility.cs:GetCurrentMilliseconds | 留 | rust 用 Instant/SystemTime 就地取
js/check/ignore/storage.yml:2119 | Utility.cs:GetHashString | 留 | 零痕迹（wbase/src/hash.rs 仅 murmur_hash2_x64_a）
js/check/ignore/storage.yml:2120 | Utility.cs:Is32Bit | 留 | 平台位数探测，rust 由目标三元组决定
js/check/ignore/storage.yml:2122 | Utility.cs:Murmur3 | 留 | 核到 wbase/src/hash.rs 全文件只 murmur_hash2_x64_a 一口，与 common.yml:363/364 同口径
js/check/ignore/storage.yml:2126 | Utility.cs:Rotr64 | 留 | 零痕迹（rust 用 rotate_right 内联于哈希实现）
js/check/ignore/storage.yml:2127 | Utility.cs:SlowWithCancellationAsync | 留 | 取消等待辅助，rust 走 select!/CancellationToken，零痕迹
js/check/ignore/storage.yml:2128 | Utility.cs:ThrowTsavoriteException | 留 | 零痕迹
js/check/ignore/storage.yml:2129 | Utility.cs:WithCancellationAsync | 留 | 同 2127
js/check/ignore/storage.yml:2130 | Utility.cs:XorBytes | 留 | 零痕迹
js/check/ignore/storage.yml:2131 | Utility.cs:strerror | 留 | libc 错误串，平台绑定

LightEpoch.cs 块（5 条）——全留：

js/check/ignore/storage.yml:1084 | …/core/Epochs/LightEpoch.cs:UserWordRef | 留 | `grep -i user_word wedb/wepoch/` 零命中，理由「rust 用原生原子/通道」经核成立
js/check/ignore/storage.yml:3714 | LightEpoch.cs:AllocateUserWord | 留 | 同上（本条即第一节 1.2 所记转入 ignore 的四名之一）
js/check/ignore/storage.yml:3715 | LightEpoch.cs:GetMinUserWord | 留 | 同上
js/check/ignore/storage.yml:3716 | LightEpoch.cs:ReleaseUserWord | 留 | 同上
js/check/ignore/storage.yml:3717 | LightEpoch.cs:ThisThreadUserWord | 留 | 同上

TsavoriteLog 族（8 条）——留 8（其中 2 条建议复核定性）：

js/check/ignore/storage.yml:1894 | …/TsavoriteLog/TsavoriteLog.Chunked.cs:AllocateBlockPartial | 留 | rust 分块由 waof aof header::AofChunkHeader 承接，无逐块分配口
js/check/ignore/storage.yml:1895 | TsavoriteLog.Chunked.cs:AllocateBlockPartialForTest | 留 | C# 测试专用分配钩子
js/check/ignore/storage.yml:1901 | TsavoriteLog.Chunked.cs:MaterializeInput | 留 | 零痕迹
js/check/ignore/storage.yml:1926 | …/TsavoriteLog/TsavoriteLog.cs:GetChecksum | 留（待复核定性） | `grep 'fn .*checksum' wedb/waof/` 零命中；若 AOF 帧需校验和则属真缺而非不承接
js/check/ignore/storage.yml:1944 | TsavoriteLog.cs:SetCommitRecordHeader | 留 | rust 提交记录头由 waof/aof/header/basic.rs:139 set_header_type 近亲承接，非同口，宜在理由中改点名该口
js/check/ignore/storage.yml:1945 | TsavoriteLog.cs:SetHeader | 留 | 同上，且本块另有 12 个 TsavoriteLog 名已挂规范锚（Enqueue/CommitAsync/…），说明块未整体过期
js/check/ignore/storage.yml:1960 | TsavoriteLog.cs:ValidateAllocatedLength | 留 | 零痕迹
js/check/ignore/storage.yml:1961 | TsavoriteLog.cs:VerifyChecksum | 留（待复核定性） | 同 GetChecksum，校验面在 rust 无口，需一次定性

其余零散（9 条）——留 7 / 改 2：

js/check/ignore/storage.yml:325 | …/Allocator/ObjectAllocatorImpl.cs:CreateSnapshotObjectReader | 留 | 对象日志物理层不存在（同块整面理由）
js/check/ignore/storage.yml:869 | …/ClientSession/TransactionalConsistentReadContext.cs:IsModified | 留 | 只读会话无脏标记面；与同块 Upsert/RMW/Delete 同口径（本块尚缺 RMW/Refresh 两名的登记，见第一节乙族）
js/check/ignore/storage.yml:873 | TransactionalConsistentReadContext.cs:ResetModified | 留 | 同上
js/check/ignore/storage.yml:1634 | …/Index/Tsavorite/Implementation/FindRecord.cs:TryFindRecordForPendingOperation | 留 | rust pending 面由 wkv raw 会话单点探针承接，理由点名 raw/mod.rs
js/check/ignore/storage.yml:1636 | FindRecord.cs:TryFindRecordInMainLogForPendingOperation | 留 | 同上
js/check/ignore/storage.yml:1849 | …/Index/Tsavorite/TsavoriteBase.cs:UpdateSlot | 留 | windex 表侧槽更新为私有内联（table.rs classify_slot 族），无具名口
js/check/ignore/storage.yml:2583 | …/Index/Common/PendingState.cs:CopyFrom | 留 | 理由「零调用方 + 字节切片原生」成立
js/check/ignore/storage.yml:— | （本块无第 8 条改判，改判两条在 common/client 交界，下列） | — | —

js/check/ignore/client.yml:95 | libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsMemoryByteArray | 留 | 客户端 SDK 应答分型，rust 传输层无该分型口
js/check/ignore/client.yml:96 | libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsNumber | 改 | `wedb/wconn/src/parser.rs:70` doc 已点名该 C# 口却写成「…（libs/client/GarnetClientProcessReplies.cs:86）」行号形态，CS_REF_REGEX 不认 → 改规范锚后条目自动淘汰

注：client.yml 两条按题面属「剩余块」，因与上表同批取数一并判尽，剩余数已相应扣除。

### 2.4 剩余块（未逐条判，标注剩余数与分块建议）

- server.yml：32 条待判（该文件 51 KB / 函数级条目最多，是暗集里唯一还没逐条过的主块）。
  建议按 C# 文件域切两批：Resp/RespServerSession + Parser/SessionParseState 域 16 条
  （含 server.yml:232 `SessionParseState.cs:Slice`——本单已单独核到 rust 有
  `wresp/src/session_parse_state.rs:55 pub fn slice` 实口，判「改」，可直接进首批）、
  Storage/ + TLS/ + AOF/ 域 16 条。
- playground.yml：7 条（playground/Bitmap/BitCount.cs 一族演练脚本，理由「非生产运行时代码」独立成立，
  预计全留；唯一提醒：同文件 `__simd_popcX128` 未登记，见第一节乙族末条）。
- libs/server/Resp/Bitmap/BitmapManagerBitCount.yml：1 条（`__simd_popcX128`，留，SIMD ISA 档位）。
- 合计剩余 40 条（原 42，扣除本单已顺手判尽的 client 2 条）。

### 2.5 第二节立即派单建议 Top 3

票 1 slug: ignore-dark-entry-slice-and-reply-number
射程文件: wedb/wresp/src/session_parse_state.rs、wedb/wconn/src/parser.rs、
js/check/ignore/server.yml（:232）、js/check/ignore/client.yml（:96）
判据: 130 条暗集中现刻仅有两条可判「改」且证据已核到实口（rust `slice()` 与 parser.rs 数值应答链），
一次改注释即触发自动淘汰，是「暗条目复活即假绿」警告下唯一确定能收的两条。工作量: 2 处注释 + 复跑 check.js 验回写。

票 2 slug: ignore-server-block-dark-review
射程文件: js/check/ignore/server.yml
判据: 剩余 32 条集中在该文件（Resp/Parser/Storage/TLS/AOF 五域），本单未判；须逐条给
「删/留/改」并按 2.1 的三条判据留证据。工作量: 只读判读 + 少量 yml 改动，禁动代码。

票 3 slug: ignore-logrecord-block-reason-truthing
射程文件: js/check/ignore/storage.yml（:220-269 块、:1926/:1961 两条）
判据: LogRecord 41 条判「留」但块理由把 rust 侧说成「对标单点」，而 wrecord 对该 C# 文件零锚；
另有 GetChecksum/VerifyChecksum 两名「留」得勉强（rust 无校验和口，可能是真缺）。
本票只做两件事：块理由改为准确表述 + 对两名做「真缺/不承接」定性，若定真缺则另开实现票。

## 第三节 裸文件名锚点按 crate 分批

### 3.1 现刻三形态读数（HEAD fbd2859，本单自算脚本口径 = CS_REF_REGEX + csPathNormalize）

- 裸文件名锚点（归一后路径不含 "/"，形如 `TsavoriteLog.cs:TryEnqueueCommitRecord`）：全注释口径
  465 处 = src 323 + tests 142；只算 doc 注释（`///`、`//!`，即前序 muse 棒原口径）= src 225 +
  tests 123 = 348。移交基线「225 处 src / 347 含 tests」在本单 doc 口径下逐字复现（差 1 为后续
  波次新增），故本批射程锁定 src doc 面 225 处。
- 这一族的实际危害（muse.design:170-176 判例复述并核实）：`csPathNormalize`
  （`js/check/rustScan.js:36`）只剥 `garnet/` 前缀、不做 basename 回查，语料侧 key 是 `libs/...`
  全相对路径，故 `doc_file_fn_map` 里登记为空 → 映射不入册；`symbolCheck.js` 第 148 行
  `if (!cs_path.includes("/")) { stat.bare_skip++; continue; }` 直接跳过不判 → 也不报失真。
  叠加第一节末段的 doc_set 词元兜底，这类锚点既掩盖 miss 又不自报违规，是最静的一族。
- 「路径.cs:行号」形态（CS_REF_REGEX 要求冒号后首字符为 `[A-Za-z_]`，数字开头即不匹配）：
  src 667 处 + tests 170 处 = 837 处（按注释行内出现次数计）。注意其中多数是带行号的佐证性
  叙述（如 `garnet/libs/server/AOF/AofProcessor.cs:451-482`），不属映射锚，不该机械改写。
- 截断路径锚点（有 "/" 但 garnet 下无此文件）：src 95 + tests 11 = 106，其中 wvector 一个 crate
  占 71，形态齐一为 `diskann-garnet/<X>.cs:<Sym>`（前缀缺 `libs/server/Resp/Vector/`），
  是 `bun js/check/symbolCheck.js` 现报 B 层 129 处的主力；属一批可机械修的机制族，与本批同派。

### 3.2 按 crate 统计（裸文件名 / 行号形态 / 截断，src 与 tests 分列）

crate         裸src 裸test  行号src 行号test  截断src 截断test  涉文件
wnode          104    93     221     98       2      5       141
wkv             60    19      46     13       4      1        41
wconf           47     0     135      4       0      0         8
wedb            34    12     161     36      11      1        70
waof            16    15       1      0       0      1        13
whlog            2     1      19      3       2      2         9
wresp           11     0      11      1       0      0         7
wtxn             5     1      10      3       0      0         5
wcol             6     0       8      4       0      0         7
wcpr             3     0       9      4       3      1         7
wbase            1     0      10      0       0      0         4
wconn            4     0       5      0       0      0         6
wbftree          6     0       1      0       0      0         5
wcompact         0     0       7      1       0      0         5
wcustom          2     0       5      0       1      0         3
wval             2     0       3      0       0      0         2
wreviv           2     1       2      0       0      0         3
wext_roaring     4     0       1      0       0      0         1
wlua             2     0       2      0       0      0         4
wedb_standalone  1     0       3      0       0      0         1
wdev             4     0       0      0       0      0         3
wmetric          3     0       1      0       0      0         3
windex           0     0       1      3       0      0         2
whyperlog        0     0       2      0       0      0         2
wpubsub          0     0       2      0       0      0         2
wext_json        1     0       1      0       1      0         2
wrecord          1     0       0      0       0      0         1
wbitmap          1     0       0      0       0      0         1
wvector          1     0       0      0      71      0         1
其余（wacl/wepoch/wtxn_test/hash 等）0 处
合计            323   142     667    170      95     11

- 高度集中在文件域：src 侧裸锚 top 文件 = `wconf/src/runtime_server_options.rs` 35、
  `wkv/src/session/raw/write/inplace.rs` 21、`wnode/tests` 之外的 `wnode/src/resp/garnet_api/slow.rs` 17、
  `wnode/src/resp/objects/tiered_collection_ops.rs` 15、`wconf/src/node_options.rs` 9、
  `wnode/src/resp/resp_server_session.rs` 5；行号形态 top = `wconf/src/node_options.rs` 123、
  `wedb/src/server/…`（migration/cluster 域）约 60、`wnode/src/resp/…` 约 90。
- 判据先于派单（防把 837 处行号引用机械改掉）：一处注释该不该改，看两条——
  (1) 它是否位于某 rust 函数/结构的直接前导 doc（rustScan.js:68 rsDocExtract 收集面）；
  (2) 该 rust 口是否已对位某 C# 函数而门禁册内无映射。两真才改锚；纯行号佐证、
  块内引用、tests 里的叙述一律留。

### 3.3 分批派单（每批 ≤40 处，同 crate 同文件域聚堆）

批 1（wconf 裸锚域，35 处）: `wedb/wconf/src/runtime_server_options.rs` 一文件独占 35 枚 doc 裸锚，
一棒可尽；`wconf/src/node_options.rs` 的 9 枚裸锚并入同批上界 44 → 建议拆为批 1a（35）+ 批 1b（9 + 行号酌改）。
批 2（wkv 原始写域，21 处）: `wedb/wkv/src/session/raw/write/inplace.rs`；与第一节甲族无重叠，
但 `wkv/src/session/raw/read.rs`（裸 4 / 行号 8）留批 3，防与在途票 `db-raw-read-variant-collapse` 撞面。
批 3（wkv 读侧 + wcpr + wreviv + wbase 零头，约 24 处）。
批 4（wnode resp/garnet_api 域，约 26 处）: slow.rs 17 + raw.rs 6 + mod.rs 零头。
批 5（wnode resp/objects 与 rangeindex 域，约 25 处）: tiered_collection_ops.rs 15 +
resp_server_session_range_index.rs（行号 10）+ objects 零头。
批 6（wnode src 其余，≈40 处）: resp_server_session.rs 5+28、server.rs 1+28、service.rs 3+21 等，
按「函数前导 doc」判据酌量取，预计实改约 30。
批 7（wnode tests 域，约 38 处）: garnet_etag.rs 裸 17、resp_objects_dispatch.rs 9、
resp_vector_set_wrong_type.rs 9、resp3_null_parity.rs 4 —— tests 面按判据多为「测试叙述」，
建议本批只补映射缺失、不追覆盖率。
批 8（wedb 与 wcustom 的 wvector 前缀族，71 处）: `diskann-garnet/` → `libs/server/Resp/Vector/`
机械加前缀，一次成型；因超出 40 上界，按 service.rs / filter/runner.rs / filter/expression.rs 三档拆
（约 30/25/16）。此批落地直接压低 symbolCheck B 层 129 读数，是本节收益最高的一批。
批 9（waof 与 wconn/wcustom/wmetric/wdev/wcol/wtxn 长尾，约 36 处）。

### 3.4 第三节立即派单建议 Top 3

票 1 slug: anchor-bare-diskann-prefix
射程文件: wedb/wvector/src/service.rs、wedb/wvector/src/filter/runner.rs、
wedb/wvector/src/filter/expression.rs
判据: 71 处同形态前缀缺段（`diskann-garnet/` 未剥、非 `libs/` 起）+ symbolCheck B 层 129 的主力；
形态齐一、可机械补 `libs/server/Resp/Vector/` 前缀，且属 A 层硬断言族，改完即被门禁采信。
工作量: 一次批量改注释 + 复跑 `bun js/check/symbolCheck.js` 核减读数。

票 2 slug: anchor-bare-wconf-runtime-options
射程文件: wedb/wconf/src/runtime_server_options.rs、wedb/wconf/src/node_options.rs
判据: 单 crate 独占 src 裸锚 44/323（其中 doc 面 35 枚集中在一文件），是「已实现未入册」最厚的一坨；
wconf 无在途票撞面。工作量: 纯注释 44 处，改后 check.js 映射册增 40+ 条。

票 3 slug: anchor-bare-wkv-write-and-wnode-garnet-api
射程文件: wedb/wkv/src/session/raw/write/inplace.rs、wedb/wnode/src/resp/garnet_api/slow.rs、
wedb/wnode/src/resp/objects/tiered_collection_ops.rs
判据: 三文件合计裸锚 53 枚，全在存储/命令派发主链，注释在位、锚点形态不登记；与在途
`db-raw-read-variant-collapse`（read.rs 侧）划界为「只动 write 与 garnet_api/objects」。
工作量: 纯注释 53 处，分两棒派（wkv 一棒、wnode 一棒）。
