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
  既无锚又无 ignore 的「门禁盲名」32。32 名即「10 份 31 名」的现刻等价像。
- 门禁为何不报这 32 名（新增发现，值得单列后续工具票）：`js/check.js:423-427` 的
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

### 1.3 现刻 32 名逐条判（实现在位缺锚 15 · 无具名对位口 17 · 完全无实现 0）

关键结论：本批 32 名按现刻 HEAD 逐条核到 C# 签名与 rust 消费点，无一属「rust 侧完全无实现」，
全部落在两族——实现在位但注释锚点形态不登记（该改注释）与「无独立具名对位口、语义在消费点就地
承接」（该补 ignore 登记，不该派实现票）。原票「下一步该按 miss 派实现票」的预设不成立，
按现刻读数应改为「派注释整改票 + 登记补全票」，实现票只 2 张（1.4 票 6、票 7）。

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

- SessionParseState.cs:SetArgument / DeserializeFrom / TryGetLong / TryGetDouble / TryGetFloat /
  GetString / TryGetBool（7）— rust 侧无 parseState 级读口，整数/浮点走 `wbase/src/num.rs` strict 通道、
  布尔走 `wedb/src/server/cluster_session/replication.rs:751` 就地 T/F 判形（可并入 `js/check/ignore/server.yml:225`
  或 `:993` 既有 SessionParseState.cs 块）
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

丙族（真缺，派实现票）：见 1.4 票 6、票 7（各 1 名，均非 byte* 族）。

### 1.4 第一节派单建议 Top 5

票 1 slug: cs-anchor-vector-callbacks-unmanaged
射程文件: wedb/wnode/src/resp/vector/vector_store_callbacks.rs、wedb/wvector/src/store.rs
判据: 甲族 1-6 六名同 C# 文件同承接面，一次改注释即可把 6 枚映射登记入门禁，且须顺手消掉
wvector 侧同名重复挂载，避免触「重复定义」。工作量: 纯注释 6 处，无函数体改动。

票 2 slug: cs-anchor-parse-state-arg-readers
射程文件: wedb/wresp/src/session_parse_state.rs、js/check/ignore/server.yml（:225 或 :993 块）
判据: SessionParseState.cs 8 名一棒判尽——GetArgSliceByRef 补锚、余 7 名补 ignore 条目并写理由，
消除「byte* 形参族暴露的缺口」里最大的一坨。工作量: 1 处注释 + 1 处 yml 条目，需逐条写理由。

票 3 slug: cs-anchor-tsavorite-tag-slot-and-pinned-span
射程文件: wedb/windex/src/table.rs、wedb/windex/src/chain.rs、wedb/wrecord/src/record_mut.rs、
wedb/wrecord/src/header.rs、js/check/ignore/storage.yml（:2114 / :861 块）
判据: 甲族 9-11 + 乙族 Utility/TransactionalConsistentReadContext 共 7 名全在 tsavorite 系 crate，
一次跑完可让 windex/wrecord/wbase 的映射与登记同时收口。工作量: 3 处注释 + 4 条 yml。

票 4 slug: cs-ignore-backfill-gate-blind-spots
射程文件: js/check/ignore/server.yml、common.yml、storage.yml、playground.yml
判据: 乙族余 10 名补登记（含 playground/Bitmap/BitCount.cs:__simd_popcX128 这条「注释自称已登记、
实际未登记」的假账），把 32 名清零成「要么有锚要么有登记」。工作量: 10 条条目 + 理由，无代码。

票 5 slug: checkjs-miss-token-coverage-hole
射程文件: js/check.js（isDocumented）、js/check/rustScan.js（doc_set）、js/check_selftest.js
判据: 本节 1.1 末段——doc_set 词元全集使 32 名结构性不进 miss，属门禁漏报，须先立判据再改口径
（建议 miss 判定只认 path 精确映射，token 面降为提示）；工具票，不与实现票混派。
工作量: 判定口径一处改 + 断言。

（另：真缺实现票仅 2 张，见票 6/票 7——原票预设「31 名皆实现缺口」经逐名取证后不成立。）

票 6 slug: my-parse-state-arg-writeback-slot
射程文件: wedb/wresp/src/session_parse_state.rs、wedb/wnode/src/resp/parser/*
判据: 若逐条复核认定 C# `SetArgument(int, PinnedSpanByte)`（garnet/libs/server/Resp/Parser/
SessionParseState.cs:39）的「回写参数槽」在 rust 无等价物且将来 AOF 重写参数需用它，则补实现；
现刻判「无消费面」，属可选实现票，优先级最低。

票 7 slug: my-session-parse-state-deserialize-from
射程文件: wedb/wresp/src/session_parse_state.rs、wedb/wnode/src/aof/replay_input.rs
判据: C# `SessionParseState.DeserializeFrom(byte*)` 有 serialize_to 对位
（`wresp/src/session_parse_state.rs:159`）而 deserialize 侧仅由 replay_input.rs:235 组合形态承接，
若回放需重建 parseState 则补对称实现。工作量: 单函数 + 1 测试。

## 第二节 ignore 暗条目逐条判

（待补）

## 第三节 裸文件名锚点按 crate 分批

（待补）
