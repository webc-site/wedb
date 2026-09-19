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
