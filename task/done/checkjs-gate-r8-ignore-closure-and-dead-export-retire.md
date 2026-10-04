# check.js 门禁清形：15 目录级缺失族补登/锚正规化 + 零消费导出函数退役（P3 治理面）

定级：P3（门禁面：`bun js/check.js` 的「实现缺失」15 项与「重复定义」1 簇全部为登记缺位或锚写形制问题，非真缺实现；门禁不净则后续甄别被噪声带偏）

甄别结论：通过（2026-10-01 只读甄别席逐项双侧现码亲验 + 主控现码抽验四处，见下「主控亲验」标注）

## 主控独立复验（2026-10-01 收票审计，交付 commit 0c0e128 + 归档 28fb515）

- `bun js/check.js` 于基线 e7efacb 单跑复核：**「# 重复定义」与「# 实现缺失」两段均已归零**（票面验收①达成），
  残余仅 B 层词法提示 3 处与 4 条「可淘汰 ignore」只读通知，EXIT=0；此后任一段复现内容即视为新红，不必再对照旧清单。
- 零消费导出确已退役：wmetric/src/latency/garnet_latency_metrics.rs 现仅存 `get_latency_metrics`（:189），
  全仓 grep `get_latency_metrics_multi` 零残留（票面验收②达成）。
- 与主控同波亲办的悬空锚订正（e7efacb）无冲突：TaskPlacementCategory.yml / TaskType.yml 现形制为
  「判定依据在册载体见 TaskManager.yml 理由行」；task/done 三处「载体注记」假锚已改正规「路径::符号」形并稳在 dev
  （B 层提示由 4 降至 3 即其效），后续票勿再回改成载体注记或「本文件 N 行」形。
- 席报两项遗留经主控现码复跑结案：`read_varsize_iid 失败语义倒置` **形制不成立**（wvector/src/store.rs:351
  实为「读失败与键缺失同形折 None」，无 false/true 取反；该同形面属 §181 已备案另案且收口点在他席在途域）；
  LuaRunner 死码属其 G2A-2 在途域 ⇒ 均并案不另开票，详见 task/done/r8-screening-candidates-closed-20261001.md。

## 背景

`bun js/check.js`（主树）当前输出：

- 「# 实现缺失」15 项，全部是 C# 目录/文件级族名（非 `File.cs:Func` 函数级），即整族无 rust 锚点被采信；
- 「# 重复定义」1 簇：`libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetLatencyMetrics`
  同时被 `wmetric/src/latency/garnet_latency_metrics.rs:189`（单类别）与 `:198`（多类别）锚定。

甄别席逐项结论：15 项**均无真缺实现**，分流全为 B（登记缺位/锚写形制不合门禁词法），零项 A。

## 主控亲验（四处抽验，坐实分流可信）

1. `GetLatencyMetrics` C# 确有两个重载：`garnet/libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:181`
   （单类别 → `MetricsItem[]`）与 `:190`（数组重载，`:195` 内部循环转调单类别）——成立。
2. rust 侧 `get_latency_metrics_multi`（`wmetric/src/latency/garnet_latency_metrics.rs:198`）
   **全仓零消费者**（grep 全 `wedb/` 仅命中定义行本身），而 `:189` 文档注释已自陈
   「多类别重载的 rust 消费面由 RESP LATENCY HISTOGRAM 以循环单类别承接，重载不转写」——
   两处口径互斥，且导出无人消费属零死代码清理面。
3. `garnet/libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs` 文件实存（新 partial 族）——成立。
4. `HyperLogLog.cs` 的 `DumpSparseRawBytes:1094`/`CompareSparseToDense:1154`/`DumpDenseRegs:1218`/
   `DumpSparseRegs:1234` 等臂消费者全在本文件内（`:1141/:1207/:1210`），全仓 `libs/` 外部零命中——调试死码，成立。

## 逐项分流与处置口径（15 项）

一律「先正规化锚注释、后 ignore 登记」取舍：凡 rust 已有等价承接点而注释形制不合门禁词法（夹行号、
文件级注记、仅词元提及）的，改写为正规锚 `libs/.../File.cs:FunctionName`；凡确属不实现/死码/
SIMD 分级不下移植口径的，补 `js/check/ignore/**.yml` 条目并写**真实双侧理由**。

1. `libs/client/GarnetClientProcessReplies: ProcessReplyAsNumber`（C# 现位 :68，:311/:320 内部消费）
   → ignore：`client.yml` 已登同族 CopyErrorToSpan/ProcessReplyAsMemoryByteArray/TryConsumeMessages，补此臂
   （.NET 客户端库不实现口径，整面理由在册）。
2. `libs/client/RespReadResponseUtils: TryReadSimpleString / TryReadIntegerAsString / TryReadIntWithLengthHeader`
   （C# 现位 :18、:44 与 Int 臂）→ ignore 补登；rust 同族其余方法已有正规锚于 `wconn/src/parser.rs`。
3. `libs/common/Crc64: Reflect64`（C# 现位 :11，仅服务 Crc64Bitwise）
   → ignore 补登（rust 侧 `wbase/src/crc64.rs` 以查表折叠替代逐位反射，不转写）。
4. `libs/common/RespReadUtils` 5 臂 → ignore 补登；SimpleString 臂注明 rust 对位于 `wlua/src/runner/resp_convert.rs`，
   `TryReadInt32WithLengthHeader` 除定义与测试外零消费（C# 死码）。**须逐臂实测后再登，禁整族一句带过。**
5. `libs/server/Lua/LuaRunner: InitializeNoScriptDetails`（C# 现位 :2634）
   → 正规化：rust 等价承接为编译期位图，位于 `wnode/src/resp/resp_server_session/lua.rs`（注释夹行号形制不合词法）。
   **该文件落同侪在途禁触域 `wnode/src/resp/**`——本波禁改，改走 ignore 登记（`ignore/garnet/libs/server/Lua/` 下
   LuaRunner.yml 已有同款条目，追加此臂并注明编译期位图承接），禁碰源文件。**
6. `libs/server/Resp/Bitmap/BitmapManager: TryValidateLengthInBytes`（C# 现位 :27，:94 自用）
   → `BitmapManager.yml` 追加（已登 IsLargeEnough/Length/NewBlockAllocLength 同径臂）。
7. `libs/server/Resp/Bitmap/BitmapManagerBitOp` 5 臂（含 Vectorized512/256/128 SIMD 分级臂）
   → 新建/追加 `BitmapManagerBitOp.yml`：SIMD 臂按 `BitmapManagerBitCount.yml` 既有「不向下移植」判例登记；
   便携折叠器等价承接待正规化项落 `wbitmap/src/bit_op.rs`（该文件非禁触域，可正规化锚注释）。
8. `libs/server/Resp/HyperLogLog/HyperLogLog` Dump*/Compare* 6 臂 → `HyperLogLog.yml` 追加，
   照既有 `SparseToDenseCopy` 死码登记格式（本文件内自用、全仓零外部消费者、生产不可达）。
9. `libs/server/Transaction/TxnClusterSlotCheck: SaveKeyArgSlice`（C# 现位 :18）
   → 正规化 `wtxn/src/txn_keys_buffer.rs` 承接锚注释（词元级提及 → 正规锚）；若不达词法则补 `server.yml` 条目。
10. `libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn: TestBucketHeadCacheLineOffset`
    （C# 现位 :1204，消费者仅 C# 测试册 `SectorAlignedBufferPoolTests.cs:57`，该册已登记）
    → `storage.yml` 补该新 partial 文件族条目（整面「rust 单形态 wbase pool 承接」理由已在册）。
11. `test/standalone/Garnet.test/Resp/RespCommandCacheTests: CachedCommandDoesNotTruncateArgumentCount`
    （C# 现位 :39）→ `test.yml` 该册段追加（整册「刻意不移植：rust 命令分发为编译期目录」理由已在册）。
12. 重复定义簇：删除零消费的 `get_latency_metrics_multi`
    （连带删除其文档注释，`garnet_latency_metrics.rs:189` 的「重载不转写」口径随之自洽，
    多类别消费面维持循环单类别承接），**不采用**「保留双函数 + 改锚 + ignore」路线
    （保留即导出无人消费，违零死代码规约，且 C# 数组重载本身仅是一层循环糖）。

## 验收

1. `bun js/check.js`（主树运行；席位沙箱若 node_modules 软链可用亦可自验，不可用则报由主控主树复跑）
   「# 实现缺失」与「# 重复定义」两块均为空；
2. 「仅词元提及」清单不得因本票新增（第 5/7/9 项若走正规化必须真达词法，否则回落 ignore）；
3. `cargo check -q --workspace --all-targets -j 3` 零告警（禁 `#[allow]`／`#[expect]`），
   `cargo nextest run -p wmetric -p wbitmap -p wtxn` 定向绿（第 12 项删函数后不得留下断引用）；
4. 禁跑 `./test.sh` 与 `./sh/clippy.sh`（门禁归主控）；
5. ignore 条目理由必须写实测双侧证据（C# 现位 + rust 承接现位或「零消费者」），禁空泛「无需实现」；
6. 单提交，消息以 `docs(check): ` 起头（第 12 项含源码删除，须在同一提交内并可被 numstat 复核）。

## 禁触域（同侪主树在途编辑，本波禁碰）

`wedb/wnode/src/resp/**`、`wedb/wnode/src/aof/**`、`wedb/wnode/src/service.rs`、
`wedb/wedb/src/server/replication/**`、`wedb/wcpr/**`、`wedb/wdev/**`、`task/refactor-backlog.md`。
另禁手改 Cargo.toml（依赖只 `cargo add`）、禁占位实现、禁 `--prune-ignore`（落盘破坏性操作归主控）。

## 终态注记
- 收口形态：补登 15 项目录级与臂级 ignore 并正规化 wbitmap/wtxn 锚注释，退役 wmetric 零消费的 get_latency_metrics_multi 导出函数；bun js/check.js 门禁「# 实现缺失」、「# 重复定义」与「仅词元提及」全数清零。
- 合入哈希：0c0e128
- 状态：已收口归档。

