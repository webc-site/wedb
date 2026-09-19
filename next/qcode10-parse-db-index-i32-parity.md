DB 号解析域后移：parse_db_index 用 u64 域，SELECT/SWAPDB/DBID 三处错误档位偏离 C# int32

来源：qcode 第 10 轮 net 条 3（next/qcode10.net.md:40-42）。取证基线 dev。

问题
- C# SELECT/SWAPDB/TryParseDatabaseId 全部经 TryGetInt → ParseUtils.TryReadInt → int32，
  且要求「整段必须消费完」（ParseUtils.cs:47-55 allowLeadingZeros:false、bytesRead==length），
  超 i32 范围的字面量在 C# 属「不是整数」档。
- rust 换成 wbase/src/num.rs:137-173 parse_db_index（u64 域，负数才报 OutOfRange，文档自称
  「支持 0..=u64::MAX」），范围门前移失败：
  `SELECT 3000000000` C# 回 `-ERR value is not an integer or out of range.`，
  rust 回 `-ERR DB index is out of range.`；
  `SWAPDB 3000000000 1` C# 回 `-ERR invalid first DB index.`，rust 回 DB index is out of range；
  BGSAVE/LASTSAVE 的 DBID 同理。错误文案是客户端可断言的线面事实。

消费点
- wnode/src/resp/array_commands.rs:364-372（SELECT）、:410-428（SWAPDB 两库号）
- wnode/src/resp/admin_commands.rs:618-631（try_parse_database_id）

C# 参考
- libs/server/Resp/Parser/SessionParseState.cs:391-395 TryGetInt
- libs/server/Resp/Parser/ParseUtils.cs:47-55 TryReadInt
- libs/server/Resp/ArrayCommands.cs:125-139、:175-191
- libs/server/Resp/AdminCommands.cs:1121-1125

修法
- parse_db_index 收口为 i32 域解析（含「整段消费完」与 C# 同档的 not-an-integer 分类），
  三处命令臂按 C# 逐臂回各自文案（value is not an integer / invalid first DB index /
  invalid second DB index / DB index is out of range）。
- 不做 u64 兼容垫片，不保留旧域常量；wbase 文档注释同步改写并注明 C# 锚点。
- 补线面断言：SELECT/SWAPDB/DBID 三命令的超 i32、负数、前导零、尾随垃圾四组输入，
  逐条对位 C# 文案。

验收
- 新断言全绿；cargo check -p wbase -p wnode；bun js/check.js exit 0。
- 无 #[allow(、无 #[ignore]、无恒真断言。

## 甄别复核（开发前独立复验，2026-09-19，基线 dev）

主张成立，逐条锚点复验：

- C# 档位单点：SessionParseState.cs:391 `TryGetInt(int i, out int value)` →
  ParseUtils.cs:47 `TryReadInt(PinnedSpanByte, out int)`（`slice.length != 0` +
  `bytesRead == slice.length` 整段消费 + `allowLeadingZeros: false`）→
  RespReadUtils.cs:258 `TryReadInt32Safe`（负值域至 `-((ulong)int.MaxValue)+1`，
  即 i32::MIN 合法、-2147483649 起 overflow → false）。库号线面只有 int32 一档。
- 三臂文案：ArrayCommands.cs:125 SELECT 解析失败 →
  `RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER`；:175/:180 SWAPDB 两库号解析失败 →
  `RESP_ERR_INVALID_FIRST_DB_INDEX` / `RESP_ERR_INVALID_SECOND_DB_INDEX`；
  AdminCommands.cs:1119-1141 TryParseDatabaseId 解析失败 → 非整数档，
  域内负数/≥MaxDatabases 才 `RESP_ERR_DB_INDEX_OUT_OF_RANGE`。
  常量文本见 CmdStrings.cs:248/254/255/256。
- rust 偏离复现：dev 的 wbase/src/num.rs:145 `parse_db_index` 返回
  `Result<u64, DbIndexError>`，文档自称「支持 0..=u64::MAX」，只有负数走
  OutOfRange。故 `SELECT 3000000000` 过了解析门、落 array_commands.rs 的
  MaxDatabases 门回 `DB index is out of range.`，C# 回
  `value is not an integer or out of range.`；SWAPDB/DBID 同形偏离。
- 负数档不偏离：现 OutOfRange 变体对 `index < 0` 的回包与 C# 一致，保留原语义。

甄别补记两点（票据未列，开发必处置）：

1. 消费点不止票列三处，全库共 6 处：另有
   wnode/src/resp/garnet_api/slow.rs（EXPDELSCAN 防御重解析、SWAPDB 慢路径
   重解析、COMMITAOF 防御重解析）与 wnode/src/resp/txn_resp_commands.rs
   （MULTI 内 SELECT 的 C# `index != activeDbId` 门），收口域须一并跟型；
   后两处按 C# 本就应随 i32 档改判（超 i32 字面量 C# 解析失败 → 不入分支）。
2. 唯一非库号消费者：wedb/src/server/cluster_session/replication.rs:291
   `CLUSTER FLUSHALL_NS <ns> …` 借道 parse_db_index 解析 u64 命名空间
   （无 C# 对应，wacl/src/user.rs:74 的 `<ns>#user` 面即 u64 全值域）。
   该字段属内部总线帧的 u64 线面值域，不在本票收口范围，不得随库号收窄，
   故为它落 wbase::num::strict_u64（文法与 strict_i64 全同的 u64 值域版），
   本处不是 parse_db_index 的 u64 兼容垫片——库号侧无 u64 出口。
3. 冲突面复验：dev 的 wnode/tests/resp_objects_dispatch.rs:257
   `select_supports_large_u64_db_id` 以 `max_databases: u64::MAX` 锁定
   `SELECT 18446744073709551614` → +OK。复验结论为该断言不成立为需求：
   配置侧 max_databases 为 i32（wconf/src/node_options.rs:333 带
   MAX_DATABASES_MIN..=MAX_DATABASES_MAX 界，C# Options.cs:687 同档），
   真实运行域内任何超 i32 库号必被上界门拒，只差文案；doc/zh/db.md §1.3
   「64 位库 ID」承诺的是内部 u64 标量表示与零膨胀（§1.2/§1.4
   `logic_db: u64`），本次不收。故该测试改为 i32::MAX 作大库号锚，
   并追断超 i32 字面量回非整数档，内部 u64 表示面（active_db_id /
   set_active_db / slot_of / swap_databases）零改动。

## 落地

- wbase/src/num.rs：`parse_db_index` 转调既有 `strict_i32`（与 C#
  TryReadInt 同口径，含整段消费与前导零拒绝）后判负，返回 `Result<i32, _>`；
  文档注释改写并注 C# 锚点；新增 `strict_u64`（ns 线面字段专用）。
- 严格整数扫描段（符号 + 前导零 + 整段消费 + u64 幅值）抽为文件内单点
  `scan_digits`，strict_i64 / strict_u64 共用，避免第二份文法副本
  （原 parse_db_index 自带的那份副本随之消失）。
- 三臂文案零改动即得 C# 档位（NotInteger → 各臂自有文案；OutOfRange →
  DB index is out of range），消费点按 `as u64`/`as i64` 无损升位。
- 线面断言：resp_tests.rs `can_select_command`（SELECT 9 组）、
  resp_tests.rs `swapdb_command_validation`（SWAPDB 9 组）、
  resp_server_session_tests.rs `database_id_validates_against_session_max_databases`
  （DBID 7 组），每组覆盖超 i32 上/下界、i32 边界、负数、前导零、尾随垃圾；
  wbase num.rs 单测同步改档。

