# r9 波 RESP SCAN 族游标语义面空手甄别档（2026-10-01，只读甄别席 + 主控复核）

基线 `ff4e1a7`。分诊面：SCAN / HSCAN / SSCAN / ZSCAN 的游标编解码、COUNT、MATCH 模式匹配、
(NO)VALUES/REV 修饰面、迭代中键被删改的游标稳定性、scan 与 TTL 过期/冷租户装载交互。
C# 权威锚：`garnet/libs/server/Resp/Cursor.cs`、`libs/server/Resp/Objects/*Commands.cs` 的 `*Scan*`、
`libs/server/Resp/KeyAdminCommands.cs:NetworkSCAN`、`libs/common/GarnetObjectBase.cs:ReadScanInput`、
`libs/server/Objects/*/*Object.cs:Scan`、`libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorScan.cs`。

## 结论：0 立案（扫描族现面与 C# 现树契约对位完整，未见未登记者）

## 淘汰项（逐条附不可达 / 已在册理由）
1. **SCAN 空页硬写游标 0**（C# `ArrayCommands.cs::NetworkSCAN:319-331` keys 空即回终结游标）——
   两侧 acceptedCount 均以**匹配项**计数（`AllocatorScan.cs::ScanLookup:268` 的 Reader Skip 不计数，
   rust `array_key_iteration_functions.rs::scan_cursor` 同口径）；空页必然 ⇒ 扫尽 ⇒ 游标天然 0，
   可观察行为全等，且该规整已登 `doc/zh/deviations.md` §188。
2. **游标校验 snap 回扫差**（C# `SnanCursorToLogicalAddress` 回退对齐续扫 vs rust 判 false 终结回 `(0, 空)`）——
   明文在册 §189（`whlog/src/scan.rs::validate_cursor` 头注同锚）。
3. **`lastScanCursor` 幂等豁免缺失**（C# `ArrayKeyIterationFunctions.cs::DbScan:96` 依连接态豁免 vs
   rust 无状态恒校验）——函数头注自证：合法续页游标恒为记录起始字节（`deliver` 后 `advance`），
   页满游标可过校验，仅收窄不丢面。
4. **TYPE 给出时 COUNT 弃用**（C# `long.MaxValue` vs rust `usize::MAX`）——
   `garnet_api/slow.rs::C::Scan` 逐形复刻，等价。
5. **zset 分值 Utf8Formatter null 项**（C# `SortedSetObject.cs::Scan` 理论 null 返回）——
   rust 注订正「C# 恒成功、null 为死臂」；inf/-inf 词形分叉在册 §80。
6. **COUNT 极值 i32 回绕 / 负值全量遍历 / count=0 首条即停**——§185 在册，怪癖 1:1 保留。
7. **HSCAN 游标双解析器**（`shared_object_commands.rs::scan_validate` 前门 `try_parse_i64` vs
   `wcol/src/types/scan_input.rs::read_scan_input` 后门 `strict_i64`）——前门仅预检，
   可观察文法 = strict，属 §32 全仓 strict 文法判据统辖，非双形。

## 已核清阴性清单（逐字对过）
- **解析面**：`read_scan_input` vs `GarnetObjectBase.cs::ReadScanInput`——cursor i64 非负、COUNT i32、
  仅 COUNT 上钳 `OBJECT_SCAN_COUNT_LIMIT`（两侧默认均 1000）、默认 10、未识别词元静默跳、
  MATCH 空 pattern = 全匹配。一致。
- **对象层三臂**：hash（NOVALUES 不翻倍/否则翻倍、expired 垫数先于 index 跳过、`total < start` 早退，
  `HashObject.cs::Scan:368`）；set（不翻倍、`cursor == Count` 收敛、无过期，`SetObject.cs::Scan:190`）；
  zset（恒成对、恒 `count*2` 截断、忽略 isNoValue，`SortedSetObject.cs::Scan:459`）——
  rust `wcol` 三实现逐判定一致；list 对位 C# `NotImplementedException`（无扫描臂）。
- **大小写文法分层**：键级 MATCH 不敏感（C# `Reader` 传 `true`）vs 对象级敏感
  （4 参 `Match` 默认 false，`GlobUtils.cs:17`）——rust `glob_match_nocase`（键级）/`glob_match`（对象级）
  分轨一致。
- **收敛判定**：`scan_converge_cursor` 的 `>=` 修复（死锁死角）在册；分层臂 `exec_tiered_scan`
  锁窗刷 meta（§129）、start 越界守卫、升阶序域切换注记齐备。
- **错误帧形**：NOTFOUND `[0,*0]`、WRONGTYPE、arity 门、INVALIDCURSOR 文案——
  `write_scan_not_found` vs `SharedObjectCommands.cs:81-86` 一致。

## 后续处置
本面已扫净，勿在相邻波次重派同缝甄别席（除非 `scan_cursor` / `read_scan_input` / `exec_tiered_scan` 出现新改动）。
