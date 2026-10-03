# checkjs-r2：B 层符号断言 42 处路径失真修正（专席）

## 背景
check.js r3（/tmp/_rs/checkjs-r3.txt）libs 族 A 层违规已清零（前票 checkjs-r1 补锚+ignore 登记毕）。`bun js/check/symbolCheck.js` 余 42 处 B 层「路径失真」——rust 注释锚点引用 C# 符号时用了截断/失真路径（缺 `libs/…` 前缀或指错文件），个别符号名与真实落点错位。42 处全清单：`bun js/check/symbolCheck.js` 现跑现出（勿引用旧快照）。

## 任务
逐处将锚点改为 C# 全路径（工具行已给「该符号真实落点」提示；同名多落点时选语义正确的那一个，须开对应 .cs 核对调用面，不许盲选第一条）：
- 多数只需补全前缀：如 `Recovery/Recovery.cs:RecoverHybridLogAsync` → `libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:RecoverHybridLogAsync`
- 语义错位需判断的：
  - `MainStore/RMWMethods.cs:InPlaceUpdater`（set.rs:331/895、bitmap_commands.rs:89、tests/hyperloglog.rs:1496）——该符号不在 MainStore/RMWMethods.cs；核对 C# InPlaceUpdater 实际域（ObjectStore/RMWMethods.cs 或 Tsavorite LogRecord/ClientSession），按注释所指语义角色改正
  - `MainStore/UpsertMethods.cs:InPlaceWriter`、`MainStore/DeleteMethods.cs:InitialDeleter`（object_store_utils.rs:1077、rmw_writeback_revalidate.rs:13）——同上，落点在 ObjectStore/UnifiedStore 族
  - `ObjectStore/ReadMethods.cs:Reader`（tests/resp_set.rs:947）
  - `Session/ObjectStore/SortedSetOps.cs:SortedSetPop`（sorted_set_pop_empty_selfheal.rs:4）
  - `Objects/SetCommands.cs:SetMembers`（rmw_rebuild_side_domain_retire.rs:233）
  - `Databases/MultiDatabaseManager.cs:GetKeyspaceStats`（keyspace.rs:567）——真实落点 IDatabaseManager.cs/DatabaseManagerBase.cs/StoreWrapper.cs 三选一按语义
  - `MainStore/RMWMethods.cs:TrySetExpiration`（ttl.rs:320）——真实落点 Allocator/LogRecord.cs
  - `Storage/Session/StorageSession.cs:StorageSession`（garnet_api/mod.rs:461）——**符号全树不存在**：核注释语义后改挂正确构造锚或删该锚行并改写文字（不许留假锚）
- test 族锚（collect_arm_rmw_window_scope.rs:17 `RespHashTests.cs:CanDoHashCollect` 落点系 Garnet.test.collections 目录）按真实落点改径

## 硬约束
- 只改注释锚点文本，零行为改动；禁改函数体/测试体
- worktree 开发：./fork.sh <名> → /tmp/fork/<名>，CARGO_TARGET_DIR=/tmp/_rs/<名>，cp -al donor 预热（先例 donor /tmp/_rs/wvector-* 或复用主 target 冷编译）
- 验收：`bun js/check/symbolCheck.js` 违规 0（或剩项给出书面理由拟登 js/check/symbolignore.yml）；`cargo check --workspace --all-targets` 无新告警；`bun js/check.js` 实现缺失保持 0
- 禁触：custom_object_commands.rs、resp_roaring_bitmap_tests.rs（他席脏文件）、task/ing/wnode-collect-fallback-blind-write-after-recheck.md 辖面（storage_session.rs obj_save 域）
- 合入 dev 后由主控门禁复核；票面报告给 42 处逐条对照表（原锚→新锚→核对证据 .cs:行）

## 收口记录（2026-09-28 06:3x）
- 合入：6aa452e2（--no-ff，双引号 merge 消息=我方波次；席提交 d8a01927 已 rebase 至 fb398a89，零冲突）
- 收口形态：36 文件 41±，diff 非注释增行 0（纯锚点面）；特例 garnet_api/mod.rs 类锚查实 C# 类真实存在（StorageSession.cs:16），补全即正确锚非假锚
- 门禁：dev 现场 symbolCheck 违规 0（锚点 5591/豁免 1/裸文件名跳 613）；check.js 实现缺失空（后续 C 族复缺两项已另票登记：2ed9f226）
- 尾巴：fmt 差异 7 处经核实全属他席 r5f 并入遗留（非我 36 文件面），逐文件 rustfmt 收口 ca7b5e33；worktree/分支已清
