锁定注记（2026-10-01 r8 波主控，基线 7397ed1；主控全仓扫面 + 三处双侧亲验，行号以本注记为准）：
- 族形制：注释体「…精确锚点见本文件 N 行」全仓 **10 处 / 7 文件**（grep -c 实测）：
  wcol/src/itembroker/collection_item_broker.rs:1073、wkv/tests/compact/more_log_compaction.rs:331、
  wnode/src/resp/basic_commands/set.rs:391、wnode/src/resp/key_admin_commands/keys.rs:44/:88/:98、
  wnode/src/resp/objects/hash_commands/write.rs:29/:43、wnode/src/resp/objects/set_commands/read.rs:105、
  wnode/src/resp/objects/list_commands/write.rs:54。
- 双侧皆不可解析（三处抽验实证，逐处必复验）：
  (a) collection_item_broker.rs:1073「对应 C# BLMOVE 弹推内核段…见本文件 661 行」——解作 rust 本文件则 :661 系
      is_contended 让核重投注释（与 BLMOVE 无涉）；解作 C# 则 CollectionItemBroker.cs BLMOVE 内核在 :418，
      :655-663 系 TryGetNext*Result 分发调用位。两读皆落空。
  (b) keys.rs:44「C# NetworkEXPIRE 族；见本文件 157 行」——rust keys.rs:157 为 RENAMENX 函数体；
      C# NetworkEXPIRE 实测在 libs/server/Resp/KeyAdminCommands.cs:364。两读皆落空。
  (c) list_commands/write.rs:54「C# ListPush 共用体（LPUSHX/RPUSHX）；见本文件 20 行」——rust :20 系 use 导入段；
      C# ListObject.cs:20 系 ListOperation 枚举注释。两读皆落空。
- 正规口径来源：checkjs-gate-r8 波已定「符号锚须作 全路径::符号名 形，门禁 symbolCheck.js 的断言以 `::` 为解析前提」
  （本仓现行门禁形制；不得回改成「载体注记 / 本文件 N 行」形，否则断言被静默旁路）。
- 禁触域（同侪在途）：wedb/wedb/src/server/replication/**、wedb/wnode/src/resp/mod.rs、
  wedb/wnode/src/resp/resp_server_session/mod.rs、wedb/wnode/src/storage/session/common/ttl_sync.rs、
  wedb/wnode/tests/resp_admin.rs、wedb/wnode/tests/ttl_sync_codec.rs；
  其中 set.rs/keys.rs 等 resp 面须逐文件先复测 git status 是否他席在途，在途即留该处并明文登记。

审核结论：通过（2026-10-01 主控亲验立案；P3 登记治理级。零行为改动，纯注释订正；
判据本体均在注释同段自陈存活，真正灭失的只是「一手来源指针」，故不上抬）

「精确锚点见本文件 N 行」相对行号锚族 10 处双侧皆不可解析，跨面追锚系统性落空

问题分析：
1. Garnet 契约对齐：本仓审查纪律要求 rust 注释与 C# 的映射可被后续审计单跳解析
   （doc/zh/deviations.md 册规「判据 + 符号锚 + 来源」三段式，且 禁钉行号；js/check/symbolCheck.js 词法断言以
   「路径::符号」为唯一可机检形）。「本文件 N 行」为相对行号，既不属 C# 路径族也不属 rust 符号族，天然在门禁盲区内。
2. 工程现状：该族由早期「整文件对标」写法遗留（一处锚原指当时同文件行位），后经函数拆分/上下搬移后行位漂移，
   注释未随迁——三处抽验（注记 a/b/c）在 rust 本文件与 C# 对标文件两种解读下均落空，无一可解析；
   余下 7 处同形制，须逐处按同一法复验。
3. 逻辑危害确证：审计/回收席按注追锚必落空并转向误猜对标段（§94 判据锚漂与 e7efacb 悬空票锚同谱，
   本案是该形态的批量存量面）；门禁不拦 ⇒ 假锚持续积累。零行为改动，定 P3。

涉及代码：
rust 文件与函数：上述 10 处注释行（逐处一跳到函数，无行为主体）
wedb/wcol/src/itembroker/collection_item_broker.rs:1073 try_move_next_list_item 段头
wedb/wnode/src/resp/key_admin_commands/keys.rs:44/:88/:98、basic_commands/set.rs:391、
  objects/list_commands/write.rs:54、objects/hash_commands/write.rs:29/:43、objects/set_commands/read.rs:105、
  wkv/tests/compact/more_log_compaction.rs:331

对应 c# 文件与函数：
libs/server/Objects/ItemBroker/CollectionItemBroker.cs:418（BLMOVE 内核段，(a) 的真锚）
libs/server/Resp/KeyAdminCommands.cs:364 NetworkEXPIRE（(b) 的真锚）
libs/server/Objects/List/ListObject.cs / ListObjectImpl.cs 的 ListPush 族（(c) 的真锚，逐处现算）

精炼执行方案：
1. 逐处弃相对行号，改「C# 路径::符号名」形（如 `CollectionItemBroker.cs::TryGetNextListResult(BLMOVE)`）；
   若该处 C# 确无对位（rust 自有枚举分派族），改为明文「C# 无对位，判据落仓内自陈契约 + 仓内锚」，
   严禁留悬空数字行号、严禁以改注释绕过而不下判据
2. 十处逐处双侧复验（照注记 a/b/c 三读法），订正后全仓 grep "精确锚点见本文件" 必零命中
3. 纯注释面零行为改动；只动上述 7 文件的注释行；禁触域内文件若他席在途则该处留册并登记，不抢改

---

## 终态注记
- **合入哈希**：`ab04502`（cherry-pick 自 `c2781d7`）
- **收口形态**：
  1. 订正全仓 7 个文件中的 10 处「精确锚点见本文件 N 行」注释，改用正规「C# 路径::符号名」形制或明文标注仓内自陈契约。
  2. 全仓 grep `精确锚点见本文件 wedb/` 结果降为 0。
- **门禁验证**：`bun js/check.js` 退出码 0，有效符号锚点 5485 处，`cargo check --workspace --all-targets` 正常通过。

## 主控全量复核（2026-10-01）

- **形制到位**：`ab04502` 的 10 处改动全为注释体内 `libs/.../<文件>.cs:<行号>` → `libs/.../<文件>.cs::符号名`，
  零代码语义改动（7 文件、每处 ±1 行），符合票面「登记级、零行为改动」口径。
- **符号锚在场性逐条亲验**（对 `./garnet` 现树 grep，非凭旧票面）：
  `CollectionItemBroker.cs::TryGetNextListResult`（:394 定义、:654 递归调用）✓；
  `MoreLogCompactionTests.cs::DeleteCompactLookup`（test.hlog 路径逐字一致，:52）✓；
  `BasicCommands.cs::NetworkSETEX`（:533，签名携 `highPrecision` 形参）✓；
  `KeyAdminCommands.cs::NetworkEXPIRETIME`（:532）✓；`HashCommands.cs::HashSet`、
  `ListCommands.cs::ListPush`、`SetCommands.cs::SetIsMember` 均有实体 ✓。
- **一处口径疑点（不另开票，留此备案）**：`basic_commands/set.rs` 的新注把该点标为
  「NetworkSETEX（毫秒精度形态 highPrecision=true）」，而 C# `RespServerSession.cs:822/825` 是
  `SETEX → NetworkSETEX(false)`、`PSETEX → NetworkSETEX(true)`，`highPrecision=true` 一形只属 PSETEX。
  若该 rust 站点实为 SETEX 系秒精度臂，括注应改指 `NetworkSETEX(false)` 或 PSETEX；符号锚本身正确，
  属注释副语精度问题，后席触到同文件时顺手订正即可，不单列一票。
