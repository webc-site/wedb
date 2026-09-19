优先级：中（线面口径偏离，功能缺口/正确性）
分拣注记（源 next/qw.net.md 第 11 轮 net 条 1，原文件已于 d0d2ed24 拆为单议题档；浅核 2026-09-19 HEAD 39ea7e58：SlotParseError slot_mgmt.rs:185、write_to :196-206、try_parse_slots :214-250、RESP_ERR_INVALID_SLOT_RANGE cluster_cmd_strings.rs:17 全部在场，三处偏离逐条命中；台账查重 task/{ing,done,reject} 与 git log 无同题票，仅 task/reject/qcode-rounds2-round11-covered.md:8-10 记其为 qcode.rounds 同题归宿）

CLUSTER 槽位参数解析的错误文案与判定序三处偏离 C#，并含一枚 C# 全仓不存在的自造错误串

现状事实（主仓 dev，取证 HEAD 39ea7e58）

- wedb/wedb/src/server/cluster_session/slot_mgmt.rs:185-194 定义 SlotParseError 四态，
  :196-206 `SlotParseError::write_to` 逐态写错误帧：:201 NotInteger 臂写
  RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER，:203 InvalidRange 臂写 RESP_ERR_INVALID_SLOT_RANGE，
  :204 Duplicate 臂已经走动态文案单点 write_slot_duplicate_error。
- 判定序：:214-250 try_parse_slots 的 range 臂是「非整数（:222/:225）→ 越界（:227）→
  倒挂（:230）→ 重复（:233）」，非 range 臂无倒挂态。
- 自造串：wedb/wresp/src/cluster_cmd_strings.rs:17
  `RESP_ERR_INVALID_SLOT_RANGE = "ERR Invalid slot range specified"`，该行上方注释为
  「非法槽位区间错误文案」、无 C# 锚点（同文件其余常量一律挂 libs/cluster/CmdStrings.cs:*），
  在 garnet/ 全域 grep "Invalid slot range" 零命中。
- 同域真锚点在位且零成本可接回：wedb/wresp/src/cluster_cmd_strings.rs:13
  RESP_ERR_INVALID_SLOT（对位 garnet/libs/cluster/CmdStrings.cs:54），已在
  slot_mgmt.rs:375/:509/:552/:617/:647 五处使用。
- 受影响的命令面即 try_parse_slots 的四个调用点：slot_mgmt.rs:315
  network_cluster_add_slots（ADDSLOTS / ADDSLOTSRANGE）、:349 network_cluster_del_slots、
  :465 network_cluster_set_slots_range、:608 network_cluster_del_keys_in_slot。
- 现有用例把错误口径钉死（落地须同批改）：
  wedb/wedb/tests/cluster_resp_session.rs:1213 断 `-ERR Invalid slot range specified\r\n`，
  :1038-1044 与 :1176-1206 断「越界先于倒挂」，:764 断非整数走 value-is-not-integer 文案。
- 现网可观测评判：`CLUSTER ADDSLOTSRANGE 20000 10000` C# 回 Invalid range 动态文案，
  rust 回 "ERR Slot out of range"；`CLUSTER ADDSLOTS abc` C# 回 "ERR Invalid or out of
  range slot"，rust 回 "ERR value is not an integer or out of range"。

目标形态

三处全部改回 C# 口径，不留中间态。

1. NotInteger 臂（slot_mgmt.rs:201）改写 RESP_ERR_INVALID_SLOT，两分支（range 与非 range）
   同口径，与 C# 的 `!TryGetInt → RESP_ERR_INVALID_SLOT` 一致。
2. InvalidRange 变体带 start/end 两参（:190-191 unit variant 改 tuple），write_to 改调新增
   动态写出器 write_slot_range_error(output, start, end)，与 C# `$"ERR Invalid range
   {slotStart} > {slotEnd}!"` 同形；该写出器落在
   wedb/wresp/src/cluster_cmd_strings.rs，紧邻同域 write_slot_duplicate_error（:65），
   共用其 itoa 收口范式（消费方不买 itoa）。
3. 删 wedb/wresp/src/cluster_cmd_strings.rs:17 的 RESP_ERR_INVALID_SLOT_RANGE 与其 import
   （slot_mgmt.rs:18）。
4. range 臂判定序调回「倒挂先于越界」（slot_mgmt.rs:227 与 :230 两段互换），非 range 臂
   不变（其倒挂态天然不可达）。

C# 对位

- garnet/libs/cluster/Session/ClusterCommands.cs:TryParseSlots（:68-81 非整数 →
  RESP_ERR_INVALID_SLOT，:86-90 倒挂 → 动态 $"ERR Invalid range {slotStart} > {slotEnd}!"，
  :92-96 越界 → RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE，:98-105 重复 → 动态文案；
  非 range 臂 :77-83 以 slotEnd = slotStart 收敛为同一段判定）。
- garnet/libs/cluster/CmdStrings.cs:54 RESP_ERR_INVALID_SLOT。
- 消费命令：garnet/libs/cluster/Session/RespClusterSlotManagementCommands.cs:32（ADDSLOTS）、
  :74（ADDSLOTSRANGE）、:200（DELSLOTS）、:242（DELSLOTSRANGE）、:316（SETSLOTSRANGE）。
- 客户端断言先例：garnet/test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:808
  断 "ERR Invalid or out of range slot"。

门禁与验收判据

- cargo check --workspace --all-targets 零错误零警告。
- 改后断言逐条对齐：ADDSLOTS 非整数参数、ADDSLOTSRANGE 奇数元、倒挂区间、越界区间、
  重复槽位五类线面各一断言，动态文案须含实参（`-ERR Invalid range 20000 > 10000!\r\n`
  整帧等值断言，不放宽为前缀匹配）。
- 既有 tests/cluster_resp_session.rs 的越界优先断言按新序改写，禁新增「两口径都接受」的
  宽松断言。
- ./sh/clippy.sh 零警告、./js/check.js 无新增缺失（本轮无需 ignore 登记）。

坑与边界

- SlotParseError 是 `#[derive(Copy)]` 的 pub(super) 枚举，加参数后 write_to 的
  `Self::InvalidRange(s, e)` 取值与 :315/:349/:465/:608 四个调用点须逐个复核，
  不要在调用侧 format! 造临时 String（协议帧写出单点在本仓一律直写 output）。
- 越界检查用 ClusterConfig::out_of_range（i64 入参），C# 用 int + OutOfRange；本次只调序，
  不改该谓词，也不改库级定槽的槽位语义。
- DELSLOTS 族与 SETSLOTSRANGE 共用同一解析器，改序对三族同时生效，验收至少覆盖两族。
- 不做向下兼容：旧错误串与旧判定序直接删，不写「兼容旧文案」分支。
- 与在途票无交叠：task/ing/cluster-provider-version-map-dead-slot.md（provider 死槽）、
  task/ing/qcode10-parse-db-index-i32-parity.md（db index 解析）均不触碰槽位文案。

分拣补记（next/agy.net.md 条 3 同题，源档已分拣清空删除；浅核 2026-09-19 主仓 dev）：本票判定序与
文案三偏离之外还有第四处同函数偏离——try_parse_slots 的 range 臂用 args.as_chunks::<2>().0 迭代
（slot_mgmt.rs:222），奇数尾参落在 as_chunks.1 被静默丢弃不报错；C# 同位（ClusterCommands.cs:57-81）
while 循环内 currTokenIdx++ 两次取数，奇数尾时第二个 TryGetInt 越界返回 false → RESP_ERR_INVALID_SLOT
报错。落地时在本票目标形态内一并补 range 臂偶数校验（奇数即 Err(SlotParseError::NotInteger)），正好
满足本票验收中「ADDSLOTSRANGE 奇数元」断言的前置。
