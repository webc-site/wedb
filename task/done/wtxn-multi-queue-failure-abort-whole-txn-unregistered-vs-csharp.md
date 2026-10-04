锁定注记（2026-10-01 r8 波主控，基线 7397ed1；wtxn 只读甄别席候选 + 主控双侧现码亲验复跑，行号以本注记为准；本票主控亲办 登记级）：
- 现码亲验：C# 全仓 `.Abort(` 调用点仅 libs/server/Transaction/TxnRespCommands.cs:26/:114/:151/:176/:187
  五处（SKIP/MULTI 嵌套/EXEC 内臂），ProcessMessages 的 ACL/NOSCRIPT/未知命令臂
  （RespServerSession.cs:690-716）只写错误行 + commandStats.IncrementRejected，无任何中止动作；
  TxnState.Aborted 的置位除 Abort()（TransactionManager.cs:387）外仅 TxnKeyManager.cs:53 一处（键面裁决，非入队失败面）。
- rust 现位：core.rs:1109 queue_failure 起始、:1199/:1208/:1216 三臂置真、:1230-1233 单点 abort_pending_transaction；
  txn_resp_commands.rs:50 侧 Aborted→EXECABORT 消费（C# TxnRespCommands.cs:50 同构，对位无漂）。
- 注释失真现位：resp_server_session/txn.rs:108-110 自称「对标 C# …ProcessMessages 排队失败置 Aborted 诸臂」——
  C# 无此臂，误引（本票要订正的正主）。
- 前案边界（查重已核）：deviations 仅 §58（HELLO 排队中止一臂）与 §121 沾边，本「三臂入队失败即中止」面无册无票；
  task 池 grep abort_pending_transaction / EXECABORT 零命中。
- 零行为改动面钉死：只动 doc/zh/deviations.md 与 txn.rs 注释行；禁改 core.rs 中止臂本体、禁改任何应答帧序。

审核结论：通过（2026-10-01 主控亲验立案；P3 登记裁决级。对外可观察行为分叉确凿但不属缺陷：
rust 的 All-or-Nothing 加固符合 redis 社区语义直觉且更保守，裁决取「登记改良」而非「回退对齐 C#」）

MULTI 排队期 ACL/NOSCRIPT/未知命令三臂统一中止事务，强于 C# 真值源且 deviations 全册无登记

问题分析：
1. Garnet 契约对齐：C# 排队期（TxnState.Started）命令入队阶段的权限拒绝/脚本缺失/未知命令，
   只回一行错误并计入 rejected 统计（RespServerSession.cs:690-716），事务态不动，
   其后 EXEC 照常把队列内已入队的合法命令全部执行并提交；全仓不存在入队失败置 Aborted 的臂。
2. 工程现状：rust 在 core.rs 三处（ACL 拒绝 :1199、脚本/权限面 :1208、未知命令面 :1216）置 queue_failure，
   :1230 单点 abort_pending_transaction 把事务与会话镜像同置 Aborted，随后 EXEC 必回 EXECABORT（txn_resp_commands.rs:50 消费），
   整笔丢弃。即 rust 的入队失败语义是 All-or-Nothing，强于 C# 与 redis 社区版。
3. 逻辑危害确证：同一命令序列 `MULTI; <越权命令>; SET k v; EXEC` 两侧行为可观察分叉——
   C# 回错误行后 EXEC 提交 SET，rust 回 EXECABORT 且 SET 不生效。本仓裁定真值源是票体/册：
   未登记的对外分叉改良会在后续审查中被误判为缺陷并「反向修复」，且客户端事务语义预期依赖此面。
   经裁量：加固方向合理（避免半笔提交），保留现形、落册登记。定 P3 登记级，零行为改动。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/resp_server_session/core.rs:1199/:1208/:1216 三臂置 queue_failure、:1230 abort_pending_transaction 单点
wedb/wnode/src/resp/resp_server_session/txn.rs:108 误引 C# 的注释（订正正主）

对应 c# 文件与函数：
libs/server/Resp/RespServerSession.cs:690 ProcessMessages 排队期权限/脚本拒绝臂（只写错误行，无中止）
libs/server/Transaction/TxnRespCommands.cs:26/:114/:151/:176/:187 全仓唯一 Abort 调用点（皆非入队失败面）

精炼执行方案：
1. doc/zh/deviations.md 取现册实测最大在册号之后一新号落一条：登记「MULTI 排队期入队失败（未知命令/未知子命令/
   ACL 拒绝/NOSCRIPT）统一中止整笔事务」为 wedb 有意改良，注明与 C# ProcessMessages 及社区版的分叉点与取舍理由
   （禁钉行号，判据 + 符号锚 + 来源三段式照册规）
2. txn.rs:108-110 误引注释改为「wedb 自有加固面，C# ProcessMessages 无对位中止臂，依据见 deviations 新条」
3. 零行为改动；补一条契约钉死测（wnode/tests 事务册）：MULTI + 未知命令 + SET + EXEC 断言 EXECABORT 且 SET 未生效，
   注释指向新登记条——防后续误「对齐 C#」反向回退

---

## 终态注记
- **合入哈希**：`75dee3a`（cherry-pick 自 `1bd30a9`）
- **收口形态**：
  1. 在 `doc/zh/deviations.md` 登记 [§193]「MULTI 排队期入队失败统一中止整笔事务（不复刻 C# 吞错继续提交）」，明确写清判据、符号锚及一手来源指针。
  2. 订正 `wedb/wnode/src/resp/resp_server_session/txn.rs:108` 误引 C# 注释，明确为 wedb 自有加固面，依据见 deviations [§193]。
  3. 在 `wedb/wnode/tests/txn_queue_abort_execabort.rs` 补充锁测契约测试 `multi_unknown_command_aborts_whole_txn_contract_sec193`，钉死该加固语义。
- **门禁验证**：`cargo check -p wnode --all-targets` 通过，`txn_queue_abort_execabort` 及事务套件全部单测通过。

主控收票审计（2026-10-01 r9 波，沙箱 dev 尖亲跑）：
- 票面「零行为改动」硬约束守住：合入笔源文件面仅 resp_server_session/txn.rs 注释体改写（2 增 3 删，
  全在 doc 注释内），无一行语义变动；其余落 deviations.md 新节与既有锁测册扩臂。
- 误引纠正到位：原注释自称「对标 C# ProcessMessages 排队失败置 Aborted 诸臂的 rust 收口」，
  而本票已亲验 C# ProcessMessages 排队臂仅写错误行 + 计 rejected、`.Abort(` 全仓仅存在于
  Transaction/TxnRespCommands.cs 族——现改为「wedb 自有加固面，C# 无对位中止臂，依据见 §193」，
  注释与本体纪律重新同步，判据真值源从虚构的 C# 对位改指在册裁决。
- §193 册文合规：判据段完整复述 C# 三臂（ACL / NOSCRIPT / 未知命令）吞错继续提交的实形与本仓
  All-or-Nothing 改良定性；符号锚走 abort_pending_transaction / queue_failure / TxnState::Aborted /
  process_messages 单点形（未钉行号，合台账规范）；来源双指针（本票 + 注释锚）。
- 号位让号复核：落笔前现册最大 §192，本笔取 §193 无撞号；本波后续需落册票自 §194 起。
- 门禁：本席沙箱复跑 txn_queue_abort_execabort 全册 6/6 绿（含新增
  multi_unknown_command_aborts_whole_txn_contract_sec193），构建零告警。
