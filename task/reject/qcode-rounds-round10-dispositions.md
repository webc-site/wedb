第 10 轮清账（台账 :135-217）：1 条新立案、其余已修复/在册/归并在途

审计时间：2026-09-19（fixloop 台账审计）。取证基点：主仓 dev 工作树 grep 实测 + 现存票据。

新立案（1 条）

- 残留待办（LOW，台账 :144-145 明记「未立案」）：aof_sync_task.rs:560/:584 仍为
  starts_with 前缀断言，应改整帧相等 → 实测仍成立（现为
  `starts_with(b"*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n")` 前缀形态），
  已立 next/aof-advance-pulse-frame-exact-assert.md。

已修复（3 条）

- net 域 HIGH（wconn/src/session.rs:257 带内脉冲帧 $11 ADVANCETIME vs 注册 $12
  ADVANCE_TIME）：已修——并发会话落 dev（wconn 前缀 $12、
  cluster_resp_session.rs:1598/1736 喂帧闭环回归），台账 :141-143 记录在案；
  同题材分支 advance-time-frame-name(4c728268) 判重复工已弃，维持弃置。
- net 条 2（SET 过期选项三份书写：is_expiry_option 五连比较绕过 wresp 单点，台账 :212
  曾让路给 task/ing/set-exist-options-single-source.md）：已修——该让路对象后被甄别拒绝
  （task/reject/set-exist-options-single-source.md：ExistOptions 方案 a 已是现状），
  而本条自身亦已落地：wnode/src/resp/basic_commands/set.rs:672 现为
  `if let Some(parsed_option) = try_get_expiration_option(next_opt)` 单点环
  （注释对标 BasicCommands.cs:628 TryGetExpirationOptionWithToken），五连比较消亡；
  同条的死别名 expiration_option_from_token 归 task/ing/zero-consumer-surfaces-batch-two.md
  第六节。原报告 next/qcode10.net.md 仍在册，其条 2 现状已过时，报告文件处置归下游。
- qcode10.checkjs-tiered-hashset-anchor（tiered_collection_ops.rs:91 锚点 HashObjectImpl.cs:Set
  失真，分支 qcode-hashset-anchor/c666e39b 因他人暂存改动停手等窗口）：已修复——
  台账 :206 记录 A 层合规门随 dev 2416beba 转绿（HashSet 锚点纠正）；票
  next/qcode10.checkjs-tiered-hashset-anchor.md 仍在 next/，核销归下游；
  分支残余不再需要合入。

在册（3 条）

- 第 10 轮新工单三条（台账 :209-211）：next/qcode10-parse-db-index-i32-parity.md、
  next/qcode10-client-outstanding-admission-gate.md、next/qcode10-enable-debug-command-knob.md
  均在 next/ 现存，已派单在途。

归并在途（不代管，详见 task/reject/qcode-rounds-foreign-inflight.md）

- 基线红警 19 条（12 条嫁接 + wkv 7 条并发在途）→ qcode-baseline-re 代理等回报。
- ScanIter 深递归（台账 :185-194 主代理裁决不立案留证据）：维持不立案；
  「若复现出真栈溢出再单独立案」的条件项挂靠 qcode-baseline-re 的
  scan_tree_in_batches（meta.size 为界）改造，条件未触发，无新证据不立。
- 第 10 轮 data/my 补跑（台账 :135「补跑中」）→ 审查循环在途。
- 教训入档（check.js 回写 js/check/ignore/*.yml，跑门后禁 git commit -am）：
  规程条目，非行动项，随本文件留痕即可。

结论：第 10 轮行动项 1 新票 + 3 已修 + 3 在册 + 4 归并在途，无悬置残留。
