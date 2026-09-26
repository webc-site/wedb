归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 4f857c9（P3），收口形态：allow.check.sh rg 三分支 fail-closed（rc=0 红/rc=1 过/rc≠1 故障 exit 1），三态实测，§146 匹配面不动。续排注：本票沙箱席与方案详情见下文。

甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P3
核验记录（现码复跑，非票面背书）：
1 allow.check.sh 现树亲验：:14 `if rg --line-number "(\[allow|#!?\[expect)" -t rust; then` 与 :20 Cargo.toml 侧 if rg 原文在位，全脚本无 rc=$? 显式取码分支；bash if 条件形态下 rg exit 2（自身错误）与 exit 1（无匹配）同落通过臂，:25-29 FAILED 汇总与「allow 检测通过」文案对 exit 2 无差别放行——检测器故障冒充绿灯现状成立。
2 查重：deviations.md §146 案四仅裁匹配面（扩 #!?\[expect），案一/二/三系 cargo_sh 仓 udeps/clippy_extra 退出码修复，不覆盖本仓 allow.check.sh 错误码分支；task/{done,ing,issue,reject} 无同轴票（wnode-tests-common-allow-deadcode 系 allow 归零整改，不同面）。
3 架构合规与可执行度：显式三分支取码为最小改动、不新建机制、不动 §146 已裁匹配面；验证点闭环（临时不可读 .rs 复现 exit 2 断言红、正常树断言绿、植入 allow 断言红不变）。
4 格式：纯文本；门禁工具票契约对象为本仓脚本，无 C# 原型对应面（对照锚 §146/unused.sh 在位），双侧路径说明齐全。

审核结论：通过（zcode-r22-review-shjs，2026-09-26）
亲验坐实：allow.check.sh:14/:20 两处 if rg 原文逐字核对；bash if 条件形态下仅 exit 0 入红臂，exit 1 与 exit 2 同落通过臂，set -euo pipefail 对 if 条件命令不触发（bash 语义）——错误码同化成立；rg 退出码 0/1/2 契约核实。查重：§146 案四仅裁匹配面（扩 #!?\[expect），案一/二/三为 cargo_sh 仓 udeps/clippy_extra 退出码修复（bd87d92/1a6edb7），本票系 wedb 仓 allow.check.sh 错误码分支缺口，同族新面非重复。
执行方案（整理版，供 fix.md 直接消费）：
1 两处 rg 各改显式取码：rg ...; rc=$? —— rc=0 走红臂（命中即禁，ERROR 文案与 FAILED 汇总不变），rc=1 继续（无匹配），rc=2 打「rg 扫描故障（exit 2），检测未完成不得视为通过」stderr 并 exit 1
2 不动 §146 已裁决的匹配面（\[allow 与 #!?\[expect 双形态、Cargo.toml lints 段）
3 验证点：仓内临时建不可读 .rs（chmod 000，注意先确认未被 gitignore 遮蔽）复现 rg exit 2，断言退出 1 且报扫描故障；正常树跑断言退出 0 与「allow 检测通过」文案不变；植入 #[allow(dead_code)] 断言命中红不变

allow.check.sh 两处 if rg 条件形态把 rg 自身错误退出（exit 2）同化为「无匹配」放行，门禁错误面被吞成绿灯

问题分析：
1 门禁契约对齐。ripgrep 退出码契约为 0 命中 / 1 无匹配 / 2 自身错误（I/O 错误、不可读文件等）。allow.check.sh 的判定语义应为：0 命中即红（禁 allow/expect 压制）、1 无匹配即通过、2 rg 故障必须显式红不得冒充判定结果。同仓门禁纪律已由 zcode-r139c-gateaudit 案一/二/三修复三处恒真退出码（sh 仓 bd87d92 udeps 双极性失真、1a6edb7 clippy_extra 空清单恒过，deviations §146 在册），本两处 if rg 形态是该族残留面。
2 工程现状确证。allow.check.sh:14 `if rg --line-number "(\[allow|#!?\[expect)" -t rust; then` 与 :20 `if rg --line-number -g 'Cargo.toml' '...'`：bash if 条件下仅 exit 0（命中）走红臂，exit 1（无匹配）与 exit 2（rg 错误）同落通过臂；:15/:21 的 ERROR 文案只覆盖命中形态，:25-27 的 FAILED 判定与 :29 「allow 检测通过」对 exit 2 无差别放行。set -euo pipefail 对 if 条件命令不触发（bash 语义），两道防线均不兜。
3 逻辑危害确证。rg 因权限/磁盘 I/O 等故障中断扫描时（已扫部分可能恰含违规项），脚本仍打印「allow 检测通过」退出 0——检测器故障冒充绿灯，正是 gateaudit 案族「检测器故障不冒充绿灯」原则（unused.sh:35-38 同款场景已按此原则点名计红）的反例残留。属条件性误绿，触发概率低于恒真形态但同族同害。

涉及代码：
脚本文件与函数：allow.check.sh:14（源码侧 if rg）、:20（Cargo.toml 侧 if rg）、:25-29（FAILED 汇总与通过文案，无 exit 2 分支）

对照物锚：deviations.md §146（gateaudit 案一/二/三恒真退出码修复族与案四 allow.check 扩 #[expect] 裁决，本票为其覆盖面收尾）；sh 仓 git log bd87d92/1a6edb7（同族已修先例）；unused.sh:36-49（检测器非零退出点名计红的正确形态）

精炼执行方案：
1 两处 rg 各改为显式取码：rg ...; rc=$? —— rc=0 走红臂（命中即禁），rc=1 继续（无匹配），rc=2 打印「rg 扫描故障（exit 2），检测未完成不得视为通过」并 exit 1
2 保持现有命中文案与 FAILED 汇总逻辑不变，仅补错误码分支，不动 §146 已裁决的匹配面（\[allow 与 #!?\[expect 双形态）
3 测试验证点：构造临时不可读 .rs 文件（chmod 000）复现 rg exit 2，断言脚本退出 1 且报扫描故障；正常树跑断言 exit 0 文案不变；植入 #[allow(dead_code)] 断言命中红不变
