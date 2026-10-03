收口记录（2026-10-01 r8 波，主控亲办 登记级，零行为改动纯注释订正）：
- 改动三面：primary_tasks.rs 模块头单轨判定行改挂在册载体
  js/check/ignore/garnet/libs/server/TaskManager/TaskManager.yml 理由行，并原地按 §94 先例补
  「原落册票 task-manager-single-lifecycle-track.md 不在五池存活、判据不可回收，论证正文随本头自注存活」注记；
  TaskPlacementCategory.yml / TaskType.yml 各两处（册头注释行 + 理由行）同口径订正，判据正文一字未重写。
- test.yml:1879 复核确认无需改动：该行「依据见 garnet/libs/server/TaskManager/TaskManager.yml」已挂实存载体。
- 复验：全仓 grep task-manager-single-lifecycle-track 票外四命中（primary_tasks.rs 一处 + 两 yml 册头各一处 +
  test.yml 一处）+ 本票存档，每一命中均可解析至实存载体或显式不可回收注记，无残留悬空 task/done 路径；
  bun js/check.js EXIT=0，输出 47 行、B 层词法提示仍 3 处（与本票前形一致，零回归）。

审核结论：通过（2026-09-30 甲轮48 审核席；P3 登记级。载体改挂 TaskManager.yml 实存路径、扩 TaskPlacementCategory/TaskType 两处同锚、先例轮号订正甲轮45-A、git 全历史核验票体从未入库不可回收）

primary_tasks.rs 模块头悬空票锚指向不存在的 task/done 票体，单轨制判据面临不可回收化

问题分析：
1. Garnet 契约对齐：本仓裁定真值源是票体（doc/zh/deviations.md 册头声明「旁证通道即真值源：新裁决落票不落册」）。被引票承载「C# TaskManager 注册表托管面（RegisterAndRun/CancelAsync/Dispose）判定不移植」的关键裁决论证，对标本体为 libs/server/TaskManager/TaskManager.cs。
2. 工程现状：primary_tasks.rs:18 模块头引「生命周期单轨判定（双轨归一，见 task/done/task-manager-single-lifecycle-track.md）」，该文件在 task/done、task/todo、task/reject、task/issue 四池全数不存在（ls/find 零命中）；git 全历史文件清单无 *task-manager* 文件、pickaxe 仅命中 init 提交——票体从未入库，不存在历史可回收形。同锚另有两处：js/check/ignore/garnet/libs/server/TaskManager/TaskPlacementCategory.yml:9 与 TaskType.yml:9 同指该幽灵票。
3. 逻辑危害确证：判据论证正文在 primary_tasks.rs 模块头自注存活，js/check/ignore/garnet/libs/server/TaskManager/TaskManager.yml 理由行亦承载判据本体，真正灭失的仅是票体这一「一手来源指针」；但码面引用幽灵票仍使后续审查/回收席按锚追票落空（§94 形态、甲轮45-A doc 锚漂先例同谱）。零行为改动，登记级。P3。

涉及代码：
rust 文件与函数：
wedb/wnode/src/primary_tasks.rs:18 模块头 doc（悬空锚行）

对应 c# 文件与函数：
libs/server/TaskManager/TaskManager.cs（RegisterAndRun/CancelAsync/Dispose，断锚论证所对标本体）

精炼执行方案：
1. 主修 primary_tasks.rs:18 悬空锚：改挂实存载体（js/check/ignore/garnet/libs/server/TaskManager/TaskManager.yml 理由行承载判据本体）或按 §94 先例原地补一行「原票体不在五池存活、判据不可回收」注记；严禁借机重写判据正文
2. 连带订正 TaskPlacementCategory.yml:9 与 TaskType.yml:9 两处同锚
3. 零行为改动，纯注释订正；验证：全仓 grep task-manager-single-lifecycle-track 每一命中均可解析至实存载体或显式不可回收注记
