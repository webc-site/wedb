DEBUG 命令保护门配置供给缺位（甄别拒绝：与在册票重复）

来源：next/qcode10.net.md 条 4（[MED]，原文件已整体拆除）。

拒绝理由：同轮同题单票已拆出在册 —— next/qcode10-enable-debug-command-knob.md
（其自述「来源：qcode 第 10 轮 net 条 4（next/qcode10.net.md:46-48）」），
覆盖本条目全部内容：门链在位但 From<&NodeArgs> 未投影、wconf 三面无
enable-debug-command 旋钮、恒 No 与 local 档语义分叉、拒答文案指向不存在选项，
并附三档线面测试修法。

浅核（当前工作树）：wconf/src/ 全目录 grep enable_debug 零命中（旋钮仍缺），
resp_server_session.rs:137/:169/:503 门链仍在 —— 问题仍待修，由在册票承接，
不重复立项。C# 锚点（Options.cs:594-595 [Option("enable-debug-command")]）存在。
