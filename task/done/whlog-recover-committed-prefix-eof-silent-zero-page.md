甄别结论：通过（甄别席 J7，2026-09-27，定级 P2——committed 前缀 EOF 静默清零页，中段漏验坐实）。hlog/mod.rs:592-599 UnexpectedEof 静默 clear_page 无 flushed_until 判、:578 仅未承诺页清零，亲验；wdev flush_range_aligned :116-153 整段写+短写校验、wcpr manager/recover.rs:120-136 仅验 flushed-1 所在末段、中段漏验，亲验坐实；§27 在册系同向条目，本票系其落实缺口非并案；EOF 容忍限定未承诺区复用 Error::Device 合规。派沙箱席 c01o。

审核通过（2026-09-27）：缺口亲验成立（hlog/mod.rs:592-599 逐页臂 UnexpectedEof 静默 clear_page 无 flushed_until 判，:578 仅跳过整页未承诺区；wdev/device.rs:116-153 flush_range_aligned 扇区圆整整段写+短写校验+水位 Ok 后推进、read_range 短读恒折 UnexpectedEof、page_size 恒扇区倍数，前缀内页 EOF 只能是截短/损坏；wcpr/recover.rs:120-136 只验 flushed-1 末段物理覆盖中段漏验；§27 deviations.md:320 声称前缀中段截断由页装载报错承接与现码静默清零不符）；方案判据边界正确（尾页跨承诺线 page_start < flushed_until < page_start+page_size 文件合法止于 flushed_until 整页读必 EOF 须容忍、等号页页末==flushed_until 有末段校验保障 EOF 即拒启正确、出口复用 Error::Device 无新错误形态）；格式纯粹度合格。移入 todo
恢复期页装载对已落盘承诺前缀内的短读静默清零，崩溃一致性快停链对段内截短失效

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# Recovery.cs:1456 AsyncReadPagesForRecoveryCallback 只判 errorCode 不验 numBytes——C# 对任意短读静默放行；rust 已按 §27 裁决方向收紧（设备错误与非 EOF 短读快停），本项是该收紧运动的未竟部分，与已登记方向的落差即缺陷。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
whlog/src/hlog/mod.rs:594-599 recover 逐页路径 Err(WdevError::UnexpectedEof) → buffer.clear_page(p) 按「合法空页」处理，不区分页是否落在 snapshot.flushed_until 已承诺前缀之内。刷盘内核恒写整页（flush_range_aligned 页对齐、页为扇区整数倍），健康文件对 flushed 前缀覆盖恒页粒度——前缀内页（page_start < flushed_until）的 EOF 只能是数据文件截短/损坏。§27 声称的承接链「前缀中段截断由后续页装载报错承接」仅对整段缺失成立（SegmentNotFound → Error::Device 快停，:600-606）；段内截短从两道闸之间漏过（wcpr 侧 recover.rs:135-161 只验 flushed-1 所在末段）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
段文件短于应有覆盖时，已承诺页被静默清零，恢复扫描遇零头静默截断记录链——历史数据无声丢失且被后续写入固化，正是 §27 要杜绝的「C# 静默截短起库」残留窗口。

涉及代码：
rust 文件与函数：
wedb/whlog/src/hlog/mod.rs:recover 逐页装载臂（:585-606）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:AsyncReadPagesForRecoveryCallback（rust 收紧方向的目标形态）

精炼执行方案：
1 EOF 容忍限定于未承诺区：page_start + page_size > snapshot.flushed_until 的页方可按空页清零（尾页/未承诺区）；完全承诺页（page_start + page_size <= flushed_until）遇 EOF 上抛 Error::Device 拒启
2 段级批量预热短缓冲路径（:585-590）同口径收紧
3 测试验证点：构造 flushed 前缀内段内截短文件，恢复必须报错拒启；正常尾页未满与空设备回归不变
