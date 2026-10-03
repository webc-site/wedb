甄别结论：通过（甄别席 J6，2026-09-27，定级 P2——冷区游标删除失败仍推进，失败区间无驱动再扫，过期键滞留）。亲验：cold_commit 删除前算好（ttl_sweep.rs:169-190）、删除循环 Err 仅 warn（:204-206，文案自承「留待下一扫描周期重试」）、:215-219 无条件 store 与 :215「删除完成才提交」注释自相矛盾；gc/mod.rs:11 承诺句精确在场；游标 max(begin) 钳制（:172）；热区仅 [read_only, tail) 故失败区间无驱动再扫成立。C# ExpiredKeysBase.Reader 回调内逐条 DELIFEXPIM 即删（ArrayKeyIterationFunctions.cs:221/:237/:245，票面 Functions/ 路径订正为 Session/Common/ 成立）。任一 Err 即不推进+幂等重扫论证成立。派沙箱席 c01l。

审核通过 2026-09-27：亲验 ttl_sweep.rs:202-206 Err 仅 warn（:206 文案自承留待重试）、:187-192 cold_commit 删除前算好、:217-219 无条件 store 与 :216「删除完成才提交」注释矛盾、gc/mod.rs:11 承诺句精确吻合、:170 读时 max(begin) 钳制单调、热区仅扫 [read_only, tail)，失败键区间 [旧游标, 新游标) 无驱动再扫成立。C# 对照成立，路径修正为 garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs（票写 Storage/Functions/ 有目录级偏差），ExpiredKeysBase.Reader 于扫描回调内逐条 DELIFEXPIM，无收集-删除-提交双阶段。方案采纳任一 Err 即本轮不推进，幂等论证成立：已删键下轮重扫经 check_expired→ttl_of None→Ok(false) 零副作用，全成功路径游标推进不变。小瑕不改判：危害节「默认均关」实指 expired-key-deletion-scan 与紧缩两通道，读路径惰性清除默认在（仅读命中触发）。

TTL 清扫冷区游标在逐键删除失败时仍推进，失败键区间从此无驱动再扫违背幂等重试承诺

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# StoreExpiredKeyDeletionScan 删除在扫描回调内逐条推进（ArrayKeyIterationFunctions.cs Reader），无「先收集后删除再提交游标」的双阶段拆分，不存在失败键被游标越过面。文档依据即仓内承诺：wkv/src/gc/mod.rs:11「删除完成后才提交游标，取消或失败下一轮重试（幂等）」。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wkv/src/gc/ttl_sweep.rs:198-219：删除循环 match check_expired Err(e) => warn! 仅留痕（:206 文案自承「留待下一扫描周期重试」），:216-219 cold_cursor.store(addr) 无条件提交——cold_commit 在删除前算好（:187-192），逐键 Err 后游标照样越过该批。冷游标单调（:170 max(begin) 钳制）、热区只扫 [read_only, tail)，失败键所在区间 [旧游标, 新游标) 从此无任何驱动再扫。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
设备 I/O/编解码错误时失败键物理清除只剩读路径惰性与紧缩 TTL 判死（默认均关）→ 空间滞留永久化；门面承诺与行为矛盾误导后续维护。

涉及代码：
rust 文件与函数：
wedb/wkv/src/gc/ttl_sweep.rs:cold_commit 提交臂

对应 c# 文件与函数：
garnet/libs/server/Storage/Functions/ArrayKeyIterationFunctions.cs:Reader（逐条推进无越过错隙）

精炼执行方案：
1 删除循环记录首个 Err 位（或任一 Err 即本轮不推进）：cold_commit 仅在全部 picked 删除成功（Ok）时提交，存在 Err 时游标停在批起点，下一轮重扫（check_expired 双检幂等保证无重复删除副作用）
2 测试验证点：注入 check_expired 失败后游标不动、下轮重扫命中同批；全成功路径游标推进回归不变

收口记录（收票席 R4 批次，2026-09-28）：合入 f5861bbb（验货 e547ad67+d4515e39 dev 前进复查全绿）。收口形态=冷区删除循环置 delete_failed，提交臂 cold_cursor.filter(|_| !delete_failed)——任一键 Err 游标停批起点、下轮整批重扫（check_expired 双检幂等），兑现 gc/mod.rs「删除完成后才提交游标」承诺句；GcStatsSnapshot 增 cold_cursor 观测口。锁测 wkv/tests/gc.rs test_cold_cursor_holds_on_delete_failure（read_range 定点设备注入；回装无条件 store 即红，17/17 绿）。deviations 无需。
