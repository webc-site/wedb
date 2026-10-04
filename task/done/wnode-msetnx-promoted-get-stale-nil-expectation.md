甄别结论：通过（2026-09-29 主控甄别，定级 P3——双侧亲验：121dd3c/d1dc889 双 worktree 复跑同红证非近期翻转；三域判型口径同步/异步漏斗 init 即含 Meta 臂，C# ReadMethods.cs Reader 单记录 ValueIsObject 锚成立；纯测试期望陈旧无生产危害）

msetnx 升阶键 GET 陈年 nil 期望与三域判型口径冲突（门禁 no-fail-fast 揭出，fail-fast 长年遮蔽）

问题分析：
1. 门禁现状确证：./test.sh --no-fail-fast 红 wnode::msetnx_atomic::msetnx_slow_meta_only_promoted_key_counts_as_existing，wnode/tests/msetnx_atomic.rs GET big 期望 `$-1` nil，实得 `-WRONGTYPE Operation against a key holding the wrong kind of value`。worktree 于 121dd3c（ttl 票前一提交）与 d1dc889（slowload 合入）两处复跑同样红——非近期合入翻转，系初版笔误被 fail-fast 长年遮蔽（wkv 二进制字母序先于 wnode，历次门禁全量跑从未到达本例）。
2. 语义裁断：键 big 为升阶哈希（Meta 域唯一身份，Hlen :2014 与 EXISTS :1 双证存活），三域判型口径（同步漏斗 ttl_sync.rs read_adjudicated_user_sync_with_prefix 与异步漏斗 storage_session.rs read_user_quiet_with_prefix 均 init 即含 Meta 臂「升阶键判对象键，C# Reader 单记录统一 ValueIsObject」）下 GET 恒 WRONGTYPE，真 Redis 同形。nil 仅属真缺席键；测试本意的「String 域无记录」由 WRONGTYPE 反证（类型不符即非字符串），期望 `$-1` 与自有漏斗、C#、Redis 三方皆冲突。
3. 危害面：纯测试期望陈旧，生产行为无恙；但该红长期遮蔽意味着历次「门禁全绿」结论实际未覆盖 wnode 尾部二进制。

涉及代码：
rust 文件与函数：
wedb/wnode/tests/msetnx_atomic.rs:msetnx_slow_meta_only_promoted_key_counts_as_existing（零写入核验段 GET big 断言）

对应 c# 文件与函数：
garnet/libs/server/Storage/Functions/MainStore/ReadMethods.cs:Reader（单记录统一 ValueIsObject 判型，对象记录 GET 即 WRONGTYPE）

精炼执行方案：
1. GET big 断言改期望 `-{RESP_ERR_WRONG_TYPE}` 帧（rmw_rebuild_side_domain_retire.rs:240 同形），注释锚三域判型口径与 C# ValueIsObject
2. 验证：wnode::msetnx_atomic 全绿 + 全量门禁 --no-fail-fast 全绿

查重：task 五池无同票。

终态注记（2026-09-29 执行收口）：
修复合入 31547bd（WRONGTYPE 断言，并发席代收）+ 本席去重提交（删除遗留 `Get big → $-1` 重复断言——31547bd 收编版本含同函数两断言自相矛盾的中间态）。
收口形态：GET big 期望改 `-{RESP_ERR_WRONG_TYPE}` 帧，注释锚三域判型口径（升阶键 Meta 域命中即对象键，C# ReadMethods.cs Reader 单记录 ValueIsObject）；零写入核验强度不降（误写 String 域即回 bulk 值必红）。
验证：msetnx_atomic 全绿；全量门禁 5264/5264 绿。
