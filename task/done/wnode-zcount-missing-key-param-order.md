甄别结论：通过（甄别席 zc-fix-r16-zcount，2026-09-26）定级 P2
核验记录（现码逐点复跑）：C# 四锚全中——SortedSetCommands.cs:SortedSetCount NOTFOUND 臂 :565 直写 RESP_RETURN_VAL_0；SortedSetOps.cs:982 经 ReadObjectStoreOperation（Common.cs:49 objectContext.Read）；对象层 SortedSetObjectImpl.cs:286 TryParseParameter 仅键存在回调内可达。rust 现状锚全中——read.rs:152-160 sorted_set_count 走 zset_rmw_read_or_bail!（mod.rs:40-48）→ zset_rmw（mod.rs:202-233）SyncRmwHandlers::new 未挂 with_on_missing（rmw_helpers.rs:991 on_missing 缺省 None，对照 hash 族 mod.rs:200 已挂为既成单源机制）；Missing 落空对象求值矩阵（rmw_helpers.rs:1057-1062）；wcol sorted_set_object_impl.rs:523-534 sorted_set_count 先 try_parse_parameter 出 -ERR 帧（sorted_set_object.rs:427 分发亲验）；同族 ZLEXCOUNT read.rs:165-194 装载先行 Missing 显式 :0，判定序分叉属实，缺陷现状仍在。慢路径三点联动订正属实：slow.rs:176「ZCOUNT 不入表」注释在位、op_opt 表（:177-192，Zlexcount :192 同形先例）确无 Zcount、rmw_spec:308 在架、分层态树内原生 Zcount 臂（tiered_collection_ops/zset.rs:419-439 流式 O(1)）确经 rmw_spec→zset_rmw_cold 接手，仅摘 rmw_spec 不补 op_opt 即退化物化，三点缺一不可成立。
非重复：deviations.md §5 系空串崩溃防御异轴，不覆盖本缺陷；各票池 grep zcount 唯此票。数据面无害确证：should_write_back（mod.rs:155 首字节 - 门 + :176 is_read_only 门）双拒写回，错误帧不落库不建键。执行方案与 ZLEXCOUNT 双侧同形、复用既有钩子与装载宏（zset_load_or_bail!，mod.rs:18 在位），无新机制无过度设计，架构合规；测试点三态锁闭环可执行。

审核结论：通过（真缺陷修复型，审核席 zcode-r19-review-zset，2026-09-26）

亲验记录：C# 链路四点全中——SortedSetCommands.cs:SortedSetCount 的 NOTFOUND 直写 :0、SortedSetOps.cs:SortedSetCount 经 ReadObjectStoreOperation（Common.cs:53 objectContext.Read）、键缺席回调不执行、对象层 SortedSetObjectImpl.cs:SortedSetCount（:286-319）TryParseParameter 校验仅键存在时可达。rust 链路四点全中——read.rs:159 zset_rmw_read_or_bail!、mod.rs:224-231 SyncRmwHandlers 未挂 with_on_missing、rmw_helpers.rs:1054-1062 Missing 落空对象求值、wcol sorted_set_count（:524-534）先参数校验出错误帧；慢路径 slow.rs:308 rmw_spec 同源。判定序分叉属实，且 C# 侧与真 Redis t_zset.c zcountCommand 的 lookupKeyReadOrReply 缺键先回 0 序一致，修复方向正确。Zcount 系 is_read_only（mod.rs:187）、计数臂无 delete_expired_items、should_write_back 恒拒，改装载先行零写回语义损失，且缺键时免进 try_rmw_window 写窗，数据面开销下降。

执行方案订正（审核席精确化，原方案第 2 点有遗漏）：慢路径 Zcount 现状不在 try_tiered_arm 的 op_opt 匹配表（slow.rs:176 注释明说「ZCOUNT 不入表：已在下方 rmw_spec 内」），分层态接手靠 zset_rmw_cold → run_async_rmw 的分层态原生臂（tiered_collection_ops/zset.rs:419-439 树内流式计数，内存 O(1)）。仅「从 rmw_spec 摘出移入装载段」而不加 op_opt 表项，会把分层态 ZCOUNT 从树内流式臂退化回 slow_load_eval 全量物化（该臂头注 :416-418 明示此形为已消除的「千万级集合一次 O(N) 内存抖动」缺陷面）。故慢路径必须三点联动，缺一不可。

精炼执行方案（订正版）：
1. 快路径 read.rs:sorted_set_count 改装载先行（zset_load_or_bail!，与同文件 ZLEXCOUNT 的 sorted_set_length_by_value 同形）：Missing 回 :0 短路；Present 后 run_operate(Zcount)（参数校验与计数语义不变，键存在 + 非法参数仍回错误帧）
2. 慢路径三点联动（与 Zlexcount 慢臂完全同形）：
   a. slow.rs op_opt 匹配表加 RespCommand::Zcount => Some((SortedSetOperation::Zcount, (0, 0)))（分层态在 try_tiered_arm 处由树内原生 Zcount 臂接手，缺失键不触树）
   b. slow.rs rmw_spec 摘除 Zcount（:308）
   c. 装载段加 Zcount 分支（slow_load_eval，on_missing 回 :0）
   d. 同步删订 slow.rs:176 「ZCOUNT 不入表」注释（改述入表位）
3. 测试验证点：resp_sorted_set.rs 补三态锁——缺失键 + 非法 min/max 回 :0（快路径线帧、慢路径 SlowWait 直驱、升阶分层臂各验一次，对齐三态对照形制）；存在键 + 非法参数错误帧回归不变；分层态存活键 ZCOUNT 既有用例零回归（防物化退化）；ZCOUNT -inf/+inf 与 (5 边界既有断言零回归

ZCOUNT 缺失键上参数校验序分叉：C# NOTFOUND 短路回 :0，rust 空对象求值先报 min/max 非法浮点错误帧

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# ZCOUNT 是纯读通道：会话层 libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetCount（:539-575）对 storageApi.SortedSetCount 的返回值处理为 OK 走 ProcessOutput、NOTFOUND 直写 :0（:564-566）、WRONGTYPE 写错误帧。存储层 libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetCount（:982-984）经 ReadObjectStoreOperation（libs/server/Storage/Session/ObjectStore/Common.cs:49-59，objectContext.Read）——键缺席时 Tsavorite Read 回调不执行、对象层 SortedSetCount（SortedSetObjectImpl.cs:286-319 的 min/max 解析）完全不运行，min/max 词形不校验。即 C#（与真 Redis t_zset.c zcountCommand 先 lookupKeyRead 缺键即回 0 的顺序）对「ZCOUNT missing_key notafloat 5」回 :0。同族对照：ZRANGEBYSCORE / ZLEXCOUNT 缺失键同走 NOTFOUND 常量帧（空数组 / :0），参数同样不校验。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 快路径 ZCOUNT 走 rmw 通道而非装载先行：wedb/wnode/src/resp/objects/sorted_set_commands/read.rs:sorted_set_count（:151-160）经 zset_rmw_read_or_bail!（mod.rs:40-48）进 zset_rmw（mod.rs:202-233）的 run_sync_rmw（wedb/wnode/src/resp/objects/rmw_helpers.rs:1017-1133）——zcount 的 SyncRmwHandlers 未挂 on_missing 短路钩子，ObjLoad::Missing 落空对象求值矩阵（:1054-1062），对象层 wedb/wcol/src/zset/sorted_set_object_impl.rs:sorted_set_count（:524-544）先做 try_parse_parameter 参数校验，非法词形直接写 -ERR min or max is not a valid float 错误帧。慢路径同形：slow.rs:rmw_spec 的 Zcount 臂（:308）经 zset_rmw_cold 复用同一 handlers（run_async_rmw 的 Missing 臂同钩同帧），快慢自洽但双侧均错。对照同族 ZLEXCOUNT 在 read.rs:sorted_set_length_by_value（:165-194）是装载先行、Missing 显式回 :0（参数不校验），两命令判定序不同源。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
对拍与契约分叉：ZCOUNT missing_key abc def 双侧应答发散（C# :0 对 rust -ERR），板块 5.1「命令处理与参数解析」判定序违约；客户端语义错乱——探针/清理脚本对不存在键做 ZCOUNT 扫描时，C#/Redis 侧静默 0，rust 侧整批命令报错中断（redis-cli 非交互模式遇错即停）。无数据面危害（错误帧不落库不建键，should_write_back 首字节 - 门拒写）。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/sorted_set_commands/read.rs:sorted_set_count
wedb/wnode/src/resp/objects/sorted_set_commands/mod.rs:zset_rmw_read_or_bail / zset_rmw
wedb/wnode/src/resp/objects/rmw_helpers.rs:run_sync_rmw（Missing 无钩子空对象求值矩阵）
wedb/wnode/src/resp/objects/sorted_set_commands/slow.rs:sorted_set（rmw_spec 的 Zcount 臂）
wedb/wcol/src/zset/sorted_set_object_impl.rs:sorted_set_count

对应 c# 文件与函数：
garnet/libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetCount
garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetCount
garnet/libs/server/Storage/Session/ObjectStore/Common.cs:ReadObjectStoreOperation
garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetCount

精炼执行方案：见文件顶部审核结论内的订正版（原方案慢路径单点表述有遗漏，以订正版三点联动为准）。

合入哈希：288149d 收口形态：快路径 read.rs 改装载先行（zset_load_or_bail! Missing 短路 :0），慢路径三点联动（op_opt 入表、rmw_spec 摘 Zcount、装载段 slow_load_eval on_missing :0）并订正注释，resp_sorted_set.rs 补三态锁测试；沙箱合 dev 后 cargo check --tests 零警告，dev 快进合入（沙箱合并提交 288149d）。
