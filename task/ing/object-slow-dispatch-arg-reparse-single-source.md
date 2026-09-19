对象族慢分派对快路径已解析参数的重推导双写：一条命令两套推导、三套值域谓词

来源：next/garnet-api-slow-path-command-split.md 条二（「对象族四慢分派同型」「tiered 门 + 各
命令族执行段内联混装」）的正交化残留。该文档余下的「exec_slow_impl 按域拆执行段」主张属纯搬运
打磨，本波已按打磨裁决撤销（原文件随本票一并删除），函数体量不在本票射程。
取证基线：主仓 /Users/z/git/db/wedb，分支 dev，行号为当下工作树实测；本目录多个文件正被在途票
改动，开工前一律按符号名重取锚点。

结论

C# 每个对象命令一个方法，参数推导与应答形态各一份，磁盘 pending 由存储层就地吸收后在同一函数体
内重放。rust 把同一命令切成快（BatchStoreSession 同步段）与慢（StorageSession 异步段）两个函数体
之后，装载核已经单源（object_store_utils.rs 的 run_async_rmw / SyncRmwHandlers / slow_load_eval /
compute_expiration_ticks / parse_elements_header、rmw_helpers.rs:64 try_tiered_arm、各族
run_operate / should_write_back），但参数推导与缺失态应答没有单源：慢侧在四个族文件里各写一份
「重推导」，注释自认（hash_commands.rs:828「快路径已校验，此处防御性重解析」、
list_commands/slow.rs:665「参数重推导（快路径已校验，防御性重解析）」、
sorted_set_commands/slow.rs:457「对齐快路径」）。同一命令的同一谓词存在两到三份实现，且已经
发生一处真实漂移：SINTERCARD 的 i32 值域过滤按 task/done/sintercard-i32-filter-lower-bound.md
只改了快侧，慢侧那一份由该票边界明令不动，至今仍是旧的半开区间谓词。

现状（成对复抄位点）

1. HEXPIRE 族：快 hash_commands.rs:594 parse_hash_expire_args（arity 门、strict_i64、负时刻拒、
   NX|XX|GT|LT 词元、FIELDS 头），失败面为 RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER /
   RESP_ERR_INVALID_EXPIRE_TIME / 参数计数帧；慢 hash_commands.rs:831 slow::parse_expire_args 同
   序列重抄，任一失败折叠为 cs::write_error_raw(RESP_ERR_ASYNC_REQUIRED)（内部哨兵外泄，同
   task/ing/slow-path-string-key-admin-arms.md 的哨兵问题同一口径）。
2. ZEXPIRE 族：快 sorted_set_commands/write.rs:586 sorted_set_expire 内联推导 :594-616；慢
   sorted_set_commands/slow.rs:163 parse_expire_args。两侧仅错误通道不同。
3. HTTL / HPERSIST / ZTTL / ZPERSIST 族：快 hash_commands.rs:675 hash_time_to_live、:716
   hash_persist、write.rs:641 sorted_set_time_to_live、:675 sorted_set_persist 各自直取
   FIELDS/MEMBERS 头；慢 hash_commands.rs:859 parse_fields_args、
   sorted_set_commands/slow.rs:191 parse_members_args。
4. 命令到毫秒/时间戳布尔对与 args12 元组的映射写了三处：快侧在薄分派表
   garnet_api/raw.rs:262-269（H 族）与 :341-348（Z 族）一次算好作形参下传；慢侧在分派函数体内
   再 match 一遍（sorted_set_commands/slow.rs:262-267 与 :293-303；hash_commands.rs 慢模块
   :920-:975 同型）。
5. ZRANDMEMBER：快 write.rs:516 sorted_set_random_member 的参数打包（strict_i32、
   param_count.min(i32::MAX >> 2)、WITHSCORES 大小写门、count 为 0 不触达后端、缺失态
   `*0\r\n`/nil 分叉 :528-534）；慢 sorted_set_commands/slow.rs:483-533 逐段重抄（含 :514-520
   同一缺失态分叉、:503-506 同一零计数短路），非整数与语法错误的应答退化为哨兵。
6. ZRANK / ZREVRANK WITHSCORE：快 sorted_set_commands/read.rs:210 sorted_set_rank；慢
   sorted_set_commands/slow.rs:451-467 重推 with_score。
7. SINTERCARD 与 ZINTERCARD 同一 C# 谓词三份实现：快 set_commands.rs:612 set_intersect_length
   （:621-627 numkeys 用 i32::try_from 全域过滤，:638-655 LIMIT 同款，三类独立错误帧 +
   "ERR LIMIT can't be negative"）、慢 set_commands.rs:1181 parse_sintercard_args（仍是
   (1..=i32::MAX).contains 与 v <= i32::MAX，全部失败折叠为 :1173 哨兵）、第三份
   sorted_set_commands/slow.rs:793 parse_zintercard_args（strict_i32 + v < 0 拒）。
8. LMPOP / BLMPOP 与 LTRIM / LRANGE：快 list_commands/blocking.rs:39 list_pop_multiple 的
   :49-84 推导、read.rs:44 list_range、write.rs:155 list_trim；慢 list_commands/slow.rs:669
   parse_mpop_common、:560 parse_i32_pair（numkeys 下标基线、方向词元、COUNT 大小写门逐字同构）。
9. SPOP / SRANDMEMBER 的 count 值域：快 set_commands.rs:368 set_pop、:436 set_random_member；慢
   set_commands.rs:1204 spop_cold 内再解析一遍 count 值域并折叠错误。
10. HRANDFIELD 的打包两份实现已经分叉：快 :487-489 对第三词元做 WITHVALUES 大小写校验、不合规回
    RESP_ERR_GENERIC_SYNTAX_ERROR；慢 hash_commands.rs:1043-1060 直接以 `refs.len() == 3` 当
    with_values，词元校验在复抄中被丢掉（同一输入的应答形态分叉：平铺 field 还是 field+value 成对）。
    命令入口因快侧先校验暂不外泄，但事务与 AOF 侧的非命令入口不享有该前提。

C# 对位（每命令一份推导，pending 在同函数内由存储层吸收重放，不存在第二份解析）

- garnet/libs/server/Resp/Objects/HashCommands.cs:HashExpire（:582）、:HashTimeToLive（:675）、
  :HashRandomField（:214）
- garnet/libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetExpire（:1751）、
  :SortedSetTimeToLive（:1847）、:SortedSetRandomMember（:815）、:SortedSetIntersectLength（:1175）
- garnet/libs/server/Resp/Objects/SetCommands.cs:SetIntersectLength（:159）、:SetPop（:517）、
  :SetRandomMember（:633）
- garnet/libs/server/Resp/Objects/ListCommands.cs:ListPopMultiple（:189）、:ListTrim（:454）、
  :ListRange（:502）
- 薄分派先例：garnet/libs/server/Resp/RespServerSession.cs:623 ProcessMessages（case 一行调用，
  命令体内不再判「命令 → 命令名 / 命令 → 布尔对」）

目标形态

一条命令一份推导，快慢两侧只接各自的 IO 原语与出帧通道。

1. 把每个推导体做成无 IO 纯函数并与 parse_elements_header 同层（object_store_utils.rs，按
   ElementHeaderKind 参数化使 HEXPIRE 与 ZEXPIRE 共用一份），或直接把快侧既有 parse 函数改为返回
   结果枚举供两侧调用。
2. 命令到布尔对、args12、LIMIT/方向词元形态的映射在命令入口算一次；慢侧不再从原始 args 反推，改由
   降级快照尾参带入（仓内既有先例三处：garnet_api/slow.rs:116-121 MSETNX 的续跑标记尾参、
   :305-312 INFO 的 8 字节 LE 上限、:786-798 对象扫描族的 4 字节 LE COUNT）。
3. 推导内核返回失败类别枚举（BadInteger / BadExpireRange / BadArity / BadSyntax /
   BadLimitRange），快侧映射为 C# 同款错误帧，慢侧映射为同一套帧；RESP_ERR_ASYNC_REQUIRED 只在真正
   未接线的命令兜底臂出现。
4. 缺失态与短路应答（`*0\r\n`、nil、`:0`、全 null 数组）随推导内核一并单源，杜绝
   slow.rs:514-520 与 write.rs:528-534 型的双份分叉。
5. 分派骨架、tiered 门、函数体量一律不动。

门禁与验收判据

1. 七组命令（HEXPIRE/ZEXPIRE、HTTL/ZTTL 族、ZRANDMEMBER、ZRANK、SINTERCARD/ZINTERCARD、
   LMPOP/LTRIM/LRANGE、HRANDFIELD）的推导实现 grep 命中数各为 1；慢侧 HRANDFIELD 臂收口后须与快侧
   同口径拒非法第三词元；
   `grep -rn "重推导\|对齐快路径" wedb/wnode/src/resp/objects` 归零或仅剩确无快侧对应的位点。
2. SINTERCARD 慢侧对 numkeys=-3000000000、LIMIT=-3000000000 的应答与快侧逐字节一致
   （"-ERR value is not an integer or out of range"），不再出现
   "-ERR command requires asynchronous completion"；断言补进 wedb/wnode/tests/resp_set.rs 与
   tests/resp_slow_path.rs，冷键复跑形态沿用 tests/debug_flushandevict.rs 的 DEBUG FLUSHANDEVICT。
3. 同输入快慢同字节：HEXPIRE、ZEXPIRE、ZRANDMEMBER、LMPOP 各一例热键直答与一例冷键慢答比对，
   RESP2/RESP3 两版本，nil 帧走 write_resp_null_ver 单点。
4. ./sh/clippy.sh 与 ./test.sh 零告警零失败（禁写 allow）；./js/check.js 的缺失与重复锚点组数不增
   （新推导内核不重复挂 C# 锚点，锚点随命令入口留一处，口径见
   task/done/cs-anchor-dup-single-mount.md）。
5. 若为本票新增的推导单源函数，无须在 js/check/ignore/garnet 下登记（它们不是「无需实现」项，而是
   合并实现）。

坑与边界

1. 在途票先行：task/ing/object-scan-coscan-kernel-single-source.md 与本票同一修法（无 IO 内核 +
   结果枚举 + 双域各落帧），射程只含 shared_object_commands.rs 的扫描校验与 COSCAN 三域判定，与本票
   的四个族文件零重叠；建议该票先落，本票复用其失败类别枚举放 object_store_utils.rs，避免第二个枚举
   类型。task/ing/slow-path-string-key-admin-arms.md 改 wedb/wnode/src/resp/garnet_api/slow.rs 的
   分派臂，与本票不同文件，但两票共用「哨兵只出现在未接线命令」这一条判据，收口时合并即可。
   task/ing/tiered-collection-ops-file-split.md 只搬 tiered_collection_ops.rs，禁在同一次提交里混搬
   本票文件。
2. 不得以「慢侧只在快侧校验通过后才到达」为由保留两份推导：该前提本身就依赖两份实现保持同步，
   SINTERCARD 已实证一处落地一处不动；且慢侧冷核另有非命令入口复用
   （storage/session/txn_proc_view.rs:59 走 sorted_set_commands/slow.rs:41 zset_rmw_cold），
   单源化后这些入口自动同口径。
3. 降级快照只带原始 args 与既有尾参：新增尾参须同时改 exec 侧降级投递与会话侧续跑解析，勿只改一侧；
   事务与 AOF 回放的入参形态与命令入口不同，改快照前先 grep 三个消费面。
4. 命令到布尔对的映射下沉时保持编译期 match 静态分发，禁改成运行时查表或字符串比较
   （.agents/skills/transpile/SKILL.md:14 的静态分发要求）。
5. 禁止用注释「同口径」「对齐快路径」代替单源（.agents/skills/transpile/SKILL.md:65 禁例）；禁止写
   占位空壳或 todo 来「先过门禁」（同文件 :79）。暂不收口的族保留现状两份并在本票追加注记，不建
   第二张票。
6. 优先级：重复逻辑（同一参数推导与同一值域谓词两到三份实现，已发生一处跨副本未同步的真实漂移）。
   排在死代码票之后、纯打磨之前开工；本票不做任何纯搬运拆分。

盘点补记（qw13.invA object-slow-dispatch-arg-reparse-single-source）：dev e75716e 复核原样：慢侧重推导注释群仍在（objects/hash_commands.rs:828/:858、sorted_set_commands/slow.rs:162/:190）；SINTERCARD 慢侧半开区间漂移仍在：set_commands.rs parse_sintercard_args 的 limit 过滤为 v <= i32::MAX（允许负 limit），与 SPOP 臂 (0..=i32::MAX)（set_commands.rs:381 附近）口径不一致。与 object-scan 收口票同文件域宜并棒。
