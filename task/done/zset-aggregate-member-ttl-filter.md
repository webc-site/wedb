zset 聚合内核不过滤成员级 TTL：过期成员被计进 ZUNION/ZINTER/ZDIFF/ZINTERCARD 与 STORE 族落盘

来源：next/glm.data.md 条 7（该文件已被并发分拣波消费删除，原文转抄存于
/Users/z/git/db/wedb/next/zset-aggregate-member-ttl-filter.md，与本档同名同题，
本档按当下主仓代码重新取证）。取证基线：主仓 /Users/z/git/db/wedb，
分支 dev，HEAD a7402c4（bb06827、6311510 两轮复核：本档取证文件未变、锚点未位移），行号按符号在当下代码复核。

现状

rust 的 zset 聚合内核直接遍历原始容器，全链零成员级过期判定：
/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:886 `diff_sets`
（:893-902 遍历 `first.sorted_set`、:899 以 `o.sorted_set_dict.contains_key` 判排斥）、
:907 `combine_sets`（交集臂 :913-943 于 :926 遍历 `min_obj.sorted_set_dict` 并以
:932 `obj.sorted_set_dict.get(member)` 判定；并集臂 :945-958 于 :950 遍历
`obj.sorted_set_dict`）、以及 `sorted_set_intersect_length`（:354 起）的多键遍历
:405-435。调用点：write.rs:278、:305（ZDIFF / ZDIFFSTORE）、:346（ZINTERSTORE）、
:481（ZUNION）、:715（STORE 族共体），异步侧
/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/sorted_set_commands/slow.rs:545、:569、
:599、:726 与 :620-640（ZINTERCARD 慢臂）。
grep 全目录 `is_expired` 在
/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/sorted_set_commands/ 零命中，
即命令层根本不做成员级过期裁决；同族 O(1) 计数与聚合结果还各走一套口径：
write.rs:408-411 与 slow.rs:626-630 的 ZINTERCARD 用 `count()`（剔过期，
/Users/z/git/db/wedb/wedb/wcol/src/zset/sorted_set_object.rs:564），
而 :417-421 的多键遍历与 `min_by_key(|(_, o)| o.count())` 之后实际取的
`sorted_set_dict` 不剔，同函数两分支自相矛盾。

过期成员当前只在装载时被剔除：
/Users/z/git/db/wedb/wedb/wcol/src/zset/sorted_set_object.rs:219 `deserialize_from_slice`
（:236-246 按 `expiration < now` 物理删除）与 :264 `serialize_wire`（:266-274 写出时过滤）。
成员在装载之后、聚合遍历之前到期（ZADD 带成员 TTL / ZEXPIREMEMBER 一类毫秒级设定，
慢路径 `load_many_cold` 逐键 await 装载磁盘冷键可把窗口拉到毫秒~几十毫秒）即被计入结果，
并随 STORE 族以「无 TTL 的活成员」形态落盘目标键——C# 侧该形态不可能出现。

C# 参考

过滤发生在聚合时刻而非装载时刻：
/Users/z/git/db/wedb/garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:237-256
`Dictionary` getter——无过期结构、或最小堆堆顶未到期（:241
`expirationQueue.TryPeek(...) && expiration > DateTimeOffset.UtcNow.Ticks`）即直返
`sortedSetDict`，否则单次遍历按 :248 `!IsExpired(kvp.Key)` 重建过滤视图；
聚合内核一律经该 getter：
/Users/z/git/db/wedb/garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:1207
与 :1242（ZUNION / ZUNIONSTORE 首集与后续集）、:1524 与 :1529（ZINTER / ZINTERSTORE，
含 `keys.Length == 1` 直返单键的臂）、:1368 `SortedSetIntersectLength` 经
:1372 `SortedSetIntersect` 复用同一路径。ZDIFF 族走
SortedSetObject.cs:537 `CopyDiff`（:541-555 单侧/空侧两臂均 `!IsExpired` 过滤，
单键 `CopyDiff(first, null)` 也过滤）与 :572 `InPlaceDiff`（:577-583 对侧 `IsExpired`
即移除）。

修法

1. 单点先落 wcol：在
   /Users/z/git/db/wedb/wedb/wcol/src/zset/sorted_set_object.rs 补一个对位 C# `Dictionary`
   getter 的存活视图口（名实对齐语义即可，例如 `live_dictionary()`：
   `expiration_times` 不在册或过期堆堆顶未到期 → 借用直返 `&sorted_set_dict`；
   否则返回重建后的过滤 `HashMap`），用返回类型区分借用与owned 两态
   （`Cow` 或既有仓内同型范式），禁止在命令层各写一份 `filter(!obj.is_expired(m))`。
   堆顶短路件已在（:595 `has_expirable_items`、:575 `is_expired_at`、:587 `is_expired`
   与过期堆字段），无需新数据结构。
2. 内核改用该单点：`diff_sets`（含 :890-892 `rest.is_empty() => first.clone()` 的
   单键臂，对位 C# `CopyDiff(first, null)` 的过滤语义，现原样 clone 会把过期成员带走）、
   `combine_sets` 两臂、`sorted_set_intersect_length` 多键遍历、
   slow.rs:620-640 的 ZINTERCARD 臂，全部换成存活视图；`contains_key`/`get` 的排斥判定
   也须走存活视图（否则「对侧只剩过期成员」仍会误判交集命中，对位 :577 InPlaceDiff）。
3. 口径统一：ZINTERCARD 的 `count()` 与遍历口径经 1/2 后自然一致，
   不新增第三套计数。计数口本身的 O(带 TTL 成员数) 复杂度不在本单射程：
   已在册票 task/ing/zset-o1-bypass-expiry-count.md（转抄 stub 为
   /Users/z/git/db/wedb/next/zcard-zset-o1-count.md，hash 同族半边在
   /Users/z/git/db/wedb/next/hlen-o1-bypass-expiry-count.md）承接该面，
   本单只把「哪些成员参与聚合」判对，不改计数策略，两单勿互相顺手改 `count()`。
4. 范围守界：只改 zset 聚合链。C# 的 SetObject 无成员级过期
   （grep /Users/z/git/db/wedb/garnet/libs/server/Objects/Set/SetObject.cs 零 `IsExpired`），
   集合聚合不涉及；hash 侧 C# 在 HashObject.cs:385、:518、:666 等自身方法内过滤，
   rust 是否同口径不在本单射程，需另开一票核查。

优先级

功能缺口（读到错结果并把错值持久化到 STORE 目标键；两侧口径分叉可复现）。

交叉引用

1. /Users/z/git/db/wedb/task/ing/slow-path-string-key-admin-arms.md 与
   /Users/z/git/db/wedb/task/ing/msetnx-slow-path-meta-domain-probe.md 同在慢路径域，
   但本单只动 sorted_set_commands/{write,slow}.rs 与 wcol，无文件冲突。
2. task/ing/resp3-command-layer-frame-parity.md（认领前在
   /Users/z/git/db/wedb/next/resp3-command-layer-frame-parity.md）改 zset 回复帧型，
   与本单同文件不同函数段，若并行需按该单先落地后重核行号。
3. /Users/z/git/db/wedb/task/ing/resp-null-protocol-single-source.md 覆盖 nil 帧版本口，
   本单不改任何应答帧。
4. 勿双花：/Users/z/git/db/wedb/next/zset-aggregate-member-ttl-filter.md 是并发分拣波从
   next/glm.data.md 条 7 原样转抄的六行 stub，与本档同名同题（无 HEAD 复核、锚点未校正，
   如其引用的 object_store_utils.rs:1041-1042 实为 `handle_bftree_drain_and_delete`
   重灌清退臂，promote 调用在 :1046-1049）；派单以本档（task/ing 细化版）为
   载体，认领时把该 stub 一并清掉，不得据 stub 另开分支。同一批转抄的
   task/ing/zset-o1-bypass-expiry-count.md 已在去重段互引本档，两单射程以本档修法
   第 3 条为界（本档管「谁参与聚合」，它管「计数口复杂度」）。

验收

1. 用例：`ZADD k 1 m` + 成员级 TTL 置毫秒级 → 睡到到期 → `ZUNIONSTORE d 1 k`、
   `ZDIFF 1 k`、`ZINTERCARD 1 k`、`ZINTERSTORE d 2 k k2` 均不含 `m`；
   对照未到期时全部含 `m`（C# 语义逐值一致）。
2. 装载后到期窗口：冷键经 `DEBUG FLUSHANDEVICT` 降级慢路径装载，成员在窗口内到期，
   聚合结果不含之（现形态必含，属本单主靶）。
3. /Users/z/git/db/wedb/wedb/wnode/tests/resp_sorted_set.rs、sorted_set_ttl_test.rs、
   tiered_field_ttl.rs 既有断言全绿；cargo check 零告警（禁写 allow）。
