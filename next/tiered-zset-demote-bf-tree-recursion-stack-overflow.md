优先级：功能缺口（正确性可达性——分层集合扫描面在长墓碑连跑上栈溢出，实际不可用）

4 单问题：分层（tiered）zset 大集后台降阶链在默认线程栈下 SIGABRT 栈溢出，
是引擎扫描递归而非用例入参问题。
（原票第 4 句「同尺寸列表族在默认栈下可通过，故缺口定位在 zset 降阶链」经复核**不成立**，
缺口在 bf-tree 扫描面全体调用面，见「复核订正」5 与 task/reject/tiered-zset-demote-stack.md 五.3。）

复核基线：主仓 /Users/z/git/db/wedb，分支 dev，HEAD bfa675cd。
全量证据（栈帧归并表、规模分档、四组分段实验、复现命令）：
/Users/z/git/db/wedb/task/reject/tiered-zset-demote-stack.md。
本轮分支 fix-tiered-zset-demote-stack 零代码改动（递归点 100% 在外部 crate 内部，
本仓公开 API 无一可界定栈深度）。

结论

问题成立、原票修法不可达。溢出由 bf-tree 0.5.6 `ScanIter::next` 的两处**尾递归自调**造成：
`/Users/z/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/bf-tree-0.5.6/src/range_scan.rs:318-321`
（`GetScanRecordByPosResult::Deleted` 臂）与 `:327-379`（`EndOfLeaf` 臂），rustc 不做 TCO ⇒
每跳过一条墓碑压一帧（实测 ≈680 B/帧）。本仓 `wbftree` 包装层**不是**自研递归封装：
`/Users/z/git/db/wedb/wedb/wbftree/src/service/ops.rs:290-327` 的 `drain_scan_iter`
是 `while let` 循环，整栈只出现 1 次。

复核订正（逐条对照原票「取证现状」）

1. 崩溃帧非递归帧：原票「lldb 指到 `bf_tree::mini_page_op::LeafEntrySLocked::
   scan_record_by_pos_with_bound`」只是溢出瞬间的最内层叶子帧（12381 帧中占 1 帧），
   该函数本身单层 match（`bf-tree-0.5.6/src/mini_page_op.rs:102-133`）。
   溢出线程逐帧归并：12232 帧 = `range_scan.rs:320`、104 帧 = `range_scan.rs:379`（跨页）。
2. 可疑链不成立：`tiered_materialize_blob` → `demote_target` 的 `from_blob` → `obj_save`
   → `handle_bftree_drain_and_delete` 四段全部零自调
   （`/Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:67`、
   `/Users/z/git/db/wedb/wedb/wkv/src/session/collection.rs:218-229` 同）。
3. 自变量是「游标之后的连续墓碑条数」，不是记录数 N：同 66000 条记录、删除集改为按键序
   逐条交错（任意连跑 ≤2）→ 默认 8MiB 栈直接通过（`demoted: 1`）。
   规模分档（同型负载 66000/存活 10000）：8MiB 炸、16MiB 炸、32MiB 过。
4. 「每条记录 3.2-6.4KB 栈增长、256MB 全过」的票面换算不成立（真实 680 B/帧 × 连跑条数）；
   132000/264000 记录即便 64/128MiB 亦炸，说明深度只随连跑长度增长、与记录数无关。
5. 影响面比降阶链宽且面向客户端：同一扫描漏斗 18 个调用面
   （`/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/tiered_collection_ops.rs:359, 794, 815,
   835, 1171, 1202, 1765, 1943, 1983, 2002, 2074, 2091, 2106, 2137, 2250` 与
   `/Users/z/git/db/wedb/wedb/wkv/src/ri.rs:122, 140, 158`）。其中 `exec_tiered_scan`
   （`tiered_collection_ops.rs:2194`，扫描体 `:2250`，即分层态 HSCAN/SSCAN/ZSCAN/LREM 负索引族
   的 COUNT 臂）、`tiered_list_arm` 的 LPOP/RPOP 有界取臂（`:1941-1948`，LPOP 臂起点 `&[0u8]`
   从树最左起跳，头端历次弹出的墓碑连跑全在游标之后）、`list_head_seq`
   （`:1763-1765`）三处一条客户端命令即可触发整进程 SIGABRT：游标之后那一段墓碑连跑必须在
   **单次 `next()` 调用内**跳完，回调早停与 COUNT 都来不及生效。

原票修法为何不可达（本仓侧无任何深度界定杠杆）

bf-tree 只有 4 个扫描入口（`src/tree.rs:1302 / 1328 / 1369 / 1399`），返回同一个 `ScanIter` /
`ScanIterMut`，`ScanIterMut::next`（`range_scan.rs:155-158 / 218`）逐字同构 ⇒ 换入口无收益；
crates.io 实测 0.5.6 已是最新版，无可升版本。四组栈档实验（40000 定宽键建树、前 30000 连删
形成 30000 长连跑）：紧上界 `scan_with_end_key(k0, k000050)` 返回 **0** 条仍 >8MiB 栈、
200 键/段分段扫描与 `scan_with_count(k0, 10)` 同样炸。机理是硬的：
`bf-tree-0.5.6/src/nodes/leaf_node.rs:1903-1911` 的墓碑 `is_absent()` 判定排在 `bound_key`
比较**之前**并直接 `return Deleted` ⇒ 上界键不截断遍历；`range_scan.rs:308` + `:322-324`
的 `scan_cnt` 只在 `Found` 分支递减 ⇒ count 只界定返回条数、不界定遍历条数。
本仓亦无页级转储 API 可换形态（`wbftree` 无对应面），`wcompact` 是 HybridLog 紧缩、与本树无关。

修法（三项，A/B 属主代理裁决，本轮未越权动手）

A. 唯一真修：把 `ScanIter::next` 与 `ScanIterMut::next` 的两处尾递归 loop 化
   （约 10 行 × 2，语义零变化——两处都是「跳墓碑/换页后继续」的纯尾递归；
   `range_scan.rs:303` 上源自带注释 `// Here we need to busy loop? Is that safe?` 即此点，
   上游 PR 有据可依；补丁草图见 task/reject/tiered-zset-demote-stack.md 四.1）。落地形态二选一：
   `[patch.crates-io]` + vendor 该 crate（全仓 `wbftree` 依赖波及，且与「不绑定微软平台代码」
   取向需主代理权衡），或先向上游提 issue（crate 自述 "from Microsoft Research"、MIT，
   `Cargo.toml.orig` 无 repository 字段，上游仓位置需先确认）、本档证据可直接附。
B. 若不动依赖：本票转裁剪边界登记 —— 实测安全边 ≈ 连续墓碑 8000 条（8MiB 栈、680 B/帧、含 2×
   余量），配套「越界后的分层删除走 `obj_writeback_tiered` 重灌臂（`bulk_load` 重建、零墓碑）」
   而非扫描侧改形。C# 无此形：对象整值常驻内存、删即就地改内存对象、删空即整键消亡
   （`/Users/z/git/db/wedb/garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:188-215`
   `PostCopyUpdater` 直接复用已克隆的 `ValueObject` → :204 `value.Operate(...)` → :207-214
   `HasRemoveKey` → `RMWAction.ExpireAndStop`），既无「按成员分记录」的树、也就无墓碑连跑，
   故 B 属本仓新增机制，与「不引入 C# 没有的新机制」冲突，须主代理显式裁决后方可落地。
C. 必做（与 A/B 无关）：订正三处与本实测相反的注释与论断 ——
   `tiered_collection_ops.rs:1723`「引擎侧 scan_cnt=1 截断」、`:1924-1926`「引擎侧 scan_cnt=n
   截断 + 回调满 n 早停，恒 O(count)」、`tiered_demote.rs:112-122` 与
   `DEMOTE_MAX_KEYS_PER_ROUND`（`:52`）的「评估轮成本有界」论证（只覆盖 I/O 次数、不覆盖栈深度）。
   顺带：`tiered_demote.rs:240` `demote_target` 仅为回答 `should_demote` 而 `from_blob` 全量重解码
   物化 blob，而 `tiered_materialize_blob` 期间的 `update_size()` 已给出计数与堆字节，
   属可省的一次全量解码（非栈问题，性能面）。

验收

1. A 或 B 落地后：连续墓碑 > 2×8000 的分层集合上，默认栈（不设 `RUST_MIN_STACK`）
   跑一轮 `tiered_demote_round` 不炸，且一条 `HSCAN key 0 COUNT 10` 不炸。
2. 「深度有界」可断言事实：单次 `next()` 的栈用量与游标后连跑条数解耦（A 后由 loop 形态给出，
   可加栈用量上界探针）；或 B 后由「树内墓碑恒低」给出。
3. C 的三处注释改写为与实测一致的口径（引用本档结论）。
4. `cargo check --all-targets -p wnode -p wbftree -p wkv` 零错零警告（禁 `#[allow(`）。

阻塞与前置

当前 dev 上原票触发用例走不到末段：
`/Users/z/git/db/wedb/wedb/wnode/tests/tiered_background_demote.rs:253`
「条目数越 65536 应升阶」先红（`ZADD key 1 m0 m1 …` 单分值多成员形态未支持），属
/Users/z/git/db/wedb/task/ing/qw13-red-tiered-watch-version-fence.md 条 3 的改动域，
非本票修法；本票结论用等价负载（成对形态 `ZADD key 1 m0 1 m1 …`）复现，不依赖该修复。

禁止：改测试规模迁就栈、加 sleep 轮询、`#[allow(`、测件里设 `RUST_MIN_STACK` 掩盖。

---

## 主代理裁决（2026-09-19，A/B/C 三项定案）

**A 判拒（不采用）**：为改约 20 行尾递归而 vendor 上游 crate（实测 src 700K / 42 个 rs 文件），
等于把第三方数据结构的长期维护分叉引进本仓，且 `[patch.crates-io]` 会波及全仓 `wbftree` 依赖面 ——
属「污染扩散」，代价与本仓转写使命不成比例。上游 issue 可另提，本票不阻塞于它。

**B 判采，且票面「B 与不引入 C# 没有的新机制冲突」这一论断不成立**：C# 侧对分层集合的成员增删
走的是 `PostCopyUpdater` → `value.Operate` → 整值写回（`RMWMethods.cs:188-215`），即**没有**
「按成员往树里逐条写删记录」这一形态；本仓现在的逐成员墓碑删除才是偏离。因此把删除重的臂
收敛到既有整值重灌面（`obj_writeback_tiered` → `apply_rmw_post_operate` →
`promote_collection_to_bftree` → `bulk_load`，全链已实存在 dev，非新建）不是新机制，
而是**把两套并存的写路径收成一处**（正好命中「一个机制只留一处」）。
墓碑恒低 ⇒ 单次 `next()` 的深度自变量消失，第三节面向客户端的 HSCAN/LPOP 同险面一并消除。

**C 判必做**：三处与实测相反的注释/论断按订正口径改写，并与本票结论互相引用。

落地次序（同一代理按序提交，每步独立 commit，勿揉包）：
1 先 C（零行为改动，最省的一棒也有收获）；
2 再做 B：先只把**批量删除臂**（降阶链触发形态：`ZREM`/`HDEL`/`SREM` 大集连删）改走整值重灌，
  用第一节的等价负载（66000 记录 / 存活 10000 / 默认 8MiB 栈）验证不炸；
  若单条命令面（第三节 HSCAN/LPOP）在仅改删除臂后仍可在既有墓碑上触发，
  须在回报里实跑确认并给出该面的处置（不可只留文档）。
3 判据不变：深度自变量必须从「游标后连跑条数」变成「恒低」，不接受用栈大小、阈值常量或
  `RUST_MIN_STACK` 掩盖。若 B 在本仓形态下做不到墓碑恒低（例如某些臂无法走重灌），
  照实回报并给出实测边界，不要退化成加常量。

---

## 二棒落地记账（2026-09-19，分支 `fix-tiered-tombstone-density`）

一棒 `3f48bc1d` = C（纯注释，三处论断订正，含 `tiered_demote.rs` 的评估轮成本口径）。
二棒 `df35b160` = B 落地 + 死件清理 + 行为验证 + 既有测试随写形订正。
`git diff --stat dev...HEAD`：9 文件 +219/−260（源 6 + 测 3）。

### B 的落点（无新建机制，全部复用既有链路）

摘臂的四张分层快速通道表（每张表都实存，全仓 grep 确认无第五张）：
`hash_commands.rs`（去 HDEL）、`set_commands.rs`（去 SREM/SPOP）、
`sorted_set_commands/slow.rs`（去 ZREM）、`list_commands/slow.rs`（去 LPOP/RPOP，连带删掉
其 pop_count 解析支与 `finalize_pop!` 宏）。命令因此落到既有
`run_async_rmw` → 对象层求值 → `apply_rmw_post_operate` 状态机：
空则 `handle_bftree_drain_and_delete` 删键；`should_promote() || (tiered && !should_demote())`
则 `promote_collection_to_bftree` → `bulk_load` 整树重灌（**零墓碑**）；跌入降阶水位之下
则就地懒降阶为 envelope。WATCH 栅栏严格一次（树内臂 `finish_tiered_arm` 与重灌臂
`apply_rmw_post_operate` 互斥）。
完整性核对：表内其余臂皆为「只伸缩键数、不写成员级删除记录」的形——写侧仅剩
HSET/HSETNX/HMSET/HINCRBY/HINCRBYFLOAT、SADD、LPUSH/RPUSH/LPUSHX/RPUSHX、ZADD/ZINCRBY
（覆盖成员即改记录，不产生独立墓碑），读侧 HGET/HMGET/HKEYS/HVALS/HGETALL/HLEN/HSTRLEN、
SCARD/SISMEMBER/SMISMEMBER/SMEMBERS/SRANDMEMBER、ZCARD/ZSCORE/ZMSCORE、LLEN/LRANGE/LINDEX
不写；而 ZPOPMIN/ZPOPMAX/ZREMRANGEBY{LEX,RANK,SCORE}/LREM/LTRIM/LINSERT/LSET/HRANDFIELD/SSCAN
本就无树内臂、直接穿透到物化面 ⇒ 删除臂无漏项；反向亦无过度摘除
（非删除命令一律留在表内，`*_needs_write` 只按被摘的臂收窄）。
交易/AOF 重放视图同获收益：`wnode/src/storage/session/txn_proc_view.rs:132` 的
`sorted_set_remove` 复用同一入口 `zset_rmw_cold` → `run_async_rmw` 分层派发，臂一摘，
该面随之改走重灌，无需另改。

### 验收 1/2 实测（默认线程栈，macOS `ulimit -s` = 8176 KiB，未设 `RUST_MIN_STACK`）

同一负载 `ZADD z 1 m0 1 m1 …` 66000 成员 → `ZREM z m0…m55999`（56000）→ `tiered_demote_round`：

    改前  P1 promoted: tiered=true
          P1 after ZREM: tiered=true
          thread 'p1_zset_demote_round_over_tombstone_run' has overflowed its stack
          SIGABRT
    改后  P1 after ZREM: tiered=false                       ← 前台写回臂就地降阶，树已不存在
          P1 demote round: TieredDemoteStats { candidates: 0, demoted: 0, aborted: 0 }
          ZCARD = :10000                                    PASS

客户端单命令面（第三节三处）：

    LPOP 面  改前 P3 LPOP 17000 ok → LPOP key 10 → has overflowed its stack / SIGABRT
             改后 P3 after LPOP 17000: tiered=false → LPOP 10 ok, 3085 bytes → PASS
    HSCAN 面 P2（20000 字段 hash，HDEL 前缀 17000 → HSCAN key 0 COUNT 10）改前改后均 PASS：
             该入参集在树键序上被打散为短连跑（非前缀连续），未越 ~8000 边；
             改后此面**恒不触发**（重灌零墓碑），与入参形态再无关系。
             前缀连续形态的同面复现见本节残余（P5，ZEXPIREAT 面）。

### 残余（不可走重灌、照实回报项）

成员级 TTL 物理出账面仍在树内逐成员 `tree_del`：三核 `member_expire_arm` /
`member_ttl_probe` / `member_persist_arm`（HEXPIRE/HEXPIREAT/ZEXPIRE… 族，一条客户端命令即可
打下 ≥1 万成员），外加 O(1) 计数臂与周期任务托管的 `collect_expired_members`。
默认 8MiB 栈实测（同一 66000 zset，删除面换成 `ZEXPIREAT z 100 MEMBERS 11200 …` ×5 覆盖
m0..m55999，随后 `ZSCAN z 0 COUNT 10`）：

    P5 after TTL 出账: tiered=true
    thread 'p5_zexpireat_accounting_tombstone_run' has overflowed its stack
    SIGABRT                        ← 改前、改后逐字相同

即 **B 之后墓碑不再「恒低」，只「命令级删除面恒低」**。未强推该面走重灌的理由：出账发生在
HLEN/SCARD/ZCARD 的 O(1) 计数修正臂与后台回收任务上，改走整值重灌会把这些票面明确记载为
O(1) 的臂变成 O(N) 全扫 + 重建（并连带改 `doc/zh/collection.md` 的复杂度契约）。
收敛方向（待裁决，未动手）：在同一次本已 O(N) 的回收扫描里，对活成员计数跌到水位以下者
改调 `promote_collection_to_bftree` 整树重灌，把 TTL 面并到零墓碑形，栈深自变量同样消失。
机理与实测边界已写入
`/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/tiered_collection_ops.rs` 的
`collect_expired_members` 文档与模块头注残余段（探针按「不留临时件」口径已删）。

### 既有测试的写形订正（3 例，改测新写形而非迁就规模）

- `tiered_background_demote.rs`：哈希例更名
  `hash_byte_dim_deadzone_holds_and_low_dims_demote_at_writeback`（原名
  `hash_byte_dim_cold_key_demotes_only_in_background_round`，见 reject 档 §五.3 的对照引用需同步）；
  两例末段改断言「前台写回臂就地降阶（`!is_tiered`、bump 恰一次）+ 后台轮
  `TieredDemoteStats::default()` 零候选零扫树」。
- `tiered_promote_demote_ttl.rs::background_demote_preserves_key_ttl`：削量手段由 HDEL 换成
  按树键序交错的成员级 TTL 出账（唯一还能产出「分层但齐低」键的面），后台降阶断言原样保留。
- `tiered_watch_fence.rs::tiered_set_zset_list_write_arms_invalidate_watch`：SADD 重复成员
  零 bump 断言前移到 SREM 之前（SREM 起键可能已就地降阶，判空臂不再是树内臂）。

### 验收 4 门禁

`CARGO_TARGET_DIR=/tmp/rs-tiered-tomb cargo check --all-targets -p wnode -p wbftree -p wkv`
零错零警告（无 `#[allow(`、无 dead_code 残留）。相关套件实跑全绿：
tiered 全族（`tiered_background_demote` / `tiered_cmds_align` / `tiered_field_ttl` /
`tiered_promote_aof_replay` / `tiered_promote_demote_ttl` / `tiered_watch_fence`）、
`collection_adaptive_tiering`、`hash_ttl`、`object_cold_degrade`、`aof_stored_proc_replay`
共 51 例，加 `resp_sorted_set` / `resp_hash` / `resp_list` / `resp_set` / `resp_slow_path` /
`object_envelope_regression` / `object_cross_type_regression` 共 121 例，0 失败。
全量门禁归主代理。
