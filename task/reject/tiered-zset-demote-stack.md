# 分层 zset 后台降阶链 bf-tree 递归栈溢出（认领票 task/ing/tiered-zset-demote-bf-tree-recursion-stack-overflow.md）

裁决：**问题成立、票面修法不成立**（2026-09-19 fixloop 子代理实证；分支
`fix-tiered-zset-demote-stack` 零代码改动——递归点 100% 在外部 crate bf-tree 0.5.6 内部，
本仓公开 API 无一可界定栈深度，详见第四节）。

## 一、递归点实证（修法要求 1）

lldb 停在守卫页（`process handle SIGSEGV --stop true --pass false`），负载 = 降阶用例末段同型：
66000 成员 ZADD 升阶 → ZREM 56000 → `tiered_demote_round`，执行线程栈 8MiB。
溢出线程（probe 线程）总帧 **12381**，逐帧归并：

| 帧数 | 帧 |
| --- | --- |
| 12232 | `<bf_tree::range_scan::ScanIter>::next` at `range_scan.rs:320` |
| 104 | `<bf_tree::range_scan::ScanIter>::next` at `range_scan.rs:379` |
| 1 | `<bf_tree::range_scan::ScanLock>::get_record_by_pos_with_bound` at `range_scan.rs:63` |
| 1 | `<bf_tree::mini_page_op::LeafEntrySLocked as LeafOperations>::scan_record_by_pos_with_bound` |
| 1 | `<wbftree::service::BfTreeService>::drain_scan_iter::<...tiered_materialize_blob 闭包>>` |
| 1 | `<wbftree::service::BfTreeService>::scan_callback::<...同上>>` |

8MiB ÷ 12381 ≈ **680 B/帧**。递归点是 bf-tree 自身两处尾递归：
`bf-tree-0.5.6/src/range_scan.rs:318-321`（`GetScanRecordByPosResult::Deleted` 臂
`self.next(out_buffer)`）与 `:327-379`（`EndOfLeaf` 臂同型）。rustc 不做 TCO ⇒
**每跳过一条墓碑记录压一帧**，`EndOfLeaf` 每跨一页再压一帧（104 帧 = 104 次跨页）。

- 票面「崩溃帧 = `scan_record_by_pos_with_bound`」= 该函数只是溢出瞬间正在执行的最内层
  叶子帧，本身不递归（`mini_page_op.rs:102-133` 单层 match）。
- 票面「可疑链 `tiered_materialize_blob` → `demote_target` `from_blob` → `obj_save`
  → `handle_bftree_drain_and_delete`」= **不成立**：本仓该四段零自调
  （`wbftree/src/service/ops.rs:290-327` `drain_scan_iter` 是 `while let` 循环、整栈只出现
  1 次；`wkv/src/range_index/stub.rs:67`、`wkv/src/session/collection.rs:218-229` 同）。
  本仓 `wbftree` 包装层不是自研递归封装。

规模分档（同型负载，最小可过栈）：

| 树记录数 | 存活 | 墓碑连跑 | 8MiB | 16MiB | 32MiB | 64MiB | 128MiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 66000 | 10000 | ~12k | 炸 | 炸 | 过 | 过 | — |
| 132000 | 10000 | ~2× | 炸 | — | 炸 | 炸 | — |
| 264000 | 10000 | ~4× | 炸 | — | — | 炸 | 炸 |

**决定性对照**：同 66000 记录，把删除集改成「按树键序逐条交错删」（存活 32768、任意墓碑连跑
≤2）→ **8MiB 默认栈直接通过**（`demoted: 1`）。⇒ 栈自变量是**连续墓碑条数**，不是记录数 N；
降阶链其余各段（32768 条 `from_blob` 解码、`obj_save` 写回、树清退）在默认栈下深度有界。
即「本仓侧降阶链调用形态与 N 无关」这一可断言事实由该对照给出（票面验收第 3 条）。

## 二、票面修法（分段 / 迭代式访问）实测不可达（修法要求 3）

bf-tree 对外只有 4 个扫描入口（`src/tree.rs:1302 / 1328 / 1369 / 1399`），返回的都是同一个
`ScanIter` / `ScanIterMut`，`next()` 同型尾递归；`ScanIterMut::next`（`range_scan.rs:155-158
/ 218`）逐字同构，换 `scan_mut_*` 无收益。crates.io 实测 0.5.6 已是最新版（0.5.5/0.5.4/…
更早），**无上游修复版可升**。

在 `wbftree` 侧对 bf-tree 公开 API 做的四组栈档实验（40000 定宽键建树、前 30000 键连删形成
30000 长墓碑连跑，扫描在独立线程执行、栈大小由 `PROBE_MB` 控制）：

| # | 调用形态 | 返回条数 | 1MiB | 8MiB | 32MiB |
| --- | --- | --- | --- | --- | --- |
| S1 | `scan_with_count(k0, usize::MAX)`（现形态全扫） | 10000 | 炸 | 炸 | 过 |
| S2 | `scan_with_end_key(k0, k000050)` 紧窗口（窗口内**零存活**） | 0 | 炸 | 炸 | 过 |
| S3 | 全区间切 200 键/段、逐段独立迭代器（分段扫描） | 10000 | 炸 | 炸 | 过 |
| S4 | `scan_with_count(k0, 10)`（限数扫描，HSCAN/ZSCAN 形态） | 10 | 炸 | 炸 | 过 |

S2/S3 直接否掉「分段访问界定深度」，机理在源码里是硬的：
`bf-tree-0.5.6/src/nodes/leaf_node.rs:1903-1911` —— `meta.op_type().is_absent()` 的墓碑判定排在
`bound_key` 比较**之前**并直接 `return Deleted`，故**上界键对墓碑连跑不生效**；
`range_scan.rs:308` + `:322-324` —— `scan_cnt` 只在 `Found` 分支递减，故 **count 只界定返回条数、
不界定遍历条数**。两条外部可界定深度的杠杆同时失效，能界定深度的唯一写法是 `next()` 自身 loop 化。

## 三、影响面比降阶链更宽（须登记的残留风险）

同一扫描漏斗共 18 个调用面（`wnode/src/resp/objects/tiered_collection_ops.rs:359, 794, 815, 835,
1171, 1202, 1765, 1943, 1983, 2002, 2074, 2091, 2106, 2137, 2250` 与 `wkv/src/ri.rs:122, 140,
158`），其中面向客户端的三处同险且更易触发：
`exec_tiered_scan`（`tiered_collection_ops.rs:2194`，扫描体 `:2250`，即分层态
HSCAN/SSCAN/ZSCAN/LREM 负索引族的 COUNT 臂）、`tiered_list_arm` 的 LPOP/RPOP 有界取臂
（`:1941-1948`，LPOP 臂起点 `&[0u8]` 即从树最左起跳，头端历次弹出留下的墓碑连跑全在游标之后）、
`list_head_seq` 的取最左键臂（`:1763-1765`）。
相关注释对「count 界定遍历」的错误预设要一并纠正：`:1735`「引擎侧 scan_cnt=1 截断」、
`:1936-1938`「引擎侧 scan_cnt=n 截断 + 回调满 n 早停，恒 O(count)」——S4 实测
`scan_with_count(k0, 10)` 只返回 10 条却仍吃满 >8MiB 栈（遍历条数不受 count 约束）。
深递归发生在**单次 `next()` 调用内部**：游标之后那一段墓碑连跑必须在一帧套一帧里跳完，
回调早停与 count 都来不及生效。
⇒ 对一个「删过前缀连续成员后仍存活」的分层集合，游标落在该连跑之前的
一条 `HSCAN key 0 COUNT 10` 或 `LPOP key 10` 即可让整进程 SIGABRT（深度自变量 =
游标之后的连续墓碑条数，与 COUNT 无关），这是比本票降阶面更严重的可用性缺口，
同根因、同不可达结论。
附带：`tiered_demote.rs:112-122`（`collect_demote_candidates` 文档「评估轮成本有界」）与
`DEMOTE_MAX_KEYS_PER_ROUND` 的限批论证只覆盖 I/O 次数、不覆盖栈深度，若走登记路线需同步改写。

## 四、可落地修法（越界项，交主代理裁决，本票未动手）

1. **唯一真修**：把 `ScanIter::next` 与 `ScanIterMut::next` 的两处尾递归改 loop
   （上游 10 行 × 2 的形态，语义零变化——两处都是「跳过墓碑/换页后继续」的纯尾递归；
   `range_scan.rs:303` 上源自带注释 `// Here we need to busy loop? Is that safe?` 即此点，
   上游 PR 有据可依）：
   ```rust
   pub fn next(&mut self, out_buffer: &mut [u8]) -> Option<(usize, usize)> {
     loop {
       if self.scan_cnt == 0 && self.end_key.is_none() { return None; }
       match self.leaf_lock.get_record_by_pos_with_bound(
         &self.scan_position, out_buffer, self.return_field, &self.end_key) {
         GetScanRecordByPosResult::Deleted => { self.scan_position.move_to_next(); }
         GetScanRecordByPosResult::Found(k, v) => {
           self.scan_position.move_to_next(); self.scan_cnt -= 1;
           return Some((k as usize, v as usize));
         }
         GetScanRecordByPosResult::EndOfLeaf => { /* 取右邻、换 cursor */ continue; }
         GetScanRecordByPosResult::BoundKeyExceeded => { self.scan_cnt = 0; return None; }
       }
     }
   }
   ```
   本仓落不了地的原因：Rust 无法在不持有该 crate 源码的前提下改第三方 crate 的固有方法，
   `[patch.crates-io]` + 全量 vendor 一个微软 MIT crate 与「不引入绑定微软平台代码」的取向冲突，
   且 fork 面波及全仓 `wbftree` 依赖——属主代理决策（或先向上游提 issue，本票证据可直接附）。
2. **若不动依赖**：本票转「裁剪边界登记」——分层集合的批量删除需在越界后走既有
   `obj_writeback_tiered`（`wnode/src/resp/objects/rmw_helpers.rs:431`）重灌臂
   （经 `apply_rmw_post_operate` 汇入 `promote_collection_to_bftree` →
   `wkv/src/range_index/stub.rs:142` `bulk_load` 重建，零墓碑）而非扫描侧改形；8MiB 默认栈的
   实测边界约「连续墓碑 ≤ ~8000 条」（680 B/帧，取 2× 余量）。代价：重灌是 O(存活数) 扫树，
   且 C# 侧对象整值常驻内存、删即就地改内存对象（`garnet/libs/server/Storage/Functions/
   ObjectStore/RMWMethods.cs:188-215` `PostCopyUpdater` → `value.Operate` → `HasRemoveKey`
   → `ExpireAndStop`），无「按成员分记录」的树、亦无墓碑连跑，属新机制 ⇒ 与
   「不引入 C# 没有的新机制」冲突，本票未自行落地。

## 五、票面 4 条主张逐条裁决

1. 「分层 zset 大集后台降阶链在 debug 默认 8MB 线程栈下 SIGABRT 栈溢出」——**成立**（第一节表）。
   但当前 dev 上该用例走不到末段：`wnode/tests/tiered_background_demote.rs:260`「条目数越 65536
   应升阶」先红（`ZADD key 1 m0 m1 …` 单分值多成员形态未支持），属
   `task/ing/qw13-red-tiered-watch-version-fence.md` 条 3 的改动域，非本票修法。本票证据用等价
   负载（`ZADD key 1 m0 1 m1 …` 成对形态，同 66000 成员 / 同 ZREM 56000）复现同一溢出形态
   （默认栈与 16MiB 炸、32MiB 过）；票面「16MB/32MB 仍溢出、64MB 通过」的具体档位数字未重现，
   且其「每记录 3.2-6.4KB 栈」换算不成立（见第一节 680 B/帧与规模表）。
2. 「是 O(N) 递归而非用例入参问题」——**成立但需修正自变量**：是 O(连续墓碑条数) 递归，
   与记录数 N 无直接关系（第一节决定性对照）。用例入参（10000 存活）本身没问题，
   但「56000 连续删除」这一入参形态是触发条件，非规模门槛。
3. 「同尺寸列表族在默认栈下可通过」——**票面表述不成立、结论方向成立**。本仓无该对照：
   `wnode/tests/tiered_background_demote.rs` 只有哈希体积维（`:116`）与 zset 条目维（`:238`）
   两个用例，无列表族降阶用例；全仓 `65540` 只出现在 `wnode/tests/resp_blocking_commands.rs`
   （阻塞命令面，与降阶无关）。真正的同文件对照是哈希体积维用例：其 `HDEL` 集按
   `f0..f5999` 生成，在树键序上恰是**前缀连续 6000 条墓碑** + 2000 存活，本次实测在默认栈
   （未设 `RUST_MIN_STACK`）下 `PASS [0.212s]`（命令见文末）。6000 连跑 < 第一节
   8MiB 边界的 ~8000 ⇒ 通过，与「栈自变量 = 连续墓碑条数」完全自洽；
   与「家族」无关（同 66000 记录、连跑 ≤2 的交错删除对照在 8MiB 下即通过，第一节决定性对照）。
4. 「缺口定位在 zset 降阶链」——**不成立**：缺口在 bf-tree 扫描面，zset 降阶链只是第一个被发现的
   消费者；用户可见 `HSCAN/SSCAN/ZSCAN` 的 COUNT 面同险且更易触发（第三节）。

## 票面原文存档（复核前，2026-09-19 由 tiered 修红代理上报）

> 触发用例：`wnode::tiered_background_demote::zset_count_dim_deadzone_excluded_and_cold_key_demoted`
> 末段（10000 成员后台降阶轮）。栈档分档实测：16MB/32MB 仍溢出、64MB 通过、256MB 全测通过，
> 即每条记录约 3.2-6.4KB 栈增长，规模线性 ⇒ 递归。
> - 崩溃帧：lldb 指到 `bf_tree::mini_page_op::LeafEntrySLocked::scan_record_by_pos_with_bound`
>   （外部 crate bf-tree 0.5.6，本仓由 `wbftree` 包一层）。
> - 可疑链：`tiered_materialize_blob`（10000 条）→ `demote_target` 的 `from_blob` 回验 → `obj_save`
>   → `handle_bftree_drain_and_delete`（此前经 56000 条墓碑的重删）。
> - 对照组：列表族 65540 条同尺寸「扫描 + drain + 重灌」默认栈通过。

逐条裁决见第五节；「每条记录约 3.2-6.4KB 栈增长」的换算不成立（真实值 ≈680 B/帧 ×
连续墓碑条数，与记录数无关），「16MB/32MB 仍溢出、64MB 通过」亦未在复现中重现
（本侧复现：66000/10000 为 16MB 炸、32MB 过）。

## 复现命令

探针文件 `wnode/tests/zz_probe_stack.rs`（第一节：降阶链栈档分档 + 交错删除对照）与
`wbftree/tests/zz_probe_scan.rs`（第二节 S1..S4）按本票「不留临时件」口径已删、未提交，
下列命令为其入参；重跑需按入参重建。

```sh
# 同文件哈希对照：树键序前缀 6000 连跑墓碑 + 2000 存活，默认栈实跑 PASS [0.212s]
# 注（2026-09-19 B 落地后）：该用例已更名并改测新写形，见文末「六、后续订正」。
cd <worktree>/wedb && CARGO_TARGET_DIR=/tmp/rs-tiered-stack cargo nextest run -p wnode \
  --test tiered_background_demote hash_byte_dim_cold_key_demotes_only_in_background_round --no-fail-fast

# 降阶链栈档分档 + 交错删除决定性对照（探针入参：总记录数 / 存活数 / 线程栈 MiB / 是否交错删）
CARGO_TARGET_DIR=/tmp/rs-tiered-stack cargo build --tests -p wnode --test zz_probe_stack
PROBE_TOTAL=66000 PROBE_LIVE=10000 PROBE_STACK_MB=8 <bin> --exact probe_zset_demote_stack --nocapture   # 炸
PROBE_TOTAL=66000 PROBE_LIVE=32768 PROBE_STACK_MB=8 PROBE_ALTERNATE=1 <bin> --exact probe_zset_demote_stack --nocapture  # 过
# bin 路径由 `cargo test --no-run --message-format=json` 解析（debug/build/<pkg>/<hash>/out/…）

# 溢出瞬间逐帧归并
PROBE_TOTAL=66000 PROBE_LIVE=10000 PROBE_STACK_MB=8 lldb -b \
  -o "process handle SIGSEGV --stop true --pass false --notify false" \
  -o "run --exact probe_zset_demote_stack" -o "bt all" -o quit <bin>

# S1..S4（wbftree 侧对 bf-tree 公开 API 的栈档实验；40000 定宽键建树、前 30000 连删）
PROBE_MB=8 <wbftree 测试 bin> --exact s2_tight_window_in_tombstone_run --nocapture   # 炸，返回 0 条
```

## 六、后续订正（B 落地后，分支 `fix-tiered-tombstone-density` @ `df35b160`）

1. **§五.3 的同文件对照用例名与写形均已失效**。`wnode/tests/tiered_background_demote.rs` 的
   哈希体积维用例已更名 `hash_byte_dim_deadzone_holds_and_low_dims_demote_at_writeback`：
   HDEL 自 B 起摘出分层快速通道，改走「物化 + 整值写回」面，那 6000 条前缀墓碑**根本不再产生**，
   「6000 < ~8000 故通过」的档位论证已无从复现（键在写回时即就地降阶，后台轮零候选）。
   本档 §五.3 的裁决结论（自变量是连续墓碑条数、与家族无关）在 B 落地前成立、未被推翻，
   其**证据用例**须按上述新名重跑，且只在新墓碑仍由 TTL 面产生时才有档位意义。
2. **本档第三节登记的面向客户端残留风险，B 只关掉命令级删除面**。
   成员级 TTL 物理出账（`member_expire_arm` 三核 + `collect_expired_members`）仍在树内逐成员
   `tree_del`，默认 8MiB 栈实测仍可 SIGABRT，形态与本档第一节一致：
   同一 66000 成员成对形态 zset，`ZEXPIREAT z 100 MEMBERS 11200 …` ×5 覆盖 m0..m55999，
   随后一条 `ZSCAN z 0 COUNT 10`：

       P5 after TTL 出账: tiered=true
       thread 'p5_zexpireat_accounting_tombstone_run' has overflowed its stack
       fatal runtime error: stack overflow, aborting   ← 改前 / 改后逐字相同

   出账集按树键序交错（任意连跑 ≤3）时同一面在默认栈下通过（`HSCAN key 0 COUNT 10` ok），
   与本档「深度只随连跑长度增长」的机理再次自洽。
   该残余的处置与不收口理由（HLEN/SCARD/ZCARD 的 O(1) 计数臂不宜改成 O(N) 重灌）
   记在 `task/ing/tiered-zset-demote-bf-tree-recursion-stack-overflow.md`
   「二棒落地记账 · 残余」与代码 `tiered_collection_ops.rs::collect_expired_members` 文档，
   待主代理裁决；探针（`wnode/tests/zz_probe_tombstone.rs`）按本仓「不留临时件」口径已删、未提交。
3. 反向确认：本节 §一/§二 的 bf-tree 侧实验（S1..S4，`wbftree/tests/zz_probe_scan.rs`）不受 B 影响，
   递归点在外部 crate 内部这一事实不变；B 是把**本仓产生墓碑的速率**压到零，不是改递归。
