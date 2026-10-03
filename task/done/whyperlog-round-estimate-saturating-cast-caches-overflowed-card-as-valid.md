锁定注记（2026-10-01 r10 波主控，基线 `c2557a1`；whyperlog 甄别席候选 + 主控两侧亲验转换语义与缓存有效性，锚以本注记为准，台账禁钉行号）
- 病灶：`wedb/whyperlog/src/estimate.rs::round_estimate` 以 `e.round_ties_even() as i64` 收口估计值出口——
  rust 的 `f64 as i64` 为**饱和**转换（`+inf` 与有限越界皆得 `i64::MAX`）。随后
  `::HyperLogLog::count` 走 `set_card(ptr, e)` 把该值写进载荷字节 8..16，
  `wedb/whyperlog/src/frame.rs::is_valid_card`（`get_card >= 0`）判其**有效**，
  `::HyperLogLog::count` 的缓存短路自此每轮直返该垃圾基数、不再重算。
- C# 权威锚（现树逐字对过）：`garnet/libs/server/Resp/HyperLogLog/HyperLogLog.cs::CountDenseNCEstimator`
  与 `::CountSparse` 两处出口皆为 `return (long)Math.Round(E);`——
  C# 在 unchecked 上下文对越界/`+inf` 的 `double → long` 是**未定义**转换，x64 实践值由 `cvttsd2si` 得
  `long.MinValue`（**负**）；`::Count` 同样 `SetCard(ptr, E)` 写入该负值，`::IsValidCard`（`GetCard >= 0`）
  判失效，下轮仍重算。即 C# 的可观测不变式是「**越界估计绝不被当作合法基数缓存**」。
- 平台注记（诚实申报，勿据此降档）：arm64 的 `fcvtzs` 对越界是**饱和**（得 `long.MaxValue`），
  与 rust 现形同。故本票判据锚在 C# 的**可观测不变式**（缓存恒失效 + 不得把越界当合法基数黏着），
  而非 x64 的具体位型；收口形态取 C# x64 实践值 `i64::MIN`（负 ⇒ `is_valid_card` 恒假 ⇒ 每轮重算，
  与 C# 主战场行为一致），不采「保留饱和 + 额外加缓存失效特判」的第二真值源形态。
- 越界估计的产生面（复核可达，非纸面）：`::HyperLogLog::nc_estimator_from_histogram` 的 rhisto 为
  `[usize; 64]`，寄存器值无条件计入直方图，但估计式只消费 `rhisto[0]`、`rhisto[1..=qbit]`、`rhisto[qbit + 1]`
  ——寄存器值落在 `qbit+2..=63` 死区时**不贡献 z**；C# 同形（`stackalloc int[64]` + 同消费集），非两侧分叉。
  构造全 16384 寄存器恒 63 的稠密载荷 ⇒ `z = 0.0` ⇒ `E = ALPHA * mcnt² / 0.0 = +inf`；
  有限变体（仅少数寄存器落 `qbit+1..`，其余落死区）⇒ `z` 极小 ⇒ `E` 有限越界，同饱和。
- 可达链：客户端 `SET k <稠密载荷>`（`HYLL` 魔数 + type=0x01 + 长度合 `dense_bytes` + 寄存器全 `0xFF`）→
  `PFCOUNT k` → `wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs::hyper_log_log_length`（快臂）/
  `wedb/wnode/src/resp/garnet_api/slow.rs::slow_hll_count`（慢臂）→ `load_hll` → `valid_hyll_payload` →
  `frame.rs::is_valid_hyll`：`is_hyll` 魔数过、`is_valid_hll_length` 稠密臂仅验长度（**不校验寄存器值域**，
  与 C# `IsValidHLLLength` 等形，故过校验本身非分叉）→ `estimate.rs::count` 命中上述链。
  多键 `PFCOUNT k1 k2` 与 `PFMERGE` 混入该源同链（累加器 max 并入保留死区寄存器值，终 `count` 同爆），
  且 PFMERGE dest 既有此载荷时垃圾基数**随值持久落盘**。
- 前案边界（查重已核）：`doc/zh/deviations.md` HLL 在册仅 §108/§132/§135/§144/§145/§16/§17/§18
  （累加器形态、版本/AOF、峰值裕度、dest 预检、分配长、tail-union、TTL 保留），皆不覆盖终级数值转换；
  `task/**` grep（`round_estimate`/`round_ties`/HLL 估计器）无同题票；
  `task/reject/checkjs-ten-files-eighteen-fn-missing-gate-unclosed.md` 只裁 HLL Dump/Compare 的 DEBUG 族缺席，异面；
  `task/done/checkjs-gate-r8-ignore-closure-and-dead-export-retire.md` 与本缝无关。
  C# 测试面 `garnet/test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs` 无畸形寄存器稠密载荷用例
  （其 reject 矩阵走 `IsValidHYLL` 直验），故无既判「维持饱和」的裁决可援引。
- 禁触域（同侪在途）：`wedb/wedb/src/server/replication/**`、`wedb/wkv/src/store/keyspace.rs`、
  `wedb/wcompact/src/compactor/run.rs` 与 `wedb/wkv/src/compact.rs`（另两票在途，禁碰）；
  本票只动 `wedb/whyperlog/src/estimate.rs::round_estimate` 一符与 `wedb/whyperlog/tests/**`、
  `wedb/wnode/tests/hyperloglog.rs` 夹具。

审核结论：通过（2026-10-01 主控亲验立案；P3。触发前提为注入式畸形稠密载荷（自然 `PFADD` 路径恒不可达：
rho 上界受 qbit 约束），非崩溃、非膨胀，属**错值 + 缓存黏着**类；但同一载荷两侧后续行为分叉是持续性的
（rust 把垃圾值钉成权威基数并随 PFMERGE 持久，C# 恒判缓存失效重算），且回包面可观测。定 P3）

HLL 估计器终级 f64→i64 用饱和转换：越界估计被当作合法基数写回缓存并随合并持久，违背 C#「越界估计绝不入缓存」不变式

问题分析：
1. 出口语义错档：`round_estimate` 承担的是 `(long)Math.Round(E)` 这一**未定义转换**的承接位。
   rust 的饱和语义本身没错，错在把它当成 C# 的等价物——C# 的可观测后果是「负 ⇒ `IsValidCard` 假 ⇒ 重算」，
   rust 的后果是「正极大 ⇒ `is_valid_card` 真 ⇒ 短路直返」。同一入口函数在两侧的**缓存有效性裁决**上反向，
   这正是本仓「对标 C# 的形态而非字面」要求要收敛的点。
2. 黏着放大：`count` 的 `set_card` 位置与 C# 同位（不属分叉），但写入值的有效性裁决不同，
   于是 rust 侧一次越界估计会被钉进载荷字节，此后 `PFCOUNT` 恒回该值、`PFMERGE` 把它随值写盘，
   错误从「单次计算产物」升级为「持久化状态」。C# 侧因恒判失效，同一载荷每轮重算，
   错值只出现在回包面而不进入持久账。
3. 收口形态必须单点且不留第二真值源：只在 `round_estimate` 的越界档返回 `i64::MIN`
   （含 `!e.is_finite()`），不动 `count` 的 `set_card` 位置、不动估计式、不动校验面、不加新线协议字段——
   与 C# 「不校验寄存器值域也能自洽」的形态保持一致。若改为在 `is_valid_card` 上加特判，
   等于把同一不变式散到两处，属禁做。

涉及代码：
rust 文件与函数：
wedb/whyperlog/src/estimate.rs::round_estimate（病灶，唯一改动点）
wedb/whyperlog/src/estimate.rs::HyperLogLog::count、::nc_estimator_from_histogram（set_card 短路面与 z 死区消费面，只读证据）
wedb/whyperlog/src/frame.rs::is_valid_card、::get_card、::set_card（缓存有效性裁决，只读证据）
wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs::hyper_log_log_length、
wedb/wnode/src/resp/garnet_api/slow.rs::slow_hll_count（RESP 可达链，只读证据）
对应 c# 文件与函数：
libs/server/Resp/HyperLogLog/HyperLogLog.cs::CountDenseNCEstimator、::CountSparse（(long)Math.Round 两出口）、
::Count（SetCard 同位）、::IsValidCard（GetCard >= 0）、::IsValidHLLLength（不核值域）

精炼执行方案：
1. **单符收口**：`round_estimate` 内先 `let r = e.round_ties_even();`，
   越界即 `!r.is_finite() || r < (i64::MIN as f64) || r >= (i64::MAX as f64)` 时返回 `i64::MIN`，
   否则 `r as i64`（该界内转换无饱和、无歧义；`i64::MAX as f64` 即 2^63，取 `>=` 恰合上界）。
   界一律由 `i64::MIN/MAX as f64` 导出，禁写 `9223372036854775808e18` 一类魔法字面量。
   文档注语写明：C# 出口为 unchecked `(long)` 转换，x64 实践得负值，
   rust 的饱和语义会令 `is_valid_card` 转真、把越界估计钉成合法基数，故本处复刻 C# 的**可观测不变式**
   （越界 ⇒ 负 ⇒ 缓存恒失效 ⇒ 下轮重算），并申报 arm64 `fcvtzs` 饱和的平台差异。
   禁改用 `wrapping_*`/`checked_*` 后 `unwrap_or(0)`（0 会被判有效且回包错值方向不同）。
2. **不动相邻面**：`count` 的 `set_card` 位置与短路形态、`is_valid_card` 判据、估计式、
   `is_valid_hll_length` 值域不校验的既有等形形态，皆一字不动。
3. 禁做项：不给 `is_valid_hyll` 新增寄存器值域校验（那是 C# 也没有的第二真值源，且会把可达面从
   「估计器」搬到「校验器」，改变 reject 矩阵形态）；不引新错误类型（本缝无错误面，纯数值出口）；
   禁 `#[allow]`/`#[expect]`；禁占位实现；禁顺手改 HLL 其余在册分叉（§108/§132/§135/§144/§145 各案勿动）。
4. 锁测（whyperlog 单元面即可，无需起服务）：
   (a) 12304B 稠密载荷（`HYLL` 魔数、type=0x01、寄存器全 `0xFF`）——先断言
       `is_valid_hyll(&blob)` 为真（钉死「过校验非分叉」这一前提），再
       `count(&mut blob)` 后断言**返回值 `< 0`** 且 `get_card(&blob) < 0`（即 `is_valid_card` 恒假）、
       二次 `count` 仍走重算（返回值同负、不因缓存短路而变正）；
   (b) 有限越界变体——单寄存器置 `qbit+1` 附近、其余落死区，使 `E` 有限但越 i64 上界，同断言 `< 0`；
   (c) 正常路径回归——`wedb/whyperlog/tests/hyperloglog.rs` 与 `wedb/wnode/tests/hyperloglog.rs`、
       `hll_alloc_probe.rs` 全绿（自然 PFADD 基数恒不触界，本票改动对其无影响，须由绿证成）。
   revert-proof：把 `round_estimate` 改回裸 `e.round_ties_even() as i64`（撤第 1 步）后
   (a) 的「返回值 `< 0`」与「`get_card < 0`」两断言必红（饱和得 `i64::MAX`、缓存转真）；
   若退化为「返回 0」，(a) 的 `< 0` 断言与「二次 count 重算」断言同红——钉死 i64::MIN 档形态。
5. 登记：本票判据（unchecked 数值转换的承接位须复刻 C# 的**可观测不变式**而非 rust 饱和语义）在
   `doc/zh/deviations.md` 册尾顺编新节登记（先入库者得号、撞号让位不写死；取号前先 grep 册尾），
   锚用 `路径::符号` 形态。
6. 验证面：`cargo check -q -p whyperlog -p wnode --all-targets` 与
   `cargo nextest run -p whyperlog`（全套）与 `cargo nextest run -p wnode --test hyperloglog --test hll_alloc_probe`；
   禁在主树或沙箱跑 `./test.sh`/`./sh/clippy.sh`（波次门禁由主控统一跑）。

---
### 收口与终态记录（2026-10-01 主控闭环归档）
- **修复方案**：`whyperlog/src/estimate.rs::round_estimate` 内越界档（`!r.is_finite() || r < i64::MIN as f64 || r >= i64::MAX as f64`）显式返回 `i64::MIN` 负哨兵，复刻 C# x64 unchecked (long) 转换下的可观测不变式（越界 ⇒ 负 ⇒ 缓存恒失效 ⇒ 下轮重算），消除 rust 饱和转换将溢出估计钉成合法缓存基数的漏洞。
- **锁测验证**：`whyperlog/tests/hyperloglog.rs` 增设注入式全死区稠密载荷锁测，断言 `count` 返回负哨兵且缓存恒失效、二次 `count` 必重算且结果一致；增设单寄存器存活的有限越界锁测；单测 21/21 绿；`wnode` HLL 测试集全绿；revert-proof 验证完成。
- **台账登记**：在 `doc/zh/deviations.md` 顺编登记 `[§197]`。
- **工单归档**：主分支已合入，工单移动至 `task/done/`。
