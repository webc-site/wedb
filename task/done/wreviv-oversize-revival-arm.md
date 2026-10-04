> 收口（2026-09-28 13:1x）：席 `19d1080`（8 文件 +492/−37，与申报全等），合入 `8c03493`。
> 主代理亲验面：单提交、无越界档、零 Cargo.toml 变更、零 `#[allow]`；`best_size`（初值 `u32::MAX`）
> 与 `best_idx`/`best_raw` 同处赋值、精确匹配臂补写无漏；`purge_below`/`is_empty`/`clear` 三面纳臂；
> `take` 早退守卫改形（仅 `required_size == 0` 早退）后越界尺寸带转 `take_oversize`、末桶落空
> 续探一档两臂互斥不重复计数；复读口 `record_block_size` 的 `read_page` 嵌套死锁面按唯一池取调用点
> `wkv/src/session/raw/mod.rs:199`（先于 `revivify_record_at`，闭包探链已出界）实测无页闩持有。
> check.js 合入后复跑 EXIT=0，本席零新簇（席自纠 `FreeRecordPool.cs:TryTake`/`:TryTakeOversize`
> 两簇重复锚后单点化在案）。
> **门禁延后**：dev tip 现红于并发席面 —— `wresp/src/cmd_strings.rs:536` E0599（`SmallVec::as_str`，
> 其未提交脏档改 `as_str_safe` 即修复面；另 `task/issue/chain-upgrade-nightly-0928-waof-const-try-compile-break.md`
> 立案 nightly-2026-09-28 升链的 `waof` const_try 32 错），孤立探针 worktree `/tmp/probe-chain`
> 实测 `cargo check --workspace --all-targets` EXIT=101（日志 `/tmp/_rs/chain_probe.log`）。
> 本席合入面与之零交集，按禁跨域代修不代修；`./test.sh`/`./sh/clippy.sh` 待链复绿后统一跑。
> 席自报遗留（非本票射程）：`js/check/ignore/storage.yml` 的 `GetRecordSize`/`TryTakeOversize`
> 「可淘汰 ignore」待 `--prune-ignore` 收口；>64KB 死槽复活缺 RESP 端到端 harness，暂由
> `oversize_arm_put_take_pairing`/`oversize_arm_big_value_churn_revival` 两单测 +
> `large_value_e2e` 页容量断言分面覆盖。
> 四处刻意分叉（复读口只读常驻页较 C# `ReadFromDisk` 保守、CAS 败换候选沿本池单次 CAS 既定裁决、
> 入臂满则弃单桶直投、末档续探一档）均在代码文档自证；`doc/zh/deviations.md` 台账本仓缺位不另登。
>
审核结论：通过，升 P2（补码案）

分席可达性坐实（供 fix 直接消费）：
- 页尺寸：whlog/src/config.rs:9 库级默认 64KB、:11 生产默认 16MB；wkv/src/config.rs:324 规划器钳 [64KB,16MB]，生产 ≥1GB 预算推导 16MB 页、单页内联承载 1MB 级大值（wedb/tests/large_value_e2e.rs:59-65 断言 2MB 页内联 1MB 值）。
- 大值路由：无独立对象日志，ObjectEnvelope 整包内联主存（whlog/src/hlog/mod.rs:765 自证），恒 ≤65535 不成立。
- reviv 启用：wkv/src/config.rs:397 默认 false，但 wnode/src/service.rs:711 生产接线可达。
- 故 (65535B, 页容量] 内联记录可达，oversize 复活臂缺失为实质假桩、跨键死槽不可复活，触板块 3.2；whlog:149/765 的「无 oversized」仅论证跨页形，与单页内 oversize 臂非同概念；deviations 无 oversize/65535 在册，非重报、非红线4。

优化执行方案：接最小 oversize 复活臂（put 仅存地址、take 复读 header.total_size 校验纳入），并同步 lib.rs:23 委托对象。验证点：reviv 开启 + ≥2MB 页的 big-value churn 对拍，补登 oversize 锚词后 check.js 复零。

reviv 省略 oversize 复活臂并把腾挪委托给 whlog，但 whlog 显式声明无该路径，委托落空致大值死槽不可复活

问题分析：
1. Garnet 契约对齐（C# 原型行为）
C# FreeRecordPool 对超内联尺寸（> kSizeBits 可容的 65535B、但仍单页可纳）的记录专设 oversize 复活臂：入池只存地址，取池时经 GetRecordSize 复读记录头真实尺寸后再判纳（garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:TryPeek oversize 分支 :72/:86、TryTakeOversize :158、TryTake oversize:315/325-326）。注意此为单页内 oversize，非跨页记录。

2. 工程现状确证（Rust 实现路径）
rust 明确省略 oversize 分桶（wedb/wreviv/src/lib.rs:23「oversize 分桶省略：16 位内联尺寸上限 65535B，超限记录的腾挪属 wedb_hlog 层职责」、record.rs:23/:45 MAX_INLINE_SIZE=65535、find_bin_index 超 65535 返回 None 即 drop）。
被委托方 wedb/whlog 却声明无该路径：whlog/src/hlog/mod.rs:149 与 :765 称「无跨页 oversized 记录路径，单页容量即单记录硬上限，本版本 C# 同样没有」——此处 whlog 谈的是跨页 oversized，与 C# reviv 的单页内 oversize 复活臂并非同一概念。全仓 grep 无 oversize 复活的实际承接实现（put/take 侧皆无复读头尺寸臂）。

3. 逻辑危害确证
尺寸落在 (65535B, 单页容量] 的主存记录，其失效死槽在 rust 侧既不入 reviv 池（省略 oversize），又无 whlog 承接（委托目标不存在，且 whlog 的「无 oversized」论证只覆盖跨页形）——这类大值增删 churn 出的死槽跨键永不可复活，只能等整页紧缩，构成日志物理空洞与无界膨胀压力，触 task/review.md 板块 3.2「槽位复活与日志空洞收敛」。属「委托未兑现」的假桩式缺口，非纯性能。
可达性须分诊坐实：若主存记录尺寸上限本就 ≤65535B（页内联尺寸封顶更低或大值全走独立对象日志不进主存记录），则该臂不可达，应降级为登记裁决而非补码。
lib.rs:23 仅登记了「省略 oversize 分桶」这一收形，未登记「委托 whlog 而 whlog 无对应实现」之失配，故本缺口非重报在册偏差。
严重度 P3（若主存可产 (65535B,页容量] 内联记录则升 P2）。

涉及代码：
rust 文件与函数：
wedb/wreviv/src/lib.rs 模块头 oversize 省略声明（:23）
wedb/wreviv/src/record.rs:FreeRecord::validate / MAX_INLINE_SIZE（:23/:45）
wedb/whlog/src/hlog/mod.rs 跨页 oversized 缺席声明（:149/:765）

对应 c# 文件与函数：
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:TryTakeOversize(:158) / TryPeek(:72/:86) / TryTake(:315)

精炼执行方案：
1. 分诊先确证主存是否可产生 65535B 以上、单页以内的内联记录（读 wrecord/whlog 页与记录尺寸上限、大值存储路由）。
2. 若可达：接最小 oversize 复活臂（put 仅存地址、take 复读 header.total_size 校验），或在 lib.rs:23 更正委托对象为实现该臂的确切层。
3. 若不可达：撤「委托 whlog 层」这句失配表述，改在 doc/zh/deviations.md 明登「rust 主存记录恒 ≤65535B，oversize 复活臂无必要」的裁决与依据，销掉假委托。
测试验证点：big-value churn 对拍或补登 oversize 锚词后 check.js 判定复零。
