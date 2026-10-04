锁定注记（2026-10-01 r9 波主控，基线 1e42558；wkv gc 只读甄别席候选 + 主控现码复算订级）：
- 主控亲验属实项：`wedb/wkv/src/gc/compact.rs::try_compact` 内 Shift 分支确在档位归一 `match` 之前 `return`，
  且分支内只取 `shift_n = n.min(max - 1)`、**不检 `boosting`**；归一臂注释自陈「None + 熔断：旁路档归一为 Lookup」，
  与 `gc/mod.rs` 门面头注「超高水位时旁路 None 短路并全速回退紧缩（以 Lookup 活性校验档执行）」、
  `config.rs::gc_dead_high_watermark` 文档「常规 None 档亦旁路短路以 Lookup 活性校验档推进」三处承诺构成分叉。
- **定级由席报 P2 订正为 P3，危害口径同步订正**（主控复算算式，非席报口径）：
  `n = if boosting { max } else { 1 }`，`shift_until = safe_ro - seg * (max - shift_n)`；
  常规档 `shift_n = 1` → `shift_until = safe_ro - (max-1)·seg`，熔断档 `shift_n = max-1` → `safe_ro - 1·seg`。
  即两档终点相差**恰一段（seg 字节）**，席报「由常规档回退 1 段被放大为移位到距安全只读线仅一段、
  整段定稿区活键无条件出局」系误读——常规 Shift 档本就一次性把 begin 推到 `safe_ro-(max-1)·seg`，
  其「移位越过活记录即丢失」在码注释里已自陈为用户自选档的既定语义。故本票真实缺陷是
  **安全机制的三处自我立法失效 + 熔断态多出的一段未判活移位**，不是新增的大规模丢数据面。
- 可达性复核：`GcConfig::default()` 为 `compaction_type = None`、`enabled = false`，但
  `gc/reclaim.rs` 头注与 `reclaim_when_scan_idle` 明写「物理回收恒被推进、不随 `gc.enabled` 关停」，
  故触发只需一次合法 `CONFIG SET compaction-type shift` + 死亡账本越 `gc_dead_high_watermark`（默认 1024），
  无崩溃/无竞态前提。定级据此维持 P3（配置面窄但全合法，危害随段大小有界）。
- 前案边界（查重已核）：`done/wcompact-dead-candidate-fast-lane-on-dropped-none-blindspot` 射程为
  Lookup/Scan 档 `on_dropped` 复查窗，不覆档位分叉；五池 grep `熔断/boost/watermark/shift_begin` 无同票；
  deviations 仅 §14 与 group-commit 水位两处他域条目。
- 禁触域（同侪在途）：`wedb/wedb/src/server/replication/**`、`wedb/wnode/src/resp/**`、
  `wedb/wkv/src/session/**`（同波 `wkv-vdb-generation-swap-slow-path-blind-alloc-...` 票面在改）；
  本票只动 `wedb/wkv/src/gc/compact.rs` 一处分支与 `gc/mod.rs`、`config.rs` 的口径注释、其 tests/。

审核结论：通过（2026-10-01 主控亲验立案；P3，方案取「改码对齐三处文档承诺」而非「改文档迁就实现」——
  该机制是仓内自设保守立法，失效侧是实现；席报方案 1 的 `!boosting` 门形与既有归一臂天然衔接，零新档型）

高低水位熔断在 compaction-type=shift 档旁落活性校验归一臂，安全立法三处承诺失效并多移一段未判活区

问题分析：
1. Garnet 契约对齐：C# `garnet/libs/server/Databases/DatabaseManagerBase.cs:DoCompactionAsync`（:425-470，Shift 臂 :444-446）
   无熔断概念（C# 无虚库换号 GC），调用点字面量恒 `numSegmentsToCompact = 1`（:379/:433）；
   熔断系 wedb 自研安全机制，其「以活性校验档推进」的保守性由 rust 三面文档自行钉死，属 rust 必须自洽的立法面，
   非 C# 偏离豁免面。
2. 工程现状：`wedb/wkv/src/gc/compact.rs::try_compact` 先 `let boosting = self.refresh_compact_boost(...)`、
   再以 `if !boosting && cfg.compaction_type == None { return }` 旁路短路，随后
   `let n = if boosting { max } else { 1 };` 求回退段数；紧接的 Shift 分支
   `if cfg.compaction_type == LogCompactionType::Shift { let shift_n = n.min(max - 1); ... shift_begin_address(shift_until); return }`
   **先于**档位归一的 `match cfg.compaction_type { Scan => ..., _ => CompactionType::Lookup }` 返回，
   且分支体不检 `boosting`。于是 `compaction_type=Shift` 且熔断加速态下，紧缩恒走「不搬记录、不判活」的移位臂，
   归一臂注释所称「None + 熔断：旁路档归一为 Lookup（活性校验，见方法文档）」在该档形同虚设。
3. 逻辑危害确证：RESP 最小复现——`CONFIG SET compaction-type shift` 后以 cron 型 `SELECT n` / `SET t i` / `FLUSHDB`
   循环使 `gc_dead.len()` 越 `gc_dead_high_watermark`（默认 1024，`db_gc_reclaim_delay_secs` 默认 86400 下账本只增不减），
   常驻回收轮（`gc/reclaim.rs::reclaim_when_scan_idle`，200ms 兜底驱动，`gc.enabled=false` 亦驱动）即进入 boosting：
   本轮 `shift_until` 由常规档的 `safe_ro-(max-1)·seg` 抬到 `safe_ro-seg`，多出的那一段定稿区内他租户已确认活键
   无条件被 begin 越过（Lookup 档同场景会判活搬运），且 `GcStatsSnapshot::compact_boosting` 只报「加速中」
   不报档位分叉，运维侧不可见。危害随单段字节数有界，定 P3。

涉及代码：
rust 文件与函数：
wedb/wkv/src/gc/compact.rs:try_compact（Shift 分支先置且不检 boosting，档位归一臂被越过）
wedb/wkv/src/gc/mod.rs（门面头注「以 Lookup 活性校验档执行」承诺面）
wedb/wkv/src/config.rs:GcConfig/gc_dead_high_watermark（水位文档与默认值，承诺第三面）
wedb/wkv/src/gc/reclaim.rs:reclaim_when_scan_idle（不随 enabled 关停的常驻驱动，可达性证据）

对应 c# 文件与函数：
libs/server/Databases/DatabaseManagerBase.cs:DoCompactionAsync（:425-470；对照 C# 恒 1 段无放大臂，:379）

精炼执行方案：
1. 档位归一收口到单机制：Shift 分支加 `!boosting` 门（`if !boosting && cfg.compaction_type == LogCompactionType::Shift { … }`），
   熔断态自然下沉既有 `match` 的 `_ => CompactionType::Lookup` 归一臂——与三面文档承诺逐字对齐，**零新档型、零新旋钮**。
2. 顺带订正 Shift 分支「直至低安全线一段即杜绝移位越过活记录」的措辞（常规档 Shift 本就是用户自选的越过活记录档，
   防后人据误读再立第二机制）；`gc/mod.rs` 与 `config.rs` 两处承诺文本若与本步终案口径不符，按终案同步订正，禁留三说。
3. 禁做项：不改 `refresh_compact_boost` 迟滞算式（`>hi` 置位 / `<=lo` 清位 / `lo.min(hi)` 钳倒挂 / `hi==0` 关闭，
   主控复核与 `config.rs` 文档逐字一致，属阴性面）；不改阈值式 `safe_ro - begin > max×seg`（与 C# :433 同构、
   上界单源 `safe_ro` 的立法在册）；不给熔断新增配置项；禁把 Shift 档整体退役（用户可选档，非本票射程）。
4. 锁测：`wedb/wkv/tests/gc.rs` 既有 `test_gc_dead_watermark_circuit_breaker`（hi=4/lo=2 三态）同册追加场景——
   `compaction_type=Shift` + 注入 5 条 `gc_dead`（`vdb.gc_dead.insert`，`tail_address` 取低于现 begin 使注销门不拦）
   + 定稿积压越阈 → 手动驱动 `run_once`/`reclaim_when_scan_idle` → 断言他活域预写键仍在、
   `compact_boosting=true` 且本轮走 Lookup 核（`begin` 推进后不越过 `safe_ro - max×seg` 之外的活区）。
   revert-proof：撤 `!boosting` 门后「他活域键仍在」断言必转红。
5. 验证面：`cargo check -q -p wkv --all-targets` 与 `cargo nextest run -p wkv --test gc`（含既有三态水位用例回归）；
   禁在主树跑 `./test.sh`/`./sh/clippy.sh`。

---

## 收口记录（2026-10-01 r9 波主控，反证式审计）
- **合入哈希**：`76a1766`（--no-ff 并 `fix-wkv-gc-breaker-shift` @ `ff29c9e`，基线 dev `a730b3e`）
- **落地面**：`wkv/src/gc/compact.rs::try_compact`（Shift 臂前置 `!boosting` 门，熔断态自然下沉既有
  `_ => CompactionType::Lookup` 归一臂）、`wkv/src/gc/mod.rs`（门面头注 + `GcStatsSnapshot::compact_boosting` 文本）、
  `wkv/src/config.rs::GcConfig`（Shift 条目与 `gc_dead_high_watermark` 文档，三面承诺同步）、
  `wkv/tests/gc.rs::test_gc_shift_boost_normalizes_to_lookup`（+65/0）。numstat 与席面申报逐字节相符，零越面。
- **主控独立复核**：沙箱 `.forks/fix-wkv-gc-breaker-shift` 原样复跑 gc 册 20/20 绿；
  反证（perl 撤 `!boosting && `）后**仅**新用例转红，其余 19 绿——
  `gc.rs:1118` `left: None / right: Some([118])`，即他活域 marker 被未判活移位越过，缺陷面恰为熔断×Shift 交叉；
  `git checkout --` 还原后复验 20/20 绿。`refresh_compact_boost` 迟滞算式与阈值式 `safe_ro - begin > max×seg`
  禁做项零改动，`gc/reclaim.rs` 未触（票面仅作可达性证据）。
- **偏差处置**：席面申报的 `compact_boosting` 注释补句属票面方案 2 三面文本同步范围，非越面，准。
- **门禁**：合并后 `cargo fmt -p wkv --check` 零 diff；`bun js/check.js` EXIT=0（「重复定义」段维持符号锚一对多
  基线 6 组，另立工具面票 `task/todo/js-checkjs-one-to-many-symbol-anchor-duplicate-whitelist.md`，与本票无关）。
- **订级留痕**：席面初报「整段定稿区活键清退」经主控重算实为一段差异（常规 `safe_ro-(max-1)·seg`
  vs 熔断 `safe_ro-seg`），故票面 P3 定级与交付提交信息均按订正口径书写，未沿用夸大表述。
