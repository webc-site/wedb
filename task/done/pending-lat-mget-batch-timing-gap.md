优先级：中（功能缺口：MGET 族读路径漏记 PENDING_LAT，与 C# 对位不齐）

来源：pending-lat-no-timing-site 收口时新发现的缺口（取证与门禁记录见 task/done/pending-lat-no-timing-site.md 第六项），该票范围是补齐计时槽位与 trait 载体，本票只做 MGET 族这一处遗漏的计时点，不重开已落地形态。

现状事实（dev 现状，行号按当下重取）
- 唯一的 PENDING_LAT 计时漏斗在 wedb/wnode/src/storage/session/storage_session.rs:162 与 :164（latency.start / latency.stop，配 LatencyMetricsType::PendingLat 与 now_stopwatch_ticks()）。
- 字符串批量读入口 read_string_batch_into 在同文件 :292 起，内部只调 self.record_read_outcome(...)（:298 附近）记读命中，不经过上述漏斗，因此 MGET/批量读整条族不产生 pending 延迟样本。
- C# 对位：libs/server/Storage/Session/MainStore/AdvancedOps.cs 的 GET_CompletePending 变体（票面记 :38 与 :62，自行重取）在等待 pending 结果处记时但不计条目数，即 C# 侧 MGET 族确实进 PENDING_LAT 直方图。

目标形态
让批量读与单键读共用同一个 pending 计时漏斗（storage_session.rs:162/:164 那一对），不新造第二个计时点、不在调用方各写一遍 start/stop。计数语义保持 C# 口径：批量族只计时、不追加条目计数，避免把 record_read_outcome 的计数复用成双份统计。

验收
- 打开 C# AdvancedOps.cs 亲验记时与不计数的分叉点，若与本文结论相反（例如 C# 也不记 PENDING_LAT），本票判不成立，走 task/reject/ 归档并给出你读到的 C# 行号证据，不改代码。
- 落地后 cargo check --workspace --all-targets（在 worktree 的 wedb 层跑）exit 0 且 0 warning；bun js/check.js 只在 worktree 内跑且报告与合入前逐字节相同。
- 自证：git grep -n "PendingLat" HEAD 的命中点数量与位置（应仍是单点漏斗被两处复用，而不是新增第二处 start/stop）；新读点必须能被证伪（若现有测试触不到批量读，加一个最小断言到 pending_latency_timing 测试面，不得写恒真断言）。
- 禁 ./test.sh、禁 ./sh/clippy.sh、禁 udeps --fix，主代理集中跑。

坑与边界
dev 已把 garnet_api 静态虚表收敛为 Arc<dyn GarnetApiFace>，计时槽位走 trait 缺省方法 + StoreGarnetApi 覆写（见 done/pending-lat-no-timing-site.md 的偏离记录），本票必须复用该载体，不得回到旧的静态表或 Into 签名。同文件在途还有 resp-session-output-drain-single-source 与 session-metrics-option-dead-track 两票，只碰批量读函数体与必要的漏斗可见性，不做无关收口。TTL 裸 await 经前手核对 C# 无记录点，不属于本票范围，不要顺手加计时。

收口记录（分支 fix-pending-lat-mget，代码提交 ebb0c14，dev 快进至 1b07a66）

裁决
成立，但票面归因偏窄：漏挂点确在 read_string_batch_into，而"MGET 族"已在漏斗内。
以下全部为合入时现刻 HEAD 复核，非沿用票面旧行号。

甄别（dev 基线 2ba13a0 与合并后 dev 双向复核）
- 漏斗计时点：wedb/wnode/src/storage/session/storage_session.rs:163（start）与 :165（stop），
  票面记的 :162/:164 已漂移一行，形态不变；None 早返不记（:160-162）。
- 漏挂坐实：read_string_batch_into（同文件 :298）此前两分支直调
  ctx.read_batch_with / self.batch.read_batch_with，整批 await 不过漏斗。
- MGET 不漏：MGET 慢路径走 wedb/wnode/src/resp/array_commands.rs:802 mget_inner，
  :811 逐键 storage.read_string → read_string_with(:243) → read_tag_with(:175)，
  其磁盘候选臂已挂 with_pending_metrics（:185 与 :194），即 MGET 冷读每键出一条样本。
- 真正漏挂的是 GET 族：单键冷 GET 与 GET_SG 流水线冷读同口——
  wedb/wnode/src/resp/basic_commands/get.rs:36 network_get 的 SG 门在 :42，
  首个冷键即整批判停并在 :131 存 sg_batched_keys 快照，
  经 wedb/wnode/src/resp/garnet_api/mod.rs:468 取快照、slow.rs:170 C::Get 臂
  落到 read_string_batch_into。故修复前连一次普通冷 GET 都不产生 PENDING_LAT 样本。

C# 亲验（结论与票面一致，但分叉点在重载之间）
- garnet/libs/server/Storage/Session/MainStore/AdvancedOps.cs:43/:45 数组重载
  （GET_CompletePending((GarnetStatus, StringOutput)[]...)，:38 起）成对
  latencyMetrics?.Start/Stop(PENDING_LAT)，条目命中计数在同函数 :52-55 逐条
  sessionMetrics?.incr_total_found/notfound；:65/:67 迭代器重载（:62 起）只起停表、
  不计数，计数由调用方做。
- 批量读调用方：garnet/libs/server/Resp/MGetReadArgBatch.cs:173 调迭代器重载，
  命中计数在 :199-206 逐条做，pending 计数在 :114 逐条 incr_session_pending；
  :163-166 在 HasGoneAsync 为假（整批纯内存）时直接 return，整批不起表。
  另有 garnet/libs/server/Resp/BasicCommands.cs:317 走数组重载。
- 单键口：garnet/libs/server/Storage/Session/Metrics.cs:23-32
  StartPendingMetrics（:25 计数 + :26 起表）/ StopPendingMetrics（:31 停表），
  MainStoreOps.cs 与 UnifiedStoreOps 各臂内联成对，rust 已由 with_pending_metrics 收为一点。

修法
- read_string_batch_into 两分支（一致读折叠口 :313 与底层批量口 :318）的整批 await
  一并纳入既有 with_pending_metrics 漏斗，未新增 start/stop 点、未动漏斗签名与可见性；
  条目命中计数仍只由 record_read_outcome 单点入账，未复用为双份。
- 自证（合入后 dev HEAD）：git grep -n "PendingLat" 在 wedb/ 下只 7 命中，
  计时代码仍是单点漏斗两处（storage_session.rs:163/:165），其余在
  wmetric/src/latency/latency_metrics_type.rs 的枚举与名字表 4 处、
  tests/pending_latency_timing.rs:100 观测面 1 处。
- 用例：tests/pending_latency_timing.rs 新增
  batch_read_records_one_pending_sample_per_batch——sg-get 会话上两条流水线 GET
  冷键聚合为一次整批降级，断言 PENDING_LAT 样本数恰为 1（按批不按条目）且应答
  逐字节为 $2\r\nv1\r\n$2\r\nv2\r\n。证伪已实跑：仅把 storage_session.rs 回退到
  dev 版（保留用例），该断言即红（assert_failed，0 != 1，:207），另两例不受影响。

与 C# 的两处口径差（共用漏斗的必然结果，未另造机制）
- 样本粒度：C# 靠 MGetReadArgBatch.cs:163 的 HasGoneAsync 短路，纯内存整批不起表；
  rust 批量收割在引擎内单次 await（wedb/wkv/src/session/raw/batch.rs:80-192 内存快路径
  与磁盘候选合流不分家，且存储会话侧无从判别），故每批一条样本、含零冷键批。
- pending 计数：C# 按 pending 条目逐条 incr_session_pending，rust 经漏斗按批 +1；
  条目数不经批量口上抛，为计数改 wkv 签名不在本票范围。

门禁
- CARGO_TARGET_DIR=/tmp/target-fix-plm cargo check --workspace --all-targets：
  分支起点（2ba13a0）与三次合并最新 dev 后的树上均 exit 0，error/warning 计数 0。
  中途一次合并曾把 dev 上他人 wip 快照的红（wkv/src/session/consistent_read.rs:146
  少一个 ?、windex tests/index 六处已删方法）带进树，为验本棒而临时补 ? 复跑，
  随后 revert，未提交任何他人文件的无关改动；该两处上游随后自行收口。
- cargo test -p wnode --test pending_latency_timing：3 passed（含新例）。
- bun js/check.js（仅 worktree 内，合并后的树上前后对跑）：报告条目集合逐项一致，
  唯一差异是同文件下方锚点的行号漂移
  storage_session.rs:501 → :514（vector_registry_delete_hook，本棒在其之上净增 13 行），
  无新增/减少 C# 映射、无新增缺失项；跑完 worktree 无残留改动。
- cargo fmt --check -p wnode 零 diff；按规程未跑 ./test.sh、未跑 ./sh/clippy.sh，
  留主代理集中跑。

状态
已合入 dev：主仓 git merge --ff-only fix-pending-lat-mget，dev a3fbca3 → 1b07a66，
本次快进只带本棒两文件（ebb0c14 numstat：storage_session.rs +16/-3、
pending_latency_timing.rs +52/-1）。
分支 fix-pending-lat-mget 与 worktree /tmp/fork/fix-pending-lat-mget 已回收，
私有 target /tmp/target-fix-plm 已删。
