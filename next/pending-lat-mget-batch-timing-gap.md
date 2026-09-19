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
