锁定注记（2026-10-01 r10 波主控，基线 `c2557a1`；wcompact 甄别席候选 + 主控现码两侧亲验错误语义与保守通道，锚以本注记为准，台账禁钉行号）
- 病灶：`wedb/wkv/src/compact.rs::WedbCompactionFunctions::on_dropped` 的并发安全垫 **无 TTL 旁路支**——
  `if let Ok(true) = host_exists_cooperative(session, key, tag_offset)` 后 `return Ok(())`，
  即只有 `Ok(true)`（宿主在场）走保守放行，`Ok(false)`（确证孤儿）与 **`Err`（迁移内核错误）同路**落入
  破坏性清退链：`emit_event(StoreEvent::RangeIndexDrop)` 先入账 AOF → `delete_index` 注销树 →
  `unregister_bftree_key` → `delete_miss_hook`，随后调用方摘槽。
- 契约自证（同文件即铁证，非席面臆断）：`wedb/wkv/src/compact.rs::host_exists_cooperative` 头注明令
  「返回 `Err` 为迁移内核错误（溢出桶耗尽 / 环形 Cycle 等），调用方须按『存活』保守处置本轮，
  **严禁折成孤儿判死丢弃**（grow 迁移窗内宿主条目未迁即新表查空，直判孤儿即 TTL 静默消失、
  ETag 对偶校验记录丢失的成批数据丢失洞）」；同文件消费该函数的另两臂皆保守——
  `::WedbCompactionFunctions::is_deleted` 的 `KeyTag::Ttl` 臂 `Err(_) => false`（判活）、
  `KeyTag::Etag` 臂 `Err(_) => return false`（判活）。三处消费点中唯 `on_dropped` 一处折损，
  且折向**破坏性**一侧——同函数同错误在相邻臂一个判活一个判死，属明显不一致而非有意设计。
- 保守通道现成（不改契约）：`wcompact/src/compactor/run.rs::CompactRun::drop_dead` 以
  `cf.on_dropped(session, key).await?` 承接——`Err` 即在 `index().delete(key, addr)` 摘槽**之前**早退；
  两个调用面（Lookup 逐记录臂、Scan 阶段 3 快速清理通道）对 `Err` 的处理皆为
  `warn!("紧缩清退失败，跳过摘槽并回退截断") + tally.retain_record(addr)`，
  即「记录保守保留、截断位点不前移、下轮重试」——与本域既有 `failed_trees` 命中臂的
  `Err(...)` 返回同形同通道，无需新枚举或新配置。
- 可达链：`wedb/wkv/src/gc/compact.rs::GcManager::try_compact` → `wedb/wkv/src/compact.rs::WedbStore::compact`
  → `wcompact/src/compactor/mod.rs::compact_with_filter` → `run.rs::compact_lookup` / `run.rs::compact_scan`
  的 `judge_dead` → `drop_dead` → `on_dropped`。判死主体为 Etag 孤儿 / TTL 旁路 / 过期数据，
  其清退窗内安全垫探针 `find_tag_cooperative` 遇 grow 迁移窗内核错误即命中。
- C# 对位：`garnet/libs/server/Storage/Functions/GarnetRecordTriggers.cs::IsDeleted` 恒 false（业务判死不存），
  无 on_dropped 对位；对齐维度取 `garnet/libs/storage/Tsavorite/cs/src/core/Compaction/ConditionalCopyToTail.cs`
  的 NOTFOUND 保守补拷臂（`TsavoriteCompaction.cs::Compact` → `FindRecord.cs` → `ConditionalCopyToTail.cs` 链，
  「不确定存活绝不宣告死亡」）与 `UpsertMethods.cs` 值/TTL 单记录一体原子——C# 形态下探测不确定时一律保守保留，
  rust 此臂把不确定折成确定死亡并做破坏性注销。
- 危害链（逐环核消费者）：(i) 真——被毁者是**仍可读的活键**：宿主记录槽位仍在（摘槽在 `on_dropped` 之后且被
  `Err` 跳过不了……不，本臂返回 `Ok(())` 故调用方照常摘槽），主端 `get_tree` 落空而数据帧健在，
  分层对象（RangeIndex/树形集合）转为不可读；(ii) 真——`RangeIndexDrop` 已先入账 AOF 并放射副本，
  副本按伪清理事件删除同一活对象（不可自愈，非本地单侧）；(iii) 真——`unregister_bftree_key` 注销后
  物理树文件按 `OnDispose(deleteFiles: true)` 语义回收，数据不可恢复。
- 前案边界（查重已核）：`task/done/wcompact-dead-candidate-fast-lane-on-dropped-none-blindspot.md`
  裁的是「None 盲点」（安全垫缺席即无判据），**其修复正是引入本 `Ok(true)` 折损位的那一步**——
  本票收其**修复残面**（`Err` 折损），非重开该票判据；`task/done/wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal.md`
  裁写面/TTL 腿次序与崩溃窗，与本票错误语义面正交；`task/done/wkv-gc-circuit-breaker-shift-bypasses-liveness-normalization.md`
  裁位点归一化，异面。五池 grep（on_dropped / host_exists / 迁移内核错误 / 溢出桶）无同题票。
- 禁触域（同侪在途）：`wedb/wedb/src/server/replication/**`、`wedb/wedb/src/server/failover/**`、
  `wedb/wcompact/src/compactor/run.rs`（本票只读其 `drop_dead` 保守通道，**零改动**；阶段 3 复查互扰属
  另票 `wcompact-scan-phase3-recheck-reads-sidecar-dropped-in-same-round-value-immortal-tree-destroyed`，
  勿顺手改）、`wedb/wkv/src/store/keyspace.rs`、`wedb/wnode/src/aof/**`；
  本票只动 `wedb/wkv/src/compact.rs`（`on_dropped` 安全垫支）与 `wedb/wkv/tests/**` 夹具。

审核结论：通过（2026-10-01 主控亲验立案；P2。触发需 grow 迁移窗内核错误与判死候选同轮并存，前置较苛、
低频窄路径；但一旦命中即为「活宿主被伪 RangeIndexDrop + 树注销」的不可逆破坏，主从双侧同毁，
且违反同一函数头名的明文禁令。危害族与已闭 None 盲点票同级，定 P2）

紧缩 on_dropped 并发安全垫把宿主存在性探查的迁移内核错误折成「宿主缺席」：对存活活键入账伪 RangeIndexDrop 并注销物理树

问题分析：
1. 错误语义单向折损：`host_exists_cooperative` 的三值语义（在场 / 确证缺席 / 探查不确定）在 `is_deleted`
   两臂被完整承接（不确定即判活），到 `on_dropped` 却被 `if let Ok(true) =` 压成两值——不确定并入缺席。
   这与 `Ok`/`Err` 在 rust 的常规含义相反：本函数把「不确定」放在 `Err`，而该臂把不确定处理成
   「确证可以破坏」，等于把最保守的一档交给了最具破坏性的调用点。
2. 破坏先于摘槽且已放射：`on_dropped` 内 `RangeIndexDrop` 入账先于 `delete_index`（该顺序由前案
   WAL 纪律锁定，不可反转——见同文件「日志先行」注），因此折损一旦发生，AOF/副本侧清理事件已经落账，
   本地回不了头；而 `Ok(())` 又让调用方继续摘槽，宿主记录与已毁树形成终态错配。
3. 修复面极小且零新机制：`Err` 上抛即复用 `drop_dead` 既有保守通道（跳过摘槽 + `retain_record` 回退截断 +
   下轮重试），与前案 `failed_trees` 命中臂的保守返回完全同形，不需新错误枚举、不需新配置项、不动契约与线协议。

涉及代码：
rust 文件与函数：
wedb/wkv/src/compact.rs::WedbCompactionFunctions::on_dropped（折损臂本体）
wedb/wkv/src/compact.rs::host_exists_cooperative（契约来源，只读复用，禁改其语义）
wedb/wkv/src/compact.rs::WedbCompactionFunctions::is_deleted（两保守臂，形态对位基准）
wedb/wcompact/src/compactor/run.rs::CompactRun::drop_dead、::compact_lookup、::compact_scan（Err 保守通道证据，禁改动）
对应 c# 文件与函数：
libs/storage/Tsavorite/cs/src/core/Compaction/ConditionalCopyToTail.cs（NOTFOUND 保守补拷，不确定不宣告死亡）
libs/server/Storage/Functions/GarnetRecordTriggers.cs::IsDeleted（恒 false，本臂属 wedb 业务扩展）

精炼执行方案：
1. **单臂改三值保守**：`on_dropped` 安全垫的无旁路支改 `match host_exists_cooperative(session, key, tag_offset)`——
   `Ok(true) => return Ok(())`（宿主在场，保守放行，语义不变）、`Ok(false) => {}`（确证孤儿，清退链不变）、
   `Err(e) => return Err(e)`（探查不确定 → 上抛，复用 `drop_dead` 既有「跳过摘槽 + `retain_record` 回退截断」通道）。
   禁止把 `Err` 折成 `Ok(())`、禁止 `log::warn` 后继续清退、禁止新增计数旁路冒充保守（本项目「禁静默冒充」硬口径）。
2. **注释同步**：安全垫支注语补一句「探查不确定即本轮保留，下轮重试（对标同函数头名禁令与 `is_deleted` 两臂）」，
   并在 `host_exists_cooperative` 头名注的「调用方须按存活保守处置」后点名 `on_dropped` 为第三消费臂，
   使三臂一致性有据可查；禁在代码注释引用票号。
3. 禁做项：不动 `RangeIndexDrop` 入账与 `delete_index` 的先后次序（前案 WAL 纪律）；不改
   `host_exists_cooperative` 的返回形态与探查集合；不改 `wcompact` 侧任何裁决次序（阶段 3 复查互扰属另票）；
   不引新错误类型（`crate::Error` 既有 `Io` 面足够，`Err` 原样透传）；禁 `#[allow]`/`#[expect]`；禁占位实现；
   禁顺手改 `wedb/wkv/src/compact.rs` 中与本臂无关的注释或格式（避免与在途同侪撞车）。
4. 锁测（`wedb/wkv/tests/**`，紧缩册内）：
   (a) **Err 注入臂**——仿同文件既有 `ON_DROPPED_PAUSE_INJECT` 的 `#[cfg(debug_assertions)]` 钩形态
       增设单次「宿主存在性探查返 `Err`」注入钩（禁在生产 cfg 编译），构造 Etag/孤儿 TTL 判死候选 +
       分层宿主键：断言该记录本轮 `retain`（紧缩后索引槽仍在、记录仍可读）、
       `get_tree` 命中（树未被注销）、AOF 事件面**零** `RangeIndexDrop` 入账；
   (b) `Ok(false)` 真孤儿回归臂保持既有断言绿（清退链不得被本票改动）；
   (c) 既有紧缩册（`wkv_compact_*`、`wcompact` 全套、TTL/Etag 旁路族）全绿。
   revert-proof：把 `Err(e) => return Err(e)` 改回 `if let Ok(true) =`（即撤第 1 步）后 (a) 的
   「零 `RangeIndexDrop`」与「树仍在」两断言必红；若只改为 `Err(_) => return Ok(())`（静默放行不破坏、
   但也不留痕）则 (a) 的「本轮 retain」断言仍红——反向钉死「上抛借道保守通道」形态，杜绝半收口。
5. 登记：本票判据（存在性探查不确定的第三档不得折成死亡）在 `doc/zh/deviations.md` 册尾顺编新节登记
   （先入库者得号、撞号让位不写死；取号前先 grep 册尾），锚用 `路径::符号` 形态。
6. 验证面：`cargo check -q -p wkv -p wnode --all-targets` 与
   `cargo nextest run -p wkv --test main`（紧缩族在 `tests/main.rs` 的 leaf 内册，禁照抄 leaf 名当 `--test` 靶）
   与 `cargo nextest run -p wcompact`；禁在主树或沙箱跑 `./test.sh`/`./sh/clippy.sh`（波次门禁由主控统一跑）。

---
### 收口与终态记录（2026-10-01 主控闭环归档）
- **修复方案**：`wkv/src/compact.rs::WedbCompactionFunctions::on_dropped` 安全垫在无 TTL 旁路支对 `host_exists_cooperative` 改写为三值显式匹配：`Ok(true) => return Ok(())`（宿主在场保守放行）、`Ok(false) => {}`（确证孤儿清退）、`Err(e) => return Err(e)`（探查不确定显式上抛），借道调用方既有 `drop_dead` 跳过摘槽与 `retain_record` 保守保留通道延至下轮重试，杜绝活宿主被误注销。
- **锁测验证**：`wkv/tests/compact/host_probe_err.rs` 增设 debug-assertions 错误注入锁测与真孤儿回归锁测，断言宿主树保全、零 `RangeIndexDrop` 入账、记录保守保留可读且下轮重试成功；单测及整套 `compact` 套件全绿；revert-proof 验证完成。
- **台账登记**：在 `doc/zh/deviations.md` 顺编登记 `[§198]`。
- **工单归档**：主分支已合入，工单移动至 `task/done/`。
