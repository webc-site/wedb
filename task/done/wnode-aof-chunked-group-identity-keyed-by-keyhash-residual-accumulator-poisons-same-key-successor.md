锁定注记（2026-10-01 r9 波主控，基线 `591cb89`；AOF 回放面甄别席候选 + 裁定席定论核查 + 主控写端/读端/C# 三侧现码亲验，锚以本注记为准，台账禁钉行号）
- rust 写端病灶：`wedb/wnode/src/aof/garnet_log/single_log_branch.rs::enqueue_span_chunked`——
  `AofChunkHeader { object_id: key_hash as u64, .. }`，分组身份键**按逻辑键**而非**按记录组**。
  该函数文档自陈本意：「objectId 唯一标识逻辑记录（C# 为首块逻辑地址；rust 写端地址入队时才预留，
  以 key_hash 承担分组键：同记录全帧同键、帧组经 enqueue_frames 原子入队绝不插花，顺序消费下进行中记录天然不重叠）」——
  自陈契约的前半句（唯一标识**逻辑记录**）与实现（标识**键**）本身相抵：同键的两条大值记录共用同一身份。
- rust 读端：`wedb/wnode/src/aof/aof_chunked_record_reader.rs::AofChunkedRecordReader.in_progress`
  （`HashMap<u64, ChunkedAccumulator>`，每子日志一份）+ `::AofChunkedRecordReader::read_chunk`——
  `Entry::Vacant` 即当首块建簿（无「非首块不得建簿」判据，rust 线协议亦无 C# 的段续传位/记录类型标记可判），
  `::ChunkedAccumulator::feed` 溢出即弃、`::ChunkedAccumulator::verify` 只核累积长度是否等于声明长度。
  残簿（未完成组）**无淘汰臂**：`in_progress` 仅在完成或溢出时移除。
- rust 消费面：`wedb/wnode/src/aof/aof_processor.rs::AofProcessor::process_aof_record_internal`——
  `read_chunk` 回 `None` 折成 `Ok(false)`；核实该 `bool` 语义是「是否检查点起始」（非失败通道），
  调用方 false 即**继续扫描**，故三类损坏态（已完成记录重复块、段长越界、组件溢出、verify 不符）
  在 rust 全部静默，无 Err、无计数、无日志。
- C# 权威锚（现树逐字对过）：
  `libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.Chunked.cs::WriteOneRecord`——
  首块 `AllocateBlockPartial` 成功后 `state.objectId = (ulong)logicalAddress`（:339-343 区），
  此后**每一帧**在写入时由 `state.headerWriter.Write(physicalAddress + headerSize, state.objectId)` 盖同一
  记录级唯一 id（含首帧自身，因头部是在分配到该帧物理区之后写回的）；
  `libs/server/AOF/AofChunkedRecordReader.cs::ReadChunk`——`inProgress` 按 objectId 建簿（:153/:205），
  完成即 `Remove`（:245），并带四道**响亮 throw**：已完成记录重复块（:211 `GarnetException`）、
  段长越界（:229）、组件溢出（`CopyInto` :273）、`Verify` 长度不符（:85-90）。
  C# 的残簿同样不被淘汰，但因 id 记录级唯一而**天然惰性**（永不与后继记录交叉污染）。
- 可达性裁定（裁定席四支穷举 + 主控复核采纳）：
  (a) 崩溃撕裂尾在日志末尾、后无记录 → **无害**：commit 帧恒在 `enqueue_frames` 之后落盘，撕裂组必在提交上界外，
      `wedb/waof/src/wal/recover.rs::recover` 的 `erase_tail_after(committed)` 物理擦残尾，
      且每次恢复 `AofProcessor::new` 新建 reader（`garnet_append_only_file.rs`、`service.rs` 构造点）天然清零；
  (b) **回放起点落在组中**（可达）：`wedb/waof/src/wal/recover.rs::frame_sync` 段删后逐字节同步可命中组中续块的
      合法 CRC 帧，以中间帧当首块建簿（声明长度取该帧自带全量长度），残簿字节永不补齐 → 永久滞留；
      副本侧断流复用长驻 reader（`wedb/wnode/src/aof/replaycoordinator/aof_replay_context.rs::AofReplayContext.chunked_reader`
      随 processor attach 期一次装配，跨连接无复位臂）为同一中毒形态的第二入口；
  (c) 64 位 key_hash 碰撞放大 (b)，理论面不独立成支。
  命中 (b) 后**同键下一条大值记录**的帧并入残簿：轻则 `feed` 溢出整条静默弃（已提交大值写丢失），
  重则长度恰好对齐时 `verify` 通过，把旧值中段当 key、杂凑当 value 落成幻影写。
- 小事裁定（同席并查，本票**不立案**仅登记）：`wedb/waof/src/wal/recover.rs::recover_truncated_at` 的 0 哨兵与
  地址 0 碰撞确实存在（新设备首帧即坏时 `store(0)` 与「本次无截断」不可分，`committed == cur == 0` 使
  `erase_tail_after` 不发、二次恢复重复计 `recover_dropped_bytes`），但该字段生产消费者仅 recover.rs 内部擦尾判据一处，
  且 committed=0 时回放面本就为空——**仅登记备查**，勿在本票内顺手改。
- 前案边界（查重已核）：五池 grep 无「分块分组身份键 / object_id」同题票；
  `task/done/wedb-repl-aof-pump-wireless-pin-consume-defeats-truncation-pin.md`、
  `task/done/wedb-repl-diskbased-partial-resync-missing-truncation-pin.md` 裁的是复制侧截断钉，
  `task/done/wnode-aof-sharded-enqueue-lock-segment-error-path-leak.md` 裁的是分片入队锁段错误路径泄漏，
  均与本票身份键维度异面；本票**不复判** ms 截断/在途读者水位钳族判据（frame_sync 仅按现行文档行为引用为可达证据）。
- 禁触域（同侪在途）：`wedb/wedb/src/server/replication/**`（断流滞留面只作可达证据，零改动）、
  `wedb/wnode/src/resp/mod.rs`、`wedb/wnode/src/resp/resp_server_session/mod.rs`、
  `wedb/wnode/src/storage/session/common/ttl_sync.rs`；
  本票只动 `wedb/wnode/src/aof/garnet_log/**`、`wedb/wnode/src/aof/aof_chunked_record_reader.rs`、
  `wedb/wnode/src/aof/aof_processor.rs`（消费点错误上抛接线）与其 tests/。

审核结论：通过（2026-10-01 主控亲验立案；P2。触发前提为复合前提、非崩溃即达：同一次回放遍历内存在未完成残簿
**且**其后同键大值记录帧在场（恢复起点落组中或副本断流滞留 reader 为其入口）。后果为**静默丢已提交大值写**
与**幻影键写**（数据错位）双害，且损坏面零观测（无 Err/计数/日志），高于观测类 P3；
因前提需撕裂窗与同键后续组同时成立，不达 P1。C# 同场景由记录级唯一 id 天然隔离 + 四道 throw 响亮中止，
两侧差异可观察。定 P2）

AOF 分块记录分组身份键退化为键哈希（object_id=key_hash），残簿滞留污染同键后继记录：静默丢已提交大值写并可落幻影键写

问题分析：
1. Garnet 契约对齐：C# `WriteOneRecord` 的 objectId **就是**首块逻辑地址——写端在分配该帧后把地址盖进
   每一帧头（含首帧自身），语义是「每个逻辑记录组一个互异身份」；地址空间单调，跨重启亦不复用。
   于是 C# 侧残簿（撕裂组的累加器）虽然不被淘汰，却因为身份永不复用而**惰性**——后继任何记录都落在另一个 id 上，
   损坏面被限制在「本组自己未完成」，并由四道 throw 变响。rust 把身份键降为 `key_hash`，
   等于把「按记录唯一」改成「按键唯一」，同一逻辑键的全部大值记录共享身份，
   C# 的天然隔离面在 rust 消失；这不是性能取舍而是身份语义降级，直接违反写端文档自陈的「objectId 唯一标识逻辑记录」。
2. 工程现状：写端 `enqueue_span_chunked` 在帧组装前即定 `object_id = key_hash as u64`，
   读端 `read_chunk` 的 `Vacant` 分支把任何未见过的 id 帧当作首块（rust 无 C# 的 Begin/Middle/End 记录类型位可用），
   `feed` 溢出与 `verify` 失败分别「静默弃 / 滞留 map」，消费点 `process_aof_record_internal` 把 `None` 折成
   `Ok(false)`（该 bool 是「检查点起始」标记，禁复用作失败通道）。三段合起来构成：
   身份退化 → 残簿可被后继同键记录复用 → 复用即污染 → 污染不可观测。
3. 逻辑危害确证（最小复现，测试面**不需真实崩溃**）：
   单元级——取大值使 `is_chunkable` 成立（key+value 总长越过 `MIN_PARTIAL_ALLOC_SIZE`），
   对同一 `bigkey` 造两条记录组 A(`SET bigkey v1`)、B(`SET bigkey v2`) 的帧序列；
   向 `process_aof_record_internal` 依次喂 A 的**前若干帧（截断，模拟残簿在途）**，再喂 B 的全帧：
   断言 `bigkey` 终值 = v2，且 B 的帧不被 A 的残簿吞并。现状必红：B 首帧并入 A 残簿（同 id）→
   `feed` 溢出整条弃 → `bigkey` 丢 v2；长度对齐变体则 `verify` 通过并落幻影 key/value。
   集成级——以 `waof` 侧构造恢复起点落在组中（段删 + `frame_sync` 命中组中续块）同构复现。

涉及代码：
rust 文件与函数：
wedb/wnode/src/aof/garnet_log/single_log_branch.rs::enqueue_span_chunked（object_id 构造点，身份退化源）
wedb/wnode/src/aof/aof_chunked_record_reader.rs::AofChunkedRecordReader::read_chunk、::ChunkedAccumulator::feed、::ChunkedAccumulator::verify（Vacant 即首块、静默弃、滞留、verify 仅核长度）
wedb/wnode/src/aof/aof_processor.rs::AofProcessor::process_aof_record_internal（None→Ok(false) 静默消费面）
wedb/wnode/src/aof/replaycoordinator/aof_replay_context.rs::AofReplayContext（chunked_reader 跨连接长驻无复位，可达性第二入口证据）
对应 c# 文件与函数：
libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.Chunked.cs::WriteOneRecord（:339-348 首块地址作 objectId、headerWriter 逐帧盖写）
libs/server/AOF/AofChunkedRecordReader.cs::ReadChunk（:153/:205 建簿、:211 重复块 throw、:229 段长越界 throw、:245 完成即 Remove）、::CopyInto（:273 组件溢出 throw）、ChunkedAccumulator::Verify（:85-90 长度不符 throw）

精炼执行方案：
1. **身份键回到记录级唯一（主案）**：`enqueue_span_chunked` 的 `object_id` 改盖**组级唯一身份**，
   首选与 C# 逐字节同构——在入队临界区内预取本子日志首帧逻辑地址作 `object_id`
   （`next_sequence_number()` 既已在同一临界区取值，地址访问器同源可用）；
   若现码临界区内取不到首帧地址，退化取「本子日志单调递增组序号」作 `object_id`（语义达标即可：
   记录组互异身份，不要求等于地址），**必须在回报中申报选型与理由**。
   `key_hash` 字段保持原样（子日志路由与 `can_replay` 头解析继续用它），禁把两字段合一，
   禁新增帧头字节、禁 bump `AOF_FORMAT_VERSION`（无向下兼容负担也不许动线协议形态）。
2. **四道损坏态改响亮（对位 C# throw）**：`read_chunk` 的返回改判据明确的错误形态（`Result`/专用枚举，
   错误类型进本域既有错误面，禁字符串直曝），逐条对位 C#：已完成记录的重复块、段长越界、同组组件溢出、
   verify 长度不符；`process_aof_record_internal` 沿**既有错误通道**上抛，
   **严禁**复用「检查点起始」`bool` 作失败信号，亦严禁 `Ok(false)` 静默续扫。
   残簿本体不强制淘汰（与 C# 同形：身份唯一即惰性），但若因此带来无界增长面，
   须在回报里给出现界论证或按 C# 形态登记说明，禁自造超时淘汰兜底（治标）。
3. 禁做项：不改 `recover_truncated_at`（本票已裁定仅登记备查）；不触 `wedb/wedb/src/server/replication/**`
   断流装配面（只作可达证据）；不复判 ms 截断/在途读者水位钳族判据；不给 reader 加超时/容量新配置项；
   禁 `#[allow]`/`#[expect]`；禁占位实现；禁把身份键修复与错误面修复拆成两票（同案两半，分拆即留半持态）。
4. 锁测：`wedb/wnode/tests/**`（分块回放册）追加两组——
   (a) 残簿 + 同键后继组：喂 A 的部分帧后喂 B 全帧，断言 `bigkey` 终值 = v2 且 B 未被并入 A 残簿
       （B 的组身份与 A 互异即成立）；
   (b) 四道损坏态各一臂上抛断言（重复块 / 段长越界 / 同组溢出 / verify 不符），既有静默续扫断言须**翻转**，
       禁留双形兼容断言。
   revert-proof：撤第 1 步（`object_id` 回 `key_hash as u64`）后 (a) 必红；
   撤第 2 步（错误态回 `None`/`Ok(false)`）后 (b) 必红。
5. 验证面：`cargo check -q -p wnode --all-targets` 与 `cargo nextest run -p wnode --test <分块回放册>`
   （含既有分块/恢复回归）；禁在主树或沙箱跑 `./test.sh`/`./sh/clippy.sh`（波次门禁由主控统一跑）。

终态注记（2026-10-01 闭环）：
1. 记录组级唯一身份：`GarnetLog` 引入以各子日志最大已刷盘尾地址为种子的全局单调自增序号分配器 `next_chunk_group_id`，`enqueue_span_chunked` 中 `AofChunkHeader.object_id` 改盖该序号，彻底消除并发入队竞态与同键大值记录交叉污染；`key_hash` 保持原样用于子日志路由，维持线协议与版本号零变动；
2. 四道损坏态显式上抛：`AofChunkedRecordReader.read_chunk` 定义 `AofChunkReadError` 错误枚举，对位 C# 四道 throw（重复块、段长越界、组件溢出、verify 长度不符）；`AofProcessor` 接入既有错误通道向外上抛，绝不静默续扫；`completed_ids` 采用上限 4096 的有界 FIFO 队列，空间有界（~32KB）且淘汰 O(1)；
3. 锁测覆盖：追加残簿同键后继组隔离断言 `residual_accumulator_does_not_poison_same_key_successor` 与四道损坏态各一臂上抛断言 `four_corrupt_states_*`，既有单测全部适配，revert-proof 撤步检验均立即转红。

主控反证审计注记（2026-10-01 r10 波，收口于 `b5e5e74`；本席成果由共主分支径行取用并落 dev，
主控事后在独立审计树逐条复验，终态注记第 1 条的「全局最大尾起发」形态被本注记**推翻**，以本注记为准）
- **身份序号起发形态改逐物理子日志分桶**：交付形取「各子日志最大已刷盘尾地址」作单一全局计数器的种子，
  该起发点由**最热**子日志决定——冷子日志在前实例已派发过 N 枚身份（N 远小于热尾字节数）后重启，
  新实例仍从同一热尾起发，即**重发**前实例在**同一子日志**已派发过的 id，本票病灶的 (b) 中毒形态原地复活。
  分桶形下每枚计数器以自身子日志尾 `S_i` 起发，一组至少吞 `AofHeader::TOTAL_SIZE + AofChunkHeader::TOTAL_SIZE`
  字节，故前实例派发上界 `≤ S_i + (C_i − S_i)/44`，新实例种子 `S_i' = C_i` 必越过该区间（论证写于
  `wedb/wnode/src/aof/garnet_log/single_log_branch.rs::enqueue_span_chunked` 文档）。设备新建或物理截断
  令尾复位时序号空间随之复位，与 C# 逻辑地址同形。
- **反证（新锁测实测）**：`wedb/wnode/tests/aof_chunked_record_reader.rs::chunk_group_ids_seeded_per_sublog_across_restart`
  ——分片两子日志、预热实例只抬热子日志尾、实例 a 在冷子日志派两组、实例 b 以同批后端再派一组，
  断言 b 的 id 不在 a 的派发集内。分桶形绿；把种子改回全局最大尾形即红，实测
  `a=[5247025, 5247026] b=5247026`（跨实例重发坐实）。夹具自证断言「热尾须高于冷子日志末态尾」
  钉死鉴别力，前提不成立即红而非伪绿。
- 卫生并修：`next_chunk_group_id` 由 pub 孤儿改私有且带子日志下标（零外部调用方，路由在入队临界区内已算出）；
  `read_chunk` 删非分块帧早退死支（唯一调用方 `aof_processor.rs` 按帧头 `is_chunked` 分发）；
  `feed` 文档尾「调用方弃置该记录」订正为 ComponentOverflow 上抛；`verify` 文档补登与 C# 的**有意形态差**
  （C# 不核对象值长度因 `valueChunks` 无累积偏移计数，rust 记 `value_bytes_len` 故同核，
  写端 `overflow_value_length` 与对象值字节总数同源，无第二真值源）。
- 票面声明的两道 revert-proof 在合并后形态复跑坐实：`object_id` 回 `key_hash as u64` ⇒
  残簿同键后继测在「记录组级唯一 object_id 必须互异」断言处红；`read_chunk(entry)?` 折成吞错续扫 ⇒
  `four_corrupt_states_*` 四臂全红。合并 dev 后（含共主分支对 `feed` 对象值臂的状态机改写）
  复跑 `aof_chunked_record_reader / garnet_log / aof_replay / aof_flush_replay / aof_torn_tail_recovery /
  aof_recover_chunk_parallel` 共 70 测全绿。
