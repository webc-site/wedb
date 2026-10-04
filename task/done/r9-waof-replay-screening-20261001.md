# r9 波 AOF/waof 回放面甄别备案（2026-10-01）

甄别席「Screen waof replay truncation defects」（基线 `591cb89`）报 立案 2 + 淘汰 4 + 阴性若干。
主控现码三侧（rust 写端 / rust 读端 / C# 权威）亲验后裁定如下，票面详情见
`task/ing/wnode-aof-chunked-group-identity-keyed-by-keyhash-residual-accumulator-poisons-same-key-successor.md`。

## 一、立案处置

1. **分块记录分组身份键退化为键哈希（P2，已入 `task/ing`）**——采纳。
   席面原案两选型含糊（地址 vs 序号未定优先级），主控改写为「记录级唯一身份」主案 + 退化案须申报，
   并补钉禁新增帧头字节、禁 bump `AOF_FORMAT_VERSION`；错误面新增「严禁复用检查点起始 bool」硬拦。
2. **`recover_truncated_at` 0 哨兵与地址 0 碰撞（席面 P4）**——**不独立成票**，并入上票锁定注记
   「小事裁定」段作**仅登记备查**。理由（按「账有无消费者」审法）：`recover_truncated_at` 生产侧
   消费者仅 `wedb/waof/src/wal/recover.rs` 擦尾判据一处，`committed == 0` 时回放面本为空，
   观测发散无下游放大；且修它需动哨兵语义（改 `Option`），与「无截断」成对矛盾的现形是
   重复计 `recover_dropped_bytes`——量级不达立案门槛。

## 二、淘汰备案（勿在本波重报）

1. `wedb/wnode/src/aof/recover/recover_log_driver.rs::consume` 并行臂尾批 `let _ = …` 折错
   （同批中途错经 `?` 上抛，判序不一）——**淘汰**：并行臂仅 `replay_task_count > 1` 进入，
   `wedb/wedb/src/server/boot.rs::aof_boot_gate_violation`（`ERR_AOF_MULTI_REPLAY`）拦死，
   无 RESP 复现（可达性门槛不过）。
   属「多回放门解锁」前置项，随门一并清，不单独立票。
2. `wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs` BasicHeader 形态 TxnStart
   参与者缺省 `0`（C# `AofReplayCoordinator.cs` 缺省 `(short)AofReplayTaskCount`）——**淘汰**：
   现产 1×1 下两者皆 `<= 1` 空栅栏；跨仓旧代文件先被 `aof_processor.rs` 版本等值门判止。同属多回放门前置项。
3. `wedb/wnode/src/aof/aof_chunked_record_reader.rs` 未知 op_type `.ok()?` 静默跳
   （vs `aof_processor.rs` 显式 `map_err`）——**淘汰**：需 op 字节位腐方达，C# 亦在分派 default 抛，
   纯响度差；上票第 2 步错误面改造已顺带覆盖同函数，勿另立。
4. `record_gate.rs::should_skip` 解析失败静默跳——**淘汰**：< 16B 条目无合法生产者，不可达。

## 三、已核清阴性（本波不复查，避免重开）

- `replay_align_barrier.rs`：轮 ID / remaining 递减 / 去重 / disable 占位与 C# 同形，
  coarsetime 饱和减法注记自洽。
- `read_consistency_manager.rs`：窗口轮转算式对 C#:245-247 逐式同，
  `virtual_sublog_idx_of_hash` 与 `GetVirtualSublogIdx` 同构，漂移判据 `max-min <= threshold` 同。
- `aof_recover.rs`：`-1 → tail`、cookie 不齐备保守零重放。
- `waof/src/wal/**`（recover 见证闸、log 单调推进、iterator / flush / pipeline / ring_buffer /
  commit / disk_window / header）：无 off-by-one、无饱和回绕（SNG 仅 `i64::MAX` 理论界）。
- `aof/header/basic.rs`：u8 union 等形 C#。
- `waof_sublog.rs`：`flush_failures` 有背压 + INFO 双消费者。
- `aof_processor_store_ops.rs`：DbMeta 反查未命中显式上抛（无 `unwrap_or(0)` 静默回退）。
- 版本等值门属已注记刻意收窄（deviations 在册）。
