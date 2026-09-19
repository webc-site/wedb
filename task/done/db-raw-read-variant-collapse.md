优先级：中
来源：next/db-raw-read-variant-collapse.md 认领细化。取证基线：主仓 dev 当下代码。

甄别记录：上游票面「单点 read_core 收敛」修法与「17 改 16」纠偏均已判否，
全文移 task/reject/db-raw-read-variant-collapse.md。

采纳的真问题（内核层确有三套重复机制）

1. ReadCache 链走查两处逐字复制：read.rs try_read_mem 步骤 1（约 :390-:428）与
   try_read_mem_fallback RC 段（约 :534-:575）。且两份语义已漂移：主路径整链走查，
   fallback 只访问首条 RC 记录即 skip_read_cache 剥链——同 Tag 多 RC 记录时 fallback
   会漏配深链记录。C# 对标单点：
   libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs
   FindInReadCache（无条件沿 PreviousAddress 整链走查）。
2. 内存反向回溯循环两处复制：try_read_mem 步骤 2（:436-:475）与 fallback while 段
   （:581-:617），含 Found 后 safe_ro 判定 + promote、Tombstone、Retry、Miss 续链
   四分支同构。C# 对标单点：FindRecord.cs TraceBackForKeyMatch +
   InternalRead.cs:CopyFromImmutable。且两份对 Miss(0)（链尽）处理不一致：主路径
   break 时 curr 停在末条内存记录地址（可能被当作伪磁盘候选透传），fallback 置 0。
3. RETRY_LATER 刷新重试环三处复制：read.rs with_addr_reader（:161-:176）、
   read_raw_with_reader（:762-:772）、session/raw/batch.rs 异步批量读内层 loop
   （:124-:141）。C# 对标单点：会话层 HandleOperationStatus 的 refresh-and-retry。
4. fast_hash + find_tag_by_hash 探测两连在 read.rs 出现 6 处（:187、:217、:232、
   :346、:760、:769）。C# 对标单点：InternalRead 入口 OperationStackContext(keyHash)
   一次算定。

修法（行为零改动为纲，仅归一两处漂移语义）

read.rs 内核抽三个私有单点，公开包装面与签名全部不动：
- fn find_in_read_cache(key, curr: &mut u64, f) -> 三态（Found/Retry/ChainEnd），
  对照 FindInReadCache；try_read_mem 与 fallback 的 RC 段改调本函数，fallback
  对齐整链走查口径（修复漏配深链、消除 skip 竞态 Retry 分支）。
- fn trace_back_for_key_match(key, curr: &mut u64, head/ro/safe_ro 快照, f) ->
  四态（Found/Tombstone/Retry/Stopped），Found 内含 safe_ro 提升判定（promote
  调用点由 2 收敛为 1）；Miss(0) 统一置 curr=0（Stopped 语义），杜绝伪磁盘候选。
- enum MemDrive { Done(Option<R>), OnDisk(cands) } + fn drive_mem_read 重试驱动
  单点；with_addr_reader、read_raw_with_reader、batch.rs 异步批量内层 loop 三处
  改接；探测两连收敛为 fn read_probe / fn reprobe_first_addr。

try_read_mem_fallback 预期从 130 行缩至约 40 行；read.rs 整体减约百行。

协调

- task/ing/rmw-atomic-read-modify-write-window.md（分支 fix-rmw-atomic-window）取证
  引用了本文件读侧位点但不动本文件，无冲突；合并时若有行号漂移以符号为准。
- next/db-read-cache-probe-unify.md 的「Promote 决策从 try_read_mem_fallback 抽出
  单点」一条被本单 trace_back 收敛自然完成，其余（RcVisit/ReadProbeResult 判据
  映射注释互指）留给该单，本单不动两套枚举。
- 不涉及 consistent_read.rs；task/ing 无同文件在途票。

验收

cargo check -p wkv 通过（worktree 内）；全部调用点语义不变，仅上述两处已漂移的
瞬态边角口径按 C# 归一（fallback 整链走查、Miss(0) 置 0），无新增 pub 面。

落位：已完成（merge cd44779，代码提交 f5230fc）。三单点 find_in_read_cache /
trace_back_for_key_match / drive_mem_read(+MemDrive) 与探测两连 read_probe /
reprobe_first_addr 全部落地；try_read_mem 收为驱动环私有；fallback RC 段改接
整链走查、Miss(0) 链尽置 0 两处按 C# 归一；batch 内层重试环改接驱动。
window.rs with_record 锚点让位，FindInReadCache 全路径锚点一处定义。
cargo check -p wkv 与 --workspace 通过；check.js 无新增重复定义组。
