优先级：中
来源：next/db-raw-read-variant-collapse.md 认领细化。取证基线：主仓 dev 当下代码。

甄别结论

票面「收敛为单点 read_core + 统一读上下文结构体（前缀 buf、KeyTag、with_size 标志、
unprotected 标志）」的修法拒绝：变体面不是机械组合爆炸，逐维度 grep 实测调用点数
try_read_sync 36、read_tag_with 53、read_raw 41、read_raw_with 20、
try_read_tag_sync_with_size 7、try_read_tag_in_memory_unprotected_with_prefix 5、
try_read_tag_sync_unprotected 5、try_read_raw_in_memory 5、
try_read_tag_sync_unprotected_with_prefix 2、try_read_raw_in_memory_with_addr 2（外部）、
try_read_tag_in_memory_unprotected 2（外部）、try_read_tag_in_memory_with_size 1，
每一维度都有活调用方，且 with_prefix 维度是 transpile SKILL「循环前缀外提」硬性要求，
with_size 维度是 MEMORY USAGE 单内核要求（杜绝平行统计链），unprotected 维度是纪元
开销规避要求。C# 侧 ClientSession/Read 便捷包装本就分层
（garnet/libs/server/Sessions/ 与 libs/server/Storage/Functions/MainStore/ReadMethods.cs），
把包装压成带运行时标志的 read_core 反而把 C# 没有的复杂度引进来，并破坏单态化内联
（read.rs:437 注释实测退化约 12%）。公开包装面一律保留。

票面「17 个 pub 方法」数字不实：本文件 pub 读方法 16 个（with_addr_reader、
try_read_mem、try_read_mem_fallback、read_from_disk、promote 为私有或 pub(super)）。

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
