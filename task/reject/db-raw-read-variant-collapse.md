裁决：原票（next/db-raw-read-variant-collapse.md 认领细化而上）的「读变体收敛」主张
整体拒绝，本棒复核维持并归档；其指认的内核层真重复另立
task/ing/db-raw-read-variant-collapse.md 实施。核销 2026-09-20。取证基线：主仓 dev
当下代码 + 本 fixloop 棒逐维度 grep 复核。

拒绝条目一：内核收敛为单点 read_core + 统一读上下文结构体（前缀 buf、KeyTag、
with_size 标志、unprotected 标志）

理由：变体面不是机械组合爆炸，每一维度都有活调用方，且三维度各有硬性存在理由。
1. with_prefix 维度是 transpile SKILL「循环前缀外提」的硬性要求（消除逐字段重读
   原子前缀与 Varint 重算）；with_size 维度是 MEMORY USAGE 单内核要求（杜绝平行
   统计链，RecordRead 通道复用）；unprotected 维度是纪元开销规避要求（绕过
   enter() 原子操作）。
2. 本棒复核外部调用点（全仓 .方法名( 计，不含 read.rs 自身定义）：try_read_sync 27、
   try_read_tag_sync_unprotected 3、try_read_tag_in_memory_unprotected 2、
   try_read_raw_in_memory_with_addr 2、try_read_tag_in_memory_with_size 1，
   各维度均非死面。
3. C# 侧 ClientSession/Read 便捷包装本就分层
   （garnet/libs/server/Sessions/ 与 libs/server/Storage/Functions/MainStore/ReadMethods.cs），
   把包装压成带运行时标志的 read_core 反而引入 C# 没有的复杂度，并破坏单态化内联
   （wkv/src/session/raw/read.rs try_read_mem 步骤 2 注释实测退化约 12%）。
   公开包装面一律保留。

拒绝条目二：ing 票对上游的「17 个 pub 方法数字不实、实为 16」纠偏本身不实

理由：本棒逐行清点 read.rs，StoreSession 的 pub 读方法为 17 个
（try_read_raw_in_memory_with_addr、try_read_raw_in_memory、
try_read_tag_in_memory_unprotected、try_read_tag_in_memory_unprotected_with_prefix、
try_read_tag_in_memory_with_size、try_read_tag_sync_unprotected、
try_read_tag_sync_unprotected_with_prefix、try_read_tag_sync_with_size、
try_read_sync_unprotected、try_read_sync、read_raw_with、read_raw_with_size、
read_tag_with、read_tag_with_size、read_with、read_raw、read）；非 pub 内核为
with_addr_reader、try_read_mem（pub(super)）、try_read_mem_fallback、
promote_immutable_read_hit、read_from_disk（pub(super)）、read_raw_with_reader。
上游「17」计数与当下代码相符，ing 票的「16」系漏数，随之作废。该条只影响叙述
数字，不影响采纳项的实施，真问题面（内核三处重复 + 探测两连复制）另见 ing 票。
