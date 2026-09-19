优先级：中
来源：next/agy.db.md 条 4 立项。取证基线：主仓 dev 当下代码，行号为当下实测。

问题
wkv session/raw/read.rs 876 行内读方法变体组合爆炸：内存读 / 磁盘回退同步读 / 异步
编排 / with_prefix / with_size / unprotected 六个维度机械组合出 17 个 pub 读方法，
其中 session_tag_key -> fast_hash -> find_tag_by_hash 探测模板逐字重复 12 处；
try_read_mem_fallback 单函数 130 行（:508-:637 前后）。

取证
- 变体清单（wedb/wkv/src/session/raw/read.rs）：try_read_raw_in_memory_with_addr :141、
  try_read_raw_in_memory :181、try_read_tag_in_memory_unprotected :196、
  try_read_tag_in_memory_unprotected_with_prefix :209、try_read_tag_in_memory_with_size :225、
  try_read_tag_sync_unprotected :245、try_read_tag_sync_unprotected_with_prefix :259、
  try_read_tag_sync_with_size :281、try_read_sync_unprotected :301、try_read_sync :311、
  try_read_mem :336、read_from_disk :669、read_raw_with_reader :752、read_raw_with :791、
  read_raw_with_size :801、read_tag_with :813、read_tag_with_size :831、read_with :854、
  read_raw :864、read :873。
- 模板重复：fast_hash / find_tag_by_hash 在本文件出现 12 次。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs
  InternalRead 单一状态机（COPY_TO_MAIN_LOG_FROM_DISK / RECORD_ON_DISK / RETRY_LATER
  等状态闭环，前缀与尺寸差异由 ReadInfo / SessionFunctions 参数承接，不派生方法变体）；
  会话包装在 garnet/libs/server/Storage/Functions/MainStore/ReadMethods.cs。
  rust 侧 ReadProbeResult / MemRead（session/raw/mod.rs:29、:40）已经是该状态机的
  rust 化枚举，变体膨胀发生在枚举之上的包装层。

修法建议
抽统一读上下文（前缀 buf、KeyTag、with_size 标志、unprotected 标志收敛为结构体），
单点 read_core 驱动「内存探测 -> 磁盘候选回退」，包装面只留 with 回调闭包与
async 编排两类出口；探测模板收敛为一次实现。行为零改动，验收 = 全部现有调用点
（含 next/rmw-atomic-read-modify-write-window.md 取证引用的读侧位点）改接新口后
语义不变。与 next/db-raw-session-write-tiny-files.md 认领时协调同目录布局。
