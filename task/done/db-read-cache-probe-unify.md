核销：本票（原自 next/db-read-cache-probe-unify.md 认领细化入 ing）指认的全部
缺口已被上游 cd44779（实现提交 f5230fc，wkv raw 读内核三套重复机制收敛单点）
落地消解，残余为零，零代码改动，不开 worktree。核销 2026-09-20。
取证基线：主仓 dev 5fb1621（含 cd44779）当下代码逐条复核。

逐条判定

一、判据映射单点化（RcVisit 与 ReadProbeResult/MemRead 换算收敛）
判定：已被 cd44779 消解。
证据：合流后不存在跨域换算——RcVisit 生产读侧唯一消费点在
wedb/wkv/src/session/raw/read.rs find_in_read_cache（RcVisit 三态就地折入
RcWalk，该函数即「RC 链走查 + 三态换算」单点，主路径与多候选扫描共用）；
ReadProbeResult 唯一生产点在 probe_hlog_record、唯一消费点在
trace_back_for_key_match（就地折入 MemBack，同样两路共用）；MemRead 由
drive_mem_read 驱动环单点收编（read.rs try_read_mem 的 Retry 仅上抛给本环），
环外只暴露 MemDrive 二终态。inplace.rs:388 的 with_record/RcVisit 消费属写侧
InternalDelete/TryFindRecordForUpdate 域，非本票读侧范畴。互指注释已就位：
ReadProbeResult 文档对照 InternalRead.cs:105-131（raw/mod.rs），RcVisit 文档
对照 ReadCache.cs:FindInReadCache（read_cache/mod.rs），with_record 文档反向
指回 find_in_read_cache 单点（read_cache/window.rs）。
票面要求的「ReadProbeResult -> RcVisit 映射函数」在合流后无此物可读——两条
探测域各经各自单点走查函数一次性折入控制流枚举，无中间换算面可收敛。

二、页驻留判定与链走查在 read_cache 与 raw 读侧重复实现，提共享谓词
判定：已被 cd44779 消解。
证据：整链走查一处（find_in_read_cache）、反向回溯走查一处
（trace_back_for_key_match），read.rs 主路径（try_read_mem）与多候选扫描
（try_read_mem_fallback）逐字共用两单点（read.rs:569/579 与 :653/664）；
read_cache 目录侧只保留单条判读（with_record + classify_record_at 一处，
RC 窗口驻留判定 abs∈[head,tail) 仅在该函数内快慢两路出现，属 seqlock 双检
固有形态非跨文件重复）。window.rs prev_address_of/skip_read_cache 对标 C#
SkipReadCache 独立方法，消费者为写侧/检查点/紧缩端口（skip_read_cache_addr
单点收敛两端复制），与 raw 读链无重叠。

三、Promote 决策从 try_read_mem_fallback 抽出为单点函数
判定：已被 cd44779 消解。
证据：promote_immutable_read_hit 为内存不可变区命中晋升唯一决策函数，全仓
唯一调用点在 trace_back_for_key_match 命中臂（read.rs:479），票面指认的
「read.rs:638 与 :508 散落分支」两处已合一（f5230fc 提交说明：promote 判定
两处合一）；read_from_disk 的回填臂为磁盘回填域分派（对标 C#
GetFromDiskCallback 独立路径，与 CopyFromImmutable 在 C# 中即两处分派），
两臂共用 cas_mount_copied_frame 挂载内核，目的地次序口径在
promote_immutable_read_hit 文档注明与磁盘回填臂严格一致。

batch.rs 复核：MGET 内层 loop 已改接 drive_mem_read（batch.rs:125），无平行
重试环残留。

原票存档
