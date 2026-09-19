核销：本票（原自 next/db-read-cache-probe-unify.md 认领细化入 ing）指认的缺口
分两步在 dev 全部落地：机制主体由 cd44779（实现提交 f5230fc，wkv raw 读内核三套
重复机制收敛单点）消解；票面残余（判据换算映射单点与对位注释、批读页驻留判定
共享谓词）由并行分支 ff0f6bc（经 8696671 并入 dev）落地。本棒零代码改动、不开
worktree，仅核销归档。核销 2026-09-20。取证基线：主仓 dev cdf426b（含 cd44779
与 8696671）当下代码逐条复核。

逐条判定

一、判据映射单点化（RcVisit 与 ReadProbeResult/MemRead 换算收敛为一个映射
函数并注释互指）
判定：残余成立，且已由 ff0f6bc 落地，本棒不重做。
证据：cd44779 后两条探测域各经各自走查单点一次性折入控制流枚举
（RcVisit 在 find_in_read_cache 折入 RcWalk、ReadProbeResult 在
trace_back_for_key_match 折入 MemBack），无跨域换算面；ff0f6bc 按票面修法将
find_in_read_cache 内的 RcVisit -> RcWalk match 抽出为 map_rc_visit 单点函数
（wedb/wkv/src/session/raw/read.rs），并在其文档与 RcVisit 枚举文档
（read_cache/mod.rs）双向补写与 ReadProbeResult 的判据对位说明（Found ↔
Found ↔ SUCCESS、Next(prev) ↔ Miss(prev) ↔ 沿 PreviousAddress 续链、Gone ↔
Retry ↔ RETRY_LATER），注明 C# FindInReadCache 与 InternalRead 共用同一
OperationStatus、rust 因域不同分立枚举、换算仅此一处。MemRead 终态由
drive_mem_read 驱动环单点收编、环外只暴露 MemDrive；inplace.rs 的
with_record/RcVisit 消费属写侧 InternalDelete/TryFindRecordForUpdate 域，
非本票读侧范畴，维持不动。

二、页驻留判定与链走查在 read_cache 与 raw 读侧重复实现，提共享谓词
判定：链走查部分已被 cd44779 消解；批读驻留判定残余已由 ff0f6bc 落地。
证据：整链走查一处（find_in_read_cache）、反向回溯走查一处
（trace_back_for_key_match），主路径（try_read_mem）与多候选扫描
（try_read_mem_fallback）共用两单点；read_cache 目录侧只保留单条判读
（with_record + classify_record_at，RC 窗口驻留判定仅在该函数快慢两路出现，
属 seqlock 双检固有形态非跨文件重复）；window.rs prev_address_of/
skip_read_cache 对标 C# SkipReadCache 独立方法，消费者为写侧/检查点/紧缩端口
（skip_read_cache_addr 单点收敛两端复制），与 raw 读链无重叠。残余：
batch.rs 预取分派内手写区间比较判内存驻留，ff0f6bc 改接 whlog 判定核单点
AddressSnapshot::region_in_memory。

三、Promote 决策从 try_read_mem_fallback 抽出为单点函数
判定：已被 cd44779 消解。
证据：promote_immutable_read_hit 为内存不可变区命中晋升唯一决策函数，全仓
唯一调用点在 trace_back_for_key_match 命中臂，票面指认的两处散落分支已合一
（f5230fc 提交说明：promote 判定两处合一）；read_from_disk 回填臂为磁盘回填
域分派（对标 C# GetFromDiskCallback 独立路径，与 CopyFromImmutable 在 C# 中
即两处分派），两臂共用 cas_mount_copied_frame 挂载内核，目的地次序口径在
promote_immutable_read_hit 文档注明与磁盘回填臂严格一致。

附注：本棒初版核销稿（措辞为全案已消解、未含 ff0f6bc 落点）被并行分支的
wip 快照提交 f4ca4f6 先于本棒入库，且缺原票存档正文；本稿按 dev 合流后实况
修正并补全原票正文，属文档纠偏，零代码改动。

原票存档

优先级：中低
来源：next/agy.db.md 条 16 与 next/muse.db.md 条 14 两轮同题合并。取证基线：主仓 dev 当下代码。

问题
读缓存探测判据与主日志读链判据各自为政：RcVisit 三态与 ReadProbeResult/MemRead
两套枚举语义重叠（命中 / 续链 / 回链头重探），页驻留判定与链走查在 read_cache 三
文件与 raw 读侧重复实现，Promote 决策分支散落 read.rs 缺统一状态机。

取证
- wedb/wkv/src/read_cache/mod.rs:42 pub enum RcVisit<R>（Found / Next(u64) / Gone——
  Gone 语义「回链头重探，绝不可按不存在降级」）；read_cache 目录三文件共 429 行。
- wedb/wkv/src/session/raw/mod.rs:29 ReadProbeResult<T>（Miss(u64) / Tombstone /
  Retry / Found）与 :40 MemRead<R>（Done / OnDisk / Retry——doc 自注「严格对照
  InternalRead.cs InternalRead 单遍分类」）。
- Promote 决策：wedb/wkv/src/session/raw/read.rs:638 promote_immutable_to_read_cache
  与 :508 try_read_mem_fallback 内散落分支。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/
  ReadCache.cs FindInReadCache 与 InternalRead.cs InternalRead——C# 读缓存探测结果
  直接复用主读链的状态枚举（NOTFOUND / RECORD_ON_DISK / RETRY_LATER 同一套），
  判据同源不重复定义。

修法建议
两套枚举不合（域不同：RcVisit 带闭包泛型），但判据映射单点化：raw 读侧的
ReadProbeResult -> RcVisit 换算收敛为一个映射函数并注释互指（C# FindInReadCache
与 InternalRead 同源枚举的对位说明）；页驻留判定（is_in_memory 类）与链走查续链
条件若在两侧重复，提共享谓词；Promote 决策从 try_read_mem_fallback 抽出为单点
函数。行为零改动，验收 = 读路径返回值逐分支等价。
