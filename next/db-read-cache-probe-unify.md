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
