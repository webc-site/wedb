优先级：低
来源：next/muse.db.md 条 6 与条 7 合并立项（两轮审查各执半句，均为文档注释级收口，
无代码改动）。取证基线：主仓 dev 当下代码。

问题
双日志引擎分层说明缺失：waof WalLog（物理 WAL，对标 TsavoriteLog）与 whlog
HybridLog（混合日志，对标 AllocatorBase）同为环形页缓冲加刷盘流水线形态，新人易误
判重复实现；whlog 根文档只对标 AllocatorBase 未提与 waof 的分工边界。另 GroupCommitPipeline
的两种 Step 注入（WalCommitStep / FlushStep）同构不同参，两 Step 头未注明共用内核，
同样易被误判为待合并重复。

取证
- wedb/waof/src/lib.rs 顶层文档（「wal/（物理层，TsavoriteLog 对标）与 aof/（语义层）」
  讲了 waof 内部分层，未提 whlog 分工）；wedb/whlog/src/lib.rs:1-9 顶层文档只写
  「对标 C# Tsavorite AllocatorBase/IDeltaLog 系列」，无 waof 互指。
- 共用刷盘截断内核已在 wdev 单点：wedb/wdev/src/device.rs Device::flush_range_aligned /
  truncate_begin_until（两 crate 均经此，无重复下沉）。
- Step 注入形态：wedb/wbase/src/group_commit.rs:48 pub struct GroupCommitPipeline；
  wedb/waof/src/wal/flush.rs:138 struct WalCommitStep；wedb/wkv/src/store/flush.rs:180
  struct FlushStep——两 Step doc 各自描述职责，均未注明共用 GroupCommitPipeline 内核。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs
  CommitAsync（WAL 提交流水线）与 AllocatorBase.AsyncFlushPagesForSnapshot（页刷盘
  流水线）在 C# 也是两套同形流水线，分层本身正确。

修法建议
whlog/src/lib.rs 与 waof/src/lib.rs 顶层文档各加两三行互指：物理 WAL（先写日志，
事务提交序）vs 混合日志（存储引擎主日志，页缓冲 + 驱逐），共用刷盘截断原语在
wdev::Device；WalCommitStep 与 FlushStep 两 struct 的 doc 各补一行「共用内核
wbase::GroupCommitPipeline，本 Step 仅注入批次目标与水位语义」。纯注释，零代码改动。
