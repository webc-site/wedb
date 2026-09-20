# bftree-restore-prestage：删除 RestoreTree 目录回退扫描与刷盘代数状态机

## 判定

票面成立。逐字核对 C# 原作 `libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:RestoreTree`（:273-368）：
冷态存根（TreeHandle == 0）在独占锁内重读存根、查 `liveIndexes` 复用赢家树，否则
`workingPath = LogDataPath(HashKeyToPrefix(key))` 只做一次 `File.Exists(workingPath)`；
不存在即 Debug.Assert + LogWarning + return false（预置不变量破坏），存在即
`BfTreeService.RecoverFromCprSnapshot(workingPath, ...)` 后 `RegisterIndex`，
最后释放 X 锁发 RIRESTORE。全函数无 `Directory.EnumerateFiles`、无文件名解码、无最大地址择优。

刷盘件的消费在 C# 里由地址精确寻址，而非目录扫描择优：
`RangeIndexManager.cs:PreStageAndRegisterPending`（:520-565）以调用方传入的
`srcFlushAddress` 拼出 `LogFlushPath` 单文件 `File.Copy` 覆盖 `data.bftree`，
源缺失时 LogError 且明文「NOT falling back to any other flush file」；
它的三个调用点在生命周期钩子上：`RMWMethods.cs:1558`（RIPROMOTE PostCopyUpdater 冷态）、
`GarnetRecordTriggers.cs:238`（PostCopyToTail 冷态）、检查点恢复期
`OnRecoverySnapshotRead`。`RangeIndexManager.cs:970-972` 的文档注释还专门强调
「一个键可能有多个刷盘件，PreStageAndRegisterPending 用精确源地址定位那一个」。
也就是说，C# 语义下「取最大地址刷盘件」本身就是偏离——正确版本由存根所在记录的
源地址唯一确定，不是目录里地址最大的那个。

rust 现状（`wbftree/src/manager/lifecycle.rs:238-264`）在 `get_or_open_tree` 里
`addr_flush_scan_pending()` 为真时调 `flush_files()` 全目录枚举、按 `key_id` 过滤、
取最大 `addr` 覆盖 `data.bftree`；为把这个 O(N) 扫描挡住，配了
`addr_flush_gen` / `addr_flush_settled_gen` / `addr_flush_scan_pending` /
`addr_flush_scan_token` / `settle_addr_flush_scan` / `notice_addr_flush_files`
一整套人造代数与竞态闭环（`manager/mod.rs:203-334`、`manager/flush.rs:70`、
`lifecycle.rs:361`）。这套机制在 C# 无对应物，属于为规避自造开销而叠加的复杂度。

预置面在 rust 已经齐备，扫描分支没有任何生产路径依赖：
1. 刷盘态存根读路径 `wkv/src/range_index/stub.rs:180,245` 先 `promote_range_index_to_tail`，
   其 `post_promote_tail_patch`（`promote.rs:260-284`）在源树句柄为 0 时转调
   `pre_stage_and_register_pending(id_key, src_addr)`，对位 C# RIPROMOTE PostCopyUpdater 冷态；
2. 日志复制入尾 `wkv/src/compact.rs:81` 转调同一入口，对位 PostCopyToTail 冷态；
3. 启动检查点恢复 `wkv/src/store/cpr_host.rs:172` 先 `recover_all_trees_from_dir`
   批量把检查点快照预置为 `data.bftree` 并注册 pending，对位 `OnRecoverySnapshotRead`
   （本仓无主日志逐 stub 回放，改按目录枚举快照批量预置，理由已写在
   `manager/replication.rs:145-150`）。

三条通道都无条件 `fs::copy` 覆盖工作文件，因此冷读时 `data.bftree` 即权威版本，
目录扫描只是同一结果的更贵、且语义更错（可能选中别的记录地址世代之外的文件）的副本。
本仓自动升阶/懒降阶面（SKILL 集合分层存储修订）不新增预置需求：升阶与换入走
`publish_tree_from_snapshot_locked`，快照直接 rename 进数据路径，与扫描无关。

## 方案

一、`wbftree/src/manager/lifecycle.rs:get_or_open_tree`
删除 `addr_flush_scan_pending` 门控的整段扫描分支（含 `flush_files` 调用、
`latest` 择优、`settle_addr_flush_scan` 回退）与「刷盘快照选择契约」「IsRecovered
绕过刷盘快照」两段注释；保留其后的 pre-stage 不变量检查（磁盘后端且
`data.bftree` 不存在即 `Error::Recovery` 显式报错），这与 C# 的
`File.Exists(workingPath)` + 预置不变量断言一一对位。`is_recovered` 判定随扫描分支
一并消失，恢复语义统一为「只打开 `data.bftree`」。

二、`wbftree/src/manager/mod.rs`
删除 `addr_flush_gen`、`addr_flush_settled_gen` 两个字段及其长注释、构造器初始化，
删除 `addr_flush_scan_pending` / `addr_flush_scan_token` / `settle_addr_flush_scan` /
`notice_addr_flush_files` 四个方法；`AtomicU64` 若再无使用一并从 import 收掉。

三、`wbftree/src/manager/flush.rs:on_flush_address` 与 `lifecycle.rs:pre_stage_and_register_pending`
删除 `notice_addr_flush_files()` 调用及其竞态闭环注释。刷盘入口本身不改：
带地址命名、地址必填、冷树复制工作文件，全部对位 C#。

四、`wbftree/src/manager/replication.rs`
`flush_files` 枚举器保留（`on_truncate` 按地址阈值回收、`remove_addr_flush_files`
按 key_id 清旧世代仍是 C# 有的行为），文档注释中「三处消费方」改为两处，
删去惰性恢复取最大地址件的表述。

五、测试与文档口径同步
`wbftree/tests/manager_and_stub/manager.rs` 中依赖扫描回退的用例改为显式走
`pre_stage_and_register_pending` 预置通道（对应 C# 真实的冷读序），
`test_get_or_open_tree_copy_failure_propagates` 改为断言预置拷贝失败传播；
`wbftree/README.md`、`readme/zh.md`、`readme/en.md` 的「取最大地址刷盘件复制为数据文件」
表述改为「预置不变量：冷读只打开 data.bftree」；`lib.rs` 接线现状注相应核对。

## 验收

`cargo check -p wbftree -p wkv` 通过、无死代码告警；全仓再无任何
`addr_flush_gen` / 目录择优扫描痕迹；`flush_files` 只剩截断与换代清理两路消费方。
