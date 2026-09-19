优先级：低（打磨/污染扩散：注释锚定与追溯链失真，会把后续转写会话引向不存在的文件与文档）
分拣注记（源 next/qw.net.md 第 11 轮 net 条 9；浅核 2026-09-19 HEAD 39ea7e58：假文件名 snapshot_transmission.rs:5 唯一命中、m4-checkpoint-import 三处命中（:8 / replica_sync_session.rs:124 / replica_diskbased_sync.rs:65）、task 三档与 git log --all 均无该文档；台账查重：task/ing/gate-anchor-drift-reclean.md 射程为参与门判定的函数级锚点，与本票不交叠）

复制链三处注释失真：模块头列出不存在的 C# 源文件名，另三处引用已消失的在途文档

现状事实（主仓 dev，取证 HEAD 39ea7e58）

- wedb/wedb/src/server/replication/snapshot_transmission.rs:5 模块头把
  `TsavoriteSnapshotReader.cs` 列为对位源文件；garnet/libs/cluster/Server/Replication/
  PrimaryOps/DiskbasedReplication/ 目录下无该文件，类 TsavoriteSnapshotReader 实名落在
  同目录 TsavoriteCheckpointReader.cs:14（`internal sealed class TsavoriteSnapshotReader :
  ISnapshotReader`），其构造函数在 :40 —— 文件名与类名不同名。全仓该假文件名仅
  snapshot_transmission.rs:5 一处命中。
- 三处引用 task/ing/m4-checkpoint-import.md 作为设计依据：
  snapshot_transmission.rs:8（自称「条 7」）、
  wedb/wedb/src/server/replication/replica_sync_session.rs:124、
  wedb/wedb/src/server/replication/replica_diskbased_sync.rs:65（自称「条 6」）。
  该文档在 task/ing、task/done、task/reject 三档均不存在，`git log --all --
  '*m4-checkpoint-import*'` 亦零命中，即指向一条从未在册的路径，被引的两条决策
  （「引擎不随 attach 重构」「检查点仅在 FullResync 导入链上换引擎」）无原委可核。
- 门拦不住这类失真：js/check.js 的符号断言只认函数文档注释里 `路径.cs:符号` 形式
  （判据基准另见 task/ing/gate-anchor-drift-reclean.md:30-33），模块头的散文式文件名
  与 task/ 文档路径都在射程外；但按该文件名去写 js/check/ignore 语料会直接落空。

目标形态

1. snapshot_transmission.rs:5 改写为 `TsavoriteCheckpointReader.cs`，并就地注明
   「C# 侧文件名与该类不同名（类实名 TsavoriteSnapshotReader）」，避免下一位读者按
   类名反查文件再错一次。
2. 三处 m4-checkpoint-import 引用删路径、把结论就地写实：该两条决策本身已是可自证的
   现状陈述（统一检查点模型下 STORE_SNAPSHOT/STORE_INDEX 无对位段、attach 不换引擎），
   注释自述即可，不再挂任何指向不存在文档的引用；若确有在册等价文档（如 task/done/
   下的检查点导入相关票）能承载原委，可改指其实名，二选一，不留死链。
3. 顺带把该三处注释的 C# 锚点补齐为 `文件.cs:符号` 形式（对位
   TsavoriteCheckpointReader.cs 的构造与 Read 面），使散文引用变成门可核的锚点。

C# 对位

- garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/
  TsavoriteCheckpointReader.cs:TsavoriteSnapshotReader（:14 类声明、:40 构造）。
- 同目录 SnapshotTransmissionDriver.cs / RangeIndexSnapshotReader.cs /
  RangeIndexFileDataSource.cs / RangeIndexFileTransmitSource.cs / FileDataSource.cs /
  TsavoriteMetadataSource.cs / TsavoriteMetadataTransmitSource.cs 为模块头其余条目，
  实测均在册，不改。
- 装配点：garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/
  ReplicaSyncSession.cs:141 的 AddReader 侧。

门禁与验收判据

- cargo check -p wedb --all-targets 零错误零警告（纯注释改动，行为不变）。
- grep 判据：`TsavoriteSnapshotReader.cs` 在 wedb/ 下零命中；
  `m4-checkpoint-import` 在 wedb/ 下零命中（现状三处命中）。
- bun js/check.js 退出码与改动前一致（不得因新写锚点新增 symbol_fail；写不准就不写
  符号位，改散文表述，先例 wedb/wbase/src/align.rs:115-117）。

坑与边界

- 只改注释与文档指向，不动发送序、段划分与元数据载荷口径（rust 元数据 key_id 16B LE
  替代 C# keyHash 32B ASCII 是已声明偏离，勿在本票里重议）。
- 与在途票 task/ing/gate-anchor-drift-reclean.md 不交叠：那票射程是参与门判定的函数级
  虚构锚点与 19 个实现缺失家族逐族甄别，本票是模块头散文文件名与失效 task/ 文档路径。
  若那票先落并顺带改到同文件注释，开工前按当下 HEAD 重取行号、命中即让位。
- 禁以「删掉注释」代替「写准注释」：三处结论本身是有价值的现状声明。
- 若为第 2 条新增 `File.cs:Symbol` 形式锚点，符号名必须是 C# 真实存在的（js/check.js
  对虚构符号名硬失败），不确定时宁可不挂符号位。

盘点补记（qw13.invA replication-comment-anchor-drift）：dev e75716e 复核四处俱在：snapshot_transmission.rs:5 TsavoriteSnapshotReader.cs（garnet 实名 TsavoriteCheckpointReader.cs）、:8 与 replica_sync_session.rs:124、replica_diskbased_sync.rs:65 引 task/ing/m4-checkpoint-import.md（task/ing 已清空，死引用坐实）。纯注释棒不变。

## 甄别记录（开发子代理，认领后现刻 HEAD 9c903e4，裁决：成立）

- 假文件名核实：garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ 实测无
  TsavoriteSnapshotReader.cs；类实名 TsavoriteSnapshotReader 落在同目录 TsavoriteCheckpointReader.cs:14
  （构造 :40），文件名与类名确不同名。rust 侧该假名仅 snapshot_transmission.rs:5 一处；同文件 :100
  的 TsavoriteSnapshotReader 是类名、本即正确，票面未误伤。
- 死引用核实：task/ing/m4-checkpoint-import.md 于 task/ing、task/done、task/reject 三档俱无，
  git log --all -- '*m4-checkpoint-import*' 零命中。命中三处（现刻行号
  snapshot_transmission.rs:8、replica_sync_session.rs:124、replica_diskbased_sync.rs:67，
  票面 65 为取证后行漂移）。
- 模块头其余条目核实：SnapshotTransmissionDriver.cs / RangeIndexSnapshotReader.cs /
  RangeIndexFileDataSource.cs / RangeIndexFileTransmitSource.cs / FileDataSource.cs /
  TsavoriteMetadataSource.cs / TsavoriteMetadataTransmitSource.cs 均在册，未改。
- 台账查重：task/ing 19 票无同射程条；在途 anchor-bare-diskann-prefix 为 wvector 裸前缀族，
  与本票文件集不相交（该票本棒期间并行落 dev，未碰撞）。票面点名的
  task/ing/gate-anchor-drift-reclean.md 现已不在三档（仅存于 6a39bef 历史），其射程与本票
  不交叠的判据仍按票面采信。

## 落地（dev FF 至 888d447；载荷 97d5c4b，认领 9c903e4）

- snapshot_transmission.rs:5-13 模块头：假名订正为 TsavoriteCheckpointReader.cs，就地注明
  「C# 侧文件名与类名不同名、类实名 TsavoriteSnapshotReader」并给出构造期编目 / 出源面
  GetTransmitSources / 逐块读 FileDataSource.cs ReadNextChunkAsync 三面指向；条 7 原委改写为
  模块头自述（wcpr 检查点产物只有 hlog/index/meta 三类，故 STORE_SNAPSHOT 文件段与
  STORE_INDEX 元数据无对位段），死路径删除；新增登记类名侧全路径锚点
  libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/TsavoriteCheckpointReader.cs:TsavoriteSnapshotReader。
- snapshot_transmission.rs 缓冲池行内注释：类名旁补注「文件名与类名不同名、见模块头第一条」，
  堵住按类名反查文件的第二次错。
- replica_sync_session.rs:122-128 与 replica_diskbased_sync.rs:64-69：删 m4-checkpoint-import
  死路径，「引擎不随 attach 重构」「换引擎仅在 FullResync 导入链」两条结论就地写实（含 C#
  recoverFromRemote → recoverStoreFromToken 的透传口径），并改指在册承载点
  ReplicationManager::disk_resync_strategy（该函数文档已登记 ValidateMetadata 全路径锚点，
  即原委在册处，属票面第 2 条的「改指实名」选项）。
- 未越面：发送序、段划分、元数据载荷口径（key_id 16B LE 替代 keyHash 32B ASCII 的已声明偏离）
  一字未动。

## 符号位取舍（第 3 条射程的实际处置）

- GetTransmitSources 与 FileDataSource.cs:ReadNextChunkAsync 在注释里以散文实名给出、未挂
  全路径 .cs:符号 锚：两文件已整文件登记在 js/check/ignore/cluster.yml:496、:502（无需实现语料），
  写全路径锚会触发 check.js 的 ignore 自动淘汰逻辑回写 cluster.yml（他人语料、500 行量级重排），
  按「不改他人语料」纪律不越面；裸名形态门不采信，故取票面「写不准就不写符号位、改散文表述」
  先例（wbase/src/align.rs:115-117 同族处置）。
- 同理 TryReplicaDiskbasedRecovery（cluster.yml:534 函数级 ignore 在册）与
  ValidateMetadata（已由 replication_manager.rs:832 单点登记）既有锚点形态未改、未复挂，
  避免回写与重复定义新增项。

## 门禁实录（全部在私有树 /tmp/fork/repl-anchor-drift，CARGO_TARGET_DIR=/tmp/ct-repl-anchor-drift）

- cargo check -p wedb --all-targets：改前/改后/并 dev 后三次 exit 0，零警告零错误。
- 定向 cargo nextest（--no-fail-fast，-p wedb）：checkpoint_import + replication_end_to_end +
  replication_pipeline 共 14 项，14 passed / 0 failed。
- bun js/check.js 前后对跑：exit 0 = 0，stdout 逐字节相同（无新增 实现缺失 / 重复定义 /
  虚构锚点，B 层提示枚数不变），js/check/ignore 与 check/miss 零回写。
  并 dev 后再跑仍 exit 0，唯一 yml 变更是 js/check/ignore/storage.yml 的 ResetRevivificationStats
  自动淘汰，来自本棒并入的他人锚点，非本票产物，未提交、随工作区销毁。
- grep 判据（dev 888d447 复核）：TsavoriteSnapshotReader.cs 与 m4-checkpoint-import 在 wedb/ 下
  *.rs 均零命中。
- rustfmt --config-path wedb/rustfmt.toml --check：本票 3 文件零额外 diff。
- 禁跑项遵守：未跑主仓 ./test.sh 与 ./sh/clippy.sh；未动主仓他人脏文件。

## 进度

- [x] 认领（9c903e4，窄 pathspec 覆盖 next 删除与 ing 新增两侧）
- [x] 甄别：两处主张现刻 HEAD 复核俱成立，模块头其余条目反证不改
- [x] 落地：3 文件纯注释订正 + 类名侧锚点登记（载荷 97d5c4b）
- [x] 门禁：cargo check / 定向 nextest / js/check.js 前后对跑全绿
- [x] 并 dev 回枝 + 主仓 FF（重试环第 4 次成，前三次 index.lock 各 sleep 5）+ 归档

