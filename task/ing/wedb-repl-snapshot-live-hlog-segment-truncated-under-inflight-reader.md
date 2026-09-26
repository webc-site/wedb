甄别结论：通过（fix 席 r27，2026-09-27，定级 P2——复制下发可用性与收敛性契约分叉，非主端数据丢失）。逐锚现码双侧复跑属实：C# 侧 TsavoriteCheckpointReader.cs:144-179（GetStoreHLogDevice 开活体 Store/hlog 设备）与 :52-59（区间取 hybridLogFile Start/EndAddress）、CheckpointStore.cs:247（TryGetLatestCheckpointEntryFromMemory→TryAddReader）、:196-210（按最旧存活读者条目 Log.ShiftBeginAddress(..., truncateLog:true) 钳活体删段，注释原文在位）全实；承重锚 GarnetClusterCheckpointManager.cs:44 PerformAutomaticCleanup=>false 与 Checkpoint.cs:54-58 集群短路属实。rust 侧 snapshot_transmission.rs:87/:115-133/:339-352/:348（IOERR 上抛无收敛）、create.rs:389（无条件 release_history_until）、shift.rs:164/:178-186/:204-213（三删段口统一 min(delete_floor)）、address.rs:191-192（fetch_max 单调）、checkpoint_store.rs:184-239（仅挡 token 文件 purge、无尾部钳）、replica_sync_session.rs:442（读者释放点）均属实；wdev handle.rs:321/:356/:413 SegmentNotFound 回错路径属实。因果链现码闭环可达：副本 A 在传（reader 持 E1、起点 B1）→ 副本 B 接入触发 take_on_demand_checkpoint（replica_sync_session.rs:367）→ E2 发布 begin B2>B1 → create.rs:389 无条件抬地板并 truncate_until_address(B2)、[B1,B2) 段 unlink → A 游标落入即 SegmentNotFound 整单失败；现码确无闸（wait_safe_read_only_drained 仅封 whlog 纪元内读者，传输读走 device.read_range 免纪元守卫，scan.rs:320 注释自证；检查点发布不被在传会话阻塞）。查重：关键符号（release_history_until / delete_floor / 在传读者 / SegmentNotFound）在 doc/zh/deviations.md 与 task 五池仅命中本票，§74 系删段吞错、§95 系无盘键门，均不圈此面；缺陷现码未自证收口。方案接线含糊由本席钉死（原票未落，须照此执行）：delete_floor 为 fetch_max 单调且 wcpr 看不见复制层，「抬升取 min(新 begin, 最旧在传读者 begin)」须由 whlog 侧增设通用 reader-pin 水位承接——复制层正向写入（上层依赖下层，合单向分层；严禁 whlog 反依赖复制态），raise 内部按 pin 取 min，pin 解除时由复制层以同一 release_history_until 单通道滞后重放补删（复用既有水位，非第二套删段通道）；登记时隙钉死在 SendReaderGuard 取条目即登记（replica_sync_session.rs:329 reader.replace，:55-59 replace 内 release 旧持新；注销面 :442 与 :67-70 Drop 同走 :60-64 remove_reader），不得晚至 send_store_checkpoint:116 算出 start 才登记（那仍留首块读前被删窗）；pin 水位须按「全体在册在传读者的最旧 begin」聚合维护（多副本并发下发时，单会话 replace/release 不得覆写他会话的 pin，登记/注销即挂既有条目读者计数 add_reader/remove_reader 单点、变化时重算集合最旧 begin），否则退化为单会话标量。原票订正注记「集群模式删段的唯一许可即该尾部钳制」表述过头：C# DatabaseManagerBase.cs:444/:453/:462 压缩路径亦删段且不受读者钳，rust 侧该路径已被 delete_floor 统一收口，本票不据此立第二闸。微瑕：scan.rs 读后收敛签名实位 :328（票面 :327）。可执行度成立：改动点精确到文件:函数，夹具可循现码 checkpoint_import.rs / replication_data_source.rs 的真设备真会话范式，撤修复则 unlink 确定发生、read_range 必回 SegmentNotFound、整单必红。格式纯粹、双侧路径齐全。派沙箱席 b02c。

审核结论：通过（订正后，供 fix.md 直接消费）

审核席逐锚点亲验双侧源码属实，订正注记：
一、补承重锚：C# 集群模式删段的唯一许可即该尾部钳制——GarnetClusterCheckpointManager.cs:44 PerformAutomaticCleanup=>false 使 Checkpoint.cs:54-59 的自动截断在集群模式短路；执行方案落地时以此固化「在传读者钳制为集群唯一删段闸」的契约链。
二、方案 2 优先形态确认：既有 CheckpointEntry 读者计数（SendReaderGuard 下发全程在册，replica_sync_session.rs:442 才释放）即现成在传读者信号，登记/注销点直接挂该计数集，改动面最小；scan.rs 式读后收敛不适用传输面（与 meta 起点对不上），勿引入。
三、格式与查重合规：deviations 与 task/ 各池关键词零命中；§74(b)/§95 分属删段吞错与无盘键门，不圈此面。

集群全量同步直读活体 hlog 段流，主端下一轮检查点发布可在传输在途时物理删段打断下发

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 磁盘复制下发主存储 hlog 时，STORE_HLOG 数据源经 CreateCheckpointDevice 走
GetStoreHLogDevice 打开的是主端活体分层日志设备（TsavoriteCheckpointReader.cs:144-179，
FileDescriptor "Store/hlog"），并非按 token 拷贝的冻结副本；下发区间取
logFileInfo.hybridLogFileStartAddress 到 hybridLogFileEndAddress（同文件 :52-59）。
既然是活体设备，C# 靠两层保护保证在途下发不被后续截断打断：其一，副本会话对
CheckPointEntry 持读者计数（CheckpointStore.cs:247 TryGetLatestCheckpointEntryFromMemory
调 try_add_reader），DeleteOutdatedCheckpoints 遇 TrySuspendReaders 失败即停链，被引用条目
不入淘汰；其二，也是关键，CheckpointStore.cs:196-210 在淘汰尾部专门按「最旧仍被活跃读者
引用的条目 curr」调 store.Log.ShiftBeginAddress(curr 的 hybridLogFileStartAddress,
truncateLog:true) 把活体日志的删段下界钳到该最旧在传条目的起点——注释明示「This is safe
because curr is the oldest entry still referenced by active readers」。即活体 hlog 的物理
删段永不超过仍有在传读者占用的最早起点，从契约上排除了在途段被删。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
Rust 沿用统一检查点模型下的活体直读：snapshot_transmission.rs:115-133 以
start = meta.hlog_meta.begin_address 扇区对齐为下界、end = 设备文件幅面为界构造
HlogSegmentSource，read_next_chunk（同文件 :339-352）直接 device.read_range(offset, want)
读主端活体设备，任一读错即 map_err 成 "IOERR device read at {offset}: {e}"（:348，含 {e} 尾缀） 上抛整单失败，无
scan.rs cold_read_page（whlog/src/scan.rs:327-348）那样的读后复验 begin、钳位跃迁收敛。
删段侧：检查点发布第 10 步 create.rs:387-393 无条件 release_history_until(meta.begin)，
whlog/src/hlog/shift.rs:178-186 先 raise_delete_floor(floor)、再 truncate_until_address(floor)；
shift_begin_address 与 truncate 亦统一 min(delete_floor) 钳制（同文件 :160-168、:204-213）。
delete_floor 仅两处被抬升（grep 全仓确证）：create.rs:389 检查点发布、wkv/src/store/cpr_host.rs:613
恢复装配，均为「最新发布检查点的 begin」，fetch_max 单调不回退（whlog/src/address.rs:191-192），
无任何在传读者参与钳制。Rust 的读者闸门 checkpoint_store.rs:192-239 delete_outdated_checkpoints
仅挡 purge_checkpoint（按 token unlink index/meta 文件集），且对照 C# 移植时整段省略了
CheckpointStore.cs:196-210 的按最旧存活条目钳活体 hlog 删段的尾部保护（其 doc 注释 :184-191
自述仅对位文件删除面）。副本会话 replica_sync_session.rs 在途只钉 AOF 截断线（pin driver 预锁
truncated_until）与文件读者闸门，活体 hlog 段窗无任何钉。故「在传读者保护删段下界」这一 C#
关键契约在 Rust 侧缺位。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
主端在副本全量下发进行中发布新一轮检查点（周期检查点、或为另一副本触发的按需检查点皆可），
新检查点 begin 高于当前在传条目 begin，第 10 步 release_history_until(新 begin) 随即抬地板并
truncate_until_address，把 [在传条目 begin, 新 begin) 区间内的历史段物理 unlink。在传的
HlogSegmentSource 恰好从该区间下界 begin 起读，游标推进落入已删段时 read_range 回
Error::SegmentNotFound（wdev/src/segmented_device/handle.rs:321/:356/:413），转 "IOERR device read"
整单失败。只要检查点节奏快于下发耗时（大库、慢链路常态），全量同步反复失败、副本无法收敛，
退化为主副持续重同步。此非主端数据丢失（数据仍在新检查点与 AOF 内），而是复制下发可用性
与收敛性的真实契约分叉：物理删段未受在途读者安全纪元/水位保护，提前删除打断流式读，命中
review.md 板块 2.2「检查点快照与辅助存储协同：物理文件删除必须受安全纪元与延迟到期队列保护，
严禁提前删除留崩溃窗口」。deviations.md 与 task/done 全仓无该面登记或收口（grep 在途传输删段/
release_history_until/delete_floor/oldest active 零命中），属新发现。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/snapshot_transmission.rs:send_store_checkpoint（锚订正：立案时名 send_checkpoint_snapshot_to_client 失实，现树 snapshot_transmission.rs:87） / HlogSegmentSource::read_next_chunk
wedb/wcpr/src/manager/create.rs:第 10 步 release_history_until(meta.hlog_meta.begin_address)
wedb/whlog/src/hlog/shift.rs:release_history_until / shift_begin_address / truncate（delete_floor 钳制）
wedb/whlog/src/address.rs:raise_delete_floor / delete_floor
wedb/wedb/src/server/replication/checkpoint_store.rs:delete_outdated_checkpoints（仅挡文件 purge，缺 hlog 尾部钳制）
wedb/wedb/src/server/replication/replica_sync_session.rs:send_checkpoint_and_recover（仅钉 AOF 线与文件读者，未钉 hlog 段窗）

对应 c# 文件与函数：
garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/TsavoriteCheckpointReader.cs:CreateFileDataSource / CreateCheckpointDevice / GetStoreHLogDevice
garnet/libs/cluster/Server/Replication/CheckpointStore.cs:DeleteOutdatedCheckpoints（:196-210 按最旧存活条目 ShiftBeginAddress 钳活体 hlog 删段）
garnet/libs/cluster/Server/Replication/CheckpointStore.cs:TryGetLatestCheckpointEntryFromMemory（读者计数）

精炼执行方案：
1. 补「在传读者钳活体删段下界」单点：由持有检查点条目读者集与 begin_address 的一侧（wedb 复制层，
   与既有 AOF pin driver 同处一层，严禁让 whlog 反向依赖复制态）在放行删段前，计算「最旧活跃读者
   条目的扇区对齐 begin」，据此约束本轮可下发的删段地板；即在检查点发布的 release_history_until
   与 shift_begin_address 的 truncate 目标上并入该在传读者水位（delete_floor 抬升取
   min(新检查点 begin, 最旧在传读者 begin)），使活体 hlog 段永不在有在传读者时被 unlink。
2. 或等价按 C# DeleteOutdatedCheckpoints 尾部对位：在检查点条目开始下发前登记其为受保护最旧条目、
   下发结束（成功/失败）后注销，注销点触发滞后补删，复用既有 delete_floor + wait_safe_read_only_drained
   单机制，不新建第二套删段通道。
3. 测试验证点：构造慢链路夹具令下发跨越一轮新检查点发布（新 begin 越过在传条目 begin），断言下发
   全程源段存活、read_range 无 SegmentNotFound、下发完整成功；下发结束注销后断言滞后补删按新地板回收
   历史段；对照当前实现应先复现下发中途整单失败作为回归锁。
