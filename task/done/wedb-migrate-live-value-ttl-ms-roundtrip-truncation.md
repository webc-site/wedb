迁移帧/复制快照/RI 树流 TTL 经 Unix 毫秒往返截断，接收端系统性提前 ≤1ms，值与 TTL 两读竞态窗可迁出永生键

审核结论：通过
1. 真实性亲验：live_value.rs:241-246 expire_unix_ms 经 unix_time_in_milliseconds_from_ticks 截断、`_ => 0` 无 TTL 臂在码；值读（:173-175 / :184-190）与 TTL 读（:180 / :195）为两独立 await，:177/:192 fire_live_value_read_hook 恰证间隙路径可定序触达；keys.rs:578-581 Gone 臂只兜值读前已亡，:582-599 Migratable(ttl_ms=0) 照常装帧入 transferred 并随后源端删除——永生键链路逐环坐实。frame_import.rs:367-373、range_index_migration_receive_state.rs:162-163 毫秒回扩在码；range_index_manager_migration.rs:236-239 同形截断在码；replication_snapshot_iterator.rs:394-404 复用同源；wconn/src/record.rs:15/16/23-28 线格式 kind=1/2/4 TTL 字段即 i64 LE expire_unix_ms。C# 侧 MigrateOperation.cs:141 TransmitKeysAsync 整记录序列化发送、UpsertMethods.cs:55-62 InitialWriter 仅 TryCopyFrom 逐位拷贝、MigrateCommand.cs:17-19 Expired 单记录内原子判定、RespClusterMigrateCommands.cs:199/295 SET(in diskLogRecord) 直落，均与票面一致。
2. 非重复非灭失：task 池与全码 grep expire_unix_ms|毫秒往返|unix_ms 仅本票命中；§96/§118 域钉族只裁跨代拼接面，§4 只裁会话入口绝对面钳制饱和算式，均不覆盖本面。frame_import.rs:369-373 引 §143 的论证系「源值本身 16 对齐时粗化等价」的条件命题——SET/GETEX/RENAME 族存非 16 对齐裸 ticks（aof_processor_store_ops.rs:329-330 自证），对这类源值毫秒往返恒丢亚毫秒位；而 §143「全链路禁止粗化」判据恰是本票应兑现的承诺，非在册既定改良裁决覆盖本面。
3. 架构合规：线格式改 ticks 直传对标 AOF TtlWrite 直设单机制（record.rs:28 kind=4 next_expiry 已系 .NET Ticks 原值先例），两读并窗沿用 §118 单探针窗形态，机制收敛无双机制；本仓无向下兼容包袱，线格式变更可接受。PTTL/EXPIRETIME 用户面毫秒读出（keys.rs/storage_session.rs）系 Redis 契约，不在改动面。
4. 格式纯粹，判据可执行。

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 迁移整记录搬运（MigrateSessionKeys.cs:TransmitKeysAsync 序列化 DiskLogRecord）到期戳以精确 .NET Ticks 逐位保真；接收端 RespClusterMigrateCommands.cs:SET(in diskLogRecord) → UnifiedStoreOps.cs:Upsert → UpsertMethods.cs:InitialWriter 仅 TryCopyFrom，Expiration 零换算零精度损失；过期判定 MigrateCommand.cs:Expired 单记录内原子（Expiration < UtcNow.Ticks），不存在值读与 TTL 读两窗。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   发送端 wedb/wedb/src/server/migration/migrate_driver/live_value.rs:expire_unix_ms（:236-247）取精确 ticks 后经 unix_time_in_milliseconds_from_ticks 截断为 Unix 毫秒（丢全部亚毫秒位），migrate_driver/keys.rs 装帧（wconn/src/record.rs kind=1/2/4 帧 TTL 字段即 i64 LE expire_unix_ms）；复制快照 replication_snapshot_iterator.rs:394-404 与 RI 树流 range_index_manager_migration.rs:236-239 同源复用。接收端 frame_import.rs:373 经 expire_at_milliseconds_to_ticks 回扩，恒向下偏 ≤9999 ticks（1ms），RI 接收 range_index_migration_receive_state.rs:160-175 同式。本仓 AOF 增量路径已在 aof_processor_store_ops.rs:332-338 明文判定同形毫秒往返为应消灭的分叉（原毫秒往返恒向下截断的重放端系统性前移至多 1ms 分叉就此消灭，PTTL 与主端逐位一致），快照/迁移面同形仍在，同仓两种口径并存。伴生竞态：read_live_value 值读与 TTL 读为两次独立 await，间隙内键到期即落 exp <= now → 0（无 TTL）臂，键以无 TTL 装帧迁出且源端随迁移清单删除，接收端成永生键。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   迁移/全量同步落地键 TTL 恒比源端早 ≤1ms，PTTL/EXPIRETIME 主从不一致，与 AOF 路径已确立的逐位一致承诺直接矛盾；两读竞态窗（微秒级）造成数据语义腐坏：接收端永生键加源端已删。本仓无向下兼容包袱，线格式字段语义可直接收紧。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/migration/migrate_driver/live_value.rs:expire_unix_ms
wedb/wedb/src/server/migration/migrate_driver/live_value.rs:read_live_value
wedb/wedb/src/server/migration/frame_import.rs
wedb/wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs:read_live_value
wedb/wnode/src/range_index/range_index_manager_migration.rs
wedb/wnode/src/range_index/range_index_migration_receive_state.rs
wedb/wconn/src/record.rs

对应 c# 文件与函数：
garnet/libs/cluster/Server/Migration/MigrateSessionKeys.cs:TransmitKeysAsync
garnet/libs/cluster/Session/RespClusterMigrateCommands.cs:SET
garnet/libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:Upsert
garnet/libs/server/Storage/Functions/UnifiedStore/UpsertMethods.cs:InitialWriter
garnet/libs/cluster/Session/MigrateCommand.cs:Expired

精炼执行方案：
1. 线格式 TTL 字段由 Unix 毫秒改为直接承载 .NET Ticks 原值（对标 AOF TtlWrite 镜像直设不折算的单机制；kind=4 流元 next_expiry 已系 ticks 原值，同格式先例）。编解码单点收口：wconn/src/record.rs 帧注释、MigrationRecord/MigrationFrame/BatchItem 三视图与 parse_typed_record 一处改齐；发送端 live_value.rs expire_unix_ms 改名 ticks 直传（保留 `exp > now_ticks` 活值过滤，仅去毫秒折算）、range_index_manager_migration.rs:236-239 同臂；接收端 frame_import.rs 与 range_index_migration_receive_state.rs 直落 put_ttl 不经毫秒折算，`> 0` 无 TTL 门语义不变。MIGRATE 与 SYNC 两链共用导入壳一次改齐，RI 流同臂。PTTL/EXPIRETIME 用户面毫秒读出（keys.rs/storage_session.rs）不动。
2. read_live_value 值读与 TTL 读合并为单次读窗（沿用 §118 单探针窗形态：值读已带惰性过期裁决，TTL 读取改同窗取值或读后复核存活，复核失败归 Gone 臂不发帧不删源），杜绝值活 TTL 死装帧窗。
3. 测试验证点：构造非 16 对齐裸 ticks TTL 键（SET/GETEX 族产物）迁移与全量同步往返，断言接收端 PTTL 与源端逐位一致；构造值读后 TTL 读前到期的窗口用例（TEST_LIVE_VALUE_READ_HOOK 同族注入），断言不产生无 TTL 装帧且源端不删。

终态注记：
- 收口形态：线格式（wconn::record kind=1/2/4）与树流中的 TTL 字段由 Unix 毫秒往返统一改为 .NET Ticks 原值直传与直落；read_live_value 增加值读后到期复核，间隙到期落 Gone 臂杜绝值活 TTL 死的永生键外溢；补齐非 16 对齐裸 ticks 逐位保真与读间隙到期锁测。
- 合入哈希：4016d62
- 状态：已收口归档。

主控收票审计（2026-10-01 r9 波，沙箱 dev 尖亲跑）：
- 改动面核对：19 文件全部可归到票面「涉及代码」七处及其同字段改名传导（sync_transport /
  replication_snapshot_iterator / tiered_sync_migration / RI 两态）与测试册，无越界触他席在途域。
- 线格式收紧达方案 1 单点要求：wconn::record 三视图（MigrationRecord/MigrationFrame/BatchItem）
  与编解码、chunked 发送、RI 流元编码一次改齐，全仓 expire_unix_ms 零残留；
  用户面 PTTL/EXPIRETIME 毫秒读出（wkv/src/ttl.rs）按票面要求未动，无双口径。
- 溢出前提复核（本席补验）：旧接收端钳制的必要前提是毫秒→ticks 的乘 16；ticks 直落后换算链退化为
  除 Law（milliseconds_from_diff_ticks / unix_time_in_milliseconds_from_ticks 皆除法，
  且前者有 ticks > 0 与 diff > 0 双门），i64::MAX 畸形入参不可达溢出——删钳不引入 debug panic 敞口。
- 方案 2 达形：读间隙到期以 ExpireTicks::{Alive,Expired} 分态归 Gone 臂，键权留源端
  （Gone 臂注释即「不发帧、不计入删除清单」），与 C# Reader 整记录原子读语义等价承接。
- 锁测非空转反证（本席核心补证，席上未申报）：沙箱内把发送端单臂还原为毫秒折算后重跑，
  raw_ticks 册转红（left=1790793117939 毫秒形 / right=639263899179391876 ticks 形），
  坐实该测真钉的是亚毫秒保真而非 PTTL 同义反复（PTTL 读出恒为毫秒域，改前改后同形，
  若只断 PTTL 即是废话测试——本测断的是落盘 ticks 原值，判据正确）。
  两读间隙 Gone 臂册在 same 反证下仍绿，因其钉的是另一臂：其判据依赖改前 `_ => 0` 无 TTL 装帧形，
  属代码面可静态坐实的非必要同形，已按票面方案 3 第二验证点核收。
- 门禁：本席沙箱复跑受影响六册 18/18 绿（migrate_live_value_ttl_roundtrip /
  migrate_import_batch_equivalence / diskless_sync_ttl / migrate_cross_generation_ttl /
  migrate_import_write_window / wconn record_codec），构建零告警。
- 残留观（不并案，另记待甄）：ticks 直落使接收端 TTL 入册值首次不经 §4 会话入口饱和钳制域，
  畸形帧可落远超 MAX_UNIX_TIMESTAMP_TICKS 的未来戳；当前无算式溢出与判错风险（除法收口），
  仅主从 PTTL 读出侧会因 unix_time_in_milliseconds_from_ticks 而回巨大值。
  属入参卫生面而非本票裁决面，待下轮以真值源对账 C# clamp 口径后再定是否立项。

