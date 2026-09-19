第 8 轮 glm 系列 +11 条清账（台账 next/qcode.rounds.md :95-108，产物直接汇报无文件载体）：7 条已修/批次在册、2 条新立案，余为批次射程覆盖

审计时间：2026-09-19（fixloop 台账审计）。取证基线：主仓 dev 工作树实时 grep 实测。
该轮产物未落 next/ 文件（主代理直接转记进台账），台账删除前逐条甄别如下。

已修复（3 条）

- db +1（wkv/src/gc.rs:344/:370 sweep_vdb 换号死亡账本删除失败静默吞错，MED）：
  已修。gc.rs:349 sweep_vdb 函数体现态：`if let Err(err) = session.delete_dbmeta(...)`
  → `warn!("内置 GC 退役墓碑注销失败: ...")` 留痕，注释明写「账本条目已离册，盘上
  墓碑未注销则重启重建后下轮重扫重投（pop_reclaimable 双检幂等），绝不静默吞硬错」；
  调用侧 :345 `self.sweep_vdb(&store).await?` 错误传播。与 dbmeta-atomic-batch.md
  豁免联动域无冲突。
- my +1（migration/keys.rs:12 迁移 KEYS 驱动循环用 std 时钟、全仓唯一无声明 coarsetime
  旁路，LOW）：已修。wedb/wedb/src/server/migration/migrate_driver/keys.rs 头部
  `use coarsetime::Instant;`，std 侧仅剩 time::Duration，无 std Instant 旁路。
- design MED（StorageSession 整套死 WATCH 登记双机制）：已修/收口为单机制。
  WATCH hook 单点落地：wnode/src/service.rs:980 生产装配
  `store.set_watch_hook(version_map_watch_hook(...))`，
  storage_session.rs:118 bump_watch_version 单向转发 batch，多个测试经同一 hook 接线；
  未发现第二套死登记形态（与台账 :43「WATCH hook 单点已落地」记录一致）。

新立案（2 条）

- net +1（wconn replies.rs:23 parse_bytes 锚点挂错，LOW）：仍成立。
  next/wconn-parse-bytes-anchor-mismatch.md。
- design LOW（512MB 上限常量双处 pub 定义）：仍成立。
  next/payload-cap-512mb-single-claim.md（wbitmap/manager.rs:9 与
  wnode/resp/basic_commands/set.rs:25 双定义，C# 单点 BitmapManager.cs:19）。

在册（批次票射程覆盖，5 条）

- design MED（basic_commands 四个 network_* 死方法与分派臂内联双轨）：
  task/ing/zero-consumer-surfaces-batch-two.md 第一节（network_ping/asking/echo 死方法
  + PING/ASKING 臂内联双轨，修法齐备）。
- design MED（GarnetLog::initialize_if 零接线致恢复链断第一步）：
  batch-two 第二节（initialize_if 生产零调用，C# ReplicationManager.cs:548 对位）。
- design MED（itembroker 异步入口死形态）：batch-two 第七节
  （get_collection_item_async/start_async 零调用，与 itembroker-shutdown-dispose 联动）。
- design LOW（iterate_store 死转写对）：batch-two 第三节
  （array_key_iteration_functions.rs:251 零生产调用）。
- design LOW（wresp 两个 RESP 解析原语零消费）：batch-five 类四第 1 条复核
  （wresp/read.rs:239 与 C# RespReadUtils.cs:725 两侧同为零消费者，同构不删）+
  batch-five 第 9 项（wconn 门面 parser.rs 死臂收口）。
- design LOW（第二批零消费装配口五件）：batch-two 来源自记「qcode 第 7/8/9 轮台账
  在册、尚未立单的死面项」射程覆盖；批五普查（25 处死口）复核后余量归零。

结论：+11 中 3 已修、2 新票、6 批次在册/射程覆盖，无悬置残留。
