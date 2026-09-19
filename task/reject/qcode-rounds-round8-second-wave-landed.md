第 8 轮待派次波 14 条清账（台账 next/qcode.rounds.md :70）：12 条已修复、2 条在册，全部闭环

审计时间：2026-09-19（fixloop 台账审计）。取证基线：主仓 dev 工作树实时 grep 实测，
非采信台账原文。原载体 next/qcode8.*.md 已被下游消费删除，以下为逐条去向与证据。

已修复（12 条）

- net 条 2（迁移/无盘单块阈值三处 (1<<17)-256 与 65280 背离加假注释，MED）：
  已修。全仓 `-256`/`65280`/`130816` 形态归零；迁移单块阈值收敛为
  wbftree DEFAULT_MIGRATION_CHUNK_SIZE 单点（wnode/src/rangeindex/range_index_manager_replication.rs:163
  消费）；快照侧 wedb/wedb/src/server/replication/snapshot_transmission.rs:57-58
  SNAPSHOT_CHUNK_SIZE = 1 << 17，注释对齐 C# FileDataSource.cs DefaultBatchSize = 1<<17。
- net 条 3（FailoverSession 零超时把 deadline 抬到 600s 与 C# 相位相反，LOW）：
  已修。wedb/wedb/src/server/cluster_session/failover.rs:81-87 注释与行为按 C#
  failoverTimeout=default(TimeSpan) 传播形态订正（RespClusterFailoverCommands.cs:28-53、
  FailoverSession::new 单点归一 600s，FailoverSession.cs:85），缺省/显式 0 传零时长。
- db 条 2（换号回收旁表七处样板，MED）：已修。pin_routing 全仓生产命中收敛为
  wkv/src/vdb.rs:757-761 与 wkv/src/gc.rs:359/:372 两处，七处样板形态消失。
- db 条 3（冷读分发内核四抄 + session/raw/mod.rs read_record 生产零消费，MED）：
  已修。wkv/src/session/raw/mod.rs:289 read_record 注释自认「冷读分派唯一单点：
  磁盘区免纪元、内存驻留持短守卫」；wkv/src/compact.rs:102-105 read_record_at 为
  「端口体单点：直接转调内核，冷读分派与纪元纪律只在内核一处维持」，read_record
  不再零消费。
- db 条 4（wreviv 相邻空洞合并自造机制，MED）：已修。wreviv/src/pool.rs
  FreeRecordPool 全量函数清单（put/take/purge_below/find_bin_index 等）无 coalesce/
  相邻空洞合并形态，自造机制已移除。
- db 条 5（backoff 阶段动作四处手抄，LOW）：已修。wbase/src/backoff.rs 模块头注释
  「wait/wait_busy/wait_async 均只在此处定义，调用方仅推进计数器」，与台账第 131 行
  修复记录（wbase backoff 三入口）一致。
- db 条 6（StoreConfig 三处重复默认尾，LOW）：已修。wkv/src/config.rs:257
  Default → Self::minimal() 单点；wnode/src/service.rs:635 store_config() →
  StoreConfig::auto()，无重复默认尾形态。
- design 条 4（wrecord 视图 38 纯转发 getter 加 previous_address 同名双方法，MED）：
  已修。previous_address 全仓仅剩 wconn/src/session.rs 客户端帧编码（:116-:312），
  wrecord（record_ref.rs/record_mut.rs/codec.rs）无同名双方法。
- design 条 5（bench 特性三处空挂，LOW）：已修。全仓 `feature = "bench"` 与
  Cargo.toml bench 段零命中（与台账 :65 主代理实码抽查一致）。
- my 条 2（OBJECT_DELTA 装饰性别名，LOW）：已修。OBJECT_DELTA 全仓零命中
  （台账 :131「waof OBJECT_DELTA 死信封删除」已执行）。
- my 条 3（db_gc_interval_secs 零读者，LOW）：已修。db_gc_interval_secs 全仓零命中，
  字段已消；GcConfig 域余量由 next/wkv-gc-compaction-interval-num-segments-surface.md 承载。
- my 条 4（RESP_ERR_GENERIC_SELECT_CLUSTER_MODE 死常量加注释与升阶写 Meta 相反，LOW）：
  已修。RESP_ERR_GENERIC_SELECT_CLUSTER_MODE 全仓零命中，常量已删（qcode10.net 条 1
  的同域清理已落地；qcode10.net.md 报告仍在 next/ 在册，其条目处置归下游）。

在册（2 条）

- design 条 3（启动装配手抄三份，MED）：next/boot-assembly-projection-single-source.md
  （票面自记「来源：qcode 第 8 轮 design 条 3」）。
- design 条 6（TCP keepalive 测试件常驻生产，LOW）：并入
  task/ing/zero-consumer-dead-surfaces-batch-five.md「测试件常驻生产」类；
  生产面现状 wnode/src/net/socket_opt.rs configure_socket 单入口（:51 set_tcp_keepalive），
  测试断言收于同文件测试区。

结论：次波 14 条全部处置完毕（12 修复 + 2 在册），无悬置残留。
