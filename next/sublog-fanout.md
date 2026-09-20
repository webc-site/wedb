1. 多物理子日志端到端扇出打通（AOF 复制泵逐子日志分发）。
   核实与裁决见 task/done/sublog-single-constraint.md（分片内核真实且有 N>1
   测试，缺 server 装配与泵扇出；最小落点清单在该文档末尾）。
   rust: wedb/wnode/src/service.rs:870（open_wal 单设备）、
   wedb/wnode/src/aof/waof_sublog.rs:39（single_log_aof 恒 vec![backend]）、
   wedb/wedb/src/server/boot.rs:82（装配防御门，扇出落地时移除）、
   wedb/wedb/src/server/replication/aof_replication_pump.rs:215（pump_backlog
   只喂 task_ref(0)）、wedb/wedb/src/server/replication/
   cluster_replication_session.rs:297（副本落盘恒进单 wal）。
   C#: libs/cluster/Server/Replication/PrimaryOps/AofOperations/
   AofSyncTask.cs:120 / AofSyncDriver.cs:111 经 ShardedLog 逐子日志迭代。
   注意：与 aof_processor.rs（重放链）与 diskless/migration 在途棒交叠，
   认领前先确认禁入清单。
