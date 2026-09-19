AOF 时间脉冲帧 CLUSTER 子命令名写错 ADVANCETIME（甄别拒绝：已修复 + 在册票重复）

来源：next/qcode10.net.md 条 7（[HIGH]，原文件已整体拆除）。

拒绝理由（双重）

一、同题在册票：next/aof-advance-time-frame-name-bug.md（其自述「来源：qcode 第 10 轮
net 条 7」），与本条目内容一致（$11 ADVANCETIME 与 $12 ADVANCE_TIME 漂移、fire-and-forget
错认领、前缀断言锁死错误形态、往返用例修法）。

二、当前工作树已修复（HEAD f71dbbc 实测）：
- wedb/wconn/src/session.rs:257 `ADVANCE_TIME_FRAME_PREFIX` 现为
  `b"*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n"`（带下划线、批量串长度 $12）；
- wedb/wedb/src/server/replication/aof_sync_task.rs:560 与 :584 断言字节已同步订正为
  `$12 ADVANCE_TIME`；replica_wire.rs 注释同形；
- `grep -rn ADVANCETIME`（无下划线形态）全仓零命中，即修法第三步「全仓确认再无其它
  无下划线形态」亦满足。
- C# 锚点核实：garnet/libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:25
  `"ADVANCE_TIME"u8`、:578 TryWriteBulkString(advance_time)；
  RespCommandHashLookupData.cs:325 ("ADVANCE_TIME", CLUSTER_ADVANCE_TIME)。

附注（不在本拆票处置范围内执行）：在册票 next/aof-advance-time-frame-name-bug.md 描述的
bug 形态（$11 ADVANCETIME）在当前 HEAD 已不存在，该票如仍挂 next/ 需由认领方复核核销；
本次仅按「不动 next/ 其他既有文件」禁令登记，不改该票。
