优先级：低

AOF 时间脉冲帧断言仍为 starts_with 前缀形态，应改整帧相等（防再次锁死错误帧名）

来源：qcode.rounds.md 第 10 轮（2026-09-19）唯一残留未立案项。ADVANCE_TIME 帧名 bug
本体已由并发会话修复（$12 ADVANCE_TIME + cluster_resp_session.rs:1598/1736 喂帧闭环回归，
弃分支 advance-time-frame-name/4c728268），仅剩本条断言形态残留。

现状（主仓 dev 实测）
- wedb/wedb/src/server/replication/aof_sync_task.rs:560
  `frames[1].starts_with(b"*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n")`
- 同文件 :584 `frames[0].starts_with(同前缀)`
- 仍在断言前缀而非整帧相等。前缀断言只锁帧头字节，帧尾元素（时间戳参数段）漂移或
  帧内多出/缺失元素时测试不报警；上一轮帧名写错（$11 ADVANCETIME）正是被这类断言
  掩盖（见 next/aof-advance-time-frame-name-bug.md :16-18 的归因），该票修法要求的
  「往返用例」已落地，但本文件的字节面断言没有升级为整帧相等。

C# 参考
- 无对位测试（C# 侧无字节面前缀断言形态）；帧构成见
  libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:557-597
  ExecuteClusterAdvanceTime（4 元素 CLUSTER ADVANCE_TIME 帧，元素与长度均可静态确定）。

修法
- aof_sync_task.rs:560/:584 改 `assert_eq!` 整帧相等（帧由 encode_advance_time_frame
  确定性产出，时间戳参数用固定测试时钟即可整帧锁定）；同文件 :481 的
  `starts_with(b"*8\r\n$7\r\nCLUSTER\r\n")` 为同病形态，可顺带评估是否整帧化。
- 验收：断言不再以 starts_with 承载帧面事实；帧名/元素数再漂移时测试必红。

## 细化方案（2026-09-19 认领方核实追加）

甄别核实（全部实测主仓 dev 源码）

- 帧编码路径：CallbackWire::advance_time → wconn::session::encode_advance_time_frame
  （wconn/src/session.rs:262，前缀常量 :257 + resp_writer2 write_array_item 逐元素）
- write_array_item(i64) = write_int64_as_bulk_string（wresp/resp_memory_writer.rs:570）
  → bulk string 形态 `$<len>\r\n<digits>\r\n`（非 :integer）
- 序列号确定性：pulse_aof() 用 RuntimeServerOptions::default()
  （wconf/runtime_server_options.rs:124 aof_physical_sublog_count=1）→
  garnet_append_only_file.rs:76 仅 physical>1 才持有 seq_num_gen → 单物理子日志
  模式 get_larger_than_maximum_sequence_number() 恒 1 → :560 脉冲帧完全确定
- node_id 渲染：CallbackWire::append_log → hex_str_u128（wbase/hex.rs:78，
  32 字符定长小写 hex）→ 0x0DE1 = "00000000000000000000000000000de1"

改法（只改断言形态，不动帧编码；4 处）

1. aof_sync_task.rs:481（test_consume_forwards_frame_via_wire）：
   starts_with(b"*8\r\n$7\r\nCLUSTER\r\n") → assert_eq! 整帧
   `*8\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$32\r\n00000000000000000000000000000de1\r\n$1\r\n0\r\n$2\r\n64\r\n$2\r\n64\r\n$2\r\n78\r\n$8\r\n\x00payload\r\n`
   （payload b"\x00payload" 为 8 字节：\x00 + 7 字符）
2. aof_sync_task.rs:560（test_advance_time_pulse_sends_frame）：整帧
   `*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n$1\r\n0\r\n$1\r\n1\r\n`
3. aof_sync_task.rs:584（test_advance_time_frame_via_wire）：wire.advance_time(0,42) 整帧
   `*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n$1\r\n0\r\n$2\r\n42\r\n`
4. 同病顺带：wedb/tests/advance_time_frame_roundtrip.rs:35-38
   encode_advance_time_frame(3,42) 参数全静态，前缀断言同款整帧化：
   `*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n$1\r\n3\r\n$2\r\n42\r\n`
   （票据修法未点名但同题同病，纯断言形态，零风险）

验收：cargo check 通过（worktree）；断言全部 assert_eq! 整帧字节，帧名/元素数/
长度/元素值任一漂移测试必红。
