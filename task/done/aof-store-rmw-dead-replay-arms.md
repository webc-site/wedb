AOF StoreRMW 重放面无生产者的四臂（Expireat/Getdel/Setex/Psetex）：删除并让 default 臂整块显式失败

来源：第 8 轮 data 二棒乙条 6（LOW，判为死代码，优先级最高档）。取证基线：主仓
/Users/z/git/db/wedb 分支 dev。位点更正：文件在
wedb/wnode/src/aof/aof_processor.rs，非报告所写 resp/aof/ 路径；行号按当前 HEAD 复核无漂移。

现状

- 重放面 wedb/wnode/src/aof/aof_processor.rs 的 StoreRMW 命令 match 起于 :1017，内含
  RespCommand::Expireat :1041-1049、RespCommand::Getdel :1075-1080、
  RespCommand::Setex :1082-1090、RespCommand::Psetex :1091-1099 四臂，
  末尾 `_ =>` default 臂 :1100-1113 已是「未知命令显式失败、严禁静默吞没」的既有判据。
- 写侧生产者全集（wedb/wnode/src/service.rs 的 StoreEvent 镜像端口）产不出这四形态：
  Write :147-179 只产 StoreUpsert/StoreDelete（String 与 Acl 标签），
  TtlWrite :180-205 恒 (Pexpireat | Persist) 且 arg1 为
  unix_time_in_milliseconds_from_ticks 的绝对毫秒，
  EtagWrite :206-219 恒 Setwithetag，RangeIndexWrite :245-265 恒 (Riset | Ridel)，
  RangeIndexCreate :282 起 Ricreate，TtlPurge :336 起 Delifexpim；
  其余 StoreRMW 入队点 wedb/wnode/src/rangeindex/range_index_manager_replication.rs:212、:234
  与 wedb/wnode/src/resp/vector/vector_manager_replication.rs:54-63 只产 RI 与 Vector 族命令。
  全仓 grep RespCommand::{Expireat,Getdel,Setex,Psetex} 的非重放命中面只剩
  解析层（resp/parser/command_table.rs、fast_patterns.rs、resp_command.rs）与 ACL/catalog 登记，
  即这四个 cmd 值从未进入 AOF 条目。
- 命令端实路径与臂口径相反，属误导面而非预留面：
  SETEX/PSETEX 在 wedb/wnode/src/resp/basic_commands/set.rs:291、:303、:314-358
  是「try_upsert_sync 普通值写 + put_ttl_sync 落 TTL」（:351 即
  expiry_ticks_from_now 折成绝对 ticks），AOF 侧只留 Pexpireat 绝对毫秒条目，
  而 Setex/Psetex 两臂 :1084、:1093 用 duration_seconds_to_ticks / duration_milliseconds_to_ticks
  把 arg1 按相对时长解释（session.setex 内部再 expire_in_ticks），与写侧唯一 TTL 条目源
  口径正好相反——条目一旦出现即按错误语义执行；GETDEL 在
  wedb/wnode/src/resp/key_admin_commands/keys.rs:130-160 走 read_user_sync + try_delete_sync，
  净效果由 StoreDelete 条目承载，同样不产 Getdel。
- C# 侧这些臂是活路径但形态不同：EXPIRE 族命令端线性化为绝对 ticks 后单形态入 AOF
  （garnet/libs/server/Resp/KeyAdminCommands.cs:420-432），
  条目产出点由 PostInitialWriter 只在真写入时置 NeedAofLog 保证与写侧一一对应
  （garnet/libs/server/Storage/Functions/ObjectStore/UpsertMethods.cs:46-50）。
  rust 的「值写 + TtlWrite 旁路镜像」漏斗已把这套形态收敛掉，遗留的重放臂是转写残留。

优先级

死代码（四臂零生产者，其中两支还携带与写侧相反的 arg1 解释，留着即是下一次
「AOF 出现 Setex 条目」误判的诱饵）。

方向

1. 删 aof_processor.rs:1041-1049（Expireat）、:1075-1080（Getdel）、
   :1082-1090（Setex）、:1091-1099（Psetex）四臂，遇该形态落 :1100-1113 default 臂显式失败，
   沿用本文件 :1034-1040 已为 Expire/Pexpire 相对时长重臂写下的同款删除声明口径
   （「写侧唯一 TTL 条目源恒绝对毫秒；相对重算叠加复制延迟使从库 TTL 系统性偏短，严禁静默执行」），
   在该注释上补一句覆盖 Expireat/Setex/Psetex/Getdel。
2. 不采「补注释登记为协议预留面」的替代方案：写侧无任何路径可产该四形态，
   登记预留等于把死代码转成永久豁免，和 :1100-1113 的显式失败判据自相矛盾。
3. 删后清 import：aof_processor.rs:24-25 的 duration_seconds_to_ticks /
   duration_milliseconds_to_ticks 随两臂失去唯一使用者（二者在
   wedb/wbase/src/convert.rs:113、:122 仍有 expiry_ticks_from_now 家族消费，
   故只删 import，不删 wbase 函数）。
4. 同批普查零消费的连带面并给结论，勿留半截：
   - wedb/wnode/src/storage/session/mainstore/main_store_ops.rs:24 getdel
     在生产者删净后仅剩测试引用 → 按零消费死 API 口径处置
     （C# 对位 MainStoreOps.cs:GETDEL 若确不落地，须在 js/check/ignore/garnet 下登记
     不实现理由，禁止留空壳）；main_store_ops.rs:38 setex 仍被
     wedb/wnode/src/storage/session/txn_proc_view.rs:94-96 消费，保留。
   - 本 match 的 :1017-1033 五支 StringRMWOp 臂（Incr/Incrby/Decr/Decrby/Incrbyfloat/
     Append/Setrange）与 :1115-1118 的 rmw_main_store 收口，同样查无生产端
     （rmw_main_store 定义 wedb/wnode/src/storage/session/mainstore/advanced_ops.rs:34，
     全仓非测试调用点仅重放自身；TxnProcView 的 increment 走 rmw_string，
     见 txn_proc_view.rs:107-120）。若确认零生产者，本票一并删除，
     使 default 臂覆盖整个 StoreRMW match，勿另立第二张同类票。

验收

- 全仓 grep "RespCommand::Setex =>"、"RespCommand::Psetex =>"、"RespCommand::Getdel =>"、
  "RespCommand::Expireat =>" 零命中。
- 构造带 TTL 的 SETEX/PSETEX/GETDEL/EXPIREAT 写入 → 主侧 AOF 条目仅含
  StoreUpsert/StoreDelete/Pexpireat 形态；从库回放与重启恢复后 TTL 与值一致
  （入 wedb/wnode/tests/aof_replay.rs 或 aof_store_rmw_replay.rs 现有夹具）。
- 人为写入 cmd=Setex 的 StoreRMW 条目时回放显式失败（断言错误串含 unsupported cmd），
  不静默跳过。
- cargo check --workspace --tests 与 ./clippy.sh 零告警（删臂后 import 与 match 臂数同步收干）。

协调

- 查重补记（主仓 HEAD b86ce149 复核，同一份第 8 轮 data 二棒乙报的其余三条已由
  并发代理先行建档，本票不重复制单，仅承接 AOF 重放面这一条）：
  BITFIELD_RO encoding 位点 → task/ing/bitfield-ro-encoding-error-class.md；
  GEO STORE 互斥模板 + arity quirk → task/ing/geo-store-cmd-strings-and-argcount-quirk.md；
  INFO KEYSPACE 绕过 I64Codec → task/ing/keyspace-stats-i64codec-single-point.md。
  AOF StoreRMW 死重放臂以本票为唯一挂载点，后续同类观察并入本票，勿另立。
- 零消费面普查口径见 task/ing/zero-consumer-dead-symbols-cleanup.md 与
  task/ing/zero-consumer-surfaces-batch-two.md，本票四臂属同一 census 的 AOF 重放切片。
- AOF TTL 条目口径（绝对毫秒 + Persist 双形态、ExpireOption 低 4 位不携带的刻意差异）
  已在 aof_processor.rs:1053-1058 声明并主端裁决，本票不重开该议题。

落地记录（收尸棒代理复验后合入，票据转 done）

- 载荷已由并发代理先行合入 dev：81c144c0（前手两枚提交 368f8081 死臂整族删除 +
  cd8fb260 写侧形态闭包对证与门禁登记，全部落地）。本代理在 refs/heads/dev 上
  逐项复验前手判据，结论：无「活该留」剔除项；main_store_ops.rs setex 按本票
  :61-62 口径保留（仍被 wedb/wnode/src/storage/session/txn_proc_view.rs:95
  storage.setex 消费），LCS 族仍被 wedb/wnode/src/resp/array_commands.rs 消费。
- 复验证据（refs/heads/dev git grep）：StringRMWOp / RmwResult /
  mainstore::advanced_ops 三符号零命中（advanced_ops.rs 整文件与 mod.rs 声明已删）；
  rmw_main_store 仅剩两处删除登记注释（aof_processor.rs:1039、
  tests/aof_store_rmw_replay.rs:183），无代码引用；
  RespCommand::{Setex,Psetex,Getdel,Expireat,Incr,Incrby,Decr,Decrby,Incrbyfloat,
  Append,Setrange} 的 match 臂在 wedb/**/src/**/*.rs 零命中，default 臂
  （unsupported cmd 显式失败）已覆盖整个 StoreRMW match；
  会话级 .getdel( / .append( / .setrange( 无调用方。
- 主存 RMW 路径唯一性确认：现仅 wkv BatchStoreSession::try_rmw_sync 快路径与
  StorageSession::rmw_string（storage_session.rs:347，HLL 与 txn_proc_view 消费）
  一条，第二套逐命令手写包装面已收口，无半截包装残留。
- 本代理补入的连带修复（merge 8e20def3，唯一新增载荷）：
  1) wedb/wedb/tests/diskless_sync_anchor_window.rs 原夹具伪造
     incr_entry/append_entry 两支 StoreRMW 形态，死臂删除后回放必按
     unsupported cmd 显式失败而使测试转红；已改接写侧真实条目形态
     （计数键与 APPEND 键走 StoreUpsert 终值，TTL 走绝对毫秒 Pexpireat），
     锚点不变量由「授予位点直读断言」守护，锚点后记录改由「值 + 主从 TTL
     同秒一致」守护。
  2) wedb/wedb/src/server/replication/diskless_replication/replication_sync_manager.rs
     锚点理由注释同步更正（仅 ObjectStoreRMW 集合累加仍非幂等，String 侧
     已终值化 StoreUpsert/StoreDelete + 绝对毫秒 Pexpireat/Persist）。
- 门禁：合并树 cargo check --workspace --all-targets EXIT=0 且零告警；
  worktree 内 bun js/check.js EXIT=0（AdvancedOps.yml / MainStoreOps.yml
  不实现登记随载荷落地，理由与 garnet 语料锚点已对证）。本票验收项
  ./clippy.sh 与实跑测试按 fixloop 约束不在子代理侧运行，由主代理收口统一跑。
- 协调：task/ing/slow-path-string-key-admin-arms.md :54-76 仍以本票已删的
  advanced_ops.rs:34 rmw_main_store 与 main_store_ops.rs:24 getdel 为「既有内核」
  提转调方案，该票须重指向命令层实路径（resp 层 SETRANGE/APPEND/INCR 族），
  勿按已删内核施工；本票删除的内核与臂一律不得复活。
