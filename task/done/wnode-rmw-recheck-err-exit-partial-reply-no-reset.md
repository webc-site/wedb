甄别结论：通过（2026-09-29 主控甄别，定级 P2——探针 Err 臂 rmw_helpers.rs:538-543 直返漏 obj_out.reset()，output 尾已有负载时 slow_arm 追加错误帧成单命令双帧破协议流；对照 :544-547/:563-565/物化臂 :476-481 三兄弟臂均守约。C# RMWMethods 记录锁内不出应答字节单命令单应答。修复：单点补 obj_out.reset() 零机制）

审核结论：通过（2026-09-29 甲轮34-A，P2 级）。:538-543 复验探针 Err 臂直返未清场、三兄弟臂（:544-547/:563-566/物化臂 :476-481）守约对照成立；obj_save_recheck_async :1115-1140 两处 ? 上抛 IO 错误可达；slow_arm 宏 Err 追加 output 尾拼帧坐实。方案：单点补一行 obj_out.reset() 零机制；故障注入+单错误帧断言闭环。无修正意见。

原票面：
run_async_rmw 落笔前复验探针 Err 臂以 ? 直返漏 obj_out.reset，operate 已写负载帧与存储错误帧拼帧，单命令双应答破协议流

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 对象 RMW 单命令单应答：garnet/libs/server/Resp/Objects/HashCommands.cs、
   SortedSetCommands.cs 等 Network* 方法均在 storageSession.RMW 完成返回后才经
   WriteInteger/WriteBulkString 一次性出应答（garnet/libs/server/Storage/Functions/
   ObjectStore/RMWMethods.cs 的 InitialUpdater/InPlaceUpdater/CopyUpdater 在记录锁内
   只改内存对象与 NeedAofLog 位，不产应答字节），结构上不存在「半截负载帧 + 错误帧」
   拼接形态；底层存储故障走 GarnetException 连接级可见失败，绝不冒答成功帧。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   run_async_rmw（wedb/wnode/src/resp/objects/rmw_helpers.rs:526-566）把 operate
   负载直写会话 output 尾段（:526-527），本函数自陈清场契约「写回失败回退挂载点
   再落错（慢路径统一应答前清场，杜绝残留负载与错误帧拼帧）」。三个错误出口两个
   守约：复验判异臂 :544-547 与 apply_rmw_post_operate 失败臂 :563-566 均先
   obj_out.reset()（wcol/src/resp/output.rs:71 truncate 回挂载点）再 Err；物化臂
   :476-481 同。唯独落笔前终态复验的探针 Err 臂 :538-543 以
   map_err(log)?. 直接返回 Err(())，未清场。obj_save_recheck_async
   （wedb/wnode/src/resp/objects/object_store_utils.rs:1115-1140）沿
   probe_alive_domain_with_prefix 内存快照 ? 与 storage.probe_alive_domain_with_prefix
   异步读通 ? 原样上抛 wkv IO 错误，该出口真实可达（设备故障/读内核 IO 失败）。
   此时 output 已含 HINCRBY/ZINCRBY/LPUSH/SADD/HSET 等写命令的成功负载帧
   （:N、bulk 分值等），慢臂漏斗（garnet_api/slow.rs:96-102 slow_arm）随后在
   output 尾部追加 -ERR slow path storage error——单命令两帧：伪成功整数 + 错误帧，
   且错误归属错位到下一条流水线命令。
3. 逻辑危害确证
   协议流破损：客户端解析器对单命令收到两个应答，请求-应答配对整体后错一位，
   流水线/多路复用客户端后续全部应答串位；伪成功整数帧先于错误帧抵达，客户端
   可能已按成功入账。触发面为复验探针读 IO 失败（真实存储故障场景，正是错误
   传播链须闭环的出口），零数据损坏、零 panic；同函数兄弟双臂已守约，属单点
   复位遗漏，非设计分叉。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/rmw_helpers.rs:run_async_rmw（:538-543 复验探针 Err 臂 ? 直返漏 obj_out.reset；:544-547/:563-566 兄弟臂守约对照；:526-527 operate 直写点）
wedb/wcol/src/resp/output.rs:ObjectOutput::reset（:71 回退挂载点清场单源）
wedb/wnode/src/resp/garnet_api/slow.rs:slow_arm（:96-102 Err 补错误帧漏斗，拼帧落地点）
wedb/wnode/src/resp/objects/object_store_utils.rs:obj_save_recheck_async（:1115-1140 Err 上抛可达性）

对应 c# 文件与函数：
garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:InitialUpdater/InPlaceUpdater/CopyUpdater（记录锁内只改对象不出应答字节）
garnet/libs/server/Resp/Objects/HashCommands.cs:NetworkHSET 等（RMW 返回后一次性出应答，无半截负载通道）

精炼执行方案：
1. :539-543 复验探针 Err 臂补 obj_out.reset() 再 return Err(())，与 :544-547、
   :563-566 两兄弟臂及物化臂 :476-481 同一清场纪律；零新机制，纯单点补复位
2. 测试验证点：故障注入（StorageSession DELETE_FAIL_INJECT 同款读故障钩子或
   SegmentedDevice IO 失败夹具）使 obj_save_recheck_async 异步读通臂 Err，
   对热分层大键外的普通信封键走慢臂 HINCRBY/ZINCRBY，断言应答恰为单条
   -ERR slow path storage error、无前导整数/bulk 残帧；既有
   obj_rmw_aof_enqueue_fail、msetnx_atomic 回归不弱化

来源：甲轮29-B 交叉复审席（2026-09-29），错误传播链维度（task/review.md 4.1 异常收敛与透明传播 / 4.2 多路径行为同构单面）。查重：wnode-object-scan-degrade 票射程为 shared_object_commands.rs slow::object_scan 的 exec_tiered_scan Ok(false) 折叠，与本票不同函数不同出口；err_frame 单源设计非缺陷（甲轮28-B 已认记）

终态注记：已收口（2026-09-29 执行席）。修复合入 0d353e9（merge 7b5acb4）：rmw_helpers.rs 探针 Err 臂改 match 收接，log 后先 obj_out.reset() 再 return Err(())，与 unchanged=false / apply_rmw_post_operate / 物化三兄弟臂同一清场纪律，零新机制。测试 wnode/tests/rmw_recheck_probe_io_err_single_frame.rs：FailAfterDevice 真设备层计数放行读注入（测试配置 ReadCache/copy_reads_to_tail 默认关，冷键单命令装载与复验两次盘读真实可分），放行首读注错次读定向命中「output 尾已有负载 + 探针失败」交叠面，断言单错误帧无 :15 残帧、失败轮零施加解除后续算闭环。worktree 内 cargo check --all-targets 零警告通过。
