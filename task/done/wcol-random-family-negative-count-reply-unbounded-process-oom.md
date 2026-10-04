甄别结论：通过（甄别席 J7，2026-09-27，定级 P1——负数 count 绕过校验致进程级 abort，纯参数放大即可触发）。hash_object_impl.rs:215 unsigned_abs 无上界、tiered hash.rs:590-596 n 直取、random_utils.rs:18-23 注释自认、tiered set.rs:198 Vec::with_capacity(n) 物化面，亲验全实；C# HashObjectImpl.cs:138-141 new int[indexCount]、RespServerSession.cs:566 会话级 catch 亲验；单命令进程级 abort vs C# 连接级，纯参数放大存态即可触发；入口钳制单点+set 臂去物化，方案合规。派沙箱席 c01o。

审核结论（2026-09-27）：立案通过，优先级高（单客户端单命令纯参数放大全进程 DoS，存态仅 1 成员即可触发）。
方案裁定：解析入口上界钳制，否决 try_reserve（写入点分散于对象层三臂 sink、tiered hash 直写、tiered set 中间 Vec、慢路径 run_operate，全覆盖即机制发散；且 try_reserve 只防分配失败 abort，不防内存充足时 GB 级慢速积聚，失败面收口不完整）。
阈值裁定：|count| 上限 i32::MAX >> 2（= 2^29-1 = 536870911），超界回受控错误帧。依据：与现有正 count 钳制常量同源复用（object_store_utils.rs:320 parse_random_member_args 的 min 钳制、C# HashCommands.cs:251 Math.Min(int.MaxValue >> 2) 同款），量级对齐 C# 隐式失败阈值（.NET 2GB 单数组上限 / sizeof(int) 约等于 5.37x10^8），成功域零削减（C# 能活的域全保留，偏差方向为 rust 更宽容且邻域不超过 21 个元素，可忽略）。
落点：HRANDFIELD/ZRANDMEMBER 在 parse_random_member_args 单点补负向 clamp；SRANDMEMBER 为三入口（set_commands/read.rs:149 快路径、set_commands/slow.rs:243 慢路径、tiered_collection_ops/set.rs:140 树内臂），共用同一常量收口。
票面量级修正两处（不翻案）：一、HRANDFIELD/ZRANDMEMBER 走 30 位打包域（parse_random_member_args:320 min 只钳正数，arg1 打包往返域 [-2^29, 2^29-1]，与 C# HashCommands.cs:253 同款逐位一致），票例 -1073741823 实际回绕 +1 无危害，真实有效极值 -536870912，约 5.37x10^8 元素，恰与 C# 隐式 OOM 阈值同量级，本质不变；二、SRANDMEMBER 为 i32 全宽直传（rust read.rs:149 / slow.rs:243 与 C# SetCommands.cs:647 均无钳制），|count| 最大 2^31，比票例大 4 倍，是最大放大面。
必补项（票方案缺口）：tiered_collection_ops/set.rs Srandmember 臂为 Vec::with_capacity(n) 加逐成员 to_vec 中间物化（n = unsigned_abs 直取），钳制后 n*48B 仍可达约 24GB 预分配立即 abort，单靠入口钳制救不了该臂，须一并改流式直写（对齐 tiered hash.rs scan_round 形态）；参数上界与内部去物化正交，不构成双机制。
测试补充：H/S/Z 三族负 count 极值快慢双路径回错误帧、进程存活、同连接续命令可用；tiered set 臂大 count 流式行为；SRANDMEMBER 三入口钳制一致。
不动项确认：正 count 互异域钳 min(count, size) 与无 count 形态维持原样（hash_object_impl.rs:211-213、tiered hash.rs:593、tiered set.rs:176-177 均已核实）。

随机成员族负 count 应答体积单命令无上界积聚，失败面进程级 abort 背离 C# 连接级收敛

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# HRANDFIELD/SRANDMEMBER/ZRANDMEMBER 负 count 放回采样（HashObjectImpl.cs:138-141 Math.Abs(countParameter) + indexCount > 256 ? new int[indexCount] : stackalloc 等）：|count| 增大到约 5.37x10^8 时辅助数组超 .NET 2GB 单对象上限抛 OutOfMemoryException，被 RespServerSession.cs:566 会话级 catch (Exception ex) 收纳，仅 DisposeNetworkSender 断本连接，服务器进程存活——失败面为连接级。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wcol/src/types/random_utils.rs:18-23 注释已自认该风险面并完成辅助空间修复（禁止按 k 预分配、sink 逐下标流式，空间 O(1)，测试 :107-111 锁定），但应答本体 O(k) 字节体积面未收口：信封臂 hash_object_impl.rs:215 index_count = count_parameter.unsigned_abs()、:228 sink 逐下标直写 output；分层臂 tiered_collection_ops/hash.rs:590-596 n = count_param.unsigned_abs()、:678-685 负 count 回绕补扫 while total < n 逐轮写 output；SRANDMEMBER/ZRANDMEMBER 同族同构。水位机制 take_output_watermark_yield（resp_server_session/pump.rs:226）只在命令边界检查（批内多命令背压），单命令执行内无水位：HRANDFIELD key -1073741823 每命令应答约 12+ 字节 x 10^9（WITHVALUES 翻倍）全部积聚 session.output 直至命令返回，分配失败 handle_alloc_error 直接 abort 整个进程，resp 层无 catch_unwind 兜底。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
单客户端单条命令即可触发全进程 DoS（纯参数放大，存态数据可忽略），违背板块「消除无界开销：网络缓冲必须具备背压与高低水位限制」与「异常收敛与透明传播：生产路径严禁未受控崩溃」；成功路径两侧逐字节一致，分叉仅在极端量级失败模式（连接级 vs 进程级）。

涉及代码：
rust 文件与函数：
wedb/wcol/src/types/random_utils.rs:pick_k_random_indexes（放回臂 for 0..k）
wedb/wcol/src/hash/hash_object_impl.rs:HashRandomField（index_count 无上界）
wedb/wnode/src/resp/objects/tiered_collection_ops/hash.rs:tiered HRANDFIELD 臂（回绕补扫 while total < n）

对应 c# 文件与函数：
garnet/libs/server/Objects/Hash/HashObjectImpl.cs:HashRandomField（int[] 辅助数组 2GB 上限）
garnet/libs/server/RespServerSession.cs:ProcessMessages（会话级 catch 收纳）

精炼执行方案：
1 对「应答元素数由客户端参数直控且与基数脱钩」的负 count 放回形态，在命令解析单点设上界钳制（对齐 C# 隐式失败阈值量级，如 |count| 上限 2^29），超界回受控错误帧（连接级失败，对齐 C# 失败面），或经 try_reserve 受控失败折错误帧——审核席裁定单点形态与阈值
2 正 count 互异域已钳 min(count, size) 不动；无 count 形态不动
3 测试验证点：负 count 极值命令回错误帧或钳制应答、进程存活、同连接后续命令可继续
