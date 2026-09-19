GarnetClient max_outstanding_tasks 形参零效果：读法 .max(CHANNEL_CAP) 只能抬容量不能限在途

来源：qcode 第 10 轮 net 条 8（next/qcode10.net.md:71-73）。取证基线 dev。

问题
- wconn/src/client.rs:21-22 字段注释写「在途命令上限」，:87 唯一读点是
  self.max_outstanding_tasks.max(CHANNEL_CAP)，即该参数只能抬高命令通道容量、永远压不小；
  CHANNEL_CAP=1024（wconn/src/types.rs:12）。生产唯一构造点 wedb/src/client.rs:100 传 32，
  测试传 16/64，全部被抬到 1024 → 「在途命令上限」在任何调用下恒为 1024，形参零效果，
  注释与实现互为反话。
- 泵侧与会话侧（wconn/src/network/pump.rs:57、wconn/src/session.rs:51）各自直用 CHANNEL_CAP，
  完全不受该形参影响，属第二处容量声明。

C# 对侧是真实准入闸
- libs/client/GarnetClient.cs:51 字段、:118 GetOutstandingTasksLimit 探针、
  :135 参数文档「Maximum outstanding tasks before client throttles new requests
  (rounds down to previous power of 2)」、:151 缺省 1<<19、
  :167-174 上限与向下取幂双校验（不合即 ThrowException）、:180 tcsArray 按该值定长分配、
  :569 PipelineLength()、:579-590 InputGateAsync（不足即 1/2/4…4096ms 指数退避）、
  :636/:749/:957/:1075 四条 InternalExecuteAsync 重载下发前各调一次（:659/:772/:975/:1092）。

修法
- 按 C# 语义把该参数实现为客户端准入闸：构造期做「>0 且为 2 的幂，否则向下取幂」校验
  （与 C# 同档的显式错误，不得静默），在途计数与闸口单点落地，四条下发路径共用一处判定。
- 删除 .max(CHANNEL_CAP) 的反话读法与第二处容量口径（泵/会话侧改为读同一闸或同一常量单源）。
- 不做向上兼容：旧「形参被抬高」的行为直接消失。
- 补测试：非 2 的幂入参构造即失败；在途达到上限时新请求退避而非无限排队；
  GetOutstandingTasksLimit 对位探针可读。

验收
- cargo test -p wconn；cargo check -p wedb；bun js/check.js exit 0。
- 无 #[allow(、无 #[ignore]、无恒真断言、无新增第二处容量常量。

## 实现方案（f58-client-gate 细化，dev 基线核实）

语义核实
- C# GarnetClient.cs:167-174 实际行为：超 kTaskMask+1 抛异常、非 2 的幂抛异常
 （PageOffset.cs kTaskBits=20 → 上限 1<<20；参数文档「rounds down」与实现不符，
  以实现为准，票据「显式错误，不得静默」按抛错落）。
- crossfire mpsc::bounded_async：容量 0 静默按 1、非 2 的幂静默取整
 （crossfire src/flavor/array.rs:Array::new）——构造期显式校验拦静默行为。
- 闸的 rust 落点：C# tcsArray 定长槽 = maxOutstandingTasks、InputGateAsync 满即
  退避；rust 对位 = 命令通道与泵内 in_flight 通道容量同为闸值（在途未回收数
  ≤ 闸），调用方 send 挂起即退避（唤醒式，语义同 C# 轮询退避）。

改动（wconn 为主）
1. error.rs：新增 Error::InvalidOutstandingTasks(usize)（文案对标 C# ThrowException）。
2. client.rs：MAX_OUTSTANDING_TASKS=1<<20（对标 PageOffset.kTaskMask+1）；
   new -> Result<Self>，非 2 的幂或超上限即 Err（is_power_of_two 拦 0）；
   connect_async 命令通道 bounded_async(self.max_outstanding_tasks)（删 .max(CHANNEL_CAP)），
   network_loop 透传闸值；新增 get_outstanding_tasks_limit() 探针（C# :118 对位）；
   字段注释改为真实准入语义。
3. network/pump.rs：network_loop 加 gate 形参，in_flight 通道 bounded_async(gate)
   （对位 tcsArray 定长槽）。
   配套死锁修复（票据未点名、闸启用后必炸的前置缺陷）：写泵挂 in_flight send 前
   必须先 flush out_buf——否则批量中途挂起时帧未写出、应答不来、in_flight 永不
   腾位（现状 1024/1024 同容量触发窗口极窄，闸 32 后必触发）。改 try_send，
   Full 即先 write_all 刷出再 await send。
4. session.rs：network_loop(..., CHANNEL_CAP)；命令通道保持 CHANNEL_CAP（C#
   GarnetClientSession 无 maxOutstandingTasks 形参，会话层容量走 types 单源）。
5. types.rs：CHANNEL_CAP 注释收敛为「GarnetClientSession 会话层在途通道缺省容量」。
6. 调用点：wedb/src/client.rs 闸值 32 为编译期常量恒过校验（expect）；其余测试
   调用点（16/32/64 全 2 的幂）unwrap。
7. 测试：client.rs 单测（0/33/1<<21 构造 Err、32 Ok、探针读数）；network/mod.rs
   集成测 gate_backpressure：闸 2 静默端点，3 帧全部到达（flush 修复生效）、
   静默期第 3 条命令短窗不完成（退避）、回 3 应答后 3 条全 Ok（回收放行）。

不做
- PipelineLength 公共面：rust 闸由通道容量承担，无轮询估计需求（C# 该函数读者
  仅 InputGateAsync 内部）；progress 的 enqueued-reclaimed 已是同源计数不另设。
- 不实现向下取幂（C# 实现即抛错，文档注释与实现不符以实现为准）。
