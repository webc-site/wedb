优先级：低

问题：wresp 的 RespCommand 枚举把 Delifexpim 与对外命令混排在一起且定义处零注释，两轮独立审查均将其误判为「有名无实的僵尸命令」。实际它是 TTL 过期物理清除的内部 RMW 条目，对外协议不接线是与 C# 一致的正确设计，但枚举定义处（读者第一入口）没有任何说明，容易被当成遗漏的公开命令去补 parser 条目。

取证（rust）：
- wedb/wresp/src/command.rs:29 `Delifexpim = 9`，定义处无注释；command_table.rs 全表无 DELIFEXPIM 条目、raw.rs/slow.rs 无 `C::Delifexpim` 分派臂——不接线现状正确
- 内部链路实际在用：wedb/wnode/src/service.rs:342 以 `ReplayInputSlice::new(RespCommand::Delifexpim, ..)` 做 TTL 物理清除；wedb/wnode/src/aof/aof_processor.rs:994 AOF 重放判定 `input.cmd == RespCommand::Delifexpim`；wedb/wnode/src/resp/key_admin_commands/keys.rs:681 已有「C# 为 RMW-EXPIRE 条目（重放端 DELIFEXPIM 确定性）」注释；集成测试 wedb/wnode/tests/service.rs:283 断言 AOF 流中单条 DELIFEXPIM StoreRMW

C# 对标（同为内部条目、无 RESP 外部接线）：
- garnet/libs/server/Resp/Parser/RespCommand.cs:39 `DELIFEXPIM = 9`
- garnet/libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:27/:67/:185/:202 内部 RMW 分派
- 消费面全在存储层：garnet/libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:120、garnet/libs/server/API/GarnetApiUnifiedCommands.cs:77、garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:221，RespServerSession 无分派

修法建议：仅在 wedb/wresp/src/command.rs:29 `Delifexpim` 变体处补一行文档注释「TTL 过期物理清除内部 RMW 条目（对标 C# RMWMethods DELIFEXPIM），仅 AOF/重放链路使用，对外协议不接线，勿补 parser 条目」。禁补对外接线（违反 transpile SKILL 1:1 对标：C# 无 RESP 分派）。
