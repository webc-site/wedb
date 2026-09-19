优先级：低
分拣注记（qw.design 第 11 轮条 6 拆出；浅核 2026-09-19：result2 在 wcol/src/resp/output.rs:35 在场（行号小漂移）；与 ing/object-output-payload-direct-write.md 不同题——那票管 payload 直写去二次拷贝，本票管 result2 死字段删除，两票同文件须排执行序）

wcol ObjectOutput::result2 死字段：全仓仅定义行出现，C# ObjectOutput 无此字段且结构注释
明令「加的每个字段必须后端写、前端读」
问题：wcol/src/resp/output.rs:34 pub result2: i64（次级结果计数）在全仓（含 tests/、js/、sh/、readme/）
出现次数 = 1，即无人写亦无人读；同结构其余字段 payload/result1/output_flags 均有产消。
修法：删字段。
c#：garnet/libs/server/Objects/Types/ObjectOutput.cs:36 struct ObjectOutput（字段仅 SpanByteAndMemory、
GarnetObject、result1、OutputFlags，:33 注释规定字段须后端写、前端读成对使用）
