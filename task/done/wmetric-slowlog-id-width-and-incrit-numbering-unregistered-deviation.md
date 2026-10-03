终态：已合入 dev（2026-09-27）。bc10c12 §167 登记:i64 不回绕+临界区串行 vs C# int 回绕/锁外取号,上游缺陷修复型分叉,三头注互引零行为

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：落册候号取 §164（票面 §162 已过时，§163 已被占）

审核结论：通过（登记级，零行为改动。双侧锚全中：C# SlowLogContainer.cs:17/:40-45 int 锁外取号后 Enqueue（倒置竞态真）、SlowlogEntry.cs:11 int Id、RespSlowlogCommands.cs:74 TryWriteInt32；rust slow_log_container.rs:24 AtomicI64、:54-55 临界区内取号与 push_back 绑定、resp_slowlog_commands.rs:94 write_resp_int 全值；两锁测在册（slowlog_id_width.rs/slowlog_concurrent_id_order.rs）。查重：册内 SLOWLOG 仅第 25 目与 §103 随行面，无 id 位宽/取号条；五池无撞票；§108 先例同形。定性：2^31 回绕生产不可达仅测面 seed_id 复刻，倒置面并发可达，对拍必发散+防回改误修，登记级成立）

整理执行方案（审核席订正版，供 fix 消费）：
1 deviations 册尾顺编新条（现册尾 §162，候号 §163 起、先入库得号撞号让位）：双宗并一条（同容器取号单链路）；钉两可观测量（跨 2^31 负 id 形、并发 id/物理序倒置形）
2 定性措辞=上游缺陷修复型刻意分叉（§16/§108 家族）；回绕宗标「生产不可达仅测面 seed_id 复刻」，倒置宗标「并发可达」
3 防回改锁声明：严禁 int 截断与锁外取号接回，锁测锚两测文件名，触 5.2 度量无害+数值域防御红线；代码注释互引锚三处（容器头注+两测头注）

SLOWLOG 条目 id 位宽与临界区内取号分叉未入偏差台账（C# int 2^31 回绕负值加锁外取号物理倒置竞态，rust 修复形在册测试无登记锚）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 一手形态：SlowLogContainer.cs:17 计数器 int、:40 Add 于入队前取号 Interlocked.Increment(ref id) - 1，取号与 Enqueue（:41）非原子，两线程调度错位可先入队后取小值号，SLOWLOG GET 出面 id 与物理顺序可倒置；累计入库跨 2^31 后 id 回绕 int.MinValue，GET 输出负 id。SlowlogEntry.cs:11 Id 为 int，RespSlowlogCommands.cs:74 以 TryWriteInt32 写出。容量裁剪 :42-45 while(Count > size) 在并发下瞬态可超上限。C# 用例 test RespSlowLogTests.cs 仅正序钉 entry[0]==Id，未锁回绕与倒置形。真 Redis slowlog id 亦为累积自增、RESET 不复位，与 C# 同谱，但 Redis 无 2^31 回绕与倒置面（单线程取号入队原子）。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧 SlowLogContainer（wmetric/src/slowlog/slow_log_container.rs:18-25）id 为 AtomicI64，add（:50-60）取号置于 log_entries 同一互斥临界区内与 push_back 强串行绑定，物理顺序与 id 单调严格一致，满容先出后入恒不超上限；慢日志面 resp_slowlog_commands.rs:94 write_resp_int(entry.id) 以 i64 全值写出，slowlog_entry.rs:11 id 为 i64。行为钉死测试已存在：wmetric/tests/slowlog_id_width.rs（越 i32::MAX 不回绕不为负）、slowlog_concurrent_id_order.rs（并发取号入队无倒置），容器头注 :14-17 与测试头注均自陈分叉理由——系真锚但非登记锚：doc/zh/deviations.md 全册 SLOWLOG 仅第 25 目（GET count 严格文法）与 §103 随行锁面一笔，无本条 id 位宽与取号序列化登记；五池（issue/todo/ing/done/reject）现无慢日志票。与 §108「码面原有自述系真锚但非登记锚，本票补登互引」先例同形。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
纯治理台账面，无运行时危害：不登记则对拍夹具在「seed 高 id 跨 2^31 后 GET」与「并发慢查询入库 GET id 序」两形上双侧必然发散（C# 负 id/可倒置 vs rust 非负单调），后审席可能误判转写缺陷按 C# 回改——回改即把 int 截断负 id 与锁外取号竞态接回 rust，触本仓 5.2 度量采集无害与数值域边界防御面，且 slowlog_id_width.rs 与 slowlog_concurrent_id_order.rs 两锁测必红，属真回归。

涉及代码：
rust 文件与函数：
wedb/wmetric/src/slowlog/slow_log_container.rs:SlowLogContainer（id: AtomicI64）/ add / get_entries
wedb/wmetric/src/slowlog/resp_slowlog_commands.rs:RespSlowlogCommands::network_slow_log_get / handle_slow_log
wedb/wmetric/src/slowlog/slowlog_entry.rs:SlowLogEntry.id

对应 c# 文件与函数：
libs/server/Metrics/Slowlog/SlowLogContainer.cs:SlowLogContainer.Add（id 字段 :17，取号 :40，裁剪 :42-45）/ GetEntries
libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:NetworkSlowLogGet（:74 TryWriteInt32(entry.Id)）
libs/server/Metrics/Slowlog/SlowlogEntry.cs:SlowLogEntry.Id（:11）

精炼执行方案：
1. doc/zh/deviations.md 册尾顺编补登一条 SLOWLOG id 位宽与临界区取号序列化登记条（按落册时现册册尾实况顺编取号，先入库者得号、撞号不覆写），钉两宗外部可观测量：跨 2^31 id 回绕负值形、并发入库 id/物理序倒置形，标注上游缺陷修复型家族（同 §16/§108 口径），严禁按 C# 回改（int 截断与锁外取号均禁接回）。
2. 零行为改动零代码改动；仅在 slowlog_id_width.rs 与 slowlog_concurrent_id_order.rs 头注及 slow_log_container.rs 头注各补一行新登记条互引锚（纯台账锚，比照 §108 补登先例）。
3. 测试验证点：既有 wmetric/tests/slowlog_id_width.rs、slowlog_concurrent_id_order.rs 全绿，本票零新增用例；对拍轮遇该两形直引新条免复勘。
