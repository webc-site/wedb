拒绝结论：判净（核对 C# api-compatibility.md，Garnet 全仓零 Stream 命令实现；wedb 仓内未挂载 Stream 模块并设自守测试，持久化与偏离核对无分叉，状态一致受控）

Stream 消息队列时序与持久化对标审查报告

一、审查视角与背景说明
审查视角：Stream 消息队列时序与持久化 (XADD/XREAD/XRANGE/XLEN 消息 ID 毫秒+序号单调递增性、* 自动生成 ID 碰撞处理、MAXLEN 裁剪边界与 AOF 重放一致性)
核查目标与范围：
1. 核查 wedb 中 Stream 相关实现（如 wnode/src/resp/stream/ 等）。若 wedb 暂未实现 Stream 或在 Garnet 中属特定状态，核实 C# 原型与工程现状。
2. 核查消息 ID（<millisecondsTime>-<sequenceNumber>）严格单调递增校验、自动生成 ID（*）时钟回拨防御、同毫秒序号递增溢出保护。
3. 核查 MAXLEN/MINID 裁剪时的边界处理、删空自愈以及 AOF 日志追加与重放一致性。
4. 对标 garnet/libs/server/Resp/Stream/ 或相应 C# 原型代码。
5. 核验 doc/zh/deviations.md 既有在册条款，严禁将既定架构改良报为缺陷。

二、原型行为与对标核查（C# Garnet 事实确证）
1. C# 官方契约与源码现状
对应 c# 文件与函数：
garnet/website/docs/commands/api-compatibility.md:API Compatibility
garnet/libs/server/Objects/Types/GarnetObjectType.cs:GarnetObjectType
garnet/libs/server/Resp/Parser/RespCommand.cs:RespCommand
garnet/libs/server/Resp/RespCommandInfoFlags.cs:RespAclCategories
garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:ArrayKeyIterationFunctions.DbScan
garnet/libs/server/AOF/AofEntryType.cs:AofEntryType

核查确证事实：
1) 官方 API 兼容性清单：
在 garnet/website/docs/commands/api-compatibility.md 中，Stream 分类下全部 13 个命令（XACK, XADD, XGROUP CREATE, XGROUP CREATECONSUMER, XGROUP DELCONSUMER, XGROUP DESTROY, XGROUP HELP, XGROUP SETID, XLEN, XRANGE, XREAD, XREADGROUP, XTRIM）明确标记为 ➖（not supported，不支持）。
2) 对象类型与命令枚举：
Garnet 内建对象枚举 GarnetObjectType 仅包含 Null=0, SortedSet=1, List=2, Hash=3, Set=4, All=0xfb，无 Stream 对象类型。
RESP 命令枚举 RespCommand 中无 Stream 命令（无 XADD/XREAD/XRANGE/XLEN/XTRIM 等枚举成员）。
3) 目录结构与实现代码：
Garnet 源码库中不存在 libs/server/Resp/Stream/ 目录，全仓无 Stream 消息队列实现代码。
4) ACL 分类位：
仅在 RespAclCategories 枚举中保留对标 Redis 规范的 Stream 分类标志位，但全仓无任何挂载该标志位的命令。
5) SCAN TYPE 过滤：
ArrayKeyIterationFunctions.DbScan 的 TYPE 参数过滤仅认 zset, list, set, hash, string，传 stream 会直接走未知类型分支终止扫描并返回空集。

三、工程现状确证（Rust wedb 实现核查）
1. 模块结构与命令分派
rust 文件与函数：
wedb/wnode/src/resp/parser/command_table.rs:COMMAND_TABLE
wedb/wresp/src/command.rs:RespCommand
wedb/wval/src/tag.rs:GarnetObjectType
wedb/wnode/src/resp/array_commands.rs:RespServerSession::network_type
wedb/wnode/tests/acl_tests.rs:test_acl_categories_and_expansion
wedb/wacl/src/acl_parser.rs:AclParser
wedb/waof/src/aof/entry_type.rs:AofEntryType

核查确证事实：
1) 仓内目录与命令：
wedb 未引入 wnode/src/resp/stream/ 模块，RespCommand 与 COMMAND_TABLE 中未分配亦未挂载任何 Stream 相关命令。
2) 对象类型系统：
GarnetObjectType 保持 Null=0, SortedSet=1, List=2, Hash=3, Set=4, RangeIndex=5, All=0xfb，类型层对齐 Garnet，未引入 Stream 类型。
3) TYPE 命令响应：
network_type 严格按 String 域、Meta 域、信封域与扩展对象清单进行类型判读，无 Stream 歧义。
4) ACL 分类自守断言：
wedb/wnode/tests/acl_tests.rs 明确维护自守断言：commands_for_category(RespAclCategories::STREAM).count() == 0，显式声明 STREAM 分类当前无挂载命令，属于有受控意识的工程维护。
5) 持久化与 AOF 条目：
AofEntryType 中定义的 RangeIndexStreamChunk 属于 RangeIndex（BfTree）数据迁移分块通道，全仓无 Stream 消息队列的 AOF 记录类型。

四、核查视角规约确证与储备分析
针对本审查视角所涉的技术要点，确证如下：
1. 消息 ID 单调性与溢出防御：
Redis 规范要求消息 ID 形如 <millisecondsTime>-<sequenceNumber>，必须严格大于当前 Stream 最大 ID，否则报错 ERR The ID specified in XADD is equal or smaller than the target stream top item。针对自动生成 ID（*），需防御系统时钟回拨（时钟回拨时保持当前最大毫秒并递增序号）及同毫秒 64 位序号递增溢出保护。目前双侧均未实现该状态机，wedb 时间基础设施（wbase/src/convert.rs）具备成熟的时间戳钳制与防溢出机制，后续若引入 Stream 模块可直接复用该防御范式。
2. 裁剪边界与删空自愈：
Redis 规范中 MAXLEN/MINID（精准 = 与近似 ~）支持限制队列容量，超额时按 FIFO 从头部淘汰节点；当 Stream 所有条目被删空时触发键清理。wedb 仓内针对集合类型已具备完善的双态自适应分层、生命周期管理与删空自愈闭环（参见 doc/zh/collection.md 与 doc/zh/deviations.md 第 19 条）。未来引入 Stream 模块时应严格遵循该删空自愈与墓碑回收契约。
3. AOF 日志追加与重放幂等：
XADD 写入时若使用 * 自动生成 ID，AOF 必须记录服务端已决定的确定性 ID（而非 *），确保主从复制与 AOF 重放逐字节一致。当前双侧均无对应 AOF 条目，无重放发散隐患。
4. 偏离在册确证：
doc/zh/deviations.md 全文核验无 Stream 相关有意偏差条目，Garnet 与 wedb 均处于明确未支持（➖）状态，契约完全对齐，不存在既定架构改良误报问题。

五、结论总结
本席对 C# Garnet 原型与 Rust wedb 仓内全链路进行了深度逐项核查。确证 Garnet 原型中 Stream 命令族（XADD/XREAD/XRANGE/XLEN 等）完全处于未实现状态（官方文档标记 ➖，无源码）。wedb 严格遵守与 Garnet 的契约对标，目前未实现 Stream 模块，且仓内 ACL 测试通过自守断言显式约束 STREAM 分类命令数为 0。视角所涉的消息 ID 单调递增、时钟回拨防御、MAXLEN 裁剪边界与 AOF 重放一致性等问题在当前代码基座中均无可立案的缺陷，契约与工程现状纯净收敛。

视角结论:已穷尽
