优先级：中
来源：next/agy.design.md 条 17 与 next/muse.design.md 条 18 的 resp_server_session.rs 部分。
取证基线：主仓 dev 当下代码，行号为当下实测。

问题
resp_server_session.rs 单文件 3172 行上帝会话：网络缓冲区管理、协议解析、鉴权、事务、
Lua 桥、指标与命令分派同文件堆叠，多个 impl 块按职责分段互相穿插。
C# 对位是部分类多文件组织，rust 并成一个文件与 C# 拓扑不吻合。

取证
- wedb/wnode/src/resp/resp_server_session.rs 全 3172 行
- 职责地标（实测）：:124 RespServerSessionOptions、:244 struct RespServerSession、
  :442 主 impl（含 :835 parse_get_and_key、:1149 process_messages、
  :1360 process_basic_commands、:1424 process_array_commands、
  :1448 process_transactional_command、:1467 process_pubsub_command、
  :1597 process_other_commands）、:2376/:2505/:3036 三个补充 impl 块、
  :2711 RespScriptingApi、:2783 parse_resp_command_buffer、:2798 impl TxnSession、
  :2965 impl PubSubSessionCommands
- C# 参考：garnet/libs/server/Resp/RespServerSession.cs 为主体，命令族与切面在
  同目录独立部分类文件分载（BasicCommands.cs、ArrayCommands.cs、ACLCommands.cs、
  AdminCommands.cs、ClientCommands.cs、Parser/、Objects/ 等）；C# 从不把全部会话职责
  并进一个文件

修法建议
按 C# 职责分界拆为 resp_server_session/ 目录模块：core.rs（结构体与缓冲区管理）、
parse.rs（协议解析与 parse_* 族）、dispatch.rs（process_*_commands 分派族）、
lua.rs（RespScriptingApi）、txn.rs（TxnSession impl）、client_info.rs、metrics.rs；
mod.rs 只留声明与 pub use 重导出，对外路径不变，消费侧零改动。
纯搬运禁夹带语义改动；可见性只调必要的 pub(crate)/pub(super)，禁为搬运扩面；
doc 注释随函数搬位一锚一位点。先例：同仓 list_commands/、sorted_set_commands/ 目录模块
与 next/tiered-collection-ops-file-split.md 的拆分口径。
