拒件：RIPROMOTE/RIRESTORE 无命令无枚举，文档与实现脱钩

来源：next/muse.my.md 条 3。判定：不成立（语义已实现且注释映射 C# 原语；C# 侧同为内部派发值非客户端命令）。

拒绝原因
1 实现在位：rust 语义经 wkv/src/range_index/stub.rs:278-344 acquire_tree_read 惰性自愈承接——刷盘存根（IsFlushed）先 RIPROMOTE 重提升至日志尾部清位、恢复态走 RIRESTORE 存根回写（新句柄写回 + 清 Recovered 位），注释逐段映射 garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:949/:954 两 case 臂；wkv/tests/store/range_index.rs:756-866 全周期（刷盘 RIPROMOTE / 下线 RIRESTORE / 计数不触发）测试在场。「原语名全仓零实现」不实。
2 RespCommand 无枚举不构成脱钩：C# 的 RIPROMOTE/RIRESTORE 是内部 RMW 派发值（MainStore/RMWMethods.cs case 臂 + RespCommand 枚举内部分支），客户端解析表本就无条目；rust 以函数内调用形态（acquire_tree_read 内两段）承接属合理转写，无需造枚举。doc/zh/collection.md:81 以原语名指称机制与 stub.rs 注释互指一致。
3 恢复路径「靠 acquire_tree_read 惰性自愈」正是 C# 同形（C# 恢复亦首次访问触发），文档无须改称。

引证
wedb/wkv/src/range_index/stub.rs:278-344；wedb/wkv/tests/store/range_index.rs:290/:756-866；garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:PromoteToTail、MainStore/RMWMethods.cs:949/:954。
