# 拒绝意见：NullDevice 断头链接线方案 (null-device-broken-chain)

来源：next/design.md 条 2。意见原文给出两个改法选项："在 wnode AOF 装配点按选项接 NullDevice，或整链删除"。

## 拒绝部分

### 1 拒绝"在 wnode AOF 装配点按选项接 NullDevice"

拒绝原因：

1 拓扑不成立。C# NullDevice 依托 TsavoriteLog 的 LogDevice 可插拔层注入
（garnet/libs/server/Servers/GarnetServerOptions.cs:1215 GetAofDevice 内
`if (UseAofNullDevice) return new NullDevice()`，经 GetAofSettings →
libs/host/GarnetServer.cs:516 装配）。rust AOF 后端已定型为 waof
`WalLog<SegmentedDevice>` + `WaofSublog<SegmentedDevice>` + `Sublog::Mem(InMemorySublog)`
双形态（wnode/src/aof/waof_sublog.rs:27 single_log_aof 为唯一权威装配点），
Sublog 枚举硬编码 SegmentedDevice；接入 NullDevice 须把 Sublog/GarnetLog/
NodeService 全链设备泛型化或加枚举变体，拓扑重创且偏离既有 rust 设计。

2 制造重复机制。rust 无盘 AOF 能力已由 InMemorySublog（纯内存子日志，
wnode 多处测试在用）承载，语义与 C# "UseAofNullDevice 无盘 AOF"完全对应；
再接 NullDevice 即第二套无盘 AOF 机制，违背"清理多套重复机制"审查目标。

3 功能面残缺。C# UseAofNullDevice 的另一消费面 AllowDataLoss
（GarnetServerOptions.cs:654，复制域丢数据容忍语义，消费点
libs/cluster ReplicaSyncSession.cs:189、AofSyncDriverStore、AofReplayCoordinator）
rust 侧尚未实现；单接设备面无行为意义。

4 无写入面。rust NodeArgs 无 --aof-null-device 旗标，选项只可能恒为
false，接线后也是永不可达分支。

### 2 待办位置信息部分过时

意见原文 "wconf/src/garnet_options.rs:550-553（校验）" 已失效：
garnet_options.rs 已被 clean-dead-server-options 整文件删除（见
task/done/clean-dead-server-options.md，C# GarnetServerOptions.cs 亦整文件
ignore）。C# 校验（GetAofDevice 的 cluster+null device 冲突 throw）随整文件
淘汰，rust 无独立校验链残留，无需也无法删除。

## 采纳部分

"整链删除（选项 + 校验 + 设备）"方向采纳，实际删除面为 rust 现存部分：
use_aof_null_device 字段、ServerConfigType::AofNullDevice、
aof-null-device 配置槽面、wdev NullDevice 设备及测试。
证据链与执行结果见 task/done/null-device-broken-chain.md。
