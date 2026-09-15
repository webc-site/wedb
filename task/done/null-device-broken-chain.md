# NullDevice 配置断头整链删除 (null-device-broken-chain)

来源：next/design.md 条 2（主代理预清理后的残留待办）。

## 甄别结论

待办成立，方向取"整链删除"，拒绝"接线"方案。证据：

1 rust 侧 use_aof_null_device 是彻底断头链
- 全仓生产装配零消费：AOF 唯一权威装配点 single_log_aof（wnode/src/aof/waof_sublog.rs:27）硬编码 WalLog<SegmentedDevice>，从不读该字段
- 无 CLI 写入面：NodeArgs 无 --aof-null-device，字段只可能恒为默认 false
- CONFIG aof-null-device（runtime_server_config.rs 只读槽）只回显该恒 false 字段，纯死镜子
- 待办原文的"wconf/src/garnet_options.rs:550-553（校验）"已过时：该文件已被 clean-dead-server-options 整文件删除，rust 无独立校验链残留

2 NullDevice 设备本体无真实消费方
- 全仓仅 wdev 自己的测试（wdev/tests/device/null.rs）引用
- C# SubscribeBroker 内存日志生态位：rust wpubsub 已是纯内存队列，无设备概念
- C# UseAofNullDevice 无盘 AOF 生态位：rust 由 Sublog::Mem(InMemorySublog)（纯内存子日志，wnode 多处测试在用）承载，语义已覆盖

3 接线方案拒绝理由
- C# NullDevice 依托 TsavoriteLog 的 LogDevice 可插拔层（GarnetServerOptions.GetAofDevice 返回 new NullDevice()）；rust AOF 后端已定型 WaofSublog<SegmentedDevice> + InMemorySublog 双形态，接线需把 Sublog/GarnetLog/NodeService 全链设备泛型化，拓扑重创
- 接线结果与既有 InMemorySublog 机制能力重复，制造第二套无盘 AOF 机制，违背"清理多套重复机制"审查目标
- C# 中 UseAofNullDevice 的另一消费面 AllowDataLoss（复制域丢数据容忍语义）rust 侧尚未实现，单接设备面无意义

## 对标关系

- garnet/libs/server/Servers/GarnetServerOptions.cs:435 UseAofNullDevice 字段 → wconf runtime_server_options.rs use_aof_null_device（删）
- garnet/libs/server/Servers/GarnetServerOptions.cs:1215 GetAofDevice（含 1217 校验 + 1219 new NullDevice）→ rust 无对应（C# 侧已随 GarnetServerOptions.cs 整文件 ignore 淘汰）
- garnet/libs/server/Config/ServerConfigType.cs:62 AOF_NULL_DEVICE → wconf server_config_type.rs AofNullDevice = 37（删）
- garnet/libs/server/Config/RuntimeServerConfig.cs:157 SetReadOnly aof-null-device → wconf runtime_server_config.rs META/NAME_LOOKUP/RUNTIME_TYPES/fmt_aof_null_device（删）
- garnet/libs/storage/Tsavorite/cs/src/core/Device/NullDevice.cs → wdev/src/null.rs（删）

## 改动点

1 wconf/src/runtime_server_options.rs
- 删字段 use_aof_null_device 及文档注释、Default 中初始化

2 wconf/src/server_config_type.rs
- 删变体 AofNullDevice = 37 与 ALL_MEMBERS 成员，数组长度 38 收缩为 37

3 wconf/src/runtime_server_config.rs
- 删 fmt_aof_null_device
- 删 META 中 37 号只读槽（表长随 TABLE_SIZE 自动收缩）
- 删 NAME_LOOKUP "aof-null-device" 条目，长度 36 收缩为 35
- 删 RUNTIME_TYPES AofNullDevice 成员，长度 35 收缩为 34
- compute_table_size 与模块级 TABLE_SIZE 改以 FastAofTruncate 为最大判别值

4 wconf/tests
- garnet_server_config_tests.rs 删 assert!(!o.use_aof_null_device)
- runtime_server_config.rs 测试删 assert!(types.contains(&ServerConfigType::AofNullDevice))

5 wdev
- 删 src/null.rs、src/lib.rs 的 mod null 与 pub use null::NullDevice
- 删 tests/device/null.rs 与 tests/device/mod.rs 的 mod null

6 js/check/ignore
- 新增 libs/storage/Tsavorite/cs/src/core/Device/NullDevice.yml：文件级淘汰理由（rust AOF 无设备可插拔层，无盘场景由 InMemorySublog 承载，设备无生产消费方）
- GarnetServerOptions.cs 已整文件 ignore，无需重复登记
- 删除后跑 bun ./js/check.js，若 ServerConfigType.cs 或 RuntimeServerConfig.cs 出现新增缺失再按需登记

7 越界观察（只记录不修改）
- wconf/src/device_config.rs DeviceType::Null = 4（对标 C# DeviceType.cs:Null = byte.MaxValue）零消费，属设备选项面孤儿，不在本待办范围，留待设备配置面任务统一裁决

## 验收口径

1 bun ./js/check.js 无新增缺失（基线 0 缺失 0 重复）
2 ./clippy.sh 零警告（禁 allow）
3 ./test.sh 全量通过
4 全仓 grep use_aof_null_device / AofNullDevice / NullDevice 仅剩 device_config.rs DeviceType::Null 观察项与 ignore 登记

## 验证结果

分支 w1-null-device（/tmp/fork/w1-null-device），2 个 commit：

1 4575200 wconf: 删除 use_aof_null_device 断头选项与 AofNullDevice 配置面
（wconf 5 文件，+11 -26）
2 wdev: 删除 NullDevice 空设备及测试，同步文档面与 ignore 登记
（11 文件，+15 -228；删 wdev/src/null.rs 与 wdev/tests/device/null.rs）

删除清单：
- wconf/src/runtime_server_options.rs：use_aof_null_device 字段 + 默认值
- wconf/src/server_config_type.rs：AofNullDevice 变体 + ALL_MEMBERS 成员（38→37）
- wconf/src/runtime_server_config.rs：fmt_aof_null_device、META 37 号只读槽、
  NAME_LOOKUP "aof-null-device"（36→35）、RUNTIME_TYPES 成员（35→34）、
  TABLE_SIZE/compute_table_size 改锚 FastAofTruncate（38→37）
- wconf/tests：table_size 断言 38→37、garnet_server_config_tests 默认值断言、
  runtime_types_contains 断言改锚 FastAofTruncate
- wdev：src/null.rs、lib.rs mod 与导出、tests/device/null.rs、device/mod.rs 注册
- 文档面：README.md、readme/zh.md、readme/en.md、wdev/README.md、
  wdev/readme/zh.md、wdev/readme/en.md 全部 NullDevice 引用
- js/check/ignore/storage.yml：登记 NullDevice.cs 文件级淘汰理由

验证结果：
1 bun ./js/check.js：0 输出，退出码 0（无新增缺失、无重复）
2 cargo clippy --workspace --all-targets：0 警告 0 错误
3 ./test.sh 全量：wdev 套件 1992 过 1 跳过；regress 套件 2 过
4 残留扫描：仅 wconf/src/device_config.rs DeviceType::Null 文档注释
（范围外观察项，已登记待设备配置面任务裁决）

合并：w1-null-device 已同步 dev 并合回主目录（见 git log）。
