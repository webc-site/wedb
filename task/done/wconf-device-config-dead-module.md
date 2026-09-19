优先级：低
wconf 设备配置模块整体死码：DeviceType/DeviceOptions/DeviceConfigError 全仓零消费，rust 设备形态固定 SegmentedDevice 无任何类型分派与选项注入，也无 ignore 登记；同 crate ConfigNameComparer 同病
  具体问题：wconf/src/device_config.rs 整文件是「设备类型 + 设备选项」的配置域对位转写：DeviceType 四变体（LocalStorage/Native/LocalMemory/Sharded）、DeviceOptions 五字段（device_type/capacity/delete_on_close/preallocate_file/recover_device）+ validate + DeviceConfigError，经 wconf/src/lib.rs:11 与 :23 对外导出；但全仓（含 wconf 自身其余模块、含测试）除定义与 re-export 外零消费——生产设备构造在 wnode 侧固定 SegmentedDevice（service.rs:38 use wdev::{Device, SegmentedDevice}，无类型分派、无 preallocate/deleteOnClose/capacity/recover 任何注入面），NodeOptions 亦无嵌套引用。C# 侧该配置链是活的：命令行 --device → GarnetServerOptions.DeviceType → GarnetServerOptions.cs:819-826 组装 CreateLogDevice 调用 → Devices.cs:36-59 按 DeviceType 分派 LocalStorage/Native/LocalMemory/Sharded 四设备并消费 deleteOnClose/preallocateFile/capacity/recoverDevice 各参数。js/check/ignore 无 DeviceOptions.cs/DeviceType 的不移植登记（storage.yml:2687 与 hosting.yml:15 只是无关测试方法名），即该模块既非「已裁定不移植」也非「待接线」，是转写后断线的死模块。同病第二件：wconf/src/config_name_comparer.rs ConfigNameComparer 全仓零消费，C# 对位 ConfigNameComparer.Instance 是活口（RuntimeServerConfig.cs:221 用作 CONFIG GET 按名查询字典的 byte[] 比较器），rust 按名解析走 RuntimeServerConfig::try_get_type 的 NAME_LOOKUP 线性 eq_ignore_ascii_case 扫描（runtime_server_config.rs:948-953），形态已换、对位件闲置。修法：compio 单设备后端形态下删除 device_config.rs 整模块与 lib.rs 两行导出，并在 js/check/ignore 登记（DeviceType 四态分派由 wdev SegmentedDevice 单设备承接的理由）；ConfigNameComparer 删除或并入 try_get_type 单点，勿留「转写了没人用」的第二套设备配置面。与 qcode10.db.md 条 1/2（GcConfig 旋钮缺写侧）不同面：那两条管字段无写侧，本条管整模块零消费且无登记。
  rust：wedb/wconf/src/device_config.rs:8-18 DeviceType、:29-52 DeviceOptions 与 Default、:56-61 validate、:21-25 DeviceConfigError；wedb/wconf/src/lib.rs:11/:23 模块声明与导出；设备构造面 wedb/wnode/src/service.rs:38（固定 SegmentedDevice）；ConfigNameComparer wedb/wconf/src/config_name_comparer.rs:9-（impl ConfigNameComparer）；按名解析现役单点 wedb/wconf/src/runtime_server_config.rs:948-953 try_get_type
  C#：libs/server/Servers/GarnetServerOptions.cs:440（DeviceType 活旋钮声明）、:819-826（组装 CreateLogDevice 调用与 NativeDeviceOptions）；libs/storage/Tsavorite/cs/src/core/Device/Devices.cs:36-59（CreateLogDevice 按 DeviceType 分派四设备，消费 preallocateFile/deleteOnClose/capacity/recoverDevice 参数）；libs/storage/Tsavorite/cs/src/core/Device/DeviceOptions.cs（rust 文件头自称对位件本体）；libs/host/Configuration/Options.cs:978（命令行设备选项源）；ConfigNameComparer 链 libs/server/Config/ConfigNameComparer.cs 与消费点 libs/server/Config/RuntimeServerConfig.cs:221

## 细化方案（实现代理追加）

甄别核实（2026-09-19）：
- 全仓 grep DeviceType/DeviceOptions/DeviceConfigError：仅 wedb/wconf/src/device_config.rs 定义与 lib.rs:11/:23 导出，零外部消费（含测试）
- 全仓 grep ConfigNameComparer：仅 wedb/wconf/src/config_name_comparer.rs 定义与 lib.rs:9/:21 导出，零消费
- wnode/src/service.rs:38 固定 use wdev::{Device, SegmentedDevice}，:747/:807 设备构造均 SegmentedDevice::single_file，无类型分派无选项注入
- ignore 现状：storage.yml 已有 Devices.cs 文件级登记（设备构造单点 wdev，无工厂分发）；DeviceType.cs / DeviceOptions.cs / ConfigNameComparer.cs 均无登记
- C# 对位核实：DeviceType 枚举独立成文件 DeviceType.cs；DeviceOptions.cs 本体是 NativeDeviceOptions/LocalMemoryDeviceOptions；ConfigNameComparer.cs 是 byte[] 大小写不敏感比较器，rust 按名解析由 runtime_server_config.rs try_get_type 的 eq_ignore_ascii_case 扫描承接（形态已换，对位件闲置）

改动清单：
1. 删 wedb/wconf/src/device_config.rs（整文件）
2. 删 wedb/wconf/src/config_name_comparer.rs（整文件）
3. wedb/wconf/src/lib.rs 删 4 行：pub mod config_name_comparer; / pub mod device_config; / pub use config_name_comparer::ConfigNameComparer; / pub use device_config::{DeviceConfigError, DeviceOptions, DeviceType};
4. 登记 js/check/ignore/garnet/libs/storage/Tsavorite/cs/src/core/Device/DeviceType.yml：设备形态固定 wdev SegmentedDevice 单设备，无四态分派消费面（判定依据同 storage.yml Devices.cs 文件级登记）
5. 登记 js/check/ignore/garnet/libs/storage/Tsavorite/cs/src/core/Device/DeviceOptions.yml：NativeDeviceOptions/LocalMemoryDeviceOptions 为特定设备后端调优参数，rust 单设备后端无注入面
6. 登记 js/check/ignore/garnet/libs/server/Config/ConfigNameComparer.yml：rust CONFIG 按名解析走 RuntimeServerConfig::try_get_type 单点 eq_ignore_ascii_case 线性扫描，无哈希字典消费面，无需比较器
7. storage.yml 已有 Devices.cs 文件级登记，不重复登记

验收：worktree 内 cargo check 通过；不跑 test.sh / clippy.sh / check.js
