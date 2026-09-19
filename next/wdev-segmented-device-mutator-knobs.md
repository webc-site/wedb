优先级：低
分拣注记（qw.design 第 11 轮条 2 拆出；浅核 2026-09-19：set_capacity :263、dir_sync_count :332、segmented :344 均在场；与 done/wconf-device-config-dead-module.md 不同面——那票管 wconf 配置域死模块，本票管 wdev 设备 mutator 口，修法方向一致互为呼应）

设备旋钮族九口零生产消费者：wdev SegmentedDevice 四 setter + 三 is_* getter + dir_sync_count +
segmented 构造别名只有用例在调，C# 侧无此 mutator 形态
问题：wdev/src/segmented_device.rs 的公开旋钮面 set_capacity :263、set_read_only :275、
set_preallocate :282、set_delete_on_close :289、is_read_only :296、is_preallocate :302、
is_delete_on_close :308、dir_sync_count :332、构造别名 segmented :344（同形态 single_file :338 在产）
生产视图全部零调用：四 setter 读者仅 wdev/tests/device/lifecycle.rs:162、:195 一带与
wnode/tests/aof_sharded_commit.rs:40，三 is_* 仅被同测试文件断言（lifecycle.rs:163、:195、:228），
dir_sync_count 全仓出现次数 = 1（定义行本身）；设备实参在装配处由 wnode/src/service.rs:747、:807、:1157
经 single_file(...) 构造期直传，从不事后 set_*，故 preallocate/delete_on_close 写入后无生产读者。
C# 对位是单链一处定义、无 mutator 形态：Devices.cs:36 CreateLogDevice 形参
（preallocateFile/deleteOnClose/capacity/readOnly）→ :50-:59 按类型分派各设备构造函数
（ManagedLocalStorageDevice.cs:53 形参 + :18-:19 readonly 字段、NativeStorageDevice.cs:1267 同形），
且全仓无 SegmentedDevice.cs 同名文件（rust 该文件与分段形态是 wdev 侧自造，无 C# 锚点可依）。
修法：九口删（is_read_only 若确需可降为私有自读），设备参数只经 new :183 / with_pool :203 构造注入；
容量/预分配/关闭即删若为运行期真需，按 C# 形态补构造形参并在装配单点传入，
同时按已落地的 wconf 设备配置票同一结论在 js/check/ignore 登记，不留「两半各删一半」的中间态。
c#：garnet/libs/storage/Tsavorite/cs/src/core/Device/Devices.cs:36、:50-59（CreateLogDevice 单点工厂与参数分派）；
garnet/libs/storage/Tsavorite/cs/src/core/Device/ManagedLocalStorageDevice.cs:18、:19、:53（readonly 字段 + ctor 注入）；
garnet/libs/storage/Tsavorite/cs/src/core/Device/NativeStorageDevice.cs:1267、:1289（同形）
