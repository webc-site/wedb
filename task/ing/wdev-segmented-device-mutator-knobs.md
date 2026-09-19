优先级：低
认领注记（fixloop 单棒 fix-wdev-mutator-knobs，2026-09-19 甄别）

甄别结论：本票九口已落八口，剩一构造别名与一处语料锚点未清，属部分落地，按剩余面续做不重开。

已落地取证（主仓 dev 现状 grep 零命中，仅定义位与历史注释）：
wdev/src/segmented_device.rs 现状无 set_capacity、set_read_only、set_preallocate、
set_delete_on_close、is_read_only、is_preallocate、is_delete_on_close、dir_sync_count；
设备旋钮唯一注入口为 DeviceParams 构造族（segmented_device.rs:180 结构、:194 new、
:208 with_params、:231 with_pool），字段在设备内部真实消费（:515 :526 只读保护与预分配、
:262 :726 容量口径与逐出、:1349 析构即删），wdev/src/lib.rs:18 已写明「运行期无可变入口」。
生产装配单点 wnode/src/service.rs:788、:848、:1210、:1234 与
wedb/src/server/replication/snapshot_transmission.rs:679 一律经 single_file(...) 构造期直传。
C# 对位同口径：Devices.cs:36 CreateLogDevice 形参 preallocateFile/deleteOnClose/capacity/readOnly
经 :50-:59 分注入各设备构造函数（ManagedLocalStorageDevice.cs:18、:19、:53 readonly 字段 +
ctor 注入），生产侧唯一真消费点 SubscribeBroker.cs:42 传 preallocateFile: false，其余为测试与基准；
rust 无运行期 mutator 即与 C# 一致，非功能缺口。

剩余面与修法：
1. 构造别名 segmented（segmented_device.rs:323）零生产消费者，仅 wdev/waof/whlog/wkv/wnode
   测试在调，共 55 处；语义与 new(path, Some(size), sector) 完全重合，属第二套构造入口。
   删除该别名，测试改用 new 显式传段尺寸与扇区，保留在产的 single_file（同形态但生产在调）。
2. ignore 语料锚点清净：js/check/ignore/storage.yml:945-950 Devices.cs 条目的理由段仍写
   「保留 set_capacity、set_delete_on_close、set_preallocate、set_read_only、segmented、
   is_delete_on_close、is_preallocate 作为存储设备通用构造器」，且声称对位件为已不存在的
   SegmentedDevice::open，与代码现状矛盾，按 C# 构造形参单点注入的既有结论重写。
3. 测试注释残锚：wdev/tests/device/capacity.rs:48 以已删的「旧的运行期 set_capacity」作对照，
   改述为构造期校验自身的理由，不留已删 API 的引用。

门禁：CARGO_TARGET_DIR=/tmp/target-fix-wdev-knobs，仅 cargo check（含 --tests 覆盖被改测试 crate），
不跑 test.sh 与 sh/clippy.sh。
