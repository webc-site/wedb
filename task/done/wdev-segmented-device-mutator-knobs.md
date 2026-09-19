优先级：低
认领注记（fixloop 单棒 fix-wdev-mutator-knobs，2026-09-19 甄别）

甄别结论：本票九口已落八口，剩一构造别名与两处锚点未清，属部分落地，按剩余面续做不重开。

已落地取证（dev 现状 grep 零命中）：
wdev/src/segmented_device.rs 现状无 set_capacity、set_read_only、set_preallocate、
set_delete_on_close、is_read_only、is_preallocate、is_delete_on_close、dir_sync_count；
设备旋钮唯一注入口为 DeviceParams 构造族（:180 结构、:194 new、:208 with_params、
:231 with_pool），字段在设备内部真实消费（:515 :526 只读保护与预分配、:262 :726 容量口径
与逐出、:1349 析构即删），wdev/src/lib.rs:18 已写明「运行期无可变入口」。
生产装配单点 wnode/src/service.rs:788、:848、:1210、:1234 与
wedb/src/server/replication/snapshot_transmission.rs:679 一律经 single_file(...) 构造期直传。
C# 对位同口径：Devices.cs:36 CreateLogDevice 形参 preallocateFile/deleteOnClose/capacity/readOnly
经 :50-:59 分注入各设备构造函数（ManagedLocalStorageDevice.cs:18、:19、:53 readonly 字段 +
ctor 注入），生产侧唯一真消费点 SubscribeBroker.cs:42 传 preallocateFile: false，其余为测试与基准；
rust 无运行期 mutator 即与 C# 一致，非功能缺口，故按「零生产消费者即删净」处置。

剩余面与修法（本棒落地）：
1. 构造别名 segmented（原 :323）零生产消费者，仅测试在调，语义与 new(path, Some(段尺寸), 扇区)
   完全重合，属第二套构造入口：删除别名，测试改走 new；在产的 single_file 保留。
2. 票面遗漏的两处外部消费者（票只盘了 wedb/ 域）：bench/bench/src/engines/wkv_engine.rs:45 与
   regress/src/harness/mod.rs:94 亦在调别名，两工程不引 wbase（Cargo.toml 禁改），改为本工程
   本地扇区常量（对齐 wdev 缺省 4096，注释标明对齐口径）后走 new。
3. ignore 语料锚点清净：js/check/ignore/storage.yml Devices.cs 条目的理由段原写「保留
   set_capacity、…、segmented、… 作为存储设备通用构造器」且声称对位件为已不存在的
   SegmentedDevice::open，与代码矛盾，重写为构造族单点注入的既有结论；形态取 check.js
   序列化器输出的标量形态，复跑 check.js 逐字节稳定（bun js/check.js 退出 0、Device 域零缺失）。
4. 测试注释残锚：wdev/tests/device/capacity.rs 以已删的「旧的运行期 set_capacity」作对照，
   改述为构造期校验前置自身的路径不变量，不留已删 API 引用。

门禁与落地：CARGO_TARGET_DIR=/tmp/target-fix-wdev-knobs，仅 cargo check（--tests 覆盖
wdev/waof/whlog/wkv/wnode/wedb，另 bench --no-default-features --features wkv 与 regress 全量），
零警告零错误；未跑 test.sh 与 sh/clippy.sh（交主代理）。分支提交：test 改口 9f2c8ec、
删别名 4b48415、锚点清净 814a99f+9782829、bench/regress 收口 4a01d77；
合并 dev 提交 94a805d、6ffa592；回合主仓 dev 合并提交 be2fca2。
