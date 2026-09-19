优先级：中
来源：next/agy.db.md 条 8 与 next/muse.db.md 条 13 两轮同题合并。取证基线：主仓 dev 当下代码。

订正注记（认领时复核 HEAD=f708718）：本票 :17-19 与旋钮票的「先拆后删、九口待删」顺序
建议已成旧指针——运行期可变九口与 segmented 构造别名均已删净，旋钮唯一注入口是
DeviceParams（载体 task/done/wdev-segmented-device-mutator-knobs.md，be2fca2 落地），
本票只做文件拆分、不改任何语义；该轮删码使 segmented_device.rs 由 1437 行降为 1432 行，
巨石判定不变（同 crate 其余五件合计 810 行）。

问题
wdev segmented_device.rs 1437 行巨石：段映射、句柄池、DirectIO 扇区探测、跨段拆分
读写、刷盘同步、物理截断删除、目录恢复扫描与 Device trait 实现同文件，远超同 crate
其余文件总量（chunk.rs 189 / device.rs / sys.rs）。

取证
- wedb/wdev/src/segmented_device.rs 全 1437 行：get_segment_and_offset :373、
  within_single_segment :643、读写切片循环 :1022 / :1206（复用 SegmentChunks）、
  恢复扫描 device recover（对标 LocalStorageDevice.RecoverFiles）、BufferPool 装配
  :220-:278。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs、
  ManagedLocalStorageDevice.cs、StorageDeviceBase.cs——C# 侧设备族按职责分文件
  （基类对齐校验 / 本地设备读写 / 托管包装），无千行单文件形态。
- 与现有票 next/wdev-segmented-device-mutator-knobs.md 不同面：该票删旋钮死面
（set_capacity 等九口），本票拆文件结构；认领顺序建议先拆后删或同分支一体处理，
避免对同一文件两轮冲突。

修法建议
按 handle.rs（句柄表与路径、single_file/with_pool 构造）、io.rs（对齐读写切片与
快慢路径）、truncate.rs（物理截断与段删除）、recover.rs（目录恢复扫描与元数据
校验）拆子模块，mod.rs 保留 SegmentedDevice 定义与 Device trait impl 统一对外；
pub API 路径不变。纯搬运，禁止夹带行为改动（对齐校验单点 validate_aligned_io 已在
chunk.rs，保持复用不动）。

结案注记（合并 sha a954680，载体分支 wdev-seg-split 32ebaee）
按票面职责块拆为目录模块 segmented_device/：mod.rs 373 行（结构 + DeviceParams
构造注入面 + Device trait 转发门面 + Drop）、handle.rs 274（段名路径与 Thread-Per-Core
句柄表、DirectIO 探测定型、reset）、io.rs 299（get_segment_and_offset 与
within_single_segment 寻址、read_impl 与 write_impl 快慢路径）、sync.rs 216（sync
sync_data sync_internal 与 debug 契约守护）、truncate.rs 183（get_file_size、
remove_segment、handle_capacity、Windows 延迟删除、truncate_until_segment_impl）、
recover.rs 209（段名编解码、SegmentEntries 流式扫描、recover 与内联测试）。
构造三口 new/with_params/with_pool 与 DeviceParams 同留门面件（旋钮唯一注入口与结构
同处，句柄件不持构造），余按票面。原 1432 行单文件降为最大 373 行，无 shim、无旧码。

中性取证：改动前后 .rs 行多重集比对，消失行仅 21 条（11 条签名加 pub(super) 或改
_impl 名、10 条导入拆件）；新增 119 行全部为件内导入、件首说明与两行门面转发，
零逻辑行。C# 锚点多重集逐条相同（10 条，形态计数不变）。bun js/check.js 前后输出
逐字节相同、ignore 语料零回写。私有 target /tmp/ct-wdev-split 实测
cargo check --tests -p wdev -p waof -p whlog -p wkv -p wnode -p wedb 退出 0，
bench 与 regress 两个独立 workspace cargo check 退出 0（均经 wdev::SegmentedDevice
公开路径，零引用点改动），wdev nextest 61/61、wdev+waof 139/139 复跑五次全绿，
另以 --target x86_64-unknown-linux-gnu 与 x86_64-pc-windows-msvc 分别 check 通过，
覆盖 O_DIRECT 探测与 Windows 延迟删除两处 cfg 专属块。
